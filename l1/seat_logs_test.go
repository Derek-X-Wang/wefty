package l1

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

func TestSeatLogRequestReplayAndCallerFence(t *testing.T) {
	h := newIntegrationHarnessWithPolicies(t, map[string]NodePolicy{"computer-node": {Tags: []string{contract.StableNodeTagPrefix + "computer-node"}, MaxServiceSlots: 4}})
	registerCapabilityNodeWithTags(t, h, "computer-node", map[string]bool{CapabilitySeatRequests: true}, []string{contract.StableNodeTagPrefix + "computer-node"})
	caller := h.client(fabric.Identity{NodeID: "cli", Tags: []string{DefaultClientPrincipalTag}})
	other := h.client(fabric.Identity{NodeID: "other", Tags: []string{DefaultClientPrincipalTag}})
	id := "seat_00000000000000000000000000000001"
	input := SeatCallInput{SeatID: id, NodeID: "computer-node", Harness: "test", Folder: "/workspace", Form: "any", Network: "none"}
	for range 2 {
		if _, err := h.store.CreateSeatCall(t.Context(), "cli", input); err != nil {
			t.Fatal(err)
		}
	}
	status, _, body := h.do(caller, http.MethodGet, "/v1/seats/"+id+"/logs", nil)
	if status != 200 {
		t.Fatalf("Seat log: %d %s", status, body)
	}
	var page struct {
		Events []struct {
			Kind string `json:"kind"`
		} `json:"events"`
		NextCursor string `json:"next_cursor"`
	}
	if err := json.Unmarshal(body, &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != 1 || page.Events[0].Kind != "requested" || page.NextCursor == "" {
		t.Fatalf("log: %s", body)
	}
	status, _, body = h.do(other, http.MethodGet, "/v1/seats/"+id+"/logs", nil)
	if status != 403 {
		t.Fatalf("foreign: %d %s", status, body)
	}
}

func TestSeatLogLifecycleAndReadCursor(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	id := "seat_00000000000000000000000000000001"
	input := SeatCallInput{SeatID: id, NodeID: f.node.NodeID, Harness: "test", Folder: "/workspace", Form: "any", Network: "none"}
	if _, err := f.store.CreateSeatCall(t.Context(), "cli", input); err != nil {
		t.Fatal(err)
	}
	f.request(t, 1, true)
	page, err := f.store.GetSeatLogs(t.Context(), "cli", id, "", "", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != 2 || page.Events[1].Kind != SeatEventApproved {
		t.Fatalf("approval log: %+v", page)
	}
	if err := f.store.MarkSeatLogRead(t.Context(), "cli", id, "", page.NextCursor); err != nil {
		t.Fatal(err)
	}
	result, err := f.store.GetSeatCall(t.Context(), "cli", id)
	if err != nil || result.UnreadEvents != 0 {
		t.Fatalf("read: %+v %v", result, err)
	}
	_, err = f.store.EndSeatRequest(t.Context(), "fabric-computer-node", id, SeatEndRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, Reason: "denied by the owner", Refused: true})
	if err != nil {
		t.Fatal(err)
	}
	result, err = f.store.GetSeatCall(t.Context(), "cli", id)
	if err != nil || result.UnreadEvents != 0 {
		t.Fatalf("reported refusal must acknowledge its outcome: %+v %v", result, err)
	}
	page, err = f.store.GetSeatLogs(t.Context(), "cli", id, "", page.NextCursor, 100)
	if err != nil || !page.Ended || len(page.Events) != 1 || page.Reason != "denied by the owner" {
		t.Fatalf("end: %+v %v", page, err)
	}
}

func TestSeatLogEndedCredentialScopeAndRetention(t *testing.T) {
	h := newIntegrationHarnessWithPolicies(t, map[string]NodePolicy{"computer-node": {Tags: []string{contract.StableNodeTagPrefix + "computer-node"}, MaxServiceSlots: 4}})
	node := registerCapabilityNodeWithTags(t, h, "computer-node", map[string]bool{CapabilitySeatRequests: true, "kind:oci": true, "computer": true, "cgroup_v2": true}, []string{contract.StableNodeTagPrefix + "computer-node"})
	f := &seatLifecycleFixture{store: h.store, clock: h.clock, node: node}
	f.computer = f.createComputer(t, "log-scope", 2)
	id := "seat_00000000000000000000000000000001"
	if _, err := h.store.CreateSeatCall(t.Context(), "cli", SeatCallInput{SeatID: id, NodeID: node.NodeID, Harness: "test", Folder: "/workspace", Form: "any", Network: "none"}); err != nil {
		t.Fatal(err)
	}
	f.request(t, 1, true)
	f.reserve(t, id, f.computer, 1, DefaultComputerRoomConfig(1<<30))
	if _, err := h.store.StartSeatRequest(t.Context(), "fabric-computer-node", id, SeatStartRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID, ReadyWait: time.Minute}); err != nil {
		t.Fatal(err)
	}
	claim, err := h.store.ClaimJob(t.Context(), "fabric-computer-node", node.NodeID, node.BootSessionID, contract.JobClassService)
	if err != nil || claim == nil {
		t.Fatalf("claim: %+v %v", claim, err)
	}
	auth := ComputerSeatAttemptRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID, FencingToken: claim.Lease.FencingToken}
	list, err := h.store.GetComputerSeatListForAttempt(t.Context(), "fabric-computer-node", f.computer.ComputerID, claim.Job.JobID, claim.Lease.AttemptID, auth)
	if err != nil {
		t.Fatal(err)
	}
	report := ComputerSeatObservationReport{NodeID: auth.NodeID, BootSessionID: auth.BootSessionID, FencingToken: auth.FencingToken, ListRevision: list.Revision, Generation: 1, Sequence: 1, Seats: []ComputerSeatObservation{{SeatID: id, Place: 1, State: ComputerSeatReady}}}
	if _, err := h.store.ReportComputerSeatObservations(t.Context(), "fabric-computer-node", f.computer.ComputerID, claim.Job.JobID, claim.Lease.AttemptID, report); err != nil {
		t.Fatal(err)
	}
	value, err := h.store.GetSeatCall(t.Context(), "cli", id)
	if err != nil || value.Credential == "" {
		t.Fatalf("live credential: %+v %v", value, err)
	}
	if _, err := h.store.EndSeatRequest(t.Context(), "fabric-computer-node", id, SeatEndRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID, Reason: "released"}); err != nil {
		t.Fatal(err)
	}
	// Deliberately use a privileged Fabric caller. The credential cannot fall
	// through and borrow that caller's broader operation authority.
	caller := h.client(fabric.Identity{NodeID: "cli", Tags: []string{DefaultClientPrincipalTag}})
	send := func(method, path string) (int, []byte) {
		t.Helper()
		req, err := http.NewRequestWithContext(t.Context(), method, "http://wefty.invalid"+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set(SeatCredentialHeader, value.Credential)
		resp, err := caller.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		return resp.StatusCode, body
	}
	status, body := send(http.MethodGet, "/v1/seats/"+id+"/logs")
	if status != 200 || !strings.Contains(string(body), `"reason":"released"`) {
		t.Fatalf("ended log: %d %s", status, body)
	}
	originalCaller := caller
	caller = h.client(fabric.Identity{NodeID: "different-fabric-reader"})
	if status, body := send(http.MethodGet, "/v1/seats/"+id+"/logs"); status != 200 {
		t.Fatalf("authenticated bearer holder cannot read: %d %s", status, body)
	}
	caller = originalCaller
	otherID := "seat_00000000000000000000000000000002"
	if _, err := h.store.CreateSeatCall(t.Context(), "cli", SeatCallInput{SeatID: otherID, NodeID: node.NodeID, Harness: "test", Folder: "/workspace", Form: "any", Network: "none"}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.GetSeatLogs(t.Context(), "cli", otherID, value.Credential, "", 100); errorCode(err) != contract.ErrorForbidden {
		t.Fatalf("Seat credential borrowed caller authority to read another Seat: %v", err)
	}
	if status, body := send(http.MethodGet, "/v1/seats/"+otherID+"/logs"); status != 403 {
		t.Fatalf("cross-Seat log request must be forbidden: %d %s", status, body)
	}
	for _, op := range []string{"command", "input", "copy", "screen"} {
		status, body = send(http.MethodPost, "/v1/seats/"+id+"/"+op)
		if status != 409 || !strings.Contains(string(body), `"code":"seat_ended"`) {
			t.Fatalf("%s: %d %s", op, status, body)
		}
	}
	// A log poll does not write activity or unread state.
	before, err := h.store.GetSeatCall(t.Context(), "cli", id)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := h.store.GetSeatLogs(t.Context(), "", id, value.Credential, "", 100); err != nil {
			t.Fatal(err)
		}
	}
	after, err := h.store.GetSeatCall(t.Context(), "cli", id)
	if err != nil || before.UnreadEvents != after.UnreadEvents {
		t.Fatalf("poll changed unread: %+v %+v %v", before, after, err)
	}
	h.clock.Advance(90 * 24 * time.Hour)
	status, body = send(http.MethodGet, "/v1/seats/"+id+"/logs")
	if status != 404 {
		t.Fatalf("retention: %d %s", status, body)
	}
	if _, err := h.store.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	status, body = send(http.MethodGet, "/v1/seats/"+id+"/logs")
	if status != 403 || !strings.Contains(string(body), "invalid Seat credential") {
		t.Fatalf("pruned digest still authorizes a request: %d %s", status, body)
	}
}

func TestSeatLogWriterAuditReplayAndRetentionCursor(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	id := "seat_00000000000000000000000000000001"
	if _, err := f.store.CreateSeatCall(t.Context(), "cli", SeatCallInput{SeatID: id, NodeID: f.node.NodeID, Harness: "test", Folder: "/workspace", Form: "any", Network: "none"}); err != nil {
		t.Fatal(err)
	}
	f.request(t, 1, true)
	event := SeatLogEvent{SeatID: id, EventID: "takeover-1:release", Kind: SeatEventControlReleased, Note: "Tests are ready", Audit: &ComputerTakeoverAuditEvent{EventID: "takeover-1:release", Kind: ComputerTakeoverControlReleased, ComputerID: f.computer.ComputerID, JobID: "job_example", AttemptID: "attempt_example", SessionID: "takeover-1", Reason: ComputerTakeoverExplicitRelease}}
	for range 2 {
		if err := f.store.AppendSeatEvent(t.Context(), event); err != nil {
			t.Fatal(err)
		}
		f.clock.Advance(time.Second)
	}
	event.Note = "different evidence"
	if err := f.store.AppendSeatEvent(t.Context(), event); errorCode(err) != contract.ErrorIdempotencyConflict {
		t.Fatalf("conflicting replay: %v", err)
	}
	page, err := f.store.GetSeatLogs(t.Context(), "cli", id, "", "", 100)
	if err != nil || len(page.Events) != 3 || page.Events[2].Note != "Tests are ready" || page.Events[2].Audit.SessionID != "takeover-1" {
		t.Fatalf("shared audit log: %+v %v", page, err)
	}
	if _, err := f.store.EndSeatRequest(t.Context(), "fabric-computer-node", id, SeatEndRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, Reason: "denied by the owner", Refused: true}); err != nil {
		t.Fatal(err)
	}
	page, err = f.store.GetSeatLogs(t.Context(), "cli", id, "", "", 100)
	if err != nil {
		t.Fatal(err)
	}
	f.clock.Advance(90 * 24 * time.Hour)
	if err := f.store.MarkSeatLogRead(t.Context(), "cli", id, "", page.NextCursor); errorCode(err) != contract.ErrorNotFound {
		t.Fatalf("read acknowledgement past retention: %v", err)
	}
}

func TestSeatLogRetentionSweepDoesNotReseedOnRestart(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	id := "seat_00000000000000000000000000000001"
	if _, err := f.store.CreateSeatCall(t.Context(), "cli", SeatCallInput{SeatID: id, NodeID: f.node.NodeID, Harness: "test", Folder: "/workspace", Form: "any", Network: "none"}); err != nil {
		t.Fatal(err)
	}
	f.request(t, 1, true)
	if _, err := f.store.EndSeatRequest(t.Context(), "fabric-computer-node", id, SeatEndRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, Reason: "denied by the owner", Refused: true}); err != nil {
		t.Fatal(err)
	}
	f.clock.Advance(90 * 24 * time.Hour)
	if _, err := f.store.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	f.reopen(t)
	if _, err := f.store.GetSeatLogs(t.Context(), "cli", id, "", "", 100); errorCode(err) != contract.ErrorNotFound {
		t.Fatalf("expired log revived: %v", err)
	}
}
