package l1

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/Derek-X-Wang/wefty/fabric"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

func liveSeatForCopy(t *testing.T) (*seatLifecycleFixture, string, string, SeatCopyAuthorityInput) {
	t.Helper()
	f := newSeatLifecycleFixture(t)
	id := "seat_00000000000000000000000000000001"
	if _, err := f.store.CreateSeatCall(t.Context(), "cli", SeatCallInput{SeatID: id, NodeID: f.node.NodeID, Harness: "test", Folder: "/workspace", Form: "any", Network: "none"}); err != nil {
		t.Fatal(err)
	}
	f.request(t, 1, true)
	f.reserve(t, id, f.computer, 1, DefaultComputerRoomConfig(1<<30))
	if _, err := f.store.StartSeatRequest(t.Context(), "fabric-computer-node", id, SeatStartRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, ReadyWait: time.Minute}); err != nil {
		t.Fatal(err)
	}
	claim, err := f.store.ClaimJob(t.Context(), "fabric-computer-node", f.node.NodeID, f.node.BootSessionID, contract.JobClassService)
	if err != nil || claim == nil {
		t.Fatalf("claim=%v %v", claim, err)
	}
	auth := ComputerSeatAttemptRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, FencingToken: claim.Lease.FencingToken}
	list, err := f.store.GetComputerSeatListForAttempt(t.Context(), "fabric-computer-node", f.computer.ComputerID, claim.Job.JobID, claim.Lease.AttemptID, auth)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.store.ReportComputerSeatObservations(t.Context(), "fabric-computer-node", f.computer.ComputerID, claim.Job.JobID, claim.Lease.AttemptID, ComputerSeatObservationReport{NodeID: auth.NodeID, BootSessionID: auth.BootSessionID, FencingToken: auth.FencingToken, ListRevision: list.Revision, Generation: 1, Sequence: 1, Seats: []ComputerSeatObservation{{SeatID: id, Place: 1, State: ComputerSeatReady}}}); err != nil {
		t.Fatal(err)
	}
	result, err := f.store.GetSeatCall(t.Context(), "cli", id)
	if err != nil || result.Credential == "" {
		t.Fatalf("credential: %+v %v", result, err)
	}
	return f, id, result.Credential, SeatCopyAuthorityInput{ComputerSeatAttemptRequest: auth, ComputerID: f.computer.ComputerID, JobID: claim.Job.JobID, AttemptID: claim.Lease.AttemptID, Credential: result.Credential}
}
func TestSeatCopyAuthorityLogsAndEndFence(t *testing.T) {
	f, id, secret, input := liveSeatForCopy(t)
	result, err := f.store.AuthorizeSeatCopy(t.Context(), "fabric-computer-node", id, input)
	if err != nil || result.Generation != 1 || result.Place != 1 {
		t.Fatalf("authority=%+v %v", result, err)
	}
	bad := input
	bad.Credential = "wrong"
	if _, err = f.store.AuthorizeSeatCopy(t.Context(), "fabric-computer-node", id, bad); errorCode(err) != contract.ErrorForbidden {
		t.Fatalf("invalid credential: %v", err)
	}
	if _, err = f.store.AuthorizeSeatCopy(t.Context(), "another-node", id, input); err == nil {
		t.Fatal("foreign node accepted")
	}
	for _, direction := range []string{"in", "out"} {
		receipt := SeatCopyReceipt{SeatCopyAuthorityInput: input, CopyID: "copy-" + direction, Direction: direction, Path: ".env", SizeBytes: 7}
		for range 2 {
			if err = f.store.RecordSeatCopy(t.Context(), "fabric-computer-node", id, receipt); err != nil {
				t.Fatal(err)
			}
		}
	}
	page, err := f.store.GetSeatLogs(t.Context(), "cli", id, secret, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	copies := 0
	for _, event := range page.Events {
		if event.Kind == SeatEventCopyIn || event.Kind == SeatEventCopyOut {
			copies++
			if event.Path != ".env" || event.SizeBytes == nil || *event.SizeBytes != 7 {
				t.Fatalf("copy event: %+v", event)
			}
		}
	}
	if copies != 2 {
		t.Fatalf("copy events=%d", copies)
	}
	if _, err = f.store.EndSeatRequest(t.Context(), "fabric-computer-node", id, SeatEndRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, Reason: "released"}); err != nil {
		t.Fatal(err)
	}
	if _, err = f.store.AuthorizeSeatCopy(t.Context(), "fabric-computer-node", id, input); errorCode(err) != contract.ErrorCode("seat_ended") {
		t.Fatalf("ended Seat: %v", err)
	}
	baseline, _, _, _ := liveSeatForCopy(t)
	for _, fixture := range []*seatLifecycleFixture{baseline, f} {
		current, e := fixture.store.GetComputer(t.Context(), fixture.computer.ComputerID)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = fixture.store.RemoveComputer(t.Context(), current.ComputerID, ComputerRemoveRequest{ComputerMutationPrecondition: computerPrecondition(current, "test")}); e != nil {
			t.Fatal(e)
		}
		directives, e := fixture.store.ListNodeRemovalDirectives(t.Context(), "fabric-computer-node", fixture.node.NodeID, fixture.node.BootSessionID)
		if e != nil || len(directives) != 1 {
			t.Fatalf("removal directives=%+v %v", directives, e)
		}
		d := directives[0]
		if _, e = fixture.store.AcknowledgeServiceRemoval(t.Context(), "fabric-computer-node", current.CurrentJobID, RemovalAcknowledgementRequest{NodeID: fixture.node.NodeID, BootSessionID: fixture.node.BootSessionID, RemovalGeneration: d.RemovalGeneration, CleanupFence: d.CleanupFence, RootInstanceID: d.RootInstanceID, IdempotencyKey: "copy-removal"}); e != nil {
			t.Fatal(e)
		}
		if _, _, e = fixture.store.FinalizeServiceRemoval(t.Context(), current.CurrentJobID); e != nil {
			t.Fatal(e)
		}
		removed, e := fixture.store.GetComputer(t.Context(), current.ComputerID)
		if e != nil || removed.RemovalOutcome != "removed_verified" {
			t.Fatalf("copy changed removal proof: %+v %v", removed, e)
		}
	}

}

func TestSeatCopyUnpublishedEndpointIsUnavailable(t *testing.T) {
	f, id, secret, _ := liveSeatForCopy(t)
	if _, err := f.store.GetSeatCopyTarget(t.Context(), id, secret); errorCode(err) != contract.ErrorUnavailable {
		t.Fatalf("unpublished endpoint must be unavailable: %v", err)
	}
}

func TestSeatCopyOutcomesAndAttemptFences(t *testing.T) {
	f, id, secret, auth := liveSeatForCopy(t)
	node := SeatCopyReceipt{SeatCopyAuthorityInput: auth, CopyID: "outcome", Direction: "out", Path: "report", SizeBytes: 7, Outcome: "sent"}
	if err := f.store.RecordSeatCopy(t.Context(), "fabric-computer-node", id, node); err != nil {
		t.Fatal(err)
	}
	page, err := f.store.GetSeatLogs(t.Context(), "cli", id, secret, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	cursor := page.NextCursor
	caller := SeatCopyOutcomeReceipt{CopyID: node.CopyID, Direction: "out", Path: "report", SizeBytes: 7, Outcome: "failed", Reason: "destination_write_denied"}
	server, e := NewServer(copyOutcomeFabric{}, f.store, ServerConfig{})
	if e != nil {
		t.Fatal(e)
	}
	sendOutcome := func(target, credential string, value SeatCopyOutcomeReceipt) int {
		t.Helper()
		body, e := json.Marshal(value)
		if e != nil {
			t.Fatal(e)
		}
		req := httptest.NewRequest(http.MethodPost, "/v1/seats/"+target+"/copy-outcome", bytes.NewReader(body))
		req.Header.Set(SeatCredentialHeader, credential)
		response := httptest.NewRecorder()
		server.Handler().ServeHTTP(response, req)
		return response.Code
	}
	if status := sendOutcome(id, secret, caller); status != 200 {
		t.Fatalf("HTTP outcome=%d", status)
	}
	if status := sendOutcome("seat_00000000000000000000000000000002", secret, caller); status != 403 {
		t.Fatalf("foreign Seat outcome=%d", status)
	}

	for range 2 {
		if err = f.store.ReportSeatCopyOutcome(t.Context(), id, secret, caller); err != nil {
			t.Fatal(err)
		}
	}
	if err = f.store.RecordSeatCopy(t.Context(), "fabric-computer-node", id, node); err != nil {
		t.Fatalf("node retry after caller outcome: %v", err)
	}
	page, err = f.store.GetSeatLogs(t.Context(), "cli", id, secret, cursor, 100)
	if err != nil || len(page.Events) != 1 || page.Events[0].Outcome != "failed" || page.Events[0].Reason != "destination_write_denied" || *page.Events[0].SizeBytes != 7 {
		t.Fatalf("outcome update must be visible after sent cursor: %+v %v", page, err)
	}
	all, err := f.store.GetSeatLogs(t.Context(), "cli", id, secret, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, e := range all.Events {
		if e.CopyID == "outcome" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("duplicate copy events=%d", count)
	}
	// Copy-in evidence is node-only; a caller cannot relabel its result.
	in := node
	in.CopyID = "landed"
	in.Direction = "in"
	in.Outcome = "completed"
	if err = f.store.RecordSeatCopy(t.Context(), "fabric-computer-node", id, in); err != nil {
		t.Fatal(err)
	}
	failure := caller
	failure.CopyID = "landed"
	failure.Direction = "in"
	if err = f.store.ReportSeatCopyOutcome(t.Context(), id, secret, failure); errorCode(err) != contract.ErrorInvalidRequest {
		t.Fatalf("caller must not update copy-in: %v", err)
	}
	// Validate both admission and recording with the stale fence.
	stale := auth
	stale.FencingToken = "stale"
	if _, err = f.store.AuthorizeSeatCopy(t.Context(), "fabric-computer-node", id, stale); err == nil {
		t.Fatal("stale admission accepted")
	}
	staleReceipt := node
	staleReceipt.CopyID = "stale"
	staleReceipt.SeatCopyAuthorityInput = stale
	if err = f.store.RecordSeatCopy(t.Context(), "fabric-computer-node", id, staleReceipt); err == nil {
		t.Fatal("stale receipt accepted")
	}
	ended, endedID, endedSecret, endedAuth := liveSeatForCopy(t)
	endedReceipt := SeatCopyReceipt{SeatCopyAuthorityInput: endedAuth, CopyID: "ended-copy", Direction: "out", Path: "result", SizeBytes: 7, Outcome: "sent"}
	if err = ended.store.RecordSeatCopy(t.Context(), "fabric-computer-node", endedID, endedReceipt); err != nil {
		t.Fatal(err)
	}
	if _, err = ended.store.EndSeatRequest(t.Context(), "fabric-computer-node", endedID, SeatEndRequest{NodeID: ended.node.NodeID, BootSessionID: ended.node.BootSessionID, Reason: "released"}); err != nil {
		t.Fatal(err)
	}
	endedServer, e := NewServer(copyOutcomeFabric{}, ended.store, ServerConfig{})
	if e != nil {
		t.Fatal(e)
	}
	server = endedServer
	if status := sendOutcome(endedID, endedSecret, SeatCopyOutcomeReceipt{CopyID: "ended-copy", Direction: "out", Path: "result", SizeBytes: 7, Outcome: "completed"}); status == 200 {
		t.Fatalf("ended receipt accepted")
	}
	if err = ended.store.ReportSeatCopyOutcome(t.Context(), endedID, endedSecret, SeatCopyOutcomeReceipt{CopyID: "ended-copy", Direction: "out", Path: "result", SizeBytes: 7, Outcome: "completed"}); errorCode(err) != contract.ErrorCode("seat_ended") {
		t.Fatalf("ended Store receipt must be refused: %v", err)
	}

	// Fake the L1 projection moving to a second Attempt, without changing
	// this Seat's immutable original binding or its live state.
	secondID, secondFence := "second-copy-attempt", "second-copy-fence"
	if _, err = f.store.db.Exec(`INSERT INTO attempts(attempt_id,job_id,node_id,boot_session_id,state,fencing_token,lease_expires_ns,authority_generation,created_ns,updated_ns) SELECT ?,job_id,node_id,boot_session_id,state,?,lease_expires_ns,authority_generation,created_ns,updated_ns FROM attempts WHERE attempt_id=?`, secondID, secondFence, auth.AttemptID); err != nil {
		t.Fatal(err)
	}
	if _, err = f.store.db.Exec(`UPDATE jobs SET current_attempt_id=? WHERE job_id=?`, secondID, auth.JobID); err != nil {
		t.Fatal(err)
	}
	for _, a := range []SeatCopyAuthorityInput{auth, func() SeatCopyAuthorityInput {
		v := auth
		v.AttemptID = secondID
		v.FencingToken = secondFence
		return v
	}()} {
		if _, err = f.store.AuthorizeSeatCopy(t.Context(), "fabric-computer-node", id, a); err == nil {
			t.Fatal("old Seat admitted on rotated Attempt")
		}
		r := node
		r.CopyID = "rotated"
		r.SeatCopyAuthorityInput = a
		if err = f.store.RecordSeatCopy(t.Context(), "fabric-computer-node", id, r); err == nil {
			t.Fatal("old Seat receipt accepted on rotated Attempt")
		}
	}
}

type copyOutcomeFabric struct{ fabric.Fabric }

func (copyOutcomeFabric) WhoIs(context.Context, string) (fabric.Identity, error) {
	return fabric.Identity{NodeID: "copy-client", Tags: []string{DefaultClientPrincipalTag}}, nil
}
