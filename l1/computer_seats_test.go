package l1

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

func TestComputerWantedSeatListRetainsIdentityTombstones(t *testing.T) {
	h := newIntegrationHarnessWithPolicies(t, map[string]NodePolicy{"computer-node": {Tags: []string{contract.StableNodeTagPrefix + "computer-node"}, MaxServiceSlots: 2}})
	settings := ComputerSettings{SeatLimit: 2, IdleAction: ComputerIdleDelete, Network: ComputerNetworkNone}
	computer, _, err := h.store.CreateComputer(t.Context(), CreateComputerRequest{Name: "seat-list", Spec: computerCapabilityJobSpec("seat-list"), Actor: "test", SeatRequestSettings: &settings})
	if err != nil {
		t.Fatal(err)
	}
	one := WantedComputerSeat{SeatID: "seat_" + strings.Repeat("a", 32), Place: 1}
	two := WantedComputerSeat{SeatID: "seat_" + strings.Repeat("b", 32), Place: 2}
	list, err := h.store.SetComputerWantedSeats(t.Context(), computer.ComputerID, 0, []WantedComputerSeat{two, one})
	if err != nil || list.Revision != 1 || len(list.Seats) != 2 || list.Seats[0] != one {
		t.Fatalf("list=%+v, %v", list, err)
	}
	if _, err := h.store.SetComputerWantedSeats(t.Context(), computer.ComputerID, 0, nil); errorCode(err) != contract.ErrorConflict {
		t.Fatalf("stale revision accepted: %v", err)
	}
	list, err = h.store.SetComputerWantedSeats(t.Context(), computer.ComputerID, 1, []WantedComputerSeat{two})
	if err != nil || list.Revision != 2 || len(list.Seats) != 1 {
		t.Fatalf("remove=%+v, %v", list, err)
	}
	if _, err := h.store.SetComputerWantedSeats(t.Context(), computer.ComputerID, 2, []WantedComputerSeat{one, two}); errorCode(err) != contract.ErrorConflict {
		t.Fatalf("retired Seat ID reused: %v", err)
	}
	moved := two
	moved.Place = 1
	if _, err := h.store.SetComputerWantedSeats(t.Context(), computer.ComputerID, 2, []WantedComputerSeat{moved}); errorCode(err) != contract.ErrorConflict {
		t.Fatalf("Seat place was not immutable: %v", err)
	}
	if _, err := h.store.SetComputerWantedSeats(t.Context(), computer.ComputerID, 2, []WantedComputerSeat{{SeatID: "invalid", Place: 1}}); errorCode(err) != contract.ErrorInvalidRequest {
		t.Fatalf("invalid Seat ID accepted: %v", err)
	}
	other, _, err := h.store.CreateComputer(t.Context(), CreateComputerRequest{Name: "other-seat-list", Spec: computerCapabilityJobSpec("other-seat-list"), Actor: "test", SeatRequestSettings: &settings})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.SetComputerWantedSeats(t.Context(), other.ComputerID, 0, []WantedComputerSeat{one}); errorCode(err) != contract.ErrorConflict {
		t.Fatalf("Seat ID moved between Computers: %v", err)
	}
}

func TestComputerSeatObservationRouteFencesAndRetainsEvidence(t *testing.T) {
	h := newIntegrationHarnessWithPolicies(t, map[string]NodePolicy{"computer-node": {Tags: []string{contract.StableNodeTagPrefix + "computer-node"}, MaxServiceSlots: 2}})
	node := registerCapabilityNodeWithTags(t, h, "computer-node", map[string]bool{"kind:oci": true, "cgroup_v2": true, "computer": true}, []string{contract.StableNodeTagPrefix + "computer-node"})
	settings := ComputerSettings{SeatLimit: 2, IdleAction: ComputerIdleDelete, Network: ComputerNetworkNone}
	computer, _, err := h.store.CreateComputer(t.Context(), CreateComputerRequest{Name: "observations", Spec: computerCapabilityJobSpec("observations"), Actor: "test", SeatRequestSettings: &settings})
	if err != nil {
		t.Fatal(err)
	}
	seat := WantedComputerSeat{SeatID: "seat_" + strings.Repeat("c", 32), Place: 1}
	list, err := h.store.SetComputerWantedSeats(t.Context(), computer.ComputerID, 0, []WantedComputerSeat{seat})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := h.store.ClaimJob(t.Context(), "fabric-computer-node", node.NodeID, node.BootSessionID, contract.JobClassService)
	if err != nil || claim == nil {
		t.Fatalf("claim=%+v %v", claim, err)
	}
	auth := ComputerSeatAttemptRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID, FencingToken: claim.Lease.FencingToken}
	path := "/v1/agent/computers/" + computer.ComputerID + "/jobs/" + claim.Job.JobID + "/attempts/" + claim.Lease.AttemptID
	client := h.client(fabric.Identity{NodeID: "fabric-computer-node", Tags: []string{DefaultAgentPrincipalTag}})
	status, _, body := h.do(client, http.MethodPost, path+"/seat-list", auth)
	if status != 200 {
		t.Fatalf("list %d %s", status, body)
	}
	var got ComputerSeatList
	if err := json.Unmarshal(body, &got); err != nil || got.Revision != list.Revision || len(got.Seats) != 1 {
		t.Fatalf("list %+v %v", got, err)
	}
	report := ComputerSeatObservationReport{NodeID: auth.NodeID, BootSessionID: auth.BootSessionID, FencingToken: auth.FencingToken, ListRevision: list.Revision, Generation: 1, Sequence: 1, Seats: []ComputerSeatObservation{{SeatID: seat.SeatID, Place: 1, State: ComputerSeatReady}}}
	send := func(want int) {
		t.Helper()
		status, _, body = h.do(client, http.MethodPost, path+"/seat-observations", report)
		if status != want {
			t.Fatalf("report %d want %d: %s", status, want, body)
		}
	}
	send(200)
	send(200) // Exact retries are idempotent.
	report.FencingToken = "stale"
	send(409)
	report.FencingToken = auth.FencingToken
	report.BootSessionID = "replaced"
	send(409)
	report.BootSessionID = auth.BootSessionID
	report.Sequence = 2
	report.Generation = 0
	send(400)
	report.Generation = 1
	report.Seats[0].State = ComputerSeatStarting
	send(200) // Image may rewrite another generation.
	report.Sequence = 1
	report.Seats[0].State = ComputerSeatReady
	send(409)
	report.Sequence = 3
	report.Seats[0].State = ComputerSeatFailed
	report.Seats[0].Reason = "readiness_deadline"
	send(200)
	failed, err := h.store.GetComputerSeatObservations(t.Context(), computer.ComputerID)
	if err != nil || len(failed) != 1 || failed[0].State != ComputerSeatFailed || failed[0].Reason != "readiness_deadline" || failed[0].SeatID != seat.SeatID {
		t.Fatalf("failed Seat reason was not retained: %+v %v", failed, err)
	}
	list, err = h.store.SetComputerWantedSeats(t.Context(), computer.ComputerID, list.Revision, nil)
	if err != nil {
		t.Fatal(err)
	}
	report.Sequence = 4
	send(409) // A stale wanted revision cannot overwrite evidence.
	report.ListRevision = list.Revision
	report.Generation = 2
	report.Seats[0].State = ComputerSeatCleaned
	report.Seats[0].Reason = ""
	send(400)
	report.Seats[0].CleanupComplete = true
	send(200)
	report.Sequence = 5
	report.Generation = 1
	send(409) // A rising sequence cannot lower the helper generation.
	report.Generation = 2
	report.Seats[0].State = ComputerSeatRemoving
	report.Seats[0].CleanupComplete = false
	send(409)
	observations, err := h.store.GetComputerSeatObservations(t.Context(), computer.ComputerID)
	if err != nil || len(observations) != 1 || observations[0].State != ComputerSeatCleaned || observations[0].AttemptID != claim.Lease.AttemptID || observations[0].Generation != 2 {
		t.Fatalf("evidence %+v %v", observations, err)
	}
	wrong := h.client(fabric.Identity{NodeID: "other", Tags: []string{DefaultAgentPrincipalTag}})
	status, _, body = h.do(wrong, http.MethodPost, path+"/seat-list", auth)
	if status != 403 {
		t.Fatalf("wrong identity %d %s", status, body)
	}
	h.clock.Advance(claim.Lease.LeaseTTL + time.Second)
	status, _, body = h.do(client, http.MethodPost, path+"/seat-list", auth)
	if status != 409 {
		t.Fatalf("expired lease read %d %s", status, body)
	}
	send(409)
}
