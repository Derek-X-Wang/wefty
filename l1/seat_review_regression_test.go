package l1

import (
	"fmt"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

func reviewClaimAll(t *testing.T, f *seatLifecycleFixture) map[string]*Claim {
	t.Helper()
	claims := map[string]*Claim{}
	for range 4 {
		c, err := f.store.ClaimJob(t.Context(), "fabric-computer-node", f.node.NodeID, f.node.BootSessionID, contract.JobClassService)
		if err != nil {
			t.Fatal(err)
		}
		if c == nil {
			break
		}
		claims[c.Job.JobID] = c
	}
	return claims
}

// Probe 1: a reservation on a running Computer whose runtime limit (spec) is
// larger than the Seat target lowers the charge with no readback.
func TestReviewProbeReservationLowersRunningCharge(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	spec := computerCapabilityJobSpec("big")
	big := int64(3 << 30)
	spec.Execution.OCI.Limits.MemoryBytes = &big
	settings := ComputerSettings{SeatLimit: 2, IdleAction: ComputerIdleDelete, Network: ComputerNetworkNone}
	c, _, err := f.store.CreateComputer(t.Context(), CreateComputerRequest{Name: "big", Spec: spec, Actor: "test", SeatRequestSettings: &settings})
	if err != nil {
		t.Fatal(err)
	}
	claims := reviewClaimAll(t, f)
	t.Logf("claimed %d jobs", len(claims))
	room := DefaultComputerRoomConfig(5 << 30)
	before, err := f.store.GetComputerRoomSnapshot(t.Context(), f.node.NodeID, room)
	t.Logf("before reserve: %+v %v", before, err)
	id := f.request(t, 1, true)
	v := f.reserve(t, id, c, 1, room)
	t.Logf("reserve: %+v", v)
	after, err := f.store.GetComputerRoomSnapshot(t.Context(), f.node.NodeID, room)
	t.Logf("after reserve: %+v %v", after, err)
	if after.HeldMemoryBytes < before.HeldMemoryBytes {
		t.Errorf("PROBE: reservation lowered held room from %d to %d without readback", before.HeldMemoryBytes, after.HeldMemoryBytes)
	}
}

// Probe 2: a dedicated request whose own new Computer is already claimed
// counts itself against the dedicated cap.
func TestReviewProbeDedicatedSelfCount(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	settings := ComputerSettings{SeatLimit: 1, IdleAction: ComputerIdleDelete, Network: ComputerNetworkNone}
	c, _, err := f.store.CreateComputer(t.Context(), CreateComputerRequest{Name: "dedicated", Spec: computerCapabilityJobSpec("dedicated"), Actor: "test", SeatRequestSettings: &settings})
	if err != nil {
		t.Fatal(err)
	}
	reviewClaimAll(t, f)
	id := fmt.Sprintf("seat_%032x", 7)
	if _, err := f.store.BeginSeatApproval(t.Context(), "fabric-computer-node", SeatApprovalRequest{SeatID: id, NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, Harness: "test", Folder: "/w", Form: "dedicated", Network: "none", AnswerWait: time.Minute}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.AnswerSeatApproval(t.Context(), "fabric-computer-node", id, SeatApprovalAnswer{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, Approved: true}); err != nil {
		t.Fatal(err)
	}
	v := f.reserve(t, id, c, 1, DefaultComputerRoomConfig(8<<30))
	t.Logf("dedicated reserve: %+v", v)
	if v.State == SeatRefused {
		t.Errorf("PROBE: only dedicated request refused by its own running Computer: %q", v.Reason)
	}
}

// Probe 3: a reservation that is ended before start has no release path.
func TestReviewProbeReservedThenRefusedLeaks(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	room := DefaultComputerRoomConfig(1 << 30)
	id := f.request(t, 1, true)
	f.reserve(t, id, f.computer, 1, room)
	v, err := f.store.EndSeatRequest(t.Context(), "fabric-computer-node", id, SeatEndRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, Reason: "memory grow failed"})
	t.Logf("end: %+v %v", v, err)
	snap, err := f.store.GetComputerRoomSnapshot(t.Context(), f.node.NodeID, room)
	t.Logf("after refused-before-start: %+v %v", snap, err)
	if snap.AvailableSeats == 0 {
		t.Errorf("PROBE: refused-before-start reservation still holds the room")
	}
}

func TestReviewDedicatedReservationRaceCountsOtherComputers(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	a := f.createComputer(t, "dedicated-a", 1)
	reviewClaimAll(t, f) // The first target is already claimed; the second is queued.
	b := f.createComputer(t, "dedicated-b", 1)
	ids := []string{fmt.Sprintf("seat_%032x", 21), fmt.Sprintf("seat_%032x", 22)}
	for _, id := range ids {
		reviewDedicatedApproval(t, f, id)
	}
	start := make(chan struct{})
	type result struct {
		value SeatRequest
		err   error
	}
	results := make(chan result, 2)
	for i, c := range []Computer{a, b} {
		go func() {
			<-start
			v, e := f.store.ReserveSeatRoom(t.Context(), "fabric-computer-node", ids[i], SeatRoomReservationRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, ComputerID: c.ComputerID, Place: 1, Room: DefaultComputerRoomConfig(8 << 30)})
			results <- result{v, e}
		}()
	}
	close(start)
	wins := 0
	for range 2 {
		r := <-results
		if r.err != nil {
			t.Fatal(r.err)
		}
		if r.value.State == SeatApproved {
			wins++
		} else if r.value.State != SeatRefused {
			t.Fatalf("unexpected result: %+v", r.value)
		}
	}
	if wins != 1 {
		t.Fatalf("dedicated reservation race had %d winners", wins)
	}
}
func reviewDedicatedApproval(t *testing.T, f *seatLifecycleFixture, id string) {
	t.Helper()
	if _, err := f.store.BeginSeatApproval(t.Context(), "fabric-computer-node", SeatApprovalRequest{SeatID: id, NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, Harness: "test", Folder: "/workspace", Form: "dedicated", Network: "none", AnswerWait: time.Minute}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.AnswerSeatApproval(t.Context(), "fabric-computer-node", id, SeatApprovalAnswer{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, Approved: true}); err != nil {
		t.Fatal(err)
	}
}

func TestReviewAnyCannotUseDedicated(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	c := f.createComputer(t, "dedicated", 1)
	id := f.request(t, 31, true)
	_, err := f.store.ReserveSeatRoom(t.Context(), "fabric-computer-node", id, SeatRoomReservationRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, ComputerID: c.ComputerID, Place: 1, Room: DefaultComputerRoomConfig(4 << 30)})
	if errorCode(err) != contract.ErrorConflict {
		t.Fatalf("any request reserved a dedicated Computer: %v", err)
	}
}

func TestReviewApprovedBootChangeReleasesReservation(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	id := f.request(t, 41, true)
	room := DefaultComputerRoomConfig(1 << 30)
	f.reserve(t, id, f.computer, 1, room)
	_, err := f.store.RegisterNode(t.Context(), fabric.Identity{NodeID: "fabric-computer-node"}, contract.NodeRegistration{NodeID: f.node.NodeID, BootSessionID: "next-boot", RootInstanceID: "root", OS: "linux", Architecture: "amd64", AgentVersion: "test", Capabilities: map[string]bool{"kind:oci": true, "cgroup_v2": true, "computer": true}, CapabilityRevision: 2, CapabilityObservedAt: f.clock.Now(), MissingCapabilities: []string{}}, NodePolicy{Tags: []string{contract.StableNodeTagPrefix + "computer-node"}, MaxServiceSlots: 4}, true)
	if err != nil {
		t.Fatal(err)
	}
	v := f.state(t, id, SeatRefused)
	if !v.CleanupComplete {
		t.Fatal("never-started Seat still owes cleanup after boot change")
	}
	snap, err := f.store.GetComputerRoomSnapshot(t.Context(), f.node.NodeID, room)
	if err != nil || snap.AvailableSeats != 1 {
		t.Fatalf("boot-change room leak: %+v %v", snap, err)
	}
}
func TestReviewStartingEndIsRefusalAndApprovalReplayIsFinal(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	id := f.request(t, 42, true)
	f.reserve(t, id, f.computer, 1, DefaultComputerRoomConfig(1<<30))
	if _, err := f.store.StartSeatRequest(t.Context(), "fabric-computer-node", id, SeatStartRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, ReadyWait: time.Minute}); err != nil {
		t.Fatal(err)
	}
	v, err := f.store.EndSeatRequest(t.Context(), "fabric-computer-node", id, SeatEndRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, Reason: "cancelled before ready"})
	if err != nil || v.State != SeatRefused || v.Reason == "" {
		t.Fatalf("starting request did not finish with refusal: %+v %v", v, err)
	}
	replay, err := f.store.BeginSeatApproval(t.Context(), "fabric-computer-node", SeatApprovalRequest{SeatID: id, NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, Harness: "test", Folder: "/workspace", Form: "any", Network: "none", AnswerWait: time.Minute})
	if err != nil || replay.State != "refused" {
		t.Fatalf("approval replay revived final Seat: %+v %v", replay, err)
	}
}

func TestReviewStartUsesL1DurationAndRejectsWithoutLeaking(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	id := f.request(t, 51, true)
	f.reserve(t, id, f.computer, 1, DefaultComputerRoomConfig(1<<30))
	request := SeatStartRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, ReadyWait: time.Minute}
	expected := f.clock.Now().Add(time.Minute)
	v, err := f.store.StartSeatRequest(t.Context(), "fabric-computer-node", id, request)
	if err != nil || !v.Deadline.Equal(expected) {
		t.Fatalf("L1 did not own the deadline: %+v %v", v, err)
	}
	f.clock.Advance(10 * time.Second)
	f.reopen(t)
	v, err = f.store.StartSeatRequest(t.Context(), "fabric-computer-node", id, request)
	if err != nil || !v.Deadline.Equal(expected) {
		t.Fatalf("retry moved deadline: %+v %v", v, err)
	}
	f2 := newSeatLifecycleFixture(t)
	other := f2.request(t, 52, true)
	room := DefaultComputerRoomConfig(1 << 30)
	f2.reserve(t, other, f2.computer, 1, room)
	request.NodeID = f2.node.NodeID
	request.BootSessionID = f2.node.BootSessionID
	request.ReadyWait = 0
	if _, err := f2.store.StartSeatRequest(t.Context(), "fabric-computer-node", other, request); errorCode(err) != contract.ErrorInvalidRequest {
		t.Fatalf("invalid duration accepted: %v", err)
	}
	refused := f2.state(t, other, SeatRefused)
	if !refused.CleanupComplete {
		t.Fatal("invalid Start retained a cleanup obligation")
	}
	snap, err := f2.store.GetComputerRoomSnapshot(t.Context(), f2.node.NodeID, room)
	if err != nil || snap.AvailableSeats != 1 {
		t.Fatalf("invalid Start leaked room: %+v %v", snap, err)
	}
}
func TestReviewPendingReadExpiresAndMissingRoomIsTyped(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	id := f.request(t, 53, false)
	f.clock.Advance(time.Minute)
	v, err := f.store.GetSeatRequest(t.Context(), id)
	if err != nil || v.State != SeatRefused || v.Reason != "no answer from the owner" {
		t.Fatalf("expired approval read as pending: %+v %v", v, err)
	}
	f = newSeatLifecycleFixture(t)
	claim, err := f.store.ClaimJob(t.Context(), "fabric-computer-node", f.node.NodeID, f.node.BootSessionID, contract.JobClassService)
	if err != nil || claim == nil {
		t.Fatalf("claim %v %v", claim, err)
	}
	auth := ComputerSeatAttemptRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, FencingToken: claim.Lease.FencingToken}
	_, err = f.store.ReportComputerRoomMemory(t.Context(), "fabric-computer-node", f.computer.ComputerID, claim.Job.JobID, claim.Lease.AttemptID, SeatRoomMemoryReport{ComputerSeatAttemptRequest: auth, RoomRevision: 1, RequestedMemoryBytes: 1 << 30, MemoryLimitBytes: 1 << 30, HeldMemoryBytes: 1 << 30})
	if errorCode(err) != contract.ErrorConflict {
		t.Fatalf("missing room did not return typed conflict: %v", err)
	}
}

func reviewLiveSeat(t *testing.T, f *seatLifecycleFixture, n int) *Claim {
	t.Helper()
	id := f.request(t, n, true)
	f.reserve(t, id, f.computer, 1, DefaultComputerRoomConfig(2<<30))
	if _, err := f.store.StartSeatRequest(t.Context(), "fabric-computer-node", id, SeatStartRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, ReadyWait: time.Minute}); err != nil {
		t.Fatal(err)
	}
	claim, err := f.store.ClaimJob(t.Context(), "fabric-computer-node", f.node.NodeID, f.node.BootSessionID, contract.JobClassService)
	if err != nil || claim == nil {
		t.Fatalf("claim %v %v", claim, err)
	}
	if _, err := f.store.ObserveAttemptImage(t.Context(), "fabric-computer-node", claim.Job.JobID, claim.Lease.AttemptID, testImageObservation(claim.Lease.FencingToken)); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.StartAttempt(t.Context(), "fabric-computer-node", claim.Job.JobID, claim.Lease.AttemptID, StartedRequest{FencingToken: claim.Lease.FencingToken}); err != nil {
		t.Fatal(err)
	}
	auth := ComputerSeatAttemptRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, FencingToken: claim.Lease.FencingToken}
	list, err := f.store.GetComputerSeatListForAttempt(t.Context(), "fabric-computer-node", f.computer.ComputerID, claim.Job.JobID, claim.Lease.AttemptID, auth)
	if err != nil {
		t.Fatal(err)
	}
	report := ComputerSeatObservationReport{NodeID: auth.NodeID, BootSessionID: auth.BootSessionID, FencingToken: auth.FencingToken, ListRevision: list.Revision, Generation: 1, Sequence: 1, Seats: []ComputerSeatObservation{{SeatID: id, Place: 1, State: ComputerSeatReady}}}
	if _, err := f.store.ReportComputerSeatObservations(t.Context(), "fabric-computer-node", f.computer.ComputerID, claim.Job.JobID, claim.Lease.AttemptID, report); err != nil {
		t.Fatal(err)
	}
	return claim
}
func TestReviewOwnerStopEndsSeatsAfterVerifiedCompletion(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	claim := reviewLiveSeat(t, f, 61)
	id := fmt.Sprintf("seat_%032x", 61)
	c, err := f.store.GetComputer(t.Context(), f.computer.ComputerID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.SetComputerDesiredState(t.Context(), c.ComputerID, ComputerDesiredStateRequest{ComputerMutationPrecondition: computerPrecondition(c, "test"), DesiredState: contract.ServiceDesiredStopped}); err != nil {
		t.Fatal(err)
	}
	f.state(t, id, SeatEnding)
	before, err := f.store.GetComputerRoomSnapshot(t.Context(), f.node.NodeID, DefaultComputerRoomConfig(2<<30))
	if err != nil || before.HeldMemoryBytes < 1<<30 {
		t.Fatalf("stop intent released room: %+v %v", before, err)
	}
	exit := 0
	if _, err := f.store.CompleteAttempt(t.Context(), "fabric-computer-node", claim.Job.JobID, claim.Lease.AttemptID, CompletionRequest{FencingToken: claim.Lease.FencingToken, IdempotencyKey: "stopped-cleaned", RuntimeQuiescenceEvidence: RuntimeQuiescenceComputerAttemptAbsent, Result: ProcessResult{ExitCode: &exit}}); err != nil {
		t.Fatal(err)
	}
	v := f.state(t, id, SeatEnded)
	if !v.CleanupComplete {
		t.Fatal("verified completion did not clean Seat")
	}
	after, err := f.store.GetComputerRoomSnapshot(t.Context(), f.node.NodeID, DefaultComputerRoomConfig(2<<30))
	if err != nil || after.HeldMemoryBytes != 0 {
		t.Fatalf("stopped room leak: %+v %v", after, err)
	}
}
func TestReviewVerifiedComputerRemovalReturnsRoom(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	reviewLiveSeat(t, f, 62)
	id := fmt.Sprintf("seat_%032x", 62)
	c, err := f.store.GetComputer(t.Context(), f.computer.ComputerID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.RemoveComputer(t.Context(), c.ComputerID, ComputerRemoveRequest{ComputerMutationPrecondition: computerPrecondition(c, "test")}); err != nil {
		t.Fatal(err)
	}
	f.state(t, id, SeatEnding)
	before, err := f.store.GetComputerRoomSnapshot(t.Context(), f.node.NodeID, DefaultComputerRoomConfig(2<<30))
	if err != nil || before.HeldMemoryBytes < 1<<30 {
		t.Fatalf("remove intent released room: %+v %v", before, err)
	}
	directives, err := f.store.ListNodeRemovalDirectives(t.Context(), "fabric-computer-node", f.node.NodeID, f.node.BootSessionID)
	if err != nil || len(directives) != 1 {
		t.Fatalf("directives: %+v %v", directives, err)
	}
	d := directives[0]
	ack := RemovalAcknowledgementRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, RemovalGeneration: d.RemovalGeneration, CleanupFence: d.CleanupFence, RootInstanceID: d.RootInstanceID, IdempotencyKey: "removed-cleaned"}
	if _, err := f.store.AcknowledgeServiceRemoval(t.Context(), "fabric-computer-node", c.CurrentJobID, ack); err != nil {
		t.Fatal(err)
	}
	if _, _, err := f.store.FinalizeServiceRemoval(t.Context(), c.CurrentJobID); err != nil {
		t.Fatal(err)
	}
	v := f.state(t, id, SeatEnded)
	if !v.CleanupComplete {
		t.Fatal("verified removal did not clean Seat")
	}
	after, err := f.store.GetComputerRoomSnapshot(t.Context(), f.node.NodeID, DefaultComputerRoomConfig(2<<30))
	if err != nil || after.HeldMemoryBytes != 0 {
		t.Fatalf("removed Computer retained room: %+v %v", after, err)
	}
}

func TestReviewOwnerRestartAndReimageEndSeatsImmediately(t *testing.T) {
	for _, verb := range []string{"restart", "reimage"} {
		t.Run(verb, func(t *testing.T) {
			f := newSeatLifecycleFixture(t)
			reviewLiveSeat(t, f, 63)
			id := fmt.Sprintf("seat_%032x", 63)
			c, err := f.store.GetComputer(t.Context(), f.computer.ComputerID)
			if err != nil {
				t.Fatal(err)
			}
			switch verb {
			case "restart":
				// Existing owner restart policy allows an active resource latch.
				if _, err := f.store.db.Exec(`UPDATE service_jobs SET last_failure=? WHERE job_id=?`, `{"code":"insufficient_disk","message":"allocation failed"}`, c.CurrentJobID); err != nil {
					t.Fatal(err)
				}
				_, _, err = f.store.RestartComputer(t.Context(), c.ComputerID, ComputerRestartRequest{ComputerMutationPrecondition: computerPrecondition(c, "test"), IdempotencyKey: "seat-restart"})
			case "reimage":
				_, err = f.store.ReimageComputer(t.Context(), c.ComputerID, ComputerReimageRequest{ComputerMutationPrecondition: computerPrecondition(c, "test"), Image: reimageTarget('b'), TerminateSessions: true, IdempotencyKey: "seat-reimage"})
			}
			if err != nil {
				t.Fatal(err)
			}
			f.state(t, id, SeatEnding)
			after, err := f.store.GetComputerRoomSnapshot(t.Context(), f.node.NodeID, DefaultComputerRoomConfig(2<<30))
			if err != nil || after.HeldMemoryBytes < 1<<30 {
				t.Fatalf("owner intent released room: %+v %v", after, err)
			}
			wanted, err := readComputerSeatList(t.Context(), f.store.db, c.ComputerID)
			if err != nil || len(wanted.Seats) != 0 {
				t.Fatalf("owner intent kept wanted Seats: %+v %v", wanted, err)
			}
		})
	}
}
