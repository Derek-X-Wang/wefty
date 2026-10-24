package l1

import (
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

func TestSeatRecoveryRetainsCleanupBindingAndSequence(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	id := f.request(t, 1, true)
	f.reserve(t, id, f.computer, 1, DefaultComputerRoomConfig(1<<30))
	if _, err := f.store.StartSeatRequest(t.Context(), "fabric-computer-node", id, SeatStartRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, ReadyWait: time.Minute}); err != nil {
		t.Fatal(err)
	}
	claim, err := f.store.ClaimJob(t.Context(), "fabric-computer-node", f.node.NodeID, f.node.BootSessionID, contract.JobClassService)
	if err != nil || claim == nil {
		t.Fatalf("claim %v %v", claim, err)
	}
	auth := ComputerSeatAttemptRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, FencingToken: claim.Lease.FencingToken}
	list, err := f.store.GetComputerSeatListForAttempt(t.Context(), "fabric-computer-node", f.computer.ComputerID, claim.Job.JobID, claim.Lease.AttemptID, auth)
	if err != nil {
		t.Fatal(err)
	}
	report := ComputerSeatObservationReport{NodeID: auth.NodeID, BootSessionID: auth.BootSessionID, FencingToken: auth.FencingToken, ListRevision: list.Revision, Generation: 2, Sequence: 7, Seats: []ComputerSeatObservation{{SeatID: id, Place: 1, State: ComputerSeatReady}}}
	if _, err := f.store.ReportComputerSeatObservations(t.Context(), "fabric-computer-node", f.computer.ComputerID, claim.Job.JobID, claim.Lease.AttemptID, report); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.EndSeatRequest(t.Context(), "fabric-computer-node", id, SeatEndRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, Reason: "released"}); err != nil {
		t.Fatal(err)
	}
	f.reopen(t)
	list, err = f.store.GetComputerSeatListForAttempt(t.Context(), "fabric-computer-node", f.computer.ComputerID, claim.Job.JobID, claim.Lease.AttemptID, auth)
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Seats) != 0 || list.Recovery == nil || len(list.Recovery.Bindings) != 1 || list.Recovery.Bindings[0].SeatID != id || list.Recovery.Sequence != 7 {
		t.Fatalf("lost recovery authority: %+v", list)
	}
	if list.Recovery.Room == nil || list.Recovery.Room.HeldMemoryBytes != 1<<30 || list.Recovery.Room.TargetMemoryBytes <= 0 {
		t.Fatalf("lost true memory target: %+v", list.Recovery)
	}
}

func TestSeatRecoveryOldAttemptRequiresAbsenceAndFreshCleanup(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	id := f.request(t, 1, true)
	f.reserve(t, id, f.computer, 1, DefaultComputerRoomConfig(1<<30))
	if _, err := f.store.StartSeatRequest(t.Context(), "fabric-computer-node", id, SeatStartRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, ReadyWait: time.Minute}); err != nil {
		t.Fatal(err)
	}
	first, err := f.store.ClaimJob(t.Context(), "fabric-computer-node", f.node.NodeID, f.node.BootSessionID, contract.JobClassService)
	if err != nil || first == nil {
		t.Fatalf("claim %v %v", first, err)
	}
	auth := ComputerSeatAttemptRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, FencingToken: first.Lease.FencingToken}
	if _, err := f.store.GetComputerSeatListForAttempt(t.Context(), "fabric-computer-node", f.computer.ComputerID, first.Job.JobID, first.Lease.AttemptID, auth); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.ObserveAttemptImage(t.Context(), "fabric-computer-node", first.Job.JobID, first.Lease.AttemptID, testImageObservation(first.Lease.FencingToken)); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.StartAttempt(t.Context(), "fabric-computer-node", first.Job.JobID, first.Lease.AttemptID, StartedRequest{FencingToken: first.Lease.FencingToken}); err != nil {
		t.Fatal(err)
	}
	exit := 1
	if _, err := f.store.CompleteAttempt(t.Context(), "fabric-computer-node", first.Job.JobID, first.Lease.AttemptID, CompletionRequest{FencingToken: first.Lease.FencingToken, IdempotencyKey: "recovery-restart", Result: ProcessResult{ExitCode: &exit}}); err != nil {
		t.Fatal(err)
	}
	f.clock.Advance(2 * time.Second)
	next, err := f.store.ClaimJob(t.Context(), "fabric-computer-node", f.node.NodeID, f.node.BootSessionID, contract.JobClassService)
	if err != nil || next == nil {
		t.Fatalf("next %v %v", next, err)
	}
	auth.FencingToken = next.Lease.FencingToken
	list, err := f.store.GetComputerSeatListForAttempt(t.Context(), "fabric-computer-node", f.computer.ComputerID, next.Job.JobID, next.Lease.AttemptID, auth)
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Seats) != 0 || len(list.Recovery.RetiredAttempts) != 1 || len(list.Recovery.Bindings) != 0 {
		t.Fatalf("old Attempt was not fenced: %+v", list.Recovery)
	}
	report := ComputerSeatObservationReport{NodeID: auth.NodeID, BootSessionID: auth.BootSessionID, FencingToken: auth.FencingToken, ListRevision: list.Revision, Generation: 1, Sequence: 1, Seats: []ComputerSeatObservation{{SeatID: id, Place: 1, State: ComputerSeatCleaned, CleanupComplete: true}}}
	if _, err := f.store.ReportComputerSeatObservations(t.Context(), "fabric-computer-node", f.computer.ComputerID, next.Job.JobID, next.Lease.AttemptID, report); errorCode(err) != contract.ErrorConflict {
		t.Fatalf("cleanup without old absence accepted: %v", err)
	}
	retired := list.Recovery.RetiredAttempts[0]
	absence := ComputerSeatAttemptAbsenceReport{ComputerSeatAttemptRequest: auth, Retired: retired, Absent: true}
	if _, err := f.store.ReportComputerSeatAttemptAbsence(t.Context(), "fabric-computer-node", f.computer.ComputerID, next.Job.JobID, next.Lease.AttemptID, absence); err != nil {
		t.Fatal(err)
	}
	vAfterAbsence := f.state(t, id, SeatRefused)
	if !vAfterAbsence.CleanupComplete {
		t.Fatal("positive old Attempt absence did not clean its Seat")
	}
	f.reopen(t)
	// Prunable execution history is not the recovery authority.
	if _, err := f.store.db.Exec(`DELETE FROM attempts WHERE attempt_id=?`, first.Lease.AttemptID); err != nil {
		t.Fatal(err)
	}
	list, err = f.store.GetComputerSeatListForAttempt(t.Context(), "fabric-computer-node", f.computer.ComputerID, next.Job.JobID, next.Lease.AttemptID, auth)
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Recovery.RetiredAttempts) != 0 || len(list.Recovery.Bindings) != 1 || list.Recovery.Room.HeldMemoryBytes != 1<<30 {
		t.Fatalf("absence alone returned room or lost cleanup: %+v", list.Recovery)
	}
	if _, err := f.store.ReportComputerSeatObservations(t.Context(), "fabric-computer-node", f.computer.ComputerID, next.Job.JobID, next.Lease.AttemptID, report); err != nil {
		t.Fatal(err)
	}
	v := f.state(t, id, SeatRefused)
	if !v.CleanupComplete {
		t.Fatal("fresh cleanup was not durable")
	}
}

func TestSeatRecoveryCleanupBeforeFirstDelivery(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	id := f.request(t, 1, true)
	f.reserve(t, id, f.computer, 1, DefaultComputerRoomConfig(1<<30))
	if _, err := f.store.StartSeatRequest(t.Context(), "fabric-computer-node", id, SeatStartRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, ReadyWait: time.Minute}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.EndSeatRequest(t.Context(), "fabric-computer-node", id, SeatEndRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, Reason: "released before delivery"}); err != nil {
		t.Fatal(err)
	}
	f.reopen(t)
	claim, err := f.store.ClaimJob(t.Context(), "fabric-computer-node", f.node.NodeID, f.node.BootSessionID, contract.JobClassService)
	if err != nil || claim == nil {
		t.Fatalf("claim: %+v %v", claim, err)
	}
	auth := ComputerSeatAttemptRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, FencingToken: claim.Lease.FencingToken}
	list, err := f.store.GetComputerSeatListForAttempt(t.Context(), "fabric-computer-node", f.computer.ComputerID, claim.Job.JobID, claim.Lease.AttemptID, auth)
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Seats) != 0 || list.Recovery == nil || len(list.Recovery.Bindings) != 1 || list.Recovery.Bindings[0].SeatID != id {
		t.Fatalf("undelivered cleanup authority lost: %+v", list)
	}
	r := ComputerSeatObservationReport{NodeID: auth.NodeID, BootSessionID: auth.BootSessionID, FencingToken: auth.FencingToken, ListRevision: list.Revision, Generation: 1, Sequence: 1, Seats: []ComputerSeatObservation{{SeatID: id, Place: 1, State: ComputerSeatCleaned, CleanupComplete: true}}}
	if _, err := f.store.ReportComputerSeatObservations(t.Context(), "fabric-computer-node", f.computer.ComputerID, claim.Job.JobID, claim.Lease.AttemptID, r); err != nil {
		t.Fatal(err)
	}
	f.state(t, id, SeatRefused)
}

func TestSeatRecoveryApprovedReservationIsNotCleanup(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	id := f.request(t, 1, true)
	f.reserve(t, id, f.computer, 1, DefaultComputerRoomConfig(1<<30))
	claim, err := f.store.ClaimJob(t.Context(), "fabric-computer-node", f.node.NodeID, f.node.BootSessionID, contract.JobClassService)
	if err != nil || claim == nil {
		t.Fatalf("claim: %+v %v", claim, err)
	}
	auth := ComputerSeatAttemptRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, FencingToken: claim.Lease.FencingToken}
	list, err := f.store.GetComputerSeatListForAttempt(t.Context(), "fabric-computer-node", f.computer.ComputerID, claim.Job.JobID, claim.Lease.AttemptID, auth)
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Seats) != 0 || len(list.Recovery.Bindings) != 0 {
		t.Fatalf("approved reservation was offered for cleanup: %+v", list.Recovery)
	}
	f.state(t, id, SeatApproved)
}

func TestSeatRecoveryCleanedAttemptStillBlocksMemoryReturn(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	id := f.request(t, 1, true)
	room := DefaultComputerRoomConfig(2 << 30)
	room.SeatMemoryBytes = 2 << 30
	f.reserve(t, id, f.computer, 1, room)
	if _, err := f.store.StartSeatRequest(t.Context(), "fabric-computer-node", id, SeatStartRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, ReadyWait: time.Minute}); err != nil {
		t.Fatal(err)
	}
	first, err := f.store.ClaimJob(t.Context(), "fabric-computer-node", f.node.NodeID, f.node.BootSessionID, contract.JobClassService)
	if err != nil || first == nil {
		t.Fatalf("claim: %v %v", first, err)
	}
	auth := ComputerSeatAttemptRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, FencingToken: first.Lease.FencingToken}
	if _, err := f.store.GetComputerSeatListForAttempt(t.Context(), "fabric-computer-node", f.computer.ComputerID, first.Job.JobID, first.Lease.AttemptID, auth); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.EndSeatRequest(t.Context(), "fabric-computer-node", id, SeatEndRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, Reason: "released"}); err != nil {
		t.Fatal(err)
	}
	list, err := f.store.GetComputerSeatListForAttempt(t.Context(), "fabric-computer-node", f.computer.ComputerID, first.Job.JobID, first.Lease.AttemptID, auth)
	if err != nil {
		t.Fatal(err)
	}
	cleanup := ComputerSeatObservationReport{NodeID: auth.NodeID, BootSessionID: auth.BootSessionID, FencingToken: auth.FencingToken, ListRevision: list.Revision, Generation: 1, Sequence: 1, Seats: []ComputerSeatObservation{{SeatID: id, Place: 1, State: ComputerSeatCleaned, CleanupComplete: true}}}
	if _, err := f.store.ReportComputerSeatObservations(t.Context(), "fabric-computer-node", f.computer.ComputerID, first.Job.JobID, first.Lease.AttemptID, cleanup); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.ObserveAttemptImage(t.Context(), "fabric-computer-node", first.Job.JobID, first.Lease.AttemptID, testImageObservation(first.Lease.FencingToken)); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.StartAttempt(t.Context(), "fabric-computer-node", first.Job.JobID, first.Lease.AttemptID, StartedRequest{FencingToken: first.Lease.FencingToken}); err != nil {
		t.Fatal(err)
	}
	exit := 1
	if _, err := f.store.CompleteAttempt(t.Context(), "fabric-computer-node", first.Job.JobID, first.Lease.AttemptID, CompletionRequest{FencingToken: first.Lease.FencingToken, IdempotencyKey: "cleaned-restart", Result: ProcessResult{ExitCode: &exit}}); err != nil {
		t.Fatal(err)
	}
	f.clock.Advance(2 * time.Second)
	f.reopen(t)
	next, err := f.store.ClaimJob(t.Context(), "fabric-computer-node", f.node.NodeID, f.node.BootSessionID, contract.JobClassService)
	if err != nil || next == nil {
		t.Fatalf("next: %v %v", next, err)
	}
	auth.FencingToken = next.Lease.FencingToken
	list, err = f.store.GetComputerSeatListForAttempt(t.Context(), "fabric-computer-node", f.computer.ComputerID, next.Job.JobID, next.Lease.AttemptID, auth)
	if err != nil {
		t.Fatal(err)
	}
	memory := SeatRoomMemoryReport{ComputerSeatAttemptRequest: auth, RoomRevision: list.Recovery.Room.RoomRevision, RequestedMemoryBytes: 1 << 30, MemoryLimitBytes: 1 << 30, HeldMemoryBytes: 1 << 30}
	if _, err := f.store.ReportComputerRoomMemory(t.Context(), "fabric-computer-node", f.computer.ComputerID, next.Job.JobID, next.Lease.AttemptID, memory); errorCode(err) != contract.ErrorConflict {
		t.Fatalf("old cleaned cgroup released held memory without absence: %v", err)
	}
	if len(list.Recovery.RetiredAttempts) != 1 || len(list.Recovery.Bindings) != 0 {
		t.Fatalf("old memory charge lost its Attempt proof: %+v", list.Recovery)
	}
	proof := ComputerSeatAttemptAbsenceReport{ComputerSeatAttemptRequest: auth, Retired: list.Recovery.RetiredAttempts[0], Absent: true}
	if _, err := f.store.ReportComputerSeatAttemptAbsence(t.Context(), "fabric-computer-node", f.computer.ComputerID, next.Job.JobID, next.Lease.AttemptID, proof); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.ReportComputerRoomMemory(t.Context(), "fabric-computer-node", f.computer.ComputerID, next.Job.JobID, next.Lease.AttemptID, memory); err != nil {
		t.Fatal(err)
	}
	snapshot, err := f.store.GetComputerRoomSnapshot(t.Context(), f.node.NodeID, room)
	if err != nil || snapshot.HeldMemoryBytes != 1<<30 {
		t.Fatalf("proved memory return: %+v %v", snapshot, err)
	}

	// A cleanup-only replacement owns a cgroup too, even though no Seat was
	// ever first-bound to it. A held readback must retain that exact authority.
	memory.MemoryLimitBytes, memory.HeldMemoryBytes = 3<<30, 3<<30
	if _, err := f.store.ReportComputerRoomMemory(t.Context(), "fabric-computer-node", f.computer.ComputerID, next.Job.JobID, next.Lease.AttemptID, memory); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.ObserveAttemptImage(t.Context(), "fabric-computer-node", next.Job.JobID, next.Lease.AttemptID, testImageObservation(next.Lease.FencingToken)); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.StartAttempt(t.Context(), "fabric-computer-node", next.Job.JobID, next.Lease.AttemptID, StartedRequest{FencingToken: next.Lease.FencingToken}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.CompleteAttempt(t.Context(), "fabric-computer-node", next.Job.JobID, next.Lease.AttemptID, CompletionRequest{FencingToken: next.Lease.FencingToken, IdempotencyKey: "cleanup-only-restart", Result: ProcessResult{ExitCode: &exit}}); err != nil {
		t.Fatal(err)
	}
	f.clock.Advance(10 * time.Second)
	f.reopen(t)
	third, err := f.store.ClaimJob(t.Context(), "fabric-computer-node", f.node.NodeID, f.node.BootSessionID, contract.JobClassService)
	if err != nil || third == nil {
		t.Fatalf("third: %v %v", third, err)
	}
	auth.FencingToken = third.Lease.FencingToken
	list, err = f.store.GetComputerSeatListForAttempt(t.Context(), "fabric-computer-node", f.computer.ComputerID, third.Job.JobID, third.Lease.AttemptID, auth)
	if err != nil {
		t.Fatal(err)
	}
	memory.ComputerSeatAttemptRequest = auth
	memory.MemoryLimitBytes, memory.HeldMemoryBytes = 1<<30, 1<<30
	if _, err := f.store.ReportComputerRoomMemory(t.Context(), "fabric-computer-node", f.computer.ComputerID, third.Job.JobID, third.Lease.AttemptID, memory); errorCode(err) != contract.ErrorConflict {
		t.Fatalf("cleanup-only old cgroup returned room without absence: %v", err)
	}
	if len(list.Recovery.RetiredAttempts) != 1 || list.Recovery.RetiredAttempts[0].AttemptID != next.Lease.AttemptID {
		t.Fatalf("cleanup-only Attempt identity lost: %+v", list.Recovery)
	}
}
