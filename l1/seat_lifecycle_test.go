package l1

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

type seatLifecycleFixture struct {
	store    *Store
	path     string
	clock    *fakeClock
	node     Node
	computer Computer
}

func newSeatLifecycleFixture(t *testing.T) *seatLifecycleFixture {
	t.Helper()
	f := &seatLifecycleFixture{path: filepath.Join(t.TempDir(), "l1.sqlite"), clock: &fakeClock{now: time.Date(2026, 10, 23, 0, 0, 0, 0, time.UTC)}}
	var err error
	f.store, err = OpenStore(f.path, StoreOptions{Clock: f.clock, LeaseDuration: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := f.store.Close(); err != nil {
			t.Error(err)
		}
	})
	f.node, err = f.store.RegisterNode(t.Context(), fabric.Identity{NodeID: "fabric-computer-node"}, contract.NodeRegistration{
		NodeID: "computer-node", BootSessionID: "boot", RootInstanceID: "root", OS: "linux", Architecture: "amd64", AgentVersion: "test",
		Capabilities: map[string]bool{"kind:oci": true, "cgroup_v2": true, "computer": true}, CapabilityRevision: 1, CapabilityObservedAt: f.clock.Now(), MissingCapabilities: []string{},
	}, NodePolicy{Tags: []string{contract.StableNodeTagPrefix + "computer-node"}, MaxServiceSlots: 4}, true)
	if err != nil {
		t.Fatal(err)
	}
	f.computer = f.createComputer(t, "lifecycle", 2)
	return f
}
func (f *seatLifecycleFixture) createComputer(t *testing.T, name string, limit int) Computer {
	t.Helper()
	settings := ComputerSettings{SeatLimit: limit, IdleAction: ComputerIdleDelete, Network: ComputerNetworkNone}
	c, _, err := f.store.CreateComputer(t.Context(), CreateComputerRequest{Name: name, Spec: computerCapabilityJobSpec(name), Actor: "test", SeatRequestSettings: &settings})
	if err != nil {
		t.Fatal(err)
	}
	return c
}
func (f *seatLifecycleFixture) reopen(t *testing.T) {
	t.Helper()
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	var err error
	f.store, err = OpenStore(f.path, StoreOptions{Clock: f.clock, LeaseDuration: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
}
func (f *seatLifecycleFixture) request(t *testing.T, n int, approve bool) string {
	t.Helper()
	id := fmt.Sprintf("seat_%032x", n)
	_, err := f.store.BeginSeatApproval(t.Context(), "fabric-computer-node", SeatApprovalRequest{SeatID: id, NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, Harness: "test", Folder: "/workspace", Form: "any", Network: "none", AnswerWait: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if approve {
		_, err = f.store.AnswerSeatApproval(t.Context(), "fabric-computer-node", id, SeatApprovalAnswer{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, Approved: true})
		if err != nil {
			t.Fatal(err)
		}
	}
	return id
}
func (f *seatLifecycleFixture) reserve(t *testing.T, id string, c Computer, place int, room ComputerRoomConfig) SeatRequest {
	t.Helper()
	value, err := f.store.ReserveSeatRoom(t.Context(), "fabric-computer-node", id, SeatRoomReservationRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, ComputerID: c.ComputerID, Place: place, Room: room})
	if err != nil {
		t.Fatal(err)
	}
	return value
}
func (f *seatLifecycleFixture) state(t *testing.T, id string, want SeatRequestState) SeatRequest {
	t.Helper()
	f.reopen(t)
	value, err := f.store.GetSeatRequest(t.Context(), id)
	if err != nil || value.State != want {
		t.Fatalf("state=%+v err=%v, want %s", value, err, want)
	}
	return value
}
func TestSeatLifecycleDurableTransitionsAndFinalList(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	id := f.request(t, 1, false)
	f.state(t, id, SeatRequested)
	answer := SeatApprovalAnswer{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, Approved: true}
	if _, err := f.store.AnswerSeatApproval(t.Context(), "fabric-computer-node", id, answer); err != nil {
		t.Fatal(err)
	}
	f.state(t, id, SeatApproved)
	room := DefaultComputerRoomConfig(1 << 30)
	value := f.reserve(t, id, f.computer, 1, room)
	if value.TargetMemoryBytes != 1<<30 {
		t.Fatalf("target=%d", value.TargetMemoryBytes)
	}
	f.state(t, id, SeatApproved)
	if _, err := f.store.StartSeatRequest(t.Context(), "fabric-computer-node", id, SeatStartRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, Deadline: f.clock.Now().Add(time.Minute)}); err != nil {
		t.Fatal(err)
	}
	f.state(t, id, SeatStarting)
	claim, err := f.store.ClaimJob(t.Context(), "fabric-computer-node", f.node.NodeID, f.node.BootSessionID, contract.JobClassService)
	if err != nil || claim == nil {
		t.Fatalf("claim=%+v %v", claim, err)
	}
	auth := ComputerSeatAttemptRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, FencingToken: claim.Lease.FencingToken}
	list, err := f.store.GetComputerSeatListForAttempt(t.Context(), "fabric-computer-node", f.computer.ComputerID, claim.Job.JobID, claim.Lease.AttemptID, auth)
	if err != nil || len(list.Seats) != 1 {
		t.Fatalf("list=%+v %v", list, err)
	}
	report := ComputerSeatObservationReport{NodeID: auth.NodeID, BootSessionID: auth.BootSessionID, FencingToken: auth.FencingToken, ListRevision: list.Revision, Generation: 1, Sequence: 1, Seats: []ComputerSeatObservation{{SeatID: id, Place: 1, State: ComputerSeatReady}}}
	if _, err := f.store.ReportComputerSeatObservations(t.Context(), "fabric-computer-node", f.computer.ComputerID, claim.Job.JobID, claim.Lease.AttemptID, report); err != nil {
		t.Fatal(err)
	}
	f.state(t, id, SeatLive)
	end := SeatEndRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, Reason: "released"}
	for range 2 {
		if _, err := f.store.EndSeatRequest(t.Context(), "fabric-computer-node", id, end); err != nil {
			t.Fatal(err)
		}
	}
	f.state(t, id, SeatEnding)
	list, err = f.store.GetComputerSeatListForAttempt(t.Context(), "fabric-computer-node", f.computer.ComputerID, claim.Job.JobID, claim.Lease.AttemptID, auth)
	if err != nil || len(list.Seats) != 0 {
		t.Fatalf("ended Seat returned: %+v %v", list, err)
	}
	report.ListRevision, report.Sequence, report.Generation = list.Revision, 2, 2
	report.Seats[0].State, report.Seats[0].CleanupComplete = ComputerSeatCleaned, true
	if _, err := f.store.ReportComputerSeatObservations(t.Context(), "fabric-computer-node", f.computer.ComputerID, claim.Job.JobID, claim.Lease.AttemptID, report); err != nil {
		t.Fatal(err)
	}
	f.state(t, id, SeatEnded)
	if _, err := f.store.StartSeatRequest(t.Context(), "fabric-computer-node", id, SeatStartRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, Deadline: f.clock.Now().Add(time.Minute)}); errorCode(err) != contract.ErrorConflict {
		t.Fatalf("ended Seat restarted: %v", err)
	}
	if _, err := f.store.SetComputerWantedSeats(t.Context(), f.computer.ComputerID, list.Revision, []WantedComputerSeat{{SeatID: id, Place: 1}}); errorCode(err) != contract.ErrorConflict {
		t.Fatalf("ended Seat reapplied: %v", err)
	}
}

// RC-2: both calls use separate SQLite transactions, not a caller-side lock.
func TestSeatRoomLastShareRaceHasOneWinner(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	one, two := f.request(t, 1, true), f.request(t, 2, true)
	room := DefaultComputerRoomConfig(1 << 30)
	type result struct {
		value SeatRequest
		err   error
	}
	results := make(chan result, 2)
	start := make(chan struct{})
	for i, id := range []string{one, two} {
		go func(id string, place int) {
			<-start
			v, e := f.store.ReserveSeatRoom(t.Context(), "fabric-computer-node", id, SeatRoomReservationRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, ComputerID: f.computer.ComputerID, Place: place, Room: room})
			results <- result{v, e}
		}(id, i+1)
	}
	close(start)
	counts := map[SeatRequestState]int{}
	for range 2 {
		r := <-results
		if r.err != nil {
			t.Fatal(r.err)
		}
		counts[r.value.State]++
		if r.value.State == SeatRefused && r.value.Reason != "out of room, try again later" {
			t.Fatalf("refusal=%+v", r.value)
		}
	}
	if counts[SeatApproved] != 1 || counts[SeatRefused] != 1 {
		t.Fatalf("race=%v", counts)
	}
	f.reopen(t)
	for _, id := range []string{one, two} {
		v, err := f.store.GetSeatRequest(t.Context(), id)
		if err != nil {
			t.Fatal(err)
		}
		if v.State == SeatRefused { // A stale approval answer cannot revive the loser.
			if _, err := f.store.AnswerSeatApproval(t.Context(), "fabric-computer-node", id, SeatApprovalAnswer{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, Approved: true}); err != nil {
				t.Fatal(err)
			}
			f.state(t, id, SeatRefused)
		}
	}
}

func TestSeatLifecycleListCannotBypassApprovalOrReservation(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	id := f.request(t, 1, false)
	if _, err := f.store.SetComputerWantedSeats(t.Context(), f.computer.ComputerID, 0, []WantedComputerSeat{{SeatID: id, Place: 1}}); errorCode(err) != contract.ErrorConflict {
		t.Fatalf("unapproved Seat published: %v", err)
	}
	if _, err := f.store.AnswerSeatApproval(t.Context(), "fabric-computer-node", id, SeatApprovalAnswer{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, Approved: true}); err != nil {
		t.Fatal(err)
	}
	f.reserve(t, id, f.computer, 1, DefaultComputerRoomConfig(1<<30))
	if _, err := f.store.StartSeatRequest(t.Context(), "fabric-computer-node", id, SeatStartRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, Deadline: f.clock.Now().Add(time.Minute)}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.SetComputerWantedSeats(t.Context(), f.computer.ComputerID, 1, nil); errorCode(err) != contract.ErrorConflict {
		t.Fatalf("lifecycle removal bypassed EndSeatRequest: %v", err)
	}

}

func TestSeatLifecycleCleanupFailureKeepsRoomUntilCleanupAndShrink(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	id := f.request(t, 1, true)
	room := DefaultComputerRoomConfig(2 << 30)
	f.reserve(t, id, f.computer, 1, room)
	other := f.request(t, 2, true)
	f.reserve(t, other, f.computer, 2, room)
	if _, err := f.store.StartSeatRequest(t.Context(), "fabric-computer-node", other, SeatStartRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, Deadline: f.clock.Now().Add(time.Minute)}); err != nil {
		t.Fatal(err)
	}

	if _, err := f.store.StartSeatRequest(t.Context(), "fabric-computer-node", id, SeatStartRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, Deadline: f.clock.Now().Add(time.Minute)}); err != nil {
		t.Fatal(err)
	}
	claim, err := f.store.ClaimJob(t.Context(), "fabric-computer-node", f.node.NodeID, f.node.BootSessionID, contract.JobClassService)
	if err != nil || claim == nil {
		t.Fatalf("claim %v %v", claim, err)
	}
	auth := ComputerSeatAttemptRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, FencingToken: claim.Lease.FencingToken}
	if _, err := f.store.GetComputerSeatListForAttempt(t.Context(), "fabric-computer-node", f.computer.ComputerID, claim.Job.JobID, claim.Lease.AttemptID, auth); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.EndSeatRequest(t.Context(), "fabric-computer-node", id, SeatEndRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, Reason: "released"}); err != nil {
		t.Fatal(err)
	}
	f.state(t, id, SeatEnding)
	list, err := f.store.GetComputerSeatListForAttempt(t.Context(), "fabric-computer-node", f.computer.ComputerID, claim.Job.JobID, claim.Lease.AttemptID, auth)
	if err != nil {
		t.Fatal(err)
	}
	report := ComputerSeatObservationReport{NodeID: auth.NodeID, BootSessionID: auth.BootSessionID, FencingToken: auth.FencingToken, ListRevision: list.Revision, Generation: 2, Sequence: 1, Seats: []ComputerSeatObservation{{SeatID: id, Place: 1, State: ComputerSeatFailed, Reason: "no space left on device"}}}
	if _, err := f.store.ReportComputerSeatObservations(t.Context(), "fabric-computer-node", f.computer.ComputerID, claim.Job.JobID, claim.Lease.AttemptID, report); err != nil {
		t.Fatal(err)
	}
	f.state(t, id, SeatEnding)
	snapshot, err := f.store.GetComputerRoomSnapshot(t.Context(), f.node.NodeID, room)
	if err != nil || snapshot.HeldMemoryBytes != 2<<30 || snapshot.AvailableSeats != 0 {
		t.Fatalf("failed cleanup freed room: %+v %v", snapshot, err)
	}
	memory := SeatRoomMemoryReport{ComputerSeatAttemptRequest: auth, RequestedMemoryBytes: 1 << 30, MemoryLimitBytes: 1 << 30, HeldMemoryBytes: 1 << 30}
	if _, err := f.store.ReportComputerRoomMemory(t.Context(), "fabric-computer-node", f.computer.ComputerID, claim.Job.JobID, claim.Lease.AttemptID, memory); errorCode(err) != contract.ErrorConflict {
		t.Fatalf("shrink without cleanup accepted: %v", err)
	}
	report.Sequence++
	report.Seats[0] = ComputerSeatObservation{SeatID: id, Place: 1, State: ComputerSeatCleaned, CleanupComplete: true}
	if _, err := f.store.ReportComputerSeatObservations(t.Context(), "fabric-computer-node", f.computer.ComputerID, claim.Job.JobID, claim.Lease.AttemptID, report); err != nil {
		t.Fatal(err)
	}
	f.state(t, id, SeatEnded)
	snapshot, err = f.store.GetComputerRoomSnapshot(t.Context(), f.node.NodeID, room)
	if err != nil || snapshot.HeldMemoryBytes != 2<<30 {
		t.Fatalf("cleanup without shrink freed room: %+v %v", snapshot, err)
	}
	value := f.state(t, id, SeatEnded)
	memory.RoomRevision = value.RoomRevision - 1
	if _, err := f.store.ReportComputerRoomMemory(t.Context(), "fabric-computer-node", f.computer.ComputerID, claim.Job.JobID, claim.Lease.AttemptID, memory); errorCode(err) != contract.ErrorConflict {
		t.Fatalf("stale memory reservation receipt accepted: %v", err)
	}
	memory.RoomRevision = value.RoomRevision
	if _, err := f.store.ReportComputerRoomMemory(t.Context(), "fabric-computer-node", f.computer.ComputerID, claim.Job.JobID, claim.Lease.AttemptID, memory); err != nil {
		t.Fatal(err)
	}
	snapshot, err = f.store.GetComputerRoomSnapshot(t.Context(), f.node.NodeID, room)
	if err != nil || snapshot.HeldMemoryBytes != 1<<30 || snapshot.AvailableSeats != 1 {
		t.Fatalf("confirmed shrink did not free room: %+v %v", snapshot, err)
	}
}

func TestSeatLifecycleDeadlineRemovesWantedSeatBeforeLateReady(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	id := f.request(t, 1, true)
	f.reserve(t, id, f.computer, 1, DefaultComputerRoomConfig(1<<30))
	if _, err := f.store.StartSeatRequest(t.Context(), "fabric-computer-node", id, SeatStartRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, Deadline: f.clock.Now().Add(time.Minute)}); err != nil {
		t.Fatal(err)
	}
	claim, err := f.store.ClaimJob(t.Context(), "fabric-computer-node", f.node.NodeID, f.node.BootSessionID, contract.JobClassService)
	if err != nil || claim == nil {
		t.Fatalf("claim %v %v", claim, err)
	}
	auth := ComputerSeatAttemptRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, FencingToken: claim.Lease.FencingToken}
	list, err := f.store.GetComputerSeatListForAttempt(t.Context(), "fabric-computer-node", f.computer.ComputerID, claim.Job.JobID, claim.Lease.AttemptID, auth)
	if err != nil || len(list.Seats) != 1 {
		t.Fatalf("list=%+v %v", list, err)
	}
	f.clock.Advance(60 * time.Second)
	f.reopen(t)
	list, err = f.store.GetComputerSeatListForAttempt(t.Context(), "fabric-computer-node", f.computer.ComputerID, claim.Job.JobID, claim.Lease.AttemptID, auth)
	if err != nil || len(list.Seats) != 0 {
		t.Fatalf("late Seat still wanted: %+v %v", list, err)
	}
	value := f.state(t, id, SeatRefused)
	if value.Reason != "Seat readiness deadline exceeded" {
		t.Fatalf("reason=%q", value.Reason)
	}
	report := ComputerSeatObservationReport{NodeID: auth.NodeID, BootSessionID: auth.BootSessionID, FencingToken: auth.FencingToken, ListRevision: list.Revision, Generation: 1, Sequence: 1, Seats: []ComputerSeatObservation{{SeatID: id, Place: 1, State: ComputerSeatReady}}}
	if _, err := f.store.ReportComputerSeatObservations(t.Context(), "fabric-computer-node", f.computer.ComputerID, claim.Job.JobID, claim.Lease.AttemptID, report); errorCode(err) != contract.ErrorConflict {
		t.Fatalf("late ready accepted: %v", err)
	}
	report.Generation, report.Sequence = 2, 2
	report.Seats[0] = ComputerSeatObservation{SeatID: id, Place: 1, State: ComputerSeatCleaned, CleanupComplete: true}
	if _, err := f.store.ReportComputerSeatObservations(t.Context(), "fabric-computer-node", f.computer.ComputerID, claim.Job.JobID, claim.Lease.AttemptID, report); err != nil {
		t.Fatal(err)
	}
	value = f.state(t, id, SeatRefused)
	if !value.CleanupComplete {
		t.Fatal("late Seat cleanup was not retained")
	}
}

func TestSeatLifecycleNewNodeBootRefusesLostDialog(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	id := f.request(t, 1, false)
	f.reopen(t)
	_, err := f.store.RegisterNode(t.Context(), fabric.Identity{NodeID: "fabric-computer-node"}, contract.NodeRegistration{NodeID: f.node.NodeID, BootSessionID: "next-boot", RootInstanceID: "root", OS: "linux", Architecture: "amd64", AgentVersion: "test", Capabilities: map[string]bool{"kind:oci": true, "cgroup_v2": true, "computer": true}, CapabilityRevision: 2, CapabilityObservedAt: f.clock.Now(), MissingCapabilities: []string{}}, NodePolicy{Tags: []string{contract.StableNodeTagPrefix + "computer-node"}, MaxServiceSlots: 4}, true)
	if err != nil {
		t.Fatal(err)
	}
	value := f.state(t, id, SeatRefused)
	if value.Reason != "approval dialog lost" || value.ComputerID != "" || value.TargetMemoryBytes != 0 {
		t.Fatalf("lost approval=%+v", value)
	}
}

func TestSeatLifecycleStartRetriesDoNotExtendRequestDeadline(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	id := f.request(t, 1, true)
	f.reserve(t, id, f.computer, 1, DefaultComputerRoomConfig(1<<30))
	request := SeatStartRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, Deadline: f.clock.Now().Add(3 * time.Minute)}
	v, err := f.store.StartSeatRequest(t.Context(), "fabric-computer-node", id, request)
	if err != nil || !v.Deadline.Equal(request.Deadline) {
		t.Fatalf("start=%+v %v", v, err)
	}
	f.reopen(t)
	if _, err := f.store.StartSeatRequest(t.Context(), "fabric-computer-node", id, request); err != nil {
		t.Fatal(err)
	}
	request.Deadline = request.Deadline.Add(time.Minute)
	if _, err := f.store.StartSeatRequest(t.Context(), "fabric-computer-node", id, request); errorCode(err) != contract.ErrorConflict {
		t.Fatalf("request deadline extended by retry: %v", err)
	}
}
