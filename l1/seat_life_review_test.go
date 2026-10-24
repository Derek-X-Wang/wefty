package l1

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
)

func reviewIdleComputer(t *testing.T, f *seatLifecycleFixture, n int) {
	t.Helper()
	id := fmt.Sprintf("seat_%032x", n)
	if _, err := f.store.CreateSeatCall(t.Context(), "cli", SeatCallInput{SeatID: id, NodeID: f.node.NodeID, Harness: "test", Folder: "/workspace", Form: "any", Network: "none"}); err != nil {
		t.Fatal(err)
	}
	f.request(t, n, true)
	f.reserve(t, id, f.computer, 1, DefaultComputerRoomConfig(1<<30))
	if _, err := f.store.EndSeatRequest(t.Context(), "fabric-computer-node", id, SeatEndRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, Reason: "released"}); err != nil {
		t.Fatal(err)
	}
}

func TestSeatIdleOwnerMutationDoesNotFailReconcile(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	reviewIdleComputer(t, f, 1)
	f.clock.Advance(10 * time.Minute)
	changed := false
	f.store.reconfigurationHook = func(point string) error {
		if point != "computer_idle_selected" || changed {
			return nil
		}
		changed = true
		c, err := f.store.GetComputer(t.Context(), f.computer.ComputerID)
		if err != nil {
			return err
		}
		_, err = f.store.SetComputerBackupCap(t.Context(), c.ComputerID, ComputerBackupCapRequest{ComputerMutationPrecondition: computerPrecondition(c, "owner"), BackupCap: 1})
		return err
	}
	if _, err := f.store.Reconcile(t.Context()); err != nil {
		t.Fatalf("owner race failed L1 reconcile: %v", err)
	}
	if !changed {
		t.Fatal("did not race idle removal")
	}
	c, err := f.store.GetComputer(t.Context(), f.computer.ComputerID)
	if err != nil || c.DesiredState != "running" {
		t.Fatalf("owner lost: %+v %v", c, err)
	}
}

func TestSeatIdleBrokenRemovalDoesNotBlockOtherComputer(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	reviewIdleComputer(t, f, 1)
	other := *f
	other.computer = f.createComputer(t, "other", 2)
	reviewIdleComputer(t, &other, 2)
	if f.computer.ComputerID > other.computer.ComputerID {
		f.computer, other.computer = other.computer, f.computer
	}
	// A durable removal failure for one Computer must not roll back its neighbor.
	if _, err := f.store.db.Exec(`CREATE TRIGGER refuse_one_idle_removal BEFORE UPDATE OF desired_state ON computers WHEN NEW.desired_state='removed' AND OLD.computer_id='` + f.computer.ComputerID + `' BEGIN SELECT RAISE(ABORT,'removal unavailable'); END`); err != nil {
		t.Fatal(err)
	}
	var logs []string
	f.store.logf = func(format string, args ...any) { logs = append(logs, fmt.Sprintf(format, args...)) }
	f.clock.Advance(10 * time.Minute)
	for range 2 {
		if err := f.store.CheckSeatLife(t.Context()); err != nil {
			t.Fatalf("one broken removal aborted pass: %v", err)
		}
	}
	c, err := f.store.GetComputer(t.Context(), other.computer.ComputerID)
	if err != nil || c.DesiredState != "removed" {
		t.Fatalf("healthy neighbor blocked: %+v %v", c, err)
	}
	if len(logs) != 2 || !strings.Contains(logs[0], "code=") || strings.Contains(logs[0], "\n") {
		t.Fatalf("missing single typed retry log: %q", logs)
	}
}

type failedSeatClock struct{ Clock }

func (failedSeatClock) BootTime() (string, time.Duration, error) {
	return "", 0, errors.New("clock unavailable")
}

func TestSeatClockFailureAtStartupDoesNotStopL1(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	liveSeatWithCredential(t, f)
	f.store.clock = failedSeatClock{f.clock}
	network := plain.NewNetwork()
	ff := network.NewFabric(fabric.Identity{NodeID: "control-plane"})
	server, err := NewServer(ff, f.store, ServerConfig{ReconcileInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := ff.Listen("tcp", "wefty://control-plane")
	if err != nil {
		t.Fatal(err)
	}
	ready := &harnessReadyListener{Listener: listener, ready: make(chan struct{})}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx, ready) }()
	select {
	case err := <-done:
		t.Fatalf("Seat clock failure stopped startup: %v", err)
	case <-ready.ready:
	case <-time.After(5 * time.Second):
		listener.Close()
		t.Fatal("L1 did not serve")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("L1 shutdown: %v", err)
	}
}

func TestSeatBootClockBackwardMicrosecondIsZeroElapsed(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	clock := &seatBootTestClock{fakeClock: f.clock, boot: "same-boot", elapsed: time.Hour}
	f.store.clock = clock
	id, _ := liveSeatWithCredential(t, f)
	req := SeatApprovalRequest{SeatID: fmt.Sprintf("seat_%032x", 2), NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, Harness: "test", Folder: "/workspace", Form: "any", Network: "none", AnswerWait: time.Minute, WindowHours: 1}
	if _, err := f.store.BeginSeatApproval(t.Context(), "fabric-computer-node", req); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.AnswerSeatApproval(t.Context(), "fabric-computer-node", req.SeatID, SeatApprovalAnswer{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, Approved: true}); err != nil {
		t.Fatal(err)
	}
	req.SeatID = fmt.Sprintf("seat_%032x", 3)
	req.Network = "internet"
	req.WindowHours = 0
	if _, err := f.store.BeginSeatApproval(t.Context(), "fabric-computer-node", req); err != nil {
		t.Fatal(err)
	}
	clock.elapsed -= time.Microsecond
	if err := f.store.CheckSeatLife(t.Context()); err != nil {
		t.Fatal(err)
	}
	value, err := f.store.GetSeatRequest(t.Context(), id)
	if err != nil || value.State != SeatLive {
		t.Errorf("tiny backstep ended Seat: %+v %v", value, err)
	}
	pending, err := f.store.BeginSeatApproval(t.Context(), "fabric-computer-node", req)
	if err != nil || pending.State != "requested" || pending.RemainingWait != time.Minute {
		t.Errorf("tiny backstep ended approval: %+v %v", pending, err)
	}
	windows, err := f.store.ListSeatApprovalWindows(t.Context(), f.node.NodeID)
	if err != nil || len(windows) != 1 {
		t.Errorf("tiny backstep dropped window: %+v %v", windows, err)
	}
	clock.elapsed = time.Hour + 30*time.Minute
	if err := f.store.CheckSeatLife(t.Context()); err != nil {
		t.Fatal(err)
	}
	value, err = f.store.GetSeatRequest(t.Context(), id)
	if err != nil || value.Reason != "expired" {
		t.Fatalf("backstep reset wait origin: %+v %v", value, err)
	}
}

func TestSeatOwnerBackupCapRestartsIdleWait(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	reviewIdleComputer(t, f, 1)
	f.clock.Advance(10 * time.Minute)
	c, err := f.store.GetComputer(t.Context(), f.computer.ComputerID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.store.SetComputerBackupCap(t.Context(), c.ComputerID, ComputerBackupCapRequest{ComputerMutationPrecondition: computerPrecondition(c, "owner"), BackupCap: 1}); err != nil {
		t.Fatal(err)
	}
	for _, advance := range []time.Duration{0, 9 * time.Minute, time.Minute} {
		f.clock.Advance(advance)
		if err = f.store.CheckSeatLife(t.Context()); err != nil {
			t.Fatal(err)
		}
		c, err = f.store.GetComputer(t.Context(), c.ComputerID)
		if err != nil {
			t.Fatal(err)
		}
		want := "running"
		if advance == time.Minute {
			want = "removed"
		}
		if string(c.DesiredState) != want {
			t.Fatalf("owner idle wait at %s: desired=%s want=%s", f.clock.Now(), c.DesiredState, want)
		}
	}
}

func TestSeatReleaseSameCredentialIsIdempotentAfterCleanup(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	id, secret := liveSeatWithCredential(t, f)
	for _, cleaned := range []bool{false, true} {
		if cleaned {
			var attempt, fence string
			if err := f.store.db.QueryRow(`SELECT attempt_id,fencing_token FROM attempts WHERE job_id=?`, f.computer.CurrentJobID).Scan(&attempt, &fence); err != nil {
				t.Fatal(err)
			}
			auth := ComputerSeatAttemptRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, FencingToken: fence}
			list, err := f.store.GetComputerSeatListForAttempt(t.Context(), "fabric-computer-node", f.computer.ComputerID, f.computer.CurrentJobID, attempt, auth)
			if err != nil {
				t.Fatal(err)
			}
			report := ComputerSeatObservationReport{NodeID: auth.NodeID, BootSessionID: auth.BootSessionID, FencingToken: fence, ListRevision: list.Revision, Generation: 2, Sequence: 2, Seats: []ComputerSeatObservation{{SeatID: id, Place: 1, State: ComputerSeatCleaned, CleanupComplete: true}}}
			if _, err = f.store.ReportComputerSeatObservations(t.Context(), "fabric-computer-node", f.computer.ComputerID, f.computer.CurrentJobID, attempt, report); err != nil {
				t.Fatal(err)
			}
		}
		for range 2 {
			reply, err := f.store.ReleaseSeat(t.Context(), id, secret)
			if err != nil || reply.Reason != "released" {
				t.Fatalf("cleaned=%t release replay=%+v %v", cleaned, reply, err)
			}
			if cleaned && reply.State != SeatEnded {
				t.Fatalf("cleanup did not enter ended: %+v", reply)
			}
		}
	}
	if _, err := f.store.ReleaseSeat(t.Context(), id, "wrong-secret"); errorCode(err) != "forbidden" {
		t.Fatalf("wrong credential replay: %v", err)
	}
}

func TestSeatIdleBusyComputerDoesNotRewriteClearedTimer(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	liveSeatWithCredential(t, f)
	if _, err := f.store.db.Exec(`CREATE TABLE idle_timer_probe(n INTEGER NOT NULL); INSERT INTO idle_timer_probe VALUES(0); CREATE TRIGGER count_idle_timer_writes AFTER UPDATE OF wait_boot_id,wait_started_ns ON computers BEGIN UPDATE idle_timer_probe SET n=n+1; END`); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := f.store.CheckSeatLife(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	var writes int
	if err := f.store.db.QueryRow(`SELECT n FROM idle_timer_probe`).Scan(&writes); err != nil {
		t.Fatal(err)
	}
	if writes != 0 {
		t.Fatalf("busy Computer timer rewritten %d times", writes)
	}
}

func TestSeatBrokenWarningDoesNotBlockAnotherSeat(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	first, _ := liveSeatWithCredential(t, f)
	second := fmt.Sprintf("seat_%032x", 2)
	if _, err := f.store.CreateSeatCall(t.Context(), "cli", SeatCallInput{SeatID: second, NodeID: f.node.NodeID, Harness: "test", Folder: "/workspace", Form: "any", Network: "none"}); err != nil {
		t.Fatal(err)
	}
	f.request(t, 2, true)
	f.reserve(t, second, f.computer, 2, DefaultComputerRoomConfig(2<<30))
	if _, err := f.store.StartSeatRequest(t.Context(), "fabric-computer-node", second, SeatStartRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, ReadyWait: time.Minute}); err != nil {
		t.Fatal(err)
	}
	var attempt, fence string
	if err := f.store.db.QueryRow(`SELECT attempt_id,fencing_token FROM attempts WHERE job_id=?`, f.computer.CurrentJobID).Scan(&attempt, &fence); err != nil {
		t.Fatal(err)
	}
	auth := ComputerSeatAttemptRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, FencingToken: fence}
	list, err := f.store.GetComputerSeatListForAttempt(t.Context(), "fabric-computer-node", f.computer.ComputerID, f.computer.CurrentJobID, attempt, auth)
	if err != nil {
		t.Fatal(err)
	}
	report := ComputerSeatObservationReport{NodeID: auth.NodeID, BootSessionID: auth.BootSessionID, FencingToken: fence, ListRevision: list.Revision, Generation: 2, Sequence: 2, Seats: []ComputerSeatObservation{{SeatID: first, Place: 1, State: ComputerSeatReady}, {SeatID: second, Place: 2, State: ComputerSeatReady}}}
	if _, err = f.store.ReportComputerSeatObservations(t.Context(), "fabric-computer-node", f.computer.ComputerID, f.computer.CurrentJobID, attempt, report); err != nil {
		t.Fatal(err)
	}
	if _, err = f.store.db.Exec(`CREATE TRIGGER refuse_one_warning BEFORE UPDATE OF warning_sent ON seat_approvals WHEN NEW.seat_id='` + first + `' AND NEW.warning_sent=1 BEGIN SELECT RAISE(ABORT,'warning unavailable'); END`); err != nil {
		t.Fatal(err)
	}
	f.clock.Advance(25 * time.Minute)
	if err = f.store.CheckSeatLife(t.Context()); err != nil {
		t.Fatalf("one Seat failed maintenance: %v", err)
	}
	if seatWarnings(t, f, first) != 0 || seatWarnings(t, f, second) != 1 {
		t.Fatal("failed Seat rolled back another Seat's warning")
	}
}

func TestSeatIdleBackwardMicrosecondDoesNotMoveOrigin(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	clock := &seatBootTestClock{fakeClock: f.clock, boot: "same-boot", elapsed: time.Hour}
	f.store.clock = clock
	reviewIdleComputer(t, f, 1)
	clock.elapsed -= time.Microsecond
	if err := f.store.CheckSeatLife(t.Context()); err != nil {
		t.Fatal(err)
	}
	clock.elapsed = time.Hour + 10*time.Minute - time.Nanosecond
	if err := f.store.CheckSeatLife(t.Context()); err != nil {
		t.Fatal(err)
	}
	c, err := f.store.GetComputer(t.Context(), f.computer.ComputerID)
	if err != nil || c.DesiredState != "running" {
		t.Fatalf("backstep moved idle origin early: %+v %v", c, err)
	}
	clock.elapsed += time.Nanosecond
	if err := f.store.CheckSeatLife(t.Context()); err != nil {
		t.Fatal(err)
	}
	c, err = f.store.GetComputer(t.Context(), f.computer.ComputerID)
	if err != nil || c.DesiredState != "removed" {
		t.Fatalf("backstep restarted idle wait: %+v %v", c, err)
	}
}
