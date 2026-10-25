package l1

import (
	"fmt"
	"testing"
	"time"
)

func reopenSeatLife(t *testing.T, f *seatLifecycleFixture, options StoreOptions) {
	t.Helper()
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	if options.Clock == nil {
		options.Clock = f.clock
	}
	options.LeaseDuration = time.Hour
	var err error
	f.store, err = OpenStore(f.path, options)
	if err != nil {
		t.Fatal(err)
	}
}
func liveSeatWithCredential(t *testing.T, f *seatLifecycleFixture) (string, string) {
	t.Helper()
	id := "seat_00000000000000000000000000000001"
	if _, err := f.store.CreateSeatCall(t.Context(), "cli", SeatCallInput{SeatID: id, NodeID: f.node.NodeID, Harness: "test", Folder: "/workspace", Form: "any", Network: "none"}); err != nil {
		t.Fatal(err)
	}
	prepareSeatLogLive(t, f, id)
	call, err := f.store.GetSeatCall(t.Context(), "cli", id)
	if err != nil || call.Credential == "" {
		t.Fatalf("credential=%+v %v", call, err)
	}
	return id, call.Credential
}
func seatWarnings(t *testing.T, f *seatLifecycleFixture, id string) int {
	t.Helper()
	page, err := f.store.GetSeatLogs(t.Context(), "cli", id, "", "", 100)
	if err != nil {
		t.Fatal(err)
	}
	warnings := 0
	for _, event := range page.Events {
		if event.Kind == SeatEventEndsSoon {
			warnings++
		}
	}
	return warnings
}

func TestSeatIdleAdmissionRaceBeforeAndAfterRemovalStarts(t *testing.T) {
	for _, arrival := range []string{"before", "after"} {
		t.Run(arrival, func(t *testing.T) {
			f := newSeatLifecycleFixture(t)
			id, secret := liveSeatWithCredential(t, f)
			if _, err := f.store.ReleaseSeat(t.Context(), id, secret); err != nil {
				t.Fatal(err)
			}
			f.clock.Advance(time.Second)
			other := f.createComputer(t, "other", 2)
			f.clock.Advance(10*time.Minute - time.Second)
			arrived := false
			var allocated SeatRequest
			f.store.reconfigurationHook = func(point string) error {
				want := "computer_idle_selected"
				if arrival == "after" {
					want = "computer_idle_started"
				}
				if point != want || arrived {
					return nil
				}
				arrived = true
				next := f.request(t, 2, true)
				var err error
				allocated, err = f.store.AdvanceSeatCall(t.Context(), "fabric-computer-node", next, SeatCallPoll{SeatNodeRequest: SeatNodeRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID}, Room: DefaultComputerRoomConfig(8 << 30), HostEnabled: true})
				return err
			}
			if err := f.store.CheckSeatLife(t.Context()); err != nil {
				t.Fatal(err)
			}
			if !arrived {
				t.Fatal("idle action did not expose its admission fence")
			}
			current, err := f.store.GetComputer(t.Context(), f.computer.ComputerID)
			if err != nil {
				t.Fatal(err)
			}
			if arrival == "before" {
				if allocated.ComputerID != current.ComputerID || current.DesiredState != "running" {
					t.Fatalf("earlier Seat lost Computer: allocated=%+v desired=%s", allocated, current.DesiredState)
				}
			} else {
				if allocated.ComputerID != other.ComputerID || current.DesiredState != "removed" || current.RemovalOutcome == "removed_verified" {
					t.Fatalf("later Seat reused removing Computer or skipped proof: allocated=%+v desired=%s outcome=%s", allocated, current.DesiredState, current.RemovalOutcome)
				}
			}
			if allocated.State != SeatStarting {
				t.Fatalf("new Seat did not start: %+v", allocated)
			}
		})
	}
}

func TestSeatIdleChangedWaitAppliesAtNextCheck(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	_, secret := liveSeatWithCredential(t, f)
	if _, err := f.store.ReleaseSeat(t.Context(), "seat_00000000000000000000000000000001", secret); err != nil {
		t.Fatal(err)
	}
	f.clock.Advance(10 * time.Minute)
	reopenSeatLife(t, f, StoreOptions{ComputerIdleWait: 20 * time.Minute})
	if err := f.store.CheckSeatLife(t.Context()); err != nil {
		t.Fatal(err)
	}
	c, err := f.store.GetComputer(t.Context(), f.computer.ComputerID)
	if err != nil || c.DesiredState != "running" {
		t.Fatalf("longer setting ignored: %+v %v", c, err)
	}
	reopenSeatLife(t, f, StoreOptions{ComputerIdleWait: 9 * time.Minute})
	if err := f.store.CheckSeatLife(t.Context()); err != nil {
		t.Fatal(err)
	}
	c, err = f.store.GetComputer(t.Context(), f.computer.ComputerID)
	if err != nil || c.DesiredState != "removed" {
		t.Fatalf("shorter setting restarted timer: %+v %v", c, err)
	}
}

func TestSeatLifeClockRecoveryAcrossL1RestartAndNewBoot(t *testing.T) {
	for _, scenario := range []struct{ newBoot, held bool }{{false, false}, {true, false}, {true, true}} {
		newBoot := scenario.newBoot
		t.Run(fmt.Sprint("new_boot=", newBoot, "_held=", scenario.held), func(t *testing.T) {
			f := newSeatLifecycleFixture(t)
			clock := &seatBootTestClock{fakeClock: f.clock, boot: "boot-a", elapsed: time.Hour}
			f.store.clock = clock
			id, _ := liveSeatWithCredential(t, f)
			if err := f.store.SeatCommandRunning(t.Context(), id, "long-command", true); err != nil {
				t.Fatal(err)
			}
			if !scenario.held {
				if err := f.store.SeatCommandRunning(t.Context(), id, "long-command", false); err != nil {
					t.Fatal(err)
				}
			}
			clock.elapsed += 20 * time.Minute
			f.clock.Advance(24 * time.Hour)
			if newBoot {
				clock.boot = "boot-b"
				clock.elapsed = time.Hour + 3*time.Minute
			}
			reopenSeatLife(t, f, StoreOptions{Clock: clock})
			if err := f.store.CheckSeatLife(t.Context()); err != nil {
				t.Fatal(err)
			}
			value, err := f.store.GetSeatRequest(t.Context(), id)
			if err != nil {
				t.Fatal(err)
			}
			if newBoot {
				if value.State != SeatLive || value.Reason != "" || value.CleanupComplete {
					t.Fatalf("L1 reboot falsely ended remote Computer Seat: %+v", value)
				}
				if scenario.held {
					if err := f.store.SeatCommandRunning(t.Context(), id, "long-command", false); err != nil {
						t.Fatal(err)
					}
				}
				clock.elapsed += 29 * time.Minute
				if err := f.store.CheckSeatLife(t.Context()); err != nil {
					t.Fatal(err)
				}
				if value, err = f.store.GetSeatRequest(t.Context(), id); err != nil || value.State != SeatLive {
					t.Fatalf("reboot shortened fresh wait: %+v %v", value, err)
				}
				clock.elapsed += time.Minute
				if err := f.store.CheckSeatLife(t.Context()); err != nil {
					t.Fatal(err)
				}
				if value, err = f.store.GetSeatRequest(t.Context(), id); err != nil || value.Reason != "expired" {
					t.Fatalf("reboot wait is unbounded: %+v %v", value, err)
				}
				return
			}

			if value.State != SeatLive {
				t.Fatalf("L1 restart/calendar change ended Seat: %+v", value)
			}
			clock.elapsed += 5 * time.Minute
			if err := f.store.CheckSeatLife(t.Context()); err != nil {
				t.Fatal(err)
			}
			if seatWarnings(t, f, id) != 1 {
				t.Fatal("warning lost after L1 restart")
			}
			reopenSeatLife(t, f, StoreOptions{Clock: clock})
			if err := f.store.CheckSeatLife(t.Context()); err != nil {
				t.Fatal(err)
			}
			if seatWarnings(t, f, id) != 1 {
				t.Fatal("L1 restart duplicated warning")
			}
			clock.elapsed += 5 * time.Minute
			if err := f.store.CheckSeatLife(t.Context()); err != nil {
				t.Fatal(err)
			}
			value, err = f.store.GetSeatRequest(t.Context(), id)
			if err != nil || value.State != SeatEnding || value.Reason != "expired" {
				t.Fatalf("L1 restart reset deadline: %+v %v", value, err)
			}
		})
	}
}

func TestSeatApprovalClockRecoveryAcrossL1RestartAndNewBoot(t *testing.T) {
	for _, newBoot := range []bool{false, true} {
		t.Run(fmt.Sprint("new_boot=", newBoot), func(t *testing.T) {
			f := newSeatLifecycleFixture(t)
			clock := &seatBootTestClock{fakeClock: f.clock, boot: "boot-a", elapsed: time.Hour}
			f.store.clock = clock
			base := SeatApprovalRequest{SeatID: "seat_00000000000000000000000000000002", NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, Harness: "test", Folder: "/workspace", Form: "any", Network: "none", AnswerWait: 5 * time.Minute, WindowHours: 1}
			if _, err := f.store.BeginSeatApproval(t.Context(), "fabric-computer-node", base); err != nil {
				t.Fatal(err)
			}
			if _, err := f.store.AnswerSeatApproval(t.Context(), "fabric-computer-node", base.SeatID, SeatApprovalAnswer{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, Approved: true}); err != nil {
				t.Fatal(err)
			}
			base.SeatID = "seat_00000000000000000000000000000003"
			if _, err := f.store.BeginSeatApproval(t.Context(), "fabric-computer-node", base); err != nil {
				t.Fatal(err)
			}
			clock.elapsed += 2 * time.Minute
			f.clock.Advance(-24 * time.Hour)
			if newBoot {
				clock.boot = "boot-b"
				clock.elapsed = time.Hour + 3*time.Minute
			}
			reopenSeatLife(t, f, StoreOptions{Clock: clock})
			windows, err := f.store.ListSeatApprovalWindows(t.Context(), f.node.NodeID)
			if err != nil {
				t.Fatal(err)
			}
			pending, err := f.store.BeginSeatApproval(t.Context(), "fabric-computer-node", base)
			if err != nil {
				t.Fatal(err)
			}
			if newBoot {
				if len(windows) != 0 || pending.State != "requested" || pending.RemainingWait != 5*time.Minute {
					t.Fatalf("L1 reboot retained a window or lost its bounded pending answer: windows=%+v pending=%+v", windows, pending)
				}
				clock.elapsed += 5 * time.Minute
				pending, err = f.store.BeginSeatApproval(t.Context(), "fabric-computer-node", base)
				if err != nil || pending.State != "refused" {
					t.Fatalf("reboot pending wait unbounded: %+v %v", pending, err)
				}
				clock.elapsed += 55 * time.Minute
				windows, err = f.store.ListSeatApprovalWindows(t.Context(), f.node.NodeID)
				if err != nil || len(windows) != 0 {
					t.Fatalf("reboot window unbounded: %+v %v", windows, err)
				}
				return
			}

			if len(windows) != 1 || pending.State != "requested" || pending.RemainingWait != 3*time.Minute {
				t.Fatalf("L1 restart reset approval clocks: windows=%+v pending=%+v", windows, pending)
			}
			clock.elapsed += 3 * time.Minute
			pending, err = f.store.BeginSeatApproval(t.Context(), "fabric-computer-node", base)
			if err != nil || pending.State != "refused" {
				t.Fatalf("approval wait renewed by restart: %+v %v", pending, err)
			}
			clock.elapsed += 55 * time.Minute
			windows, err = f.store.ListSeatApprovalWindows(t.Context(), f.node.NodeID)
			if err != nil || len(windows) != 0 {
				t.Fatalf("approval window renewed by restart: %+v %v", windows, err)
			}
		})
	}
}

func TestSeatWarningCallRestartsThirtyMinutesAndWarnsOnceAgain(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	id, secret := liveSeatWithCredential(t, f)
	f.clock.Advance(25 * time.Minute)
	for range 2 {
		if err := f.store.CheckSeatLife(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	if got := seatWarnings(t, f, id); got != 1 {
		t.Fatalf("first warning count=%d", got)
	}
	f.clock.Advance(time.Minute)
	if err := f.store.SeatCallHappened(t.Context(), id, secret); err != nil {
		t.Fatal(err)
	}
	f.clock.Advance(4 * time.Minute)
	if err := f.store.CheckSeatLife(t.Context()); err != nil {
		t.Fatal(err)
	}
	seat, err := f.store.GetSeatRequest(t.Context(), id)
	if err != nil || seat.State != SeatLive {
		t.Fatalf("Seat call did not restart wait: %+v %v", seat, err)
	}
	f.clock.Advance(21 * time.Minute)
	for range 2 {
		if err := f.store.CheckSeatLife(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	if got := seatWarnings(t, f, id); got != 2 {
		t.Fatalf("renewed period warning count=%d", got)
	}
	f.clock.Advance(5 * time.Minute)
	if err := f.store.CheckSeatLife(t.Context()); err != nil {
		t.Fatal(err)
	}
	seat, err = f.store.GetSeatRequest(t.Context(), id)
	if err != nil || seat.State != SeatEnding || seat.Reason != "expired" {
		t.Fatalf("renewed deadline=%+v %v", seat, err)
	}
}
