package l1

import (
	"fmt"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

// TestOwnerComputerIdleActionAfterItsSeatsLeave covers BN-3 (#1155): an
// owner-made Computer with idle action stop stops after its idle wait and
// keeps its Storage; delete removes it; nothing leaves it running. A fresh
// Computer that no Seat has used is never idle.
func TestOwnerComputerIdleActionAfterItsSeatsLeave(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	create := func(name string, action ComputerIdleAction, used bool) Computer {
		t.Helper()
		c, _, err := f.store.CreateComputer(t.Context(), CreateComputerRequest{Name: name, Spec: computerCapabilityJobSpec(name),
			Actor: "owner", SeatLimit: intPointer(2), IdleAction: action, ImageSeatPlaces: 4})
		if err != nil {
			t.Fatal(err)
		}
		if used {
			// A Seat that has used this Computer and left: its identity stays.
			if _, err := f.store.db.Exec(`INSERT INTO computer_seat_identities(seat_id,computer_id,place,introduced_revision,ever_live) VALUES(?,?,1,1,1)`, "seat_"+name, c.ComputerID); err != nil {
				t.Fatal(err)
			}
		}
		return c
	}
	stop := create("owner-stop", ComputerIdleStop, true)
	remove := create("owner-delete", ComputerIdleDelete, true)
	keep := create("owner-nothing", ComputerIdleNothing, true)
	fresh := create("owner-fresh", ComputerIdleStop, false)
	state := func(c Computer) Computer {
		t.Helper()
		got, err := f.store.GetComputer(t.Context(), c.ComputerID)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}

	if err := f.store.CheckSeatLife(t.Context()); err != nil { // starts the idle waits
		t.Fatal(err)
	}
	f.clock.Advance(10*time.Minute - time.Second)
	if err := f.store.CheckSeatLife(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := state(stop); got.DesiredState != contract.ServiceDesiredRunning {
		t.Fatalf("stopped before its idle wait: %s", got.DesiredState)
	}
	f.clock.Advance(time.Second)
	if err := f.store.CheckSeatLife(t.Context()); err != nil {
		t.Fatal(err)
	}
	got := state(stop)
	if got.DesiredState != contract.ServiceDesiredStopped || got.StorageID != stop.StorageID || got.StorageGeneration != stop.StorageGeneration || got.RemovalOutcome != "" {
		t.Fatalf("idle stop = desired %s storage %s@%d removal %q", got.DesiredState, got.StorageID, got.StorageGeneration, got.RemovalOutcome)
	}
	if got := state(remove); got.DesiredState != contract.ServiceDesiredRemoved {
		t.Fatalf("idle delete = %s", got.DesiredState)
	}
	if got := state(keep); got.DesiredState != contract.ServiceDesiredRunning {
		t.Fatalf("idle nothing = %s", got.DesiredState)
	}
	if got := state(fresh); got.DesiredState != contract.ServiceDesiredRunning {
		t.Fatalf("a Computer no Seat used went idle: %s", got.DesiredState)
	}

	// A stopped Computer stays stopped: the timer never removes it, and it
	// waits for the owner (or a by-name approval).
	f.clock.Advance(time.Hour)
	if err := f.store.CheckSeatLife(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := state(stop); got.DesiredState != contract.ServiceDesiredStopped {
		t.Fatalf("idle timer acted on a stopped Computer: %s", got.DesiredState)
	}
	history, err := testReads(f.store).ListComputerIntents(t.Context(), stop.ComputerID, "", MaxJobPageLimit)
	if err != nil || len(history.Intents) != 2 || history.Intents[1].Actor != "idle action" || history.Intents[1].DesiredState != contract.ServiceDesiredStopped {
		t.Fatalf("idle stop intent = %#v err=%v", history, err)
	}
}

// TestOwnerComputerIdleStopYieldsToARacingChange proves the stop rechecks
// idleness inside its own write fence: a Seat that arrives between selection
// and the stop wins, as it does for the idle removal.
func TestOwnerComputerIdleStopYieldsToARacingChange(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	c, _, err := f.store.CreateComputer(t.Context(), CreateComputerRequest{Name: "racing-stop", Spec: computerCapabilityJobSpec("racing-stop"),
		Actor: "owner", SeatLimit: intPointer(2), IdleAction: ComputerIdleStop, ImageSeatPlaces: 4})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.db.Exec(`INSERT INTO computer_seat_identities(seat_id,computer_id,place,introduced_revision,ever_live) VALUES('seat_racing',?,1,1,1)`, c.ComputerID); err != nil {
		t.Fatal(err)
	}
	if err := f.store.CheckSeatLife(t.Context()); err != nil {
		t.Fatal(err)
	}
	f.clock.Advance(10 * time.Minute)
	raced := false
	f.store.reconfigurationHook = func(point string) error {
		if point != "computer_idle_selected" || raced {
			return nil
		}
		raced = true
		// A live Seat arrives without any intent revision, so only the stop's
		// own idleness recheck can see it.
		id := fmt.Sprintf("seat_%032x", 0xa11)
		if _, err := f.store.db.Exec(`INSERT INTO seat_approvals(seat_id,node_id,boot_session_id,request_json,state,deadline_ns,lifecycle_state) VALUES(?,?,?,'{}','approved',0,'live')`, id, f.node.NodeID, f.node.BootSessionID); err != nil {
			return err
		}
		if err := createSeatLog(t.Context(), f.store.db, id, f.clock.Now()); err != nil {
			return err
		}
		_, err := f.store.db.Exec(`INSERT INTO computer_seat_identities(seat_id,computer_id,place,introduced_revision) VALUES(?,?,2,2)`, id, c.ComputerID)
		return err
	}
	if err := f.store.CheckSeatLife(t.Context()); err != nil {
		t.Fatal(err)
	}
	got, err := f.store.GetComputer(t.Context(), c.ComputerID)
	if !raced || err != nil || got.DesiredState != contract.ServiceDesiredRunning {
		t.Fatalf("raced=%v desired=%s err=%v: the idle stop overrode a newer change", raced, got.DesiredState, err)
	}
}

// TestOwnerComputerIdleWaitRestartsAfterOwnerStart proves an owner start gives
// a full idle wait: the old, expired stamp never stops it at once.
func TestOwnerComputerIdleWaitRestartsAfterOwnerStart(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	c, _, err := f.store.CreateComputer(t.Context(), CreateComputerRequest{Name: "restarted", Spec: computerCapabilityJobSpec("restarted"),
		Actor: "owner", SeatLimit: intPointer(2), IdleAction: ComputerIdleStop, ImageSeatPlaces: 4})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.db.Exec(`INSERT INTO computer_seat_identities(seat_id,computer_id,place,introduced_revision,ever_live) VALUES('seat_restarted',?,1,1,1)`, c.ComputerID); err != nil {
		t.Fatal(err)
	}
	if err := f.store.CheckSeatLife(t.Context()); err != nil {
		t.Fatal(err)
	}
	f.clock.Advance(10 * time.Minute)
	if err := f.store.CheckSeatLife(t.Context()); err != nil {
		t.Fatal(err)
	}
	stopped, err := f.store.GetComputer(t.Context(), c.ComputerID)
	if err != nil || stopped.DesiredState != contract.ServiceDesiredStopped {
		t.Fatalf("idle stop = %s err=%v", stopped.DesiredState, err)
	}
	f.clock.Advance(time.Hour)
	started, err := f.store.SetComputerDesiredState(t.Context(), c.ComputerID, computerDesiredRequest(stopped, contract.ServiceDesiredRunning, "owner"))
	if err != nil {
		t.Fatal(err)
	}
	check := func(want contract.ServiceDesiredState) {
		t.Helper()
		if err := f.store.CheckSeatLife(t.Context()); err != nil {
			t.Fatal(err)
		}
		got, err := f.store.GetComputer(t.Context(), started.ComputerID)
		if err != nil || got.DesiredState != want {
			t.Fatalf("desired = %s err=%v, want %s", got.DesiredState, err, want)
		}
	}
	check(contract.ServiceDesiredRunning)
	f.clock.Advance(10*time.Minute - time.Second)
	check(contract.ServiceDesiredRunning)
	f.clock.Advance(time.Second)
	check(contract.ServiceDesiredStopped)
}
