package l1

import (
	"testing"
	"time"
)

func TestSeatLifeExpiresAndRetainsRoomUntilCleanup(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	id := "seat_00000000000000000000000000000001"
	if _, err := f.store.CreateSeatCall(t.Context(), "cli", SeatCallInput{SeatID: id, NodeID: f.node.NodeID, Harness: "test", Folder: "/workspace", Form: "any", Network: "none"}); err != nil {
		t.Fatal(err)
	}
	prepareSeatLogLive(t, f, id)
	f.clock.Advance(25 * time.Minute)
	if err := f.store.CheckSeatLife(t.Context()); err != nil {
		t.Fatal(err)
	}
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
	if warnings != 1 {
		t.Fatalf("warnings=%d", warnings)
	}
	f.clock.Advance(5 * time.Minute)
	if err := f.store.CheckSeatLife(t.Context()); err != nil {
		t.Fatal(err)
	}
	value, err := f.store.GetSeatRequest(t.Context(), id)
	if err != nil || value.State != SeatEnding || value.Reason != "expired" || value.CleanupComplete || value.TargetMemoryBytes != 1<<30 {
		t.Fatalf("expired=%+v %v", value, err)
	}
	page, err = f.store.GetSeatLogs(t.Context(), "cli", id, "", "", 100)
	if err != nil || !page.Ended || page.Reason != "expired" {
		t.Fatalf("log=%+v %v", page, err)
	}
}

func TestSeatLifeCommandAndPersonHoldThenLatestEnd(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	id := "seat_00000000000000000000000000000001"
	prepareSeatLogLive(t, f, id)
	if err := f.store.SeatCommandRunning(t.Context(), id, "run-one", true); err != nil {
		t.Fatal(err)
	}
	f.clock.Advance(2 * time.Hour)
	if err := f.store.CheckSeatLife(t.Context()); err != nil {
		t.Fatal(err)
	}
	v, err := f.store.GetSeatRequest(t.Context(), id)
	if err != nil || v.State != SeatLive {
		t.Fatalf("running=%+v %v", v, err)
	}
	if err := f.store.SeatPersonDriving(t.Context(), id, true); err != nil {
		t.Fatal(err)
	}
	if err := f.store.SeatCommandRunning(t.Context(), id, "run-one", false); err != nil {
		t.Fatal(err)
	}
	f.clock.Advance(time.Hour)
	if err := f.store.CheckSeatLife(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := f.store.SeatPersonDriving(t.Context(), id, false); err != nil {
		t.Fatal(err)
	}
	f.clock.Advance(29 * time.Minute)
	if err := f.store.CheckSeatLife(t.Context()); err != nil {
		t.Fatal(err)
	}
	v, err = f.store.GetSeatRequest(t.Context(), id)
	if err != nil || v.State != SeatLive {
		t.Fatalf("released screen=%+v %v", v, err)
	}
	f.clock.Advance(time.Minute)
	if err := f.store.CheckSeatLife(t.Context()); err != nil {
		t.Fatal(err)
	}
	v, err = f.store.GetSeatRequest(t.Context(), id)
	if err != nil || v.Reason != "expired" {
		t.Fatalf("expiry=%+v %v", v, err)
	}
}

type seatBootTestClock struct {
	*fakeClock
	boot    string
	elapsed time.Duration
}

func (c *seatBootTestClock) BootTime() (string, time.Duration, error) { return c.boot, c.elapsed, nil }
func (c *seatBootTestClock) BootIdentity() (string, error)            { return c.boot, nil }
func (c *seatBootTestClock) BootElapsed() (time.Duration, error)      { return c.elapsed, nil }

func TestSeatLifeIgnoresCalendarChangesAndCountsSleep(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	clock := &seatBootTestClock{fakeClock: f.clock, boot: "boot-a", elapsed: time.Hour}
	f.store.clock = clock
	id := "seat_00000000000000000000000000000001"
	prepareSeatLogLive(t, f, id)
	f.clock.Advance(24 * time.Hour)
	if err := f.store.CheckSeatLife(t.Context()); err != nil {
		t.Fatal(err)
	}
	v, err := f.store.GetSeatRequest(t.Context(), id)
	if err != nil || v.State != SeatLive {
		t.Fatalf("wall change=%+v %v", v, err)
	}
	// Simulate a suspended host: Go's ordinary monotonic clock has not moved.
	clock.elapsed += 30 * time.Minute
	if err := f.store.CheckSeatLife(t.Context()); err != nil {
		t.Fatal(err)
	}
	v, err = f.store.GetSeatRequest(t.Context(), id)
	if err != nil || v.Reason != "expired" {
		t.Fatalf("sleep=%+v %v", v, err)
	}
}

func TestSeatApprovalWindowUsesSleepCountingClock(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	clock := &seatBootTestClock{fakeClock: f.clock, boot: "boot-a", elapsed: time.Hour}
	f.store.clock = clock
	id := "seat_00000000000000000000000000000001"
	request := SeatApprovalRequest{SeatID: id, NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, Harness: "test", Folder: "/workspace", Form: "any", Network: "none", AnswerWait: time.Minute, WindowHours: 1}
	if _, err := f.store.BeginSeatApproval(t.Context(), "fabric-computer-node", request); err != nil {
		t.Fatal(err)
	}
	f.clock.Advance(24 * time.Hour)
	value, err := f.store.AnswerSeatApproval(t.Context(), "fabric-computer-node", id, SeatApprovalAnswer{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, Approved: true})
	if err != nil || value.State != "approved" {
		t.Fatalf("calendar ended approval early: %+v %v", value, err)
	}
	clock.elapsed += time.Hour
	windows, err := f.store.ListSeatApprovalWindows(t.Context(), f.node.NodeID)
	if err != nil || len(windows) != 0 {
		t.Fatalf("window survived sleep: %+v %v", windows, err)
	}
}

func TestSeatReleaseRevokesCredentialAndCancelsRunningCommands(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	id := "seat_00000000000000000000000000000001"
	if _, err := f.store.CreateSeatCall(t.Context(), "cli", SeatCallInput{SeatID: id, NodeID: f.node.NodeID, Harness: "test", Folder: "/workspace", Form: "any", Network: "none"}); err != nil {
		t.Fatal(err)
	}
	prepareSeatLogLive(t, f, id)
	call, err := f.store.GetSeatCall(t.Context(), "cli", id)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.store.SeatCommandRunning(t.Context(), id, "run-one", true); err != nil {
		t.Fatal(err)
	}
	if err := f.store.SeatPersonDriving(t.Context(), id, true); err != nil {
		t.Fatal(err)
	}
	result, err := f.store.ReleaseSeat(t.Context(), id, call.Credential)
	if err != nil || result.State != SeatEnding || result.Reason != "released" || result.CleanupComplete {
		t.Fatalf("release=%+v %v", result, err)
	}
	if err := f.store.SeatCallHappened(t.Context(), id, call.Credential); errorCode(err) != "seat_ended" {
		t.Fatalf("ended credential=%v", err)
	}
	commands, err := f.store.SeatCommandsToCancel(t.Context(), id)
	if err != nil || len(commands) != 1 || commands[0] != "run-one" {
		t.Fatalf("cancel=%v %v", commands, err)
	}
	page, err := f.store.GetSeatLogs(t.Context(), "", id, call.Credential, "", 100)
	if err != nil || !page.Ended || page.Reason != "released" {
		t.Fatalf("retained log=%+v %v", page, err)
	}
}

func TestSeatComputerIdleDeletesAtTenMinutesAfterDeparture(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	id := "seat_00000000000000000000000000000001"
	prepareSeatLogLive(t, f, id)
	if _, err := f.store.EndSeatRequest(t.Context(), "fabric-computer-node", id, SeatEndRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, Reason: "released"}); err != nil {
		t.Fatal(err)
	}
	f.clock.Advance(10 * time.Minute)
	if err := f.store.CheckSeatLife(t.Context()); err != nil {
		t.Fatal(err)
	}
	c, err := f.store.GetComputer(t.Context(), f.computer.ComputerID)
	if err != nil || c.DesiredState != "removed" {
		t.Fatalf("idle Computer=%+v %v", c, err)
	}
	if c.RemovalOutcome == "removed_verified" {
		t.Fatal("deletion claimed proof before helper removal")
	}
}

func TestSeatOwnerStopListsEndsAndKeepsComputer(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	id := "seat_00000000000000000000000000000001"
	prepareSeatLogLive(t, f, id)
	c, err := f.store.SetComputerDesiredState(t.Context(), f.computer.ComputerID, ComputerDesiredStateRequest{ComputerMutationPrecondition: ComputerMutationPrecondition{IntentRevision: f.computer.IntentRevision, StorageID: f.computer.StorageID, StorageGeneration: f.computer.StorageGeneration, Actor: "owner"}, DesiredState: "stopped"})
	if err != nil || len(c.EndedSeats) != 1 || c.EndedSeats[0].SeatID != id || c.EndedSeats[0].Reason != "computer_stopped" {
		t.Fatalf("owner stop=%+v %v", c.EndedSeats, err)
	}
	v, err := f.store.GetSeatRequest(t.Context(), id)
	if err != nil || v.State != SeatEnding {
		t.Fatalf("Seat=%+v %v", v, err)
	}
	f.clock.Advance(time.Hour)
	if err := f.store.CheckSeatLife(t.Context()); err != nil {
		t.Fatal(err)
	}
	c, err = f.store.GetComputer(t.Context(), c.ComputerID)
	if err != nil || c.DesiredState != "stopped" {
		t.Fatalf("owner stopped=%+v %v", c, err)
	}
}

func TestSeatOwnerCommandsEndLiveSeatsAndReturnSummary(t *testing.T) {
	for _, verb := range []string{"stop", "restart", "reimage", "reset", "remove"} {
		t.Run(verb, func(t *testing.T) {
			f := newSeatLifecycleFixture(t)
			id := "seat_00000000000000000000000000000001"
			if _, err := f.store.CreateSeatCall(t.Context(), "cli", SeatCallInput{SeatID: id, NodeID: f.node.NodeID, Harness: "test", Folder: "/workspace", Form: "any", Network: "none"}); err != nil {
				t.Fatal(err)
			}
			reviewLiveSeat(t, f, 1)
			before, err := f.store.GetComputer(t.Context(), f.computer.ComputerID)
			if err != nil {
				t.Fatal(err)
			}
			precondition := computerPrecondition(before, "owner")
			var reply Computer
			switch verb {
			case "stop":
				reply, err = f.store.SetComputerDesiredState(t.Context(), before.ComputerID, ComputerDesiredStateRequest{ComputerMutationPrecondition: precondition, DesiredState: "stopped"})
			case "restart":
				reply, _, err = f.store.RestartComputer(t.Context(), before.ComputerID, ComputerRestartRequest{ComputerMutationPrecondition: precondition, IdempotencyKey: "owner-restart"})
			case "reimage":
				reply, err = f.store.ReimageComputer(t.Context(), before.ComputerID, ComputerReimageRequest{ComputerMutationPrecondition: precondition, Image: reimageTarget('b'), IdempotencyKey: "owner-reimage"})
			case "reset":
				reply, _, err = f.store.BeginComputerStorageReset(t.Context(), before.ComputerID, ComputerStorageResetRequest{ComputerMutationPrecondition: precondition, IdempotencyKey: "owner-reset"})
			case "remove":
				reply, err = f.store.RemoveComputer(t.Context(), before.ComputerID, ComputerRemoveRequest{ComputerMutationPrecondition: precondition})
			}
			if err != nil {
				t.Fatalf("owner %s waited/refused live Seats: %v", verb, err)
			}
			reasons := map[string]string{"stop": "computer_stopped", "restart": "computer_restarted", "reimage": "computer_reimaged", "reset": "computer_reset", "remove": "computer_removed"}
			if len(reply.EndedSeats) != 1 || reply.EndedSeats[0].SeatID != id || reply.EndedSeats[0].Reason != reasons[verb] {
				t.Fatalf("%s immediate summary=%+v", verb, reply.EndedSeats)
			}
			if verb == "restart" && (reply.StorageID != before.StorageID || reply.StorageGeneration != before.StorageGeneration) {
				t.Fatal("restart changed shared Storage")
			}
			seat, err := f.store.GetSeatRequest(t.Context(), id)
			if err != nil || seat.State != SeatEnding || seat.Reason != reasons[verb] || seat.CleanupComplete {
				t.Fatalf("Seat=%+v %v", seat, err)
			}
			page, err := f.store.GetSeatLogs(t.Context(), "cli", id, "", "", 100)
			if err != nil {
				t.Fatal(err)
			}
			kinds := map[string]SeatEventKind{"stop": SeatEventOwnerStop, "restart": SeatEventOwnerRestart, "reimage": SeatEventOwnerReimage, "reset": SeatEventOwnerReset, "remove": SeatEventOwnerRemove}
			ownerAt, endAt := -1, -1
			for i, event := range page.Events {
				if event.Kind == kinds[verb] && event.Reason == reasons[verb] {
					ownerAt = i
				}
				if event.Kind == SeatEventEnded {
					endAt = i
				}
			}
			if ownerAt < 0 || endAt <= ownerAt {
				t.Fatalf("owner action must precede end: %+v", page.Events)
			}
			room, err := f.store.GetComputerRoomSnapshot(t.Context(), f.node.NodeID, DefaultComputerRoomConfig(2<<30))
			if err != nil || room.HeldMemoryBytes < 1<<30 {
				t.Fatalf("owner action returned room before proof: %+v %v", room, err)
			}
			f.reopen(t)
			durable, err := f.store.GetComputer(t.Context(), before.ComputerID)
			if err != nil || len(durable.EndedSeats) != 1 || durable.EndedSeats[0] != reply.EndedSeats[0] {
				t.Fatalf("summary lost on restart: %+v %v", durable.EndedSeats, err)
			}
		})
	}
}

func TestSeatCommandEndReplayCannotRenewOrRestart(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	id, _ := liveSeatWithCredential(t, f)
	if err := f.store.SeatCommandRunning(t.Context(), id, "command-one", true); err != nil {
		t.Fatal(err)
	}
	if err := f.store.SeatCommandRunning(t.Context(), id, "command-one", false); err != nil {
		t.Fatal(err)
	}
	f.clock.Advance(25 * time.Minute)
	if err := f.store.CheckSeatLife(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := f.store.SeatCommandRunning(t.Context(), id, "command-one", false); err != nil {
		t.Fatal(err)
	}
	if err := f.store.SeatCommandRunning(t.Context(), id, "command-one", true); errorCode(err) != "conflict" {
		t.Fatalf("ended command ID acknowledged a fresh start: %v", err)
	}
	f.clock.Advance(5 * time.Minute)
	if err := f.store.CheckSeatLife(t.Context()); err != nil {
		t.Fatal(err)
	}
	seat, err := f.store.GetSeatRequest(t.Context(), id)
	if err != nil || seat.Reason != "expired" {
		t.Fatalf("end replay renewed Seat: %+v %v", seat, err)
	}
}
