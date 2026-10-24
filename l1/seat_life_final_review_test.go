package l1

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestSeatApprovalWindowRebootAtFiftyNineMinutesCannotExtendGrant(t *testing.T) {
	for _, hours := range []int{1, 24} {
		t.Run(fmt.Sprint(hours, "h"), func(t *testing.T) {
			f := newSeatLifecycleFixture(t)
			clock := &seatBootTestClock{fakeClock: f.clock, boot: "boot-a", elapsed: time.Hour}
			f.store.clock = clock
			request := SeatApprovalRequest{SeatID: fmt.Sprintf("seat_%032x", 1), NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, Harness: "test", Folder: "/workspace", Form: "any", Network: "none", AnswerWait: 5 * time.Minute, WindowHours: hours}
			if _, err := f.store.BeginSeatApproval(t.Context(), "fabric-computer-node", request); err != nil {
				t.Fatal(err)
			}
			if _, err := f.store.AnswerSeatApproval(t.Context(), "fabric-computer-node", request.SeatID, SeatApprovalAnswer{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, Approved: true}); err != nil {
				t.Fatal(err)
			}
			beforeBoot := time.Duration(hours)*time.Hour - time.Minute
			clock.elapsed += beforeBoot
			f.clock.Advance(beforeBoot)
			clock.boot = "boot-b"
			var logs []string
			f.store.logf = func(format string, args ...any) { logs = append(logs, fmt.Sprintf(format, args...)) }
			if err := f.store.CheckSeatLife(t.Context()); err != nil {
				t.Fatal(err)
			}
			reopenSeatLife(t, f, StoreOptions{Clock: clock})
			clock.elapsed += 50 * time.Minute
			f.clock.Advance(50 * time.Minute)
			windows, err := f.store.ListSeatApprovalWindows(t.Context(), f.node.NodeID)
			if err != nil || len(windows) != 0 {
				t.Fatalf("reboot extended %dh grant past its cap: windows=%+v err=%v", hours, windows, err)
			}
			if len(logs) != 1 || !strings.Contains(logs[0], "reason=l1_host_restarted") {
				t.Fatalf("window closure lacks clear reason: %q", logs)
			}
			request.SeatID = fmt.Sprintf("seat_%032x", 2)
			request.WindowHours = 0
			next, err := f.store.BeginSeatApproval(t.Context(), "fabric-computer-node", request)
			if err != nil || next.State != "requested" || next.Source != "" {
				t.Fatalf("new request skipped owner dialog: %+v %v", next, err)
			}
		})
	}
}

func TestSeatApprovalDispatchDelayCapsBootClockWait(t *testing.T) {
	f := newSeatLifecycleFixture(t)
	clock := &seatBootTestClock{fakeClock: f.clock, boot: "boot-a", elapsed: time.Hour}
	f.store.clock = clock
	id := fmt.Sprintf("seat_%032x", 1)
	if _, err := f.store.CreateSeatCall(t.Context(), "cli", SeatCallInput{SeatID: id, NodeID: f.node.NodeID, Harness: "test", Folder: "/workspace", Form: "any", Network: "none"}); err != nil {
		t.Fatal(err)
	}
	clock.elapsed += 2 * time.Minute
	f.clock.Advance(2 * time.Minute)
	poll := SeatCallPoll{SeatNodeRequest: SeatNodeRequest{NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID}, Room: DefaultComputerRoomConfig(3 << 30), HostEnabled: true, AnswerWait: 5 * time.Minute}
	request := SeatApprovalRequest{SeatID: id, NodeID: f.node.NodeID, BootSessionID: f.node.BootSessionID, Harness: "test", Folder: "/workspace", Form: "any", Network: "none", AnswerWait: 5 * time.Minute}
	for _, want := range []time.Duration{3 * time.Minute, 2 * time.Minute} {
		if _, err := f.store.PollSeatCalls(t.Context(), "fabric-computer-node", poll); err != nil {
			t.Fatal(err)
		}
		pending, err := f.store.BeginSeatApproval(t.Context(), "fabric-computer-node", request)
		if err != nil || pending.State != "requested" || pending.RemainingWait != want {
			t.Fatalf("dispatch/repeated poll lost original boot deadline: %+v want=%s err=%v", pending, want, err)
		}
		clock.elapsed += time.Minute
		f.clock.Advance(time.Minute)
	}
	clock.elapsed += time.Minute
	f.clock.Advance(time.Minute)
	pending, err := f.store.BeginSeatApproval(t.Context(), "fabric-computer-node", request)
	if err != nil || pending.State != "refused" || pending.Reason != "no answer from the owner" {
		t.Fatalf("answer wait exceeded five minutes from request: %+v %v", pending, err)
	}
}
