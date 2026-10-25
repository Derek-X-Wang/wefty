package agent

import (
	"context"
	"errors"
	"strings"

	"github.com/Derek-X-Wang/wefty/l1"
	"testing"
	"time"
)

func TestSeatWaitClockUsesSleepCountingSource(t *testing.T) {
	elapsed := time.Hour
	clock := seatWaitClock{read: func() (time.Duration, error) { return elapsed, nil }}
	start := clock.Now()
	elapsed += 2 * time.Hour
	if got := clock.Now().Sub(start); got != 2*time.Hour {
		t.Fatalf("sleep wait advanced %s", got)
	}
}

type zeroWaitApprovalClient struct {
	before func()
	posts  *int
}

func (c zeroWaitApprovalClient) BeginSeatApproval(context.Context, l1.SeatApprovalRequest) (l1.SeatApproval, error) {
	if c.before != nil {
		c.before()
	}
	return l1.SeatApproval{State: "requested", Deadline: time.Now().Add(time.Hour), RemainingWait: 0}, nil
}
func (c zeroWaitApprovalClient) AnswerSeatApproval(_ context.Context, _ string, answer l1.SeatApprovalAnswer) (l1.SeatApproval, error) {
	if c.posts != nil {
		*c.posts++
	}
	state := "refused"
	if answer.Approved {
		state = "approved"
	}
	return l1.SeatApproval{State: state, Reason: answer.Reason}, nil
}

func TestSeatApprovalZeroRemainingWaitNeverUsesWallDeadline(t *testing.T) {
	shown := false
	controller, err := NewSeatApprovalController(zeroWaitApprovalClient{}, "node", "boot", SeatApprovalSettings{}, SeatApprovalDialogFunc(func(context.Context, SeatApprovalPrompt) (SeatApprovalChoice, error) {
		shown = true
		return SeatApprovalApprove, nil
	}), newManualClock(time.Now()))
	if err != nil {
		t.Fatal(err)
	}
	reply, err := controller.RequestApproval(t.Context(), l1.SeatApprovalRequest{SeatID: "seat_" + strings.Repeat("a", 32), Harness: "test", Folder: "/workspace", Form: "any", Network: "none"})
	if err != nil || shown || reply.State != "refused" {
		t.Fatalf("zero remaining wait opened dialog: shown=%t reply=%+v err=%v", shown, reply, err)
	}
}

func TestSeatApprovalClockErrorIsReturnedWithoutPanic(t *testing.T) {
	clock := seatWaitClock{read: func() (time.Duration, error) { return 0, errors.New("clock unavailable") }}
	_, err := NewSeatApprovalController(zeroWaitApprovalClient{}, "node", "boot", SeatApprovalSettings{}, SeatApprovalDialogFunc(func(context.Context, SeatApprovalPrompt) (SeatApprovalChoice, error) {
		t.Error("dialog opened without clock")
		return SeatApprovalDeny, nil
	}), clock)
	if err == nil {
		t.Fatal("clock error was not returned")
	}
}

func TestSeatApprovalRuntimeClockErrorReturnsWithoutAnswer(t *testing.T) {
	bad := false
	posts := 0
	failure := errors.New("clock unavailable")
	clock := seatWaitClock{read: func() (time.Duration, error) {
		if bad {
			return 0, failure
		}
		return time.Hour, nil
	}}
	client := zeroWaitApprovalClient{before: func() { bad = true }, posts: &posts}
	controller, err := NewSeatApprovalController(client, "node", "boot", SeatApprovalSettings{}, SeatApprovalDialogFunc(func(context.Context, SeatApprovalPrompt) (SeatApprovalChoice, error) { return SeatApprovalApprove, nil }), clock)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			t.Fatalf("clock failure panicked: %v", recovered)
		}
	}()
	_, err = controller.RequestApproval(t.Context(), l1.SeatApprovalRequest{SeatID: "seat_" + strings.Repeat("b", 32), Harness: "test", Folder: "/workspace", Form: "any", Network: "none"})
	if !errors.Is(err, failure) || posts != 0 {
		t.Fatalf("clock failure granted authority or lost error: posts=%d err=%v", posts, err)
	}
}
