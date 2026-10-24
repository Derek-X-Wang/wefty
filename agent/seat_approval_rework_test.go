//go:build darwin || linux

package agent

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/l1"
)

type seatAnswerBlip struct {
	SeatApprovalClient
	posts       atomic.Int32
	posted      chan struct{}
	afterCommit bool
}

func (c *seatAnswerBlip) AnswerSeatApproval(ctx context.Context, id string, answer l1.SeatApprovalAnswer) (l1.SeatApproval, error) {
	if c.posts.Add(1) == 1 {
		if c.afterCommit {
			if _, err := c.SeatApprovalClient.AnswerSeatApproval(ctx, id, answer); err != nil {
				return l1.SeatApproval{}, err
			}
		}
		close(c.posted)
		return l1.SeatApproval{}, fmt.Errorf("temporary L1 outage")
	}
	return c.SeatApprovalClient.AnswerSeatApproval(ctx, id, answer)
}
func TestSeatApprovalAnswerBlipRetriesWithoutSecondDialog(t *testing.T) {
	for _, delay := range []time.Duration{time.Second, DefaultSeatApprovalWait} {
		for _, afterCommit := range []bool{false, true} {
			t.Run(fmt.Sprintf("after_commit=%v_delay=%s", afterCommit, delay), func(t *testing.T) {
				clock := newManualClock(time.Date(2026, 10, 23, 0, 0, 0, 0, time.UTC))
				client, _ := approvalTestClient(t, clock)
				blip := &seatAnswerBlip{SeatApprovalClient: client, posted: make(chan struct{}), afterCommit: afterCommit}
				var dialogs atomic.Int32
				c, err := NewSeatApprovalController(blip, "mac", "boot", SeatApprovalSettings{}, SeatApprovalDialogFunc(func(context.Context, SeatApprovalPrompt) (SeatApprovalChoice, error) {
					dialogs.Add(1)
					return SeatApprovalApprove, nil
				}), clock)
				if err != nil {
					t.Fatal(err)
				}
				input := l1.SeatApprovalRequest{SeatID: "seat_" + strings.Repeat("e", 32), Folder: "/workspace", Form: "any", Network: "none"}
				want := "approved"
				if !afterCommit && delay == DefaultSeatApprovalWait {
					want = "refused"
				}
				done := make(chan error, 1)
				go func() {
					v, e := c.RequestApproval(t.Context(), input)
					if e == nil && v.State != want {
						e = fmt.Errorf("state %s", v.State)
					}
					done <- e
				}()
				awaitSeatApprovalTest(t, blip.posted)
				select {
				case e := <-done:
					t.Fatalf("answer was not retried: %v", e)
				default:
				}
				clock.waitForDeadline(t, clock.Now().Add(time.Second))
				clock.Advance(delay)
				if err := awaitSeatApprovalTest(t, done); err != nil {
					t.Fatal(err)
				}
				if _, err := c.RequestApproval(t.Context(), input); err != nil {
					t.Fatal(err)
				}
				if blip.posts.Load() != 2 || dialogs.Load() != 1 {
					t.Fatalf("posts=%d dialogs=%d", blip.posts.Load(), dialogs.Load())
				}
			})
		}
	}
}

// Once L1 has settled an answer, subsequent calls must consult its durable
// state rather than retain the controller's old Approved response forever.
func TestSeatApprovalAnsweredCallRechecksL1(t *testing.T) {
	clock := newManualClock(time.Date(2026, 10, 24, 0, 0, 0, 0, time.UTC))
	client, store := approvalTestClient(t, clock)
	var dialogs atomic.Int32
	controller, err := NewSeatApprovalController(client, "mac", "boot", SeatApprovalSettings{}, SeatApprovalDialogFunc(func(context.Context, SeatApprovalPrompt) (SeatApprovalChoice, error) {
		dialogs.Add(1)
		return SeatApprovalApprove, nil
	}), clock)
	if err != nil {
		t.Fatal(err)
	}
	input := l1.SeatApprovalRequest{SeatID: "seat_" + strings.Repeat("f", 32), Folder: "/workspace", Form: "any", Network: "none"}
	first, err := controller.RequestApproval(t.Context(), input)
	if err != nil || first.State != "approved" {
		t.Fatalf("first answer: %+v %v", first, err)
	}
	if _, err := store.EndSeatRequest(t.Context(), "dialog-node", input.SeatID, l1.SeatEndRequest{NodeID: "mac", BootSessionID: "boot", Refused: true, Reason: "start failed"}); err != nil {
		t.Fatal(err)
	}
	next, err := controller.RequestApproval(t.Context(), input)
	if err != nil || next.State != "refused" || next.Reason != "start failed" || dialogs.Load() != 1 {
		t.Fatalf("settled answer must read L1 without another dialog: %+v %v dialogs=%d", next, err, dialogs.Load())
	}
}

type seatAnswerOutage struct {
	SeatApprovalClient
	posts  atomic.Int32
	posted chan struct{}
}

func (c *seatAnswerOutage) AnswerSeatApproval(context.Context, string, l1.SeatApprovalAnswer) (l1.SeatApproval, error) {
	if c.posts.Add(1) == 1 {
		close(c.posted)
	}
	return l1.SeatApproval{}, fmt.Errorf("answer transport unavailable")
}

func TestSeatApprovalPostedAnswerExpiresFromController(t *testing.T) {
	clock := newManualClock(time.Date(2026, 10, 24, 0, 0, 0, 0, time.UTC))
	client, _ := approvalTestClient(t, clock)
	outage := &seatAnswerOutage{SeatApprovalClient: client, posted: make(chan struct{})}
	var dialogs atomic.Int32
	controller, err := NewSeatApprovalController(outage, "mac", "boot", SeatApprovalSettings{}, SeatApprovalDialogFunc(func(context.Context, SeatApprovalPrompt) (SeatApprovalChoice, error) {
		dialogs.Add(1)
		return SeatApprovalApprove, nil
	}), clock)
	if err != nil {
		t.Fatal(err)
	}
	input := l1.SeatApprovalRequest{SeatID: "seat_" + strings.Repeat("9", 32), Folder: "/workspace", Form: "any", Network: "none"}
	done := make(chan error, 1)
	go func() { _, err := controller.RequestApproval(t.Context(), input); done <- err }()
	awaitSeatApprovalTest(t, outage.posted)
	clock.waitForDeadline(t, clock.Now().Add(time.Second))
	clock.Advance(DefaultSeatApprovalWait)
	if err := awaitSeatApprovalTest(t, done); err == nil {
		t.Fatal("outage was hidden")
	}
	next, err := controller.RequestApproval(t.Context(), input)
	if err != nil || next.State != "refused" || next.Reason != "no answer from the owner" || dialogs.Load() != 1 || outage.posts.Load() != 2 {
		t.Fatalf("deadline result must read L1 without another dialog or post: %+v %v dialogs=%d posts=%d", next, err, dialogs.Load(), outage.posts.Load())
	}
}
