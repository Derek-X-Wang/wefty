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
