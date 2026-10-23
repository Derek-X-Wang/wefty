//go:build linux

package agent

import (
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/l1"
)

func TestSeatApprovalNonMacOSRefusesImmediately(t *testing.T) {
	choice, err := (NativeSeatApprovalDialog{}).Show(t.Context(), SeatApprovalPrompt{Wait: 5 * time.Minute})
	if choice != SeatApprovalUnavailable || err == nil {
		t.Fatalf("non-macOS dialog=%s err=%v; want unavailable error", choice, err)
	}
	clock := newManualClock(time.Now())
	client, _ := approvalTestClient(t, clock)
	controller, err := NewSeatApprovalController(client, "mac", "boot", SeatApprovalSettings{}, nil, clock)
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan l1.SeatApproval, 1)
	go func() {
		value, err := controller.RequestApproval(t.Context(), seatApprovalTestRequest(50))
		if err != nil {
			t.Error(err)
		}
		result <- value
	}()
	if value := awaitSeatApprovalTest(t, result); value.State != "refused" || value.Reason != "approval dialog unavailable" {
		t.Fatalf("non-macOS refusal=%+v", value)
	}
}
