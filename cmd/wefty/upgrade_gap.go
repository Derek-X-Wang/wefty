package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/l1"
)

// Keep admission open while ordinary one-shots prevent replacement. Restartable
// work does not prevent a gap. The caller carries the same deadline into the
// post-disable drain, including any claim that races the disable CAS.
func waitUpgradeGap(ctx context.Context, op upgradeHost, profile connprofile.Profile, node l1.Node, yes bool, timeout time.Duration, out io.Writer, work *[]l1.NodeActiveAttempt, counts *upgradeDrainCounts) (l1.Node, time.Time, error) {
	deadline := op.now().Add(timeout)
	if yes || !node.ClaimsEnabled {
		return node, deadline, ctx.Err()
	}
	revision, reason, boot := node.IntentRevision, node.IntentReason, node.BootSessionID
	bounded, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for {
		if ctx.Err() != nil {
			return node, deadline, ctx.Err()
		}
		*counts = upgradeDrainCounts{}
		for _, attempt := range node.ActiveAttempts {
			recordUpgradeWork(work, attempt)
			if attempt.Class != contract.JobClassService && !attempt.Restartable {
				counts.WaitingNonrestartable++
			}
		}
		left := deadline.Sub(op.now())
		if left <= 0 || errors.Is(bounded.Err(), context.DeadlineExceeded) {
			return node, deadline, upgradeDrainTimedOut()
		}
		if counts.WaitingNonrestartable == 0 {
			return node, deadline, nil
		}
		if _, err := fmt.Fprintf(out, "Draining: %d one-shot attempts remain; waiting_nonrestartable=%d stopping_restartable=0 restart_pending=0; claims remain open; deadline %s.\n", counts.WaitingNonrestartable, counts.WaitingNonrestartable, deadline.Format(time.RFC3339)); err != nil {
			return node, deadline, err
		}
		if err := op.wait(bounded, min(upgradeDrainPoll, left)); err != nil {
			if ctx.Err() != nil {
				return node, deadline, ctx.Err()
			}
			if errors.Is(err, context.DeadlineExceeded) {
				return node, deadline, upgradeDrainTimedOut()
			}
			return node, deadline, err
		}
		if !op.now().Before(deadline) {
			return node, deadline, upgradeDrainTimedOut()
		}
		current, err := op.observe(bounded, profile)
		if err != nil {
			if ctx.Err() == nil && errors.Is(bounded.Err(), context.DeadlineExceeded) {
				return current, deadline, upgradeDrainTimedOut()
			}
			return current, deadline, err
		}
		if current.NodeID != node.NodeID || !current.ClaimsEnabled || current.IntentRevision != revision || current.IntentReason != reason || current.BootSessionID != boot {
			return current, deadline, &apiResponseError{APIError: contract.APIError{Code: contract.ErrorConflict, Message: "node intent or boot changed while waiting for upgrade gap; installed stack unchanged"}}
		}
		node = current
	}
}
