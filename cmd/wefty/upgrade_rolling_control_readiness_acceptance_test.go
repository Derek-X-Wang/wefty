//go:build rolling_upgrade_acceptance

package main

import (
	"context"
	"fmt"
	"io"
	"syscall"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/l1"
)

// Fake-launchd evidence: bootstrap acknowledges process creation, while L1
// can still refuse connections during startup. No native services are used.
func TestB1RollingUpgradeWaitsForControlPlaneReadiness(t *testing.T) {
	for _, startupGap := range []bool{false, true} {
		name := "ready_control"
		if startupGap {
			name = "transient_L1_refusal_after_L3_bootstrap"
		}
		t.Run(name, func(t *testing.T) {
			f := newRollingFixture(t)
			claim := f.claim(t, "surviving-one-shot", contract.JobClassOneShot, false)
			observe := f.operations.observe
			refusals := 0
			f.operations.observe = func(ctx context.Context, profile connprofile.Profile) (l1.Node, error) {
				// The fixture sets restarted only when setup bootstraps L3,
				// after replacing L1. The next read is afterService's renewal
				// probe, exactly where isolated upgrade-1 failed.
				if startupGap && f.restarted && refusals == 0 {
					refusals++
					return l1.Node{}, markTransientTransport(ctx, fmt.Errorf("call L1 after bootstrap: dial tcp: %w", syscall.ECONNREFUSED))
				}
				return observe(ctx, profile)
			}
			_, err := runUpgrade(t.Context(), f.host, false, io.Discard)
			if err != nil {
				t.Fatalf("rolling upgrade must wait for L1 startup within the renewal window (refusals=%d, promoted=%v): %v", refusals, f.promoted, err)
			}
			if startupGap && refusals != 1 {
				t.Fatal("test did not exercise the post-bootstrap startup refusal")
			}
			job, err := f.job(t.Context(), claim.Job.JobID)
			mustRolling(t, err)
			if !f.promoted || len(job.Attempts) != 1 || job.CurrentAttemptID != claim.Lease.AttemptID {
				t.Fatal("upgrade did not preserve the original Attempt through promotion")
			}
		})
	}
}
