package main

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestUpgradeGapKeepsClaimsOpen(t *testing.T) {
	for _, finish := range []bool{false, true} {
		t.Run(map[bool]string{false: "timeout-never-pauses", true: "proceeds-after-last-finish"}[finish], func(t *testing.T) {
			f := newUpgradeFixture(t)
			f.node.ActiveAttempts = []l1.NodeActiveAttempt{{JobID: "ordinary", AttemptID: "ordinary", Class: contract.JobClassOneShot}}
			disables := 0
			waits := 0
			set := f.operations.setClaims
			f.operations.setClaims = func(ctx context.Context, p connprofile.Profile, r l1.NodeIntentRequest) (l1.Node, error) {
				if !r.ClaimsEnabled {
					disables++
					if len(f.node.ActiveAttempts) != 0 {
						t.Fatal("disabled before last ordinary run finished")
					}
				}
				return set(ctx, p, r)
			}
			wait := f.operations.wait
			f.operations.wait = func(ctx context.Context, d time.Duration) error {
				if !f.restarted {
					if !f.node.ClaimsEnabled {
						t.Fatal("gap wait paused claims")
					}
					waits++
					paths, _ := resolveSetupPaths(f.home)
					if receipt, err := readUpgradeDrain(paths.config); err == nil {
						t.Fatalf("receipt before gap: %+v", receipt)
					}
					if finish && waits == 2 {
						f.node.ActiveAttempts = nil
					}
				}
				return wait(ctx, d)
			}
			started := f.now
			report, err := runUpgrade(t.Context(), f.host, false, io.Discard, 12*time.Second)
			if finish {
				if err != nil || disables != 1 || !f.replaced {
					t.Fatalf("did not proceed: %v disables=%d", err, disables)
				}
			} else {
				if err == nil || commandExitCode(err) != exitConflict || !strings.Contains(err.Error(), "drain timed out; installed stack unchanged") || disables != 0 || f.replaced || f.now.Sub(started) != 12*time.Second {
					t.Fatalf("timeout changed admission: %v disables=%d elapsed=%s", err, disables, f.now.Sub(started))
				}
				if report.WaitingNonrestartable != 1 {
					t.Fatalf("missing timeout count: %+v", report)
				}
			}
			if !f.node.ClaimsEnabled {
				t.Fatal("claims left closed")
			}
			assertNoUpgradeDrainReceipt(t, f)
		})
	}
}

func TestUpgradeGapLateClaimSharesOriginalDeadline(t *testing.T) {
	f := newUpgradeFixture(t)
	resident := l1.NodeActiveAttempt{JobID: "resident", AttemptID: "resident"}
	late := l1.NodeActiveAttempt{JobID: "late", AttemptID: "late"}
	f.node.ActiveAttempts = []l1.NodeActiveAttempt{resident}
	openWaits, closedWaits := 0, 0
	wait := f.operations.wait
	f.operations.wait = func(ctx context.Context, d time.Duration) error {
		if d == upgradeDrainPoll || d == 2*time.Second {
			if f.node.ClaimsEnabled {
				openWaits++
				f.node.ActiveAttempts = nil
			} else {
				closedWaits++
			}
		}
		return wait(ctx, d)
	}
	set := f.operations.setClaims
	f.operations.setClaims = func(ctx context.Context, p connprofile.Profile, r l1.NodeIntentRequest) (l1.Node, error) {
		node, err := set(ctx, p, r)
		if err == nil && !r.ClaimsEnabled {
			f.node.ActiveAttempts = []l1.NodeActiveAttempt{late}
		}
		return node, err
	}
	started := f.now
	report, err := runUpgrade(t.Context(), f.host, false, io.Discard, 12*time.Second)
	if err == nil || openWaits != 1 || closedWaits != 2 || f.now.Sub(started) != 12*time.Second || f.replaced || !f.node.ClaimsEnabled || len(report.RunningWork) != 2 {
		t.Fatalf("deadline reset or late claim ignored: %v open=%d closed=%d elapsed=%s report=%+v", err, openWaits, closedWaits, f.now.Sub(started), report)
	}
}
