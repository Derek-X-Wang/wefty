package main

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/internal/serviceplan"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestUpgradeGapDeadlineExpiresBeforeDisable(t *testing.T) {
	f := newUpgradeFixture(t)
	op := *f.operations
	paths, _ := resolveSetupPaths(f.home)
	profile := f.profile(t)
	calls := 0
	op.setClaims = func(context.Context, connprofile.Profile, l1.NodeIntentRequest) (l1.Node, error) {
		calls++
		f.node.ClaimsEnabled = false
		return f.node, nil
	}
	var work []l1.NodeActiveAttempt
	counts := upgradeDrainCounts{}
	node, deadline, err := waitUpgradeGap(t.Context(), op, profile, f.node, false, 12*time.Second, io.Discard, &work, &counts)
	if err != nil {
		t.Fatal(err)
	}
	f.now = f.now.Add(13 * time.Second) // latency after gap detection, before persistence/CAS
	receipt, err := beginUpgradeDrain(t.Context(), op, paths, profile, node, 12*time.Second, deadline)
	if err == nil || !strings.Contains(err.Error(), "drain timed out; installed stack unchanged") || receipt != nil || calls != 0 || !f.node.ClaimsEnabled {
		t.Fatalf("expired gap deadline disabled claims: err=%v receipt=%v calls=%d", err, receipt, calls)
	}
	assertNoUpgradeDrainReceipt(t, f)
}

func TestUpgradeReceiptLatencyCannotExtendDrain(t *testing.T) {
	for _, transport := range []string{"plain", "fleet"} {
		t.Run(transport, func(t *testing.T) {
			var f *upgradeFixture
			if transport == "fleet" {
				f = newFleetUpgradeFixture(t, serviceplan.Join)
			} else {
				f = newUpgradeFixture(t)
			}
			calls := 0
			set := f.operations.setClaims
			f.operations.setClaims = func(ctx context.Context, p connprofile.Profile, r l1.NodeIntentRequest) (l1.Node, error) {
				calls++
				return set(ctx, p, r)
			}
			f.operations.writeDrain = func(ctx context.Context, config string, r *upgradeDrainReceipt) error {
				deadline, ok := ctx.Deadline()
				if !ok || time.Until(deadline) > 12*time.Second {
					t.Error("receipt persistence has no remaining-budget context")
				}
				f.now = f.now.Add(13 * time.Second)
				return writeUpgradeDrain(config, r)
			}
			_, err := runUpgrade(t.Context(), f.host, false, io.Discard, 12*time.Second)
			if err == nil || commandExitCode(err) != exitConflict || !strings.Contains(err.Error(), "drain timed out; installed stack unchanged") || calls != 0 || !f.node.ClaimsEnabled || f.replaced || len(f.launchd.mutating()) != 0 {
				t.Fatalf("receipt latency escaped budget: err=%v calls=%d", err, calls)
			}
			assertNoUpgradeDrainReceipt(t, f)
		})
	}
}

func TestUpgradeGapAndReceiptLatencyShareAbsoluteDeadline(t *testing.T) {
	for _, transport := range []string{"plain", "fleet"} {
		t.Run(transport, func(t *testing.T) {
			var f *upgradeFixture
			if transport == "fleet" {
				f = newFleetUpgradeFixture(t, serviceplan.Join)
			} else {
				f = newUpgradeFixture(t)
			}
			f.node.ActiveAttempts = []l1.NodeActiveAttempt{{JobID: "ordinary", AttemptID: "ordinary"}}
			started := f.now
			wait := f.operations.wait
			f.operations.wait = func(ctx context.Context, d time.Duration) error {
				if f.node.ClaimsEnabled && d == upgradeDrainPoll {
					f.node.ActiveAttempts = nil
				}
				return wait(ctx, d)
			}
			f.operations.writeDrain = func(ctx context.Context, config string, r *upgradeDrainReceipt) error {
				deadline, ok := ctx.Deadline()
				if !ok || time.Until(deadline) > 7*time.Second {
					t.Fatal("gap time was not charged before receipt persistence")
				}
				recorded, err := upgradeReasonDeadline(r.Reason)
				if err != nil || !recorded.Equal(started.Add(12*time.Second)) {
					t.Fatalf("deadline moved after gap: %s %v", recorded, err)
				}
				f.now = f.now.Add(3 * time.Second)
				return writeUpgradeDrain(config, r)
			}
			set := f.operations.setClaims
			disabled := false
			f.operations.setClaims = func(ctx context.Context, p connprofile.Profile, r l1.NodeIntentRequest) (l1.Node, error) {
				if !r.ClaimsEnabled {
					disabled = true
					deadline, ok := ctx.Deadline()
					if !ok || time.Until(deadline) > 4*time.Second {
						t.Fatal("receipt time was not charged to the disable RPC")
					}
				}
				node, err := set(ctx, p, r)
				if err == nil && !r.ClaimsEnabled {
					f.node.ActiveAttempts = []l1.NodeActiveAttempt{{JobID: "late", AttemptID: "late"}}
				}
				return node, err
			}
			_, err := runUpgrade(t.Context(), f.host, false, io.Discard, 12*time.Second)
			if err == nil || commandExitCode(err) != exitConflict || !disabled || !f.node.ClaimsEnabled || f.replaced || f.now.Sub(started) != 12*time.Second {
				t.Fatalf("deadline extended: err=%v elapsed=%s disabled=%v", err, f.now.Sub(started), disabled)
			}
			assertNoUpgradeDrainReceipt(t, f)
		})
	}
}
