package main

import (
	"context"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/internal/serviceplan"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestUpgradeRestartableDisappearsBeforeDisabledObservation(t *testing.T) {
	for _, transport := range []string{"plain", "fleet"} {
		for _, evidence := range []string{"lost", "unknown", "missing-attempt", "succeeded", "interrupted"} {
			t.Run(transport+"/"+evidence, func(t *testing.T) {
				var f *upgradeFixture
				if transport == "fleet" {
					f = newFleetUpgradeFixture(t, serviceplan.Join)
				} else {
					f = newUpgradeFixture(t)
				}
				old := l1.NodeActiveAttempt{JobID: "disappeared", AttemptID: "old-attempt", BootSessionID: f.node.BootSessionID, Class: contract.JobClassOneShot, Restartable: true}
				f.node.ActiveAttempts = []l1.NodeActiveAttempt{old}
				before := installedUpgradeFiles(t, f)
				set := f.operations.setClaims
				disabled := false
				f.operations.setClaims = func(ctx context.Context, p connprofile.Profile, r l1.NodeIntentRequest) (l1.Node, error) {
					node, err := set(ctx, p, r)
					if err == nil && !r.ClaimsEnabled {
						disabled = true
						f.node.ActiveAttempts = nil
					}
					return node, err
				}
				reads := 0
				f.operations.readJob = func(context.Context, connprofile.Profile, string) (l1.Job, error) {
					reads++
					if !disabled {
						t.Fatal("settlement read before disable")
					}
					if evidence == "unknown" {
						return l1.Job{}, errors.New("settlement unavailable")
					}
					job := l1.Job{JobID: old.JobID, State: contract.JobQueued}
					if evidence == "missing-attempt" {
						return job, nil
					}
					state := contract.AttemptLost
					if evidence == "succeeded" {
						state = contract.AttemptSucceeded
						job.State = contract.JobSucceeded
					}
					if evidence == "interrupted" {
						state = contract.AttemptInterrupted
					}
					job.Attempts = []l1.Attempt{{AttemptID: old.AttemptID, State: state}}
					return job, nil
				}
				f.operations.interruptAttempt = func(context.Context, connprofile.Profile, string, l1.InterruptRestartableAttemptRequest) (l1.Job, error) {
					t.Fatal("sent interruption to a disappeared Attempt")
					return l1.Job{}, nil
				}
				report, err := runUpgrade(t.Context(), f.host, false, io.Discard, 12*time.Second)
				safe := evidence == "succeeded" || evidence == "interrupted"
				if safe {
					if err != nil || !f.replaced {
						t.Fatalf("positive settlement did not allow replacement: %v", err)
					}
				} else if err == nil || commandExitCode(err) != exitConflict || !strings.Contains(err.Error(), "drain timed out; installed stack unchanged") || f.replaced || !reflect.DeepEqual(before, installedUpgradeFiles(t, f)) || len(f.launchd.mutating()) != 0 {
					t.Fatalf("disappeared Attempt allowed replacement: err=%v replaced=%v", err, f.replaced)
				}
				if reads == 0 || !disabled || !f.node.ClaimsEnabled || len(report.RunningWork) != 1 || report.RunningWork[0].AttemptID != old.AttemptID {
					t.Fatalf("settlement/restore evidence missing: reads=%d claims=%v work=%+v", reads, f.node.ClaimsEnabled, report.RunningWork)
				}
				assertNoUpgradeDrainReceipt(t, f)
			})
		}
	}
}
