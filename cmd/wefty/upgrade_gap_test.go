package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/internal/serviceplan"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestUpgradeGapTimeoutJSONIsRetryable(t *testing.T) {
	for _, transport := range []string{"plain", "fleet"} {
		t.Run(transport, func(t *testing.T) {
			var f *upgradeFixture
			if transport == "fleet" {
				f = newFleetUpgradeFixture(t, serviceplan.Join)
			} else {
				f = newUpgradeFixture(t)
			}
			f.node.ActiveAttempts = []l1.NodeActiveAttempt{
				{JobID: "ordinary-1", AttemptID: "attempt-1"},
				{JobID: "ordinary-1", AttemptID: "duplicate-attempt"},
				{AttemptID: "empty-job-id"},
				{JobID: "ordinary-2", AttemptID: "attempt-2"},
				{JobID: "ordinary-3", AttemptID: "attempt-3"},
				{JobID: "ordinary-4", AttemptID: "attempt-4"},
				{JobID: "service", Class: contract.JobClassService},
				{JobID: "restartable", Restartable: true},
			}
			var reads []string
			f.operations.readJob = func(_ context.Context, _ connprofile.Profile, id string) (l1.Job, error) {
				reads = append(reads, id)
				switch id {
				case "ordinary-1":
					return l1.Job{Spec: contract.JobSpec{Labels: map[string]string{contract.LabelRunID: "run_blocking"}}}, nil
				case "ordinary-2":
					return l1.Job{}, nil // A Job without a Run keeps its Job ID.
				default:
					return l1.Job{}, errors.New("job read unavailable")
				}
			}
			_, err := runUpgrade(t.Context(), f.host, false, io.Discard, 12*time.Second)
			var out bytes.Buffer
			writeCommandError(&out, err, hasJSONFlag([]string{"upgrade", "--json"}))
			var envelope struct {
				Error contract.APIError `json:"error"`
			}
			if decodeErr := json.Unmarshal(out.Bytes(), &envelope); decodeErr != nil {
				t.Fatal(decodeErr)
			}
			if commandExitCode(err) != 5 || envelope.Error.Code != contract.ErrorConflict || !envelope.Error.Retryable || !strings.HasPrefix(envelope.Error.Message, "drain timed out; installed stack unchanged; try again when the running one-shots finish") {
				t.Fatalf("wrong retry advice: err=%v JSON=%s", err, out.String())
			}
			if !strings.HasSuffix(envelope.Error.Message, "(runs: run_blocking (ordinary-1), ordinary-2, ordinary-3)") || !slices.Equal(reads, []string{"ordinary-1", "ordinary-2", "ordinary-3"}) {
				t.Fatalf("wrong blocking Runs or read bound: reads=%v JSON=%s", reads, out.String())
			}
			for _, id := range []string{"ordinary-1", "ordinary-2", "ordinary-3"} {
				if !strings.Contains(envelope.Error.Message, id) {
					t.Fatalf("missing blocking Job %s: %s", id, out.String())
				}
			}
			for _, id := range []string{"ordinary-4", "service", "restartable"} {
				if strings.Contains(envelope.Error.Message, id) {
					t.Fatalf("extra or nonblocking Job %s: %s", id, out.String())
				}
			}
			if !f.node.ClaimsEnabled || f.replaced || len(f.launchd.mutating()) != 0 {
				t.Fatal("gap timeout changed admission or installation")
			}
			assertNoUpgradeDrainReceipt(t, f)
		})
	}
}

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
