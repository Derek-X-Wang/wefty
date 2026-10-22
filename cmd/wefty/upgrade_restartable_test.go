package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/agent"
	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestUpgradeRestartableDrain(t *testing.T) {
	for _, scenario := range []string{"all-restartable", "mixed", "late-restartable", "late-nonrestartable", "lost-reply", "natural-exit", "cancelled-job", "operator-disabled", "rollback", "cancel-drain", "intent-race", "settlement-timeout", "lost-lease"} {
		t.Run(scenario, func(t *testing.T) {
			f := newUpgradeFixture(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			restart := l1.NodeActiveAttempt{JobID: "repeat", AttemptID: "old-repeat", BootSessionID: f.node.BootSessionID, Class: contract.JobClassOneShot, Restartable: true}
			normal := l1.NodeActiveAttempt{JobID: "finish", AttemptID: "old-finish", Class: contract.JobClassOneShot}
			f.node.ActiveAttempts = []l1.NodeActiveAttempt{restart}
			if scenario == "mixed" {
				f.node.ActiveAttempts = append(f.node.ActiveAttempts, normal)
			}
			if scenario == "late-restartable" {
				f.node.ActiveAttempts = nil
			}
			if scenario == "operator-disabled" {
				f.node.ClaimsEnabled = false
				f.node.IntentRevision = 8
				f.node.IntentReason = "operator maintenance"
			}
			if scenario == "rollback" {
				f.failure = "replacement"
			}
			set := f.operations.setClaims
			f.operations.setClaims = func(ctx context.Context, p connprofile.Profile, r l1.NodeIntentRequest) (l1.Node, error) {
				node, err := set(ctx, p, r)
				if err == nil && !r.ClaimsEnabled {
					if scenario == "late-restartable" {
						f.node.ActiveAttempts = []l1.NodeActiveAttempt{restart}
					}
					if scenario == "late-nonrestartable" {
						f.node.ActiveAttempts = append(f.node.ActiveAttempts, normal)
					}
				}
				return node, err
			}
			calls := 0
			key := ""
			settled := false
			waits := 0
			f.operations.interruptAttempt = func(ctx context.Context, p connprofile.Profile, id string, r l1.InterruptRestartableAttemptRequest) (l1.Job, error) {
				if f.node.ClaimsEnabled || r.IntentRevision != f.node.IntentRevision || id != restart.AttemptID || r.JobID != restart.JobID || r.BootSessionID != restart.BootSessionID || r.Reason != l1.InterruptionUpgrade || r.IdempotencyKey == "" {
					t.Fatalf("invalid stop: %+v node=%+v", r, f.node)
				}
				calls++
				if key != "" && key != r.IdempotencyKey {
					t.Fatal("retry changed key")
				}
				key = r.IdempotencyKey
				if scenario == "lost-reply" && calls == 1 {
					return l1.Job{}, errors.New("lost reply")
				}
				return l1.Job{State: contract.JobRunning}, nil
			}
			f.operations.readJob = func(context.Context, connprofile.Profile, string) (l1.Job, error) {
				state := contract.AttemptRunning
				jobState := contract.JobRunning
				if settled {
					state = contract.AttemptInterrupted
					jobState = contract.JobQueued
				}
				if scenario == "lost-lease" && waits > 0 {
					state = contract.AttemptLost
					jobState = contract.JobQueued
					f.node.ActiveAttempts = nil
				}
				if settled && scenario == "natural-exit" {
					state = contract.AttemptSucceeded
					jobState = contract.JobSucceeded
				}
				if settled && scenario == "cancelled-job" {
					state = contract.AttemptFailed
					jobState = contract.JobFailed
				}
				return l1.Job{JobID: restart.JobID, State: jobState, Attempts: []l1.Attempt{{AttemptID: restart.AttemptID, State: state}}}, nil
			}
			wait := f.operations.wait
			f.operations.wait = func(ctx context.Context, d time.Duration) error {
				if d == upgradeDrainPoll {
					if f.node.ClaimsEnabled && scenario == "mixed" {
						f.node.ActiveAttempts = []l1.NodeActiveAttempt{restart}
						return wait(ctx, d)
					}
					waits++
					if calls == 0 {
						t.Fatal("waited before initiating restartable interruption")
					}
					if scenario == "cancel-drain" {
						cancel()
						return ctx.Err()
					}
					if scenario == "intent-race" {
						f.node.IntentRevision++
						f.node.IntentReason = "new operator disable"
					}
					if scenario != "settlement-timeout" && scenario != "lost-lease" && !(scenario == "lost-reply" && calls == 1) {
						settled = true
						f.node.ActiveAttempts = nil
						if scenario == "late-nonrestartable" && waits == 1 {
							f.node.ActiveAttempts = []l1.NodeActiveAttempt{normal}
						}
					}
				}
				return wait(ctx, d)
			}
			var out bytes.Buffer
			report, err := runUpgrade(ctx, f.host, false, &out)
			fail := scenario == "rollback" || scenario == "cancel-drain" || scenario == "intent-race" || scenario == "settlement-timeout" || scenario == "lost-lease"
			if (err != nil) != fail {
				t.Fatalf("err=%v output=%s", err, &out)
			}
			if scenario == "late-nonrestartable" && (waits != 2 || !strings.Contains(out.String(), "waiting_nonrestartable=1 stopping_restartable=1")) {
				t.Fatalf("late ordinary claim did not block after restartable settlement: waits=%d output=%s", waits, &out)
			}
			payload, marshalErr := json.Marshal(report)
			if marshalErr != nil {
				t.Fatal(marshalErr)
			}
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(payload, &fields); err != nil {
				t.Fatal(err)
			}
			for _, field := range []string{"waiting_nonrestartable", "stopping_restartable", "restart_pending"} {
				if _, ok := fields[field]; !ok {
					t.Fatalf("missing JSON drain count %s: %s", field, payload)
				}
			}
			if calls == 0 || waits == 0 {
				t.Fatalf("calls=%d waits=%d", calls, waits)
			}
			if scenario == "settlement-timeout" || scenario == "lost-lease" {
				if !strings.Contains(err.Error(), "drain timed out; installed stack unchanged") || f.replaced {
					t.Fatalf("unsafe timeout: %v", err)
				}
				if time.Duration(waits)*upgradeDrainPoll != agent.DefaultRenewalInterval+agent.ForcedShutdownBound(agent.ShutdownTimeouts{}) {
					t.Fatalf("settlement not bounded: waits=%d", waits)
				}
			}
			if scenario == "lost-reply" && calls != 2 {
				t.Fatalf("lost response not retried: %d", calls)
			}
			if scenario == "operator-disabled" || scenario == "intent-race" {
				if f.node.ClaimsEnabled {
					t.Fatal("operator disable reopened")
				}
			} else if !f.node.ClaimsEnabled {
				t.Fatal("owned disable not restored")
			}
			if !fail && scenario != "natural-exit" && scenario != "cancelled-job" && report.RestartPending != 1 {
				t.Fatalf("missing queued recovery count: %+v", report)
			}
			if !fail && (report.WaitingNonrestartable != 0 || report.StoppingRestartable != 0) {
				t.Fatalf("unsettled report: %+v", report)
			}
			if scenario == "mixed" && !strings.Contains(out.String(), "waiting_nonrestartable=1 stopping_restartable=0 restart_pending=0; claims remain open") {
				t.Fatalf("missing split counts: %s", &out)
			}
			if !strings.Contains(out.String(), "stopping_restartable=1") {
				t.Fatalf("missing stopping count: %s", &out)
			}
			if len(report.RunningWork) == 0 {
				t.Fatal("work not retained")
			}
			assertNoUpgradeDrainReceipt(t, f)
		})
	}
}

func TestUpgradeRestartableInitiatesEveryStopBeforeSettlement(t *testing.T) {
	f := newUpgradeFixture(t)
	f.node.ActiveAttempts = []l1.NodeActiveAttempt{{JobID: "a", AttemptID: "a", Restartable: true}, {JobID: "b", AttemptID: "b", Restartable: true}}
	stopped := map[string]bool{}
	f.operations.interruptAttempt = func(_ context.Context, _ connprofile.Profile, id string, _ l1.InterruptRestartableAttemptRequest) (l1.Job, error) {
		stopped[id] = true
		return l1.Job{}, nil
	}
	f.operations.readJob = func(_ context.Context, _ connprofile.Profile, id string) (l1.Job, error) {
		if len(stopped) != 2 {
			t.Fatal("settlement observed before all stop requests")
		}
		f.node.ActiveAttempts = nil
		return l1.Job{State: contract.JobQueued, Attempts: []l1.Attempt{{AttemptID: id, State: contract.AttemptInterrupted}}}, nil
	}
	report, err := runUpgrade(t.Context(), f.host, false, io.Discard)
	if err != nil || report.RestartPending != 2 {
		t.Fatalf("report=%+v err=%v", report, err)
	}
}
