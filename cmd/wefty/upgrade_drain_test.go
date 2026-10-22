package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/internal/serviceplan"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestUpgradeDrainWaitsForOneShotAndLateClaim(t *testing.T) {
	for _, late := range []bool{false, true} {
		t.Run(map[bool]string{false: "resident", true: "late-claimed"}[late], func(t *testing.T) {
			f := newUpgradeFixture(t)
			attempt := l1.NodeActiveAttempt{JobID: "finish-normally", AttemptID: "attempt", Class: contract.JobClassOneShot, State: contract.AttemptClaimed}
			if !late {
				f.node.ActiveAttempts = []l1.NodeActiveAttempt{attempt}
			}
			original := f.operations.observe
			f.operations.observe = func(ctx context.Context, p connprofile.Profile) (l1.Node, error) {
				if late && !f.node.ClaimsEnabled && !f.restarted {
					f.node.ActiveAttempts = []l1.NodeActiveAttempt{attempt}
				}
				return original(ctx, p)
			}
			wait := f.operations.wait
			waited := false
			f.operations.wait = func(ctx context.Context, d time.Duration) error {
				if d == 5*time.Second {
					if f.restarted || f.node.ClaimsEnabled == late {
						t.Fatal("wait after installation or wrong admission state")
					}
					waited = true
					late = false
					f.node.ActiveAttempts = nil
				}
				return wait(ctx, d)
			}
			var output bytes.Buffer
			_, err := runUpgrade(t.Context(), f.host, false, &output)
			if err != nil {
				t.Fatal(err)
			}
			if !waited || !f.node.ClaimsEnabled || len(f.node.ActiveAttempts) != 0 {
				t.Fatalf("waited=%v node=%+v", waited, f.node)
			}
			if !strings.Contains(output.String(), "1 one-shot") || !strings.Contains(output.String(), "deadline") {
				t.Fatalf("missing drain progress: %s", output.String())
			}
		})
	}
}

func TestUpgradeDrainTimeoutPreservesStack(t *testing.T) {
	f := newUpgradeFixture(t)
	f.node.ActiveAttempts = []l1.NodeActiveAttempt{{JobID: "long", AttemptID: "long", Class: contract.JobClassOneShot}}
	before := installedUpgradeFiles(t, f)
	var output bytes.Buffer
	_, err := runUpgrade(t.Context(), f.host, false, &output)
	if err == nil || commandExitCode(err) != exitConflict || !strings.Contains(err.Error(), "drain timed out; installed stack unchanged") {
		t.Fatalf("wrong timeout: %v", err)
	}
	if !f.node.ClaimsEnabled || !reflect.DeepEqual(before, installedUpgradeFiles(t, f)) || len(f.launchd.mutating()) != 0 {
		t.Fatal("timeout changed installation or left claims disabled")
	}
}

func TestUpgradeDrainTimeoutRetryabilityAfterRestore(t *testing.T) {
	for _, transport := range []string{"plain", "fleet"} {
		for _, outcome := range []string{"restored", "restore-failed", "pre-existing-disable", "new-disable"} {
			t.Run(transport+"/"+outcome, func(t *testing.T) {
				var f *upgradeFixture
				if transport == "fleet" {
					f = newFleetUpgradeFixture(t, serviceplan.Join)
				} else {
					f = newUpgradeFixture(t)
				}
				attempt := l1.NodeActiveAttempt{JobID: "late-job", AttemptID: "late-attempt"}
				if outcome == "pre-existing-disable" {
					f.node.ClaimsEnabled = false
					f.node.IntentReason = "operator maintenance"
					f.node.ActiveAttempts = []l1.NodeActiveAttempt{attempt}
				}
				set := f.operations.setClaims
				f.operations.setClaims = func(ctx context.Context, p connprofile.Profile, request l1.NodeIntentRequest) (l1.Node, error) {
					if request.ClaimsEnabled {
						if outcome == "restore-failed" {
							return l1.Node{}, errors.New("restore unavailable")
						}
						if outcome == "new-disable" {
							f.node.IntentRevision++
							f.node.IntentReason = "new operator disable"
							return f.node, &apiResponseError{APIError: contract.APIError{Code: contract.ErrorConflict}}
						}
					}
					node, err := set(ctx, p, request)
					if err == nil && !request.ClaimsEnabled {
						f.node.ActiveAttempts = []l1.NodeActiveAttempt{attempt}
					}
					return node, err
				}
				before := installedUpgradeFiles(t, f)
				_, err := runUpgrade(t.Context(), f.host, false, io.Discard, 12*time.Second)
				var out bytes.Buffer
				writeCommandError(&out, err, true)
				var envelope struct {
					Error contract.APIError `json:"error"`
				}
				if decodeErr := json.Unmarshal(out.Bytes(), &envelope); decodeErr != nil {
					t.Fatal(decodeErr)
				}
				wantRetryable := outcome == "restored"
				if commandExitCode(err) != 5 || envelope.Error.Code != contract.ErrorConflict || envelope.Error.Retryable != wantRetryable || strings.Contains(envelope.Error.Message, "try again when") != wantRetryable || f.node.ClaimsEnabled != wantRetryable {
					t.Fatalf("wrong timeout recovery advice: err=%v claims=%v JSON=%s", err, f.node.ClaimsEnabled, out.String())
				}
				if wantRetryable && !strings.Contains(envelope.Error.Message, attempt.JobID) {
					t.Fatalf("missing blocking Job: %s", out.String())
				}
				if f.replaced || !reflect.DeepEqual(before, installedUpgradeFiles(t, f)) || len(f.launchd.mutating()) != 0 {
					t.Fatal("timeout changed installation")
				}
				if outcome == "restore-failed" {
					paths, _ := resolveSetupPaths(f.home)
					if _, readErr := readUpgradeDrain(paths.config); readErr != nil || !strings.Contains(envelope.Error.Message, "restore upgrade intent failed") {
						t.Fatalf("restore failure lost recovery evidence: %v %s", readErr, out.String())
					}
				} else {
					assertNoUpgradeDrainReceipt(t, f)
				}
			})
		}
	}
}

func TestUpgradeDrainServiceDoesNotWait(t *testing.T) {
	f := newUpgradeFixture(t)
	f.node.ActiveAttempts = []l1.NodeActiveAttempt{{JobID: "hosted-app", AttemptID: "service-attempt", Class: contract.JobClassService, State: contract.AttemptRunning, LeaseExpiresAt: f.now.Add(l1.DefaultLeaseDuration)}}
	restarted := false
	f.operations.readJob = func(ctx context.Context, p connprofile.Profile, id string) (l1.Job, error) {
		return l1.Job{JobID: id, State: contract.JobFailed, CurrentAttemptID: "service-attempt", Spec: contract.JobSpec{Class: contract.JobClassService, Restart: contract.RestartNever}, ServiceJob: &l1.ServiceJob{DesiredState: contract.ServiceDesiredRunning, BoundNodeID: f.node.NodeID}}, nil
	}
	f.operations.restartService = func(ctx context.Context, p connprofile.Profile, id, key string) (l1.Job, error) {
		if !f.replaced || f.node.ClaimsEnabled {
			t.Fatal("service restart before healthy replacement or after opening claims")
		}
		restarted = true
		return l1.Job{JobID: id}, nil
	}
	var output bytes.Buffer
	report, err := runUpgrade(t.Context(), f.host, false, &output)
	if err != nil {
		t.Fatal(err)
	}
	if !restarted || !reflect.DeepEqual(report.RestartedServices, []string{"hosted-app"}) || !strings.Contains(output.String(), "hosted-app") {
		t.Fatalf("never service not restored or named: %+v %s", report, output.String())
	}
}

func TestStatusUpgradeDrainIsBusyAndQueued(t *testing.T) {
	for _, jsonMode := range []bool{false, true} {
		h := newStatusHarness(t)
		node := readyNode("upgrading")
		node.ClaimsEnabled = false
		node.IntentReason = "wefty upgrade test-upgrade"
		updated := time.Now()
		node.IntentUpdatedAt = &updated
		h.nodes = l1.NodeList{Nodes: []l1.Node{node}}
		var output bytes.Buffer
		err := executeStatus(t.Context(), h.clients(), jsonMode, nil, &output, io.Discard)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(output.String(), "busy, queued: upgrading; new runs wait and start when it ends") {
			t.Fatalf("missing upgrade queue notice: %s", output.String())
		}
	}
}

func TestUpgradeDrainYesSkipsWait(t *testing.T) {
	f := newUpgradeFixture(t)
	f.node.ActiveAttempts = []l1.NodeActiveAttempt{{JobID: "end-at-once", AttemptID: "active", Class: contract.JobClassOneShot}}
	wait := f.operations.wait
	f.operations.wait = func(ctx context.Context, d time.Duration) error {
		if d == upgradeDrainPoll {
			t.Fatal("--yes waited for one-shots")
		}
		return wait(ctx, d)
	}
	if _, err := runUpgrade(t.Context(), f.host, true, io.Discard); err != nil {
		t.Fatal(err)
	}
	if !f.replaced || !f.node.ClaimsEnabled {
		t.Fatal("replacement or claims restoration missing")
	}
}

func TestUpgradeDrainPreservesOperatorIntent(t *testing.T) {
	for _, change := range []string{"pre-existing", "new-disable", "new-enable", "restore-race"} {
		t.Run(change, func(t *testing.T) {
			f := newUpgradeFixture(t)
			if change == "pre-existing" {
				f.node.ClaimsEnabled = false
				f.node.IntentRevision = 8
				f.node.IntentReason = "operator maintenance"
			}
			f.node.ActiveAttempts = []l1.NodeActiveAttempt{{JobID: "finishes", AttemptID: "active", Class: contract.JobClassOneShot}}
			wait := f.operations.wait
			f.operations.wait = func(ctx context.Context, d time.Duration) error {
				if d == upgradeDrainPoll {
					f.node.ActiveAttempts = nil
					if change == "new-disable" || change == "new-enable" {
						f.node.IntentRevision++
						f.node.IntentReason = "new operator decision"
						f.node.ClaimsEnabled = change == "new-enable"
					}
				}
				return wait(ctx, d)
			}
			set := f.operations.setClaims
			raced := false
			f.operations.setClaims = func(ctx context.Context, p connprofile.Profile, request l1.NodeIntentRequest) (l1.Node, error) {
				if request.ClaimsEnabled && change == "restore-race" && !raced {
					raced = true
					f.node.IntentRevision++
					f.node.IntentReason = "new operator decision"
					f.node.ClaimsEnabled = false
				}
				return set(ctx, p, request)
			}
			_, err := runUpgrade(t.Context(), f.host, false, io.Discard)
			if strings.HasPrefix(change, "new-") {
				if err == nil || len(f.launchd.mutating()) != 0 {
					t.Fatalf("new operator decision did not stop activation: %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if change == "pre-existing" {
				if f.node.ClaimsEnabled || f.node.IntentRevision != 8 || f.node.IntentReason != "operator maintenance" {
					t.Fatal("pre-existing disable was changed")
				}
			} else if f.node.IntentReason != "new operator decision" || f.node.ClaimsEnabled != (change == "new-enable") {
				t.Fatalf("new intent overwritten: %+v", f.node)
			}
			assertNoUpgradeDrainReceipt(t, f)
		})
	}
}

func assertNoUpgradeDrainReceipt(t *testing.T, f *upgradeFixture) {
	t.Helper()
	paths, _ := resolveSetupPaths(f.home)
	if _, err := os.Stat(filepath.Join(paths.config, upgradeDrainFile)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unexpected receipt: %v", err)
	}
}

func TestUpgradeDrainFailureAndCancelRestoreIntent(t *testing.T) {
	for _, failure := range []string{"renewal", "replacement", "ctrl-c-drain", "ctrl-c-activation"} {
		t.Run(failure, func(t *testing.T) {
			f := newUpgradeFixture(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if strings.HasPrefix(failure, "ctrl-c") {
				if failure == "ctrl-c-drain" {
					f.node.ActiveAttempts = []l1.NodeActiveAttempt{{JobID: "active", AttemptID: "active"}}
				}
				wait := f.operations.wait
				f.operations.wait = func(ctx context.Context, d time.Duration) error {
					if (failure == "ctrl-c-drain" && d == upgradeDrainPoll) || (failure == "ctrl-c-activation" && f.restarted) {
						cancel()
					}
					return wait(ctx, d)
				}
			} else {
				f.failure = failure
			}
			set := f.operations.setClaims
			restored := false
			f.operations.setClaims = func(ctx context.Context, p connprofile.Profile, request l1.NodeIntentRequest) (l1.Node, error) {
				if request.ClaimsEnabled {
					if ctx.Err() != nil {
						t.Fatal("cleanup used canceled context")
					}
					restored = true
				}
				return set(ctx, p, request)
			}
			if _, err := runUpgrade(ctx, f.host, false, io.Discard); err == nil {
				t.Fatal("expected failure")
			}
			if (!restored && failure != "ctrl-c-drain") || !f.node.ClaimsEnabled {
				t.Fatal("claims left disabled")
			}
			assertNoUpgradeDrainReceipt(t, f)
		})
	}
}

func TestUpgradeDrainReceiptRecoversBeforeNextBuild(t *testing.T) {
	f := newUpgradeFixture(t)
	f.failure = "replacement"
	set := f.operations.setClaims
	f.operations.setClaims = func(ctx context.Context, p connprofile.Profile, request l1.NodeIntentRequest) (l1.Node, error) {
		if request.ClaimsEnabled {
			return l1.Node{}, errors.New("L1 unavailable")
		}
		return set(ctx, p, request)
	}
	if _, err := runUpgrade(t.Context(), f.host, true, io.Discard); err == nil || !strings.Contains(err.Error(), "receipt kept") {
		t.Fatalf("missing retained receipt: %v", err)
	}
	paths, _ := resolveSetupPaths(f.home)
	if _, err := readUpgradeDrain(paths.config); err != nil {
		t.Fatal(err)
	}
	if f.node.ClaimsEnabled {
		t.Fatal("test did not leave a disabled gate")
	}
	f.operations.setClaims = set
	build := f.operations.build
	f.operations.build = func(ctx context.Context, a, b, c, d string) error {
		if !f.node.ClaimsEnabled {
			t.Fatal("next upgrade built before restoring receipt")
		}
		return build(ctx, a, b, c, d)
	}
	f.failure = "build"
	_, err := runUpgrade(t.Context(), f.host, false, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "build failed") {
		t.Fatal(err)
	}
	assertNoUpgradeDrainReceipt(t, f)
}

func TestUpgradeDrainReceiptPrecedesCASAndLostReplyRecovers(t *testing.T) {
	for _, lost := range []string{"disable", "restore"} {
		t.Run(lost, func(t *testing.T) {
			f := newUpgradeFixture(t)
			set := f.operations.setClaims
			injected := false
			f.operations.setClaims = func(ctx context.Context, p connprofile.Profile, request l1.NodeIntentRequest) (l1.Node, error) {
				paths, _ := resolveSetupPaths(f.home)
				receipt, err := readUpgradeDrain(paths.config)
				if err != nil {
					t.Fatal("intent mutation without durable receipt:", err)
				}
				if !request.ClaimsEnabled && receipt.Revision != request.IntentRevision+1 {
					t.Fatal("receipt does not name owned post-CAS revision")
				}
				node, err := set(ctx, p, request)
				if err == nil && !injected && request.ClaimsEnabled == (lost == "restore") {
					injected = true
					return l1.Node{}, errors.New("lost mutation response")
				}
				return node, err
			}
			_, err := runUpgrade(t.Context(), f.host, true, io.Discard)
			if lost == "disable" && err == nil {
				t.Fatal("lost disable reply not reported")
			}
			if lost == "restore" && err != nil {
				t.Fatal(err)
			}
			if !f.node.ClaimsEnabled {
				t.Fatal("lost reply left claims disabled")
			}
			assertNoUpgradeDrainReceipt(t, f)
		})
	}
}

func TestStatusNamesInterruptedUpgradeAndRecoveryCommand(t *testing.T) {
	f := newUpgradeFixture(t)
	paths, _ := resolveSetupPaths(f.home)
	f.node.ClaimsEnabled = false
	f.node.IntentRevision = 1
	f.node.IntentReason = "wefty upgrade interrupted"
	if err := writeUpgradeDrain(paths.config, &upgradeDrainReceipt{Home: f.home, Version: 1, NodeID: f.node.NodeID, Revision: 1, Reason: f.node.IntentReason, RestoreClaims: true}); err != nil {
		t.Fatal(err)
	}
	for _, active := range []bool{true, false} {
		t.Run(map[bool]string{true: "active", false: "interrupted"}[active], func(t *testing.T) {
			if active {
				unlock, err := lockSetup(paths, false)
				if err != nil {
					t.Fatal(err)
				}
				defer unlock()
			}
			h := newStatusHarness(t)
			h.nodes = l1.NodeList{Nodes: []l1.Node{f.node}}
			clients := h.clients()
			clients.nodeConfigPath = paths.nodeConfig
			var output bytes.Buffer
			err := executeStatus(t.Context(), clients, false, nil, &output, io.Discard)
			if active {
				if err != nil || !strings.Contains(output.String(), upgradeBusyMessage) {
					t.Fatalf("active drain: %v %s", err, output.String())
				}
			} else {
				if !strings.Contains(output.String(), "claims disabled by an interrupted upgrade") || !strings.Contains(output.String(), "wefty upgrade") {
					t.Fatalf("missing recovery instruction: %v %s", err, output.String())
				}
			}
		})
	}
}

func TestUpgradeDrainInterruptedAfterShutdownBeforeNewBootRecoversNeverService(t *testing.T) {
	f := newUpgradeFixture(t)
	f.node.ClaimsEnabled = false
	f.node.IntentRevision = 1
	f.node.IntentReason = "wefty upgrade crashed-after-shutdown"
	f.node.State = contract.NodeDraining
	paths, _ := resolveSetupPaths(f.home)
	receipt := &upgradeDrainReceipt{Home: f.home, Version: 1, NodeID: f.node.NodeID, Revision: 1, Reason: f.node.IntentReason, RestoreClaims: true, OldBoot: f.node.BootSessionID, Services: []l1.NodeActiveAttempt{{JobID: "never-app", AttemptID: "old-attempt", Class: contract.JobClassService}}}
	if err := writeUpgradeDrain(paths.config, receipt); err != nil {
		t.Fatal(err)
	}
	f.operations.readJob = func(context.Context, connprofile.Profile, string) (l1.Job, error) {
		return l1.Job{JobID: "never-app", State: contract.JobFailed, CurrentAttemptID: "old-attempt", ServiceJob: &l1.ServiceJob{DesiredState: contract.ServiceDesiredRunning, BoundNodeID: f.node.NodeID}}, nil
	}
	restarted := false
	f.operations.restartService = func(context.Context, connprofile.Profile, string, string) (l1.Job, error) {
		restarted = true
		return l1.Job{}, nil
	}
	f.failure = "build"
	_, err := runUpgrade(t.Context(), f.host, false, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "build failed") {
		t.Fatal(err)
	}
	if !restarted || !f.node.ClaimsEnabled {
		t.Fatal("interrupted never service or intent not recovered before new build")
	}
	assertNoUpgradeDrainReceipt(t, f)
}

func TestUpgradeDrainTimeoutFlagControlsBoundAndProgress(t *testing.T) {
	f := newUpgradeFixture(t)
	f.node.ActiveAttempts = []l1.NodeActiveAttempt{{JobID: "long", AttemptID: "long"}}
	var output bytes.Buffer
	started := f.now
	_, err := runUpgrade(t.Context(), f.host, false, &output, 12*time.Second)
	if err == nil || commandExitCode(err) != exitConflict {
		t.Fatal(err)
	}
	if f.now.Sub(started) != 12*time.Second || strings.Count(output.String(), "Draining:") != 3 {
		t.Fatalf("wrong bound or 5s cadence: elapsed=%s output=%s", f.now.Sub(started), output.String())
	}
}

func TestStatusInterruptedUpgradeThroughConfinedConfigLink(t *testing.T) {
	f := newUpgradeFixture(t)
	paths, _ := resolveSetupPaths(f.home)
	f.node.ClaimsEnabled = false
	f.node.IntentRevision = 1
	f.node.IntentReason = "wefty upgrade interrupted-linked-config"
	if err := writeUpgradeDrain(paths.config, &upgradeDrainReceipt{Home: f.home, Version: 1, NodeID: f.node.NodeID, Revision: 1, Reason: f.node.IntentReason, RestoreClaims: true}); err != nil {
		t.Fatal(err)
	}
	redirected := filepath.Join(f.home, ".config", "wefty-linked")
	if err := os.Rename(paths.config, redirected); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("wefty-linked", paths.config); err != nil {
		t.Fatal(err)
	}
	h := newStatusHarness(t)
	h.nodes = l1.NodeList{Nodes: []l1.Node{f.node}}
	clients := h.clients()
	clients.nodeConfigPath = paths.nodeConfig
	var output bytes.Buffer
	err := executeStatus(t.Context(), clients, false, nil, &output, io.Discard)
	if err == nil || commandExitCode(err) != exitNotReady || !strings.Contains(output.String(), "claims disabled by an interrupted upgrade") {
		t.Fatalf("linked config hid interrupted upgrade: %v %s", err, output.String())
	}
}
