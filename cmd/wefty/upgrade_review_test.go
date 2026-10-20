package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestUpgradeServiceRefusalRecovery(t *testing.T) {
	for _, failure := range []string{"capacity", "missing", "transient"} {
		t.Run(failure, func(t *testing.T) {
			f := newUpgradeFixture(t)
			paths, _ := resolveSetupPaths(f.home)
			receipt := &upgradeDrainReceipt{Home: f.home, Version: 1, NodeID: f.node.NodeID, Revision: 1, Reason: "wefty upgrade refused", RestoreClaims: true, OldBoot: "previous", Services: []l1.NodeActiveAttempt{{JobID: "refused-app", AttemptID: "old"}, {JobID: "healthy-app", AttemptID: "other"}}}
			f.node.ClaimsEnabled = false
			f.node.IntentRevision = 1
			f.node.IntentReason = receipt.Reason
			if err := writeUpgradeDrain(paths.config, receipt); err != nil {
				t.Fatal(err)
			}
			restarted := false
			var capacityErr error
			if failure == "capacity" {
				capacityErr = realUpgradeCapacityRefusal(t)
			}
			f.operations.readJob = func(_ context.Context, _ connprofile.Profile, id string) (l1.Job, error) {
				if id == "refused-app" && failure == "missing" {
					return l1.Job{}, &apiResponseError{StatusCode: 404, APIError: contract.APIError{Code: contract.ErrorNotFound}}
				}
				return l1.Job{JobID: id, ServiceJob: &l1.ServiceJob{BoundNodeID: f.node.NodeID, DesiredState: contract.ServiceDesiredRunning}}, nil
			}
			f.operations.restartService = func(_ context.Context, _ connprofile.Profile, id, key string) (l1.Job, error) {
				if id == "refused-app" {
					if failure == "transient" {
						return l1.Job{}, &apiResponseError{StatusCode: 503, APIError: contract.APIError{Code: contract.ErrorUnavailable, Retryable: true}}
					}
					return l1.Job{}, capacityErr
				}
				restarted = true
				// The refused service must already have been durably dropped.
				saved, err := readUpgradeDrain(paths.config)
				if err != nil || len(saved.Services) != 1 || saved.Services[0].JobID != "healthy-app" {
					t.Fatalf("refusal not persisted: %+v %v", saved, err)
				}
				return l1.Job{}, nil
			}
			var out bytes.Buffer
			profile, _ := connprofile.Read(paths.profile)
			var err error
			if failure == "transient" {
				err = finishUpgradeDrain(t.Context(), *f.operations, paths, profile, receipt, false, &out)
			} else {
				err = recoverUpgradeDrain(t.Context(), *f.operations, paths, profile, &out)
			}
			if failure == "transient" {
				if err == nil {
					t.Fatal("transient refusal discarded")
				}
				if _, err := readUpgradeDrain(paths.config); err != nil {
					t.Fatal("transient receipt lost", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !restarted || !strings.Contains(out.String(), "wefty services restart refused-app --idempotency-key") {
				t.Fatalf("missing continuation/manual restart: %s", out.String())
			}
			assertNoUpgradeDrainReceipt(t, f)
			f.failure = "build"
			_, err = runUpgrade(t.Context(), f.host, false, io.Discard)
			if err == nil || !strings.Contains(err.Error(), "build failed") {
				t.Fatalf("next upgrade blocked: %v", err)
			}
		})
	}
}

func TestStatusUpgradeRestartWindow(t *testing.T) {
	for _, state := range []string{"l1-down", "l3-down", "draining"} {
		for _, evidence := range []string{"active", "unlocked", "lock-only"} {
			t.Run(state+"-"+evidence, func(t *testing.T) {
				active := evidence != "unlocked"
				f := newUpgradeFixture(t)
				paths, _ := resolveSetupPaths(f.home)
				f.node.ClaimsEnabled = false
				f.node.IntentRevision = 1
				f.node.IntentReason = "wefty upgrade restart-window"
				now := time.Now()
				f.node.IntentUpdatedAt = &now
				receipt := &upgradeDrainReceipt{Home: f.home, Version: 1, NodeID: f.node.NodeID, Revision: 1, Reason: f.node.IntentReason, RestoreClaims: true}
				if evidence != "lock-only" {
					if err := writeUpgradeDrain(paths.config, receipt); err != nil {
						t.Fatal(err)
					}
				}
				if active {
					unlock, err := lockSetup(paths, false)
					if err != nil {
						t.Fatal(err)
					}
					defer unlock()
				}
				h := newStatusHarness(t)
				h.l1Down = state == "l1-down"
				h.l3Down = state == "l3-down"
				if state == "draining" {
					f.node.State = contract.NodeDraining
				}
				h.nodes = l1.NodeList{Nodes: []l1.Node{f.node}}
				clients := h.clients()
				clients.nodeConfigPath = paths.nodeConfig
				var out bytes.Buffer
				err := executeStatus(t.Context(), clients, false, nil, &out, io.Discard)
				if !active || evidence == "lock-only" {
					if err == nil || commandExitCode(err) != exitNotReady {
						t.Fatalf("unlocked receipt became ready: %v %s", err, out.String())
					}
					return
				}
				want := "busy: upgrading; retry in a few seconds"
				if state == "draining" {
					want = upgradeBusyMessage
				}
				if err != nil || !strings.Contains(out.String(), want) {
					t.Fatalf("restart window not busy: %v %s", err, out.String())
				}
			})
		}
	}
}

func TestUpgradeRechecksBeforeNodeReplacement(t *testing.T) {
	for _, race := range []string{"operator-enables", "operator-changes-disable", "late-one-shot"} {
		t.Run(race, func(t *testing.T) {
			f := newUpgradeFixture(t)
			if race == "operator-changes-disable" {
				f.node.ClaimsEnabled = false
				f.node.IntentRevision = 7
				f.node.IntentReason = "maintenance"
			}
			before := installedUpgradeFiles(t, f)
			observe := f.operations.observe
			injected := false
			f.operations.observe = func(ctx context.Context, p connprofile.Profile) (l1.Node, error) {
				if f.restarted && !injected {
					injected = true
					if race == "late-one-shot" {
						f.node.ActiveAttempts = []l1.NodeActiveAttempt{{JobID: "late", AttemptID: "late", Class: contract.JobClassOneShot}}
					} else {
						f.node.IntentRevision++
						f.node.IntentReason = "operator changed intent"
						f.node.ClaimsEnabled = race == "operator-enables"
					}
				}
				return observe(ctx, p)
			}
			_, err := runUpgrade(t.Context(), f.host, false, io.Discard)
			if err == nil || commandExitCode(err) != exitConflict || !strings.Contains(err.Error(), "before node replacement") {
				t.Fatalf("race not refused: %v", err)
			}
			if f.replaced || !reflect.DeepEqual(before, installedUpgradeFiles(t, f)) {
				t.Fatal("race replaced node or installation")
			}
			if race == "late-one-shot" && (!f.node.ClaimsEnabled || len(f.node.ActiveAttempts) != 1) {
				t.Fatal("late run not preserved/claims not restored")
			}
			assertNoUpgradeDrainReceipt(t, f)
		})
	}
}

func TestStatusProbeDoesNotContendWithUpgrade(t *testing.T) {
	f := newUpgradeFixture(t)
	paths, _ := resolveSetupPaths(f.home)
	receipt := &upgradeDrainReceipt{Home: f.home, Version: 1, NodeID: f.node.NodeID, Reason: "wefty upgrade probe"}
	if err := writeUpgradeDrain(paths.config, receipt); err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	group.Add(1)
	stop := make(chan struct{})
	defer func() { close(stop); group.Wait() }()
	go func() {
		defer group.Done()
		for {
			select {
			case <-stop:
				return
			default:
				localUpgradeStatus(paths.nodeConfig)
			}
		}
	}()
	for i := 0; i < 3000; i++ {
		unlock, err := lockSetup(paths, false)
		if err != nil {
			t.Fatalf("status blocked upgrade: %v", err)
		}
		unlock()
	}
}

func TestUpgradeLateDisableReplyIsFenced(t *testing.T) {
	for _, winner := range []string{"disable", "fence"} {
		t.Run(winner, func(t *testing.T) {
			f := newUpgradeFixture(t)
			set := f.operations.setClaims
			var pending *l1.NodeIntentRequest
			f.operations.setClaims = func(ctx context.Context, p connprofile.Profile, r l1.NodeIntentRequest) (l1.Node, error) {
				if !r.ClaimsEnabled {
					pending = &r
					return l1.Node{}, errors.New("reply lost before commit")
				}
				// Match the real intent API's nonempty reason validation.
				if strings.TrimSpace(r.Reason) == "" {
					return l1.Node{}, &apiResponseError{StatusCode: 400, APIError: contract.APIError{Code: contract.ErrorInvalidRequest, Message: "reason required"}}
				}
				return set(ctx, p, r)
			}
			read := f.operations.readNode
			injected := false
			f.operations.readNode = func(ctx context.Context, p connprofile.Profile) (l1.Node, error) {
				node, err := read(ctx, p)
				if winner == "disable" && pending != nil && !injected {
					injected = true
					if _, err := set(ctx, p, *pending); err != nil {
						t.Fatal(err)
					}
				}
				return node, err
			}
			_, err := runUpgrade(t.Context(), f.host, false, io.Discard)
			if err == nil || !strings.Contains(err.Error(), "reply lost") {
				t.Fatal(err)
			}
			if winner == "fence" {
				if _, err := set(t.Context(), connprofile.Profile{}, *pending); err == nil {
					t.Fatal("late disable CAS was not fenced")
				}
			}
			if !f.node.ClaimsEnabled {
				t.Fatal("late disable committed after cleanup and stranded claims")
			}
			assertNoUpgradeDrainReceipt(t, f)
		})
	}
}

func TestStatusUpgradeReasonExpiresWithoutReceipt(t *testing.T) {
	for _, age := range []time.Duration{time.Minute, time.Hour} {
		t.Run(age.String(), func(t *testing.T) {
			f := newSetupFixture(t)
			f.setup(t, setupRequest{})
			paths, _ := resolveSetupPaths(f.home)
			if err := os.WriteFile(filepath.Join(paths.config, upgradeDrainFile), []byte("broken receipt"), 0600); err != nil {
				t.Fatal(err)
			}
			h := newStatusHarness(t)
			node := readyNode("aging")
			node.ClaimsEnabled = false
			node.IntentReason = "wefty upgrade unreadable receipt"
			updated := time.Now().Add(-age)
			node.IntentUpdatedAt = &updated
			h.nodes = l1.NodeList{Nodes: []l1.Node{node}}
			var out bytes.Buffer
			clients := h.clients()
			clients.nodeConfigPath = paths.nodeConfig
			err := executeStatus(t.Context(), clients, false, nil, &out, io.Discard)
			if age == time.Minute {
				if err != nil || !strings.Contains(out.String(), upgradeBusyMessage) {
					t.Fatal(err, out.String())
				}
				return
			}
			if err == nil || commandExitCode(err) != exitNotReady || !strings.Contains(out.String(), "claims disabled by an interrupted upgrade; run wefty upgrade") {
				t.Fatalf("stale drain hidden: %v %s", err, out.String())
			}
		})
	}
}

func TestUpgradeJSONIncludesRestoreFailure(t *testing.T) {
	f := newUpgradeFixture(t)
	f.node.ActiveAttempts = []l1.NodeActiveAttempt{{JobID: "long", AttemptID: "long"}}
	set := f.operations.setClaims
	f.operations.setClaims = func(ctx context.Context, p connprofile.Profile, r l1.NodeIntentRequest) (l1.Node, error) {
		if r.ClaimsEnabled {
			return l1.Node{}, &unavailableError{cause: errors.New("restore transport unavailable")}
		}
		return set(ctx, p, r)
	}
	_, err := runUpgrade(t.Context(), f.host, false, io.Discard, 10*time.Second)
	var out bytes.Buffer
	decideCommandReport(err).writeError(&out, true)
	if commandExitCode(err) != exitConflict || !strings.Contains(out.String(), "drain timed out") || !strings.Contains(out.String(), "restore upgrade intent failed") {
		t.Fatalf("JSON hid restore failure: %v %s", err, out.String())
	}
}

func TestUpgradeTextNamesEndedWorkAndDisabledServices(t *testing.T) {
	for _, disabled := range []bool{false, true} {
		t.Run(map[bool]string{true: "disabled", false: "enabled"}[disabled], func(t *testing.T) {
			f := newUpgradeFixture(t)
			f.node.ClaimsEnabled = !disabled
			if disabled {
				f.node.IntentReason = "operator maintenance"
			}
			f.node.ActiveAttempts = []l1.NodeActiveAttempt{{JobID: "end-this-job", AttemptID: "end-this-attempt", Class: contract.JobClassOneShot}}
			var out, warning bytes.Buffer
			err := executeUpgradeWithHost(t.Context(), f.host, globalOptions{}, true, &out, &warning)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(warning.String(), "end-this-job") || !strings.Contains(warning.String(), "end-this-attempt") {
				t.Fatalf("unnamed ended work: %s", warning.String())
			}
			if disabled && (strings.Contains(out.String(), "services come back") || !strings.Contains(out.String(), "claims remain disabled")) {
				t.Fatalf("misleading service summary: %s", out.String())
			}
		})
	}
}

func TestUpgradeDrainFirstReadHasIndependentBudget(t *testing.T) {
	f := newUpgradeFixture(t)
	paths, _ := resolveSetupPaths(f.home)
	profile, _ := connprofile.Read(paths.profile)
	receipt := &upgradeDrainReceipt{Revision: f.node.IntentRevision, Reason: f.node.IntentReason}
	f.node.ClaimsEnabled = false
	observe := f.operations.observe
	f.operations.observe = func(ctx context.Context, p connprofile.Profile) (l1.Node, error) {
		if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) < time.Second {
			return l1.Node{}, context.DeadlineExceeded
		}
		f.now = f.now.Add(time.Second)
		return observe(ctx, p)
	}
	var work []l1.NodeActiveAttempt
	if _, err := waitUpgradeDrain(t.Context(), *f.operations, profile, receipt, false, time.Nanosecond, io.Discard, &work); err != nil {
		t.Fatalf("idle first read expired: %v", err)
	}
}

func TestUpgradeDrainRejectsTinyTimeout(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	err := executeUpgrade(t.Context(), globalOptions{}, []string{"--drain-timeout=9s"}, io.Discard, io.Discard)
	if err == nil || commandExitCode(err) != exitUsage || !strings.Contains(err.Error(), "at least 10s") {
		t.Fatalf("tiny duration not usage: %v", err)
	}
}

// Use a second process so this exercises the kernel query as well as the local
// owner map. No probe is permitted to acquire or release the owner's lock.
func TestStatusSetupLockAcrossProcesses(t *testing.T) {
	if home := os.Getenv("WEFTY_TEST_LOCK_HOME"); home != "" {
		paths, err := resolveSetupPaths(home)
		if err != nil {
			t.Fatal(err)
		}
		unlock, err := lockSetup(paths, false)
		if err != nil {
			t.Fatal(err)
		}
		defer unlock()
		if _, err := io.WriteString(os.Stdout, "locked\n"); err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, os.Stdin)
		return
	}
	f := newUpgradeFixture(t)
	paths, _ := resolveSetupPaths(f.home)
	command := exec.Command(os.Args[0], "-test.run=^TestStatusSetupLockAcrossProcesses$")
	command.Env = append(os.Environ(), "WEFTY_TEST_LOCK_HOME="+f.home)
	input, err := command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	output, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var diagnostic bytes.Buffer
	command.Stderr = &diagnostic
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = input.Close(); _ = command.Wait() })
	scanner := bufio.NewScanner(output)
	if !scanner.Scan() || scanner.Text() != "locked" {
		t.Fatalf("child did not lock: %s %s", scanner.Text(), diagnostic.String())
	}
	for i := 0; i < 100; i++ {
		if !setupLockHeld(paths) {
			t.Fatal("status lost the live cross-process lock")
		}
	}
	if unlock, err := lockSetup(paths, false); err == nil {
		unlock()
		t.Fatal("status released upgrade lock")
	}
	if err := input.Close(); err != nil {
		t.Fatal(err)
	}
	if err := command.Wait(); err != nil {
		t.Fatal(err, diagnostic.String())
	}
	if setupLockHeld(paths) {
		t.Fatal("released lock still appears held")
	}
}
