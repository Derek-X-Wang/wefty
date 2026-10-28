//go:build darwin || linux

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/agentinstall"
	"github.com/Derek-X-Wang/wefty/l1"
	"golang.org/x/sys/unix"
)

func reviewReceipt(t *testing.T, phase string) (setupPaths, *upgradeRollingReceipt) {
	t.Helper()
	home, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	paths, _ := resolveSetupPaths(home)
	if err := os.MkdirAll(paths.config, 0700); err != nil {
		t.Fatal(err)
	}
	// A valid free companion is positive evidence of an inactive upgrader.
	release, err := lockSetup(paths, true)
	if err != nil {
		t.Fatal(err)
	}
	release()
	r := &upgradeRollingReceipt{Version: 1, OperationID: "review-operation", NodeID: "node", Candidate: agentinstall.Generation{ID: "primary"}}
	if err := writeUpgradeRolling(paths, r, phase); err != nil {
		t.Fatal(err)
	}
	return paths, r
}
func TestReviewMissingEnrolledObservations(t *testing.T) {
	h := newStatusHarness(t)
	h.nodes = generationStatusFixture(time.Now(), 0)
	h.nodes.Nodes[0].Generations = nil
	got := collectStatus(t.Context(), h.clients())
	if got.Ready || got.UpgradeState != "unverified" {
		t.Fatalf("enrolled Node without observations became ready: %+v", got)
	}
	reviewDiagnosticOutputs(t, got.rollingStatusFields, "unverified", "")
}
func TestReviewDiagnosticCatalogContention(t *testing.T) {
	paths, _ := reviewReceipt(t, "complete")
	for _, p := range []string{paths.launchAgents, filepath.Join(paths.data, agentinstall.Directory)} {
		if err := os.MkdirAll(p, 0700); err != nil {
			t.Fatal(err)
		}
	}
	directory, err := os.Open(filepath.Join(paths.data, agentinstall.Directory))
	if err != nil {
		t.Fatal(err)
	}
	defer directory.Close()
	if err := unix.Flock(int(directory.Fd()), unix.LOCK_EX); err != nil {
		t.Fatal(err)
	}
	done := make(chan rollingStatusFields, 1)
	go func() {
		ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		defer cancel()
		done <- collectRollingStatus(ctx, paths.nodeConfig, generationStatusFixture(time.Now(), 1), true, time.Now())
	}()
	select {
	case got := <-done:
		_ = unix.Flock(int(directory.Fd()), unix.LOCK_UN)
		if got.UpgradeState != "unverified" || len(got.Generations) != 2 || !strings.Contains(strings.Join(got.EvidenceErrors, " "), unix.EWOULDBLOCK.Error()) {
			t.Fatalf("%+v", got)
		}
		reviewDiagnosticOutputs(t, got, "catalog", "")
	case <-time.After(300 * time.Millisecond):
		_ = unix.Flock(int(directory.Fd()), unix.LOCK_UN)
		<-done // Release the baseline's blocking reader before reporting failure.
		t.Fatal("status exceeded the caller's catalog retry budget")
	}
}
func TestReviewUnknownUpgradeLock(t *testing.T) {
	for _, kind := range []string{"missing", "nonregular", "unreadable"} {
		t.Run(kind, func(t *testing.T) {
			paths, _ := reviewReceipt(t, "candidate_loading")
			probe := filepath.Join(paths.data, setupLockProbeFile)
			if kind == "unreadable" {
				if err := os.Chmod(probe, 0000); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := os.Remove(probe); err != nil {
					t.Fatal(err)
				}
				if kind == "nonregular" {
					if err := os.Mkdir(probe, 0700); err != nil {
						t.Fatal(err)
					}
				}
			}
			got := collectRollingStatus(t.Context(), paths.nodeConfig, generationStatusFixture(time.Now(), 0), true, time.Now())
			if got.UpgradeState != "unverified" || !strings.Contains(strings.Join(got.EvidenceErrors, " "), "lock") {
				t.Fatalf("unknown lock became interruption without cause: %+v", got)
			}
			reviewDiagnosticOutputs(t, got, "lock", "interrupted")
		})
	}
}

func TestReviewMissingAttemptRetainsRebootInterruption(t *testing.T) {
	paths, r := reviewReceipt(t, "reboot_reconciled")
	r.RebootAttempts = map[string]l1.Attempt{"attempt-0": {State: contract.AttemptInterrupted}}
	if err := writeUpgradeRolling(paths, r, "reboot_reconciled"); err != nil {
		t.Fatal(err)
	}
	nodes := generationStatusFixture(time.Now(), 1)
	nodes.Nodes[0].Generations[1].Attempts[0].State = nil
	got := collectRollingStatus(t.Context(), paths.nodeConfig, nodes, true, time.Now())
	if len(got.Generations) != 2 || got.Generations[1].RenewalHealth != "unverified" {
		t.Fatalf("current uncertainty lost: %+v", got)
	}
	if got.Generations[1].Attempts[0].State != nil {
		t.Fatal("receipt overwrote unknown current state")
	}
	if !strings.Contains(string(mustReviewJSON(t, got)), "interrupted") {
		t.Fatalf("known interruption disappeared: %+v", got)
	}
	reviewDiagnosticOutputs(t, got, "interrupted", "interrupted ()")
}
func TestReviewInterruptionWithoutEmptyReason(t *testing.T) {
	for _, historical := range []bool{false, true} {
		t.Run(map[bool]string{false: "current", true: "receipt"}[historical], func(t *testing.T) {
			nodes := generationStatusFixture(time.Now(), 1)
			config := ""
			if historical {
				paths, r := reviewReceipt(t, "reboot_reconciled")
				config = paths.nodeConfig
				r.RebootAttempts = map[string]l1.Attempt{"historical": {State: contract.AttemptInterrupted}}
				r.Inventory.Generations = []l1.NodeGeneration{{AgentGenerationID: "gone", ResidentAttempts: []l1.ResidentAttempt{{JobID: "job-history", AttemptID: "historical"}}}}
				if err := writeUpgradeRolling(paths, r, "reboot_reconciled"); err != nil {
					t.Fatal(err)
				}
			} else {
				nodes.Nodes[0].Generations[1].Attempts[0].State = generationAttemptState(contract.AttemptInterrupted)
			}
			got := collectRollingStatus(t.Context(), config, nodes, true, time.Now())
			reviewDiagnosticOutputs(t, got, "interrupted", "interrupted ()")
		})
	}
}
func mustReviewJSON(t *testing.T, value any) []byte {
	t.Helper()
	b, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestReviewReceiptRevalidatedAfterFreeLock(t *testing.T) {
	for _, phase := range []string{"complete", "rolled_back", "candidate_ready"} {
		t.Run(phase, func(t *testing.T) {
			paths, original := reviewReceipt(t, "candidate_loading")
			latest := *original
			next, active, err := revalidateRollingCoordinator(paths, original, func(setupPaths) setupLockObservation {
				if err := writeUpgradeRolling(paths, &latest, phase); err != nil {
					t.Fatal(err)
				}
				return setupLockObservation{state: setupLockFree}
			})
			if err == nil || active || next.Phase != phase {
				t.Fatalf("completed or changed operation became interrupted: next=%+v active=%t err=%v", next, active, err)
			}
		})
	}
	t.Run("replacement-operation", func(t *testing.T) {
		paths, original := reviewReceipt(t, "candidate_loading")
		_, _, err := revalidateRollingCoordinator(paths, original, func(setupPaths) setupLockObservation {
			replacement := *original
			replacement.OperationID = "replacement"
			if err := writeUpgradeRolling(paths, &replacement, "candidate_loading"); err != nil {
				t.Fatal(err)
			}
			return setupLockObservation{state: setupLockFree}
		})
		if err == nil {
			t.Fatal("different operation treated as interrupted original")
		}
	})
}

func TestReviewObservationCauseRendersUnknown(t *testing.T) {
	nodes := generationStatusFixture(time.Now(), 1)
	a := &nodes.Nodes[0].Generations[1].Attempts[0]
	a.State = nil
	a.ObservationError = "Job job-0 (Attempt attempt-0): unreadable retained Job spec: unexpected end of JSON input"
	got := collectRollingStatus(t.Context(), "", nodes, true, time.Now())
	reviewDiagnosticOutputs(t, got, "unreadable retained Job spec", "")
}

func TestReviewDoctorLabelAndRetirementReadsRefuseContention(t *testing.T) {
	paths, _ := reviewReceipt(t, "complete")
	root := filepath.Join(paths.data, agentinstall.Directory)
	for _, p := range []string{root, paths.launchAgents} {
		if err := os.MkdirAll(p, 0700); err != nil {
			t.Fatal(err)
		}
	}
	directory, err := os.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	defer directory.Close()
	if err := unix.Flock(int(directory.Fd()), unix.LOCK_EX); err != nil {
		t.Fatal(err)
	}
	defer unix.Flock(int(directory.Fd()), unix.LOCK_UN)
	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		defer cancel()
		label, enrolled, labelErr := plainDoctorAgentLabel(ctx, setupHost{home: paths.home})
		if label != "" || !enrolled || labelErr == nil || !strings.Contains(labelErr.Error(), unix.EWOULDBLOCK.Error()) {
			done <- fmt.Errorf("unknown primary label called legacy: %q %t", label, enrolled)
			return
		}
		_, err := (agentinstall.Store{Data: paths.data, LaunchAgents: paths.launchAgents}).ReadRetirementDiagnostic(strings.Repeat("a", 32))
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "temporarily unavailable") {
			t.Fatalf("missing contention cause: %v", err)
		}
	case <-time.After(300 * time.Millisecond):
		_ = unix.Flock(int(directory.Fd()), unix.LOCK_UN)
		<-done
		t.Fatal("doctor label or retirement acknowledgment exceeded the caller's retry budget")
	}
}

func TestReviewPayloadExitRequiresSnapshotAuthority(t *testing.T) {
	for _, tc := range []struct {
		name, initiator, current string
		recorded, problem        bool
		canceled                 bool
		recordedState            contract.AttemptState
	}{
		{name: "payload-own"},
		{name: "agent-initiated", initiator: "agent", problem: true},
		{name: "guardian-initiated", initiator: "guardian", problem: true},
		{name: "owner-canceled", initiator: "agent", canceled: true},
		{name: "receipt-only", recorded: true, current: "absent", problem: true},
		{name: "receipt-current-payload-own", recorded: true},
		{name: "receipt-current-owner-canceled", recorded: true, initiator: "agent", canceled: true},
		{name: "receipt-loss-stays-visible", recorded: true, recordedState: contract.AttemptLost, problem: true},
		{name: "receipt-current-agent", recorded: true, initiator: "agent", problem: true},
		{name: "receipt-current-unknown", recorded: true, current: "unknown", problem: true},
		{name: "receipt-current-live", recorded: true, current: "live", problem: true},
		{name: "receipt-other-node", recorded: true, current: "other-node", problem: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now()
			nodes := generationStatusFixture(now, 1)
			a := &nodes.Nodes[0].Generations[1].Attempts[0]
			exit := 17
			a.State = generationAttemptState(contract.AttemptFailed)
			a.ExitCode = &exit
			if tc.canceled {
				a.JobOutcome = contract.JobOutcomeCanceled
			}
			payload, err := json.Marshal(nodes)
			if err != nil {
				t.Fatal(err)
			}
			if tc.initiator != "" {
				payload = []byte(strings.Replace(string(payload), `"exit_code":17`, `"exit_code":17,"termination_initiator":"`+tc.initiator+`"`, 1))
			}
			if err := json.Unmarshal(payload, &nodes); err != nil {
				t.Fatal(err)
			}
			config := ""
			if tc.recorded {
				paths, r := reviewReceipt(t, "reboot_reconciled")
				config = paths.nodeConfig
				state := contract.AttemptFailed
				if tc.recordedState != "" {
					state = tc.recordedState
				}
				r.RebootAttempts = map[string]l1.Attempt{"attempt-0": {State: state, Result: &l1.ProcessResult{ExitCode: &exit}}}
				r.Inventory.Generations = []l1.NodeGeneration{{AgentGenerationID: "old-0", ResidentAttempts: []l1.ResidentAttempt{{JobID: "job-0", AttemptID: "attempt-0"}}}}
				if err := writeUpgradeRolling(paths, r, "reboot_reconciled"); err != nil {
					t.Fatal(err)
				}
			}
			switch tc.current {
			case "absent":
				nodes.Nodes[0].Generations = nodes.Nodes[0].Generations[:1]
			case "unknown":
				nodes.Nodes[0].Generations[1].Attempts[0].State = nil
			case "live":
				nodes.Nodes[0].Generations[1].Attempts[0].State = generationAttemptState(contract.AttemptRunning)
			case "other-node":
				other := nodes.Nodes[0]
				other.NodeID = "other-node"
				nodes.Nodes = append(nodes.Nodes, other)
				nodes.Nodes[0].Generations = nodes.Nodes[0].Generations[:1]
			}
			got := collectRollingStatus(t.Context(), config, nodes, true, now)
			foundProblem := false
			for _, g := range got.Generations {
				for _, p := range g.Problems {
					foundProblem = foundProblem || p.Code == "retiring_attempt_problem"
				}
			}
			if foundProblem != tc.problem {
				t.Fatalf("retiring_attempt_problem=%t want=%t projection=%s", foundProblem, tc.problem, mustReviewJSON(t, got))
			}
		})
	}
}
