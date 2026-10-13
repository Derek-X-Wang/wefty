//go:build darwin || linux

package process

import (
	"fmt"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestLimitMarkerRequiresDeliveredStop(t *testing.T) {
	zero, nonzero := 0, 7
	for _, limit := range []struct {
		err  error
		want contract.LimitReached
	}{{ErrMaxRuntime, contract.LimitReachedMaxRuntime}, {fmt.Errorf("wrapped: %w", ErrIdleTimeout), contract.LimitReachedIdleTimeout}} {
		for _, tc := range []struct {
			name      string
			result    contract.ProcessResult
			delivered bool
		}{
			{"self exit", contract.ProcessResult{ExitCode: &zero}, false},
			{"spontaneous signal", contract.ProcessResult{Signal: "terminated", TerminationCause: contract.TerminationCauseSpontaneous}, false},
			{"unknown signal cause", contract.ProcessResult{Signal: "terminated"}, false},
			{"missing result", contract.ProcessResult{}, false},
			{"wait error", spawnFailure(contract.SpawnFailureProcessWait, ErrProcessReapTimeout), false},
			{"wait error with initiator", contract.ProcessResult{SpawnError: &contract.SpawnFailure{Code: contract.SpawnFailureProcessWait, Message: "wait stalled"}, TerminationInitiator: contract.TerminationCauseAgent}, false},
			{"agent exit zero", contract.ProcessResult{ExitCode: &zero, TerminationInitiator: contract.TerminationCauseAgent}, true},
			{"guardian exit nonzero", contract.ProcessResult{ExitCode: &nonzero, TerminationInitiator: contract.TerminationCauseGuardian}, true},
			{"agent signal", contract.ProcessResult{Signal: "killed", TerminationCause: contract.TerminationCauseAgent}, true},
			{"guardian signal", contract.ProcessResult{Signal: "terminated", TerminationCause: contract.TerminationCauseGuardian}, true},
		} {
			t.Run(string(limit.want)+"/"+tc.name, func(t *testing.T) {
				result := tc.result
				markReachedLimit(&result, limit.err)
				want := contract.LimitReached("")
				if tc.delivered {
					want = limit.want
				}
				if result.LimitReached != want {
					t.Fatalf("result=%+v, want limit %q", result, want)
				}
			})
		}
	}
	t.Run("other stop", func(t *testing.T) {
		result := contract.ProcessResult{ExitCode: &zero, TerminationInitiator: contract.TerminationCauseAgent}
		markReachedLimit(&result, ErrCompletionTimeout)
		if result.LimitReached != "" {
			t.Fatalf("completion stop marked as limit: %+v", result)
		}
	})
}

// Pin the precompleted side of the race at the existing termination seam.
// Run's select between the exit and timer channels itself has no deterministic
// process-wait seam; the shared marker helper covers those result shapes above.
func TestLimitStopAfterReapedExitKeepsL1Succeeded(t *testing.T) {
	for _, limitErr := range []error{ErrMaxRuntime, ErrIdleTimeout} {
		t.Run(limitErr.Error(), func(t *testing.T) {
			command := exec.Command("/bin/sh", "-c", "exit 0")
			if err := configureProcessGroup(command); err != nil {
				t.Fatal(err)
			}
			if err := command.Run(); err != nil {
				t.Fatal(err)
			}
			wait := make(chan waitResult, 1)
			wait <- waitResult{state: command.ProcessState}
			result := New(Config{}).terminate(command.Process.Pid, wait)
			markReachedLimit(&result, limitErr)
			assertExitCode(t, result, 0)
			if result.TerminationInitiator != "" {
				t.Fatalf("self exit has initiator: %+v", result)
			}
			store, err := l1.OpenStore(filepath.Join(t.TempDir(), "l1.sqlite"), l1.StoreOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			_, err = store.RegisterNode(t.Context(), fabric.Identity{NodeID: "agent"}, contract.NodeRegistration{
				NodeID: "node", BootSessionID: "boot", OS: "linux", Architecture: "amd64", AgentVersion: "test",
				Capabilities: map[string]bool{"kind:process": true}, CapabilityRevision: 1, CapabilityObservedAt: time.Now(),
			}, l1.DefaultNodePolicy(), true)
			if err != nil {
				t.Fatal(err)
			}
			_, _, err = store.CreateJob(t.Context(), contract.JobSpec{SchemaVersion: 1, DispatchKey: "race", Kind: "process", Class: "one-shot", Execution: helperExecution("exit", "0"), Limits: &contract.JobLimits{MaxRuntimeSeconds: 1}})
			if err != nil {
				t.Fatal(err)
			}
			claim, err := store.ClaimJob(t.Context(), "agent", "node", "boot", "one-shot")
			if err != nil || claim == nil {
				t.Fatalf("claim=%+v err=%v", claim, err)
			}
			job, err := store.CompleteAttempt(t.Context(), "agent", claim.Job.JobID, claim.Lease.AttemptID, l1.CompletionRequest{
				FencingToken: claim.Lease.FencingToken, IdempotencyKey: "race", Result: l1.ProcessResult{ExitCode: result.ExitCode, LimitReached: result.LimitReached},
			})
			if err != nil || job.State != contract.JobSucceeded || result.LimitReached != "" {
				t.Fatalf("self exit result=%+v job=%+v err=%v", result, job, err)
			}
		})
	}
}

func TestLimitMarkerOmitsStalledProcessWait(t *testing.T) {
	clock := newFakeClock(time.Unix(4_000, 0))
	runner := New(Config{Clock: clock, TerminationGraceTime: time.Second, ProcessReapTimeout: 10 * time.Millisecond})
	done := make(chan contract.ProcessResult, 1)
	go func() { done <- runner.terminate(1<<30, make(chan waitResult)) }()
	clock.WaitForTimerCount(t, 1)
	clock.Advance(time.Second)
	select {
	case result := <-done:
		if result.SpawnError == nil || result.SpawnError.Code != contract.SpawnFailureProcessWait || result.SpawnError.Message != ErrProcessReapTimeout.Error() {
			t.Fatalf("wait result=%+v", result)
		}
		for _, limitErr := range []error{ErrMaxRuntime, ErrIdleTimeout} {
			marked := result
			markReachedLimit(&marked, limitErr)
			if marked.LimitReached != "" {
				t.Errorf("wait failure carries limit: %+v", marked)
			}
		}
	case <-time.After(5 * time.Second):
		t.Fatal("stalled wait did not return")
	}
}
