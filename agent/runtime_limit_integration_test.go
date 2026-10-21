//go:build darwin || linux

package agent

import (
	"context"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	processrunner "github.com/Derek-X-Wang/wefty/runner/process"
)

// Force the existing Guardian path for a one-shot as well as the plain runner.
// Both still execute a real payload and publish through the production agent.
type guardedLimitExecutor struct{ processrunner.Executor }

func (executor guardedLimitExecutor) Run(ctx context.Context, request processrunner.Request, sink processrunner.OutputSink) (contract.ProcessResult, error) {
	request.Guarded = true
	return executor.Executor.Run(ctx, request, sink)
}

func TestProcessReachedLimitFailsThroughL1(t *testing.T) {
	for _, guarded := range []bool{false, true} {
		for _, limit := range []string{"max_runtime", "idle_timeout", "unset"} {
			name := limit
			if guarded {
				name += "_guardian"
			}
			t.Run(name, func(t *testing.T) {
				network := plain.NewNetwork()
				_, stop := startFailureServer(t, network, nil, map[string][]string{"limit-node": {"limits"}})
				defer stop()
				client, err := NewClient(network.NewFabric(fabric.Identity{NodeID: "app", Tags: []string{l1.DefaultClientPrincipalTag}}), "wefty://control-plane")
				if err != nil {
					t.Fatal(err)
				}
				defer client.Close()
				script := `trap 'exit 0' TERM; while :; do sleep 0.05; done`
				var limits *contract.JobLimits
				wantState := contract.JobFailed
				switch limit {
				case "max_runtime":
					limits = &contract.JobLimits{MaxRuntimeSeconds: 1}
				case "idle_timeout":
					limits = &contract.JobLimits{IdleTimeoutSeconds: 1}
				case "unset":
					script = "exit 0"
					wantState = contract.JobSucceeded
				}
				var job l1.Job
				if err := client.post(t.Context(), "/v1/jobs", contract.JobSpec{SchemaVersion: 1, DispatchKey: name, Kind: "process", Class: "one-shot", RoutingTags: []string{"limits"}, Limits: limits,
					Execution: contract.ExecutionSpec{Executable: contract.ExecutableSpec{Path: "/bin/sh"}, Argv: []string{"sh", "-c", script}, WorkingDirectory: t.TempDir()}}, &job); err != nil {
					t.Fatal(err)
				}
				root, err := filepath.EvalSymlinks(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				var executor processrunner.Executor = processrunner.New(processrunner.Config{GuardianExecutable: agentBinaryPath})
				if guarded {
					executor = guardedLimitExecutor{executor}
				}
				node, err := New(Config{Fabric: network.NewFabric(fabric.Identity{NodeID: "agent", Tags: []string{l1.DefaultAgentPrincipalTag}}), ControlPlaneAddress: "wefty://control-plane", NodeID: "limit-node", BootSessionID: "boot", Version: "test", Capabilities: map[string]bool{"kind:process": true}, ManagedRootDirectory: root, LogSpoolDirectory: t.TempDir(),
					WorkloadRuntimes: map[string]WorkloadRuntime{"process": processrunner.NewAdapter(executor)}, HeartbeatInterval: 50 * time.Millisecond, ClaimInterval: 5 * time.Millisecond, RenewalInterval: 50 * time.Millisecond})
				if err != nil {
					t.Fatal(err)
				}
				defer node.Close()
				ctx, cancel := context.WithCancel(t.Context())
				done := make(chan error, 1)
				go func() { done <- node.Run(ctx) }()
				defer func() {
					cancel()
					if err := <-done; err != nil {
						t.Errorf("agent shutdown: %v", err)
					}
				}()
				// The wait returns as soon as the job ends. Its bound covers the 1 s
				// limit, the termination grace the stop may use, and a busy machine
				// running the full suite (#1093).
				deadline := time.Now().Add(time.Second + processrunner.DefaultTerminationGraceTime + 20*time.Second)
				var got l1.Job
				for {
					if err := client.request(t.Context(), http.MethodGet, "/v1/jobs/"+job.JobID, nil, &got); err != nil {
						t.Fatal(err)
					}
					if got.State == contract.JobFailed || got.State == contract.JobSucceeded {
						break
					}
					if time.Now().After(deadline) {
						t.Fatalf("job did not finish: %+v", got)
					}
					time.Sleep(5 * time.Millisecond)
				}
				if got.State != wantState {
					t.Errorf("job = %s, want %s", got.State, wantState)
				}
				if len(got.Attempts) != 1 || got.Attempts[0].Result == nil {
					t.Fatalf("attempts=%+v", got.Attempts)
				}
				attempt := got.Attempts[0]
				if attempt.Result.ExitCode == nil || *attempt.Result.ExitCode != 0 {
					t.Fatalf("payload exit was changed: %+v", attempt.Result)
				}
				if limit != "unset" {
					if attempt.State != contract.AttemptFailed {
						t.Errorf("limit attempt=%+v", attempt)
					}
					if attempt.Result.LimitReached != contract.LimitReached(limit) {
						t.Errorf("limit evidence=%+v, want %s", attempt.Result, limit)
					}
				}
			})
		}
	}
}

// Keep the limit stop beside a genuine finalization failure, which remains the
// primary outcome and must not retain the payload's exit code.
func TestLimitMarkerSurvivesOutputFinalizationFailure(t *testing.T) {
	for _, limit := range []contract.LimitReached{contract.LimitReachedMaxRuntime, contract.LimitReachedIdleTimeout} {
		t.Run(string(limit), func(t *testing.T) {
			lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{
				runtimes: workloadRuntimeSet{contract.JobKindProcess: limitFinalizationRuntime{limit: limit}},
				clock:    systemClock{}, finalizationTimeout: time.Second,
				logSinkFactory: func(context.Context, l1.Claim) (attemptLogSink, error) { return innerDeadlineLogSink{}, nil },
			})
			result, err := lifecycle.runWorkload(t.Context(), l1.Claim{
				Job:   l1.Job{JobID: "limit-finalization", Spec: contract.JobSpec{Kind: contract.JobKindProcess, Class: contract.JobClassOneShot}},
				Lease: l1.AttemptLease{AttemptID: "limit-finalization-attempt"},
			})
			if err == nil || result.OutputError == "" || result.ExitCode != nil || result.Signal != "" || result.LimitReached != limit {
				t.Fatalf("finalization result=%+v err=%v, want output_error plus %s", result, err, limit)
			}
			if wire := toL1Result(result); wire.OutputError == "" || wire.ExitCode != nil || wire.LimitReached != limit {
				t.Fatalf("completion lost evidence: %+v", wire)
			}
		})
	}
}

type limitFinalizationRuntime struct {
	instantWorkloadRuntime
	limit contract.LimitReached
}

func (runtime limitFinalizationRuntime) Run(context.Context, workloadrunner.Request, workloadrunner.OutputSink) (workloadrunner.Result, error) {
	zero := 0
	runErr := processrunner.ErrMaxRuntime
	if runtime.limit == contract.LimitReachedIdleTimeout {
		runErr = processrunner.ErrIdleTimeout
	}
	return workloadrunner.Result{Outcome: contract.ProcessResult{ExitCode: &zero, TerminationInitiator: contract.TerminationCauseAgent, LimitReached: runtime.limit}}, runErr
}
