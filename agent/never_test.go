package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
	processrunner "github.com/Derek-X-Wang/wefty/runner/process"
)

func TestNeverProcessAcknowledgesRunnerStart(t *testing.T) {
	for _, refuse := range []bool{false, true} {
		t.Run(map[bool]string{false: "accepted", true: "refused"}[refuse], func(t *testing.T) {
			var acknowledged atomic.Bool
			client, closeServer := startEvidenceReplayServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/v1/agent/jobs/job/attempts/attempt/started" {
					t.Errorf("unexpected path %s", r.URL.Path)
				}
				var req l1.StartedRequest
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.FencingToken != "fence" {
					t.Errorf("start request = %+v %v", req, err)
				}
				acknowledged.Store(true)
				w.Header().Set("Content-Type", "application/json")
				if refuse {
					w.WriteHeader(http.StatusConflict)
					_ = json.NewEncoder(w).Encode(map[string]any{"error": contract.APIError{Code: contract.ErrorStaleFence, Message: "lost fence"}})
				} else {
					_ = json.NewEncoder(w).Encode(l1.Job{JobID: "job", State: contract.JobRunning})
				}
			}), time.Second)
			defer closeServer()
			defer client.Close()
			root, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			resource, err := initializeManagedResource(root, "node", "boot")
			if err != nil {
				t.Fatal(err)
			}
			executor := directiveContinuationRunner(func(ctx context.Context, req processrunner.Request, _ processrunner.OutputSink) (contract.ProcessResult, error) {
				if req.Started == nil {
					t.Fatal("process runner has no start acknowledgement hook")
				}
				if acknowledged.Load() {
					t.Fatal("acknowledged before runner start")
				}
				req.Started()
				if !acknowledged.Load() {
					t.Fatal("runner start did not reach L1")
				}
				if refuse {
					if ctx.Err() == nil {
						t.Fatal("refused acknowledgement left payload running")
					}
					return contract.ProcessResult{Signal: "terminated", TerminationCause: contract.TerminationCauseAgent}, ctx.Err()
				}
				zero := 0
				return contract.ProcessResult{ExitCode: &zero}, nil
			})
			lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{nodeID: "node", bootSessionID: "boot", managedResource: resource, client: client, runtimes: testRuntimeSet(executor), clock: systemClock{}, finalizationTimeout: time.Second})
			claim := l1.Claim{Job: l1.Job{JobID: "job", Spec: contract.JobSpec{Kind: "process", Class: "service", Restart: "never", Execution: contract.ExecutionSpec{Executable: contract.ExecutableSpec{Path: "/bin/true"}, Argv: []string{"true"}, WorkingDirectory: t.TempDir()}}}, Lease: l1.AttemptLease{AttemptID: "attempt", FencingToken: "fence"}}
			result, err := lifecycle.runWorkload(t.Context(), claim)
			if refuse {
				if err == nil || !strings.Contains(err.Error(), "acknowledge process start") || result.Signal == "" {
					t.Fatalf("refused start = %+v %v", result, err)
				}
			} else if err != nil || result.ExitCode == nil || *result.ExitCode != 0 {
				t.Fatalf("accepted start = %+v %v", result, err)
			}
		})
	}
}

// Only an answer without an L1 verdict is retried. A refusal cancels the
// payload on its first answer, even with a lease window left to spend.
func TestNeverProcessStartRetriesOnlyUndecidedAnswers(t *testing.T) {
	refusal := func(status int, code contract.ErrorCode) func(http.ResponseWriter) {
		return func(w http.ResponseWriter) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{Code: code, Message: string(code)}})
		}
	}
	accept := func(w http.ResponseWriter) {
		_ = json.NewEncoder(w).Encode(l1.Job{JobID: "job", State: contract.JobRunning})
	}
	for _, tc := range []struct {
		name    string
		answers []func(http.ResponseWriter)
		alive   bool
	}{
		{"internal then accepted", []func(http.ResponseWriter){refusal(http.StatusInternalServerError, contract.ErrorInternal), accept}, true},
		{"unavailable then accepted", []func(http.ResponseWriter){refusal(http.StatusServiceUnavailable, contract.ErrorRunLedgerUnavailable), accept}, true},
		{"stale fence", []func(http.ResponseWriter){refusal(http.StatusConflict, contract.ErrorStaleFence)}, false},
		{"lease expired", []func(http.ResponseWriter){refusal(http.StatusConflict, contract.ErrorLeaseExpired)}, false},
		{"pending cancellation", []func(http.ResponseWriter){refusal(http.StatusConflict, contract.ErrorConflict)}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			client, closeServer := startEvidenceReplayServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				call := int(calls.Add(1))
				if call > len(tc.answers) {
					t.Errorf("start acknowledgement call %d after a final answer", call)
					return
				}
				tc.answers[call-1](w)
			}), time.Second)
			defer closeServer()
			defer client.Close()
			root, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			resource, err := initializeManagedResource(root, "node", "boot")
			if err != nil {
				t.Fatal(err)
			}
			executor := directiveContinuationRunner(func(ctx context.Context, req processrunner.Request, _ processrunner.OutputSink) (contract.ProcessResult, error) {
				req.Started()
				if (ctx.Err() == nil) != tc.alive {
					t.Errorf("payload alive = %t after %d answers", ctx.Err() == nil, calls.Load())
				}
				if ctx.Err() != nil {
					return contract.ProcessResult{Signal: "terminated", TerminationCause: contract.TerminationCauseAgent}, ctx.Err()
				}
				zero := 0
				return contract.ProcessResult{ExitCode: &zero}, nil
			})
			lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{nodeID: "node", bootSessionID: "boot", managedResource: resource, client: client, runtimes: testRuntimeSet(executor), clock: systemClock{}, completionRetry: time.Millisecond, finalizationTimeout: time.Second})
			claim := l1.Claim{Job: l1.Job{JobID: "job", Spec: contract.JobSpec{Kind: "process", Class: "service", Restart: "never", Execution: contract.ExecutionSpec{Executable: contract.ExecutableSpec{Path: "/bin/true"}, Argv: []string{"true"}, WorkingDirectory: t.TempDir()}}}, Lease: l1.AttemptLease{AttemptID: "attempt", FencingToken: "fence", LeaseTTL: time.Minute}}
			_, err = lifecycle.runWorkload(t.Context(), claim)
			if tc.alive != (err == nil) || int(calls.Load()) != len(tc.answers) {
				t.Fatalf("start acknowledgement = %v after %d calls, want %d", err, calls.Load(), len(tc.answers))
			}
		})
	}
}

// An app drives a real L1 and node agent entirely through the client contract.
// The real process runner and guardian must acknowledge start and publish the
// chosen exit, and a new execution requires explicit start/restart.
func TestNeverProcessPublicContractWithRealAgent(t *testing.T) {
	for _, code := range []int{0, 7} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			network := plain.NewNetwork()
			_, stopServer := startFailureServerWithPoliciesAndLease(t, network, nil, map[string]l1.NodePolicy{"never-node": {MaxServiceSlots: 1}}, time.Second)
			defer stopServer()
			client := newHTTPClient(network.NewFabric(fabric.Identity{NodeID: "app", Tags: []string{l1.DefaultClientPrincipalTag}}), "wefty://control-plane")
			defer client.CloseIdleConnections()
			call := func(method, path string, value any, want int) l1.Job {
				t.Helper()
				var payload []byte
				if value != nil {
					var err error
					payload, err = json.Marshal(value)
					if err != nil {
						t.Fatal(err)
					}
				}
				req, err := http.NewRequestWithContext(t.Context(), method, "http://l1"+path, bytes.NewReader(payload))
				if err != nil {
					t.Fatal(err)
				}
				req.Header.Set("Content-Type", "application/json")
				resp, err := client.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				defer resp.Body.Close()
				body, err := io.ReadAll(resp.Body)
				if err != nil {
					t.Fatal(err)
				}
				if resp.StatusCode != want {
					t.Fatalf("%s %s = %d %s", method, path, resp.StatusCode, body)
				}
				var job l1.Job
				if err := json.Unmarshal(body, &job); err != nil {
					t.Fatal(err)
				}
				return job
			}
			root, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			nodeAgent, err := New(Config{Fabric: network.NewFabric(fabric.Identity{NodeID: "never-agent", Tags: []string{l1.DefaultAgentPrincipalTag}}), ControlPlaneAddress: "wefty://control-plane", NodeID: "never-node", BootSessionID: "boot-never", Version: "test", Capabilities: map[string]bool{"kind:process": true}, MaxServiceSlots: 1, ManagedRootDirectory: root, LogSpoolDirectory: t.TempDir(), GuardianExecutable: agentBinaryPath, HeartbeatInterval: 20 * time.Millisecond, ClaimInterval: 5 * time.Millisecond, RenewalInterval: 20 * time.Millisecond})
			if err != nil {
				t.Fatal(err)
			}
			defer nodeAgent.Close()
			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan error, 1)
			go func() { done <- nodeAgent.Run(ctx) }()
			defer func() {
				cancel()
				if err := <-done; err != nil {
					t.Errorf("agent shutdown: %v", err)
				}
			}()
			job := call(http.MethodPost, "/v1/jobs", contract.JobSpec{SchemaVersion: 1, DispatchKey: "never-real", Kind: "process", Class: "service", Restart: "never", Execution: contract.ExecutionSpec{Executable: contract.ExecutableSpec{Path: "/bin/sh"}, Argv: []string{"sh", "-c", fmt.Sprintf("exit %d", code)}, WorkingDirectory: t.TempDir()}}, http.StatusCreated)
			wait := func() l1.Job {
				t.Helper()
				deadline := time.Now().Add(5 * time.Second)
				for {
					got := call(http.MethodGet, "/v1/jobs/"+job.JobID+"?class=service", nil, http.StatusOK)
					if got.PolicyStop != nil {
						return got
					}
					if time.Now().After(deadline) {
						t.Fatalf("no policy stop: %+v", got)
					}
					time.Sleep(5 * time.Millisecond)
				}
			}
			stopped := wait()
			want := contract.JobStopped
			if code != 0 {
				want = contract.JobFailed
			}
			if stopped.State != want || stopped.DesiredState != contract.ServiceDesiredRunning || stopped.PolicyStop.ExitCode == nil || *stopped.PolicyStop.ExitCode != code || stopped.HoldsSlot(stopped.State) {
				t.Fatalf("payload verdict = %+v", stopped)
			}
			// L1 queues automatic retries immediately with a durable timer. A
			// terminal state with no timer and no restart accounting proves
			// suppression without waiting through a backoff window.
			if stopped.NextRestartAt != nil || stopped.RestartStreak != 0 || stopped.LifetimeRestartCount != 0 || stopped.LeaseLossCount != 0 {
				t.Fatalf("automatic retry scheduled = %+v", stopped.ServiceJob)
			}
			if code == 0 {
				call(http.MethodPut, "/v1/jobs/"+job.JobID+"/desired-state?class=service", l1.ServiceDesiredStateRequest{DesiredState: contract.ServiceDesiredRunning}, http.StatusAccepted)
			} else {
				call(http.MethodPost, "/v1/jobs/"+job.JobID+"/restart?class=service", l1.ServiceRestartRequest{IdempotencyKey: "resume"}, http.StatusAccepted)
			}
			resumed := wait()
			if resumed.State != want || resumed.CurrentAttemptID == stopped.CurrentAttemptID {
				t.Fatalf("explicit resume = %+v", resumed)
			}
		})
	}
}

// L1 commits the process start acknowledgement, but its answer never reaches
// the agent. The agent retries the idempotent call, the retry replays the
// committed start, and the payload keeps running. The service is never failed:
// its own clean exit later records the usual never policy stop.
func TestNeverProcessStartSurvivesLostAcknowledgement(t *testing.T) {
	store, err := l1.OpenStore(filepath.Join(t.TempDir(), "lost-start.sqlite"), l1.StoreOptions{LeaseDuration: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	network := plain.NewNetwork()
	serverFabric := network.NewFabric(fabric.Identity{NodeID: "control-plane"})
	l1Server, err := l1.NewServer(serverFabric, store, l1.ServerConfig{NodePolicies: map[string]l1.NodePolicy{"never-node": {MaxServiceSlots: 1}}})
	if err != nil {
		t.Fatal(err)
	}
	var startCalls atomic.Int32
	committed := make(chan int, 1)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/started") || startCalls.Add(1) > 1 {
			l1Server.Handler().ServeHTTP(w, r)
			return
		}
		recorder := httptest.NewRecorder()
		l1Server.Handler().ServeHTTP(recorder, r)
		committed <- recorder.Code
		// The start is durable in L1; its answer is lost with the connection.
		connection, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		_ = connection.Close()
	})
	listener, err := serverFabric.Listen("tcp", "wefty://control-plane")
	if err != nil {
		t.Fatal(err)
	}
	httpServer := &http.Server{Handler: handler}
	served := make(chan error, 1)
	go func() { served <- httpServer.Serve(listener) }()
	defer func() {
		_ = httpServer.Close()
		if err := <-served; err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("serve lost-start L1: %v", err)
		}
	}()

	alive := make(chan bool, 1)
	release := make(chan struct{})
	runner := directiveContinuationRunner(func(ctx context.Context, req processrunner.Request, _ processrunner.OutputSink) (contract.ProcessResult, error) {
		req.Started()
		alive <- ctx.Err() == nil
		select {
		case <-release:
			zero := 0
			return contract.ProcessResult{ExitCode: &zero}, nil
		case <-ctx.Done():
			return contract.ProcessResult{Signal: "terminated", TerminationCause: contract.TerminationCauseAgent}, ctx.Err()
		}
	})
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	nodeAgent, err := New(Config{
		Fabric: network.NewFabric(fabric.Identity{NodeID: "never-agent", Tags: []string{l1.DefaultAgentPrincipalTag}}), ControlPlaneAddress: "wefty://control-plane",
		NodeID: "never-node", BootSessionID: "boot-never", Version: "test", Capabilities: map[string]bool{"kind:process": true}, MaxServiceSlots: 1,
		ManagedRootDirectory: root, LogSpoolDirectory: t.TempDir(), WorkloadRuntimes: testRuntimeSet(runner),
		HeartbeatInterval: 20 * time.Millisecond, ClaimInterval: 5 * time.Millisecond, RenewalInterval: 100 * time.Millisecond,
		OperationTimeout: 2 * time.Second, LogRetryInterval: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer nodeAgent.Close()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- nodeAgent.Run(ctx) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("agent shutdown: %v", err)
		}
	}()
	job, _, err := store.CreateJob(t.Context(), contract.JobSpec{
		SchemaVersion: contract.SchemaVersionV1, DispatchKey: "never-lost-start", Kind: contract.JobKindProcess,
		Class: contract.JobClassService, Restart: contract.RestartNever,
		Execution: contract.ExecutionSpec{Executable: contract.ExecutableSpec{Path: "/bin/true"}, Argv: []string{"true"}, WorkingDirectory: t.TempDir()},
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case code := <-committed:
		if code != http.StatusOK {
			t.Fatalf("first start acknowledgement = HTTP %d, want a committed start", code)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("start acknowledgement never reached L1")
	}
	select {
	case running := <-alive:
		if !running {
			t.Fatal("a lost start acknowledgement canceled the payload")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("start acknowledgement never returned to the runner")
	}
	if calls := startCalls.Load(); calls < 2 {
		t.Fatalf("start acknowledgement calls = %d, want a retry after the lost answer", calls)
	}
	running, err := store.GetJob(t.Context(), job.JobID)
	if err != nil || running.State != contract.JobRunning || len(running.LastFailure) != 0 {
		t.Fatalf("service after a lost start answer = %+v %v", running, err)
	}
	close(release)
	deadline := time.Now().Add(10 * time.Second)
	for {
		got, err := store.GetJob(t.Context(), job.JobID)
		if err != nil {
			t.Fatal(err)
		}
		if got.State == contract.JobFailed {
			t.Fatalf("service failed after a lost start answer: %+v", got.ServiceJob)
		}
		if got.State == contract.JobStopped {
			if got.PolicyStop == nil || got.PolicyStop.ExitCode == nil || *got.PolicyStop.ExitCode != 0 || got.CurrentAttemptID != running.CurrentAttemptID {
				t.Fatalf("payload verdict = %+v", got.ServiceJob)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("payload exit never completed: %+v", got)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
