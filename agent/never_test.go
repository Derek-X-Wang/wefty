package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
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

// startVerdict waits for the start acknowledgement running alongside the
// payload: true once the agent records the attempt running, false once a
// refusal cancels the payload.
func startVerdict(t *testing.T, ctx context.Context, observer *lifecycleObserver, attemptID string) bool {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		if observer.snapshot(ClassOccupancy{}, ClassOccupancy{}).Attempts[attemptID].State == AttemptRunning {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-deadline:
			t.Error("start acknowledgement reached no verdict")
			return false
		case <-time.After(time.Millisecond):
		}
	}
}

func neverProcessClaim(t *testing.T, leaseTTL time.Duration, limits *contract.JobLimits, executable string, argv ...string) l1.Claim {
	return l1.Claim{Job: l1.Job{JobID: "job", Spec: contract.JobSpec{Kind: "process", Class: "service", Restart: "never", Limits: limits,
		Execution: contract.ExecutionSpec{Executable: contract.ExecutableSpec{Path: executable}, Argv: argv, WorkingDirectory: t.TempDir()}}},
		Lease: l1.AttemptLease{AttemptID: "attempt", FencingToken: "fence", LeaseTTL: leaseTTL}}
}

func neverProcessLifecycle(t *testing.T, client *Client, clock Clock, executor processrunner.Executor) (*attemptLifecycle, *lifecycleObserver) {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	resource, err := initializeManagedResource(root, "node", "boot")
	if err != nil {
		t.Fatal(err)
	}
	observer := newLifecycleObserver(clock)
	observer.beginAttempt("attempt", "job", contract.JobClassService)
	return newAttemptLifecycle(attemptLifecycleDependencies{nodeID: "node", bootSessionID: "boot", managedResource: resource, client: client,
		runtimes: testRuntimeSet(executor), clock: clock, completionRetry: time.Millisecond, finalizationTimeout: time.Second, observer: observer}), observer
}

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
			var observer *lifecycleObserver
			executor := directiveContinuationRunner(func(ctx context.Context, req processrunner.Request, _ processrunner.OutputSink) (contract.ProcessResult, error) {
				if req.Started == nil {
					t.Fatal("process runner has no start acknowledgement hook")
				}
				if acknowledged.Load() {
					t.Fatal("acknowledged before runner start")
				}
				req.Started()
				if alive := startVerdict(t, ctx, observer, "attempt"); alive == refuse {
					t.Errorf("payload alive = %t after the start answer", alive)
				}
				if !acknowledged.Load() {
					t.Error("runner start did not reach L1")
				}
				if ctx.Err() != nil {
					return contract.ProcessResult{Signal: "terminated", TerminationCause: contract.TerminationCauseAgent}, ctx.Err()
				}
				zero := 0
				return contract.ProcessResult{ExitCode: &zero}, nil
			})
			var lifecycle *attemptLifecycle
			lifecycle, observer = neverProcessLifecycle(t, client, systemClock{}, executor)
			result, err := lifecycle.runWorkload(t.Context(), neverProcessClaim(t, 0, nil, "/bin/true", "true"))
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

// Only an answer that never arrived, or a 5xx other than 501, is retried.
// Any other status L1 sent is its verdict, even when the body is unreadable:
// a refusal cancels the payload on its first answer with a lease window left
// to spend, and a 2xx is the committed start.
func TestNeverProcessStartRetriesOnlyUndecidedAnswers(t *testing.T) {
	refusal := func(status int, code contract.ErrorCode) func(http.ResponseWriter) {
		return func(w http.ResponseWriter) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{Code: code, Message: string(code)}})
		}
	}
	unreadable := func(status int, body string) func(http.ResponseWriter) {
		return func(w http.ResponseWriter) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_, _ = w.Write([]byte(body))
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
		{"unreadable unavailable then accepted", []func(http.ResponseWriter){unreadable(http.StatusServiceUnavailable, "<html>busy"), accept}, true},
		{"truncated acceptance", []func(http.ResponseWriter){unreadable(http.StatusOK, `{"job_id":"jo`)}, true},
		{"stale fence", []func(http.ResponseWriter){refusal(http.StatusConflict, contract.ErrorStaleFence)}, false},
		{"lease expired", []func(http.ResponseWriter){refusal(http.StatusConflict, contract.ErrorLeaseExpired)}, false},
		{"pending cancellation", []func(http.ResponseWriter){refusal(http.StatusConflict, contract.ErrorConflict)}, false},
		{"truncated conflict", []func(http.ResponseWriter){unreadable(http.StatusConflict, `{"error":{"code":"stale_fe`)}, false},
		{"unreadable not implemented", []func(http.ResponseWriter){unreadable(http.StatusNotImplemented, "<html>")}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			client, closeServer := startEvidenceReplayServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				call := int(calls.Add(1))
				if call > len(tc.answers) {
					t.Errorf("start acknowledgement call %d after a final answer", call)
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				tc.answers[call-1](w)
			}), time.Second)
			defer closeServer()
			defer client.Close()
			var observer *lifecycleObserver
			executor := directiveContinuationRunner(func(ctx context.Context, req processrunner.Request, _ processrunner.OutputSink) (contract.ProcessResult, error) {
				req.Started()
				if alive := startVerdict(t, ctx, observer, "attempt"); alive != tc.alive {
					t.Errorf("payload alive = %t after %d answers", alive, calls.Load())
				}
				if ctx.Err() != nil {
					return contract.ProcessResult{Signal: "terminated", TerminationCause: contract.TerminationCauseAgent}, ctx.Err()
				}
				zero := 0
				return contract.ProcessResult{ExitCode: &zero}, nil
			})
			var lifecycle *attemptLifecycle
			lifecycle, observer = neverProcessLifecycle(t, client, systemClock{}, executor)
			_, err := lifecycle.runWorkload(t.Context(), neverProcessClaim(t, time.Minute, nil, "/bin/true", "true"))
			if tc.alive != (err == nil) || int(calls.Load()) != len(tc.answers) {
				t.Fatalf("start acknowledgement = %v after %d calls, want %d", err, calls.Load(), len(tc.answers))
			}
		})
	}
}

// The retry ends with the lease window. A backoff that reaches the window's
// end sends no further request, even one L1 would have accepted.
func TestNeverProcessStartRetryEndsWithLeaseWindow(t *testing.T) {
	for _, overshoot := range []time.Duration{0, time.Second} {
		t.Run(fmt.Sprint(overshoot), func(t *testing.T) {
			var calls atomic.Int32
			answered := make(chan struct{}, 1)
			client, closeServer := startEvidenceReplayServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if calls.Add(1) > 1 {
					_ = json.NewEncoder(w).Encode(l1.Job{JobID: "job", State: contract.JobRunning})
					return
				}
				w.WriteHeader(http.StatusServiceUnavailable)
				_ = json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{Code: contract.ErrorInternal, Message: "busy"}})
				answered <- struct{}{}
			}), time.Second)
			defer closeServer()
			defer client.Close()
			start := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
			clock := newManualClock(start)
			window := time.Second
			lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{client: client, clock: clock, completionRetry: window})
			result := make(chan error, 1)
			go func() {
				result <- lifecycle.acknowledgeStart(t.Context(), neverProcessClaim(t, window, nil, "/bin/true", "true"))
			}()
			select {
			case <-answered:
			case <-time.After(5 * time.Second):
				t.Fatal("first start acknowledgement was not answered")
			}
			clock.waitForDeadline(t, start.Add(window))
			clock.Advance(window + overshoot)
			select {
			case err := <-result:
				if err == nil || calls.Load() != 1 {
					t.Fatalf("acknowledgement after its window = %v after %d calls, want the undecided answer and no new request", err, calls.Load())
				}
			case <-time.After(5 * time.Second):
				t.Fatal("acknowledgement outlived its lease window")
			}
		})
	}
}

// While L1 has not answered the start acknowledgement, the real process
// runner still enforces the payload's maximum runtime, and no acknowledgement
// request outlives the attempt.
func TestNeverProcessMaxRuntimeHoldsWhileStartUndecided(t *testing.T) {
	var calls atomic.Int32
	client, closeServer := startEvidenceReplayServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		<-r.Context().Done()
	}), 500*time.Millisecond)
	defer closeServer()
	defer client.Close()
	lifecycle, _ := neverProcessLifecycle(t, client, systemClock{}, processrunner.New(processrunner.Config{}))
	started := time.Now()
	result, err := lifecycle.runWorkload(t.Context(), neverProcessClaim(t, 20*time.Second, &contract.JobLimits{MaxRuntimeSeconds: 1}, "/bin/sleep", "sleep", "30"))
	elapsed := time.Since(started)
	if !errors.Is(err, processrunner.ErrMaxRuntime) || elapsed > 5*time.Second {
		t.Fatalf("payload under an undecided start = %+v %v after %s, want the 1s maximum runtime enforced", result, err, elapsed)
	}
	if calls.Load() == 0 {
		t.Fatal("start acknowledgement never reached L1")
	}
	settled := calls.Load()
	time.Sleep(100 * time.Millisecond)
	if calls.Load() != settled {
		t.Fatal("start acknowledgement kept retrying after its attempt ended")
	}
}

// A portful never service whose backend is ready while L1 keeps answering the
// start acknowledgement with 503. The held readiness reports nothing running
// or serving, publishes nothing and forwards nothing. If L1 then accepts, the
// held readiness applies: the service serves, is published and forwards. If L1
// refuses, the payload stops and nothing was ever published.
func TestNeverPortfulServiceWaitsForStartAcceptance(t *testing.T) {
	for _, accept := range []bool{true, false} {
		t.Run(map[bool]string{true: "accepted", false: "refused"}[accept], func(t *testing.T) {
			var startCalls atomic.Int32
			var published atomic.Bool
			answer := make(chan struct{})
			client, closeServer := startEvidenceReplayServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch {
				case strings.HasSuffix(r.URL.Path, "/started"):
					startCalls.Add(1)
					select {
					case <-answer:
						if accept {
							_ = json.NewEncoder(w).Encode(l1.Job{JobID: "job", State: contract.JobRunning})
							return
						}
						w.WriteHeader(http.StatusConflict)
						_ = json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{Code: contract.ErrorStaleFence, Message: "lost fence"}})
					default:
						w.WriteHeader(http.StatusServiceUnavailable)
						_ = json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{Code: contract.ErrorInternal, Message: "busy"}})
					}
				case strings.HasSuffix(r.URL.Path, "/publication"):
					var request l1.PublicationRequest
					if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
						t.Error(err)
					}
					if request.Ready != nil && *request.Ready {
						published.Store(true)
					}
					_ = json.NewEncoder(w).Encode(l1.Job{JobID: "job", State: contract.JobRunning})
				default:
					t.Errorf("unexpected L1 call %s", r.URL.Path)
					w.WriteHeader(http.StatusNotFound)
				}
			}), time.Second)
			defer closeServer()
			defer client.Close()
			backend, err := net.Listen("tcp4", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer backend.Close()
			go serveTestEcho(backend)
			frontDoor, err := net.Listen("tcp4", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			reported, release := make(chan struct{}), make(chan struct{})
			executor := directiveContinuationRunner(func(ctx context.Context, req processrunner.Request, _ processrunner.OutputSink) (contract.ProcessResult, error) {
				req.Started()
				// The guardian finds the backend ready before L1 has answered.
				req.ReadinessChanged(true, true)
				close(reported)
				select {
				case <-release:
					zero := 0
					return contract.ProcessResult{ExitCode: &zero}, nil
				case <-ctx.Done():
					return contract.ProcessResult{Signal: "terminated", TerminationCause: contract.TerminationCauseAgent}, ctx.Err()
				}
			})
			lifecycle, observer := neverProcessLifecycle(t, client, systemClock{}, executor)
			lifecycle.dependencies.reservePublishedPort = func(l1.Claim) (net.Listener, *contract.SpawnFailure) { return frontDoor, nil }
			lifecycle.dependencies.prepareServiceEndpoint = func(context.Context) (serviceRuntimeEndpoint, error) {
				dialer := &net.Dialer{}
				return serviceRuntimeEndpoint{address: backend.Addr().String(), dial: func(ctx context.Context) (net.Conn, error) {
					return dialer.DialContext(ctx, "tcp4", backend.Addr().String())
				}}, nil
			}
			claim := neverProcessClaim(t, time.Minute, nil, "/bin/true", "true")
			port := 8080
			claim.Job.Spec.PublishedPort = &port
			done := make(chan error, 1)
			go func() {
				_, err := lifecycle.runWorkload(t.Context(), claim)
				done <- err
			}()
			select {
			case <-reported:
			case <-time.After(5 * time.Second):
				t.Fatal("runner never reported readiness")
			}
			state := func() AttemptLifecycleState {
				return observer.snapshot(ClassOccupancy{}, ClassOccupancy{}).Attempts["attempt"].State
			}
			unpublished := func() {
				t.Helper()
				if got := state(); got == AttemptRunning || got == AttemptServing {
					t.Fatalf("attempt state = %s before L1 accepted the start", got)
				}
				if published.Load() {
					t.Fatal("published before L1 accepted the start")
				}
				if publishedEchoForwarded(frontDoor.Addr().String()) {
					t.Fatal("front door forwarded before L1 accepted the start")
				}
			}
			holdUntil, giveUp := time.Now().Add(300*time.Millisecond), time.Now().Add(5*time.Second)
			for time.Now().Before(holdUntil) || startCalls.Load() < 2 {
				if time.Now().After(giveUp) {
					t.Fatalf("start acknowledgement calls = %d, want undecided retries", startCalls.Load())
				}
				unpublished()
				time.Sleep(10 * time.Millisecond)
			}
			close(answer)
			if !accept {
				select {
				case err := <-done:
					if err == nil || !strings.Contains(err.Error(), "acknowledge process start") {
						t.Fatalf("refused portful service = %v", err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("refused start left the payload running")
				}
				unpublished()
				return
			}
			waitForPublishedEcho(t, frontDoor.Addr().String(), true)
			if got := state(); !published.Load() || got != AttemptServing {
				t.Fatalf("after acceptance: published = %t, attempt state = %s", published.Load(), got)
			}
			close(release)
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("accepted portful service = %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("portful service did not finish")
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

	canceled := make(chan struct{})
	release := make(chan struct{})
	runner := directiveContinuationRunner(func(ctx context.Context, req processrunner.Request, _ processrunner.OutputSink) (contract.ProcessResult, error) {
		req.Started()
		select {
		case <-release:
			zero := 0
			return contract.ProcessResult{ExitCode: &zero}, nil
		case <-ctx.Done():
			close(canceled)
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
	// The agent records the attempt running only once L1's start verdict
	// arrives, here through the retry after the lost answer.
	assertAttemptStatus(t, nodeAgent, AttemptRunning)
	select {
	case <-canceled:
		t.Fatal("a lost start acknowledgement canceled the payload")
	default:
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
