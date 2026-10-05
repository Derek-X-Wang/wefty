package agent

import (
	"context"
	"encoding/json"
	"net/http"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
)

func TestHeartbeatCancelDuringCompletionFinishesResultNow(t *testing.T) {
	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)
	var calls atomic.Int32
	var first l1.CompletionRequest
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/complete") {
			var body l1.CompletionRequest
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
				return
			}
			if calls.Add(1) == 1 {
				first = body
				// This is the resident heartbeat cancellation, not a renewal response.
				cancel(errAttemptDirectiveCancel)
				<-r.Context().Done()
				return
			}
			if body.IdempotencyKey != first.IdempotencyKey || body.FencingToken != first.FencingToken || body.Result.ExitCode == nil || *body.Result.ExitCode != *first.Result.ExitCode {
				t.Error("completion retry changed evidence")
			}
			_ = json.NewEncoder(w).Encode(l1.Job{})
			return
		}
		http.NotFound(w, r)
	})
	client, stop := startEvidenceReplayServer(t, handler, time.Second)
	defer stop()
	defer client.Close()
	outbox, err := newEvidenceOutbox(t.TempDir(), "stable-node", 1024, systemClock{}, 8, time.Hour, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer outbox.Close()
	claim := spoolTestClaim("heartbeat-completion-cancel")
	claim.Job.Spec.Kind = contract.JobKindProcess
	lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{client: client, runtimes: workloadRuntimeSet{contract.JobKindProcess: &captureRuntime{}}, outbox: outbox,
		clock: systemClock{}, renewalInterval: time.Hour, completionRetry: time.Millisecond, finalizationTimeout: time.Second, observer: newLifecycleObserver(systemClock{})})
	destination, err := lifecycle.execute(ctx, claim, time.Now())
	if err != nil || destination != errorDestinationUnclassified || calls.Load() != 2 || !lifecycle.resultUploaded.Load() {
		t.Fatalf("completion missed normal result finalization: calls=%d retained=%t uploaded=%t destination=%v err=%v", calls.Load(), lifecycle.resultsRetained.Load(), lifecycle.resultUploaded.Load(), destination, err)
	}
	waitCompletionReceiptState(t, outbox, claim.Lease.AttemptID, "delivered", time.Second)
}

func TestCancelDuringOCIStartedResponsePreservesCommittedStart(t *testing.T) {
	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)
	entered, release := make(chan struct{}), make(chan struct{})
	recorder := &resultUploadRecorder{}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/started") {
			close(entered)
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
			_ = json.NewEncoder(w).Encode(l1.Job{State: contract.JobRunning, Outcome: "canceled"})
			return
		}
		recorder.handler().ServeHTTP(w, r)
	})
	client, stop := startEvidenceReplayServer(t, handler, time.Second)
	defer stop()
	defer client.Close()
	claim := ociHandoffClaim("run-started-response", "attempt-started-response")
	claim.Job.JobID = "job-started-response"
	runtime := &startedResponseOCIRuntime{ack: make(chan error, 1)}
	lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{client: client, runtimes: workloadRuntimeSet{contract.JobKindOCI: runtime}, clock: systemClock{}, renewalInterval: time.Hour, observer: newLifecycleObserver(systemClock{}), attemptDeadman: newRecordingDeadmanRenewer()})
	done := make(chan error, 1)
	go func() { _, err := lifecycle.execute(ctx, claim, time.Now()); done <- err }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("Started never reached L1")
	}
	// The durable edge already won; cancel arrives before the HTTP response.
	cancel(errAttemptDirectiveCancel)
	close(release)
	select {
	case err := <-runtime.ack:
		if err != nil {
			t.Fatalf("cancel erased committed Started response: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Started acknowledgement did not join")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceled OCI did not finish")
	}
}

type startedResponseOCIRuntime struct {
	captureRuntime
	ack chan error
}

func (r *startedResponseOCIRuntime) Run(ctx context.Context, request workloadrunner.Request, _ workloadrunner.OutputSink) (workloadrunner.Result, error) {
	err := request.OCIStarted(ctx, workloadrunner.OCIImageObservation{})
	r.ack <- err
	if err != nil {
		return workloadrunner.Result{Outcome: contract.ProcessResult{SpawnError: &contract.SpawnFailure{Code: contract.SpawnFailureProcessRequest, Message: err.Error()}}}, err
	}
	<-ctx.Done()
	return workloadrunner.Result{Outcome: contract.ProcessResult{Signal: "terminated", TerminationCause: contract.TerminationCauseAgent}}, nil
}

// The outage is held until the live cancellation retry has exhausted its
// configured budget and the same agent has returned to claiming work.
func TestCancelCompletionRetryOutageKeepsAgentRunning(t *testing.T) {
	for _, delivery := range []string{"heartbeat", "renewal"} {
		t.Run(delivery, func(t *testing.T) {
			claim := spoolTestClaim("cancel-outage-" + delivery)
			claim.Job.Spec.Kind = contract.JobKindProcess
			claim.Lease.LeaseTTL = time.Hour
			claim.Lease.LeaseExpires = time.Now().Add(time.Hour)
			node := l1.Node{NodeRegistration: contract.NodeRegistration{NodeID: "stable-node", BootSessionID: "boot-outage"},
				State: contract.NodeAlive, ClaimsEnabled: true, MaxOneshotSlots: 1}
			var claimed, completing, unavailable, finished atomic.Bool
			unavailable.Store(true)
			var calls atomic.Int32
			var mu sync.Mutex
			var first l1.CompletionRequest
			nextClaim := make(chan struct{}, 1)
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch {
				case strings.HasSuffix(r.URL.Path, "/register"):
					_ = json.NewEncoder(w).Encode(node)
				case strings.HasSuffix(r.URL.Path, "/heartbeat"):
					response := l1.HeartbeatResponse{Node: node}
					if delivery == "heartbeat" && completing.Load() {
						response.OneShotCancelDirectives = []l1.OneShotCancelDirective{{JobID: claim.Job.JobID, AttemptID: claim.Lease.AttemptID, FencingToken: claim.Lease.FencingToken}}
					}
					_ = json.NewEncoder(w).Encode(response)
				case strings.HasSuffix(r.URL.Path, "/claim"):
					var request l1.ClaimRequest
					_ = json.NewDecoder(r.Body).Decode(&request)
					if request.Class == contract.JobClassOneShot && claimed.CompareAndSwap(false, true) {
						_ = json.NewEncoder(w).Encode(claim)
					} else {
						if request.Class == contract.JobClassOneShot && finished.Load() {
							select {
							case nextClaim <- struct{}{}:
							default:
							}
						}
						w.WriteHeader(http.StatusNoContent)
					}
				case strings.HasSuffix(r.URL.Path, "/lease"):
					lease := claim.Lease
					if delivery == "renewal" && completing.Load() {
						lease.Directive = l1.AttemptDirectiveCancel
					}
					_ = json.NewEncoder(w).Encode(lease)
				case strings.HasSuffix(r.URL.Path, "/complete"):
					var body l1.CompletionRequest
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
						t.Error(err)
						return
					}
					mu.Lock()
					if calls.Add(1) == 1 {
						first = body
						mu.Unlock()
						completing.Store(true)
						<-r.Context().Done() // abandon the original delivery on cancel
						return
					}
					if !reflect.DeepEqual(first, body) {
						t.Errorf("recovery changed completion: first=%+v retry=%+v", first, body)
					}
					mu.Unlock()
					if unavailable.Load() {
						w.WriteHeader(http.StatusServiceUnavailable)
						_ = json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{Code: contract.ErrorInternal, Retryable: true, Message: "temporary L1 outage"}})
						return
					}
					_ = json.NewEncoder(w).Encode(l1.Job{})
				default:
					http.NotFound(w, r)
				}
			})
			network := plain.NewNetwork()
			listener, err := network.NewFabric(fabric.Identity{NodeID: "control-plane"}).Listen("tcp", "wefty://control-plane")
			if err != nil {
				t.Fatal(err)
			}
			server := &http.Server{Handler: handler}
			go func() { _ = server.Serve(listener) }()
			defer server.Close()
			root, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			heartbeat, renewal := 10*time.Millisecond, time.Hour
			if delivery == "renewal" {
				heartbeat, renewal = time.Hour, 10*time.Millisecond
			}
			nodeAgent, err := New(Config{
				Fabric: network.NewFabric(fabric.Identity{NodeID: "agent"}), ControlPlaneAddress: "wefty://control-plane",
				NodeID: node.NodeID, BootSessionID: node.BootSessionID, Version: "test",
				Capabilities:         map[string]bool{"kind:process": true},
				WorkloadRuntimes:     map[string]WorkloadRuntime{contract.JobKindProcess: &captureRuntime{}},
				ManagedRootDirectory: root, HandoffRoot: t.TempDir(), LogSpoolDirectory: t.TempDir(),
				OperationTimeout: 200 * time.Millisecond, HeartbeatInterval: heartbeat, RenewalInterval: renewal,
				ClaimInterval: time.Millisecond, LogRetryInterval: time.Millisecond,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer nodeAgent.Close()
			nodeAgent.session.residentBeforeCompletionRecord = func(error) { finished.Store(true) }
			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan error, 1)
			go func() { done <- nodeAgent.Run(ctx) }()
			defer func() {
				cancel()
				select {
				case err := <-done:
					if err != nil {
						t.Errorf("agent shutdown: %v", err)
					}
				case <-time.After(3 * time.Second):
					t.Error("agent did not join shutdown")
				}
			}()
			select {
			case <-nextClaim:
			case err := <-done:
				// Preserve the result for the shutdown join.
				done <- err
				t.Fatalf("cancel retry ended agent during L1 outage: %v", err)
			case <-time.After(3 * time.Second):
				t.Fatal("agent did not resume claims after cancellation retry budget")
			}
			pending := nodeAgent.outbox.spool.inspectCompletion(t.Context(), claim.Lease.AttemptID)
			mu.Lock()
			live := first
			mu.Unlock()
			if pending.State != "durable_completion" || !reflect.DeepEqual(pending.Result, live.Result) || calls.Load() < 2 {
				t.Fatalf("outage lost durable completion: %+v calls=%d", pending, calls.Load())
			}
			unavailable.Store(false)
			waitCompletionReceiptState(t, nodeAgent.outbox, claim.Lease.AttemptID, "delivered", 3*time.Second)
			select {
			case err := <-done:
				done <- err
				t.Fatalf("outbox recovery ended agent: %v", err)
			default:
			}
		})
	}
}
