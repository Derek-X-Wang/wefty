package agent

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

type refusedRenewalRuntime struct {
	captureRuntime
	reaps atomic.Int32
}

func (*refusedRenewalRuntime) Run(_ context.Context, request workloadrunner.Request, _ workloadrunner.OutputSink) (workloadrunner.Result, error) {
	return workloadrunner.Result{Outcome: contract.ProcessResult{RuntimeFailure: &contract.RuntimeFailure{Code: contract.RuntimeFailureUnavailable}}}, &ocihelper.AttemptLostError{Authority: ocihelper.AttemptAuthority{AttemptID: request.Authority.AttemptID}}
}
func (r *refusedRenewalRuntime) ReapAndVerify(context.Context, workloadrunner.ReapRequest) (workloadrunner.ReapReceipt, error) {
	r.reaps.Add(1)
	return workloadrunner.ReapReceipt{RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidenceAttempt}, nil
}

func TestOCILateRenewalSettlesLostWithoutCompletionOrSessionRecovery(t *testing.T) {
	var requests atomic.Int32
	client, stop := startEvidenceReplayServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		http.Error(w, "unexpected completion", 500)
	}), time.Second)
	defer stop()
	defer client.Close()
	outbox, err := newEvidenceOutbox(t.TempDir(), "node", 1024, systemClock{}, 8, time.Hour, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer outbox.Close()
	claim := spoolTestClaim("late-renewal")
	claim.Lease.LeaseTTL = time.Minute
	claim.Job.Spec.Kind = contract.JobKindOCI
	claim.Job.Spec.Execution = contract.ExecutionSpec{OCI: &contract.OCIExecutionSpec{Image: contract.OCIImageSpec{Reference: "example.invalid/test"}}}
	runtime := &refusedRenewalRuntime{}
	lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{client: client, outbox: outbox, runtimes: workloadRuntimeSet{contract.JobKindOCI: runtime}, nodeID: "node", bootSessionID: "boot", clock: systemClock{}, renewalInterval: time.Hour, finalizationTimeout: time.Second, observer: newLifecycleObserver(systemClock{}), embargoOCIRuntime: func(workloadrunner.RuntimeGeneration) { t.Error("attempt loss embargoed OCI") }, recoverOCIRuntime: func(context.Context, workloadrunner.RuntimeGeneration) error {
		t.Error("attempt loss recovered session")
		return nil
	}})
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	destination, err := lifecycle.execute(ctx, claim, time.Now())
	var lost *ocihelper.AttemptLostError
	if destination != errorDestinationAttemptAuthority || !errors.As(err, &lost) || runtime.reaps.Load() != 1 {
		t.Fatalf("destination=%v err=%v reaps=%d", destination, err, runtime.reaps.Load())
	}
	if requests.Load() != 0 {
		t.Fatalf("lost attempt published completion: requests=%d", requests.Load())
	}
	if _, _, present, err := outbox.spool.durableCompletion(t.Context(), claim.Lease.AttemptID); err != nil || present {
		t.Fatalf("lost attempt queued durable completion: present=%t err=%v", present, err)
	}
}

// Hold Run until the lifecycle cancels it, so the renewal/shutdown branch wins
// the select even though Watch can return a different error (or no error).
type cancellationRenewalRuntime struct {
	refusedRenewalRuntime
	entered chan struct{}
	runErr  error
}

func (r *cancellationRenewalRuntime) Run(ctx context.Context, request workloadrunner.Request, _ workloadrunner.OutputSink) (workloadrunner.Result, error) {
	if err := admitReadyOCIHelper(request); err != nil {
		return workloadrunner.Result{}, err
	}
	close(r.entered)
	<-ctx.Done()
	return workloadrunner.Result{}, r.runErr
}

func (*cancellationRenewalRuntime) RemovalResourceManifest(request workloadrunner.Request) (workloadrunner.RuntimeResourceManifest, error) {
	manifest := testRuntimeResourceManifest(request.Authority.JobID, request.Authority.AttemptID)
	manifest.NodeID, manifest.BootSessionID = request.Authority.NodeID, request.Authority.BootSessionID
	manifest.FencingToken, manifest.WorkloadClass = request.Authority.FencingToken, request.Authority.WorkloadClass
	manifest.RemovalGeneration = request.Authority.RemovalGeneration
	return manifest, nil
}

func TestOCILateRenewalDirectiveAndShutdownKeepDurableCompletion(t *testing.T) {
	for _, branch := range []string{"shutdown", "cancel", "stop", "restart"} {
		t.Run(branch, func(t *testing.T) {
			claim := spoolTestClaim("late-renewal-" + branch)
			claim.Lease.LeaseTTL = time.Minute
			claim.Job.Spec.Kind = contract.JobKindOCI
			if branch == "stop" || branch == "restart" {
				claim.Job.Spec.Class = contract.JobClassService
			}
			claim.Job.Spec.Execution = contract.ExecutionSpec{OCI: &contract.OCIExecutionSpec{Image: contract.OCIImageSpec{Reference: "example.invalid/test"}}}
			runtime := &cancellationRenewalRuntime{entered: make(chan struct{}), runErr: &ocihelper.AttemptLostError{Authority: ocihelper.AttemptAuthority{AttemptID: claim.Lease.AttemptID}}}
			var completions atomic.Int32
			firstCompletion := make(chan l1.CompletionRequest, 1)
			client, stop := startEvidenceReplayServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case strings.HasSuffix(r.URL.Path, "/lease"):
					select {
					case <-runtime.entered:
					case <-r.Context().Done():
						return
					}
					lease := claim.Lease
					lease.Directive = l1.AttemptDirective(branch)
					_ = json.NewEncoder(w).Encode(lease)
				case strings.HasSuffix(r.URL.Path, "/complete"):
					var request l1.CompletionRequest
					if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
						t.Error(err)
					}
					if completions.Add(1) == 1 {
						firstCompletion <- request
					}
					w.WriteHeader(http.StatusServiceUnavailable)
					_ = json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{Code: contract.ErrorInternal, Retryable: true}})
				default:
					http.NotFound(w, r)
				}
			}), 50*time.Millisecond)
			defer stop()
			defer client.Close()
			outbox, err := newEvidenceOutbox(t.TempDir(), "node", 1024, systemClock{}, 8, time.Hour, time.Millisecond)
			if err != nil {
				t.Fatal(err)
			}
			defer outbox.Close()
			interval := time.Millisecond
			if branch == "shutdown" {
				interval = time.Hour
			}
			lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{client: client, outbox: outbox, runtimes: workloadRuntimeSet{contract.JobKindOCI: runtime}, nodeID: "node", bootSessionID: "boot", clock: systemClock{}, renewalInterval: interval, completionRetry: time.Millisecond, finalizationTimeout: time.Second, observer: newLifecycleObserver(systemClock{}), attemptDeadman: newRecordingDeadmanRenewer(), managedResource: &preflightManagedResource{}})
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			shutdown, stopShutdown := context.WithCancel(ctx)
			defer stopShutdown()
			if branch == "shutdown" {
				go func() {
					select {
					case <-runtime.entered:
						stopShutdown()
					case <-ctx.Done():
					}
				}()
			}
			_, err = lifecycle.execute(shutdown, claim, time.Now())
			if err == nil || completions.Load() == 0 || runtime.reaps.Load() != 1 {
				t.Fatalf("err=%v completions=%d reaps=%d", err, completions.Load(), runtime.reaps.Load())
			}
			sent := <-firstCompletion
			pending := outbox.spool.inspectCompletion(t.Context(), claim.Lease.AttemptID)
			if pending.State != "durable_completion" || !reflect.DeepEqual(pending.Result, sent.Result) || pending.Result.Signal != "terminated" || pending.Result.TerminationCause != contract.TerminationCauseAgent {
				t.Fatalf("lost terminated completion: pending=%+v sent=%+v", pending, sent)
			}
		})
	}
}

func TestOCILateRenewalLoopSettlesLostWithoutCompletionOrSessionRecovery(t *testing.T) {
	claim := spoolTestClaim("late-renewal-loop")
	claim.Lease.LeaseTTL = time.Minute
	claim.Job.Spec.Kind = contract.JobKindOCI
	claim.Job.Spec.Execution = contract.ExecutionSpec{OCI: &contract.OCIExecutionSpec{Image: contract.OCIImageSpec{Reference: "example.invalid/test"}}}
	runtime := &cancellationRenewalRuntime{entered: make(chan struct{})}
	lost := &ocihelper.AttemptLostError{Authority: ocihelper.AttemptAuthority{AttemptID: claim.Lease.AttemptID}}
	// The renewal adapter forwards QueueAttemptRenewalUntil's typed refusal.
	renewer := &recordingDeadmanRenewer{err: lost}
	var completions atomic.Int32
	client, stop := startEvidenceReplayServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/lease") {
			select {
			case <-runtime.entered:
			case <-r.Context().Done():
				return
			}
			_ = json.NewEncoder(w).Encode(claim.Lease)
			return
		}
		completions.Add(1)
		http.Error(w, "unexpected completion", http.StatusInternalServerError)
	}), time.Second)
	defer stop()
	defer client.Close()
	outbox, err := newEvidenceOutbox(t.TempDir(), "node", 1024, systemClock{}, 8, time.Hour, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer outbox.Close()
	lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{client: client, outbox: outbox, runtimes: workloadRuntimeSet{contract.JobKindOCI: runtime}, nodeID: "node", bootSessionID: "boot", clock: systemClock{}, renewalInterval: time.Millisecond, finalizationTimeout: time.Second, observer: newLifecycleObserver(systemClock{}), attemptDeadman: renewer, embargoOCIRuntime: func(workloadrunner.RuntimeGeneration) { t.Error("attempt loss embargoed OCI") }, recoverOCIRuntime: func(context.Context, workloadrunner.RuntimeGeneration) error {
		t.Error("attempt loss recovered session")
		return nil
	}})
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	destination, err := lifecycle.execute(ctx, claim, time.Now())
	var refused *ocihelper.AttemptLostError
	calls, _, _ := renewer.snapshot()
	if destination != errorDestinationAttemptAuthority || !errors.As(err, &refused) || refused != lost || runtime.reaps.Load() != 1 || calls != 1 {
		t.Fatalf("destination=%v err=%v reaps=%d queues=%d", destination, err, runtime.reaps.Load(), calls)
	}
	if completions.Load() != 0 {
		t.Fatalf("lost attempt published completion: %d", completions.Load())
	}
	if _, _, present, err := outbox.spool.durableCompletion(t.Context(), claim.Lease.AttemptID); err != nil || present {
		t.Fatalf("renewal loop queued durable completion: present=%t err=%v", present, err)
	}
}
