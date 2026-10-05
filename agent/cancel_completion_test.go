package agent

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
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
