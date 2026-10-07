package agent

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
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
