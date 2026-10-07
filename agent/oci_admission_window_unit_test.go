//go:build darwin || linux

package agent

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
)

// lateAdmissionRuntime opens the helper's admission window as the adapter
// does just before Run, acknowledges Started, and then asks for admission
// even when the acknowledgement failed, as a careless adapter might.
type lateAdmissionRuntime struct {
	captureRuntime
	budget             time.Duration
	startErr, admitErr error
}

func (runtime *lateAdmissionRuntime) Run(ctx context.Context, request workloadrunner.Request, _ workloadrunner.OutputSink) (workloadrunner.Result, error) {
	if request.OCIAdmissionBudget != nil {
		request.OCIAdmissionBudget(runtime.budget)
	}
	runtime.startErr = request.OCIStarted(ctx, workloadrunner.OCIImageObservation{})
	runtime.admitErr = admitReadyOCIHelper(request)
	if runtime.startErr != nil {
		return workloadrunner.Result{Outcome: spawnFailure(contract.SpawnFailureProcessRequest, runtime.startErr)}, runtime.startErr
	}
	exitCode := 0
	return workloadrunner.Result{Outcome: contract.ProcessResult{ExitCode: &exitCode}}, nil
}

// L1 accepts the OCI start, but the acceptance reaches the agent only after
// the helper's admission window has closed, when the helper may already have
// expired the attempt. The attempt is lost: the start counts as failed, the
// attempt is never admitted, and the L1 renewal waiting for admission never
// reaches the helper. Inside the window the same acceptance admits the
// attempt and forwards that renewal.
func TestOCIStartAcceptedAfterAdmissionWindowIsNeverAdmitted(t *testing.T) {
	const budget = time.Second
	for _, late := range []bool{true, false} {
		t.Run(map[bool]string{true: "late", false: "in time"}[late], func(t *testing.T) {
			clock := newManualClock(time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC))
			var startCalls atomic.Int32
			client, stop := startEvidenceReplayServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if strings.HasSuffix(r.URL.Path, "/started") {
					startCalls.Add(1)
					if late {
						// L1 commits, but its answer lands after the window.
						clock.Advance(2 * budget)
					}
					_ = json.NewEncoder(w).Encode(l1.Job{JobID: "job-late-start", State: contract.JobRunning})
					return
				}
				_ = json.NewEncoder(w).Encode(l1.Job{JobID: "job-late-start"})
			}), time.Second)
			defer stop()
			defer client.Close()
			claim := ociHandoffClaim("late-start", "attempt-late-start")
			claim.Job.JobID = "job-late-start"
			claim.Lease.FencingToken = "fence-late-start"
			claim.Lease.LeaseTTL = time.Minute
			deadman := newRecordingDeadmanRenewer()
			admission := newAttemptDeadmanAdmission(deadman, claim, clock, nil)
			// A successful L1 renewal waits for the helper to admit the attempt.
			if err := admission.queue(claim.Lease, clock.Now().Add(time.Minute)); err != nil {
				t.Fatal(err)
			}
			runtime := &lateAdmissionRuntime{budget: budget}
			lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{client: client, clock: clock,
				runtimes: workloadRuntimeSet{contract.JobKindOCI: runtime}, observer: newLifecycleObserver(clock),
				completionRetry: time.Millisecond, finalizationTimeout: time.Second})
			finalization := newAttemptFinalization(t.Context(), time.Second)
			defer finalization.stop()
			if _, err := lifecycle.runWorkloadContexts(t.Context(), finalization, claim, nil, nil, admission); (err != nil) != late {
				t.Fatalf("attempt error = %v, want failure %t", err, late)
			}
			calls, _, _ := deadman.snapshot()
			if !late {
				if runtime.startErr != nil || runtime.admitErr != nil || calls != 1 {
					t.Fatalf("in-time start = %v, admission = %v, renewals = %d; want admitted with its renewal forwarded", runtime.startErr, runtime.admitErr, calls)
				}
				return
			}
			if !errors.Is(runtime.startErr, errOCIAdmissionWindowClosed) || startCalls.Load() != 1 {
				t.Fatalf("late acceptance = %v after %d calls, want a failed start for a closed admission window", runtime.startErr, startCalls.Load())
			}
			if !errors.Is(runtime.admitErr, errOCIAdmissionWindowClosed) || calls != 0 {
				t.Fatalf("admission after the window = %v with %d renewals forwarded, want refusal and none", runtime.admitErr, calls)
			}
		})
	}
}
