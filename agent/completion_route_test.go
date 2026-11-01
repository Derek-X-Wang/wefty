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
	"github.com/Derek-X-Wang/wefty/internal/testwait"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
)

type completionRetryBlockingRuntime struct {
	captureRuntime
	started chan struct{}
}

func (runtime *completionRetryBlockingRuntime) Run(ctx context.Context, request workloadrunner.Request, _ workloadrunner.OutputSink) (workloadrunner.Result, error) {
	close(runtime.started)
	<-ctx.Done()
	return workloadrunner.Result{Outcome: contract.ProcessResult{Signal: "terminated", TerminationCause: contract.TerminationCauseAgent}}, nil
}

// A stopped recovery loop must not replace live completion retries when logs
// are complete. A transient answer must be retried before the lifecycle returns.
func TestShutdownNoPendingLogsRetriesTransientCompletion(t *testing.T) {
	for _, test := range []struct {
		name  string
		cause error
		code  contract.ErrorCode
	}{
		{"shutdown_internal", context.Canceled, contract.ErrorInternal},
		{"cancel_internal", errAttemptDirectiveCancel, contract.ErrorInternal},
		{"shutdown_ledger", context.Canceled, contract.ErrorRunLedgerUnavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			var calls atomic.Int32
			client, stop := startEvidenceReplayServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if !strings.HasSuffix(r.URL.Path, "/complete") {
					http.NotFound(w, r)
					return
				}
				if calls.Add(1) == 1 {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusServiceUnavailable)
					_ = json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{Code: test.code, Retryable: true}})
					return
				}
				_ = json.NewEncoder(w).Encode(l1.Job{})
			}), time.Second)
			defer stop()
			defer client.Close()
			outbox, err := newEvidenceOutbox(t.TempDir(), "stable-node", 1<<20, systemClock{}, 8, time.Hour, time.Millisecond)
			if err != nil {
				t.Fatal(err)
			}
			defer outbox.Close()
			recoveryContext, stopRecovery := context.WithCancel(t.Context())
			outbox.startRecovery(recoveryContext, client, func(err error) { t.Logf("recovery: %v", err) })
			claim := spoolTestClaim("no-logs-retry-" + test.name)
			claim.Job.Spec.Kind = contract.JobKindProcess
			claim.Lease.LeaseTTL = time.Hour
			runtime := &completionRetryBlockingRuntime{started: make(chan struct{})}
			lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{client: client, outbox: outbox,
				runtimes: workloadRuntimeSet{contract.JobKindProcess: runtime}, clock: systemClock{},
				renewalInterval: time.Hour, completionRetry: time.Millisecond, finalizationTimeout: time.Second,
				observer: newLifecycleObserver(systemClock{})})
			ctx, cancel := context.WithCancelCause(t.Context())
			defer cancel(nil)
			done := make(chan error, 1)
			go func() { _, err := lifecycle.execute(ctx, claim, time.Now()); done <- err }()
			select {
			case <-runtime.started:
			case <-time.After(testwait.Bound):
				t.Fatal("runtime did not start")
			}
			// Process shutdown: Run's context ends recovery and the session together.
			stopRecovery()
			outbox.recoveryWG.Wait()
			cancel(test.cause)
			var executeErr error
			select {
			case executeErr = <-done:
			case <-time.After(testwait.Bound):
				t.Fatal("lifecycle did not join")
			}
			receipt := outbox.spool.inspectCompletion(t.Context(), claim.Lease.AttemptID)
			t.Logf("calls=%d err=%v receipt=%s", calls.Load(), executeErr, receipt.State)
			wantErr := error(context.Canceled)
			if errors.Is(test.cause, errAttemptDirectiveCancel) {
				wantErr = nil
			}
			if !errors.Is(executeErr, wantErr) {
				t.Fatalf("execute error = %v, want %v", executeErr, wantErr)
			}
			if calls.Load() != 2 {
				t.Errorf("completion calls = %d, want 2", calls.Load())
			}
			if receipt.State != "delivered" {
				t.Fatalf("shutdown left completion for restart after one transient answer: calls=%d receipt=%s err=%v", calls.Load(), receipt.State, executeErr)
			}
		})
	}
}

func TestCompletionRoute(t *testing.T) {
	for _, test := range []struct {
		name             string
		logsPending      bool
		recoveryStopping bool
		want             completionDeliveryRoute
	}{
		{"complete_logs_running_recovery", false, false, completionRouteLive},
		{"complete_logs_stopped_recovery", false, true, completionRouteLive},
		{"pending_logs_running_recovery", true, false, completionRouteHandoff},
		{"pending_logs_stopped_recovery", true, true, completionRouteInline},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := completionRoute(test.logsPending, test.recoveryStopping); got != test.want {
				t.Fatalf("completionRoute(%t, %t) = %d, want %d", test.logsPending, test.recoveryStopping, got, test.want)
			}
		})
	}
}
