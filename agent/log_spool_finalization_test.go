package agent

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
)

// Holding the spool's sole connection after the payload result forces the
// close-request query, rather than an HTTP upload, to consume finalization.
func TestPendingSpoolDeadlinePreservesServiceRestart(t *testing.T) {
	clock := &boundedReplayL1Clock{now: time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)}
	store, err := l1.OpenStore(filepath.Join(t.TempDir(), "l1.sqlite"), l1.StoreOptions{Clock: clock, LeaseDuration: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	identity := fabric.Identity{NodeID: "spool-agent"}
	registration := contract.NodeRegistration{
		NodeID: "spool-node", BootSessionID: "spool-boot",
		OS: "linux", Architecture: "amd64", AgentVersion: "test",
		Capabilities: map[string]bool{"kind:process": true},
	}
	if _, err := store.RegisterNode(t.Context(), identity, registration, l1.NodePolicy{MaxServiceSlots: 1}, true); err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	job, _, err := store.CreateJob(t.Context(), contract.JobSpec{
		SchemaVersion: contract.SchemaVersionV1, DispatchKey: "spool-deadline",
		Kind: contract.JobKindProcess, Class: contract.JobClassService, Restart: contract.RestartAlways,
		Execution: contract.ExecutionSpec{
			Executable: contract.ExecutableSpec{Path: "/bin/true"}, Argv: []string{"true"},
			WorkingDirectory: directory, HandoffDirectory: directory,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimJob(t.Context(), identity.NodeID, registration.NodeID, registration.BootSessionID, contract.JobClassService)
	if err != nil || claim == nil {
		t.Fatalf("claim=%+v err=%v", claim, err)
	}

	client, stopServer := startEvidenceReplayServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("SQL-blocked finalization unexpectedly reached HTTP")
		w.WriteHeader(http.StatusInternalServerError)
	}), time.Second)
	defer stopServer()
	defer client.Close()
	outbox, err := newEvidenceOutbox(t.TempDir(), registration.NodeID, 1024*1024, systemClock{}, 8, time.Hour, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer outbox.Close()
	runtime := &pendingSpoolDeadlineRuntime{spool: outbox.spool}
	defer func() {
		if runtime.connection != nil {
			if err := runtime.connection.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	managedRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	managedResource, err := initializeManagedResource(managedRoot, registration.NodeID, registration.BootSessionID)
	if err != nil {
		t.Fatal(err)
	}
	lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{
		nodeID: registration.NodeID, bootSessionID: registration.BootSessionID,
		managedResource: managedResource, client: client, outbox: outbox,
		runtimes: workloadRuntimeSet{contract.JobKindProcess: runtime},
		clock:    systemClock{}, finalizationTimeout: 25 * time.Millisecond,
	})
	before := outbox.spool.db.Stats()
	result, runErr := lifecycle.runWorkload(t.Context(), *claim)
	after := outbox.spool.db.Stats()
	if runtime.finalization == nil {
		t.Fatalf("probe never reached reaping: result=%+v err=%v", result, runErr)
	}
	t.Logf("SQL wait_count=%d wait_duration=%s finalization Err=%v Cause=%v result=%+v err=%v", after.WaitCount-before.WaitCount, after.WaitDuration-before.WaitDuration, runtime.finalization.Err(), context.Cause(runtime.finalization), result, runErr)
	if after.WaitCount-before.WaitCount != 1 || after.WaitDuration <= before.WaitDuration || context.Cause(runtime.finalization) != context.DeadlineExceeded {
		t.Fatal("invalid probe: pending query did not wait for bounded finalization")
	}
	if err := runtime.connection.Close(); err != nil {
		t.Fatal(err)
	}
	runtime.connection = nil
	pending, err := outbox.spool.pending(t.Context(), claim.Lease.AttemptID, 8)
	if err != nil || len(pending) != 1 || string(pending[0].Bytes) != "before crash" {
		t.Fatalf("pending=%+v err=%v", pending, err)
	}
	completed, err := store.CompleteAttempt(t.Context(), identity.NodeID, job.JobID, claim.Lease.AttemptID, l1.CompletionRequest{FencingToken: claim.Lease.FencingToken, IdempotencyKey: "spool-completion", Result: toL1Result(result), RuntimeQuiescenceEvidence: l1.RuntimeQuiescenceAttempt})
	if err != nil {
		t.Fatal(err)
	}
	retained, err := store.ListJobAttempts(t.Context(), job.JobID)
	if err != nil || len(retained) != 1 || retained[0].Result == nil {
		t.Fatalf("retained attempt results=%+v err=%v", retained, err)
	}
	if persisted := retained[0].Result; persisted.Signal != "killed" || persisted.TerminationCause != contract.TerminationCauseSpontaneous || persisted.OutputError != "" || !persisted.LogEvidenceIncomplete {
		t.Errorf("durable payload result=%+v", persisted)
	}
	t.Logf("completion state=%s bound=%s holds_slot=%t restart_streak=%d", completed.State, completed.ServiceJob.BoundNodeID, completed.ServiceJob.HoldsSlot(completed.State), completed.ServiceJob.RestartStreak)
	if runErr != nil || result.Signal != "killed" || result.TerminationCause != contract.TerminationCauseSpontaneous || result.OutputError != "" || !result.LogEvidenceIncomplete {
		t.Errorf("payload authority was replaced by SQL finalization: result=%+v err=%v", result, runErr)
	}
	if completed.State != contract.JobQueued || completed.ServiceJob.BoundNodeID != registration.NodeID || !completed.ServiceJob.HoldsSlot(completed.State) || completed.ServiceJob.NextRestartAt == nil || completed.ServiceJob.RestartStreak != 1 {
		t.Errorf("service restart reservation=%+v state=%s", completed.ServiceJob, completed.State)
	}
}

type pendingSpoolDeadlineRuntime struct {
	restartableCrashRuntime
	spool        *logSpool
	connection   *sql.Conn
	finalization context.Context
}

func (runtime *pendingSpoolDeadlineRuntime) ReapAndVerify(ctx context.Context, _ workloadrunner.ReapRequest) (workloadrunner.ReapReceipt, error) {
	runtime.finalization = ctx
	connection, err := runtime.spool.db.Conn(ctx)
	if err != nil {
		return workloadrunner.ReapReceipt{}, err
	}
	runtime.connection = connection
	return workloadrunner.ReapReceipt{RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidenceAttempt}, nil
}

func TestPendingSpoolQueryPreservesCancellationProvenance(t *testing.T) {
	spool := openTestLogSpool(t, t.TempDir(), "query-context", 1<<20)
	defer spool.Close()
	for _, test := range []struct {
		name  string
		cause error
	}{
		{"propagated_finalization_deadline", context.DeadlineExceeded},
		{"ordinary_cancellation", context.Canceled},
		{"authority_loss", errors.New("authority lost")},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancelCause(t.Context())
			cancel(test.cause)
			_, err := spool.pending(ctx, "attempt", 8)
			t.Logf("Err=%v Cause=%v query error=%v", ctx.Err(), context.Cause(ctx), err)
			if err != test.cause {
				t.Fatalf("query error=%v, want exact originating cause %v", err, test.cause)
			}
			marked := markLogFinalizationDeadline(ctx, logFinalizationStageUpload, err)
			incomplete, _, remaining := classifyLogFinalizationError(marked)
			if incomplete != (test.cause == context.DeadlineExceeded) || (test.cause != context.DeadlineExceeded && remaining == nil) {
				t.Fatalf("incomplete=%t remaining=%v", incomplete, remaining)
			}
		})
	}
}

func TestSpoolContextNormalizationRetainsGenuineErrors(t *testing.T) {
	ctx, cancel := context.WithCancelCause(t.Context())
	cancel(context.DeadlineExceeded)
	for _, raw := range []error{errors.New("sqlite corruption"), fmt.Errorf("database fault: %w", context.Canceled), errors.Join(context.Canceled, errors.New("disk failure")), errors.Join(context.DeadlineExceeded, errors.New("disk failure")), fmt.Errorf("operation deadline: %w", context.DeadlineExceeded)} {
		got := wrapLogSpoolContextError(ctx, "spool operation", raw)
		incomplete, _, remaining := classifyLogFinalizationError(markLogFinalizationDeadline(ctx, logFinalizationStageUpload, got))
		if incomplete || remaining == nil || !errors.Is(remaining, raw) {
			t.Fatalf("genuine error suppressed: raw=%v got=%v", raw, got)
		}
	}
}

func TestPendingSpoolDatabaseFailureRemainsTerminalAfterDeadline(t *testing.T) {
	spool := openTestLogSpool(t, t.TempDir(), "closed-query", 1<<20)
	if err := spool.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancelCause(t.Context())
	cancel(context.DeadlineExceeded)
	_, err := spool.pending(ctx, "attempt", 8)
	t.Logf("Err=%v Cause=%v closed database error=%v", ctx.Err(), context.Cause(ctx), err)
	incomplete, _, remaining := classifyLogFinalizationError(markLogFinalizationDeadline(ctx, logFinalizationStageUpload, err))
	if err == nil || incomplete || remaining == nil || errors.Is(remaining, context.Canceled) || errors.Is(remaining, context.DeadlineExceeded) {
		t.Fatalf("real database error was suppressed: %v", err)
	}
}
