package l1

import (
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

func TestServiceSlotLifecycle(t *testing.T) {
	t.Parallel()

	boundRunning := ServiceJob{DesiredState: contract.ServiceDesiredRunning, BoundNodeID: "node-1"}
	boundStopped := ServiceJob{DesiredState: contract.ServiceDesiredStopped, BoundNodeID: "node-1"}
	unbound := ServiceJob{DesiredState: contract.ServiceDesiredRunning}
	tests := []struct {
		name    string
		service ServiceJob
		state   contract.JobState
		want    bool
	}{
		{name: "unbound queued", service: unbound, state: contract.JobQueued},
		{name: "restart backoff", service: boundRunning, state: contract.JobQueued, want: true},
		{name: "claimed", service: boundRunning, state: contract.JobClaimed, want: true},
		{name: "running", service: boundRunning, state: contract.JobRunning, want: true},
		{name: "stopping", service: boundStopped, state: contract.JobStopping, want: true},
		{name: "removal pending", service: boundRunning, state: contract.JobRemovalPending, want: true},
		{name: "agent cleaned", service: boundRunning, state: contract.JobAgentCleaned, want: true},
		{name: "stopped", service: boundStopped, state: contract.JobStopped},
		{name: "latched failed", service: boundRunning, state: contract.JobFailed},
		{name: "removed verified", service: boundRunning, state: contract.JobRemovedVerified},
		{name: "force forgotten", service: boundRunning, state: contract.JobForgottenCleanupUnverified},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := test.service.HoldsSlot(test.state); got != test.want {
				t.Fatalf("HoldsSlot(%q) = %t, want %t", test.state, got, test.want)
			}
		})
	}
}

func TestRestartPendingIsComputedAtReadTime(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	future := now.Add(time.Second)
	service := ServiceJob{
		DesiredState:  contract.ServiceDesiredRunning,
		BoundNodeID:   "node-1",
		NextRestartAt: &future,
	}
	if !service.RestartPending(contract.JobQueued, now) {
		t.Fatal("queued desired-running service before next_restart_at must project restart-pending")
	}
	if service.RestartPending(contract.JobQueued, future) {
		t.Fatal("restart-pending must clear at the persisted due time")
	}
	if service.RestartPending(contract.JobRunning, now) {
		t.Fatal("running service must not project restart-pending")
	}
}

func TestImageReconciliationLatchMustRetainCompletionReplay(t *testing.T) {
	store, _, node, job := newProcessStabilityStore(t)
	claim := claimProcessStabilityService(t, store, node)
	queued := failProcessStabilityService(t, store, claim)
	if queued.CurrentAttemptID != claim.Lease.AttemptID {
		t.Fatal("queued service must retain its terminal predecessor before detachment")
	}
	exitCode := 1
	request := CompletionRequest{FencingToken: claim.Lease.FencingToken, IdempotencyKey: "completion:" + claim.Lease.AttemptID,
		Result: ProcessResult{ExitCode: &exitCode}}
	latched, err := store.LatchServiceImageReconciliationFailure(t.Context(), "stability-agent", job.JobID,
		ServiceImageReconciliationFailureRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID,
			Failure: contract.SpawnFailure{Code: contract.SpawnFailureImageUnavailable, Message: "pin unavailable"}})
	if err != nil || latched.State != contract.JobFailed || latched.CurrentAttemptID != "" {
		t.Fatalf("image reconciliation latch=%+v err=%v", latched, err)
	}
	// Repeating the latch after detachment must preserve the existing binding.
	if _, err := store.LatchServiceImageReconciliationFailure(t.Context(), "stability-agent", job.JobID,
		ServiceImageReconciliationFailureRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID,
			Failure: contract.SpawnFailure{Code: contract.SpawnFailureImageUnavailable, Message: "pin unavailable"}}); err != nil {
		t.Fatal(err)
	}
	beforeReplay := snapshotDatabase(t, store.db)
	replay, err := store.CompleteAttemptOutcome(t.Context(), "stability-agent", job.JobID, claim.Lease.AttemptID, request)
	if err != nil || !replay.Replayed || replay.Job.State != contract.JobFailed {
		t.Fatalf("accepted completion refused after image latch: code=%s replay=%+v err=%v", errorCode(err), replay, err)
	}
	if snapshotDatabase(t, store.db) != beforeReplay {
		t.Fatal("completion replay changed the image latch or applied restart accounting twice")
	}
	changed := request
	changed.IdempotencyKey = "changed-completion"
	if _, err := store.CompleteAttemptOutcome(t.Context(), "stability-agent", job.JobID, claim.Lease.AttemptID, changed); errorCode(err) != contract.ErrorIdempotencyConflict {
		t.Fatalf("changed replay err=%v, want idempotency_conflict", err)
	}
	if _, _, err := store.RestartService(t.Context(), job.JobID, ServiceRestartRequest{IdempotencyKey: "restart-after-latch"}); err != nil {
		t.Fatal(err)
	}
	claimProcessStabilityService(t, store, node)
	if _, err := store.CompleteAttemptOutcome(t.Context(), "stability-agent", job.JobID, claim.Lease.AttemptID, request); errorCode(err) != contract.ErrorAttemptMismatch {
		t.Fatalf("superseded completion replay err=%v, want attempt_mismatch", err)
	}
}
