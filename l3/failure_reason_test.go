package l3

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

// fixedJobClient answers GetJob with whatever the test put in jobs. It never
// dispatches: the runs it serves were dispatched by the test itself.
type fixedJobClient struct {
	jobs map[string]l1.Job
}

func (c *fixedJobClient) SubmitJob(context.Context, contract.JobSpec) (l1.Job, error) {
	return l1.Job{}, errors.New("fixedJobClient does not dispatch")
}

func (c *fixedJobClient) GetJob(_ context.Context, jobID string) (l1.Job, error) {
	job, ok := c.jobs[jobID]
	if !ok {
		return l1.Job{}, errors.New("unknown job " + jobID)
	}
	return job, nil
}

func exitCode(code int) *int { return &code }

// reconcileUntilTerminal runs passes until the run settles. A job observed
// as succeeded while its run is still queued is projected through running
// first, so a fast run takes two passes.
func reconcileUntilTerminal(t *testing.T, s *Store, client JobClient, runID string) contract.RunRecord {
	t.Helper()
	reconciler, err := NewReconciler(s, client, ReconcilerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	for pass := 0; pass < 4; pass++ {
		if err := reconciler.ReconcileOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
		record, err := s.GetRun(context.Background(), runID)
		if err != nil {
			t.Fatal(err)
		}
		if record.Status == contract.RunSucceeded || record.Status == contract.RunFailed {
			return record
		}
	}
	t.Fatalf("run %s never settled", runID)
	return contract.RunRecord{}
}

// TestFastRunIsAttributedToItsNode is #604 item 8: L1 names a job's node only
// through its current attempt, and a terminal job has none. A run that
// finished between two reconciler passes was never attributed, so
// `.runs[].node_id` was null for exactly the quick runs the dogfood check ran.
func TestFastRunIsAttributedToItsNode(t *testing.T) {
	s, _, _ := recoveryStore(t)
	run, _ := dispatchedRecoveryRun(t, s, "fast")
	client := &fixedJobClient{jobs: map[string]l1.Job{run.JobID: {
		JobID: run.JobID, State: contract.JobSucceeded,
		Attempts: []l1.Attempt{{AttemptID: "attempt-1", NodeID: "node-a", State: contract.AttemptSucceeded,
			Result: &l1.ProcessResult{ExitCode: exitCode(0)}}},
	}}}
	record := reconcileUntilTerminal(t, s, client, run.RunID)
	if record.Status != contract.RunSucceeded {
		t.Fatalf("status = %s, want succeeded", record.Status)
	}
	if record.NodeID != "node-a" {
		t.Fatalf("node_id = %q, want the node of the job's last attempt", record.NodeID)
	}
	if record.FailureReason != "" {
		t.Fatalf("a succeeded run carries a failure reason: %q", record.FailureReason)
	}
}

// TestFailedRunRecordsWhyItFailed is #604 item 3: the ledger records the
// reason where the failure is decided, so `wait` and `inspect` can print it.
func TestFailedRunRecordsWhyItFailed(t *testing.T) {
	tests := map[string]struct {
		required bool
		job      l1.Job
		want     string
	}{
		"exit code": {
			job: l1.Job{State: contract.JobFailed, Attempts: []l1.Attempt{{AttemptID: "a", NodeID: "node-a",
				State: contract.AttemptFailed, Result: &l1.ProcessResult{ExitCode: exitCode(3)}}}},
			want: "exit 3",
		},
		"agent signal": {
			job: l1.Job{State: contract.JobFailed, Attempts: []l1.Attempt{{AttemptID: "a", NodeID: "node-a",
				State: contract.AttemptFailed, Result: &l1.ProcessResult{Signal: "terminated", TerminationCause: contract.TerminationCauseAgent}}}},
			want: "signal terminated (agent)",
		},
		"pre-start failure": {
			job:  l1.Job{State: contract.JobFailed, FailureReason: "image_unavailable"},
			want: "L1: image_unavailable",
		},
		"missing required envelope": {
			required: true,
			job: l1.Job{State: contract.JobSucceeded, Attempts: []l1.Attempt{{AttemptID: "a", NodeID: "node-a",
				State: contract.AttemptSucceeded, Result: &l1.ProcessResult{ExitCode: exitCode(0)}}}},
			want: "the job exited 0 without reporting the envelope --required-envelope requires",
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			s, _, _ := recoveryStore(t)
			run, _ := dispatchedRecoveryRun(t, s, "reason")
			if test.required {
				if _, err := s.db.Exec(`UPDATE runs SET required_envelope=1 WHERE run_id=?`, run.RunID); err != nil {
					t.Fatal(err)
				}
			}
			job := test.job
			job.JobID = run.JobID
			record := reconcileUntilTerminal(t, s, &fixedJobClient{jobs: map[string]l1.Job{run.JobID: job}}, run.RunID)
			if record.Status != contract.RunFailed {
				t.Fatalf("status = %s, want failed", record.Status)
			}
			if record.FailureReason != test.want {
				t.Fatalf("failure_reason = %q, want %q", record.FailureReason, test.want)
			}
		})
	}
}

// TestDispatchRefusalAndLostJobRecordAReason covers the two failures the
// ledger decides without an L1 job outcome.
func TestDispatchRefusalAndLostJobRecordAReason(t *testing.T) {
	s, _, _ := recoveryStore(t)
	ctx := context.Background()
	record, _, err := s.CreateRun(ctx, CreateRunInput{IdempotencyKey: "refused", Actor: "test", Request: inlineRunRequest("#!/bin/sh\nexit 0\n")})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.beginDispatch(ctx, record.RunID); err != nil {
		t.Fatal(err)
	}
	if err := s.failDispatch(ctx, record.RunID, &Error{Code: contract.ErrorInvalidRequest, Message: "no such kind"}); err != nil {
		t.Fatal(err)
	}
	refused, err := s.GetRun(ctx, record.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if refused.Status != contract.RunFailed || !strings.Contains(refused.FailureReason, "L1 refused the dispatch") ||
		!strings.Contains(refused.FailureReason, "no such kind") {
		t.Fatalf("refused dispatch = %s %q", refused.Status, refused.FailureReason)
	}

	run, _ := dispatchedRecoveryRun(t, s, "lost")
	if changed, err := s.failMissingL1Job(ctx, run); err != nil || !changed {
		t.Fatalf("failMissingL1Job = %t, %v", changed, err)
	}
	lost, err := s.GetRun(ctx, run.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(lost.FailureReason, "L1 lost the dispatched job") {
		t.Fatalf("lost job reason = %q", lost.FailureReason)
	}
}
