package l3

import (
	"context"
	"encoding/json"
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
		// L1 failed the job for the lease loss; the attempt's late exit 0
		// is evidence, not the cause (#604 review).
		"lease lost with a late result": {
			job: l1.Job{State: contract.JobFailed, Attempts: []l1.Attempt{{AttemptID: "a", NodeID: "node-a",
				State: contract.AttemptLost, LateResult: &l1.LateResultEvidence{Kind: l1.LateResultObservation, Late: true,
					Result: &l1.ProcessResult{ExitCode: exitCode(0)}}}}},
			want: "the attempt on node node-a lost its lease (late result: exit 0)",
		},
		"lease lost with no late result": {
			job: l1.Job{State: contract.JobFailed, Attempts: []l1.Attempt{{AttemptID: "a", NodeID: "node-a",
				State: contract.AttemptLost, LateResult: &l1.LateResultEvidence{Kind: l1.LateResultGapKind,
					Gap: &l1.LateResultGap{Reason: l1.LateResultGapObservationWindowExpired}}}}},
			want: "the attempt on node node-a lost its lease (late result unavailable: observation_window_expired)",
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
	if _, err := s.beginDispatch(ctx, record.RunID); err != nil {
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

// TestRequeuedRunIsAttributedToTheNodeThatSettledIt is the #604 review case:
// an attempt on node A fails before it starts and the job is requeued, and a
// later attempt on node B succeeds. The run ran on B, whatever an earlier pass
// recorded while A held the job.
func TestRequeuedRunIsAttributedToTheNodeThatSettledIt(t *testing.T) {
	s, _, _ := recoveryStore(t)
	run, _ := dispatchedRecoveryRun(t, s, "requeued")
	client := &fixedJobClient{jobs: map[string]l1.Job{}}
	reconciler, err := NewReconciler(s, client, ReconcilerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	attributed := func() string {
		t.Helper()
		record, err := s.GetRun(context.Background(), run.RunID)
		if err != nil {
			t.Fatal(err)
		}
		return record.NodeID
	}
	failedOnA := l1.Attempt{AttemptID: "attempt-a", NodeID: "node-a", State: contract.AttemptFailed,
		Result: &l1.ProcessResult{SpawnError: &contract.SpawnFailure{Code: "image_unavailable", Message: "pull failed"}}}

	// Claimed on A: provisional attribution to A.
	client.jobs[run.JobID] = l1.Job{JobID: run.JobID, State: contract.JobClaimed, NodeID: "node-a",
		Attempts: []l1.Attempt{{AttemptID: "attempt-a", NodeID: "node-a", State: contract.AttemptClaimed}}}
	if err := reconciler.ReconcileOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := attributed(); got != "node-a" {
		t.Fatalf("claimed on A: node_id = %q", got)
	}

	// Requeued: no current attempt, and the failed attempt on A is not an
	// answer for a job that is still live.
	client.jobs[run.JobID] = l1.Job{JobID: run.JobID, State: contract.JobQueued, Attempts: []l1.Attempt{failedOnA}}
	if err := reconciler.ReconcileOnce(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Succeeded on B: the settled answer replaces the provisional one.
	client.jobs[run.JobID] = l1.Job{JobID: run.JobID, State: contract.JobSucceeded, Attempts: []l1.Attempt{
		failedOnA,
		{AttemptID: "attempt-b", NodeID: "node-b", State: contract.AttemptSucceeded, Result: &l1.ProcessResult{ExitCode: exitCode(0)}},
	}}
	record := reconcileUntilTerminal(t, s, client, run.RunID)
	if record.Status != contract.RunSucceeded || record.NodeID != "node-b" {
		t.Fatalf("run = %s on %q, want succeeded on node-b", record.Status, record.NodeID)
	}
}

// TestRequeuedJobIsNotAttributedToTheAttemptThatFailed: while the job is live
// and has no current attempt, nothing is recorded.
func TestRequeuedJobIsNotAttributedToTheAttemptThatFailed(t *testing.T) {
	s, _, _ := recoveryStore(t)
	run, _ := dispatchedRecoveryRun(t, s, "requeued-live")
	client := &fixedJobClient{jobs: map[string]l1.Job{run.JobID: {JobID: run.JobID, State: contract.JobQueued,
		Attempts: []l1.Attempt{{AttemptID: "attempt-a", NodeID: "node-a", State: contract.AttemptFailed,
			Result: &l1.ProcessResult{SpawnError: &contract.SpawnFailure{Code: "image_unavailable", Message: "pull failed"}}}}}}}
	reconciler, err := NewReconciler(s, client, ReconcilerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := reconciler.ReconcileOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	record, err := s.GetRun(context.Background(), run.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if record.NodeID != "" {
		t.Fatalf("a requeued job's failed attempt attributed the run to %q", record.NodeID)
	}
}

func TestSanitizeFailureReasonIsOneCappedLine(t *testing.T) {
	got := SanitizeFailureReason("spawn failed:\n  line two\r\n\tline\x00three  ")
	if got != "spawn failed: line two line three" {
		t.Fatalf("sanitized = %q", got)
	}
	long := SanitizeFailureReason(strings.Repeat("x", 500))
	if runes := []rune(long); len(runes) != maxFailureReasonRunes || !strings.HasSuffix(long, "…") {
		t.Fatalf("capped reason is %d runes: %q", len(runes), long)
	}
}

// TestRejectedWriteReasonNamesThePathNotTheValue is the #604 review P2: a
// schema error quotes the rejected value, and a token put in an envelope
// field was copied into the run's failure_reason and onto stderr. The
// recorded reason names where the write failed, never what it held.
func TestRejectedWriteReasonNamesThePathNotTheValue(t *testing.T) {
	s, _, _ := recoveryStore(t)
	ctx := context.Background()
	const secret = "wefty_tok_live_4f9c2a7e1b"
	request := inlineRunRequest("#!/bin/sh\nexit 0\n")
	request.EnvelopeSchema = json.RawMessage(`{"type":"object","properties":{"summary":{"pattern":"^[a-z ]+$"}}}`)
	record, _, err := s.CreateRun(ctx, CreateRunInput{IdempotencyKey: "leaky", Actor: "test", Request: request})
	if err != nil {
		t.Fatal(err)
	}
	token, err := s.ensureRunToken(ctx, record.RunID)
	if err != nil {
		t.Fatal(err)
	}
	scope, err := s.AuthenticateRunToken(ctx, token)
	if err != nil {
		t.Fatal(err)
	}
	envelope := validEnvelope(record.RunID, scope.AttemptID, "leaky")
	envelope.Summary = "result\n" + secret
	raw, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.AppendEnvelope(ctx, scope, raw); err == nil {
		t.Fatal("an envelope failing the caller schema was accepted")
	}
	failed, err := s.GetRun(ctx, record.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if failed.Status != contract.RunFailed {
		t.Fatalf("status = %s", failed.Status)
	}
	if failed.FailureReason != "rejected envelope: schema validation failed at /summary" {
		t.Fatalf("failure_reason = %q", failed.FailureReason)
	}
	if strings.Contains(failed.FailureReason, secret) || strings.ContainsAny(failed.FailureReason, "\r\n") {
		t.Fatalf("failure_reason leaks the value or spans lines: %q", failed.FailureReason)
	}
}

// TestLedgerFailedRunIsAttributedOnceItsJobSettles is the #604 review P2 on
// ledger-driven failures: an attempt on A fails before it starts, the job is
// retried on B, and B reports a failed gate before its L1 job is terminal.
// The gate fails the run at once and the projection loop then skips it, so a
// node recorded while A held the job used to stay. The ledger cannot tell
// which node wrote the gate, so it clears the provisional node and names the
// node once the job settles.
func TestLedgerFailedRunIsAttributedOnceItsJobSettles(t *testing.T) {
	s, _, _ := recoveryStore(t)
	ctx := context.Background()
	run, token := dispatchedRecoveryRun(t, s, "gate-on-b")
	client := &fixedJobClient{jobs: map[string]l1.Job{}}
	reconciler, err := NewReconciler(s, client, ReconcilerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	nodeOf := func() string {
		t.Helper()
		record, err := s.GetRun(ctx, run.RunID)
		if err != nil {
			t.Fatal(err)
		}
		return record.NodeID
	}
	failedOnA := l1.Attempt{AttemptID: "attempt-a", NodeID: "node-a", State: contract.AttemptFailed,
		Result: &l1.ProcessResult{SpawnError: &contract.SpawnFailure{Code: "image_unavailable", Message: "pull failed"}}}
	runningOnB := l1.Attempt{AttemptID: "attempt-b", NodeID: "node-b", State: contract.AttemptRunning}

	client.jobs[run.JobID] = l1.Job{JobID: run.JobID, State: contract.JobClaimed, NodeID: "node-a",
		Attempts: []l1.Attempt{{AttemptID: "attempt-a", NodeID: "node-a", State: contract.AttemptClaimed}}}
	if err := reconciler.ReconcileOnce(ctx); err != nil {
		t.Fatal(err)
	}
	client.jobs[run.JobID] = l1.Job{JobID: run.JobID, State: contract.JobRunning, NodeID: "node-b",
		Attempts: []l1.Attempt{failedOnA, runningOnB}}
	if err := reconciler.ReconcileOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if got := nodeOf(); got != "node-a" {
		t.Fatalf("precondition: provisional node = %q, want the stale node-a", got)
	}

	scope, err := s.AuthenticateRunToken(ctx, token)
	if err != nil {
		t.Fatal(err)
	}
	gate := validGate(run.RunID, scope.AttemptID, "failing-gate")
	gate.Outcome = contract.GateFail
	raw, err := json.Marshal(gate)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.AppendGateResult(ctx, scope, raw); err != nil {
		t.Fatal(err)
	}
	if got := nodeOf(); got != "" {
		t.Fatalf("after the gate: node_id = %q, want the stale node cleared", got)
	}

	// Still running on B: the run stays unattributed rather than guessed.
	if err := reconciler.ReconcileOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if got := nodeOf(); got != "" {
		t.Fatalf("while B runs: node_id = %q", got)
	}

	// B settles the job: the terminal run is named from empty, once.
	runningOnB.State = contract.AttemptSucceeded
	runningOnB.Result = &l1.ProcessResult{ExitCode: exitCode(0)}
	client.jobs[run.JobID] = l1.Job{JobID: run.JobID, State: contract.JobSucceeded, Attempts: []l1.Attempt{failedOnA, runningOnB}}
	if err := reconciler.ReconcileOnce(ctx); err != nil {
		t.Fatal(err)
	}
	record, err := s.GetRun(ctx, run.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if record.NodeID != "node-b" || record.Status != contract.RunFailed || !strings.Contains(record.FailureReason, `gate "tests" reported fail`) {
		t.Fatalf("run = %s on %q (%q), want failed on node-b for the gate", record.Status, record.NodeID, record.FailureReason)
	}
	var pending int
	if err := s.db.QueryRow(`SELECT node_attribution_pending FROM runs WHERE run_id=?`, run.RunID).Scan(&pending); err != nil || pending != 0 {
		t.Fatalf("attribution still pending (%d, %v)", pending, err)
	}
}

// TestPendingNodeAttributionsUseThePartialIndex keeps the per-pass lookup off
// a scan of every run the ledger holds (#604 review P3).
func TestPendingNodeAttributionsUseThePartialIndex(t *testing.T) {
	s, _, _ := recoveryStore(t)
	rows, err := s.db.Query(`EXPLAIN QUERY PLAN ` + pendingNodeAttributionsQuery)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var plan []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		plan = append(plan, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(plan, "; ")
	if !strings.Contains(joined, "runs_node_attribution_pending") {
		t.Fatalf("pending attribution query plan does not use the partial index: %s", joined)
	}
	t.Logf("plan: %s", joined)
}
