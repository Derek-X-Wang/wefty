package l3

import (
	"context"
	"testing"

	"github.com/Derek-X-Wang/wefty/l1"
)

// ackDuringLookupClient answers the dispatch lookup with an authoritative
// absence, but first lets an acknowledgement for the same dispatch land, the
// way an overlapping dispatch pass's completeDispatch would.
type ackDuringLookupClient struct {
	ack     func(context.Context) error
	lookups int
}

func (c *ackDuringLookupClient) LookupJobByDispatchKey(ctx context.Context, key string) (l1.Job, error) {
	c.lookups++
	if c.lookups == 1 {
		if err := c.ack(ctx); err != nil {
			return l1.Job{}, err
		}
	}
	return l1.Job{}, &DispatchNotFoundError{DispatchKey: key}
}

// crashWindowRun leaves a run ended with one dispatch attempt whose job L1
// committed but L3 never recorded.
func crashWindowRun(t *testing.T, h *integrationHarness, key string) (RunAccepted, l1.Job) {
	t.Helper()
	run := h.submit(inlineRunRequest("#!/bin/sh\nexit 0\n"), key)
	crashing := &loseSubmitResponseClient{JobClient: h.l1Client}
	reconciler, err := NewReconciler(h.l3Store, crashing, ReconcilerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := reconciler.ReconcileOnce(context.Background()); err == nil || crashing.submitted.JobID == "" {
		t.Fatalf("crash fixture = job %q, err %v", crashing.submitted.JobID, err)
	}
	failRunAfterDispatchAttempt(t, h.l3Store, run.RunID)
	return run, crashing.submitted
}

// Settlement is a compare-and-set on the acknowledgement recovery read. An
// acknowledgement that lands between the lookup and the settlement must link
// the run, not be buried under a dispatch_not_found settlement.
func TestAcknowledgementDuringRecoveryLookupLinksTheRun(t *testing.T) {
	for _, testCase := range []struct {
		name string
		ack  func(context.Context, *Store, string, string) error
	}{
		{
			name: "completeDispatch",
			ack: func(ctx context.Context, s *Store, runID, jobID string) error {
				return s.completeDispatch(ctx, runID, jobID)
			},
		},
		{
			// A ledger written before completeDispatch linked terminal runs
			// holds the acknowledgement in the outbox only.
			name: "outbox-only acknowledgement",
			ack: func(ctx context.Context, s *Store, runID, jobID string) error {
				_, err := s.db.ExecContext(ctx, `UPDATE dispatch_outbox SET job_id=?, dispatched_ns=? WHERE run_id=? AND dispatched_ns IS NULL`, jobID, s.clock.Now().UnixNano(), runID)
				return err
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			h := newIntegrationHarness(t)
			ctx := context.Background()
			run, job := crashWindowRun(t, h, "ack-during-lookup")
			lookup := &ackDuringLookupClient{ack: func(ctx context.Context) error {
				return testCase.ack(ctx, h.l3Store, run.RunID, job.JobID)
			}}
			reconciler, err := NewReconciler(h.l3Store, h.l1Client, ReconcilerConfig{DispatchLookup: lookup})
			if err != nil {
				t.Fatal(err)
			}
			_ = reconciler.ReconcileOnce(ctx)
			execution, err := h.l3Store.GetRunExecution(ctx, run.RunID)
			if err != nil {
				t.Fatal(err)
			}
			if execution.L1JobID != job.JobID || execution.DispatchError != nil {
				t.Fatalf("execution = job %q error %+v, want linked to %q with no diagnostic", execution.L1JobID, execution.DispatchError, job.JobID)
			}
		})
	}
}

// An acknowledgement that arrives after recovery settled the dispatch as not
// found still links the run and clears the settled diagnostic.
func TestAcknowledgementAfterSettlementLinksTheRun(t *testing.T) {
	h := newIntegrationHarness(t)
	ctx := context.Background()
	run := h.submit(inlineRunRequest("#!/bin/sh\nexit 0\n"), "ack-after-settlement")
	if _, err := h.l3Store.beginDispatch(ctx, run.RunID); err != nil {
		t.Fatal(err)
	}
	failRunAfterDispatchAttempt(t, h.l3Store, run.RunID)
	before := snapshotTerminalRun(t, h.l3Store, run.RunID)
	reconciler, err := NewReconciler(h.l3Store, h.l1Client, ReconcilerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := reconciler.ReconcileOnce(ctx); err == nil {
		t.Fatal("authoritative absence was not reported")
	}
	settled, err := h.l3Store.GetRunExecution(ctx, run.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if settled.L1JobID != "" || settled.DispatchError == nil || settled.DispatchError.Details["reason"] != dispatchNotFoundReason {
		t.Fatalf("settled execution = %+v", settled)
	}

	if err := h.l3Store.completeDispatch(ctx, run.RunID, "job-late-ack"); err != nil {
		t.Fatal(err)
	}
	linked, err := h.l3Store.GetRunExecution(ctx, run.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if linked.L1JobID != "job-late-ack" || linked.DispatchError != nil {
		t.Fatalf("late acknowledgement left execution = job %q error %+v", linked.L1JobID, linked.DispatchError)
	}
	if after := snapshotTerminalRun(t, h.l3Store, run.RunID); after != before {
		t.Fatalf("late acknowledgement changed the terminal run: before=%+v after=%+v", before, after)
	}
	var pending int
	if err := h.l3Store.db.QueryRow(`SELECT node_attribution_pending FROM runs WHERE run_id=?`, run.RunID).Scan(&pending); err != nil {
		t.Fatal(err)
	}
	if pending != 1 {
		t.Fatalf("node_attribution_pending = %d, want 1 so the node is named", pending)
	}
}
