package l3

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestLinkCancelOwnsLegacyCompletedUnresolved(t *testing.T) {
	for _, outcome := range []string{"settled", "refused"} {
		for _, shape := range []string{"attempted", "outbox-only", "outbox-only-no-attempt"} {
			name := outcome + "-" + shape
			acknowledged := shape != "attempted"
			t.Run(name, func(t *testing.T) {
				h := newIntegrationHarness(t)
				ctx := context.Background()
				run, job := crashWindowRun(t, h, "legacy-completed-unresolved")
				attempt := h.l3Store.clock.Now()
				fixture := linkKnowledgeFixture{cancellation: outcome}
				if shape != "outbox-only-no-attempt" {
					fixture.attemptedAt = &attempt
				}
				if acknowledged {
					fixture.outboxJob = linkID(job.JobID)
					fixture.acknowledged = true
				}
				buildLinkKnowledge(t, h.l3Store, run.RunID, fixture)
				before := snapshotTerminalRun(t, h.l3Store, run.RunID)
				recovery, err := h.l3Store.unrecordedDispatches(ctx, 16)
				if err != nil || len(recovery) != 0 {
					t.Fatalf("recovery claimed cancellation: %+v %v", recovery, err)
				}
				client := &recordingCancelClient{L1Client: h.l1Client}
				r, err := NewReconciler(h.l3Store, client, ReconcilerConfig{})
				if err != nil {
					t.Fatal(err)
				}
				if err := r.ReconcileOnce(ctx); err != nil {
					t.Fatal(err)
				}
				record, err := h.l3Store.GetRun(ctx, run.RunID)
				found, getErr := h.l1Client.GetJob(ctx, job.JobID)
				if err != nil || getErr != nil || record.L1JobID != job.JobID || record.CancelStatus != "settled" || found.Outcome != contract.JobOutcomeCanceled || client.calls != 1 {
					t.Fatalf("cancel did not resolve completed legacy link: run=%+v job=%+v calls=%d errors=%v/%v", record, found, client.calls, err, getErr)
				}
				if after := snapshotTerminalRun(t, h.l3Store, run.RunID); !reflect.DeepEqual(before, after) {
					t.Fatalf("terminal history changed: %+v -> %+v", before, after)
				}
				if err := r.ReconcileOnce(ctx); err != nil || client.calls != 1 {
					t.Fatalf("completed cancel redelivered: calls=%d err=%v", client.calls, err)
				}
			})
		}
	}
}

func TestLinkRecoveryLookupCannotOverwriteNewRegression(t *testing.T) {
	h := newIntegrationHarness(t)
	ctx := context.Background()
	run, job := crashWindowRun(t, h, "recovery-observation")
	r, err := NewReconciler(h.l3Store, h.l1Client, ReconcilerConfig{DispatchLookup: &regressionDuringLinkLookup{h: h, runID: run.RunID, jobID: job.JobID}})
	if err != nil {
		t.Fatal(err)
	}
	_ = r.ReconcileOnce(ctx)
	record, err := h.l3Store.GetRun(ctx, run.RunID)
	execution, getErr := h.l3Store.GetRunExecution(ctx, run.RunID)
	if err != nil || getErr != nil || record.L1JobID != "" || execution.DispatchError == nil || execution.DispatchError.Details["reason"] != l1RegressedReason {
		t.Fatalf("recovery erased newer regression: run=%+v execution=%+v errors=%v/%v", record, execution, err, getErr)
	}
}

type regressionDuringLinkLookup struct {
	h            *integrationHarness
	runID, jobID string
}

func (c *regressionDuringLinkLookup) LookupJobByDispatchKey(ctx context.Context, key string) (l1.Job, error) {
	// Legacy storage is necessary here: no current regression writer can act on
	// a recovery-owned terminal Run. The module still must honor its observation.
	attempt := c.h.l3Store.clock.Now()
	buildLinkKnowledge(c.h.t, c.h.l3Store, c.runID, linkKnowledgeFixture{outboxJob: linkID(c.jobID), acknowledged: true, attemptedAt: &attempt, diagnostic: &contract.APIError{Code: contract.ErrorNotFound, Details: map[string]any{"reason": l1RegressedReason, "l1_job_id": c.jobID}}})
	return c.h.l1Client.LookupJobByDispatchKey(ctx, key)
}

func TestLinkLegacyCompletedAbsenceBacksOffAndSettles(t *testing.T) {
	for _, outcome := range []string{"settled", "refused"} {
		t.Run(outcome, func(t *testing.T) {
			h := newIntegrationHarness(t)
			ctx := context.Background()
			clock := &mutableClock{now: time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)}
			h.l3Store.clock = clock
			run := h.submit(inlineRunRequest("exit 0\n"), "legacy-completed-absence")
			if _, err := h.l3Store.beginDispatch(ctx, run.RunID); err != nil {
				t.Fatal(err)
			}
			failRunAfterDispatchAttempt(t, h.l3Store, run.RunID)
			attempt := clock.Now()
			buildLinkKnowledge(t, h.l3Store, run.RunID, linkKnowledgeFixture{attemptedAt: &attempt, cancellation: outcome})
			before := snapshotTerminalRun(t, h.l3Store, run.RunID)
			lookups := 0
			client := &controlledCancelLookupClient{recordingCancelClient: &recordingCancelClient{L1Client: h.l1Client}}
			client.lookup = func(ctx context.Context, key string) (l1.Job, error) {
				lookups++
				return h.l1Client.LookupJobByDispatchKey(ctx, key)
			}
			r, err := NewReconciler(h.l3Store, client, ReconcilerConfig{})
			if err != nil {
				t.Fatal(err)
			}
			if err := r.ReconcileOnce(ctx); err != nil {
				t.Fatal(err)
			}
			if lookups != 1 {
				t.Fatalf("completed unresolved cancellation not looked up: %d", lookups)
			}
			if err := r.ReconcileOnce(ctx); err != nil || lookups != 1 {
				t.Fatalf("completed unresolved cancellation ignored backoff: %d %v", lookups, err)
			}
			clock.now = clock.now.Add(unrecordedDispatchSettleHorizon)
			if err := r.ReconcileOnce(ctx); err != nil {
				t.Fatal(err)
			}
			record, err := h.l3Store.GetRun(ctx, run.RunID)
			if err != nil || record.CancelStatus != "settled" || record.L1JobID != "" || lookups != 2 {
				t.Fatalf("absence not settled: %+v lookups=%d err=%v", record, lookups, err)
			}
			if err := r.ReconcileOnce(ctx); err != nil || lookups != 2 {
				t.Fatalf("settled absence revisited: %d %v", lookups, err)
			}
			if after := snapshotTerminalRun(t, h.l3Store, run.RunID); !reflect.DeepEqual(before, after) {
				t.Fatalf("terminal history changed: %+v -> %+v", before, after)
			}
		})
	}
}
