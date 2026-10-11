package l3

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestLinkHTTPCancelWakesLegacyCompletedOutbox(t *testing.T) {
	for _, outcome := range []string{"settled", "refused"} {
		for _, attempted := range []bool{true, false} {
			name := outcome + "/no-attempt"
			if attempted {
				name = outcome + "/attempted"
			}
			t.Run(name, func(t *testing.T) {
				h := newIntegrationHarness(t)
				ctx := context.Background()
				run, job := crashWindowRun(t, h, "http-legacy-outbox")
				fixture := linkKnowledgeFixture{outboxJob: linkID(job.JobID), acknowledged: true, cancellation: outcome}
				attempt := h.l3Store.clock.Now()
				if attempted {
					fixture.attemptedAt = &attempt
				}
				buildLinkKnowledge(t, h.l3Store, run.RunID, fixture)
				before := snapshotTerminalRun(t, h.l3Store, run.RunID)
				client := &recordingCancelClient{L1Client: h.l1Client}
				r, err := NewReconciler(h.l3Store, client, ReconcilerConfig{})
				if err != nil {
					t.Fatal(err)
				}
				h.l3Server.jobs, h.l3Server.reconciler = client, r
				status, _, body := h.do(h.caller, http.MethodPost, "/v1/runs/"+run.RunID+"/cancel", nil, nil)
				var record contract.RunRecord
				if err := json.Unmarshal(body, &record); err != nil || status != http.StatusOK {
					t.Fatalf("HTTP %d %s: %v", status, body, err)
				}
				if err := r.ReconcileOnce(ctx); err != nil {
					t.Fatal(err)
				}
				found, err := h.l1Client.GetJob(ctx, job.JobID)
				if err != nil || record.CancelStatus != "settled" || record.L1JobID != job.JobID || client.calls != 1 || found.Outcome != contract.JobOutcomeCanceled {
					t.Fatalf("HTTP link stranded cancellation: run=%+v job=%+v cancel calls=%d err=%v", record, found, client.calls, err)
				}
				if after := snapshotTerminalRun(t, h.l3Store, run.RunID); after != before {
					t.Fatalf("terminal history changed: %+v -> %+v", before, after)
				}
			})
		}
	}
}

func TestLinkCompletedBackoffPreservesOutcome(t *testing.T) {
	for _, outcome := range []string{"settled", "refused", "completed"} {
		t.Run(outcome, func(t *testing.T) {
			h := newIntegrationHarness(t)
			ctx := context.Background()
			run := h.submit(inlineRunRequest("exit 0\n"), "completed-backoff")
			if _, err := h.l3Store.beginDispatch(ctx, run.RunID); err != nil {
				t.Fatal(err)
			}
			failRunAfterDispatchAttempt(t, h.l3Store, run.RunID)
			attempt := h.l3Store.clock.Now()
			fixture := linkKnowledgeFixture{attemptedAt: &attempt, cancellation: outcome}
			if outcome != "settled" {
				fixture.cancellationReason = "recorded delivery reason"
			}
			buildLinkKnowledge(t, h.l3Store, run.RunID, fixture)
			before, err := h.l3Store.GetRun(ctx, run.RunID)
			if err != nil {
				t.Fatal(err)
			}
			if before.CancelStatus != outcome {
				t.Fatalf("fixture: %+v", before)
			}
			r, err := NewReconciler(h.l3Store, h.l1Client, ReconcilerConfig{})
			if err != nil {
				t.Fatal(err)
			}
			if err := r.ReconcileOnce(ctx); err != nil {
				t.Fatal(err)
			}
			after, err := h.l3Store.GetRun(ctx, run.RunID)
			if err != nil || after.CancelStatus != before.CancelStatus || after.CancelReason != before.CancelReason {
				t.Fatalf("backoff rewrote outcome: %s/%q -> %s/%q (%v)", before.CancelStatus, before.CancelReason, after.CancelStatus, after.CancelReason, err)
			}
			pending, err := h.l3Store.pendingRunCancellations(ctx, run.RunID)
			if err != nil || len(pending) != 0 {
				t.Fatalf("backoff not scheduled: %+v %v", pending, err)
			}
		})
	}
}

func TestLinkRecoveryFillsAcknowledgedOutboxIdentity(t *testing.T) {
	h := newIntegrationHarness(t)
	ctx := context.Background()
	run, job := crashWindowRun(t, h, "ack-no-identity")
	attempt := h.l3Store.clock.Now()
	buildLinkKnowledge(t, h.l3Store, run.RunID, linkKnowledgeFixture{attemptedAt: &attempt, acknowledged: true})
	before := snapshotTerminalRun(t, h.l3Store, run.RunID)
	r, err := NewReconciler(h.l3Store, h.l1Client, ReconcilerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.ReconcileOnce(ctx); err != nil {
		t.Fatal(err)
	}
	observed, err := readDispatchObservation(ctx, h.l3Store.db, run.RunID)
	if err != nil || observed.outboxJob.String != job.JobID || !observed.outboxJob.Valid || observed.runJob.String != job.JobID || !observed.dispatched.Valid {
		t.Fatalf("lookup did not fill outbox identity: %+v %v", observed, err)
	}
	if after := snapshotTerminalRun(t, h.l3Store, run.RunID); after != before {
		t.Fatalf("terminal history changed: %+v -> %+v", before, after)
	}
}

// Deliberately bypass client validation to exercise the ledger's own identity
// check. The normal HTTP client and controlled fake reject this answer earlier.
type wrongIdentityFallbackClient struct {
	*recordingCancelClient
	gets int
}

func (c *wrongIdentityFallbackClient) LookupJobByDispatchKey(_ context.Context, key string) (l1.Job, error) {
	return l1.Job{}, &DispatchNotFoundError{DispatchKey: key}
}
func (c *wrongIdentityFallbackClient) GetJob(_ context.Context, _ string) (l1.Job, error) {
	c.gets++
	return l1.Job{JobID: "different-job"}, nil
}

func TestLinkCancelFallbackChecksReturnedIdentity(t *testing.T) {
	h := newIntegrationHarness(t)
	ctx := context.Background()
	run, job := crashWindowRun(t, h, "wrong-fallback-identity")
	attempt := h.l3Store.clock.Now()
	buildLinkKnowledge(t, h.l3Store, run.RunID, linkKnowledgeFixture{attemptedAt: &attempt, outboxJob: linkID(job.JobID), acknowledged: true, cancellation: "pending", diagnostic: &contract.APIError{Details: map[string]any{"reason": l1RegressedReason, "l1_job_id": job.JobID}}})
	before := snapshotTerminalRun(t, h.l3Store, run.RunID)
	client := &wrongIdentityFallbackClient{recordingCancelClient: &recordingCancelClient{L1Client: h.l1Client}}
	r, err := NewReconciler(h.l3Store, client, ReconcilerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	pending, err := h.l3Store.pendingRunCancellations(ctx, run.RunID)
	if err != nil || len(pending) != 1 {
		t.Fatalf("pending=%+v %v", pending, err)
	}
	err = r.cancelRunJob(ctx, pending[0])
	var identityErr *Error
	if !errors.As(err, &identityErr) || identityErr.Message != "resolve canceled dispatch" || identityErr.Cause == nil || identityErr.Cause.Error() != "job lookup conflicts with stored identity" || client.gets != 1 || client.calls != 0 {
		t.Fatalf("ledger identity check not reached: %v gets=%d cancels=%d", err, client.gets, client.calls)
	}
	record, getErr := h.l3Store.GetRun(ctx, run.RunID)
	if getErr != nil || record.L1JobID != "" || record.CancelStatus != "pending" {
		t.Fatalf("wrong identity linked: %+v %v", record, getErr)
	}
	if after := snapshotTerminalRun(t, h.l3Store, run.RunID); after != before {
		t.Fatal("wrong identity changed terminal history")
	}
}
