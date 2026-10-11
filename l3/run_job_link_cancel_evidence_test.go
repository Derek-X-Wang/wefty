package l3

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"reflect"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestCancelContradictedIdentityFallback(t *testing.T) {
	for _, shape := range []string{"regressed-outbox", "legacy-regression-diagnostic", "stale-dispatch-not-found"} {
		for _, found := range []bool{true, false} {
			name := "absent"
			if found {
				name = "found"
			}
			t.Run(shape+"/"+name, func(t *testing.T) {
				h := newIntegrationHarness(t)
				ctx := context.Background()
				run, job := crashWindowRun(t, h, "fallback")
				if shape != "stale-dispatch-not-found" {
					recordOutboxOnlyAcknowledgement(t, h.l3Store, run.RunID, job.JobID)
				} else {
					ageDispatchAttemptPastSettleHorizon(t, h.l3Store, run.RunID)
				}
				empty, _ := newEmptyL1LookupClient(t)
				recovery, err := NewReconciler(h.l3Store, empty, ReconcilerConfig{})
				if err != nil {
					t.Fatal(err)
				}
				if err := recovery.ReconcileOnce(ctx); err == nil {
					t.Fatal("expected absence verdict")
				}
				if shape == "legacy-regression-diagnostic" {
					if _, err := h.l3Store.db.Exec(`UPDATE runs SET job_link_settled=0 WHERE run_id=?`, run.RunID); err != nil {
						t.Fatal(err)
					}
				}
				if shape == "stale-dispatch-not-found" {
					if _, err := h.l3Store.db.Exec(`UPDATE dispatch_outbox SET job_id=?, dispatched_ns=? WHERE run_id=?`, job.JobID, h.l3Store.clock.Now().UnixNano(), run.RunID); err != nil {
						t.Fatal(err)
					}
				}
				before, err := h.l3Store.GetRunExecution(ctx, run.RunID)
				if err != nil || before.DispatchError == nil {
					t.Fatalf("verdict = %+v %v", before, err)
				}
				terminal := snapshotTerminalRun(t, h.l3Store, run.RunID)
				client := &recordingCancelClient{L1Client: h.l1Client}
				if !found {
					client.L1Client = empty
				}
				reconciler, err := NewReconciler(h.l3Store, client, ReconcilerConfig{DispatchLookup: &absentDispatchLookup{}})
				if err != nil {
					t.Fatal(err)
				}
				h.l3Server.jobs, h.l3Server.reconciler = client, reconciler
				status, _, body := h.do(h.caller, http.MethodPost, "/v1/runs/"+run.RunID+"/cancel", nil, nil)
				var record contract.RunRecord
				if err := json.Unmarshal(body, &record); err != nil || status != http.StatusOK || record.CancelStatus != "settled" || record.CancelReason != "" {
					t.Fatalf("fallback cancel = %d %s err=%v", status, body, err)
				}
				after, err := h.l3Store.GetRunExecution(ctx, run.RunID)
				if err != nil {
					t.Fatal(err)
				}
				if found {
					observed, err := h.l1Client.GetJob(ctx, job.JobID)
					if err != nil || observed.Outcome != contract.JobOutcomeCanceled || record.L1JobID != job.JobID || after.DispatchError != nil || client.calls != 1 {
						t.Fatalf("identity lookup did not link and cancel: run=%+v job=%+v diagnostic=%+v calls=%d err=%v", record, observed, after.DispatchError, client.calls, err)
					}
				} else if record.L1JobID != "" || client.calls != 0 || !reflect.DeepEqual(before.DispatchError, after.DispatchError) {
					t.Fatalf("absent identity changed evidence: before=%+v after=%+v calls=%d", before, after, client.calls)
				}
				if after := snapshotTerminalRun(t, h.l3Store, run.RunID); after != terminal {
					t.Fatalf("terminal facts/token changed: %+v -> %+v", terminal, after)
				}
			})
		}
	}
}

type controlledCancelLookupClient struct {
	*recordingCancelClient
	lookup func(context.Context, string) (l1.Job, error)
	get    func(context.Context, string) (l1.Job, error)
}

func (c *controlledCancelLookupClient) LookupJobByDispatchKey(ctx context.Context, key string) (l1.Job, error) {
	job, err := c.lookup(ctx, key)
	if err == nil && (job.JobID == "" || job.Spec.DispatchKey != key) {
		return l1.Job{}, invalidL1Success(http.MethodGet, "/v1/dispatch-keys/"+url.PathEscape(key)+"/job", "dispatch lookup")
	}
	return job, err
}
func (c *controlledCancelLookupClient) GetJob(ctx context.Context, id string) (l1.Job, error) {
	if c.get != nil {
		job, err := c.get(ctx, id)
		if err == nil && job.JobID != id {
			return l1.Job{}, invalidL1Success(http.MethodGet, "/v1/jobs/"+url.PathEscape(id), "job projection")
		}
		return job, err
	}
	return c.L1Client.GetJob(ctx, id)
}

func TestCancelLookupDeliversSameJobAcknowledgedDuringLookup(t *testing.T) {
	for _, delivery := range []string{"explicit", "background"} {
		t.Run(delivery, func(t *testing.T) {
			h := newIntegrationHarness(t)
			ctx := context.Background()
			run := h.submit(inlineRunRequest("exit 0\n"), "ack-during-cancel-lookup")
			intents, err := h.l3Store.pendingDispatches(ctx)
			if err != nil || len(intents) != 1 {
				t.Fatalf("intents=%+v %v", intents, err)
			}
			token, err := h.l3Store.beginDispatch(ctx, run.RunID)
			if err != nil {
				t.Fatal(err)
			}
			job, err := h.l1Client.SubmitJob(ctx, intents[0].jobSpec(token))
			if err != nil {
				t.Fatal(err)
			}
			client := &controlledCancelLookupClient{recordingCancelClient: &recordingCancelClient{L1Client: h.l1Client}}
			lookups := 0
			client.lookup = func(ctx context.Context, key string) (l1.Job, error) {
				lookups++
				record, err := h.l3Store.GetRun(ctx, run.RunID)
				if err != nil || record.Status != contract.RunDispatching || record.L1JobID != "" || record.CancelStatus != "pending" {
					t.Fatalf("lookup did not start on an active, unlinked cancellation: %+v %v", record, err)
				}
				// The submit response lands after cancellation reads the row,
				// before the lookup returns the same Job.
				if err := h.l3Store.completeDispatch(ctx, run.RunID, job.JobID); err != nil {
					return l1.Job{}, err
				}
				return h.l1Client.LookupJobByDispatchKey(ctx, key)
			}
			r, err := NewReconciler(h.l3Store, client, ReconcilerConfig{})
			if err != nil {
				t.Fatal(err)
			}
			if delivery == "explicit" {
				h.l3Server.jobs, h.l3Server.reconciler = client, r
				status, _, body := h.do(h.caller, http.MethodPost, "/v1/runs/"+run.RunID+"/cancel", nil, nil)
				var record contract.RunRecord
				if err := json.Unmarshal(body, &record); err != nil || status != http.StatusOK || record.CancelStatus != "settled" || record.CancelReason != "" {
					t.Fatalf("same-Job acknowledgement interrupted HTTP cancel: %d %s %v", status, body, err)
				}
			} else {
				if err := h.l3Store.requestRunCancellation(ctx, run.RunID, h.callerUser); err != nil {
					t.Fatal(err)
				}
				if err := r.ReconcileOnce(ctx); err != nil {
					t.Fatalf("same-Job acknowledgement interrupted cancellation pass: %v", err)
				}
			}
			record, err := h.l3Store.GetRun(ctx, run.RunID)
			observed, getErr := h.l1Client.GetJob(ctx, job.JobID)
			if err != nil || getErr != nil || record.CancelStatus != "settled" || record.CancelReason != "" || record.L1JobID != job.JobID || observed.Outcome != contract.JobOutcomeCanceled || lookups != 1 || client.calls != 1 {
				t.Fatalf("cancel was not delivered in this pass: run=%+v job=%+v lookups=%d cancels=%d errors=%v/%v", record, observed, lookups, client.calls, err, getErr)
			}
		})
	}
}

func TestCancelLookupCannotOverwriteLaterRegression(t *testing.T) {
	h := newIntegrationHarness(t)
	ctx := context.Background()
	run := h.submit(inlineRunRequest("exit 0\n"), "lookup-before-regression")
	intents, err := h.l3Store.pendingDispatches(ctx)
	if err != nil || len(intents) != 1 {
		t.Fatalf("intents=%+v %v", intents, err)
	}
	token, err := h.l3Store.beginDispatch(ctx, run.RunID)
	if err != nil {
		t.Fatal(err)
	}
	job, err := h.l1Client.SubmitJob(ctx, intents[0].jobSpec(token))
	if err != nil {
		t.Fatal(err)
	}
	if err := h.l3Store.requestRunCancellation(ctx, run.RunID, h.callerUser); err != nil {
		t.Fatal(err)
	}
	client := &controlledCancelLookupClient{recordingCancelClient: &recordingCancelClient{L1Client: h.l1Client}}
	client.lookup = func(ctx context.Context, _ string) (l1.Job, error) {
		if err := h.l3Store.completeDispatch(ctx, run.RunID, job.JobID); err != nil {
			return l1.Job{}, err
		}
		record, err := h.l3Store.GetRun(ctx, run.RunID)
		if err != nil {
			return l1.Job{}, err
		}
		changed, err := h.l3Store.failMissingL1Job(ctx, projectedRun{RunID: run.RunID, JobID: job.JobID, State: record.Status})
		if err != nil || !changed {
			t.Fatalf("regression did not commit during lookup: %t %v", changed, err)
		}
		return job, nil
	}
	r, err := NewReconciler(h.l3Store, client, ReconcilerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	pending, err := h.l3Store.pendingRunCancellations(ctx, run.RunID)
	if err != nil || len(pending) != 1 || pending[0].JobID != "" {
		t.Fatalf("pending=%+v %v", pending, err)
	}
	_ = r.cancelRunJob(ctx, pending[0])
	execution, err := h.l3Store.GetRunExecution(ctx, run.RunID)
	if err != nil || execution.DispatchError == nil || execution.DispatchError.Details["reason"] != l1RegressedReason {
		t.Fatalf("stale lookup erased later verdict: %+v %v", execution, err)
	}
	record, err := h.l3Store.GetRun(ctx, run.RunID)
	if err != nil || record.CancelStatus != "pending" || client.calls != 0 {
		t.Fatalf("stale lookup delivered cancellation: %+v calls=%d %v", record, client.calls, err)
	}
}

func TestCancelLookupSupersedesRecoverySettlementDuringLookup(t *testing.T) {
	for _, delivery := range []string{"explicit", "background"} {
		t.Run(delivery, func(t *testing.T) {
			h := newIntegrationHarness(t)
			ctx := context.Background()
			run, job := crashWindowRun(t, h, "settlement-during-cancel-lookup")
			ageDispatchAttemptPastSettleHorizon(t, h.l3Store, run.RunID)
			before := snapshotTerminalRun(t, h.l3Store, run.RunID)
			client := &controlledCancelLookupClient{recordingCancelClient: &recordingCancelClient{L1Client: h.l1Client}}
			lookups := 0
			client.lookup = func(ctx context.Context, key string) (l1.Job, error) {
				lookups++
				observed, err := readDispatchObservation(ctx, h.l3Store.db, run.RunID)
				if err != nil || observed.settled || observed.reason == dispatchNotFoundReason || observed.outboxJob.Valid || observed.runJob.Valid {
					t.Fatalf("lookup did not start before settlement: %+v %v", observed, err)
				}
				outcome, err := h.l3Store.settleUnrecordedDispatch(ctx, unrecordedDispatch{RunID: run.RunID, DispatchKey: key}, h.l3Store.recoveryNow())
				if err != nil || outcome != settleChanged {
					t.Fatalf("recovery crossed cancellation ownership during lookup: %v %v", outcome, err)
				}
				// #777 prevents recovery from writing after cancellation owns the
				// Run. Inject the settlement after the delivery read to retain
				// #776's promise that positive lookup evidence supersedes it.
				payload, err := json.Marshal(contract.APIError{
					Code: contract.ErrorNotFound, Message: "L1 did not find the attempted dispatch; work was not replayed",
					Details: map[string]any{"reason": dispatchNotFoundReason, "dispatch_key": key},
				})
				if err != nil {
					t.Fatal(err)
				}
				tx, err := h.l3Store.db.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback()
				if _, err := tx.ExecContext(ctx, `UPDATE runs SET job_link_settled=1 WHERE run_id=?`, run.RunID); err != nil {
					t.Fatal(err)
				}
				if _, err := tx.ExecContext(ctx, `UPDATE dispatch_outbox SET last_error=?, token_delivery=NULL WHERE run_id=?`, string(payload), run.RunID); err != nil {
					t.Fatal(err)
				}
				if err := tx.Commit(); err != nil {
					t.Fatal(err)
				}
				observed, err = readDispatchObservation(ctx, h.l3Store.db, run.RunID)
				if err != nil || !observed.settled || observed.reason != dispatchNotFoundReason {
					t.Fatalf("injected settlement = %+v %v", observed, err)
				}
				return h.l1Client.LookupJobByDispatchKey(ctx, key)
			}
			r, err := NewReconciler(h.l3Store, client, ReconcilerConfig{})
			if err != nil {
				t.Fatal(err)
			}
			if delivery == "explicit" {
				h.l3Server.jobs, h.l3Server.reconciler = client, r
				status, _, body := h.do(h.caller, http.MethodPost, "/v1/runs/"+run.RunID+"/cancel", nil, nil)
				if status != http.StatusOK {
					t.Fatalf("cancel = %d %s", status, body)
				}
			} else {
				if err := h.l3Store.requestRunCancellation(ctx, run.RunID, h.callerUser); err != nil {
					t.Fatal(err)
				}
				if err := r.ReconcileOnce(ctx); err != nil {
					t.Fatalf("settlement interrupted cancellation pass: %v", err)
				}
			}
			var record contract.RunRecord
			record, err = h.l3Store.GetRun(ctx, run.RunID)
			observed, getErr := h.l1Client.GetJob(ctx, job.JobID)
			if err != nil || getErr != nil || record.CancelStatus != "settled" || record.CancelReason != "" || record.L1JobID != job.JobID || observed.Outcome != contract.JobOutcomeCanceled || lookups != 1 || client.calls != 1 {
				t.Fatalf("cancel was not delivered in this pass: run=%+v job=%+v lookups=%d cancels=%d errors=%v/%v", record, observed, lookups, client.calls, err, getErr)
			}
			execution, err := h.l3Store.GetRunExecution(ctx, run.RunID)
			if err != nil || execution.DispatchError != nil {
				t.Fatalf("fresh lookup left settlement diagnostic: %+v %v", execution, err)
			}
			if after := snapshotTerminalRun(t, h.l3Store, run.RunID); after != before {
				t.Fatalf("lookup changed terminal history or token expiry: %+v -> %+v", before, after)
			}
		})
	}
}

func TestCancelLookupCannotOverwriteRerecordedRegression(t *testing.T) {
	h := newIntegrationHarness(t)
	ctx := context.Background()
	run, job := crashWindowRun(t, h, "regression-rerecorded-during-lookup")
	recordOutboxOnlyAcknowledgement(t, h.l3Store, run.RunID, job.JobID)
	record, err := h.l3Store.GetRun(ctx, run.RunID)
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := h.l3Store.settleUnrecordedDispatch(ctx, unrecordedDispatch{RunID: run.RunID, DispatchKey: record.DispatchKey, OutboxJobID: job.JobID}, h.l3Store.recoveryNow())
	if err != nil || outcome != settled {
		t.Fatalf("initial regression = %v %v", outcome, err)
	}
	before := snapshotTerminalRun(t, h.l3Store, run.RunID)
	client := &controlledCancelLookupClient{recordingCancelClient: &recordingCancelClient{L1Client: h.l1Client}}
	lookups := 0
	client.lookup = func(ctx context.Context, key string) (l1.Job, error) {
		lookups++
		observed, err := readDispatchObservation(ctx, h.l3Store.db, run.RunID)
		if err != nil || observed.reason != l1RegressedReason || observed.diagnosticJob != job.JobID {
			t.Fatalf("lookup did not observe initial regression: %+v %v", observed, err)
		}
		var cause contract.APIError
		if err := json.Unmarshal([]byte(observed.lastError.String), &cause); err != nil {
			t.Fatal(err)
		}
		cause.Message = "newer regression verdict"
		payload, err := json.Marshal(cause)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := h.l3Store.db.ExecContext(ctx, `UPDATE dispatch_outbox SET last_error=? WHERE run_id=?`, string(payload), run.RunID); err != nil {
			return l1.Job{}, err
		}
		current, err := readDispatchObservation(ctx, h.l3Store.db, run.RunID)
		if err != nil || current.reason != observed.reason || current.diagnosticJob != observed.diagnosticJob || current.settled != observed.settled || current.lastError == observed.lastError {
			t.Fatalf("new verdict must change only last_error: before=%+v after=%+v %v", observed, current, err)
		}
		return h.l1Client.LookupJobByDispatchKey(ctx, key)
	}
	r, err := NewReconciler(h.l3Store, client, ReconcilerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.l3Store.requestRunCancellation(ctx, run.RunID, h.callerUser); err != nil {
		t.Fatal(err)
	}
	pending, err := h.l3Store.pendingRunCancellations(ctx, run.RunID)
	if err != nil || len(pending) != 1 || pending[0].JobID != "" {
		t.Fatalf("pending=%+v %v", pending, err)
	}
	if err := r.cancelRunJob(ctx, pending[0]); err == nil {
		t.Error("stale positive lookup accepted a newer regression verdict")
	}
	execution, err := h.l3Store.GetRunExecution(ctx, run.RunID)
	if err != nil || execution.DispatchError == nil || execution.DispatchError.Message != "newer regression verdict" || execution.DispatchError.Details["reason"] != l1RegressedReason || execution.DispatchError.Details["l1_job_id"] != job.JobID {
		t.Fatalf("stale lookup erased newer regression: %+v %v", execution, err)
	}
	record, err = h.l3Store.GetRun(ctx, run.RunID)
	if err != nil || record.L1JobID != "" || record.CancelStatus != "pending" || lookups != 1 || client.calls != 0 {
		t.Fatalf("stale lookup linked or canceled Job: run=%+v lookups=%d cancels=%d %v", record, lookups, client.calls, err)
	}
	if after := snapshotTerminalRun(t, h.l3Store, run.RunID); after != before {
		t.Fatalf("lookup changed terminal history or token expiry: %+v -> %+v", before, after)
	}
}

func TestCancelMalformedDispatchErrorDoesNotBlockBatch(t *testing.T) {
	h := newIntegrationHarness(t)
	ctx := context.Background()
	malformed, first := crashWindowRun(t, h, "malformed")
	other, second := crashWindowRun(t, h, "other")
	for _, runID := range []string{malformed.RunID, other.RunID} {
		if err := h.l3Store.requestRunCancellation(ctx, runID, h.callerUser); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := h.l3Store.db.Exec(`UPDATE runs SET l1_job_id=NULL WHERE run_id=?`, malformed.RunID); err != nil {
		t.Fatal(err)
	}
	if _, err := h.l3Store.db.Exec(`UPDATE dispatch_outbox SET last_error='not-json' WHERE run_id=?`, malformed.RunID); err != nil {
		t.Fatal(err)
	}
	// A malformed diagnostic must also allow a repeated request and acknowledgement.
	if err := h.l3Store.requestRunCancellation(ctx, malformed.RunID, h.callerUser); err != nil {
		t.Errorf("malformed diagnostic blocked request: %v", err)
	}
	if err := h.l3Store.completeDispatch(ctx, malformed.RunID, first.JobID); err != nil {
		t.Errorf("malformed diagnostic blocked acknowledgement: %v", err)
	}
	// Keep the malformed row in the cancellation batch and completion read.
	if _, err := h.l3Store.db.Exec(`UPDATE runs SET l1_job_id=NULL WHERE run_id=?`, malformed.RunID); err != nil {
		t.Fatal(err)
	}
	if _, err := h.l3Store.db.Exec(`UPDATE dispatch_outbox SET last_error='not-json' WHERE run_id=?`, malformed.RunID); err != nil {
		t.Fatal(err)
	}
	pending, err := h.l3Store.pendingRunCancellations(ctx, "")
	if err != nil || len(pending) != 2 {
		t.Fatalf("malformed row blocked peers: pending=%+v err=%v", pending, err)
	}
	r, err := NewReconciler(h.l3Store, h.l1Client, ReconcilerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	for _, cancellation := range pending {
		if err := r.cancelRunJob(ctx, cancellation); err != nil {
			t.Fatal(err)
		}
	}
	for runID, jobID := range map[string]string{malformed.RunID: first.JobID, other.RunID: second.JobID} {
		record, err := h.l3Store.GetRun(ctx, runID)
		job, getErr := h.l1Client.GetJob(ctx, jobID)
		if err != nil || getErr != nil || record.CancelStatus != "settled" || job.Outcome != contract.JobOutcomeCanceled {
			t.Fatalf("batch did not cancel peer: run=%+v job=%+v errors=%v/%v", record, job, err, getErr)
		}
	}
}

func TestCancelIdentityFallbackRechecksAndDefers(t *testing.T) {
	for _, answer := range []string{"transient", "wrong-identity", "later-verdict"} {
		t.Run(answer, func(t *testing.T) {
			h := newIntegrationHarness(t)
			ctx := context.Background()
			run, job := crashWindowRun(t, h, "fallback-recheck")
			recordOutboxOnlyAcknowledgement(t, h.l3Store, run.RunID, job.JobID)
			empty, _ := newEmptyL1LookupClient(t)
			recovery, err := NewReconciler(h.l3Store, empty, ReconcilerConfig{})
			if err != nil {
				t.Fatal(err)
			}
			if err := recovery.ReconcileOnce(ctx); err == nil {
				t.Fatal("expected regression")
			}
			terminal := snapshotTerminalRun(t, h.l3Store, run.RunID)
			calls := 0
			client := &controlledCancelLookupClient{recordingCancelClient: &recordingCancelClient{L1Client: h.l1Client}}
			client.lookup = func(_ context.Context, key string) (l1.Job, error) {
				return l1.Job{}, &DispatchNotFoundError{DispatchKey: key}
			}
			client.get = func(ctx context.Context, id string) (l1.Job, error) {
				calls++
				switch answer {
				case "transient":
					return l1.Job{}, errors.New("transport unavailable")
				case "wrong-identity":
					return l1.Job{JobID: "unrelated-job"}, nil
				default:
					// Commit new evidence while GetJob is in flight: no write lock is held.
					if _, err := h.l3Store.db.ExecContext(ctx, `UPDATE dispatch_outbox SET last_error=? WHERE run_id=?`, `{"code":"not_found","message":"newer verdict","details":{"reason":"l1_regressed","l1_job_id":"`+id+`"}}`, run.RunID); err != nil {
						return l1.Job{}, err
					}
					return empty.GetJob(ctx, id)
				}
			}
			r, err := NewReconciler(h.l3Store, client, ReconcilerConfig{})
			if err != nil {
				t.Fatal(err)
			}
			h.l3Server.jobs, h.l3Server.reconciler = client, r
			status, _, body := h.do(h.caller, http.MethodPost, "/v1/runs/"+run.RunID+"/cancel", nil, nil)
			var record contract.RunRecord
			if err := json.Unmarshal(body, &record); err != nil || status != http.StatusOK || record.CancelStatus != "pending" || calls != 1 || client.calls != 0 || record.L1JobID != "" {
				t.Fatalf("unusable answer did not defer: %d %s gets=%d cancels=%d %v", status, body, calls, client.calls, err)
			}
			if answer == "later-verdict" {
				execution, err := h.l3Store.GetRunExecution(ctx, run.RunID)
				if err != nil || execution.DispatchError == nil || execution.DispatchError.Message != "newer verdict" {
					t.Fatalf("newer verdict lost: %+v %v", execution, err)
				}
			}
			client.get = nil
			status, _, body = h.do(h.caller, http.MethodPost, "/v1/runs/"+run.RunID+"/cancel", nil, nil)
			if err := json.Unmarshal(body, &record); err != nil || status != http.StatusOK || record.CancelStatus != "settled" || record.L1JobID != job.JobID {
				t.Fatalf("retry did not settle: %d %s %v", status, body, err)
			}
			if after := snapshotTerminalRun(t, h.l3Store, run.RunID); after != terminal {
				t.Fatal("fallback changed terminal facts/token expiry")
			}
		})
	}
}

func TestCancelAbsentIdentityFallbackDefersActiveRun(t *testing.T) {
	h := newIntegrationHarness(t)
	ctx := context.Background()
	clock := &mutableClock{now: time.Now().UTC()}
	h.l3Store.clock = clock
	run := h.submit(inlineRunRequest("exit 0\n"), "active-identity-fallback")
	crashing := &loseSubmitResponseClient{JobClient: h.l1Client}
	r, err := NewReconciler(h.l3Store, crashing, ReconcilerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.ReconcileOnce(ctx); err == nil || crashing.submitted.JobID == "" {
		t.Fatalf("lost acknowledgement fixture: job=%q err=%v", crashing.submitted.JobID, err)
	}
	job := crashing.submitted
	// A legacy row can carry a contradicted outbox identity while the Run
	// remains active and unlinked. The fallback must not fail or settle it.
	recordOutboxOnlyAcknowledgement(t, h.l3Store, run.RunID, job.JobID)
	diagnostic := `{"code":"not_found","message":"earlier regression","details":{"reason":"l1_regressed","l1_job_id":"` + job.JobID + `"}}`
	if _, err := h.l3Store.db.Exec(`UPDATE dispatch_outbox SET last_error=? WHERE run_id=?`, diagnostic, run.RunID); err != nil {
		t.Fatal(err)
	}
	before := snapshotTerminalRun(t, h.l3Store, run.RunID)
	if before.Status != contract.RunDispatching || before.EndedNS.Valid || before.TokenExpiry.Valid {
		t.Fatalf("fixture is not an active Run: %+v", before)
	}
	if err := h.l3Store.requestRunCancellation(ctx, run.RunID, h.callerUser); err != nil {
		t.Fatal(err)
	}
	empty, _ := newEmptyL1LookupClient(t)
	client := &controlledCancelLookupClient{recordingCancelClient: &recordingCancelClient{L1Client: h.l1Client}}
	gets := 0
	client.get = func(ctx context.Context, id string) (l1.Job, error) {
		gets++
		return empty.GetJob(ctx, id)
	}
	lookup := &absentDispatchLookup{}
	r, err = NewReconciler(h.l3Store, client, ReconcilerConfig{DispatchLookup: lookup})
	if err != nil {
		t.Fatal(err)
	}
	pending, err := h.l3Store.pendingRunCancellations(ctx, run.RunID)
	if err != nil || len(pending) != 1 || pending[0].JobID != "" {
		t.Fatalf("contradicted identity bypassed lookup: %+v %v", pending, err)
	}
	if err := r.cancelRunJob(ctx, pending[0]); err != nil {
		t.Fatalf("active-Run absence fallback returned an error: %v", err)
	}
	record, err := h.l3Store.GetRun(ctx, run.RunID)
	if err != nil || record.CancelStatus != "pending" || record.L1JobID != "" || client.calls != 0 || gets != 1 || lookup.calls != 1 {
		t.Fatalf("active-Run absence did not defer: run=%+v cancels=%d gets=%d lookups=%d err=%v", record, client.calls, gets, lookup.calls, err)
	}
	if after := snapshotTerminalRun(t, h.l3Store, run.RunID); after != before {
		t.Fatalf("fallback changed active facts/token: %+v -> %+v", before, after)
	}
	execution, err := h.l3Store.GetRunExecution(ctx, run.RunID)
	if err != nil || execution.DispatchError == nil || execution.DispatchError.Message != "earlier regression" {
		t.Fatalf("fallback erased diagnostic: %+v %v", execution, err)
	}
	// Restored L1 evidence must wait for the durable backoff, then resolve
	// through the existing identity-bound lookup and cancellation rules.
	client.get = nil
	if err := r.ReconcileOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if lookup.calls != 1 || client.calls != 0 {
		t.Fatalf("fallback retried before backoff: lookups=%d cancels=%d", lookup.calls, client.calls)
	}
	clock.now = clock.now.Add(unrecordedDispatchRetryBase)
	if err := r.ReconcileOnce(ctx); err != nil {
		t.Fatal(err)
	}
	record, err = h.l3Store.GetRun(ctx, run.RunID)
	observed, getErr := h.l1Client.GetJob(ctx, job.JobID)
	if err != nil || getErr != nil || record.CancelStatus != "settled" || record.CancelReason != "" || record.L1JobID != job.JobID || record.Status != contract.RunFailed || record.FailureReason != "the L1 job was canceled" || observed.Outcome != contract.JobOutcomeCanceled || lookup.calls != 2 || client.calls != 1 {
		t.Fatalf("due pass did not resolve and cancel: run=%+v job=%+v lookups=%d cancels=%d errors=%v/%v", record, observed, lookup.calls, client.calls, err, getErr)
	}
	execution, err = h.l3Store.GetRunExecution(ctx, run.RunID)
	if err != nil || execution.DispatchError != nil {
		t.Fatalf("fresh evidence did not clear diagnostic: %+v %v", execution, err)
	}
}

// Preservation: absence of a conflicting outbox Job is not absence of the
// Run's own Job or of the Job named by its regression diagnostic.
func TestCancelAbsentFallbackPreservesConflictingOwnership(t *testing.T) {
	for _, owner := range []string{"run", "diagnostic"} {
		t.Run(owner, func(t *testing.T) {
			h := newIntegrationHarness(t)
			ctx := context.Background()
			run, job := crashWindowRun(t, h, "conflicting-absent")
			recordOutboxOnlyAcknowledgement(t, h.l3Store, run.RunID, job.JobID)
			empty, _ := newEmptyL1LookupClient(t)
			r, err := NewReconciler(h.l3Store, empty, ReconcilerConfig{})
			if err != nil {
				t.Fatal(err)
			}
			if err := r.ReconcileOnce(ctx); err == nil {
				t.Fatal("expected regression")
			}
			if owner == "run" {
				if _, err := h.l3Store.db.Exec(`UPDATE runs SET l1_job_id='different-owner' WHERE run_id=?`, run.RunID); err != nil {
					t.Fatal(err)
				}
			} else {
				if _, err := h.l3Store.db.Exec(`UPDATE dispatch_outbox SET last_error=? WHERE run_id=?`, `{"code":"not_found","message":"regression","details":{"reason":"l1_regressed","l1_job_id":"different-owner"}}`, run.RunID); err != nil {
					t.Fatal(err)
				}
			}
			before, err := h.l3Store.GetRunExecution(ctx, run.RunID)
			if err != nil {
				t.Fatal(err)
			}
			terminal := snapshotTerminalRun(t, h.l3Store, run.RunID)
			h.l3Server.jobs, h.l3Server.reconciler = empty, r
			status, _, body := h.do(h.caller, http.MethodPost, "/v1/runs/"+run.RunID+"/cancel", nil, nil)
			var record contract.RunRecord
			if err := json.Unmarshal(body, &record); err != nil || status != http.StatusOK || record.CancelStatus != "pending" {
				t.Fatalf("conflicting identity settled: %d %s %v", status, body, err)
			}
			after, err := h.l3Store.GetRunExecution(ctx, run.RunID)
			if err != nil || !reflect.DeepEqual(before.DispatchError, after.DispatchError) {
				t.Fatalf("ownership verdict changed: %+v -> %+v %v", before, after, err)
			}
			if after := snapshotTerminalRun(t, h.l3Store, run.RunID); after != terminal {
				t.Fatal("conflict changed terminal facts/token")
			}
		})
	}
}
