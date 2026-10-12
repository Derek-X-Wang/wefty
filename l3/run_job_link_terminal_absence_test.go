package l3

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestCancelTerminalLinkedJobAbsence(t *testing.T) {
	for _, migrated := range []bool{false, true} {
		for _, state := range []contract.RunState{contract.RunFailed, contract.RunSucceeded} {
			for _, absent := range []bool{false, true} {
				t.Run(fmt.Sprintf("migrated=%t/%s/absent=%t", migrated, state, absent), func(t *testing.T) {
					h := newIntegrationHarness(t)
					ctx := context.Background()
					run, job := crashWindowRun(t, h, "cancel-linked")
					jobID := job.JobID
					if absent {
						jobID = "vanished-job"
					}
					attempted := h.l3Store.clock.Now()
					fixture := linkKnowledgeFixture{outboxJob: linkID(jobID), acknowledged: true, attemptedAt: &attempted}
					if !migrated {
						fixture.runJob = linkID(jobID)
					}
					buildLinkKnowledge(t, h.l3Store, run.RunID, fixture)
					if state == contract.RunSucceeded {
						legacyMigrationShape(t, h.l3Store, run.RunID, "succeeded")
					}
					if migrated {
						prepareLegacyLinkUpgrade(t, h.l3Store)
						h.restartLedger()
						h.l3Server.store = h.l3Store
						// Cancel wins before validation is due, including after durable backoff.
						migrationFixtureSQL(t, h.l3Store, `UPDATE run_job_link_validations SET retry_ns=? WHERE run_id=?`, time.Now().Add(time.Hour).UnixNano(), run.RunID)
					}
					before := snapshotTerminalRun(t, h.l3Store, run.RunID)
					code, _, raw := h.do(h.caller, http.MethodPost, "/v1/runs/"+run.RunID+"/cancel", nil, nil)
					if code != http.StatusOK {
						t.Fatalf("cancel status=%d body=%s", code, raw)
					}
					record, err := h.l3Store.GetRun(ctx, run.RunID)
					if err != nil || record.CancelStatus != "settled" || record.CancelReason != "" || record.L1JobID != jobID {
						t.Errorf("cancel did not settle retained link: %+v %v", record, err)
					}
					if absent {
						code, _, raw = h.do(h.caller, http.MethodGet, "/v1/runs/"+run.RunID+"/execution", nil, nil)
						var execution RunExecution
						if code != http.StatusOK {
							t.Errorf("execution status=%d body=%s", code, raw)
						} else if err := json.Unmarshal(raw, &execution); err != nil {
							t.Fatal(err)
						}
						if execution.Job != nil || execution.L1JobID != jobID || !isL1Regression(execution.DispatchError, jobID) {
							t.Errorf("terminal absence not diagnosed: %+v", execution)
						}
					} else {
						found, err := h.l1Client.GetJob(ctx, jobID)
						execution, viewErr := h.l3Store.GetRunExecution(ctx, run.RunID)
						if err != nil || viewErr != nil || found.Outcome != contract.JobOutcomeCanceled || execution.DispatchError != nil {
							t.Errorf("found Job not cancelled normally: %+v %+v %v/%v", found, execution, err, viewErr)
						}
					}
					r, err := NewReconciler(h.l3Store, h.l1Client, ReconcilerConfig{})
					if err != nil {
						t.Fatal(err)
					}
					if err := r.ReconcileOnce(ctx); err != nil {
						t.Fatal(err)
					}
					if after := snapshotTerminalRun(t, h.l3Store, run.RunID); before != after {
						t.Fatalf("terminal facts changed: %+v -> %+v", before, after)
					}
					record, err = h.l3Store.GetRun(ctx, run.RunID)
					if err != nil || record.CancelStatus != "settled" {
						t.Fatalf("settled cancel changed: %+v %v", record, err)
					}
				})
			}
		}
	}
}

func TestMigratedLinkValidationLeavesRecoveryBudget(t *testing.T) {
	s, path, clock := recoveryStore(t)
	legacyLinkRun(t, s, "slow-validation")
	keys := endedUnrecordedRuns(t, s, clock, 1)
	prepareLegacyLinkUpgrade(t, s)
	s = reopenLinkStore(t, s, path)
	client := &migratedLinkClient{JobClient: &recordingJobClient{}, read: func(ctx context.Context, id string) (l1.Job, error) {
		// Spend the entire validation share. Ordinary recovery must still run
		// in this pass, even when ledger work is slow on a loaded runner.
		<-ctx.Done()
		return l1.Job{}, ctx.Err()
	}}
	lookupStarted := make(chan error, 1)
	lookup := &cancelDuringRecoveryLookup{lookup: func(ctx context.Context, key string) (l1.Job, error) {
		lookupStarted <- ctx.Err()
		return l1.Job{JobID: "recovered-job"}, nil
	}}
	// Use the production-sized budget so the half-share bound decides the
	// outcome, with ample time for local ledger work after validation ends.
	r, err := NewReconciler(s, client, ReconcilerConfig{DispatchLookup: lookup, DispatchRecoveryBudget: DefaultDispatchRecoveryBudget})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	passDone := make(chan error, 1)
	go func() { passDone <- r.ReconcileOnce(ctx) }()
	select {
	case err := <-lookupStarted:
		if err != nil {
			t.Fatalf("ordinary recovery started with spent budget: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("watchdog expired waiting for ordinary recovery to start")
	}
	select {
	case err := <-passDone:
		if err == nil {
			t.Fatal("slow validation not reported")
		}
	case <-ctx.Done():
		t.Fatal("watchdog expired waiting for the recovery pass to finish")
	}
	if lookup.calls != 1 {
		t.Fatalf("ordinary recovery lookups=%d, want 1", lookup.calls)
	}
	record, err := s.GetRun(context.Background(), runIDForDispatchKey(t, s, keys[0]))
	if err != nil || record.L1JobID != "recovered-job" {
		t.Fatalf("ordinary recovery did not link: %+v %v", record, err)
	}
}

type terminalAbsentCancelClient struct {
	*recordingCancelClient
	beforeCancel func()
	beforeGet    func()
}

func (c *terminalAbsentCancelClient) CancelJob(ctx context.Context, id string) (l1.Job, error) {
	if c.beforeCancel != nil {
		c.beforeCancel()
	}
	return c.recordingCancelClient.CancelJob(ctx, id)
}

func (c *terminalAbsentCancelClient) GetJob(ctx context.Context, id string) (l1.Job, error) {
	if c.beforeGet != nil {
		c.beforeGet()
	}
	return l1.Job{}, &JobNotFoundError{JobID: id}
}

func TestCancelTerminalAbsenceRechecksObservation(t *testing.T) {
	for _, during := range []string{"cancel", "get"} {
		for _, change := range []string{"identity", "diagnostic"} {
			t.Run(during+"/"+change, func(t *testing.T) {
				s, _, _ := recoveryStore(t)
				ctx := context.Background()
				run := legacyLinkRun(t, s, "cancel-recheck")
				if err := s.completeDispatch(ctx, run.RunID, "job-cancel-recheck"); err != nil {
					t.Fatal(err)
				}
				if err := s.requestRunCancellation(ctx, run.RunID, "test"); err != nil {
					t.Fatal(err)
				}
				pending, err := s.pendingRunCancellations(ctx, run.RunID)
				if err != nil || len(pending) != 1 {
					t.Fatalf("cancel not pending: %+v %v", pending, err)
				}
				mutate := func() {
					attempted := s.clock.Now()
					fixture := linkKnowledgeFixture{runJob: linkID("job-cancel-recheck"), outboxJob: linkID("job-cancel-recheck"), acknowledged: true, attemptedAt: &attempted}
					if change == "identity" {
						fixture.runJob, fixture.outboxJob = linkID("new-job"), linkID("new-job")
					} else {
						fixture.diagnostic = &contract.APIError{Code: contract.ErrorInternal, Message: "newer verdict", Retryable: true}
					}
					buildLinkKnowledge(t, s, run.RunID, fixture)
				}
				client := &terminalAbsentCancelClient{recordingCancelClient: &recordingCancelClient{err: &Error{Code: contract.ErrorNotFound, Message: "job was not found"}}}
				if during == "cancel" {
					client.beforeCancel = mutate
				} else {
					client.beforeGet = mutate
				}
				r, err := NewReconciler(s, client, ReconcilerConfig{})
				if err != nil {
					t.Fatal(err)
				}
				before := snapshotTerminalRun(t, s, run.RunID)
				if err := r.cancelRunJob(ctx, pending[0]); err == nil {
					t.Fatal("stale absence settled delivery")
				}
				record, err := s.GetRun(ctx, run.RunID)
				execution, viewErr := s.GetRunExecution(ctx, run.RunID)
				if err != nil || viewErr != nil || record.CancelStatus != "pending" || isL1Regression(execution.DispatchError, execution.L1JobID) {
					t.Fatalf("stale absence overwrote changed evidence: %+v %+v %v/%v", record, execution, err, viewErr)
				}
				if change == "identity" && record.L1JobID != "new-job" {
					t.Fatalf("new link lost: %+v", record)
				}
				if change == "diagnostic" && (execution.DispatchError == nil || execution.DispatchError.Message != "newer verdict") {
					t.Fatalf("new diagnostic lost: %+v", execution)
				}
				if after := snapshotTerminalRun(t, s, run.RunID); before != after {
					t.Fatal("stale absence changed terminal facts")
				}
			})
		}
	}
}
