package l3

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	"net/http"
	"reflect"
	"testing"
	"time"
)

func legacyLinkRun(t *testing.T, s *Store, key string) contract.RunRecord {
	t.Helper()
	run, _, err := s.CreateRun(context.Background(), CreateRunInput{Actor: "test", IdempotencyKey: key, Request: inlineRunRequest("exit 0\n")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.beginDispatch(context.Background(), run.RunID); err != nil {
		t.Fatal(err)
	}
	failRunAfterDispatchAttempt(t, s, run.RunID)
	attempt := s.clock.Now()
	buildLinkKnowledge(t, s, run.RunID, linkKnowledgeFixture{outboxJob: linkID("job-" + key), acknowledged: true, attemptedAt: &attempt})
	return run
}

func reopenLinkStore(t *testing.T, s *Store, path string) *Store {
	t.Helper()
	clock := s.clock
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenStore(path, StoreOptions{Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reopened.Close() })
	return reopened
}

type migratedLinkClient struct {
	JobClient
	read func(context.Context, string) (l1.Job, error)
	ids  []string
}

func (c *migratedLinkClient) GetJob(ctx context.Context, id string) (l1.Job, error) {
	c.ids = append(c.ids, id)
	if c.read != nil {
		return c.read(ctx, id)
	}
	return l1.Job{JobID: id}, nil
}

func TestLegacyRunJobLinkMigrationAndValidation(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(map[bool]string{false: "found", true: "absent"}[missing], func(t *testing.T) {
			s, path, _ := recoveryStore(t)
			run := legacyLinkRun(t, s, "legacy")
			before := snapshotTerminalRun(t, s, run.RunID)
			prepareLegacyLinkUpgrade(t, s)
			s = reopenLinkStore(t, s, path)
			record, err := s.GetRun(context.Background(), run.RunID)
			if err != nil || record.L1JobID != "job-legacy" {
				t.Fatalf("store open did not link legacy run: %+v %v", record, err)
			}
			client := &migratedLinkClient{JobClient: &recordingJobClient{}}
			if missing {
				client.read = func(ctx context.Context, id string) (l1.Job, error) { return l1.Job{}, &JobNotFoundError{JobID: id} }
			}
			r, err := NewReconciler(s, client, ReconcilerConfig{})
			if err != nil {
				t.Fatal(err)
			}
			_ = r.recoverUnrecordedDispatches(context.Background())
			execution, err := s.GetRunExecution(context.Background(), run.RunID)
			if err != nil {
				t.Fatal(err)
			}
			if missing && (execution.DispatchError == nil || execution.DispatchError.Details["reason"] != l1RegressedReason) {
				t.Fatalf("missing job not regressed: %+v", execution)
			}
			if !missing && execution.DispatchError != nil {
				t.Fatalf("found job regressed: %+v", execution)
			}
			if after := snapshotTerminalRun(t, s, run.RunID); !reflect.DeepEqual(before, after) {
				t.Fatalf("terminal facts changed: before=%+v after=%+v", before, after)
			}
			s = reopenLinkStore(t, s, path)
			r, _ = NewReconciler(s, client, ReconcilerConfig{})
			_ = r.recoverUnrecordedDispatches(context.Background())
			if len(client.ids) != 1 {
				t.Fatalf("completed validation count=%d want 1", len(client.ids))
			}
		})
	}
}

func TestLegacyRunJobLinkMigrationRejectsAmbiguousShapes(t *testing.T) {
	for _, shape := range []string{"settled", "regressed", "cancellation-pending", "cancellation-completed", "cancellation-refused", "mismatched-key", "no-dispatch-time", "no-job", "empty-job", "empty-run-job", "staged-token", "malformed-diagnostic", "empty-diagnostic", "diagnostic", "old-cancel-reason", "abbreviated-cancel-reason", "new-cancel-reason", "no-attempt", "active"} {
		t.Run(shape, func(t *testing.T) {
			s, path, _ := recoveryStore(t)
			rejected := legacyLinkRun(t, s, "rejected")
			legacyMigrationShape(t, s, rejected.RunID, shape)
			before := snapshotLegacyLinkStorage(t, s, rejected.RunID)
			control := legacyLinkRun(t, s, "control")
			prepareLegacyLinkUpgrade(t, s)
			s = reopenLinkStore(t, s, path)
			if after := snapshotLegacyLinkStorage(t, s, rejected.RunID); !reflect.DeepEqual(before, after) {
				t.Fatalf("rejected shape changed: before=%v after=%v", before, after)
			}
			got, err := s.GetRun(context.Background(), control.RunID)
			if err != nil || got.L1JobID != "job-control" {
				t.Fatalf("eligible control not linked: %+v %v", got, err)
			}
		})
	}
}

func TestLegacyRunJobLinkMigrationPreservesNodeAttributionAndBackfills(t *testing.T) {
	s, path, _ := recoveryStore(t)
	failed := legacyLinkRun(t, s, "backfill")
	legacyMigrationShape(t, s, failed.RunID, "backfill-attempt")
	succeeded := legacyLinkRun(t, s, "succeeded")
	legacyMigrationShape(t, s, succeeded.RunID, "succeeded")
	known := legacyLinkRun(t, s, "known")
	legacyMigrationShape(t, s, known.RunID, "known-node")
	before := snapshotTerminalRun(t, s, succeeded.RunID)
	prepareLegacyLinkUpgrade(t, s)
	s = reopenLinkStore(t, s, path)
	pending, err := s.pendingNodeAttributions(context.Background())
	if err != nil || len(pending) != 2 {
		t.Fatalf("terminal attribution not preserved: %+v %v", pending, err)
	}
	for _, run := range []contract.RunRecord{failed, succeeded, known} {
		got, err := s.GetRun(context.Background(), run.RunID)
		if err != nil || got.L1JobID == "" {
			t.Fatalf("eligible run not linked: %+v %v", got, err)
		}
	}
	if after := snapshotTerminalRun(t, s, succeeded.RunID); !reflect.DeepEqual(before, after) {
		t.Fatal("succeeded terminal facts changed")
	}
	client := &migratedLinkClient{JobClient: &recordingJobClient{}, read: func(ctx context.Context, id string) (l1.Job, error) { return l1.Job{}, &JobNotFoundError{JobID: id} }}
	r, _ := NewReconciler(s, client, ReconcilerConfig{})
	_ = r.recoverUnrecordedDispatches(context.Background())
	if after := snapshotTerminalRun(t, s, succeeded.RunID); !reflect.DeepEqual(before, after) {
		t.Fatal("succeeded terminal facts changed on regression")
	}
}

func TestLegacyRunJobLinkInterruptedUpgradeResumesAtomically(t *testing.T) {
	for _, phase := range []string{"link", "queue", "marker"} {
		t.Run(phase, func(t *testing.T) {
			s, path, clock := recoveryStore(t)
			run := legacyLinkRun(t, s, "interrupted")
			before := snapshotLegacyLinkStorage(t, s, run.RunID)
			prepareLegacyLinkUpgrade(t, s)
			legacyMigrationFault(t, s, phase, true)
			opened, err := OpenStore(path, StoreOptions{Clock: clock})
			if opened != nil {
				opened.Close()
			}
			if err == nil {
				t.Fatal("injected upgrade interruption did not refuse store open")
			}
			if after := snapshotLegacyLinkStorage(t, s, run.RunID); !reflect.DeepEqual(before, after) {
				t.Fatal("interrupted upgrade partially linked the run")
			}
			legacyMigrationFault(t, s, phase, false)
			s = reopenLinkStore(t, s, path)
			record, err := s.GetRun(context.Background(), run.RunID)
			if err != nil || record.L1JobID != "job-interrupted" {
				t.Fatalf("upgrade did not resume: %+v %v", record, err)
			}
			client := &migratedLinkClient{JobClient: &recordingJobClient{}}
			r, _ := NewReconciler(s, client, ReconcilerConfig{})
			_ = r.recoverUnrecordedDispatches(context.Background())
			if len(client.ids) != 1 {
				t.Fatalf("validation not durable with upgrade: %v", client.ids)
			}
		})
	}
}

func TestLegacyRunJobLinkValidationReadCompletionInterruption(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(map[bool]string{false: "found", true: "absent"}[missing], func(t *testing.T) {
			s, path, _ := recoveryStore(t)
			run := legacyLinkRun(t, s, "completion")
			prepareLegacyLinkUpgrade(t, s)
			s = reopenLinkStore(t, s, path)
			before := snapshotTerminalRun(t, s, run.RunID)
			record, err := s.GetRun(context.Background(), run.RunID)
			if err != nil || record.L1JobID != "job-completion" {
				t.Fatalf("legacy link missing before completion interruption: %+v %v", record, err)
			}
			legacyMigrationFault(t, s, "completion", true)
			client := &migratedLinkClient{JobClient: &recordingJobClient{}}
			if missing {
				client.read = func(ctx context.Context, id string) (l1.Job, error) { return l1.Job{}, &JobNotFoundError{JobID: id} }
			}
			r, _ := NewReconciler(s, client, ReconcilerConfig{})
			if errs := r.recoverUnrecordedDispatches(context.Background()); len(errs) == 0 {
				t.Fatal("completion interruption not reported")
			}
			execution, err := s.GetRunExecution(context.Background(), run.RunID)
			if err != nil || execution.DispatchError != nil {
				t.Fatalf("verdict escaped failed transaction: %+v %v", execution, err)
			}
			legacyMigrationFault(t, s, "completion", false)
			s = reopenLinkStore(t, s, path)
			r, _ = NewReconciler(s, client, ReconcilerConfig{})
			_ = r.recoverUnrecordedDispatches(context.Background())
			_ = r.recoverUnrecordedDispatches(context.Background())
			if len(client.ids) != 2 {
				t.Fatalf("read interrupted before completion was not retried exactly once: %v", client.ids)
			}
			execution, err = s.GetRunExecution(context.Background(), run.RunID)
			if err != nil || missing != (execution.DispatchError != nil) {
				t.Fatalf("resumed verdict incorrect: %+v %v", execution, err)
			}
			if after := snapshotTerminalRun(t, s, run.RunID); !reflect.DeepEqual(before, after) {
				t.Fatal("terminal facts changed")
			}
		})
	}
}

func TestLegacyRunJobLinkConcurrentOpeners(t *testing.T) {
	s, path, clock := recoveryStore(t)
	run := legacyLinkRun(t, s, "concurrent")
	prepareLegacyLinkUpgrade(t, s)
	start := make(chan struct{})
	results := make(chan *Store, 4)
	errs := make(chan error, 4)
	for range 4 {
		go func() {
			<-start
			opened, err := OpenStore(path, StoreOptions{Clock: clock})
			results <- opened
			errs <- err
		}()
	}
	close(start)
	var stores []*Store
	for range 4 {
		opened := <-results
		err := <-errs
		if err != nil {
			t.Fatal(err)
		}
		stores = append(stores, opened)
		t.Cleanup(func() { opened.Close() })
	}
	for _, opened := range stores {
		record, err := opened.GetRun(context.Background(), run.RunID)
		if err != nil || record.L1JobID != "job-concurrent" {
			t.Fatalf("opener missed migration: %+v %v", record, err)
		}
	}
	client := &migratedLinkClient{JobClient: &recordingJobClient{}}
	for _, opened := range stores {
		r, _ := NewReconciler(opened, client, ReconcilerConfig{})
		_ = r.recoverUnrecordedDispatches(context.Background())
	}
	if len(client.ids) != 1 {
		t.Fatalf("duplicate validation work after concurrent opens: %v", client.ids)
	}
	// Once marked, a historical shape introduced later is not migrated again.
	late := legacyLinkRun(t, s, "after-marker")
	s = reopenLinkStore(t, s, path)
	record, err := s.GetRun(context.Background(), late.RunID)
	if err != nil || record.L1JobID != "" {
		t.Fatalf("one-time migration reran: %+v %v", record, err)
	}
}

func TestLegacyRunJobLinkValidationRetriesTransientAnswers(t *testing.T) {
	for _, answer := range []string{"transport", "wrong-identity", "unbound-absence"} {
		t.Run(answer, func(t *testing.T) {
			s, path, clock := recoveryStore(t)
			run := legacyLinkRun(t, s, "retry")
			prepareLegacyLinkUpgrade(t, s)
			s = reopenLinkStore(t, s, path)
			client := &migratedLinkClient{JobClient: &recordingJobClient{}, read: func(ctx context.Context, id string) (l1.Job, error) {
				switch answer {
				case "wrong-identity":
					return l1.Job{JobID: "wrong-job"}, nil
				case "unbound-absence":
					return l1.Job{}, &JobNotFoundError{JobID: "wrong-job"}
				default:
					return l1.Job{}, errors.New("transient L1 failure")
				}
			}}
			r, _ := NewReconciler(s, client, ReconcilerConfig{})
			if errs := r.recoverUnrecordedDispatches(context.Background()); len(errs) == 0 {
				t.Fatal("transient validation failure not reported")
			}
			execution, err := s.GetRunExecution(context.Background(), run.RunID)
			if err != nil || execution.DispatchError != nil || execution.L1JobID != "job-retry" {
				t.Fatalf("transient failure became a verdict: %+v %v", execution, err)
			}
			s = reopenLinkStore(t, s, path)
			r, _ = NewReconciler(s, client, ReconcilerConfig{})
			_ = r.recoverUnrecordedDispatches(context.Background())
			if len(client.ids) != 1 {
				t.Fatalf("retry backoff not durable: %v", client.ids)
			}
			clock.now = clock.now.Add(unrecordedDispatchRetryBase)
			_ = r.recoverUnrecordedDispatches(context.Background())
			if len(client.ids) != 2 {
				t.Fatalf("due validation did not retry: %v", client.ids)
			}
			clock.now = clock.now.Add(unrecordedDispatchRetryBase)
			_ = r.recoverUnrecordedDispatches(context.Background())
			if len(client.ids) != 2 {
				t.Fatal("second backoff did not double")
			}
			clock.now = clock.now.Add(unrecordedDispatchRetryBase)
			client.read = nil
			_ = r.recoverUnrecordedDispatches(context.Background())
			_ = r.recoverUnrecordedDispatches(context.Background())
			if len(client.ids) != 3 {
				t.Fatalf("completed retry not checked once: %v", client.ids)
			}
		})
	}
}

func TestLegacyRunJobLinkValidationRechecksCurrentOwnershipAndIdentity(t *testing.T) {
	for _, change := range []string{"cancellation", "new-identity", "no-job", "mismatched-key", "regressed"} {
		t.Run(change, func(t *testing.T) {
			s, path, _ := recoveryStore(t)
			run := legacyLinkRun(t, s, "ownership")
			prepareLegacyLinkUpgrade(t, s)
			s = reopenLinkStore(t, s, path)
			record, err := s.GetRun(context.Background(), run.RunID)
			if err != nil || record.L1JobID == "" {
				t.Fatalf("legacy link missing before race: %+v %v", record, err)
			}
			var changed []any
			client := &migratedLinkClient{JobClient: &recordingJobClient{}, read: func(ctx context.Context, id string) (l1.Job, error) {
				// These writes prove the L1 read holds no ledger write lock.
				if change == "cancellation" {
					if err := s.requestRunCancellation(ctx, run.RunID, "test"); err != nil {
						t.Fatal(err)
					}
				} else {
					legacyMigrationShape(t, s, run.RunID, change)
				}
				changed = snapshotLegacyLinkStorage(t, s, run.RunID)
				return l1.Job{}, &JobNotFoundError{JobID: id}
			}}
			r, _ := NewReconciler(s, client, ReconcilerConfig{})
			if errs := r.recoverUnrecordedDispatches(context.Background()); len(errs) != 0 {
				t.Fatalf("stale absence reported as a verdict: %v", errs)
			}
			if len(client.ids) != 1 {
				t.Fatalf("migration validation not run: %v", client.ids)
			}
			if after := snapshotLegacyLinkStorage(t, s, run.RunID); !reflect.DeepEqual(changed, after) {
				t.Fatal("stale absence overwrote changed identity or cancel ownership")
			}
		})
	}
}

func TestLegacyRunJobLinkValidationConcurrentVerdicts(t *testing.T) {
	s, path, _ := recoveryStore(t)
	run := legacyLinkRun(t, s, "concurrent-verdict")
	prepareLegacyLinkUpgrade(t, s)
	s = reopenLinkStore(t, s, path)
	positive := &migratedLinkClient{JobClient: &recordingJobClient{}}
	winner, _ := NewReconciler(s, positive, ReconcilerConfig{})
	stale := &migratedLinkClient{JobClient: &recordingJobClient{}, read: func(ctx context.Context, id string) (l1.Job, error) {
		// Another validator completes after this one's captured read and before
		// its negative answer. The completed work cannot be regressed twice.
		if errs := winner.recoverUnrecordedDispatches(ctx); len(errs) != 0 {
			t.Fatal(errs)
		}
		return l1.Job{}, &JobNotFoundError{JobID: id}
	}}
	r, _ := NewReconciler(s, stale, ReconcilerConfig{})
	if errs := r.recoverUnrecordedDispatches(context.Background()); len(errs) != 0 {
		t.Fatalf("completed validation reported stale absence: %v", errs)
	}
	execution, err := s.GetRunExecution(context.Background(), run.RunID)
	if err != nil || execution.DispatchError != nil || len(positive.ids) != 1 || len(stale.ids) != 1 {
		t.Fatalf("stale verdict replaced completion: %+v %v reads=%v/%v", execution, err, positive.ids, stale.ids)
	}
}

func TestLegacyRunJobLinkValidationRecoveryPassIsBounded(t *testing.T) {
	s, path, _ := recoveryStore(t)
	for i := range unrecordedDispatchBatch + 3 {
		legacyLinkRun(t, s, fmt.Sprintf("bounded-%02d", i))
	}
	prepareLegacyLinkUpgrade(t, s)
	s = reopenLinkStore(t, s, path)
	client := &migratedLinkClient{JobClient: &recordingJobClient{}, read: func(ctx context.Context, id string) (l1.Job, error) { return l1.Job{}, errors.New("L1 unavailable") }}
	r, _ := NewReconciler(s, client, ReconcilerConfig{})
	_ = r.ReconcileOnce(context.Background())
	if len(client.ids) != unrecordedDispatchBatch {
		t.Fatalf("validation bypassed bound via recovery or node attribution: %d reads", len(client.ids))
	}
	_ = r.ReconcileOnce(context.Background())
	if len(client.ids) != unrecordedDispatchBatch+3 {
		t.Fatalf("backed-off validation blocked later rows: %v", client.ids)
	}
	_ = r.ReconcileOnce(context.Background())
	if len(client.ids) != unrecordedDispatchBatch+3 {
		t.Fatal("backed-off validation read early")
	}
}

func TestLegacyRunJobLinkRegressionExecutionPreservesTerminalOutcome(t *testing.T) {
	for _, state := range []contract.RunState{contract.RunFailed, contract.RunSucceeded} {
		t.Run(string(state), func(t *testing.T) {
			h := newIntegrationHarness(t)
			run := legacyLinkRun(t, h.l3Store, "route-regression")
			if state == contract.RunSucceeded {
				legacyMigrationShape(t, h.l3Store, run.RunID, "succeeded")
			}
			before := snapshotTerminalRun(t, h.l3Store, run.RunID)
			prepareLegacyLinkUpgrade(t, h.l3Store)
			h.restartLedger()
			h.l3Server.store = h.l3Store
			r, err := NewReconciler(h.l3Store, h.l1Client, ReconcilerConfig{})
			if err != nil {
				t.Fatal(err)
			}
			_ = r.recoverUnrecordedDispatches(context.Background())
			code, _, raw := h.do(h.caller, http.MethodGet, "/v1/runs/"+run.RunID+"/execution", nil, nil)
			var execution RunExecution
			if code != http.StatusOK {
				t.Fatalf("terminal regression execution status=%d body=%s", code, raw)
			}
			if err := json.Unmarshal(raw, &execution); err != nil {
				t.Fatal(err)
			}
			if execution.Job != nil || execution.L1JobID != "job-route-regression" || !isL1Regression(execution.DispatchError, execution.L1JobID) {
				t.Fatalf("regression not visible without vanished Job: %+v", execution)
			}
			if after := snapshotTerminalRun(t, h.l3Store, run.RunID); !reflect.DeepEqual(before, after) {
				t.Fatal("execution read changed terminal outcome")
			}
		})
	}
}

func TestLegacyRunJobLinkValidationBudgetStopsFurtherReads(t *testing.T) {
	s, path, clock := recoveryStore(t)
	for i := range 3 {
		legacyLinkRun(t, s, fmt.Sprintf("budget-validation-%d", i))
	}
	prepareLegacyLinkUpgrade(t, s)
	s = reopenLinkStore(t, s, path)
	client := &migratedLinkClient{JobClient: &recordingJobClient{}, read: func(ctx context.Context, id string) (l1.Job, error) {
		select {
		case <-ctx.Done():
			return l1.Job{}, ctx.Err()
		case <-time.After(10 * time.Second):
			return l1.Job{}, errors.New("deadlock watchdog")
		}
	}}
	r, _ := NewReconciler(s, client, ReconcilerConfig{DispatchRecoveryBudget: 100 * time.Millisecond})
	start := time.Now()
	if err := r.ReconcileOnce(context.Background()); err == nil {
		t.Fatal("budget-cut validation not reported")
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("validation exceeded recovery budget watchdog")
	}
	if len(client.ids) != 1 {
		t.Fatalf("spent budget started further reads: %v", client.ids)
	}
	client.read = nil
	r, _ = NewReconciler(s, client, ReconcilerConfig{})
	_ = r.recoverUnrecordedDispatches(context.Background())
	if len(client.ids) != 3 {
		t.Fatalf("budget-cut row did not back off: %v", client.ids)
	}
	clock.now = clock.now.Add(unrecordedDispatchRetryBase)
	_ = r.recoverUnrecordedDispatches(context.Background())
	if len(client.ids) != 4 {
		t.Fatalf("budget-cut row not retried when due: %v", client.ids)
	}
}
