package l3

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

// These tests use the production recovery transitions before extraction in
// #778. Cancellation status and execution are observed through ledger reads;
// only the existing backoff helper inspects the durable retry schedule.
func TestRecoveryWritesRespectCancellationOwnership(t *testing.T) {
	for _, status := range []string{"pending", "settled", "refused"} {
		for _, write := range []string{"link", "settle", "backoff", "reread"} {
			t.Run(status+"/"+write, func(t *testing.T) {
				s, _, clock := recoveryStore(t)
				ctx := context.Background()
				key := endedUnrecordedRuns(t, s, clock, 1)[0]
				id := runIDForDispatchKey(t, s, key)
				item, waiting, err := s.unrecordedDispatchFor(ctx, id)
				if err != nil || !waiting {
					t.Fatalf("selected dispatch = %+v %v %v", item, waiting, err)
				}
				if err := s.requestRunCancellation(ctx, id, "test"); err != nil {
					t.Fatal(err)
				}
				switch status {
				case "settled":
					// A completed delivery can leave an unresolved link in an old ledger.
					if err := s.finishRunCancellation(ctx, id); err != nil {
						t.Fatal(err)
					}
				case "refused":
					if err := s.refuseRunCancellation(ctx, id, &Error{Code: contract.ErrorConflict, Message: "cancel refused"}); err != nil {
						t.Fatal(err)
					}
				}
				before, err := s.GetRun(ctx, id)
				if err != nil {
					t.Fatal(err)
				}
				switch write {
				case "link":
					err = s.linkUnrecordedDispatch(ctx, item, "job-discovered")
				case "settle":
					_, err = s.settleUnrecordedDispatch(ctx, item, clock.now.Add(unrecordedDispatchSettleHorizon))
				case "backoff":
					err = s.deferUnrecordedDispatch(ctx, id)
				case "reread":
					_, waiting, err = s.unrecordedDispatchFor(ctx, id)
					if waiting {
						t.Error("recovery reread retained a cancel-owned Run")
					}
				}
				if err != nil {
					t.Fatal(err)
				}
				after, err := s.GetRun(ctx, id)
				if err != nil {
					t.Fatal(err)
				}
				execution, err := s.GetRunExecution(ctx, id)
				if err != nil {
					t.Fatal(err)
				}
				if after.L1JobID != "" || after.CancelStatus != before.CancelStatus || after.CancelReason != before.CancelReason || execution.DispatchError != nil || jobLinkSettled(t, s, id) {
					t.Errorf("recovery %s overrode %s cancel: before=%+v after=%+v execution=%+v", write, status, before, after, execution)
				}
				if failures, retry := jobLinkBackoff(t, s, key); failures != 0 || !retry.Equal(time.Unix(0, 0)) {
					t.Errorf("recovery %s postponed cancel-owned Run: failures=%d retry=%v", write, failures, retry)
				}
			})
		}
	}
}

type cancelDuringRecoveryLookup struct {
	lookup func(context.Context, string) (l1.Job, error)
	calls  int
}

func (c *cancelDuringRecoveryLookup) LookupJobByDispatchKey(ctx context.Context, key string) (l1.Job, error) {
	c.calls++
	return c.lookup(ctx, key)
}

func TestRecoveryRereadStopsWhenCancelTakesOwnership(t *testing.T) {
	s, _, clock := recoveryStore(t)
	ctx := context.Background()
	key := endedUnrecordedRuns(t, s, clock, 1)[0]
	id := runIDForDispatchKey(t, s, key)
	lookup := &cancelDuringRecoveryLookup{}
	lookup.lookup = func(ctx context.Context, requested string) (l1.Job, error) {
		if requested != key {
			t.Fatalf("lookup key = %q, want %q", requested, key)
		}
		if lookup.calls == 1 {
			if err := s.requestRunCancellation(ctx, id, "test"); err != nil {
				return l1.Job{}, err
			}
			// Legacy outbox-only acknowledgement changes recovery's captured identity
			// during the lookup, forcing its settlement to miss and its caller to reread.
			recordOutboxOnlyAcknowledgement(t, s, id, "job-late")
			return l1.Job{}, &DispatchNotFoundError{DispatchKey: requested}
		}
		return l1.Job{JobID: "job-late", Spec: contract.JobSpec{DispatchKey: requested}}, nil
	}
	r, err := NewReconciler(s, &recordingJobClient{}, ReconcilerConfig{DispatchLookup: lookup})
	if err != nil {
		t.Fatal(err)
	}
	if errs := r.recoverUnrecordedDispatches(ctx); len(errs) != 0 {
		t.Fatal(errs)
	}
	record, err := s.GetRun(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if lookup.calls != 1 || record.L1JobID != "" || record.CancelStatus != "pending" {
		t.Fatalf("recovery continued after cancel took ownership: lookups=%d run=%+v", lookup.calls, record)
	}
	if item, waiting, err := s.unrecordedDispatchFor(ctx, id); err != nil || waiting {
		t.Fatalf("cancel-owned reread = %+v %v %v", item, waiting, err)
	}
}

func TestRecoveryLookupAbsenceOrFailureCannotOverridePendingCancel(t *testing.T) {
	for _, answer := range []string{"absence", "transient"} {
		t.Run(answer, func(t *testing.T) {
			s, _, clock := recoveryStore(t)
			ctx := context.Background()
			key := endedUnrecordedRuns(t, s, clock, 1)[0]
			id := runIDForDispatchKey(t, s, key)
			clock.now = clock.now.Add(unrecordedDispatchSettleHorizon)
			lookup := &cancelDuringRecoveryLookup{lookup: func(ctx context.Context, requested string) (l1.Job, error) {
				if requested != key {
					t.Fatalf("lookup key = %q, want %q", requested, key)
				}
				// This write completing inside the callback also proves no L1 read holds
				// the ledger write lock. Test timeout is only a deadlock watchdog.
				if err := s.requestRunCancellation(ctx, id, "test"); err != nil {
					return l1.Job{}, err
				}
				if answer == "absence" {
					return l1.Job{}, &DispatchNotFoundError{DispatchKey: requested}
				}
				return l1.Job{}, errors.New("injected transport failure")
			}}
			r, err := NewReconciler(s, &recordingJobClient{}, ReconcilerConfig{DispatchLookup: lookup})
			if err != nil {
				t.Fatal(err)
			}
			errs := r.recoverUnrecordedDispatches(ctx)
			if answer == "transient" && len(errs) == 0 {
				t.Fatal("transport failure was lost")
			}
			execution, err := s.GetRunExecution(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			if jobLinkSettled(t, s, id) || execution.DispatchError != nil {
				t.Fatalf("recovery settled cancel-owned Run: %+v", execution)
			}
			if failures, _ := jobLinkBackoff(t, s, key); failures != 0 {
				t.Fatalf("recovery backed off cancel-owned Run: %d", failures)
			}
		})
	}
}

func TestRecoveryDiscoveryReachesCancel(t *testing.T) {
	for _, scenario := range []string{"pending", "settled", "refused", "settled-terminal"} {
		t.Run(scenario, func(t *testing.T) {
			status := scenario
			if scenario == "settled-terminal" {
				status = "settled"
			}
			h := newIntegrationHarness(t)
			ctx := context.Background()
			clock := &mutableClock{now: time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)}
			h.l3Store.clock = clock
			run := h.submit(inlineRunRequest("exit 0\n"), "recovery-cancel-discovery")
			intents, err := h.l3Store.pendingDispatches(ctx)
			if err != nil || len(intents) != 1 {
				t.Fatalf("dispatch intents = %+v %v", intents, err)
			}
			token, err := h.l3Store.beginDispatch(ctx, run.RunID)
			if err != nil {
				t.Fatal(err)
			}
			if scenario != "settled" {
				failRunAfterDispatchAttempt(t, h.l3Store, run.RunID)
			}
			clock.now = clock.now.Add(unrecordedDispatchSettleHorizon)
			var discovered l1.Job
			var terminal terminalRunSnapshot
			lookup := &cancelDuringRecoveryLookup{lookup: func(ctx context.Context, key string) (l1.Job, error) {
				if key != intents[0].DispatchKey {
					t.Fatalf("lookup key = %q, want %q", key, intents[0].DispatchKey)
				}
				if err := h.l3Store.requestRunCancellation(ctx, run.RunID, h.callerUser); err != nil {
					return l1.Job{}, err
				}
				switch status {
				case "settled":
					if err := h.l3Store.cancelAbsentDispatch(ctx, run.RunID, clock.now); err != nil {
						return l1.Job{}, err
					}
				case "refused":
					if err := h.l3Store.refuseRunCancellation(ctx, run.RunID, &Error{Code: contract.ErrorConflict, Message: "cancel refused before confirmation"}); err != nil {
						return l1.Job{}, err
					}
				}
				record, err := h.l3Store.GetRun(ctx, run.RunID)
				if err != nil || record.CancelStatus != status {
					t.Fatalf("cancel fixture = %+v %v", record, err)
				}
				if scenario == "settled" && record.FailureReason != "canceled before dispatch was confirmed" {
					t.Fatalf("settled reason = %q", record.FailureReason)
				}
				terminal = snapshotTerminalRun(t, h.l3Store, run.RunID)
				clock.now = clock.now.Add(time.Minute)
				// The saved in-flight submit commits after cancel's absence settlement.
				discovered, err = h.l1Client.SubmitJob(ctx, intents[0].jobSpec(token))
				if err != nil {
					return l1.Job{}, err
				}
				if status == "refused" {
					// An older outbox acknowledgement must not prevent a newly
					// linked lookup from reopening refused cancellation delivery.
					recordOutboxOnlyAcknowledgement(t, h.l3Store, run.RunID, discovered.JobID)
				}
				return h.l1Client.LookupJobByDispatchKey(ctx, key)
			}}
			r, err := NewReconciler(h.l3Store, h.l1Client, ReconcilerConfig{DispatchLookup: lookup})
			if err != nil {
				t.Fatal(err)
			}
			// The settled case hand-builds a non-terminal recovery item that
			// production never selects, to check the canceled-before-confirmation
			// reason. settled-terminal is the reachable case; it and the other
			// cases use recovery's actual terminal-Run selector.
			item := unrecordedDispatch{RunID: run.RunID, DispatchKey: intents[0].DispatchKey}
			if scenario == "settled" {
				item.observed, err = readDispatchObservation(ctx, h.l3Store.db, item.RunID)
				if err != nil {
					t.Fatal(err)
				}
				if err := r.recoverUnrecordedDispatch(ctx, ctx, item); err != nil {
					t.Fatal(err)
				}
			} else if errs := r.recoverUnrecordedDispatches(ctx); len(errs) != 0 {
				t.Fatal(errs)
			}
			record, err := h.l3Store.GetRun(ctx, run.RunID)
			if err != nil {
				t.Fatal(err)
			}
			job, err := h.l1Client.GetJob(ctx, discovered.JobID)
			if err != nil {
				t.Fatal(err)
			}
			if record.L1JobID != discovered.JobID || record.CancelStatus != "settled" || job.Outcome != contract.JobOutcomeCanceled {
				t.Fatalf("discovery did not reach cancel: run=%+v job=%+v", record, job)
			}
			if after := snapshotTerminalRun(t, h.l3Store, run.RunID); after != terminal {
				t.Fatalf("discovery changed terminal history: before=%+v after=%+v", terminal, after)
			}
			if lookup.calls != 1 {
				t.Fatalf("discovery triggered redundant dispatch lookup: %d", lookup.calls)
			}
		})
	}
}

// The lookup can exhaust its budget after finding a Job. The observation and
// reopened delivery must survive; the next pass delivers without looking up
// the dispatch again. Local time advances use only the fake ledger clock.
func TestRecoveryDiscoverySurvivesSpentBudget(t *testing.T) {
	h := newIntegrationHarness(t)
	ctx := context.Background()
	clock := &mutableClock{now: time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)}
	h.l3Store.clock = clock
	run, job := crashWindowRun(t, h, "cancel-budget-discovery")
	clock.now = clock.now.Add(unrecordedDispatchSettleHorizon)
	remote, cancel := context.WithCancel(ctx)
	defer cancel()
	lookup := &cancelDuringRecoveryLookup{lookup: func(ctx context.Context, key string) (l1.Job, error) {
		if err := h.l3Store.requestRunCancellation(ctx, run.RunID, h.callerUser); err != nil {
			return l1.Job{}, err
		}
		if err := h.l3Store.cancelAbsentDispatch(ctx, run.RunID, clock.now); err != nil {
			return l1.Job{}, err
		}
		observed, err := h.l1Client.LookupJobByDispatchKey(ctx, key)
		cancel()
		return observed, err
	}}
	r, err := NewReconciler(h.l3Store, h.l1Client, ReconcilerConfig{DispatchLookup: lookup})
	if err != nil {
		t.Fatal(err)
	}
	if errs := r.recoverUnrecordedDispatchesRemote(ctx, remote); len(errs) != 0 {
		t.Fatal(errs)
	}
	record, err := h.l3Store.GetRun(ctx, run.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if record.L1JobID != job.JobID || record.CancelStatus != "pending" {
		t.Fatalf("spent budget lost cancel handoff: %+v", record)
	}
	observed, err := h.l1Client.GetJob(ctx, job.JobID)
	if err != nil || observed.State != contract.JobQueued {
		t.Fatalf("Job before next pass = %+v %v", observed, err)
	}
	if err := r.ReconcileOnce(ctx); err != nil {
		t.Fatal(err)
	}
	observed, err = h.l1Client.GetJob(ctx, job.JobID)
	record, readErr := h.l3Store.GetRun(ctx, run.RunID)
	if err != nil || readErr != nil || observed.Outcome != contract.JobOutcomeCanceled || record.CancelStatus != "settled" || lookup.calls != 1 {
		t.Fatalf("durable handoff = run %+v job %+v lookups %d errors %v %v", record, observed, lookup.calls, err, readErr)
	}
}

func TestRecoveryLinkRechecksAcknowledgedIdentity(t *testing.T) {
	for _, cancelOwned := range []bool{false, true} {
		t.Run(map[bool]string{false: "recovery-owned", true: "cancel-owned"}[cancelOwned], func(t *testing.T) {
			s, _, clock := recoveryStore(t)
			ctx := context.Background()
			key := endedUnrecordedRuns(t, s, clock, 1)[0]
			id := runIDForDispatchKey(t, s, key)
			lookup := &cancelDuringRecoveryLookup{lookup: func(ctx context.Context, requested string) (l1.Job, error) {
				if requested != key {
					t.Fatalf("lookup key = %q, want %q", requested, key)
				}
				if cancelOwned {
					if err := s.requestRunCancellation(ctx, id, "test"); err != nil {
						return l1.Job{}, err
					}
				}
				// An acknowledgement of another identity lands during the L1
				// read. The stale positive observation must never replace it.
				recordOutboxOnlyAcknowledgement(t, s, id, "job-acknowledged")
				return l1.Job{JobID: "job-observed", Spec: contract.JobSpec{DispatchKey: requested}}, nil
			}}
			r, err := NewReconciler(s, &recordingJobClient{}, ReconcilerConfig{DispatchLookup: lookup})
			if err != nil {
				t.Fatal(err)
			}
			if errs := r.recoverUnrecordedDispatches(ctx); len(errs) == 0 {
				t.Error("conflicting observation was accepted")
			}
			record, err := s.GetRun(ctx, id)
			if err != nil || record.L1JobID != "" {
				t.Fatalf("conflicting observation changed Run identity: %+v %v", record, err)
			}
		})
	}
}

// A Job found by recovery is lookup evidence even when cancellation owns the
// receiving write. A regression written during lookup refuses the handoff;
// a settlement written during lookup must link and reopen delivery.
func TestRecoveryCancelHandoffCannotOverwriteLaterVerdict(t *testing.T) {
	for _, verdict := range []string{"regression", "changed-regression", "settlement"} {
		t.Run(verdict, func(t *testing.T) {
			h := newIntegrationHarness(t)
			ctx := context.Background()
			clock := &mutableClock{now: time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)}
			h.l3Store.clock = clock
			run, job := crashWindowRun(t, h, "handoff-newer-verdict")
			if verdict != "settlement" {
				recordOutboxOnlyAcknowledgement(t, h.l3Store, run.RunID, job.JobID)
			}
			if verdict == "changed-regression" {
				if _, err := h.l3Store.db.ExecContext(ctx, `UPDATE dispatch_outbox SET last_error=? WHERE run_id=?`,
					`{"code":"not_found","message":"earlier handoff verdict","details":{"reason":"l1_regressed","l1_job_id":"`+job.JobID+`"}}`, run.RunID); err != nil {
					t.Fatal(err)
				}
			}
			terminal := snapshotTerminalRun(t, h.l3Store, run.RunID)
			clock.now = clock.now.Add(unrecordedDispatchSettleHorizon)
			client := &recordingCancelClient{L1Client: h.l1Client}
			lookup := &cancelDuringRecoveryLookup{lookup: func(ctx context.Context, key string) (l1.Job, error) {
				if key != job.Spec.DispatchKey {
					t.Fatalf("lookup key = %q, want %q", key, job.Spec.DispatchKey)
				}
				// This write also proves the L1 read holds no ledger write transaction.
				if verdict != "settlement" {
					if _, err := h.l3Store.db.ExecContext(ctx, `UPDATE dispatch_outbox SET last_error=? WHERE run_id=?`,
						`{"code":"not_found","message":"newer handoff verdict","details":{"reason":"l1_regressed","l1_job_id":"`+job.JobID+`"}}`, run.RunID); err != nil {
						return l1.Job{}, err
					}
				}
				if err := h.l3Store.requestRunCancellation(ctx, run.RunID, h.callerUser); err != nil {
					return l1.Job{}, err
				}
				if verdict == "settlement" {
					if err := h.l3Store.cancelAbsentDispatch(ctx, run.RunID, clock.now); err != nil {
						return l1.Job{}, err
					}
				}
				return h.l1Client.LookupJobByDispatchKey(ctx, key)
			}}
			r, err := NewReconciler(h.l3Store, client, ReconcilerConfig{DispatchLookup: lookup})
			if err != nil {
				t.Fatal(err)
			}
			errs := r.recoverUnrecordedDispatches(ctx)
			if verdict == "settlement" {
				if len(errs) != 0 {
					t.Fatalf("settlement blocked positive lookup: %v", errs)
				}
			} else {
				if len(errs) == 0 {
					t.Error("stale recovery handoff was accepted")
				}
				execution, err := h.l3Store.GetRunExecution(ctx, run.RunID)
				if err != nil || execution.DispatchError == nil || execution.DispatchError.Message != "newer handoff verdict" || execution.DispatchError.Details["reason"] != l1RegressedReason {
					t.Fatalf("stale handoff erased the newer verdict: %+v %v", execution, err)
				}
				record, err := h.l3Store.GetRun(ctx, run.RunID)
				observed, getErr := h.l1Client.GetJob(ctx, job.JobID)
				if err != nil || getErr != nil || record.L1JobID != "" || record.CancelStatus != "pending" || client.calls != 0 || observed.State != contract.JobQueued || lookup.calls != 1 {
					t.Fatalf("stale handoff linked or delivered: run=%+v job=%+v cancels=%d lookups=%d errors=%v/%v", record, observed, client.calls, lookup.calls, err, getErr)
				}
				if after := snapshotTerminalRun(t, h.l3Store, run.RunID); after != terminal {
					t.Fatalf("stale handoff changed terminal facts: before=%+v after=%+v", terminal, after)
				}
				// A fresh cancel-owned observation can resolve the regression.
				r.lookup = &absentDispatchLookup{}
				if err := r.ReconcileOnce(ctx); err != nil {
					t.Fatal(err)
				}
			}
			record, err := h.l3Store.GetRun(ctx, run.RunID)
			observed, getErr := h.l1Client.GetJob(ctx, job.JobID)
			if err != nil || getErr != nil || record.L1JobID != job.JobID || record.CancelStatus != "settled" || observed.Outcome != contract.JobOutcomeCanceled || client.calls != 1 || lookup.calls != 1 {
				t.Fatalf("fresh cancel handoff failed: run=%+v job=%+v cancels=%d lookups=%d errors=%v/%v", record, observed, client.calls, lookup.calls, err, getErr)
			}
			if after := snapshotTerminalRun(t, h.l3Store, run.RunID); after != terminal {
				t.Fatalf("fresh handoff changed terminal facts: before=%+v after=%+v", terminal, after)
			}
		})
	}
}

func TestRecoveryAcknowledgedDiscoveryReachesCancel(t *testing.T) {
	t.Run("job-id-fallback", func(t *testing.T) {
		h := newIntegrationHarness(t)
		ctx := context.Background()
		run, job := crashWindowRun(t, h, "recovery-cancel-id-fallback")
		recordOutboxOnlyAcknowledgement(t, h.l3Store, run.RunID, job.JobID)
		// A prior regression prevents requestRunCancellation from linking the
		// outbox identity itself; recovery's GetJob handoff must do the link.
		if _, err := h.l3Store.db.ExecContext(ctx, `UPDATE dispatch_outbox SET last_error=? WHERE run_id=?`,
			`{"code":"not_found","message":"prior regression","details":{"reason":"l1_regressed","l1_job_id":"`+job.JobID+`"}}`, run.RunID); err != nil {
			t.Fatal(err)
		}
		terminal := snapshotTerminalRun(t, h.l3Store, run.RunID)
		client := &recordingCancelClient{L1Client: h.l1Client}
		lookup := &cancelDuringRecoveryLookup{lookup: func(ctx context.Context, key string) (l1.Job, error) {
			if key != job.Spec.DispatchKey {
				t.Fatalf("lookup key = %q, want %q", key, job.Spec.DispatchKey)
			}
			// Recovery selected the Run before cancellation took ownership.
			if err := h.l3Store.requestRunCancellation(ctx, run.RunID, h.callerUser); err != nil {
				return l1.Job{}, err
			}
			return l1.Job{}, &DispatchNotFoundError{DispatchKey: key}
		}}
		r, err := NewReconciler(h.l3Store, client, ReconcilerConfig{DispatchLookup: lookup})
		if err != nil {
			t.Fatal(err)
		}
		if errs := r.recoverUnrecordedDispatches(ctx); len(errs) != 0 {
			t.Fatal(errs)
		}
		record, err := h.l3Store.GetRun(ctx, run.RunID)
		observed, getErr := h.l1Client.GetJob(ctx, job.JobID)
		if err != nil || getErr != nil || record.L1JobID != job.JobID || record.CancelStatus != "settled" || observed.Outcome != contract.JobOutcomeCanceled || client.calls != 1 || lookup.calls != 1 {
			t.Fatalf("acknowledged discovery did not reach cancel: run=%+v job=%+v cancels=%d lookups=%d errors=%v/%v", record, observed, client.calls, lookup.calls, err, getErr)
		}
		if after := snapshotTerminalRun(t, h.l3Store, run.RunID); after != terminal {
			t.Fatalf("acknowledged discovery changed terminal facts: before=%+v after=%+v", terminal, after)
		}
	})
}

func TestRecoveryIdentityConflictBacksOff(t *testing.T) {
	for _, conflict := range []string{"regression-job", "outbox-key"} {
		t.Run(conflict, func(t *testing.T) {
			s, _, clock := recoveryStore(t)
			ctx := context.Background()
			keys := endedUnrecordedRuns(t, s, clock, unrecordedDispatchBatch+1)
			for _, key := range keys[:unrecordedDispatchBatch] {
				id := runIDForDispatchKey(t, s, key)
				var err error
				if conflict == "regression-job" {
					_, err = s.db.ExecContext(ctx, `UPDATE dispatch_outbox SET last_error=? WHERE run_id=?`,
						`{"code":"not_found","message":"corrupt regression identity","details":{"reason":"l1_regressed","l1_job_id":"job-other"}}`, id)
				} else {
					_, err = s.db.ExecContext(ctx, `UPDATE dispatch_outbox SET dispatch_key=? WHERE run_id=?`, "corrupt-"+key, id)
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			lookup := &cancelDuringRecoveryLookup{lookup: func(_ context.Context, key string) (l1.Job, error) {
				return l1.Job{JobID: "job-" + key, Spec: contract.JobSpec{DispatchKey: key}}, nil
			}}
			r, err := NewReconciler(s, &recordingJobClient{}, ReconcilerConfig{DispatchLookup: lookup})
			if err != nil {
				t.Fatal(err)
			}
			if errs := r.recoverUnrecordedDispatches(ctx); len(errs) != unrecordedDispatchBatch {
				t.Fatalf("identity conflicts = %v, want %d", errs, unrecordedDispatchBatch)
			}
			for _, key := range keys[:unrecordedDispatchBatch] {
				if failures, retry := jobLinkBackoff(t, s, key); failures != 1 || !retry.Equal(clock.now.Add(unrecordedDispatchRetryBase)) {
					t.Fatalf("identity conflict did not back off: failures=%d retry=%v", failures, retry)
				}
				record, err := s.GetRun(ctx, runIDForDispatchKey(t, s, key))
				execution, executionErr := s.GetRunExecution(ctx, record.RunID)
				if err != nil || executionErr != nil || record.L1JobID != "" || jobLinkSettled(t, s, record.RunID) ||
					(conflict == "regression-job" && (execution.DispatchError == nil || execution.DispatchError.Details["l1_job_id"] != "job-other")) {
					t.Fatalf("identity conflict altered evidence: run=%+v execution=%+v errors=%v/%v", record, execution, err, executionErr)
				}
			}
			// Sixteen corrupt rows must not monopolize the next batch.
			if errs := r.recoverUnrecordedDispatches(ctx); len(errs) != 0 {
				t.Fatal(errs)
			}
			last := keys[unrecordedDispatchBatch]
			record, err := s.GetRun(ctx, runIDForDispatchKey(t, s, last))
			if err != nil || record.L1JobID != "job-"+last || lookup.calls != unrecordedDispatchBatch+1 {
				t.Fatalf("corrupt rows blocked later recovery: run=%+v lookups=%d error=%v", record, lookup.calls, err)
			}
			clock.now = clock.now.Add(unrecordedDispatchRetryBase)
			if errs := r.recoverUnrecordedDispatches(ctx); len(errs) != unrecordedDispatchBatch {
				t.Fatalf("due conflict retries = %v", errs)
			}
			for _, key := range keys[:unrecordedDispatchBatch] {
				if failures, retry := jobLinkBackoff(t, s, key); failures != 2 || !retry.Equal(clock.now.Add(2*unrecordedDispatchRetryBase)) {
					t.Fatalf("identity conflict retry did not double backoff: failures=%d retry=%v", failures, retry)
				}
			}
		})
	}
}
