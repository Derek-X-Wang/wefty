package l3

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

func recoveryStore(t *testing.T) (*Store, string, *mutableClock) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ledger.sqlite")
	clock := &mutableClock{now: time.Date(2026, 9, 8, 8, 0, 0, 0, time.UTC)}
	s, err := OpenStore(path, StoreOptions{Clock: clock, RunTokenGrace: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s, path, clock
}
func dispatchedRecoveryRun(t *testing.T, s *Store, key string) (projectedRun, string) {
	t.Helper()
	ctx := context.Background()
	record, _, err := s.CreateRun(ctx, CreateRunInput{IdempotencyKey: key, Actor: "test", Request: inlineRunRequest("#!/bin/sh\nexit 0\n")})
	if err != nil {
		t.Fatal(err)
	}
	token, err := s.ensureRunToken(ctx, record.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.beginDispatch(ctx, record.RunID); err != nil {
		t.Fatal(err)
	}
	if err = s.completeDispatch(ctx, record.RunID, "job-"+key); err != nil {
		t.Fatal(err)
	}
	return projectedRun{RunID: record.RunID, JobID: "job-" + key, State: contract.RunQueued}, token
}

type recoverySnapshot struct {
	state                     contract.RunState
	runJob, key, outboxJob    string
	attempts                  int
	updated                   int64
	dispatched                sql.NullInt64
	started, finished, expiry sql.NullInt64
	reason, delivery          sql.NullString
}

func snapshotRecovery(t *testing.T, s *Store, id string) recoverySnapshot {
	t.Helper()
	var v recoverySnapshot
	err := s.db.QueryRow(`SELECT r.status,r.l1_job_id,r.dispatch_key,o.job_id,o.attempt_count,o.dispatched_ns,r.updated_ns,r.started_ns,r.finished_ns,t.expires_ns,o.last_error,o.token_delivery FROM runs r JOIN dispatch_outbox o ON o.run_id=r.run_id JOIN run_tokens t ON t.run_id=r.run_id WHERE r.run_id=?`, id).Scan(&v.state, &v.runJob, &v.key, &v.outboxJob, &v.attempts, &v.dispatched, &v.updated, &v.started, &v.finished, &v.expiry, &v.reason, &v.delivery)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestL1RegressionDurableIdentityAndTokenGrace(t *testing.T) {
	s, path, clock := recoveryStore(t)
	run, token := dispatchedRecoveryRun(t, s, "durable")
	ctx := context.Background()
	if err := s.projectJobState(ctx, run, contract.JobRunning); err != nil {
		t.Fatal(err)
	}
	run.State = contract.RunRunning
	before := snapshotRecovery(t, s, run.RunID)
	clock.now = clock.now.Add(time.Second)
	changed, err := s.failMissingL1Job(ctx, run)
	if err != nil || !changed {
		t.Fatalf("transition=%t %v", changed, err)
	}
	after := snapshotRecovery(t, s, run.RunID)
	if after.state != contract.RunFailed || after.runJob != before.runJob || after.key != before.key || after.outboxJob != before.outboxJob || after.attempts != before.attempts || after.dispatched != before.dispatched || after.started != before.started || after.delivery.Valid {
		t.Fatalf("identity changed: before=%+v after=%+v", before, after)
	}
	if !after.finished.Valid || after.finished.Int64 != clock.now.UnixNano() || !after.expiry.Valid || after.expiry.Int64 != clock.now.Add(time.Minute).UnixNano() {
		t.Fatalf("terminal time/grace: %+v", after)
	}
	if _, err = s.AuthenticateRunToken(ctx, token); err != nil {
		t.Fatal(err)
	}
	clock.now = clock.now.Add(10 * time.Second)
	changed, err = s.failMissingL1Job(ctx, run)
	if err != nil || changed {
		t.Fatalf("duplicate transition=%t %v", changed, err)
	}
	if err = s.completeDispatch(ctx, run.RunID, run.JobID); err != nil {
		t.Fatal(err)
	}
	if got := snapshotRecovery(t, s, run.RunID); !reflect.DeepEqual(got, after) {
		t.Fatalf("duplicate modified terminal: %+v != %+v", got, after)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenStore(path, StoreOptions{Clock: clock, RunTokenGrace: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got := snapshotRecovery(t, reopened, run.RunID); !reflect.DeepEqual(got, after) {
		t.Fatalf("reopen changed evidence: %+v", got)
	}
	changed, err = reopened.failMissingL1Job(ctx, run)
	if err != nil || changed {
		t.Fatalf("reopen duplicate=%t %v", changed, err)
	}
	projection, err := reopened.GetRunExecution(ctx, run.RunID)
	if err != nil || !isL1Regression(projection.DispatchError, run.JobID) {
		t.Fatalf("durable reason=%+v %v", projection, err)
	}
	c := &recoveryJobClient{get: func(context.Context, string) (l1.Job, error) {
		t.Fatal("terminal run fetched after reopen")
		return l1.Job{}, nil
	}}
	r, err := NewReconciler(reopened, c, ReconcilerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err = r.ReconcileOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if c.submits != 0 {
		t.Fatalf("replayed after reopen: %d", c.submits)
	}
	clock.now = time.Unix(0, after.expiry.Int64)
	if _, err = reopened.AuthenticateRunToken(ctx, token); err == nil {
		t.Fatal("expired token accepted")
	}
}

func TestL1RegressionGuardAndRollback(t *testing.T) {
	for _, test := range []string{"wrong captured job", "wrong outbox job", "not dispatched", "stale state", "terminal", "reason failure", "token failure"} {
		t.Run(test, func(t *testing.T) {
			s, _, _ := recoveryStore(t)
			run, _ := dispatchedRecoveryRun(t, s, "guard")
			ctx := context.Background()
			switch test {
			case "wrong captured job":
				run.JobID = "wrong"
			case "wrong outbox job":
				_, err := s.db.Exec(`UPDATE dispatch_outbox SET job_id='wrong' WHERE run_id=?`, run.RunID)
				if err != nil {
					t.Fatal(err)
				}
			case "not dispatched":
				_, err := s.db.Exec(`UPDATE dispatch_outbox SET dispatched_ns=NULL WHERE run_id=?`, run.RunID)
				if err != nil {
					t.Fatal(err)
				}
			case "stale state":
				run.State = contract.RunRunning
			case "terminal":
				if err := s.projectJobState(ctx, run, contract.JobFailed); err != nil {
					t.Fatal(err)
				}
			case "reason failure":
				_, err := s.db.Exec(`CREATE TRIGGER fail_reason BEFORE UPDATE OF last_error ON dispatch_outbox BEGIN SELECT RAISE(ABORT,'injected reason persistence failure'); END`)
				if err != nil {
					t.Fatal(err)
				}
			case "token failure":
				_, err := s.db.Exec(`CREATE TRIGGER fail_expiry BEFORE UPDATE OF expires_ns ON run_tokens BEGIN SELECT RAISE(ABORT,'injected token persistence failure'); END`)
				if err != nil {
					t.Fatal(err)
				}
			}
			before := snapshotRecovery(t, s, run.RunID)
			changed, err := s.failMissingL1Job(ctx, run)
			wantErr := test == "reason failure" || test == "token failure"
			if changed || (err != nil) != wantErr {
				t.Fatalf("transition=%t %v", changed, err)
			}
			if got := snapshotRecovery(t, s, run.RunID); !reflect.DeepEqual(got, before) {
				t.Fatalf("partial/stale transition: before=%+v after=%+v", before, got)
			}
		})
	}
}

func TestL1RegressionConcurrentDuplicateStoreTransitions(t *testing.T) {
	s, _, _ := recoveryStore(t)
	run, _ := dispatchedRecoveryRun(t, s, "duplicate")
	start := make(chan struct{})
	results := make(chan bool, 2)
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			changed, err := s.failMissingL1Job(context.Background(), run)
			results <- changed
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)
	wins := 0
	for changed := range results {
		if changed {
			wins++
		}
	}
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if wins != 1 {
		t.Fatalf("terminal winners=%d", wins)
	}
}

func TestL1RegressionFirstTerminalProjectionWins(t *testing.T) {
	for _, order := range []string{"success first", "missing first", "protocol first", "duplicate missing"} {
		t.Run(order, func(t *testing.T) {
			s, _, clock := recoveryStore(t)
			run, _ := dispatchedRecoveryRun(t, s, "ordering")
			ctx := context.Background()
			if err := s.projectJobState(ctx, run, contract.JobRunning); err != nil {
				t.Fatal(err)
			}
			run.State = contract.RunRunning
			entered, release := make(chan struct{}), make(chan struct{})
			c := &recoveryJobClient{get: func(context.Context, string) (l1.Job, error) {
				close(entered)
				<-release
				if order == "missing first" {
					return l1.Job{JobID: run.JobID, State: contract.JobSucceeded, NodeID: "late-node"}, nil
				}
				return l1.Job{}, &JobNotFoundError{JobID: run.JobID, Cause: &Error{Code: contract.ErrorNotFound}}
			}}
			r, err := NewReconciler(s, c, ReconcilerConfig{})
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- r.ReconcileOnce(ctx) }()
			<-entered
			switch order {
			case "success first":
				err = s.projectJobState(ctx, run, contract.JobSucceeded)
			case "protocol first":
				err = s.rejectProtocolWrite(ctx, run.RunID, "envelope", "rejected", []byte(`{}`), "hash", "invalid envelope")
			default:
				_, err = s.failMissingL1Job(ctx, run)
			}
			if err != nil {
				t.Fatal(err)
			}
			winner := snapshotRecovery(t, s, run.RunID)
			if order == "success first" && winner.state != contract.RunSucceeded || order != "success first" && winner.state != contract.RunFailed {
				t.Fatalf("winner not terminal: %+v", winner)
			}
			clock.now = clock.now.Add(time.Second)
			close(release)
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("stale pass should lose without another failure: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("stale pass hung")
			}
			if got := snapshotRecovery(t, s, run.RunID); !reflect.DeepEqual(got, winner) {
				t.Fatalf("late pass changed winner: before=%+v after=%+v", winner, got)
			}
		})
	}
}

func TestL1RegressionOnlyTypedMatchingAbsenceAndHealthyPeersProgress(t *testing.T) {
	for _, cause := range []error{errors.New("transport"), &Error{Code: contract.ErrorNotFound}, &Error{Code: contract.ErrorUnauthorized}, &Error{Code: contract.ErrorInternal, Retryable: true}, &JobNotFoundError{JobID: "other"}} {
		t.Run(cause.Error(), func(t *testing.T) {
			s, _, _ := recoveryStore(t)
			bad, _ := dispatchedRecoveryRun(t, s, "bad")
			good, _ := dispatchedRecoveryRun(t, s, "good")
			c := &recoveryJobClient{get: func(_ context.Context, id string) (l1.Job, error) {
				if id == bad.JobID {
					return l1.Job{}, cause
				}
				return l1.Job{JobID: id, State: contract.JobSucceeded}, nil
			}}
			r, _ := NewReconciler(s, c, ReconcilerConfig{})
			if err := r.ReconcileOnce(context.Background()); err == nil {
				t.Fatal("lost transient pass error")
			}
			if got := snapshotRecovery(t, s, bad.RunID); got.state != contract.RunQueued || got.reason.Valid {
				t.Fatalf("ordinary error terminalized: %+v", got)
			}
			if got := snapshotRecovery(t, s, good.RunID); got.state != contract.RunRunning {
				t.Fatalf("healthy peer stalled: %+v", got)
			}
			c.get = func(_ context.Context, id string) (l1.Job, error) {
				return l1.Job{JobID: id, State: contract.JobSucceeded}, nil
			}
			if err := r.ReconcileOnce(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err := r.ReconcileOnce(context.Background()); err != nil {
				t.Fatal(err)
			}
			if got := snapshotRecovery(t, s, bad.RunID); got.state != contract.RunSucceeded {
				t.Fatalf("did not recover: %+v", got)
			}
		})
	}
}

type recoveryImageErrorClient struct{ err error }

func (c recoveryImageErrorClient) GetJobImageEvidence(context.Context, string) ([]AttemptImageEvidence, error) {
	return nil, c.err
}
func TestL1RegressionDoesNotTerminalizeImageEvidenceAbsence(t *testing.T) {
	s, _, _ := recoveryStore(t)
	run, _ := dispatchedRecoveryRun(t, s, "image")
	c := &recoveryJobClient{get: func(context.Context, string) (l1.Job, error) {
		return l1.Job{JobID: run.JobID, State: contract.JobFailed}, nil
	}}
	r, _ := NewReconciler(s, c, ReconcilerConfig{ImageEvidence: recoveryImageErrorClient{err: &JobNotFoundError{JobID: run.JobID, Cause: &Error{Code: contract.ErrorNotFound}}}})
	if err := r.ReconcileOnce(context.Background()); err == nil {
		t.Fatal("image error not reported")
	}
	if got := snapshotRecovery(t, s, run.RunID); got.state != contract.RunQueued || got.reason.Valid {
		t.Fatalf("image absence terminalized run: %+v", got)
	}
}
