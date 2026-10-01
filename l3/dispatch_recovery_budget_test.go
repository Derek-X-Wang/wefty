package l3

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/l1"
)

// unavailableLookupClient stands in for an L1 that does not answer: in hang
// mode each lookup blocks until its context ends (or a safety ceiling far
// above any budget), otherwise it fails at once with a transport error.
type unavailableLookupClient struct {
	hang   bool
	looked []string
}

func (c *unavailableLookupClient) LookupJobByDispatchKey(ctx context.Context, key string) (l1.Job, error) {
	c.looked = append(c.looked, key)
	if c.hang {
		select {
		case <-ctx.Done():
			return l1.Job{}, ctx.Err()
		case <-time.After(10 * time.Second):
		}
	}
	return l1.Job{}, errors.New("injected: L1 unavailable")
}

// endedUnrecordedRuns creates count ended runs, each with one dispatch attempt
// and no recorded job, created one second apart, and returns their keys in
// creation order.
func endedUnrecordedRuns(t *testing.T, s *Store, clock *mutableClock, count int) []string {
	t.Helper()
	ctx := context.Background()
	keys := make([]string, 0, count)
	for i := range count {
		clock.now = clock.now.Add(time.Second)
		record, _, err := s.CreateRun(ctx, CreateRunInput{IdempotencyKey: fmt.Sprintf("budget-%02d", i), Actor: "test", Request: inlineRunRequest("#!/bin/sh\nexit 0\n")})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.beginDispatch(ctx, record.RunID); err != nil {
			t.Fatal(err)
		}
		if err := s.rejectProtocolWrite(ctx, record.RunID, "envelope", fmt.Sprintf("budget-%02d", i), []byte(`{}`), "budget-hash", "invalid envelope", errors.New("invalid envelope")); err != nil {
			t.Fatal(err)
		}
		keys = append(keys, record.DispatchKey)
	}
	return keys
}

func jobLinkBackoff(t *testing.T, s *Store, dispatchKey string) (int, time.Time) {
	t.Helper()
	var failures int
	var retryNS int64
	if err := s.db.QueryRow(`SELECT job_link_failures, job_link_retry_ns FROM runs WHERE dispatch_key=?`, dispatchKey).Scan(&failures, &retryNS); err != nil {
		t.Fatal(err)
	}
	return failures, time.Unix(0, retryNS).UTC()
}

// An L1 that hangs costs one recovery budget per pass, not a timeout per
// unsettled row, and the row the budget cut short backs off so the next pass
// reaches the rows behind it.
func TestUnavailableL1CannotHoldARecoveryPassPastItsBudget(t *testing.T) {
	s, _, clock := recoveryStore(t)
	keys := endedUnrecordedRuns(t, s, clock, 4)
	lookup := &unavailableLookupClient{hang: true}
	const budget = 100 * time.Millisecond
	reconciler, err := NewReconciler(s, &recordingJobClient{}, ReconcilerConfig{DispatchLookup: lookup, DispatchRecoveryBudget: budget})
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if err := reconciler.ReconcileOnce(context.Background()); err == nil {
		t.Fatal("budget-cut lookup was not reported")
	}
	if elapsed := time.Since(started); elapsed > budget+2*time.Second {
		t.Fatalf("recovery pass took %v with a %v budget", elapsed, budget)
	}
	if len(lookup.looked) != 1 || lookup.looked[0] != keys[0] {
		t.Fatalf("lookups after the budget was spent = %v, want only the oldest %q", lookup.looked, keys[0])
	}
	failures, retryAt := jobLinkBackoff(t, s, keys[0])
	if failures != 1 || !retryAt.Equal(clock.now.Add(unrecordedDispatchRetryBase)) {
		t.Fatalf("cut-short row backoff = %d failures, retry at %v; want 1, %v", failures, retryAt, clock.now.Add(unrecordedDispatchRetryBase))
	}
	for _, key := range keys[1:] {
		if failures, _ := jobLinkBackoff(t, s, key); failures != 0 {
			t.Fatalf("row %q never looked up was backed off", key)
		}
	}

	lookup.hang, lookup.looked = false, nil
	_ = reconciler.ReconcileOnce(context.Background())
	if want := keys[1:]; fmt.Sprint(lookup.looked) != fmt.Sprint(want) {
		t.Fatalf("second pass looked up %v, want the rows behind the backed-off one %v", lookup.looked, want)
	}
}

func TestRecoveryBacksOffTransientFailuresAndReadsABoundedBatch(t *testing.T) {
	s, _, clock := recoveryStore(t)
	keys := endedUnrecordedRuns(t, s, clock, unrecordedDispatchBatch+4)
	lookup := &unavailableLookupClient{}
	reconciler, err := NewReconciler(s, &recordingJobClient{}, ReconcilerConfig{DispatchLookup: lookup})
	if err != nil {
		t.Fatal(err)
	}
	_ = reconciler.ReconcileOnce(context.Background())
	if want := keys[:unrecordedDispatchBatch]; fmt.Sprint(lookup.looked) != fmt.Sprint(want) {
		t.Fatalf("first pass looked up %d rows %v, want the oldest %d", len(lookup.looked), lookup.looked, unrecordedDispatchBatch)
	}

	// The same instant: the failed batch is backed off, the rest get a turn.
	lookup.looked = nil
	_ = reconciler.ReconcileOnce(context.Background())
	if want := keys[unrecordedDispatchBatch:]; fmt.Sprint(lookup.looked) != fmt.Sprint(want) {
		t.Fatalf("second pass looked up %v, want only the rows not backed off %v", lookup.looked, want)
	}
	lookup.looked = nil
	_ = reconciler.ReconcileOnce(context.Background())
	if len(lookup.looked) != 0 {
		t.Fatalf("backed-off rows were looked up again at once: %v", lookup.looked)
	}

	// Each consecutive failure doubles the wait.
	clock.now = clock.now.Add(unrecordedDispatchRetryBase)
	_ = reconciler.ReconcileOnce(context.Background())
	if len(lookup.looked) != unrecordedDispatchBatch {
		t.Fatalf("rows due after the first backoff = %d lookups, want %d", len(lookup.looked), unrecordedDispatchBatch)
	}
	failures, retryAt := jobLinkBackoff(t, s, keys[0])
	if failures != 2 || !retryAt.Equal(clock.now.Add(2*unrecordedDispatchRetryBase)) {
		t.Fatalf("second failure backoff = %d failures, retry at %v", failures, retryAt)
	}
}

func TestUnrecordedDispatchBackoffDoublesToItsCap(t *testing.T) {
	for failures, want := range []time.Duration{30 * time.Second, time.Minute, 2 * time.Minute, 4 * time.Minute, 8 * time.Minute, 16 * time.Minute, 30 * time.Minute, 30 * time.Minute} {
		if got := unrecordedDispatchBackoff(failures); got != want {
			t.Errorf("backoff after %d failures = %v, want %v", failures, got, want)
		}
	}
	if got := unrecordedDispatchBackoff(1 << 20); got != unrecordedDispatchRetryCap {
		t.Errorf("backoff after many failures = %v, want the cap", got)
	}
}

// A ledger from before dispatch_attempt_ns recorded attempts only as a count.
// Opening it backfills the attempt time from the run's finish, so its
// historical crash windows still enter the recovery index.
func TestOpeningAnOlderLedgerBackfillsDispatchAttemptTime(t *testing.T) {
	s, path, clock := recoveryStore(t)
	keys := endedUnrecordedRuns(t, s, clock, 1)
	ctx := context.Background()
	if _, err := s.db.ExecContext(ctx, `DROP INDEX runs_job_link_recovery`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(ctx, `ALTER TABLE runs DROP COLUMN dispatch_attempt_ns`); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenStore(path, StoreOptions{Clock: clock, RunTokenGrace: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reopened.Close() })
	var attemptNS, finishedNS int64
	if err := reopened.db.QueryRow(`SELECT dispatch_attempt_ns, finished_ns FROM runs WHERE dispatch_key=?`, keys[0]).Scan(&attemptNS, &finishedNS); err != nil {
		t.Fatal(err)
	}
	if attemptNS != finishedNS {
		t.Fatalf("backfilled attempt time = %d, want the run's finish %d", attemptNS, finishedNS)
	}
	pending, err := reopened.unrecordedDispatches(ctx, unrecordedDispatchBatch)
	if err != nil || len(pending) != 1 || pending[0].DispatchKey != keys[0] {
		t.Fatalf("recovery after migration = %+v, %v", pending, err)
	}
}

// A run that ended before any submit attempt has nothing to recover and never
// enters the recovery index.
func TestRunEndedBeforeAnyAttemptIsNotRecoveryEligible(t *testing.T) {
	s, _, _ := recoveryStore(t)
	ctx := context.Background()
	record, _, err := s.CreateRun(ctx, CreateRunInput{IdempotencyKey: "never-attempted", Actor: "test", Request: inlineRunRequest("#!/bin/sh\nexit 0\n")})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.rejectProtocolWrite(ctx, record.RunID, "envelope", "never-attempted", []byte(`{}`), "never-hash", "invalid envelope", errors.New("invalid envelope")); err != nil {
		t.Fatal(err)
	}
	var indexed int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM runs INDEXED BY runs_job_link_recovery
WHERE l1_job_id IS NULL AND job_link_settled=0 AND status IN ('succeeded','failed') AND dispatch_attempt_ns IS NOT NULL`).Scan(&indexed); err != nil {
		t.Fatal(err)
	}
	if indexed != 0 {
		t.Fatalf("never-attempted run is in the recovery index (%d rows)", indexed)
	}
}
