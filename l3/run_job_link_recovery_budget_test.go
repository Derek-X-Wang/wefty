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

	// The second pass proves the backed-off row is skipped, not the budget, so
	// it runs with the default budget: on a loaded runner, 100ms of wall time
	// can run out after one healthy lookup and cut the pass short again.
	lookup.hang, lookup.looked = false, nil
	relaxed, err := NewReconciler(s, &recordingJobClient{}, ReconcilerConfig{DispatchLookup: lookup})
	if err != nil {
		t.Fatal(err)
	}
	_ = relaxed.ReconcileOnce(context.Background())
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
	removeRecoveryAttemptColumn(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenStore(path, StoreOptions{Clock: clock, RunTokenGrace: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reopened.Close() })
	attemptNS, finishedNS := recoveryAttemptTimes(t, reopened, keys[0])
	if !attemptNS.Valid || attemptNS.Int64 != finishedNS {
		t.Fatalf("backfilled attempt time = %v, want the run's finish %d", attemptNS, finishedNS)
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
	pending, err := s.unrecordedDispatches(ctx, unrecordedDispatchBatch)
	if err != nil || len(pending) != 0 {
		t.Fatalf("never-attempted run is recovery eligible: %+v %v", pending, err)
	}
}

// An upgrade interrupted after adding dispatch_attempt_ns leaves attempted
// runs with no attempt time, outside the recovery index. Every open heals
// them; a run never attempted stays without one.
func TestOpeningALedgerHealsMissingDispatchAttemptTimes(t *testing.T) {
	s, path, clock := recoveryStore(t)
	ctx := context.Background()
	attempted := endedUnrecordedRuns(t, s, clock, 1)[0]
	never, _, err := s.CreateRun(ctx, CreateRunInput{IdempotencyKey: "heal-never-attempted", Actor: "test", Request: inlineRunRequest("#!/bin/sh\nexit 0\n")})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.rejectProtocolWrite(ctx, never.RunID, "envelope", "heal-never", []byte(`{}`), "heal-hash", "invalid envelope", errors.New("invalid envelope")); err != nil {
		t.Fatal(err)
	}
	clearRecoveryAttemptTimes(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenStore(path, StoreOptions{Clock: clock, RunTokenGrace: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reopened.Close() })
	attemptNS, finishedNS := recoveryAttemptTimes(t, reopened, attempted)
	if !attemptNS.Valid || attemptNS.Int64 != finishedNS {
		t.Fatalf("healed attempt time = %v, want the run's finish %d", attemptNS, finishedNS)
	}
	attemptNS = recoveryRunAttemptTime(t, reopened, never.RunID)
	if attemptNS.Valid {
		t.Fatalf("never-attempted run was given an attempt time %d", attemptNS.Int64)
	}
	pending, err := reopened.unrecordedDispatches(ctx, unrecordedDispatchBatch)
	if err != nil || len(pending) != 1 || pending[0].DispatchKey != attempted {
		t.Fatalf("recovery after healing = %+v, %v", pending, err)
	}
}
