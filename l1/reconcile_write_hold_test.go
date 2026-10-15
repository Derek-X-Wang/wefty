package l1

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
)

// #939: Reconcile used to apply log and attempt-summary retention to every
// service, one by one, inside its write transaction: about 0.18 ms per
// service. At thousands of services the passes ran back to back and a
// CreateJob waited out SQLite's five-second busy wait. These tests hold one
// pass's write lock to a bound that idle services do not move, keep writers
// getting in, and check that the bounded passes still remove everything the
// old sweep removed.

// reconcileWriteHoldBound is the ticket's per-pass bound, held by the median
// pass; reconcileWriteHoldCeiling bounds every pass with room for a scheduler
// pause on a loaded runner. Main held about 950 ms a pass over this fixture.
// The deterministic work bounds are the page-count tests below.
const (
	reconcileWriteHoldBound   = 50 * time.Millisecond
	reconcileWriteHoldCeiling = 250 * time.Millisecond
)

const retentionSweepNodeID = "sweep-node"

func openRetentionSweepStore(t *testing.T, options StoreOptions) (*Store, *fakeClock) {
	t.Helper()
	clock := &fakeClock{now: time.Date(2026, 8, 9, 10, 0, 0, 0, time.UTC)}
	options.Clock = clock
	store, err := OpenStore(filepath.Join(t.TempDir(), "l1.sqlite"), options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})
	// The node the seeded attempts ran on.
	retentionSweepExec(t, store, `INSERT INTO nodes(node_id, identity_node_id, boot_session_id, os, architecture,
		agent_version, capabilities_json, state, last_heartbeat_ns, max_oneshot_slots, max_service_slots)
		VALUES(?, ?, 'boot', 'linux', 'arm64', 'test', '{}', ?, ?, 4, 4)`,
		retentionSweepNodeID, retentionSweepNodeID, contract.NodeAlive, clock.Now().UnixNano())
	return store, clock
}

func retentionSweepExec(t *testing.T, store *Store, query string, args ...any) {
	t.Helper()
	if _, err := store.db.Exec(query, args...); err != nil {
		t.Fatal(err)
	}
}

// seedIdleServices adds count services named prefix-000001 onward, cloned
// from one submitted through CreateJob: a job row and a service row each,
// with no attempts and no logs.
func seedIdleServices(t *testing.T, store *Store, prefix string, count int) {
	t.Helper()
	spec := validJobSpec(prefix+"-template", nil)
	spec.Class = contract.JobClassService
	spec.Execution.HandoffDirectory = ""
	spec.Restart = contract.RestartAlways
	template, _, err := store.CreateJob(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	retentionSweepExec(t, store, `WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<?)
		INSERT INTO jobs(job_id, dispatch_key, request_hash, spec_json, state, created_ns, updated_ns)
		SELECT printf('%s-%06d', ?, i), printf('%s-key-%06d', ?, i), request_hash, spec_json, state, created_ns, updated_ns
		FROM n, jobs WHERE jobs.job_id=?`, count, prefix, prefix, template.JobID)
	retentionSweepExec(t, store, `WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<?)
		INSERT INTO service_jobs(job_id, desired_state)
		SELECT printf('%s-%06d', ?, i), desired_state FROM n, service_jobs WHERE service_jobs.job_id=?`,
		count, prefix, template.JobID)
}

func seededServiceID(prefix string, index int) string {
	return fmt.Sprintf("%s-%06d", prefix, index)
}

// seedAttempts adds count attempts of jobID, created one nanosecond apart from
// createdNS, named <jobID>-a001 onward; the last is the newest.
func seedAttempts(t *testing.T, store *Store, jobID string, count int, state contract.AttemptState, createdNS int64) {
	t.Helper()
	retentionSweepExec(t, store, `WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<?)
		INSERT INTO attempts(attempt_id, job_id, node_id, boot_session_id, state, fencing_token,
			lease_expires_ns, authority_generation, created_ns, updated_ns)
		SELECT printf('%s-a%03d', ?, i), ?, ?, 'boot', ?, printf('fence-%d', i), ?+i, 0, ?+i, ?+i FROM n`,
		count, jobID, jobID, retentionSweepNodeID, state, createdNS, createdNS, createdNS)
}

func seededAttemptID(jobID string, index int) string {
	return fmt.Sprintf("%s-a%03d", jobID, index)
}

// seedLogRows adds one stdout row per timestamp to attemptID, sequences from
// firstSequence on, each carrying payload.
func seedLogRows(t *testing.T, store *Store, jobID, attemptID string, firstSequence int, payload []byte, timestamps ...int64) {
	t.Helper()
	for index, timestamp := range timestamps {
		sequence := firstSequence + index
		retentionSweepExec(t, store, `INSERT INTO log_events(job_id, attempt_id, stream, sequence, sequence_end, timestamp_ns, bytes, event_json)
			VALUES(?, ?, 'stdout', ?, ?, ?, ?, X'7B7D')`, jobID, attemptID, sequence, sequence, timestamp, payload)
	}
}

// assertNothingLeftForTheOldSweep runs the sweep L1 had before #939 -- the
// service age bound, the per-job byte cap and attempt-summary pruning over
// each whole history (#942), for every service -- with no budget, in a
// transaction it rolls back, and fails if it would still remove anything.
func assertNothingLeftForTheOldSweep(t *testing.T, store *Store) {
	t.Helper()
	ctx := context.Background()
	now := canonicalTime(store.clock.Now())
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, "SELECT job_id FROM service_jobs ORDER BY job_id")
	if err != nil {
		t.Fatal(err)
	}
	services, err := scanJobIDs(rows, "service")
	if err != nil {
		t.Fatal(err)
	}
	unlimited := &evictionBudget{events: math.MaxInt, bytes: math.MaxInt64}
	cutoff := now.Add(-store.serviceLogRetentionAge).UnixNano()
	var aged, overCap, pruned int64
	for _, jobID := range services {
		perJob := map[string]*logRetentionStats{}
		if err := evictLogEvents(ctx, tx, jobID, " AND e.timestamp_ns < ?", []any{cutoff}, unlimited, -1, perJob); err != nil {
			t.Fatal(err)
		}
		for _, stats := range perJob {
			aged += stats.events
		}
		stats, err := store.enforceJobLogByteRetention(ctx, tx, jobID, now, unlimited)
		if err != nil {
			t.Fatal(err)
		}
		overCap += stats.events
		count, err := unboundedServiceAttemptSummaryPrune(ctx, tx, store, jobID, now)
		if err != nil {
			t.Fatal(err)
		}
		pruned += count
	}
	if aged != 0 || overCap != 0 || pruned != 0 {
		t.Fatalf("the old per-service sweep would still evict %d aged and %d over-cap rows and prune %d attempt summaries",
			aged, overCap, pruned)
	}
}

// unboundedServiceAttemptSummaryPrune is the attempt-summary prune claims,
// completions, log appends and the sweep ran before #939 and #942: one
// statement over the service's whole history. The bounded passes and calls
// are checked against it.
func unboundedServiceAttemptSummaryPrune(ctx context.Context, tx *sql.Tx, store *Store, jobID string, now time.Time) (int64, error) {
	result, err := tx.ExecContext(ctx, `DELETE FROM attempts
		WHERE job_id=?
			AND EXISTS (SELECT 1 FROM service_jobs WHERE service_jobs.job_id=attempts.job_id)
			AND attempt_id<>COALESCE((SELECT current_attempt_id FROM jobs WHERE job_id=?), '')
			AND state NOT IN (?, ?, ?)
			AND NOT (state=? AND updated_ns>=?)
			AND NOT EXISTS (SELECT 1 FROM log_events WHERE log_events.attempt_id=attempts.attempt_id)
			AND attempt_id NOT IN (
				SELECT attempt_id FROM attempts recent
				WHERE recent.job_id=?
					AND recent.attempt_id<>COALESCE((SELECT current_attempt_id FROM jobs WHERE job_id=?), '')
				ORDER BY recent.created_ns DESC, recent.attempt_id DESC
				LIMIT ?
			)`, jobID, jobID,
		contract.AttemptClaimed, contract.AttemptRunning, contract.AttemptAwaitingInput,
		contract.AttemptLost, now.Add(-store.lateEvidenceWindow).UnixNano(),
		jobID, jobID, DefaultServiceAttemptSummaries)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func TestReconcileWriteHoldStaysBoundedAtThousandsOfIdleServices(t *testing.T) {
	store, clock := openRetentionSweepStore(t, StoreOptions{})
	seedIdleServices(t, store, "idle", 6000)
	// A few services do have work: logs past the 7-day bound, and some have
	// attempt summaries past the floor.
	const busy = 24
	seedIdleServices(t, store, "busy", busy)
	old := clock.Now().Add(-8 * 24 * time.Hour).UnixNano()
	for index := 1; index <= busy; index++ {
		jobID := seededServiceID("busy", index)
		attempts := 1
		if index%8 == 0 {
			attempts = DefaultServiceAttemptSummaries + 8
		}
		seedAttempts(t, store, jobID, attempts, contract.AttemptFailed, old)
		seedLogRows(t, store, jobID, seededAttemptID(jobID, attempts), 0, []byte("payload"),
			old+int64(index), old+int64(busy+index), old+int64(2*busy+index), clock.Now().UnixNano())
	}

	var holds []time.Duration
	store.reconcileHoldForTest = func(hold time.Duration) { holds = append(holds, hold) }
	for pass := 0; pass < 8; pass++ {
		if _, err := store.Reconcile(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("write lock held per pass over %d services: %v", 6000+busy, holds)
	sorted := slices.Clone(holds)
	slices.Sort(sorted)
	if median := sorted[len(sorted)/2]; median > reconcileWriteHoldBound {
		t.Fatalf("median pass held the write lock %s over %d services, over the %s bound; holds %v",
			median, 6000+busy, reconcileWriteHoldBound, holds)
	}
	if longest := sorted[len(sorted)-1]; longest > reconcileWriteHoldCeiling {
		t.Fatalf("a pass held the write lock %s over %d services, over the %s ceiling; holds %v",
			longest, 6000+busy, reconcileWriteHoldCeiling, holds)
	}
	assertNothingLeftForTheOldSweep(t, store)
}

// The production failure (#923): with the passes back to back, a waiting
// writer misses every gap and CreateJob fails after the busy wait.
func TestCreateJobGetsTheWriteLockWhileReconcileRunsOverThousandsOfServices(t *testing.T) {
	store, _ := openRetentionSweepStore(t, StoreOptions{})
	const services = 8000
	seedIdleServices(t, store, "idle", services)
	var passes atomic.Int64
	store.reconcileHoldForTest = func(time.Duration) { passes.Add(1) }

	network := plain.NewNetwork()
	serverFabric := network.NewFabric(fabric.Identity{NodeID: "control-plane"})
	// The production reconcile interval.
	server, err := NewServer(serverFabric, store, ServerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := serverFabric.Listen("tcp", "wefty://control-plane")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- server.Serve(ctx, listener) }()
	defer func() {
		cancel()
		if err := <-served; err != nil {
			t.Errorf("Serve = %v", err)
		}
	}()

	started := time.Now()
	guard := func(what string) {
		t.Helper()
		if time.Since(started) > time.Minute {
			t.Fatalf("%s: only %d reconcile passes in a minute", what, passes.Load())
		}
	}
	for passes.Load() == 0 {
		guard("initial reconcile")
		time.Sleep(10 * time.Millisecond)
	}
	// Each CreateJob either gets the write lock within SQLite's busy wait or
	// fails; keep creating jobs across several background passes.
	created := 0
	for passes.Load() < 4 {
		guard("background passes")
		if _, _, err := store.CreateJob(ctx, validJobSpec(fmt.Sprintf("probe-%d", created), nil)); err != nil {
			t.Fatalf("CreateJob %d failed while reconcile ran over %d services (%d passes so far, SQLite lock contention %t): %v: %v",
				created, services, passes.Load(), sqliteLockContention(err), err, errors.Unwrap(err))
		}
		created++
		time.Sleep(2 * time.Millisecond)
	}
	t.Logf("created %d jobs across %d reconcile passes over %d services", created, passes.Load(), services)
}

// A pass longer than the interval must not make the next one start at once,
// as a fixed ticker would: writers get a full interval between passes.
func TestReconcileLoopWaitsAFullIntervalAfterEachPass(t *testing.T) {
	store, _ := openRetentionSweepStore(t, StoreOptions{})
	const interval = 50 * time.Millisecond
	network := plain.NewNetwork()
	serverFabric := network.NewFabric(fabric.Identity{NodeID: "control-plane"})
	server, err := NewServer(serverFabric, store, ServerConfig{ReconcileInterval: interval})
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var starts, ends []time.Time
	enough := make(chan struct{})
	server.reconcile = func(ctx context.Context) (ReconcileResult, error) {
		mu.Lock()
		starts = append(starts, time.Now())
		mu.Unlock()
		select {
		case <-time.After(3 * interval):
		case <-ctx.Done():
			return ReconcileResult{}, ctx.Err()
		}
		mu.Lock()
		defer mu.Unlock()
		ends = append(ends, time.Now())
		if len(ends) == 4 {
			close(enough)
		}
		return ReconcileResult{}, nil
	}
	listener, err := serverFabric.Listen("tcp", "wefty://control-plane")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- server.Serve(ctx, listener) }()
	select {
	case <-enough:
	case <-time.After(30 * time.Second):
		t.Fatal("four reconcile passes did not finish")
	}
	cancel()
	if err := <-served; err != nil {
		t.Fatalf("Serve = %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	for pass := 1; pass < len(ends); pass++ {
		if gap := starts[pass].Sub(ends[pass-1]); gap < interval {
			t.Fatalf("pass %d started %s after pass %d ended, want at least the %s interval", pass, gap, pass-1, interval)
		}
	}
}

// Bounded passes must not trade completeness for the bound: after enough
// passes nothing the old per-service sweep would remove is left, including
// work that becomes due later and work spread across every part of the
// service list, while rows still inside their bounds stay.
func TestBoundedRetentionPassesLeaveNothingTheOldSweepWouldRemove(t *testing.T) {
	store, clock := openRetentionSweepStore(t, StoreOptions{ServiceLogRetentionBytes: 64})
	const services = 3000
	seedIdleServices(t, store, "svc", services)
	now := clock.Now()
	day := 24 * time.Hour
	bandStart := now.Add(-20 * day).UnixNano()

	// A one-shot's rows inside its own 30-day bound but past the services'
	// 7 days, more than one pass reads. Service rows past their bound lie
	// among them, so the age bound must step over these and evict none.
	oneshot, _, err := store.CreateJob(context.Background(), validJobSpec("band", nil))
	if err != nil {
		t.Fatal(err)
	}
	seedAttempts(t, store, oneshot.JobID, 1, contract.AttemptSucceeded, bandStart)
	const bandRows = 5000
	retentionSweepExec(t, store, `WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i+1 FROM n WHERE i<?-1)
		INSERT INTO log_events(job_id, attempt_id, stream, sequence, sequence_end, timestamp_ns, bytes, event_json)
		SELECT ?, ?, 'stdout', i, i, ?+i*?, X'62', X'7B7D' FROM n`,
		bandRows, oneshot.JobID, seededAttemptID(oneshot.JobID, 1), bandStart, int64(time.Minute))
	// The oldest rows share one timestamp across more than one batch the walk
	// reads, and a service's due rows come last among them, so the walk must
	// resume inside a run of equal timestamps.
	const tiedRows = 600
	tied := bandStart - int64(time.Minute)
	retentionSweepExec(t, store, `WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i+1 FROM n WHERE i<?-1)
		INSERT INTO log_events(job_id, attempt_id, stream, sequence, sequence_end, timestamp_ns, bytes, event_json)
		SELECT ?, ?, 'stdout', ?+i, ?+i, ?, X'62', X'7B7D' FROM n`,
		tiedRows, oneshot.JobID, seededAttemptID(oneshot.JobID, 1), bandRows, bandRows, tied)
	tiedJob := seededServiceID("svc", services-2)
	seedAttempts(t, store, tiedJob, 1, contract.AttemptSucceeded, bandStart)
	seedLogRows(t, store, tiedJob, seededAttemptID(tiedJob, 1), 0, []byte("tied"), tied, tied, tied, now.UnixNano())

	aged := []string{tiedJob}
	var overCap, crashLooping []string
	for index := 1; index <= services; index++ {
		jobID := seededServiceID("svc", index)
		switch {
		case index%60 == 1:
			// Three rows past the age bound, scattered through the band, and
			// one fresh row.
			aged = append(aged, jobID)
			seedAttempts(t, store, jobID, 1, contract.AttemptSucceeded, bandStart)
			minute := int64(time.Minute)
			spread := func(k int) int64 { return bandStart + int64((index*97+k*1777)%bandRows)*minute + 1 }
			seedLogRows(t, store, jobID, seededAttemptID(jobID, 1), 0, []byte("aged"), spread(0), spread(1), spread(2), now.UnixNano())
		case index%150 == 7:
			// 100 fresh bytes over a cap lowered to 64.
			overCap = append(overCap, jobID)
			seedAttempts(t, store, jobID, 1, contract.AttemptSucceeded, now.UnixNano())
			timestamps := make([]int64, 10)
			for k := range timestamps {
				timestamps[k] = now.UnixNano() + int64(k)
			}
			seedLogRows(t, store, jobID, seededAttemptID(jobID, 1), 0, []byte("0123456789"), timestamps...)
		case index%37 == 5:
			// Eight summaries past the floor of 32.
			crashLooping = append(crashLooping, jobID)
			seedAttempts(t, store, jobID, DefaultServiceAttemptSummaries+8, contract.AttemptFailed, now.Add(-day).UnixNano())
		}
	}
	// The last service's oldest summary past the floor is a lost attempt still
	// inside its late-evidence window: prunable only once the window closes.
	lateJob := seededServiceID("svc", services)
	seedAttempts(t, store, lateJob, DefaultServiceAttemptSummaries+1, contract.AttemptFailed, now.Add(-day).UnixNano())
	lateAttempt := seededAttemptID(lateJob, 1)
	retentionSweepExec(t, store, "UPDATE attempts SET state=?, updated_ns=? WHERE attempt_id=?",
		contract.AttemptLost, now.UnixNano(), lateAttempt)

	passes := func(count int) ReconcileResult {
		t.Helper()
		total := ReconcileResult{}
		for pass := 0; pass < count; pass++ {
			result, err := store.Reconcile(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			total.EvictedLogEvents += result.EvictedLogEvents
			total.PrunedAttempts += result.PrunedAttempts
		}
		return total
	}
	first := passes(64)
	wantEvicted := int64(3*len(aged) + 4*len(overCap))
	wantPruned := int64(8 * len(crashLooping))
	if first.EvictedLogEvents != wantEvicted || first.PrunedAttempts != wantPruned {
		t.Fatalf("passes evicted %d rows and pruned %d summaries, want %d and %d",
			first.EvictedLogEvents, first.PrunedAttempts, wantEvicted, wantPruned)
	}
	assertNothingLeftForTheOldSweep(t, store)
	var band int
	if err := store.db.QueryRow("SELECT COUNT(*) FROM log_events WHERE job_id=?", oneshot.JobID).Scan(&band); err != nil {
		t.Fatal(err)
	}
	if band != bandRows+tiedRows {
		t.Fatalf("one-shot rows inside their own bound = %d, want all %d", band, bandRows+tiedRows)
	}
	for _, jobID := range aged {
		truncation, err := readLogTruncation(context.Background(), store.db, jobID)
		if err != nil {
			t.Fatal(err)
		}
		if truncation == nil || truncation.BoundKind != LogRetentionAge || truncation.EvictedEventCount != 3 ||
			truncation.EarliestRetainedAt == nil || !truncation.EarliestRetainedAt.Equal(now) {
			t.Fatalf("aged service %s marker = %#v, want three age evictions and the fresh row retained", jobID, truncation)
		}
	}
	var lateKept int
	if err := store.db.QueryRow("SELECT COUNT(*) FROM attempts WHERE attempt_id=?", lateAttempt).Scan(&lateKept); err != nil {
		t.Fatal(err)
	}
	if lateKept != 1 {
		t.Fatal("a lost attempt inside its late-evidence window was pruned")
	}
	assertUsageCountersMatchLogEvents(t, store)

	clock.Advance(DefaultLateEvidenceWindow + time.Minute)
	if later := passes(64); later.EvictedLogEvents != 0 || later.PrunedAttempts != 1 {
		t.Fatalf("after the window closed, passes evicted %d rows and pruned %d summaries, want 0 and the one lost attempt",
			later.EvictedLogEvents, later.PrunedAttempts)
	}
	if err := store.db.QueryRow("SELECT COUNT(*) FROM attempts WHERE attempt_id=?", lateAttempt).Scan(&lateKept); err != nil {
		t.Fatal(err)
	}
	if lateKept != 0 {
		t.Fatal("a lost attempt past the floor outlived its late-evidence window")
	}
	assertNothingLeftForTheOldSweep(t, store)
	assertUsageCountersMatchLogEvents(t, store)
}

// Deleting an attempt clears the publication of the service that names it.
// Without an index that lookup scanned every service, so each pruned
// summary cost a pass time in proportion to the idle services (#939).
func TestDeletingAnAttemptLooksUpItsPublishingServiceByIndex(t *testing.T) {
	store, _ := openRetentionSweepStore(t, StoreOptions{})
	rows, err := store.db.Query("EXPLAIN QUERY PLAN DELETE FROM attempts WHERE attempt_id='any'")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var plan []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		plan = append(plan, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(plan, "SEARCH service_jobs USING COVERING INDEX service_jobs_published_attempt (published_attempt_id=?)") {
		t.Fatalf("attempt deletion plan = %q, want the published-attempt index on service_jobs", plan)
	}
}
