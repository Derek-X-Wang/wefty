package l1

import (
	"context"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"modernc.org/sqlite"
)

// These tests bound the work of one reconcile pass deterministically, in
// SQLite pages read, rather than in wall-clock time (#939). The store runs on
// one connection, so the pages that connection reads during a pass are the
// pass's. A fixture is built the same way every run, so the counts are too.

// pagesRead returns, and resets, the pages the store's one connection has
// read from its page cache or the file since the last call.
func pagesRead(t *testing.T, store *Store) int {
	t.Helper()
	conn, err := store.db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	var total int
	if err := conn.Raw(func(driverConn any) error {
		status := driverConn.(sqlite.DBStatus)
		for _, op := range []sqlite.DBStatusOp{sqlite.DBStatusCacheHit, sqlite.DBStatusCacheMiss} {
			count, _, err := status.Status(op, true)
			if err != nil {
				return err
			}
			total += count
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return total
}

// countedPass runs one reconcile pass and returns its result and the pages it
// read. The store must have been given one connection.
func countedPass(t *testing.T, store *Store) (ReconcileResult, int) {
	t.Helper()
	pagesRead(t, store)
	result, err := store.Reconcile(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return result, pagesRead(t, store)
}

func oneConnection(store *Store) { store.db.SetMaxOpenConns(1) }

// seedOneshotWithAttempt submits a one-shot and gives it one attempt for log
// rows to belong to.
func seedOneshotWithAttempt(t *testing.T, store *Store, dispatchKey string) (string, string) {
	t.Helper()
	job, _, err := store.CreateJob(context.Background(), validJobSpec(dispatchKey, nil))
	if err != nil {
		t.Fatal(err)
	}
	seedAttempts(t, store, job.JobID, 1, contract.AttemptSucceeded, 1)
	return job.JobID, seededAttemptID(job.JobID, 1)
}

// seedRows adds count one-byte stdout rows to attemptID, sequences from
// firstSequence on, timestamped first, first+step, and so on.
func seedRows(t *testing.T, store *Store, jobID, attemptID string, firstSequence, count int, first, step int64) {
	t.Helper()
	retentionSweepExec(t, store, `WITH RECURSIVE n(i) AS (SELECT 0 UNION ALL SELECT i+1 FROM n WHERE i<?-1)
		INSERT INTO log_events(job_id, attempt_id, stream, sequence, sequence_end, timestamp_ns, bytes, event_json)
		SELECT ?, ?, 'stdout', ?+i, ?+i, ?+i*?, X'62', X'7B7D' FROM n`,
		count, jobID, attemptID, firstSequence, firstSequence, first, step)
}

func serviceAgeCutoff(store *Store) int64 {
	return store.clock.Now().Add(-store.serviceLogRetentionAge).UnixNano()
}

// One-shot rows can share a timestamp by the thousand. The age walk must seek
// past the ones it has read rather than read the whole group again each pass.
func TestServiceAgeWalkReadsFlatThroughAnEqualTimestampGroup(t *testing.T) {
	store, clock := openRetentionSweepStore(t, StoreOptions{})
	jobID, attemptID := seedOneshotWithAttempt(t, store, "tied")
	const tied = 30000
	seedRows(t, store, jobID, attemptID, 0, tied, clock.Now().Add(-10*24*time.Hour).UnixNano(), 0)
	oneConnection(store)
	var longestHold time.Duration
	store.reconcileHoldForTest = func(hold time.Duration) { longestHold = max(longestHold, hold) }

	var pages []int
	for pass := 0; pass < tied/ageSweepScanRows; pass++ {
		_, read := countedPass(t, store)
		pages = append(pages, read)
	}
	t.Logf("pages read per pass through %d rows sharing one timestamp: %v; longest hold %s", tied, pages, longestHold)
	// Every pass reads the same number of rows, each with its own lookups;
	// what must not grow is the part spent passing rows already read, which
	// grows with every pass when the walk seeks on the timestamp alone.
	if grown := pages[len(pages)-1] - pages[0]; grown > 64 {
		t.Fatalf("the pass %d rows into the group read %d pages more than the first; pages %v",
			(len(pages)-1)*ageSweepScanRows, grown, pages)
	}
}

// Rows keep arriving ahead of the walk on every pass: rows aging past the
// cutoff, and one-shot uploads with timestamps already past it. A round must
// still end, so a service row stored behind the walk is reached in the next
// round, again and again.
func TestServiceAgeWalkRoundsEndWhileRowsArriveAheadOfThem(t *testing.T) {
	store, clock := openRetentionSweepStore(t, StoreOptions{})
	oneshotJob, oneshotAttempt := seedOneshotWithAttempt(t, store, "stream")
	bandStart := clock.Now().Add(-10 * 24 * time.Hour).UnixNano()
	const band, arriving = 6000, 2500
	seedRows(t, store, oneshotJob, oneshotAttempt, 0, band, bandStart, int64(time.Millisecond))
	sequence := band
	seedIdleServices(t, store, "svc", 1)
	serviceJob := seededServiceID("svc", 1)
	seedAttempts(t, store, serviceJob, 1, contract.AttemptSucceeded, 1)
	strand := func(sequence int) {
		// Older than every row the walk has read.
		seedLogRows(t, store, serviceJob, seededAttemptID(serviceJob, 1), sequence, []byte("late"), bandStart-int64(10*24*time.Hour))
	}
	stranded := func() bool {
		var left int
		if err := store.db.QueryRow("SELECT COUNT(*) FROM log_events WHERE job_id=?", serviceJob).Scan(&left); err != nil {
			t.Fatal(err)
		}
		return left > 0
	}
	var longestHold time.Duration
	store.reconcileHoldForTest = func(hold time.Duration) { longestHold = max(longestHold, hold) }

	// Every round ends at or after this cutoff, so rows stamped just before
	// it lie ahead of the walk until a round reaches its very end.
	firstCutoff := serviceAgeCutoff(store)
	if _, err := store.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	strand(0)
	var evicted []int
	for pass := 1; pass <= 64 && len(evicted) < 2; pass++ {
		// More rows than a pass reads arrive ahead of the walk on every pass:
		// rows that just aged past the cutoff, and uploads all backdated to
		// one timestamp just before the end of any round, whose ever higher
		// ordinals stay ahead of the walk even inside that timestamp.
		seedRows(t, store, oneshotJob, oneshotAttempt, sequence, arriving, serviceAgeCutoff(store)-int64(arriving), 1)
		sequence += arriving
		seedRows(t, store, oneshotJob, oneshotAttempt, sequence, arriving, firstCutoff-1, 0)
		sequence += arriving
		clock.Advance(time.Second)
		if _, err := store.Reconcile(context.Background()); err != nil {
			t.Fatal(err)
		}
		if !stranded() {
			evicted = append(evicted, pass)
			// Strand another while the next round is under way.
			strand(len(evicted))
		}
	}
	t.Logf("rows stranded behind the walk were evicted on passes %v; longest hold %s", evicted, longestHold)
	if len(evicted) < 2 {
		t.Fatalf("with rows arriving ahead of the walk on every pass, only %d of 2 stranded service rows were evicted in 64 passes: a round never ended", len(evicted))
	}
}

// Uploads backdated to a timestamp ahead of the walk after its round began
// belong to the next round. However many share that timestamp, the walk must
// inspect a bounded number of them before passing on.
func TestAgeWalkInspectsBoundedRowsUploadedAfterItsRoundBegan(t *testing.T) {
	pagesWith := func(uploaded int) int {
		store, clock := openRetentionSweepStore(t, StoreOptions{})
		jobID, attemptID := seedOneshotWithAttempt(t, store, "fenced")
		// One pass's rows and a few more, so the measured pass has little
		// else to read.
		bandStart := clock.Now().Add(-10 * 24 * time.Hour).UnixNano()
		const band = ageSweepScanRows + 10
		seedRows(t, store, jobID, attemptID, 0, band, bandStart, int64(time.Millisecond))
		oneConnection(store)
		if _, err := store.Reconcile(context.Background()); err != nil {
			t.Fatal(err)
		}
		// Ahead of the walk, at one timestamp among the band's last rows.
		seedRows(t, store, jobID, attemptID, band, uploaded, bandStart+(ageSweepScanRows+2)*int64(time.Millisecond)+1, 0)
		_, pages := countedPass(t, store)
		return pages
	}
	few, many := pagesWith(8000), pagesWith(80000)
	t.Logf("the pass that reached the uploads read %d pages past 8000 of them and %d past 80000", few, many)
	if many > few+few/2 {
		t.Fatalf("a pass read %d pages past 80,000 rows uploaded after its round began against %d past 8,000", many, few)
	}
}

// A backlog of service rows older than the one-shot bound lies at the front
// of the age index. The one-shot age bound must step over a bounded number of
// them a pass, not read the whole backlog every pass.
func TestOneshotAgeWalkReadsBoundedRowsPastAServiceBacklog(t *testing.T) {
	pagesWith := func(backlog int) int {
		store, clock := openRetentionSweepStore(t, StoreOptions{})
		seedIdleServices(t, store, "svc", 1)
		serviceJob := seededServiceID("svc", 1)
		seedAttempts(t, store, serviceJob, 1, contract.AttemptSucceeded, 1)
		seedRows(t, store, serviceJob, seededAttemptID(serviceJob, 1), 0, backlog, clock.Now().Add(-40*24*time.Hour).UnixNano(), 1)
		oneConnection(store)
		_, pages := countedPass(t, store)
		return pages
	}
	few, many := pagesWith(8000), pagesWith(80000)
	t.Logf("a pass over a service backlog past the one-shot bound read %d pages at 8000 rows and %d at 80000", few, many)
	if many > few+few/2 {
		t.Fatalf("a pass read %d pages over an 80,000-row service backlog against %d over 8,000", many, few)
	}
}

// Lowering the service cap leaves one-shots between the two caps in the usage
// index above it. The service re-trim must not read all of them every pass.
func TestServiceByteRetrimReadsBoundedUsageRows(t *testing.T) {
	pagesWith := func(oneshots int) (int, int) {
		store, _ := openRetentionSweepStore(t, StoreOptions{ServiceLogRetentionBytes: 64})
		// The service over its cap shares its byte count with every one-shot
		// and was stored first, so the walk reaches it last.
		seedIdleServices(t, store, "svc", 1)
		serviceJob := seededServiceID("svc", 1)
		seedAttempts(t, store, serviceJob, 1, contract.AttemptSucceeded, 1)
		seedLogRows(t, store, serviceJob, seededAttemptID(serviceJob, 1), 0, make([]byte, 100), store.clock.Now().UnixNano())
		template, attemptID := seedOneshotWithAttempt(t, store, "under-own-cap")
		retentionSweepExec(t, store, `WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<?)
			INSERT INTO jobs(job_id, dispatch_key, request_hash, spec_json, state, created_ns, updated_ns)
			SELECT printf('oneshot-%06d', i), printf('oneshot-key-%06d', i), request_hash, spec_json, state, created_ns, updated_ns
			FROM n, jobs WHERE jobs.job_id=?`, oneshots, template)
		retentionSweepExec(t, store, `WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<?)
			INSERT INTO log_events(job_id, attempt_id, stream, sequence, sequence_end, timestamp_ns, bytes, event_json)
			SELECT printf('oneshot-%06d', i), ?, 'stdout', i, i, ?, zeroblob(100), X'7B7D' FROM n`,
			oneshots, attemptID, store.clock.Now().UnixNano())
		oneConnection(store)
		_, first := countedPass(t, store)
		for pass := 2; ; pass++ {
			var retained int64
			if err := store.db.QueryRow("SELECT retained_bytes FROM job_log_usage WHERE job_id=?", serviceJob).Scan(&retained); err != nil {
				t.Fatal(err)
			}
			if retained <= 64 {
				return first, pass - 1
			}
			if pass > oneshots/byteSweepScanRows+3 {
				t.Fatalf("the service over its cap was not re-trimmed in %d passes", pass-1)
			}
			countedPass(t, store)
		}
	}
	few, fewPasses := pagesWith(800)
	many, manyPasses := pagesWith(8000)
	t.Logf("first pass read %d pages with 800 one-shots between the caps and %d with 8000; re-trimmed after %d and %d passes",
		few, many, fewPasses, manyPasses)
	if many > few+few/2 {
		t.Fatalf("a pass read %d pages with 8000 one-shots between the caps against %d with 800", many, few)
	}
}

// A service's attempt history can be long, and much of it kept because its
// logs are retained. Summary pruning must read and delete a bounded number of
// attempts per pass, whatever the history's length, and still remove every
// summary past the floor.
func TestServiceSummaryPruningIsBoundedPerPass(t *testing.T) {
	mostPagesWith := func(kept, prunable int) int {
		store, clock := openRetentionSweepStore(t, StoreOptions{})
		seedIdleServices(t, store, "svc", 1)
		jobID := seededServiceID("svc", 1)
		// Oldest first: attempts kept by a fresh log row each, then prunable
		// ones, then the floor.
		seedAttempts(t, store, jobID, kept+prunable+DefaultServiceAttemptSummaries, contract.AttemptFailed, clock.Now().Add(-24*time.Hour).UnixNano())
		retentionSweepExec(t, store, `WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<?)
			INSERT INTO log_events(job_id, attempt_id, stream, sequence, sequence_end, timestamp_ns, bytes, event_json)
			SELECT ?, printf('%s-a%03d', ?, i), 'stdout', 0, 0, ?, X'6B', X'7B7D' FROM n`,
			kept, jobID, jobID, clock.Now().UnixNano())
		oneConnection(store)
		var longestHold time.Duration
		store.reconcileHoldForTest = func(hold time.Duration) { longestHold = max(longestHold, hold) }

		most, passes := 0, 0
		var total int64
		for total < int64(prunable) {
			passes++
			if passes > 2*(kept+prunable)/attemptSummarySweepRows+2 {
				t.Fatalf("pruned %d of %d summaries past the floor in %d passes", total, prunable, passes-1)
			}
			result, read := countedPass(t, store)
			if result.PrunedAttempts > attemptSummarySweepRows {
				t.Fatalf("pass %d pruned %d attempt summaries, over the %d-entry bound", passes, result.PrunedAttempts, attemptSummarySweepRows)
			}
			total += result.PrunedAttempts
			most = max(most, read)
		}
		var left int
		if err := store.db.QueryRow("SELECT COUNT(*) FROM attempts WHERE job_id=?", jobID).Scan(&left); err != nil {
			t.Fatal(err)
		}
		if left != kept+DefaultServiceAttemptSummaries {
			t.Fatalf("retained attempts = %d, want the %d kept by logs and the floor of %d", left, kept, DefaultServiceAttemptSummaries)
		}
		assertNothingLeftForTheOldSweep(t, store)
		t.Logf("history of %d kept and %d prunable: pruned in %d passes, at most %d pages and %s a pass",
			kept, prunable, passes, most, longestHold)
		return most
	}
	// Ten times the history may cost a pass more only through deeper
	// B-trees; deleting the whole backlog at once costs about ten times more.
	short := mostPagesWith(2000, 2000)
	long := mostPagesWith(20000, 20000)
	if long > 2*short {
		t.Fatalf("the costliest pass read %d pages over a 40,000-attempt history against %d over 4,000", long, short)
	}
}

// Pruning the listed attempts must look each one up by its ID. Planned
// through job_id, the delete reads the service's whole history every pass,
// which a page count barely shows because index pages hold many entries.
func TestSummaryPruningLooksUpListedAttemptsByID(t *testing.T) {
	store, _ := openRetentionSweepStore(t, StoreOptions{})
	rows, err := store.db.Query("EXPLAIN QUERY PLAN "+pruneListedAttemptSummariesSQL, "[]", "job", "job", "a", "b", "c", "d", 0)
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
	if len(plan) == 0 || plan[0] != "SEARCH attempts USING INDEX sqlite_autoindex_attempts_1 (attempt_id=?)" {
		t.Fatalf("summary prune plan = %q, want a lookup of each listed attempt ID", plan)
	}
}
