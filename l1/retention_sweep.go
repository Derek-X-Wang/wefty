package l1

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"sync"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

// The reconcile sweep's retention steps never visit services one by one and
// each reads a bounded number of index rows per pass, so the write lock a pass
// holds does not grow with the number of jobs, rows or attempts (#939). Each
// step walks one index in key order, resumes where the last committed pass
// stopped, and starts over at the beginning once it reaches the end, so every
// item still due is reached within one round.
//
// Every walk seeks on both parts of its key: the rest of the current key
// group with an equality and a range on the second part, then later groups
// with a range on the first part. SQLite seeks a row-value comparison on the
// first column only, so a long run of equal first parts (one-shot rows that
// share a timestamp, usage rows that share a byte count) would otherwise be
// read again on every pass.
const (
	// ageSweepScanRows bounds how many age-index rows one pass reads for each
	// class's age bound. Each walk steps over rows of the other class, which
	// have their own bound, so it is bounded by rows read, not only by rows
	// evicted.
	ageSweepScanRows = 2048
	// byteSweepScanRows bounds how many usage-index rows one pass reads for
	// each class's per-job cap. The rows over one class's cap include jobs of
	// the other class still under their own cap.
	byteSweepScanRows = 256
	// attemptSummarySweepWindow bounds how many services one pass checks for
	// attempt summaries past the floor, reading at most the floor's worth of
	// index entries each.
	attemptSummarySweepWindow = 256
	// attemptSummarySweepRows bounds how many attempt index entries one pass
	// reads and so how many summaries it may prune, across every service,
	// including attempts it must keep because their logs are retained.
	attemptSummarySweepRows = 256
	// fencedRunRows is how many rows of one timestamp stored after a round
	// began the age walk reads before it seeks past that timestamp. Such
	// rows follow all of their timestamp's round rows, so the rest of the
	// run is the next round's too.
	fencedRunRows = 32
)

// retentionSweepCursor is where the bounded retention steps resume. It only
// spreads their work across passes: any position is safe, because each walk
// starts over once it reaches its end. A pass that does not commit leaves it
// as it was, and a new process starts every walk at its beginning.
type retentionSweepCursor struct {
	mu   sync.Mutex
	next retentionSweepResume
}

type retentionSweepResume struct {
	serviceAge   logAgeWalk
	serviceBytes usageWalk
	summaries    summaryWalk
	oneshotBytes usageWalk
	oneshotAge   logAgeWalk
}

func (cursor *retentionSweepCursor) load() retentionSweepResume {
	cursor.mu.Lock()
	defer cursor.mu.Unlock()
	return cursor.next
}

func (cursor *retentionSweepCursor) save(next retentionSweepResume) {
	cursor.mu.Lock()
	defer cursor.mu.Unlock()
	cursor.next = next
}

// logAgeWalk is one round of an age walk over the (timestamp_ns, ordinal)
// index. The zero value starts a new round. A round covers only the rows
// that were already stored and past the cutoff when it began: its end is that
// cutoff and the highest ordinal then stored. Ordinals are AUTOINCREMENT, so
// every row stored later has a higher one, and neither ordinal nor timestamp
// ever changes. Rows that age past the cutoff, and rows uploaded later with an
// old timestamp, therefore belong to the next round. The walk still reads
// those later rows when it meets them, counts them against its per-pass read
// budget and skips them, so the write-lock hold stays bounded. Under a
// sustained overload of uploads with old timestamps, each at its own
// timestamp and faster than the budget, a round can stay open.
type logAgeWalk struct {
	started      bool
	roundEnd     int64
	roundOrdinal int64
	read         bool
	timestampNS  int64
	ordinal      int64
}

type logAgeWalkRow struct {
	logEvictionCandidate
	timestampNS int64
	service     bool
}

// enforceLogAgeBound applies one class's age bound: #49's for services, #52's
// for one-shots. It walks the (timestamp_ns, ordinal) index oldest-first, so a
// job with nothing old enough is never visited, and steps over rows of the
// other class, which have their own bound. One pass reads at most
// ageSweepScanRows rows and evicts within budget; the next resumes after the
// last row read.
func (s *Store) enforceLogAgeBound(ctx context.Context, tx *sql.Tx, now time.Time, budget *evictionBudget, services bool, walk logAgeWalk) (logRetentionStats, logAgeWalk, error) {
	age := s.logRetention.oneshotAge
	if services {
		age = s.serviceLogRetentionAge
	}
	cutoff := now.Add(-age).UnixNano()
	if budget.exhausted() {
		return logRetentionStats{}, walk, nil
	}
	if !walk.started {
		walk = logAgeWalk{started: true, roundEnd: cutoff}
		if err := tx.QueryRowContext(ctx, "SELECT COALESCE(MAX(ordinal), 0) FROM log_events").Scan(&walk.roundOrdinal); err != nil {
			return logRetentionStats{}, logAgeWalk{}, internalError(err, "read the last stored log row")
		}
	}
	// The cutoff only moves forward, unless the clock steps back; a row is
	// never evicted before it is due either way.
	end := min(walk.roundEnd, cutoff)
	perJob := map[string]*logRetentionStats{}
	scanned := 0
walking:
	for scanned < ageSweepScanRows {
		limit := min(ageSweepScanRows-scanned, logEvictionBatch)
		rows, fenced, err := logAgeWalkRows(ctx, tx, end, walk, limit)
		if err != nil {
			return logRetentionStats{}, walk, err
		}
		for _, row := range rows {
			switch {
			case row.ordinal > walk.roundOrdinal:
				// Stored after the round began: the next round's. The walk
				// moves past it.
			case row.service == services:
				// The whole-row budget rule of evictLogEvents: a row that does
				// not fit ends this pass's eviction, and the walk resumes at it.
				if budget.exhausted() {
					break walking
				}
				if row.bytes > budget.bytes {
					budget.bytes = 0
					break walking
				}
				stats := perJob[row.jobID]
				if stats == nil {
					stats = &logRetentionStats{}
					perJob[row.jobID] = stats
				}
				before := stats.events
				if err := deleteLogEvent(ctx, tx, row.logEvictionCandidate, stats); err != nil {
					return logRetentionStats{}, walk, err
				}
				if stats.events > before {
					budget.events--
					budget.bytes -= row.bytes
				}
			}
			walk.read, walk.timestampNS, walk.ordinal = true, row.timestampNS, row.ordinal
			scanned++
		}
		if !fenced && len(rows) < limit {
			walk = logAgeWalk{}
			break
		}
	}
	stats, err := recordLogTruncations(ctx, tx, perJob, LogRetentionAge, now)
	if err != nil {
		return logRetentionStats{}, walk, err
	}
	return stats, walk, nil
}

// logAgeWalkRows reads up to limit rows after the walk's position with a
// timestamp before end, in key order. Within the current timestamp the seek
// itself stops at the round's last ordinal. Beyond it, rows stored after the
// round began are read and counted like any other, so they bound the read
// too, and a timestamp's such rows follow all of its round rows: after
// fencedRunRows of them in a row the read stops and reports fenced, and the
// next read seeks past that timestamp without inspecting the rest of them.
func logAgeWalkRows(ctx context.Context, tx *sql.Tx, end int64, walk logAgeWalk, limit int) (rows []logAgeWalkRow, fenced bool, err error) {
	const columns = `SELECT e.timestamp_ns, e.ordinal, e.job_id, LENGTH(e.bytes),
			EXISTS (SELECT 1 FROM service_jobs sj WHERE sj.job_id=e.job_id)
		FROM log_events e `
	from := int64(math.MinInt64)
	if walk.read {
		if walk.timestampNS < end {
			rows, _, err = scanLogAgeWalkRows(ctx, tx, walk.roundOrdinal, columns+`WHERE e.timestamp_ns=? AND e.ordinal>? AND e.ordinal<=?
				ORDER BY e.ordinal LIMIT ?`, walk.timestampNS, walk.ordinal, walk.roundOrdinal, limit)
			if err != nil {
				return nil, false, err
			}
		}
		if len(rows) == limit || walk.timestampNS == math.MaxInt64 {
			return rows, false, nil
		}
		from = walk.timestampNS + 1
	}
	later, fenced, err := scanLogAgeWalkRows(ctx, tx, walk.roundOrdinal, columns+`WHERE e.timestamp_ns>=? AND e.timestamp_ns<?
		ORDER BY e.timestamp_ns, e.ordinal LIMIT ?`, from, end, limit-len(rows))
	if err != nil {
		return nil, false, err
	}
	return append(rows, later...), fenced, nil
}

// scanLogAgeWalkRows reads the query's rows, stopping after fencedRunRows
// rows of one timestamp past the round's last ordinal; closing the query
// there keeps SQLite from inspecting any further entries.
func scanLogAgeWalkRows(ctx context.Context, tx *sql.Tx, roundOrdinal int64, query string, args ...any) ([]logAgeWalkRow, bool, error) {
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, false, internalError(err, "walk log rows past an age bound")
	}
	defer rows.Close()
	var walked []logAgeWalkRow
	run := 0
	for rows.Next() {
		var row logAgeWalkRow
		if err := rows.Scan(&row.timestampNS, &row.ordinal, &row.jobID, &row.bytes, &row.service); err != nil {
			return nil, false, internalError(err, "scan log row past an age bound")
		}
		switch {
		case row.ordinal <= roundOrdinal:
			run = 0
		case run > 0 && walked[len(walked)-1].timestampNS == row.timestampNS:
			run++
		default:
			run = 1
		}
		walked = append(walked, row)
		if run == fencedRunRows {
			return walked, true, nil
		}
	}
	if err := rows.Err(); err != nil {
		return nil, false, internalError(err, "iterate log rows past an age bound")
	}
	return walked, false, nil
}

// usageWalk is the last row the byte re-trim of one class read from the
// usage index, walked from the most retained bytes down. The zero value
// starts a new round.
type usageWalk struct {
	read          bool
	retainedBytes int64
	rowID         int64
}

type usageWalkRow struct {
	rowID         int64
	jobID         string
	retainedBytes int64
	service       bool
}

// retrimJobsOverLogByteCap re-trims, within budget, the jobs of one class
// that the usage index shows over that class's per-job cap: at most
// byteSweepJobBudget jobs and byteSweepScanRows index rows a pass. Ingest is
// the mandatory byte site; this applies a cap lowered across a restart, or
// what an append's budget left, to a job that sends nothing more. A job
// trimmed only partly moves further along the walk and is met again.
func (s *Store) retrimJobsOverLogByteCap(ctx context.Context, tx *sql.Tx, now time.Time, budget *evictionBudget, services bool, walk usageWalk) (logRetentionStats, usageWalk, error) {
	total := logRetentionStats{}
	if budget.exhausted() {
		return total, walk, nil
	}
	limit := s.logRetention.oneshotBytes
	if services {
		limit = s.serviceLogRetentionBytes
	}
	rows, err := usageWalkRows(ctx, tx, limit, walk, byteSweepScanRows)
	if err != nil {
		return logRetentionStats{}, walk, err
	}
	trimmed := 0
	for _, row := range rows {
		if row.service == services {
			if trimmed == byteSweepJobBudget || budget.exhausted() {
				return total, walk, nil
			}
			stats, err := s.enforceJobLogByteRetention(ctx, tx, row.jobID, now, budget)
			if err != nil {
				return logRetentionStats{}, walk, err
			}
			total.add(stats)
			trimmed++
		}
		walk = usageWalk{read: true, retainedBytes: row.retainedBytes, rowID: row.rowID}
	}
	if len(rows) < byteSweepScanRows {
		walk = usageWalk{}
	}
	return total, walk, nil
}

// usageWalkRows reads up to limit usage rows over the byte cap after the
// walk's position, from the most retained bytes down.
func usageWalkRows(ctx context.Context, tx *sql.Tx, byteCap int64, walk usageWalk, limit int) ([]usageWalkRow, error) {
	const columns = `SELECT u.rowid, u.job_id, u.retained_bytes,
			EXISTS (SELECT 1 FROM service_jobs sj WHERE sj.job_id=u.job_id)
		FROM job_log_usage u `
	var rows []usageWalkRow
	below := int64(math.MaxInt64)
	if walk.read {
		rest, err := scanUsageWalkRows(ctx, tx, columns+`WHERE u.retained_bytes=? AND u.rowid<? AND u.retained_bytes>?
			ORDER BY u.rowid DESC LIMIT ?`, walk.retainedBytes, walk.rowID, byteCap, limit)
		if err != nil {
			return nil, err
		}
		rows = rest
		if len(rows) == limit {
			return rows, nil
		}
		below = walk.retainedBytes
	}
	query := columns + `WHERE u.retained_bytes>? AND u.retained_bytes<?
		ORDER BY u.retained_bytes DESC, u.rowid DESC LIMIT ?`
	if !walk.read {
		query = columns + `WHERE u.retained_bytes>? AND u.retained_bytes<=?
		ORDER BY u.retained_bytes DESC, u.rowid DESC LIMIT ?`
	}
	later, err := scanUsageWalkRows(ctx, tx, query, byteCap, below, limit-len(rows))
	if err != nil {
		return nil, err
	}
	return append(rows, later...), nil
}

func scanUsageWalkRows(ctx context.Context, tx *sql.Tx, query string, args ...any) ([]usageWalkRow, error) {
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, internalError(err, "walk jobs over their log byte cap")
	}
	defer rows.Close()
	var walked []usageWalkRow
	for rows.Next() {
		var row usageWalkRow
		if err := rows.Scan(&row.rowID, &row.jobID, &row.retainedBytes, &row.service); err != nil {
			return nil, internalError(err, "scan job over its log byte cap")
		}
		walked = append(walked, row)
	}
	if err := rows.Err(); err != nil {
		return nil, internalError(err, "iterate jobs over their log byte cap")
	}
	return walked, nil
}

// summaryWalk is where the attempt-summary step resumes: inside the history
// of jobID after the attempt key (createdNS, attemptID) when inHistory, and
// otherwise at the service after jobID. The zero value starts a new round
// at the first service.
type summaryWalk struct {
	jobID     string
	inHistory bool
	read      bool
	createdNS int64
	attemptID string
}

type attemptKey struct {
	createdNS int64
	attemptID string
}

// pruneServiceAttemptSummaryWindow is the sweep's attempt-summary step.
// Claims, completions and log appends prune their own service; the sweep
// catches what becomes prunable with no such write, such as a lost attempt
// whose late-evidence window closes or an attempt whose last log row ages out.
// Only a service with more non-current attempts than the floor can have one
// to prune, which a read of at most the floor's worth of index entries shows,
// so the step checks the next window of services in job order with one query
// and visits only those. Each visited history is walked oldest-first below
// the floor, at most attemptSummarySweepRows entries a pass across every
// service, kept attempts included, so neither a long history nor many
// prunable summaries can stretch one pass.
func (s *Store) pruneServiceAttemptSummaryWindow(ctx context.Context, tx *sql.Tx, now time.Time, walk summaryWalk) (int64, summaryWalk, error) {
	budget := attemptSummarySweepRows
	var pruned int64
	if walk.inHistory {
		count, next, done, read, err := s.pruneAttemptSummaryHistory(ctx, tx, now, walk, budget)
		if err != nil {
			return 0, walk, err
		}
		pruned += count
		budget -= read
		if !done {
			return pruned, next, nil
		}
		walk = summaryWalk{jobID: walk.jobID}
	}
	var last sql.NullString
	var checked int
	if err := tx.QueryRowContext(ctx, `SELECT MAX(job_id), COUNT(*) FROM (
		SELECT job_id FROM service_jobs WHERE job_id>? ORDER BY job_id LIMIT ?)`,
		walk.jobID, attemptSummarySweepWindow).Scan(&last, &checked); err != nil {
		return 0, walk, internalError(err, "select services to check for attempt summaries")
	}
	if checked == 0 {
		return pruned, summaryWalk{}, nil
	}
	rows, err := tx.QueryContext(ctx, `SELECT job_id FROM service_jobs
		WHERE job_id>? AND job_id<=?
			AND (SELECT 1 FROM attempts a
				WHERE a.job_id=service_jobs.job_id
					AND a.attempt_id<>COALESCE((SELECT current_attempt_id FROM jobs WHERE jobs.job_id=service_jobs.job_id), '')
				LIMIT 1 OFFSET ?) IS NOT NULL
		ORDER BY job_id`, walk.jobID, last.String, DefaultServiceAttemptSummaries)
	if err != nil {
		return 0, walk, internalError(err, "select services past the attempt summary floor")
	}
	candidates, err := scanJobIDs(rows, "service past the attempt summary floor")
	if err != nil {
		return 0, walk, err
	}
	for _, jobID := range candidates {
		history := summaryWalk{jobID: jobID, inHistory: true}
		if budget <= 0 {
			return pruned, history, nil
		}
		count, next, done, read, err := s.pruneAttemptSummaryHistory(ctx, tx, now, history, budget)
		if err != nil {
			return 0, walk, err
		}
		pruned += count
		budget -= read
		if !done {
			return pruned, next, nil
		}
	}
	if checked < attemptSummarySweepWindow {
		return pruned, summaryWalk{}, nil
	}
	return pruned, summaryWalk{jobID: last.String}, nil
}

// pruneAttemptSummaryHistory walks one service's attempts oldest-first from
// the walk's position, reading at most budget index entries, and prunes those
// pruneServiceAttemptSummaries would. It stops below the floor: the newest
// DefaultServiceAttemptSummaries non-current attempts, which the walk never
// reaches. done reports that it reached the floor.
func (s *Store) pruneAttemptSummaryHistory(ctx context.Context, tx *sql.Tx, now time.Time, walk summaryWalk, budget int) (pruned int64, next summaryWalk, done bool, read int, err error) {
	jobID := walk.jobID
	// The floor's oldest attempt: the walk covers only attempts before it.
	var floor attemptKey
	err = tx.QueryRowContext(ctx, `SELECT created_ns, attempt_id FROM attempts
		WHERE job_id=? AND attempt_id<>COALESCE((SELECT current_attempt_id FROM jobs WHERE job_id=?), '')
		ORDER BY created_ns DESC, attempt_id DESC LIMIT 1 OFFSET ?`,
		jobID, jobID, DefaultServiceAttemptSummaries-1).Scan(&floor.createdNS, &floor.attemptID)
	read = DefaultServiceAttemptSummaries
	if errors.Is(err, sql.ErrNoRows) {
		return 0, walk, true, read, nil
	}
	if err != nil {
		return 0, walk, false, read, internalError(err, "read the service attempt summary floor")
	}
	limit := max(budget-read, 1)
	keys, err := attemptHistoryKeys(ctx, tx, jobID, walk, floor, limit)
	if err != nil {
		return 0, walk, false, read, err
	}
	read += len(keys)
	if len(keys) > 0 {
		ids := make([]string, len(keys))
		for index, key := range keys {
			ids[index] = key.attemptID
		}
		encoded, err := json.Marshal(ids)
		if err != nil {
			return 0, walk, false, read, internalError(err, "encode attempt summaries to check")
		}
		result, err := tx.ExecContext(ctx, pruneListedAttemptSummariesSQL,
			string(encoded), jobID, jobID,
			contract.AttemptClaimed, contract.AttemptRunning, contract.AttemptAwaitingInput,
			contract.AttemptLost, now.Add(-s.lateEvidenceWindow).UnixNano())
		if err != nil {
			return 0, walk, false, read, internalError(err, "prune service attempt summaries past the floor")
		}
		if pruned, err = result.RowsAffected(); err != nil {
			return 0, walk, false, read, internalError(err, "read service attempt pruning result")
		}
		lastKey := keys[len(keys)-1]
		walk = summaryWalk{jobID: jobID, inHistory: true, read: true, createdNS: lastKey.createdNS, attemptID: lastKey.attemptID}
	}
	return pruned, walk, len(keys) < limit, read, nil
}

// pruneListedAttemptSummariesSQL deletes the listed attempts of one service
// that pruneServiceAttemptSummaries would delete, less its floor check: the
// sweep lists only attempts older than the floor. The unary plus keeps the
// planner on the listed attempt IDs instead of walking the whole history
// through job_id.
const pruneListedAttemptSummariesSQL = `DELETE FROM attempts
	WHERE attempt_id IN (SELECT value FROM json_each(?))
		AND +job_id=?
		AND EXISTS (SELECT 1 FROM service_jobs WHERE service_jobs.job_id=attempts.job_id)
		AND attempt_id<>COALESCE((SELECT current_attempt_id FROM jobs WHERE job_id=?), '')
		AND state NOT IN (?, ?, ?)
		AND NOT (state=? AND updated_ns>=?)
		AND NOT EXISTS (SELECT 1 FROM log_events WHERE log_events.attempt_id=attempts.attempt_id)`

// attemptHistoryKeys reads up to limit attempt keys of jobID after the walk's
// position and before floor, oldest-first.
func attemptHistoryKeys(ctx context.Context, tx *sql.Tx, jobID string, walk summaryWalk, floor attemptKey, limit int) ([]attemptKey, error) {
	var keys []attemptKey
	from := int64(math.MinInt64)
	if walk.read {
		rest, err := scanAttemptKeys(ctx, tx, `SELECT created_ns, attempt_id FROM attempts
			WHERE job_id=? AND created_ns=? AND attempt_id>?
				AND (created_ns<? OR attempt_id<?)
			ORDER BY attempt_id LIMIT ?`, jobID, walk.createdNS, walk.attemptID, floor.createdNS, floor.attemptID, limit)
		if err != nil {
			return nil, err
		}
		keys = rest
		if len(keys) == limit || walk.createdNS == math.MaxInt64 {
			return keys, nil
		}
		from = walk.createdNS + 1
	}
	later, err := scanAttemptKeys(ctx, tx, `SELECT created_ns, attempt_id FROM attempts
		WHERE job_id=? AND created_ns>=? AND created_ns<=?
			AND (created_ns<? OR attempt_id<?)
		ORDER BY created_ns, attempt_id LIMIT ?`, jobID, from, floor.createdNS, floor.createdNS, floor.attemptID, limit-len(keys))
	if err != nil {
		return nil, err
	}
	return append(keys, later...), nil
}

func scanAttemptKeys(ctx context.Context, tx *sql.Tx, query string, args ...any) ([]attemptKey, error) {
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, internalError(err, "walk a service's attempt summaries")
	}
	defer rows.Close()
	var keys []attemptKey
	for rows.Next() {
		var key attemptKey
		if err := rows.Scan(&key.createdNS, &key.attemptID); err != nil {
			return nil, internalError(err, "scan a service's attempt summary")
		}
		keys = append(keys, key)
	}
	if err := rows.Err(); err != nil {
		return nil, internalError(err, "iterate a service's attempt summaries")
	}
	return keys, nil
}
