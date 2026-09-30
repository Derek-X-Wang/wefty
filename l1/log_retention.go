package l1

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

// logRetentionLimits are the #52 bounds beside #49's service bounds: one-shot
// jobs keep their logs up to oneshotAge and oneshotBytes each, and every job's
// logs together stay under totalBytes. Services keep their own 7-day/32 MiB
// per-job bounds and fall under the same total ceiling.
type logRetentionLimits struct {
	oneshotBytes int64
	oneshotAge   time.Duration
	totalBytes   int64
}

func resolveLogRetentionLimits(options StoreOptions) (logRetentionLimits, error) {
	limits := logRetentionLimits{
		oneshotBytes: options.OneshotLogRetentionBytes,
		oneshotAge:   options.OneshotLogRetentionAge,
		totalBytes:   options.LogRetentionTotalBytes,
	}
	if limits.oneshotBytes < 0 {
		return logRetentionLimits{}, fmt.Errorf("l1: one-shot log retention bytes must be non-negative")
	}
	if limits.oneshotAge < 0 {
		return logRetentionLimits{}, fmt.Errorf("l1: one-shot log retention age must be non-negative")
	}
	if limits.totalBytes < 0 {
		return logRetentionLimits{}, fmt.Errorf("l1: total log retention bytes must be non-negative")
	}
	if limits.oneshotBytes == 0 {
		limits.oneshotBytes = DefaultOneshotLogRetentionBytes
	}
	if limits.oneshotAge == 0 {
		limits.oneshotAge = DefaultOneshotLogRetentionAge
	}
	if limits.totalBytes == 0 {
		limits.totalBytes = DefaultLogRetentionTotalBytes
	}
	return limits, nil
}

const (
	// logEvictionBatch is how many candidate rows one query materializes.
	logEvictionBatch = 512
	// The reconcile sweep evicts at most this much per pass across every
	// bound and every job, so a backlog (an upgrade, a lowered cap) is worked
	// off across passes instead of holding the write transaction that
	// renewals, claims and completions wait behind.
	sweepEvictionEvents       = 4096
	sweepEvictionBytes  int64 = 64 << 20
	// An append transaction evicts at most twice what one batch can add
	// (MaxLogBatchEvents events inside a MaxLogUploadBodyBytes body), so a job
	// at its cap stays there under steady ingest; anything left over is the
	// sweep's.
	appendEvictionEvents       = 2 * MaxLogBatchEvents
	appendEvictionBytes  int64 = 2 * MaxLogUploadBodyBytes
	// oneshotByteSweepJobBudget bounds how many over-cap one-shots one pass
	// looks at. Ingest is the first byte site; this catches a job left over a
	// lowered cap or an append-bounded backlog.
	oneshotByteSweepJobBudget = 16
)

// evictionBudget is what one transaction may still delete.
type evictionBudget struct {
	events int
	bytes  int64
}

func sweepEvictionBudget() *evictionBudget {
	return &evictionBudget{events: sweepEvictionEvents, bytes: sweepEvictionBytes}
}

func appendEvictionBudget() *evictionBudget {
	return &evictionBudget{events: appendEvictionEvents, bytes: appendEvictionBytes}
}

func (budget *evictionBudget) exhausted() bool {
	return budget.events <= 0 || budget.bytes <= 0
}

type logRetentionStats struct {
	events         int64
	bytes          int64
	throughOrdinal int64
}

func (stats *logRetentionStats) add(other logRetentionStats) {
	stats.events += other.events
	stats.bytes += other.bytes
	if other.throughOrdinal > stats.throughOrdinal {
		stats.throughOrdinal = other.throughOrdinal
	}
}

type logEvictionCandidate struct {
	ordinal int64
	jobID   string
	bytes   int64
}

func jobIsService(ctx context.Context, q queryer, jobID string) (bool, error) {
	var service bool
	if err := q.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM service_jobs WHERE job_id=?)", jobID).Scan(&service); err != nil {
		return false, internalError(err, "read log retention class")
	}
	return service, nil
}

// enforceJobLogByteRetention applies the per-job byte cap of the job's class
// within budget. The append transaction calls it with a bounded budget; the
// sweep calls it again so a lowered cap, or what an append left over, is
// worked off an idle job too.
func (s *Store) enforceJobLogByteRetention(ctx context.Context, tx *sql.Tx, jobID string, now time.Time, budget *evictionBudget) (logRetentionStats, error) {
	service, err := jobIsService(ctx, tx, jobID)
	if err != nil {
		return logRetentionStats{}, err
	}
	limit := s.logRetention.oneshotBytes
	if service {
		limit = s.serviceLogRetentionBytes
	}
	var retainedBytes int64
	err = tx.QueryRowContext(ctx, "SELECT retained_bytes FROM job_log_usage WHERE job_id=?", jobID).Scan(&retainedBytes)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return logRetentionStats{}, internalError(err, "measure retained job log bytes")
	}
	if retainedBytes <= limit {
		if err := refreshLogTruncation(ctx, tx, jobID, now); err != nil {
			return logRetentionStats{}, err
		}
		return logRetentionStats{}, nil
	}
	perJob := map[string]*logRetentionStats{}
	if err := evictLogEvents(ctx, tx, jobID, "", nil, budget, retainedBytes-limit, perJob); err != nil {
		return logRetentionStats{}, err
	}
	return recordLogTruncations(ctx, tx, perJob, LogRetentionBytes, now)
}

func refreshLogTruncation(ctx context.Context, tx *sql.Tx, jobID string, now time.Time) error {
	_, err := tx.ExecContext(ctx, `UPDATE job_log_truncations
		SET earliest_retained_ns=(SELECT MIN(timestamp_ns) FROM log_events WHERE job_id=?), updated_ns=?
		WHERE job_id=? AND earliest_retained_ns IS NOT (SELECT MIN(timestamp_ns) FROM log_events WHERE job_id=?)`,
		jobID, now.UnixNano(), jobID, jobID)
	if err != nil {
		return internalError(err, "refresh earliest retained job log timestamp")
	}
	return nil
}

// enforceServiceLogAgeRetention is #49's per-service age sweep. Services are
// few and each is walked; one-shots are many and kept forever as records, so
// their age bound walks the timestamp index instead (enforceOneshotLogRetention).
func (s *Store) enforceServiceLogAgeRetention(ctx context.Context, tx *sql.Tx, jobID string, now time.Time, budget *evictionBudget) (logRetentionStats, error) {
	cutoff := now.Add(-s.serviceLogRetentionAge).UnixNano()
	perJob := map[string]*logRetentionStats{}
	if err := evictLogEvents(ctx, tx, jobID, " AND e.timestamp_ns < ?", []any{cutoff}, budget, -1, perJob); err != nil {
		return logRetentionStats{}, err
	}
	return recordLogTruncations(ctx, tx, perJob, LogRetentionAge, now)
}

// enforceOneshotLogRetention is the sweep's one-shot half: a re-trim of
// one-shots over their per-job cap, then age eviction oldest-first by each
// event's own timestamp across every one-shot, all within budget.
func (s *Store) enforceOneshotLogRetention(ctx context.Context, tx *sql.Tx, now time.Time, budget *evictionBudget) (logRetentionStats, error) {
	total := logRetentionStats{}
	if budget.exhausted() {
		return total, nil
	}
	rows, err := tx.QueryContext(ctx, `SELECT u.job_id FROM job_log_usage u
		WHERE u.retained_bytes > ?
			AND NOT EXISTS (SELECT 1 FROM service_jobs sj WHERE sj.job_id=u.job_id)
		ORDER BY u.retained_bytes DESC, u.job_id LIMIT ?`, s.logRetention.oneshotBytes, oneshotByteSweepJobBudget)
	if err != nil {
		return logRetentionStats{}, internalError(err, "select one-shots over their log byte cap")
	}
	overCap, err := scanJobIDs(rows, "one-shot over its log byte cap")
	if err != nil {
		return logRetentionStats{}, err
	}
	for _, jobID := range overCap {
		stats, err := s.enforceJobLogByteRetention(ctx, tx, jobID, now, budget)
		if err != nil {
			return logRetentionStats{}, err
		}
		total.add(stats)
	}

	cutoff := now.Add(-s.logRetention.oneshotAge).UnixNano()
	perJob := map[string]*logRetentionStats{}
	if err := evictLogEvents(ctx, tx, "", ` AND e.timestamp_ns < ?
		AND NOT EXISTS (SELECT 1 FROM service_jobs sj WHERE sj.job_id=e.job_id)`, []any{cutoff}, budget, -1, perJob); err != nil {
		return logRetentionStats{}, err
	}
	stats, err := recordLogTruncations(ctx, tx, perJob, LogRetentionAge, now)
	if err != nil {
		return logRetentionStats{}, err
	}
	total.add(stats)
	return total, nil
}

// enforceLogRetentionTotal applies the cluster-wide ceiling to every job's
// logs, one-shot and service alike: while the maintained total exceeds it,
// the oldest events by their own timestamp go first, within budget.
func (s *Store) enforceLogRetentionTotal(ctx context.Context, tx *sql.Tx, now time.Time, budget *evictionBudget) (logRetentionStats, error) {
	var retained int64
	err := tx.QueryRowContext(ctx, "SELECT retained_bytes FROM log_usage_total WHERE singleton=1").Scan(&retained)
	if err != nil {
		return logRetentionStats{}, internalError(err, "measure total retained log bytes")
	}
	excess := retained - s.logRetention.totalBytes
	if excess <= 0 {
		return logRetentionStats{}, nil
	}
	perJob := map[string]*logRetentionStats{}
	if err := evictLogEvents(ctx, tx, "", "", nil, budget, excess, perJob); err != nil {
		return logRetentionStats{}, err
	}
	return recordLogTruncations(ctx, tx, perJob, LogRetentionTotal, now)
}

// evictLogEvents deletes matching rows oldest-first, attributing each to its
// job, until byteTarget bytes are gone (negative: every matching row) or the
// budget is spent. Any row may go: upload continuity lives in
// log_stream_continuity, never in the retained rows.
func evictLogEvents(ctx context.Context, tx *sql.Tx, jobID, extraPredicate string, args []any, budget *evictionBudget, byteTarget int64, perJob map[string]*logRetentionStats) error {
	var freed int64
	done := func() bool { return budget.exhausted() || (byteTarget >= 0 && freed >= byteTarget) }
	for !done() {
		limit := min(budget.events, logEvictionBatch)
		candidates, err := logEvictionCandidates(ctx, tx, jobID, extraPredicate, limit, args...)
		if err != nil {
			return err
		}
		deleted := 0
		for _, candidate := range candidates {
			if done() {
				return nil
			}
			// A row is deleted whole or not at all, so one that does not fit
			// what is left of the byte budget ends the pass; no row exceeds a
			// fresh budget (MaxLogEventBytes), so every pass makes progress.
			if candidate.bytes > budget.bytes {
				budget.bytes = 0
				return nil
			}
			stats := perJob[candidate.jobID]
			if stats == nil {
				stats = &logRetentionStats{}
				perJob[candidate.jobID] = stats
			}
			before := stats.events
			if err := deleteLogEvent(ctx, tx, candidate, stats); err != nil {
				return err
			}
			if stats.events > before {
				deleted++
				freed += candidate.bytes
				budget.events--
				budget.bytes -= candidate.bytes
			}
		}
		if len(candidates) < limit || deleted == 0 {
			return nil
		}
	}
	return nil
}

func recordLogTruncations(ctx context.Context, tx *sql.Tx, perJob map[string]*logRetentionStats, bound LogRetentionBound, now time.Time) (logRetentionStats, error) {
	jobIDs := make([]string, 0, len(perJob))
	for jobID := range perJob {
		jobIDs = append(jobIDs, jobID)
	}
	sort.Strings(jobIDs)
	total := logRetentionStats{}
	for _, jobID := range jobIDs {
		if err := recordLogTruncation(ctx, tx, jobID, bound, *perJob[jobID], now); err != nil {
			return logRetentionStats{}, err
		}
		total.add(*perJob[jobID])
	}
	return total, nil
}

// logEvictionCandidates lists rows oldest-first. With a jobID it walks that
// job in insertion order; without one it walks every job by event timestamp.
func logEvictionCandidates(ctx context.Context, tx *sql.Tx, jobID, extraPredicate string, limit int, args ...any) ([]logEvictionCandidate, error) {
	query := `SELECT e.ordinal, e.job_id, LENGTH(e.bytes) FROM log_events e WHERE `
	queryArgs := []any{}
	order := ` ORDER BY e.timestamp_ns, e.ordinal`
	if jobID != "" {
		query += `e.job_id=?`
		queryArgs = append(queryArgs, jobID)
		order = ` ORDER BY e.ordinal`
	} else {
		query += `1=1`
	}
	query += extraPredicate + order + ` LIMIT ?`
	queryArgs = append(queryArgs, args...)
	queryArgs = append(queryArgs, limit)
	rows, err := tx.QueryContext(ctx, query, queryArgs...)
	if err != nil {
		return nil, internalError(err, "select log eviction candidates")
	}
	defer rows.Close()
	var candidates []logEvictionCandidate
	for rows.Next() {
		var candidate logEvictionCandidate
		if err := rows.Scan(&candidate.ordinal, &candidate.jobID, &candidate.bytes); err != nil {
			return nil, internalError(err, "scan log eviction candidate")
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, internalError(err, "iterate log eviction candidates")
	}
	return candidates, nil
}

func scanJobIDs(rows *sql.Rows, what string) ([]string, error) {
	defer rows.Close()
	var jobIDs []string
	for rows.Next() {
		var jobID string
		if err := rows.Scan(&jobID); err != nil {
			return nil, internalError(err, "scan "+what)
		}
		jobIDs = append(jobIDs, jobID)
	}
	if err := rows.Err(); err != nil {
		return nil, internalError(err, "iterate "+what)
	}
	return jobIDs, nil
}

func deleteLogEvent(ctx context.Context, tx *sql.Tx, candidate logEvictionCandidate, stats *logRetentionStats) error {
	result, err := tx.ExecContext(ctx, "DELETE FROM log_events WHERE ordinal=?", candidate.ordinal)
	if err != nil {
		return internalError(err, "evict retained log event")
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return internalError(err, "read retained log eviction result")
	}
	if changed == 0 {
		return nil
	}
	stats.events += changed
	stats.bytes += candidate.bytes
	if candidate.ordinal > stats.throughOrdinal {
		stats.throughOrdinal = candidate.ordinal
	}
	return nil
}

func recordLogTruncation(ctx context.Context, tx *sql.Tx, jobID string, bound LogRetentionBound, stats logRetentionStats, now time.Time) error {
	if stats.events == 0 {
		return nil
	}
	var earliestRetained sql.NullInt64
	if err := tx.QueryRowContext(ctx, "SELECT MIN(timestamp_ns) FROM log_events WHERE job_id=?", jobID).Scan(&earliestRetained); err != nil {
		return internalError(err, "read earliest retained job log timestamp")
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO job_log_truncations(
		job_id, bound_kind, evicted_event_count, evicted_byte_count,
		evicted_through_ordinal, earliest_retained_ns, updated_ns
	) VALUES(?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(job_id) DO UPDATE SET
		bound_kind=excluded.bound_kind,
		evicted_event_count=job_log_truncations.evicted_event_count+excluded.evicted_event_count,
		evicted_byte_count=job_log_truncations.evicted_byte_count+excluded.evicted_byte_count,
		evicted_through_ordinal=MAX(job_log_truncations.evicted_through_ordinal, excluded.evicted_through_ordinal),
		earliest_retained_ns=excluded.earliest_retained_ns,
		updated_ns=excluded.updated_ns`, jobID, bound, stats.events, stats.bytes, stats.throughOrdinal,
		nullableInt64(earliestRetained), now.UnixNano())
	if err != nil {
		return internalError(err, "record aggregate job log truncation")
	}
	return nil
}

func nullableInt64(value sql.NullInt64) any {
	if !value.Valid {
		return nil
	}
	return value.Int64
}

// pruneServiceAttemptSummaries treats attempt cleanup as a consequence of log
// retention, never as a third log-eviction cause. The current attempt and last
// 32 summaries are a floor, and any older attempt with retained logs remains
// (deleting it would cascade those logs). Whether an attempt can still send
// evidence is decided from its own state, never from whether rows of it are
// retained: retention may have deleted every row of a live or lost attempt,
// and pruning such an attempt would cascade its continuity record and refuse
// its next upload as attempt_not_found. So a live attempt, and a lost one
// whose late-evidence window is still open, are never pruned.
func (s *Store) pruneServiceAttemptSummaries(ctx context.Context, tx *sql.Tx, jobID string, now time.Time) (int64, error) {
	lateEvidenceOpenSince := now.Add(-s.lateEvidenceWindow).UnixNano()
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
		contract.AttemptLost, lateEvidenceOpenSince,
		jobID, jobID, DefaultServiceAttemptSummaries)
	if err != nil {
		return 0, internalError(err, "prune empty service attempt summaries")
	}
	pruned, err := result.RowsAffected()
	if err != nil {
		return 0, internalError(err, "read service attempt pruning result")
	}
	return pruned, nil
}

func readLogTruncation(ctx context.Context, q queryer, jobID string) (*LogTruncation, error) {
	var truncation LogTruncation
	var earliest sql.NullInt64
	var updatedNS int64
	err := q.QueryRowContext(ctx, `SELECT bound_kind, evicted_event_count, evicted_byte_count,
		evicted_through_ordinal, earliest_retained_ns, updated_ns
		FROM job_log_truncations WHERE job_id=?`, jobID).Scan(
		&truncation.BoundKind, &truncation.EvictedEventCount, &truncation.EvictedByteCount,
		&truncation.EvictedThroughOrdinal, &earliest, &updatedNS,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, internalError(err, "read aggregate job log truncation")
	}
	if earliest.Valid {
		value := time.Unix(0, earliest.Int64).UTC()
		truncation.EarliestRetainedAt = &value
	}
	truncation.UpdatedAt = time.Unix(0, updatedNS).UTC()
	return &truncation, nil
}

// migrateServiceLogTruncations carries #49's service-only markers into the
// job-generic job_log_truncations table and drops the old table. The old
// table's foreign key named service_jobs and its CHECK admitted only two
// bounds, and CREATE TABLE IF NOT EXISTS can change neither, so the marker
// moves rather than widening in place. Every row keeps its values.
func (s *Store) migrateServiceLogTruncations(ctx context.Context) error {
	var legacy bool
	if err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='service_log_truncations')`).Scan(&legacy); err != nil {
		return fmt.Errorf("l1: inspect service log truncation schema: %w", err)
	}
	if !legacy {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("l1: begin log truncation migration: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO job_log_truncations(
		job_id, bound_kind, evicted_event_count, evicted_byte_count,
		evicted_through_ordinal, earliest_retained_ns, updated_ns
	) SELECT job_id, bound_kind, evicted_event_count, evicted_byte_count,
		evicted_through_ordinal, earliest_retained_ns, updated_ns
	FROM service_log_truncations WHERE job_id IN (SELECT job_id FROM jobs)`); err != nil {
		return fmt.Errorf("l1: copy service log truncations: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DROP TABLE service_log_truncations`); err != nil {
		return fmt.Errorf("l1: retire service log truncation table: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("l1: commit log truncation migration: %w", err)
	}
	return nil
}

// ensureLogUsageCounters seeds the trigger-maintained retained-byte counters
// from log_events once: on a new database, and on the first open of a
// database that predates them. The singleton total row records that the
// seed happened; the triggers keep both counters exact from then on.
func (s *Store) ensureLogUsageCounters(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("l1: begin log usage seed: %w", err)
	}
	defer tx.Rollback()
	var seeded bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM log_usage_total WHERE singleton=1)`).Scan(&seeded); err != nil {
		return fmt.Errorf("l1: inspect log usage counters: %w", err)
	}
	if seeded {
		return nil
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM job_log_usage;
		INSERT INTO job_log_usage(job_id, retained_bytes)
			SELECT job_id, SUM(LENGTH(bytes)) FROM log_events GROUP BY job_id;
		INSERT INTO log_usage_total(singleton, retained_bytes)
			SELECT 1, COALESCE(SUM(LENGTH(bytes)), 0) FROM log_events;`); err != nil {
		return fmt.Errorf("l1: seed log usage counters: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("l1: commit log usage seed: %w", err)
	}
	return nil
}
