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
	// logRetentionSweepEventBudget bounds the cross-job evictions (one-shot
	// age plus the total ceiling) in one reconcile pass, so a large backlog
	// is worked off across passes instead of stalling the transaction every
	// other L1 transition waits behind.
	logRetentionSweepEventBudget = 4096
	// oneshotByteSweepJobBudget bounds how many over-cap one-shots one pass
	// re-trims. Ingest is the mandatory byte site; this only catches a job
	// left over a lowered cap after restart.
	oneshotByteSweepJobBudget = 16
)

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

// liveWatermarkExclusion keeps the newest row for every stream of a
// non-terminal attempt: it is the provenance watermark continuity is checked
// against, so no retention bound may evict it. Sequences only grow within a
// stream, so the newest row is the one with the highest sequence.
const liveWatermarkExclusion = ` AND NOT EXISTS (
		SELECT 1 FROM attempts live
		WHERE live.attempt_id=e.attempt_id AND live.state IN (?, ?, ?)
			AND e.sequence=(SELECT MAX(w.sequence) FROM log_events w WHERE w.attempt_id=e.attempt_id AND w.stream=e.stream)
	)`

var liveWatermarkStates = []any{contract.AttemptClaimed, contract.AttemptRunning, contract.AttemptAwaitingInput}

func jobIsService(ctx context.Context, q queryer, jobID string) (bool, error) {
	var service bool
	if err := q.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM service_jobs WHERE job_id=?)", jobID).Scan(&service); err != nil {
		return false, internalError(err, "read log retention class")
	}
	return service, nil
}

// enforceJobLogByteRetention applies the per-job byte cap of the job's class.
// It runs inside the same immediate transaction that accepts a batch, and the
// sweep applies it again so a lowered cap binds an idle job after restart.
func (s *Store) enforceJobLogByteRetention(ctx context.Context, tx *sql.Tx, jobID string, now time.Time) (logRetentionStats, error) {
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

	stats := logRetentionStats{}
	for retainedBytes > limit {
		candidates, err := logEvictionCandidates(ctx, tx, jobID, "", logEvictionBatch)
		if err != nil {
			return logRetentionStats{}, err
		}
		before := stats.events
		for _, candidate := range candidates {
			if retainedBytes <= limit {
				break
			}
			if err := deleteLogEvent(ctx, tx, candidate, &stats); err != nil {
				return logRetentionStats{}, err
			}
			retainedBytes -= candidate.bytes
		}
		if len(candidates) < logEvictionBatch || stats.events == before {
			break
		}
	}
	if err := recordLogTruncation(ctx, tx, jobID, LogRetentionBytes, stats, now); err != nil {
		return logRetentionStats{}, err
	}
	return stats, nil
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
func (s *Store) enforceServiceLogAgeRetention(ctx context.Context, tx *sql.Tx, jobID string, now time.Time) (logRetentionStats, error) {
	cutoff := now.Add(-s.serviceLogRetentionAge).UnixNano()
	stats := logRetentionStats{}
	for {
		candidates, err := logEvictionCandidates(ctx, tx, jobID, " AND e.timestamp_ns < ?", logEvictionBatch, cutoff)
		if err != nil {
			return logRetentionStats{}, err
		}
		before := stats.events
		for _, candidate := range candidates {
			if err := deleteLogEvent(ctx, tx, candidate, &stats); err != nil {
				return logRetentionStats{}, err
			}
		}
		if len(candidates) < logEvictionBatch || stats.events == before {
			break
		}
	}
	if err := recordLogTruncation(ctx, tx, jobID, LogRetentionAge, stats, now); err != nil {
		return logRetentionStats{}, err
	}
	return stats, nil
}

// enforceOneshotLogRetention is the sweep's one-shot half: a bounded re-trim
// of one-shots over their per-job cap, then age eviction oldest-first by each
// event's own timestamp across every one-shot, within budget events.
func (s *Store) enforceOneshotLogRetention(ctx context.Context, tx *sql.Tx, now time.Time, budget int) (logRetentionStats, int, error) {
	total := logRetentionStats{}
	rows, err := tx.QueryContext(ctx, `SELECT u.job_id FROM job_log_usage u
		WHERE u.retained_bytes > ?
			AND NOT EXISTS (SELECT 1 FROM service_jobs sj WHERE sj.job_id=u.job_id)
		ORDER BY u.retained_bytes DESC, u.job_id LIMIT ?`, s.logRetention.oneshotBytes, oneshotByteSweepJobBudget)
	if err != nil {
		return logRetentionStats{}, budget, internalError(err, "select one-shots over their log byte cap")
	}
	overCap, err := scanJobIDs(rows, "one-shot over its log byte cap")
	if err != nil {
		return logRetentionStats{}, budget, err
	}
	for _, jobID := range overCap {
		stats, err := s.enforceJobLogByteRetention(ctx, tx, jobID, now)
		if err != nil {
			return logRetentionStats{}, budget, err
		}
		total.add(stats)
	}

	cutoff := now.Add(-s.logRetention.oneshotAge).UnixNano()
	perJob := map[string]*logRetentionStats{}
	for budget > 0 {
		candidates, err := logEvictionCandidates(ctx, tx, "", ` AND e.timestamp_ns < ?
			AND NOT EXISTS (SELECT 1 FROM service_jobs sj WHERE sj.job_id=e.job_id)`, min(budget, logEvictionBatch), cutoff)
		if err != nil {
			return logRetentionStats{}, budget, err
		}
		evicted, err := deleteLogEventsForJobs(ctx, tx, candidates, perJob, -1)
		if err != nil {
			return logRetentionStats{}, budget, err
		}
		budget -= evicted
		if len(candidates) < logEvictionBatch || evicted == 0 {
			break
		}
	}
	stats, err := recordLogTruncations(ctx, tx, perJob, LogRetentionAge, now)
	if err != nil {
		return logRetentionStats{}, budget, err
	}
	total.add(stats)
	return total, budget, nil
}

// enforceLogRetentionTotal applies the cluster-wide ceiling to every job's
// logs, one-shot and service alike: while the maintained total exceeds it,
// the oldest events by their own timestamp go first, within budget events.
func (s *Store) enforceLogRetentionTotal(ctx context.Context, tx *sql.Tx, now time.Time, budget int) (logRetentionStats, error) {
	var retained int64
	err := tx.QueryRowContext(ctx, "SELECT retained_bytes FROM log_usage_total WHERE singleton=1").Scan(&retained)
	if err != nil {
		return logRetentionStats{}, internalError(err, "measure total retained log bytes")
	}
	excess := retained - s.logRetention.totalBytes
	perJob := map[string]*logRetentionStats{}
	for excess > 0 && budget > 0 {
		candidates, err := logEvictionCandidates(ctx, tx, "", "", min(budget, logEvictionBatch))
		if err != nil {
			return logRetentionStats{}, err
		}
		before := sumEvictedBytes(perJob)
		evicted, err := deleteLogEventsForJobs(ctx, tx, candidates, perJob, excess)
		if err != nil {
			return logRetentionStats{}, err
		}
		budget -= evicted
		excess -= sumEvictedBytes(perJob) - before
		if len(candidates) < logEvictionBatch || evicted == 0 {
			break
		}
	}
	return recordLogTruncations(ctx, tx, perJob, LogRetentionTotal, now)
}

func sumEvictedBytes(perJob map[string]*logRetentionStats) int64 {
	var total int64
	for _, stats := range perJob {
		total += stats.bytes
	}
	return total
}

// deleteLogEventsForJobs evicts candidates in order, attributing each to its
// job, and stops once byteTarget bytes are gone (a negative target means all).
func deleteLogEventsForJobs(ctx context.Context, tx *sql.Tx, candidates []logEvictionCandidate, perJob map[string]*logRetentionStats, byteTarget int64) (int, error) {
	evicted := 0
	var freed int64
	for _, candidate := range candidates {
		if byteTarget >= 0 && freed >= byteTarget {
			break
		}
		stats := perJob[candidate.jobID]
		if stats == nil {
			stats = &logRetentionStats{}
			perJob[candidate.jobID] = stats
		}
		before := stats.events
		if err := deleteLogEvent(ctx, tx, candidate, stats); err != nil {
			return evicted, err
		}
		if stats.events > before {
			evicted++
			freed += candidate.bytes
		}
	}
	return evicted, nil
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

// logEvictionCandidates lists evictable rows oldest-first, never a live
// attempt's per-stream watermark. With a jobID it walks that job in insertion
// order; without one it walks every job by event timestamp.
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
	query += extraPredicate + liveWatermarkExclusion + order + ` LIMIT ?`
	queryArgs = append(queryArgs, args...)
	queryArgs = append(queryArgs, liveWatermarkStates...)
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
// 32 summaries are a floor, and any older attempt with retained logs remains.
func pruneServiceAttemptSummaries(ctx context.Context, tx *sql.Tx, jobID string) (int64, error) {
	result, err := tx.ExecContext(ctx, `DELETE FROM attempts
		WHERE job_id=?
			AND EXISTS (SELECT 1 FROM service_jobs WHERE service_jobs.job_id=attempts.job_id)
			AND attempt_id<>COALESCE((SELECT current_attempt_id FROM jobs WHERE job_id=?), '')
			AND NOT EXISTS (SELECT 1 FROM log_events WHERE log_events.attempt_id=attempts.attempt_id)
			AND attempt_id NOT IN (
				SELECT attempt_id FROM attempts recent
				WHERE recent.job_id=?
					AND recent.attempt_id<>COALESCE((SELECT current_attempt_id FROM jobs WHERE job_id=?), '')
				ORDER BY recent.created_ns DESC, recent.attempt_id DESC
				LIMIT ?
			)`, jobID, jobID, jobID, jobID, DefaultServiceAttemptSummaries)
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
