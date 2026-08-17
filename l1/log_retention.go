package l1

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

type logRetentionStats struct {
	events         int64
	bytes          int64
	throughOrdinal int64
}

type logEvictionCandidate struct {
	ordinal int64
	bytes   int64
}

// enforceServiceLogByteRetention runs inside the same immediate transaction
// that accepts a batch. The newest row for every stream of a non-terminal
// attempt is a provenance watermark and is deliberately not evictable.
func (s *Store) enforceServiceLogByteRetention(ctx context.Context, tx *sql.Tx, jobID string, now time.Time) (logRetentionStats, error) {
	var service bool
	if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM service_jobs WHERE job_id=?)", jobID).Scan(&service); err != nil {
		return logRetentionStats{}, internalError(err, "read service log retention applicability")
	}
	if !service {
		return logRetentionStats{}, nil
	}

	var retainedBytes int64
	if err := tx.QueryRowContext(ctx, "SELECT COALESCE(SUM(LENGTH(bytes)), 0) FROM log_events WHERE job_id=?", jobID).Scan(&retainedBytes); err != nil {
		return logRetentionStats{}, internalError(err, "measure retained service log bytes")
	}
	if retainedBytes <= s.serviceLogRetentionBytes {
		if err := refreshServiceLogTruncation(ctx, tx, jobID, now); err != nil {
			return logRetentionStats{}, err
		}
		return logRetentionStats{}, nil
	}

	candidates, err := serviceLogEvictionCandidates(ctx, tx, jobID, "")
	if err != nil {
		return logRetentionStats{}, err
	}
	stats := logRetentionStats{}
	for _, candidate := range candidates {
		if retainedBytes <= s.serviceLogRetentionBytes {
			break
		}
		if err := deleteServiceLogEvent(ctx, tx, candidate, &stats); err != nil {
			return logRetentionStats{}, err
		}
		retainedBytes -= candidate.bytes
	}
	if err := recordServiceLogTruncation(ctx, tx, jobID, ServiceLogRetentionBytes, stats, now); err != nil {
		return logRetentionStats{}, err
	}
	return stats, nil
}

func refreshServiceLogTruncation(ctx context.Context, tx *sql.Tx, jobID string, now time.Time) error {
	_, err := tx.ExecContext(ctx, `UPDATE service_log_truncations
		SET earliest_retained_ns=(SELECT MIN(timestamp_ns) FROM log_events WHERE job_id=?), updated_ns=?
		WHERE job_id=? AND earliest_retained_ns IS NOT (SELECT MIN(timestamp_ns) FROM log_events WHERE job_id=?)`,
		jobID, now.UnixNano(), jobID, jobID)
	if err != nil {
		return internalError(err, "refresh earliest retained service log timestamp")
	}
	return nil
}

func (s *Store) enforceServiceLogAgeRetention(ctx context.Context, tx *sql.Tx, jobID string, now time.Time) (logRetentionStats, error) {
	cutoff := now.Add(-s.serviceLogRetentionAge).UnixNano()
	candidates, err := serviceLogEvictionCandidates(ctx, tx, jobID, " AND e.timestamp_ns < ?", cutoff)
	if err != nil {
		return logRetentionStats{}, err
	}
	stats := logRetentionStats{}
	for _, candidate := range candidates {
		if err := deleteServiceLogEvent(ctx, tx, candidate, &stats); err != nil {
			return logRetentionStats{}, err
		}
	}
	if err := recordServiceLogTruncation(ctx, tx, jobID, ServiceLogRetentionAge, stats, now); err != nil {
		return logRetentionStats{}, err
	}
	return stats, nil
}

func serviceLogEvictionCandidates(ctx context.Context, tx *sql.Tx, jobID, extraPredicate string, args ...any) ([]logEvictionCandidate, error) {
	query := `WITH watermarks AS (
			SELECT watermark.attempt_id, watermark.stream, MAX(watermark.ordinal) AS ordinal
			FROM log_events watermark
			JOIN attempts watermark_attempt ON watermark_attempt.attempt_id=watermark.attempt_id
			WHERE watermark.job_id=? AND watermark_attempt.state IN (?, ?, ?)
			GROUP BY watermark.attempt_id, watermark.stream
		)
		SELECT e.ordinal, LENGTH(e.bytes)
		FROM log_events e
		LEFT JOIN watermarks ON watermarks.ordinal=e.ordinal
		WHERE e.job_id=?` + extraPredicate + ` AND watermarks.ordinal IS NULL
		ORDER BY e.ordinal`
	queryArgs := []any{
		jobID, contract.AttemptClaimed, contract.AttemptRunning, contract.AttemptAwaitingInput, jobID,
	}
	queryArgs = append(queryArgs, args...)
	rows, err := tx.QueryContext(ctx, query, queryArgs...)
	if err != nil {
		return nil, internalError(err, "select service log eviction candidates")
	}
	defer rows.Close()
	var candidates []logEvictionCandidate
	for rows.Next() {
		var candidate logEvictionCandidate
		if err := rows.Scan(&candidate.ordinal, &candidate.bytes); err != nil {
			return nil, internalError(err, "scan service log eviction candidate")
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, internalError(err, "iterate service log eviction candidates")
	}
	return candidates, nil
}

func deleteServiceLogEvent(ctx context.Context, tx *sql.Tx, candidate logEvictionCandidate, stats *logRetentionStats) error {
	result, err := tx.ExecContext(ctx, "DELETE FROM log_events WHERE ordinal=?", candidate.ordinal)
	if err != nil {
		return internalError(err, "evict retained service log event")
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return internalError(err, "read retained service log eviction result")
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

func recordServiceLogTruncation(ctx context.Context, tx *sql.Tx, jobID string, bound ServiceLogRetentionBound, stats logRetentionStats, now time.Time) error {
	if stats.events == 0 {
		return nil
	}
	var earliestRetained sql.NullInt64
	if err := tx.QueryRowContext(ctx, "SELECT MIN(timestamp_ns) FROM log_events WHERE job_id=?", jobID).Scan(&earliestRetained); err != nil {
		return internalError(err, "read earliest retained service log timestamp")
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO service_log_truncations(
		job_id, bound_kind, evicted_event_count, evicted_byte_count,
		evicted_through_ordinal, earliest_retained_ns, updated_ns
	) VALUES(?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(job_id) DO UPDATE SET
		bound_kind=excluded.bound_kind,
		evicted_event_count=service_log_truncations.evicted_event_count+excluded.evicted_event_count,
		evicted_byte_count=service_log_truncations.evicted_byte_count+excluded.evicted_byte_count,
		evicted_through_ordinal=MAX(service_log_truncations.evicted_through_ordinal, excluded.evicted_through_ordinal),
		earliest_retained_ns=excluded.earliest_retained_ns,
		updated_ns=excluded.updated_ns`, jobID, bound, stats.events, stats.bytes, stats.throughOrdinal,
		nullableInt64(earliestRetained), now.UnixNano())
	if err != nil {
		return internalError(err, "record aggregate service log truncation")
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

func readServiceLogTruncation(ctx context.Context, q queryer, jobID string) (*ServiceLogTruncation, error) {
	var truncation ServiceLogTruncation
	var earliest sql.NullInt64
	var updatedNS int64
	err := q.QueryRowContext(ctx, `SELECT bound_kind, evicted_event_count, evicted_byte_count,
		evicted_through_ordinal, earliest_retained_ns, updated_ns
		FROM service_log_truncations WHERE job_id=?`, jobID).Scan(
		&truncation.BoundKind, &truncation.EvictedEventCount, &truncation.EvictedByteCount,
		&truncation.EvictedThroughOrdinal, &earliest, &updatedNS,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, internalError(err, "read aggregate service log truncation")
	}
	if earliest.Valid {
		value := time.Unix(0, earliest.Int64).UTC()
		truncation.EarliestRetainedAt = &value
	}
	truncation.UpdatedAt = time.Unix(0, updatedNS).UTC()
	return &truncation, nil
}
