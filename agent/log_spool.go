package agent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	_ "modernc.org/sqlite"
)

// ErrLogSpoolFull means the durable spool cannot accept more one-shot output
// without exceeding the class-scoped unacknowledged-payload budget. The caller
// must stop the producer; one-shot output is never evicted or silently dropped.
var ErrLogSpoolFull = errors.New("agent: log spool retention limit reached")

type logSpoolAttempt struct {
	jobID        string
	attemptID    string
	fencingToken string
	class        string
}

type durableSpoolEvent struct {
	ordinal int64
	event   contract.LogEvent
}

type incompleteEvidenceTombstone struct {
	Kind                  string             `json:"kind"`
	Reason                string             `json:"reason"`
	ErrorCode             contract.ErrorCode `json:"error_code,omitempty"`
	SealedAt              time.Time          `json:"sealed_at"`
	LostEventCount        int64              `json:"lost_event_count"`
	LostByteCount         int64              `json:"lost_byte_count"`
	CompletionUndelivered bool               `json:"completion_undelivered"`
	FinishedAt            *time.Time         `json:"finished_at,omitempty"`
}

type logSpool struct {
	db              *sql.DB
	maxOneShotBytes int64
	maxServiceBytes int64
}

func openLogSpool(directory, nodeID string, maxOneShotBytes int64) (*logSpool, error) {
	return openLogSpoolWithBudgets(directory, nodeID, maxOneShotBytes, DefaultServiceLogSpoolMaxBytes)
}

func openLogSpoolWithBudgets(directory, nodeID string, maxOneShotBytes, maxServiceBytes int64) (*logSpool, error) {
	var err error
	directory, err = resolveLogSpoolDirectory(directory)
	if err != nil {
		return nil, err
	}
	if maxOneShotBytes <= 0 || maxServiceBytes <= 0 {
		return nil, errors.New("agent: log spool class budgets must be positive")
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return nil, fmt.Errorf("agent: create log spool directory: %w", err)
	}
	path := filepath.Join(directory, spoolFileName(nodeID))
	query := make(url.Values)
	query.Add("_pragma", "busy_timeout(5000)")
	query.Add("_pragma", "foreign_keys(1)")
	query.Add("_pragma", "synchronous(FULL)")
	query.Set("_txlock", "immediate")
	dsn := (&url.URL{Scheme: "file", Path: path, RawQuery: query.Encode()}).String()
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("agent: open log spool: %w", err)
	}
	db.SetMaxOpenConns(1)
	spool := &logSpool{db: db, maxOneShotBytes: maxOneShotBytes, maxServiceBytes: maxServiceBytes}
	if err := spool.initialize(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	return spool, nil
}

func resolveLogSpoolDirectory(directory string) (string, error) {
	if strings.TrimSpace(directory) != "" {
		return filepath.Clean(directory), nil
	}
	cacheDirectory, err := os.UserCacheDir()
	if err != nil {
		return "", fmt.Errorf("agent: locate log spool directory: %w", err)
	}
	return filepath.Join(cacheDirectory, "wefty", "log-spool"), nil
}

func spoolFileName(nodeID string) string {
	var builder strings.Builder
	for _, value := range nodeID {
		if builder.Len() >= 48 {
			break
		}
		if value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z' || value >= '0' && value <= '9' || value == '-' || value == '_' {
			builder.WriteRune(value)
		} else {
			builder.WriteByte('_')
		}
	}
	if builder.Len() == 0 {
		builder.WriteString("node")
	}
	digest := sha256.Sum256([]byte(nodeID))
	return builder.String() + "-" + hex.EncodeToString(digest[:8]) + ".sqlite"
}

func (spool *logSpool) initialize(ctx context.Context) error {
	var mode string
	if err := spool.db.QueryRowContext(ctx, "PRAGMA journal_mode=WAL").Scan(&mode); err != nil {
		return fmt.Errorf("agent: enable log spool WAL: %w", err)
	}
	if !strings.EqualFold(mode, "wal") {
		return fmt.Errorf("agent: log spool did not enable WAL (mode %q)", mode)
	}
	const schema = `
CREATE TABLE IF NOT EXISTS spool_attempts (
  attempt_id TEXT PRIMARY KEY,
  job_id TEXT NOT NULL,
  fencing_token TEXT NOT NULL,
  class TEXT NOT NULL,
  created_ns INTEGER NOT NULL,
  result_json BLOB,
  finished_ns INTEGER,
  incomplete_json BLOB,
  CHECK ((result_json IS NULL) = (finished_ns IS NULL))
);
CREATE TABLE IF NOT EXISTS spool_events (
  ordinal INTEGER PRIMARY KEY AUTOINCREMENT,
  attempt_id TEXT NOT NULL REFERENCES spool_attempts(attempt_id) ON DELETE CASCADE,
  stream TEXT NOT NULL,
  sequence INTEGER NOT NULL,
  timestamp_ns INTEGER NOT NULL,
  bytes BLOB NOT NULL,
  gap_json BLOB,
  payload_bytes INTEGER NOT NULL,
  UNIQUE(attempt_id, stream, sequence)
);
CREATE INDEX IF NOT EXISTS spool_events_upload ON spool_events(attempt_id, ordinal);
CREATE TABLE IF NOT EXISTS spool_acknowledgements (
  attempt_id TEXT NOT NULL REFERENCES spool_attempts(attempt_id) ON DELETE CASCADE,
  stream TEXT NOT NULL,
  sequence INTEGER NOT NULL,
  PRIMARY KEY(attempt_id, stream)
);`
	if _, err := spool.db.ExecContext(ctx, schema); err != nil {
		return fmt.Errorf("agent: initialize log spool: %w", err)
	}
	return nil
}

func (spool *logSpool) Close() error { return spool.db.Close() }

func (spool *logSpool) ensureAttempt(ctx context.Context, claim l1.Claim) error {
	_, err := spool.db.ExecContext(ctx, `INSERT INTO spool_attempts(attempt_id, job_id, fencing_token, class, created_ns)
VALUES(?, ?, ?, ?, ?)
ON CONFLICT(attempt_id) DO UPDATE SET job_id=excluded.job_id, fencing_token=excluded.fencing_token, class=excluded.class
WHERE spool_attempts.job_id=excluded.job_id
  AND spool_attempts.fencing_token=excluded.fencing_token
  AND spool_attempts.class=excluded.class`,
		claim.Lease.AttemptID, claim.Job.JobID, claim.Lease.FencingToken, claim.Job.Spec.Class, claim.Job.CreatedAt.UTC().Round(0).UnixNano())
	if err != nil {
		return fmt.Errorf("agent: store log spool attempt: %w", err)
	}
	var jobID, fencingToken, class string
	if err := spool.db.QueryRowContext(ctx, "SELECT job_id, fencing_token, class FROM spool_attempts WHERE attempt_id=?", claim.Lease.AttemptID).Scan(&jobID, &fencingToken, &class); err != nil {
		return fmt.Errorf("agent: verify log spool attempt: %w", err)
	}
	if jobID != claim.Job.JobID || fencingToken != claim.Lease.FencingToken || class != claim.Job.Spec.Class {
		return fmt.Errorf("agent: log spool attempt %q conflicts with stored authority", claim.Lease.AttemptID)
	}
	return nil
}

func (spool *logSpool) append(ctx context.Context, event contract.LogEvent) error {
	if event.Sequence > math.MaxInt64 {
		return fmt.Errorf("agent: log sequence %d exceeds durable spool range", event.Sequence)
	}
	tx, err := spool.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("agent: begin log spool append: %w", err)
	}
	defer tx.Rollback()
	var class string
	if err := tx.QueryRowContext(ctx, "SELECT class FROM spool_attempts WHERE attempt_id=?", event.AttemptID).Scan(&class); err != nil {
		return fmt.Errorf("agent: read log spool attempt class: %w", err)
	}
	storedEvent := event
	if class == contract.JobClassService && storedEvent.Gap == nil && int64(len(storedEvent.Bytes)) > spool.maxServiceBytes {
		storedEvent.Bytes = []byte{}
		storedEvent.Gap = &contract.LogGap{
			ThroughSequence: storedEvent.Sequence,
			LostEventCount:  1,
			LostByteCount:   uint64(len(event.Bytes)),
			Reason:          contract.LogGapOversizedEvent,
		}
	}
	gapJSON, err := json.Marshal(storedEvent.Gap)
	if err != nil {
		return fmt.Errorf("agent: encode log spool gap: %w", err)
	}
	if storedEvent.Gap == nil {
		gapJSON = nil
	}
	var storedTimestamp int64
	var storedBytes, storedGap []byte
	err = tx.QueryRowContext(ctx, `SELECT timestamp_ns, bytes, gap_json FROM spool_events
WHERE attempt_id=? AND stream=? AND sequence=?`, storedEvent.AttemptID, storedEvent.Stream, storedEvent.Sequence).Scan(&storedTimestamp, &storedBytes, &storedGap)
	if err == nil {
		if storedTimestamp != storedEvent.Timestamp.UTC().Round(0).UnixNano() || !bytes.Equal(storedBytes, storedEvent.Bytes) || !bytes.Equal(storedGap, gapJSON) {
			return fmt.Errorf("agent: log event (%s, %d) conflicts with its durable spool record", storedEvent.Stream, storedEvent.Sequence)
		}
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("agent: read log spool event: %w", err)
	}
	var used int64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(SUM(e.payload_bytes), 0)
FROM spool_events e
JOIN spool_attempts a ON a.attempt_id=e.attempt_id
WHERE a.class=?`, class).Scan(&used); err != nil {
		return fmt.Errorf("agent: measure log spool retention: %w", err)
	}
	payloadBytes := int64(len(storedEvent.Bytes))
	switch class {
	case contract.JobClassOneShot:
		if payloadBytes > spool.maxOneShotBytes-used {
			return fmt.Errorf("%w: %d one-shot bytes used, %d-byte event, %d-byte class maximum", ErrLogSpoolFull, used, payloadBytes, spool.maxOneShotBytes)
		}
	case contract.JobClassService:
		if required := used + payloadBytes - spool.maxServiceBytes; required > 0 {
			if err := spool.evictServicePrefix(ctx, tx, required); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("agent: unsupported log spool attempt class %q", class)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO spool_events(attempt_id, stream, sequence, timestamp_ns, bytes, gap_json, payload_bytes)
VALUES(?, ?, ?, ?, ?, ?, ?)`, storedEvent.AttemptID, storedEvent.Stream, storedEvent.Sequence, storedEvent.Timestamp.UTC().Round(0).UnixNano(), storedEvent.Bytes, gapJSON, payloadBytes)
	if err != nil {
		return fmt.Errorf("agent: append durable log event: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("agent: commit durable log event: %w", err)
	}
	return nil
}

type serviceEvictionEvent struct {
	ordinal      int64
	attemptID    string
	stream       contract.LogStream
	sequence     uint64
	payloadBytes uint64
}

func (spool *logSpool) evictServicePrefix(ctx context.Context, tx *sql.Tx, required int64) error {
	rows, err := tx.QueryContext(ctx, `SELECT e.ordinal, e.attempt_id, e.stream, e.sequence, e.payload_bytes
FROM spool_events e
JOIN spool_attempts a ON a.attempt_id=e.attempt_id
WHERE a.class=? AND e.payload_bytes>0
ORDER BY e.ordinal`, contract.JobClassService)
	if err != nil {
		return fmt.Errorf("agent: select service log eviction prefix: %w", err)
	}
	var candidates []serviceEvictionEvent
	var released int64
	for released < required && rows.Next() {
		var candidate serviceEvictionEvent
		var sequence int64
		var payloadBytes int64
		if err := rows.Scan(&candidate.ordinal, &candidate.attemptID, &candidate.stream, &sequence, &payloadBytes); err != nil {
			rows.Close()
			return fmt.Errorf("agent: scan service log eviction prefix: %w", err)
		}
		candidate.sequence = uint64(sequence)
		candidate.payloadBytes = uint64(payloadBytes)
		candidates = append(candidates, candidate)
		released += payloadBytes
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("agent: iterate service log eviction prefix: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("agent: close service log eviction prefix: %w", err)
	}
	if released < required {
		return fmt.Errorf("agent: service log spool cannot release %d bytes from %d bytes of retained payload", required, released)
	}

	type streamKey struct {
		attemptID string
		stream    contract.LogStream
	}
	byStream := make(map[streamKey][]serviceEvictionEvent)
	var keyOrder []streamKey
	for _, candidate := range candidates {
		key := streamKey{attemptID: candidate.attemptID, stream: candidate.stream}
		if _, present := byStream[key]; !present {
			keyOrder = append(keyOrder, key)
		}
		byStream[key] = append(byStream[key], candidate)
	}
	for _, key := range keyOrder {
		events := byStream[key]
		for first := 0; first < len(events); {
			last := first + 1
			for last < len(events) && events[last].sequence == events[last-1].sequence+1 {
				last++
			}
			if err := replaceEvictedServiceRun(ctx, tx, events[first:last]); err != nil {
				return err
			}
			first = last
		}
	}
	return nil
}

func replaceEvictedServiceRun(ctx context.Context, tx *sql.Tx, events []serviceEvictionEvent) error {
	first := events[0]
	last := events[len(events)-1]
	var lostBytes uint64
	for _, event := range events {
		lostBytes += event.payloadBytes
	}
	gapJSON, err := json.Marshal(contract.LogGap{
		ThroughSequence: last.sequence,
		LostEventCount:  uint64(len(events)),
		LostByteCount:   lostBytes,
		Reason:          contract.LogGapSpoolEviction,
	})
	if err != nil {
		return fmt.Errorf("agent: encode service log eviction gap: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE spool_events SET bytes=X'', gap_json=?, payload_bytes=0
WHERE ordinal=? AND attempt_id=? AND stream=?`, gapJSON, first.ordinal, first.attemptID, first.stream); err != nil {
		return fmt.Errorf("agent: store service log eviction gap: %w", err)
	}
	for _, event := range events[1:] {
		if _, err := tx.ExecContext(ctx, "DELETE FROM spool_events WHERE ordinal=? AND attempt_id=?", event.ordinal, event.attemptID); err != nil {
			return fmt.Errorf("agent: release evicted service log payload: %w", err)
		}
	}
	return nil
}

func (spool *logSpool) pendingCount(ctx context.Context, attemptID string) (int, error) {
	var count int
	if err := spool.db.QueryRowContext(ctx, "SELECT count(*) FROM spool_events WHERE attempt_id=?", attemptID).Scan(&count); err != nil {
		return 0, fmt.Errorf("agent: count pending log spool: %w", err)
	}
	return count, nil
}

func (spool *logSpool) pending(ctx context.Context, attemptID string, limit int) ([]contract.LogEvent, error) {
	durable, err := spool.pendingBatch(ctx, attemptID, limit)
	if err != nil {
		return nil, err
	}
	events := make([]contract.LogEvent, 0, len(durable))
	for _, stored := range durable {
		events = append(events, stored.event)
	}
	return events, nil
}

func (spool *logSpool) pendingBatch(ctx context.Context, attemptID string, limit int) ([]durableSpoolEvent, error) {
	rows, err := spool.db.QueryContext(ctx, `SELECT ordinal, stream, sequence, timestamp_ns, bytes, gap_json
FROM spool_events WHERE attempt_id=? ORDER BY ordinal LIMIT ?`, attemptID, limit)
	if err != nil {
		return nil, fmt.Errorf("agent: read pending log spool: %w", err)
	}
	defer rows.Close()
	events := make([]durableSpoolEvent, 0, limit)
	for rows.Next() {
		var stored durableSpoolEvent
		var event contract.LogEvent
		var sequence, timestamp int64
		var gapJSON []byte
		if err := rows.Scan(&stored.ordinal, &event.Stream, &sequence, &timestamp, &event.Bytes, &gapJSON); err != nil {
			return nil, fmt.Errorf("agent: scan pending log spool: %w", err)
		}
		if len(gapJSON) != 0 {
			var gap contract.LogGap
			if err := json.Unmarshal(gapJSON, &gap); err != nil {
				return nil, fmt.Errorf("agent: decode pending log gap: %w", err)
			}
			event.Gap = &gap
		}
		event.AttemptID = attemptID
		event.Sequence = uint64(sequence)
		event.Timestamp = time.Unix(0, timestamp).UTC()
		stored.event = event
		events = append(events, stored)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("agent: iterate pending log spool: %w", err)
	}
	return events, nil
}

func (spool *logSpool) acknowledge(ctx context.Context, attemptID string, acknowledged map[contract.LogStream]uint64) error {
	tx, err := spool.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("agent: begin log spool acknowledgement: %w", err)
	}
	defer tx.Rollback()
	for stream, sequence := range acknowledged {
		if sequence > math.MaxInt64 {
			return fmt.Errorf("agent: log acknowledgement %d exceeds durable spool range", sequence)
		}
		_, err := tx.ExecContext(ctx, `INSERT INTO spool_acknowledgements(attempt_id, stream, sequence)
VALUES(?, ?, ?)
ON CONFLICT(attempt_id, stream) DO UPDATE SET sequence=MAX(sequence, excluded.sequence)`, attemptID, stream, sequence)
		if err != nil {
			return fmt.Errorf("agent: persist log acknowledgement: %w", err)
		}
		if _, err := tx.ExecContext(ctx, "DELETE FROM spool_events WHERE attempt_id=? AND stream=? AND sequence<=?", attemptID, stream, sequence); err != nil {
			return fmt.Errorf("agent: release acknowledged log retention: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("agent: commit log spool acknowledgement: %w", err)
	}
	return nil
}

func (spool *logSpool) pendingAttempts(ctx context.Context) ([]logSpoolAttempt, error) {
	rows, err := spool.db.QueryContext(ctx, `SELECT a.job_id, a.attempt_id, a.fencing_token, a.class
FROM spool_attempts a
WHERE a.incomplete_json IS NULL
  AND (a.result_json IS NOT NULL OR EXISTS (SELECT 1 FROM spool_events e WHERE e.attempt_id=a.attempt_id))
ORDER BY a.created_ns, a.attempt_id`)
	if err != nil {
		return nil, fmt.Errorf("agent: list pending log spool attempts: %w", err)
	}
	defer rows.Close()
	var attempts []logSpoolAttempt
	for rows.Next() {
		var attempt logSpoolAttempt
		if err := rows.Scan(&attempt.jobID, &attempt.attemptID, &attempt.fencingToken, &attempt.class); err != nil {
			return nil, fmt.Errorf("agent: scan pending log spool attempt: %w", err)
		}
		attempts = append(attempts, attempt)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("agent: iterate pending log spool attempts: %w", err)
	}
	return attempts, nil
}

func (spool *logSpool) storeCompletion(ctx context.Context, attemptID string, result l1.ProcessResult, finishedAt time.Time) error {
	resultJSON, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("agent: encode durable completion: %w", err)
	}
	storedAt := finishedAt.UTC().Round(0).UnixNano()
	response, err := spool.db.ExecContext(ctx, `UPDATE spool_attempts
SET result_json=COALESCE(result_json, ?), finished_ns=COALESCE(finished_ns, ?)
WHERE attempt_id=? AND (result_json IS NULL OR result_json=?) AND incomplete_json IS NULL`, resultJSON, storedAt, attemptID, resultJSON)
	if err != nil {
		return fmt.Errorf("agent: persist durable completion: %w", err)
	}
	changed, err := response.RowsAffected()
	if err != nil {
		return fmt.Errorf("agent: inspect durable completion persistence: %w", err)
	}
	if changed != 1 {
		return fmt.Errorf("agent: completion for attempt %q conflicts with durable evidence", attemptID)
	}
	return nil
}

func (spool *logSpool) completion(ctx context.Context, attemptID string) (l1.ProcessResult, time.Time, bool, error) {
	var resultJSON []byte
	var finishedNS int64
	err := spool.db.QueryRowContext(ctx, `SELECT result_json, finished_ns FROM spool_attempts
WHERE attempt_id=? AND result_json IS NOT NULL AND incomplete_json IS NULL`, attemptID).Scan(&resultJSON, &finishedNS)
	if errors.Is(err, sql.ErrNoRows) {
		return l1.ProcessResult{}, time.Time{}, false, nil
	}
	if err != nil {
		return l1.ProcessResult{}, time.Time{}, false, fmt.Errorf("agent: read durable completion: %w", err)
	}
	var result l1.ProcessResult
	if err := json.Unmarshal(resultJSON, &result); err != nil {
		return l1.ProcessResult{}, time.Time{}, false, fmt.Errorf("agent: decode durable completion: %w", err)
	}
	return result, time.Unix(0, finishedNS).UTC(), true, nil
}

func (spool *logSpool) completionDelivered(ctx context.Context, attemptID string) error {
	tx, err := spool.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("agent: begin durable completion acknowledgement: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `UPDATE spool_attempts SET result_json=NULL, finished_ns=NULL WHERE attempt_id=?`, attemptID); err != nil {
		return fmt.Errorf("agent: release delivered completion: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM spool_attempts
WHERE attempt_id=? AND result_json IS NULL AND incomplete_json IS NULL
  AND NOT EXISTS (SELECT 1 FROM spool_events WHERE attempt_id=?)`, attemptID, attemptID); err != nil {
		return fmt.Errorf("agent: clean delivered spool attempt: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("agent: commit durable completion acknowledgement: %w", err)
	}
	return nil
}

func (spool *logSpool) replaceBatchWithReplayGaps(ctx context.Context, attemptID string, batch []durableSpoolEvent) error {
	byStream := make(map[contract.LogStream][]durableSpoolEvent, 2)
	for _, stored := range batch {
		if stored.event.Gap != nil {
			return errors.New("agent: rejected replay gap cannot be replaced by another gap")
		}
		byStream[stored.event.Stream] = append(byStream[stored.event.Stream], stored)
	}
	tx, err := spool.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("agent: begin rejected replay replacement: %w", err)
	}
	defer tx.Rollback()
	for stream, events := range byStream {
		first := events[0]
		last := events[len(events)-1]
		var lostBytes uint64
		ordinals := make([]int64, 0, len(events)-1)
		for index, stored := range events {
			if stored.event.Sequence != first.event.Sequence+uint64(index) {
				return fmt.Errorf("agent: rejected replay for %s is not a contiguous sequence", stream)
			}
			lostBytes += uint64(len(stored.event.Bytes))
			if index > 0 {
				ordinals = append(ordinals, stored.ordinal)
			}
		}
		gap := contract.LogGap{
			ThroughSequence: last.event.Sequence,
			LostEventCount:  last.event.Sequence - first.event.Sequence + 1,
			LostByteCount:   lostBytes,
			Reason:          contract.LogGapReplayRejected,
		}
		gapJSON, err := json.Marshal(gap)
		if err != nil {
			return fmt.Errorf("agent: encode rejected replay gap: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE spool_events
SET bytes=X'', gap_json=?, payload_bytes=0 WHERE ordinal=? AND attempt_id=? AND stream=?`, gapJSON, first.ordinal, attemptID, stream); err != nil {
			return fmt.Errorf("agent: store rejected replay gap: %w", err)
		}
		for _, ordinal := range ordinals {
			if _, err := tx.ExecContext(ctx, "DELETE FROM spool_events WHERE ordinal=? AND attempt_id=?", ordinal, attemptID); err != nil {
				return fmt.Errorf("agent: release rejected replay payload: %w", err)
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("agent: commit rejected replay replacement: %w", err)
	}
	return nil
}

func (spool *logSpool) sealIncomplete(ctx context.Context, attemptID, reason string, code contract.ErrorCode, sealedAt time.Time) error {
	tx, err := spool.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("agent: begin incomplete evidence seal: %w", err)
	}
	defer tx.Rollback()
	var lostEvents, lostBytes int64
	rows, err := tx.QueryContext(ctx, `SELECT payload_bytes, gap_json FROM spool_events WHERE attempt_id=?`, attemptID)
	if err != nil {
		return fmt.Errorf("agent: read incomplete log evidence: %w", err)
	}
	for rows.Next() {
		var payloadBytes int64
		var gapJSON []byte
		if err := rows.Scan(&payloadBytes, &gapJSON); err != nil {
			rows.Close()
			return fmt.Errorf("agent: scan incomplete log evidence: %w", err)
		}
		if len(gapJSON) == 0 {
			lostEvents++
			lostBytes += payloadBytes
			continue
		}
		var gap contract.LogGap
		if err := json.Unmarshal(gapJSON, &gap); err != nil {
			rows.Close()
			return fmt.Errorf("agent: decode incomplete log gap: %w", err)
		}
		lostEvents += int64(gap.LostEventCount)
		lostBytes += int64(gap.LostByteCount)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("agent: iterate incomplete log evidence: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("agent: close incomplete log evidence: %w", err)
	}
	var resultJSON []byte
	var finishedNS sql.NullInt64
	err = tx.QueryRowContext(ctx, "SELECT result_json, finished_ns FROM spool_attempts WHERE attempt_id=?", attemptID).Scan(&resultJSON, &finishedNS)
	if err != nil {
		return fmt.Errorf("agent: read incomplete completion evidence: %w", err)
	}
	var finishedAt *time.Time
	if finishedNS.Valid {
		value := time.Unix(0, finishedNS.Int64).UTC()
		finishedAt = &value
	}
	tombstone := incompleteEvidenceTombstone{
		Kind: "incomplete", Reason: reason, ErrorCode: code, SealedAt: sealedAt.UTC().Round(0),
		LostEventCount: lostEvents, LostByteCount: lostBytes,
		CompletionUndelivered: len(resultJSON) != 0, FinishedAt: finishedAt,
	}
	tombstoneJSON, err := json.Marshal(tombstone)
	if err != nil {
		return fmt.Errorf("agent: encode incomplete evidence tombstone: %w", err)
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM spool_events WHERE attempt_id=?", attemptID); err != nil {
		return fmt.Errorf("agent: release incomplete log payload: %w", err)
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM spool_acknowledgements WHERE attempt_id=?", attemptID); err != nil {
		return fmt.Errorf("agent: release incomplete log acknowledgements: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE spool_attempts
SET result_json=NULL, finished_ns=NULL, incomplete_json=? WHERE attempt_id=?`, tombstoneJSON, attemptID); err != nil {
		return fmt.Errorf("agent: persist incomplete evidence tombstone: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("agent: commit incomplete evidence tombstone: %w", err)
	}
	return nil
}

func (spool *logSpool) highWater(ctx context.Context, attemptID string, stream contract.LogStream) (uint64, bool, error) {
	var sequence int64
	err := spool.db.QueryRowContext(ctx, `SELECT sequence FROM spool_acknowledgements
WHERE attempt_id=? AND stream=?`, attemptID, stream).Scan(&sequence)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("agent: read log acknowledgement high-water mark: %w", err)
	}
	return uint64(sequence), true, nil
}
