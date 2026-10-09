package l1

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/Derek-X-Wang/wefty/contract"
)

// Upload continuity and replay idempotency are durable facts of their own,
// kept in log_stream_continuity: the highest sequence L1 has accepted for
// each (attempt, stream). They never derive from the retained log_events
// rows, because retention may delete any row, including in the very
// transaction that accepted it. Before #52 an evicted newest row reset a
// stream's expected sequence to 0, so an identical retry of an accepted
// batch, or the next upload of a lost attempt inside its late-evidence
// window, was refused as a conflict.

// readLogContinuity returns the highest accepted sequence for the stream,
// or -1 when L1 has accepted nothing on it.
func readLogContinuity(ctx context.Context, q queryer, attemptID string, stream contract.LogStream) (int64, error) {
	var through int64
	err := q.QueryRowContext(ctx, `SELECT accepted_through FROM log_stream_continuity
		WHERE attempt_id=? AND stream=?`, attemptID, stream).Scan(&through)
	if errors.Is(err, sql.ErrNoRows) {
		return -1, nil
	}
	if err != nil {
		return 0, internalError(err, "read log stream continuity")
	}
	return through, nil
}

func recordLogContinuity(ctx context.Context, tx *sql.Tx, attemptID string, stream contract.LogStream, through uint64) error {
	if _, err := tx.ExecContext(ctx, `INSERT INTO log_stream_continuity(attempt_id, stream, accepted_through)
		VALUES(?, ?, ?)
		ON CONFLICT(attempt_id, stream) DO UPDATE SET
			accepted_through=MAX(log_stream_continuity.accepted_through, excluded.accepted_through)`,
		attemptID, stream, int64(through)); err != nil {
		return internalError(err, "record log stream continuity")
	}
	return nil
}

const logContinuitySeedMigration = "log_stream_continuity_seed"

// seedLogContinuity derives the record for a database that predates it from
// the rows still retained: the highest accepted sequence per attempt stream.
// The seed rows and its l1_data_migrations marker commit together, so a crash
// between creating the table and seeding it seeds on the next open. A new
// database seeds nothing and records the marker. #49 never evicted a live
// attempt's newest row per stream, so every stream that can still grow is
// seeded exactly. A stream whose rows were all evicted before the upgrade can
// only belong to an attempt that was no longer live; it gets no record and
// behaves as it did before the upgrade. INSERT OR IGNORE never lowers a record
// an append already wrote.
func (s *Store) seedLogContinuity(ctx context.Context) error {
	write, err := s.beginWriteTransaction(ctx, nil)
	if err != nil {
		return fmt.Errorf("l1: begin log stream continuity seed: %w", err)
	}
	tx := write.tx
	defer write.rollback()
	var seeded bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM l1_data_migrations WHERE name=?)`,
		logContinuitySeedMigration).Scan(&seeded); err != nil {
		return fmt.Errorf("l1: inspect log stream continuity seed: %w", err)
	}
	if seeded {
		return nil
	}
	if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO log_stream_continuity(attempt_id, stream, accepted_through)
		SELECT attempt_id, stream, MAX(sequence_end) FROM log_events GROUP BY attempt_id, stream`); err != nil {
		return fmt.Errorf("l1: seed log stream continuity: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO l1_data_migrations(name, applied_ns) VALUES(?, ?)`,
		logContinuitySeedMigration, s.clock.Now().UnixNano()); err != nil {
		return fmt.Errorf("l1: record log stream continuity seed: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("l1: commit log stream continuity seed: %w", err)
	}
	return nil
}

// checkRetainedLogReplay decides a replay of an accepted range
// [event.Sequence, end] against every retained row that intersects it.
// Accepted rows never overlap, so those are the rows starting inside the
// range plus at most the one row starting before it. The rule:
//   - no retained row intersects: the accepted rows were evicted, and the
//     continuity record alone acknowledges the replay;
//   - exactly one intersects, it has exactly this range, and its content is
//     this event: an idempotent replay;
//   - anything else (a partial overlap, several rows, other content) is not
//     the event L1 accepted: idempotency_conflict.
func checkRetainedLogReplay(ctx context.Context, tx *sql.Tx, attemptID string, event contract.LogEvent, end uint64, originalRaw, raw []byte) error {
	conflict := protocolError(contract.ErrorIdempotencyConflict, "log event (%s, %d..%d) conflicts with the accepted event", event.Stream, event.Sequence, end)
	var previousEnd int64
	err := tx.QueryRowContext(ctx, `SELECT sequence_end FROM log_events
		WHERE attempt_id=? AND stream=? AND sequence<? ORDER BY sequence DESC LIMIT 1`,
		attemptID, event.Stream, event.Sequence).Scan(&previousEnd)
	switch {
	case errors.Is(err, sql.ErrNoRows):
	case err != nil:
		return internalError(err, "read retained log row before a replay")
	case previousEnd >= int64(event.Sequence):
		return conflict
	}
	rows, err := tx.QueryContext(ctx, `SELECT sequence, sequence_end, event_json, bytes FROM log_events
		WHERE attempt_id=? AND stream=? AND sequence>=? AND sequence<=? ORDER BY sequence LIMIT 2`,
		attemptID, event.Stream, event.Sequence, end)
	if err != nil {
		return internalError(err, "read retained log rows inside a replay")
	}
	defer rows.Close()
	intersecting := 0
	matched := false
	for rows.Next() {
		var start, stop int64
		var stored, payload []byte
		if err := rows.Scan(&start, &stop, &stored, &payload); err != nil {
			return internalError(err, "scan retained log row inside a replay")
		}
		intersecting++
		document := logEventDocument(stored, payload)
		matched = start == int64(event.Sequence) && stop == int64(end) &&
			(bytes.Equal(document, originalRaw) || bytes.Equal(document, raw))
	}
	if err := rows.Err(); err != nil {
		return internalError(err, "iterate retained log rows inside a replay")
	}
	if intersecting == 0 || (intersecting == 1 && matched) {
		return nil
	}
	return conflict
}
