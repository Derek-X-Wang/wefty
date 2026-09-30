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

// logContinuityTableExists is read before the schema is applied, so the seed
// runs exactly once: on the open that creates the table.
func (s *Store) logContinuityTableExists(ctx context.Context) (bool, error) {
	var exists bool
	if err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM sqlite_master
		WHERE type='table' AND name='log_stream_continuity')`).Scan(&exists); err != nil {
		return false, fmt.Errorf("l1: inspect log stream continuity schema: %w", err)
	}
	return exists, nil
}

// seedLogContinuity derives the record for a database that predates it from
// the rows still retained: the highest accepted sequence per attempt stream.
// #49 never evicted a live attempt's newest row per stream, so every stream
// that can still grow is seeded exactly. A stream whose rows were all evicted
// before the upgrade can only belong to an attempt that was no longer live;
// it gets no record and behaves as it did before the upgrade.
func (s *Store) seedLogContinuity(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO log_stream_continuity(attempt_id, stream, accepted_through)
		SELECT attempt_id, stream, MAX(sequence_end) FROM log_events GROUP BY attempt_id, stream`); err != nil {
		return fmt.Errorf("l1: seed log stream continuity: %w", err)
	}
	return nil
}

// checkRetainedLogReplay compares a replayed event with the accepted row it
// names, if that row is still retained. A replay that lands inside a retained
// multi-sequence row without starting it is not the event L1 accepted. When
// neither is retained, the replay is acknowledged on the high-water mark.
func checkRetainedLogReplay(ctx context.Context, tx *sql.Tx, attemptID string, event contract.LogEvent, originalRaw, raw []byte) error {
	var stored []byte
	err := tx.QueryRowContext(ctx, "SELECT event_json FROM log_events WHERE attempt_id=? AND stream=? AND sequence=?",
		attemptID, event.Stream, event.Sequence).Scan(&stored)
	switch {
	case err == nil:
		if !bytes.Equal(stored, originalRaw) && !bytes.Equal(stored, raw) {
			return protocolError(contract.ErrorIdempotencyConflict, "log event (%s, %d) conflicts with the accepted event", event.Stream, event.Sequence)
		}
		return nil
	case !errors.Is(err, sql.ErrNoRows):
		return internalError(err, "read accepted log event")
	}
	// Accepted rows never overlap, so only the nearest earlier row can cover it.
	var previousEnd int64
	err = tx.QueryRowContext(ctx, `SELECT sequence_end FROM log_events
		WHERE attempt_id=? AND stream=? AND sequence<? ORDER BY sequence DESC LIMIT 1`,
		attemptID, event.Stream, event.Sequence).Scan(&previousEnd)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil
	case err != nil:
		return internalError(err, "read covering accepted log event")
	case previousEnd >= int64(event.Sequence):
		return protocolError(contract.ErrorIdempotencyConflict, "log event (%s, %d) conflicts with the accepted event", event.Stream, event.Sequence)
	}
	return nil
}
