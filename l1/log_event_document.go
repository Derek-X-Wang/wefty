package l1

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/Derek-X-Wang/wefty/contract"
)

// A log event is stored as its raw payload (log_events.bytes) beside a JSON
// document of every other field (log_events.event_json). Before #52 the
// document also carried the payload, Base64-encoded, so every logged byte was
// kept twice and a third larger the second time. Every read rebuilds the full
// event from the two columns, so the wire event is unchanged.
//
// encoding/json writes struct fields in declaration order and omits an empty
// Bytes, so a raw event's full document is its stored document with
// `,"bytes":"<base64>"` inserted before the closing brace: Bytes follows
// Timestamp, and a raw event carries no Gap (validateLogEvent). A gap event
// has no payload, and its stored document is the full one.

// logEventDocumentKey appears in a stored document only as the key of a
// payload it still carries: inside a JSON string every quote is escaped, so
// the unescaped sequence cannot come from a value.
var logEventDocumentKey = []byte(`"bytes":`)

// storedLogEventDocument is the document L1 stores for an accepted event.
func storedLogEventDocument(event contract.LogEvent) ([]byte, error) {
	event.Bytes = nil
	return json.Marshal(event)
}

// logEventDocument rebuilds an event's full JSON document from its stored
// document and payload. A row written before #52, whose document still
// carries its payload, is returned as stored.
func logEventDocument(stored, payload []byte) []byte {
	if len(payload) == 0 || bytes.Contains(stored, logEventDocumentKey) || len(stored) < 2 || stored[len(stored)-1] != '}' {
		return stored
	}
	body := stored[:len(stored)-1]
	document := make([]byte, 0, len(stored)+len(`,"bytes":""`)+base64.StdEncoding.EncodedLen(len(payload)))
	document = append(document, body...)
	if body[len(body)-1] != '{' {
		document = append(document, ',')
	}
	document = append(document, `"bytes":"`...)
	document = base64.StdEncoding.AppendEncode(document, payload)
	return append(document, `"}`...)
}

// decodeStoredLogEvent is logEventDocument for a caller that wants the event.
func decodeStoredLogEvent(stored, payload []byte) (contract.LogEvent, error) {
	var event contract.LogEvent
	if err := json.Unmarshal(stored, &event); err != nil {
		return contract.LogEvent{}, err
	}
	if event.Bytes == nil && len(payload) != 0 {
		event.Bytes = payload
	}
	return event, nil
}

// compactLegacyLogEventDocument strips the payload from a document written
// before #52. It strips only a document that is exactly what
// logEventDocument would rebuild from the stripped one and the payload, so a
// migrated row reads back byte for byte; any other document is left as it
// is, and read as it is.
func compactLegacyLogEventDocument(stored, payload []byte) ([]byte, bool) {
	if len(payload) == 0 {
		return nil, false
	}
	suffix := make([]byte, 0, len(`,"bytes":""}`)+base64.StdEncoding.EncodedLen(len(payload)))
	suffix = append(suffix, `,"bytes":"`...)
	suffix = base64.StdEncoding.AppendEncode(suffix, payload)
	suffix = append(suffix, `"}`...)
	if !bytes.HasSuffix(stored, suffix) {
		return nil, false
	}
	compact := make([]byte, 0, len(stored)-len(suffix)+1)
	compact = append(compact, stored[:len(stored)-len(suffix)]...)
	compact = append(compact, '}')
	if bytes.Contains(compact, logEventDocumentKey) || !bytes.Equal(logEventDocument(compact, payload), stored) {
		return nil, false
	}
	return compact, true
}

const (
	logEventDocumentMigration = "log_event_document_without_payload"
	// One migration pass rewrites at most this many rows, or stops after the
	// row that takes it past logEventDocumentMigrationBytes of stored
	// documents, so an upgraded database is compacted in pieces rather than
	// in one write transaction that renewals and claims wait behind.
	logEventDocumentMigrationRows        = 512
	logEventDocumentMigrationBytes int64 = 16 << 20
)

// LogEventCompaction reports one pass of CompactLogEventDocuments.
type LogEventCompaction struct {
	Rewritten int64
	Done      bool
}

// markLogEventDocumentsCompactOnNewDatabase records the migration as applied
// when there is nothing to migrate, so a new database never runs it.
func (s *Store) markLogEventDocumentsCompactOnNewDatabase(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("l1: begin log event document migration check: %w", err)
	}
	defer tx.Rollback()
	var applied, rows bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM l1_data_migrations WHERE name=?),
		EXISTS(SELECT 1 FROM log_events)`, logEventDocumentMigration).Scan(&applied, &rows); err != nil {
		return fmt.Errorf("l1: inspect log event document migration: %w", err)
	}
	if applied || rows {
		return nil
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO l1_data_migrations(name, applied_ns) VALUES(?, ?)`,
		logEventDocumentMigration, s.clock.Now().UnixNano()); err != nil {
		return fmt.Errorf("l1: record log event document migration: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("l1: commit log event document migration check: %w", err)
	}
	return nil
}

// CompactLogEventDocuments rewrites one bounded batch of rows written before
// #52 to the stored document without the payload. The rows it may need to
// touch are fixed on its first pass: everything up to the highest ordinal
// then, since every later row is written compact. Its resume point is
// committed with each batch (l1_data_migration_cursors), and the
// l1_data_migrations marker with the last one, so a crash repeats at most
// the uncommitted batch, and rewriting a row twice is a no-op. The retained
// byte counters are untouched: they count the payload, which never moves.
func (s *Store) CompactLogEventDocuments(ctx context.Context) (LogEventCompaction, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return LogEventCompaction{}, internalError(err, "begin log event document migration")
	}
	defer tx.Rollback()
	var applied bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM l1_data_migrations WHERE name=?)`,
		logEventDocumentMigration).Scan(&applied); err != nil {
		return LogEventCompaction{}, internalError(err, "inspect log event document migration")
	}
	if applied {
		return LogEventCompaction{Done: true}, nil
	}
	var through, ceiling int64
	err = tx.QueryRowContext(ctx, `SELECT through_ordinal, ceiling_ordinal FROM l1_data_migration_cursors WHERE name=?`,
		logEventDocumentMigration).Scan(&through, &ceiling)
	if errors.Is(err, sql.ErrNoRows) {
		if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(ordinal), 0) FROM log_events`).Scan(&ceiling); err != nil {
			return LogEventCompaction{}, internalError(err, "bound log event document migration")
		}
		through = 0
	} else if err != nil {
		return LogEventCompaction{}, internalError(err, "read log event document migration cursor")
	}

	type legacyRow struct {
		ordinal  int64
		document []byte
	}
	rows, err := tx.QueryContext(ctx, `SELECT ordinal, event_json, bytes FROM log_events
		WHERE ordinal>? AND ordinal<=? ORDER BY ordinal LIMIT ?`, through, ceiling, logEventDocumentMigrationRows)
	if err != nil {
		return LogEventCompaction{}, internalError(err, "select log event documents to migrate")
	}
	var rewrites []legacyRow
	scanned := 0
	var scannedBytes int64
	for rows.Next() && scannedBytes < logEventDocumentMigrationBytes {
		var ordinal int64
		var stored, payload []byte
		if err := rows.Scan(&ordinal, &stored, &payload); err != nil {
			rows.Close()
			return LogEventCompaction{}, internalError(err, "scan log event document to migrate")
		}
		scanned++
		scannedBytes += int64(len(stored))
		through = ordinal
		if compact, ok := compactLegacyLogEventDocument(stored, payload); ok {
			rewrites = append(rewrites, legacyRow{ordinal: ordinal, document: compact})
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return LogEventCompaction{}, internalError(err, "iterate log event documents to migrate")
	}
	if err := rows.Close(); err != nil {
		return LogEventCompaction{}, internalError(err, "close log event documents to migrate")
	}
	for _, row := range rewrites {
		if _, err := tx.ExecContext(ctx, `UPDATE log_events SET event_json=? WHERE ordinal=?`, row.document, row.ordinal); err != nil {
			return LogEventCompaction{}, internalError(err, "rewrite log event document")
		}
	}
	result := LogEventCompaction{Rewritten: int64(len(rewrites))}
	if scanned == 0 || through >= ceiling {
		if _, err := tx.ExecContext(ctx, `DELETE FROM l1_data_migration_cursors WHERE name=?`, logEventDocumentMigration); err != nil {
			return LogEventCompaction{}, internalError(err, "retire log event document migration cursor")
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO l1_data_migrations(name, applied_ns) VALUES(?, ?)`,
			logEventDocumentMigration, s.clock.Now().UnixNano()); err != nil {
			return LogEventCompaction{}, internalError(err, "record log event document migration")
		}
		result.Done = true
	} else if _, err := tx.ExecContext(ctx, `INSERT INTO l1_data_migration_cursors(name, through_ordinal, ceiling_ordinal)
		VALUES(?, ?, ?) ON CONFLICT(name) DO UPDATE SET through_ordinal=excluded.through_ordinal`,
		logEventDocumentMigration, through, ceiling); err != nil {
		return LogEventCompaction{}, internalError(err, "advance log event document migration cursor")
	}
	if err := tx.Commit(); err != nil {
		return LogEventCompaction{}, internalError(err, "commit log event document migration")
	}
	return result, nil
}
