package l1

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

// SeatEventKind is the closed vocabulary shared by Seat lifecycle, command,
// copy and take-over writers. The log never contains program output.
type SeatEventKind string

const (
	SeatEventRequested       SeatEventKind = "requested"
	SeatEventApproved        SeatEventKind = "approved"
	SeatEventRefused         SeatEventKind = "refused"
	SeatEventStarting        SeatEventKind = "starting"
	SeatEventStarted         SeatEventKind = "started"
	SeatEventReleased        SeatEventKind = "released"
	SeatEventEnded           SeatEventKind = "ended"
	SeatEventOwnerStop       SeatEventKind = "owner_stop"
	SeatEventOwnerRestart    SeatEventKind = "owner_restart"
	SeatEventCommandStarted  SeatEventKind = "command_started"
	SeatEventCommandFinished SeatEventKind = "command_finished"
	SeatEventCopyIn          SeatEventKind = "copy_in"
	SeatEventCopyOut         SeatEventKind = "copy_out"
	SeatEventControlTaken    SeatEventKind = "control_taken"
	SeatEventControlReleased SeatEventKind = "control_released"
)

type SeatLogEvent struct {
	EventID    string        `json:"event_id"`
	SeatID     string        `json:"seat_id"`
	Kind       SeatEventKind `json:"kind"`
	OccurredAt time.Time     `json:"occurred_at"`
	Reason     string        `json:"reason,omitempty"`
	Note       string        `json:"note,omitempty"`
	RunID      string        `json:"run_id,omitempty"`
	ExitCode   *int          `json:"exit_code,omitempty"`
	Path       string        `json:"path,omitempty"`
	SizeBytes  *int64        `json:"size_bytes,omitempty"`
	// Take-over evidence and the optional release note live in this same record.
	Audit *ComputerTakeoverAuditEvent `json:"audit,omitempty"`
}

type SeatLogPage struct {
	SeatID     string         `json:"seat_id"`
	Events     []SeatLogEvent `json:"events"`
	NextCursor string         `json:"next_cursor"`
	Ended      bool           `json:"ended"`
	Reason     string         `json:"reason"`
}
type SeatLogRead struct {
	Cursor string `json:"cursor"`
}

const seatLogSchema = `
CREATE TABLE IF NOT EXISTS seat_logs (
 seat_id TEXT PRIMARY KEY,
 caller_identity TEXT NOT NULL DEFAULT '',
 credential_hash TEXT NOT NULL DEFAULT '',
 created_ns INTEGER NOT NULL,
 ever_live INTEGER NOT NULL DEFAULT 0 CHECK(ever_live IN (0,1)),
 ended_ns INTEGER,
 reason TEXT NOT NULL DEFAULT '',
 read_ordinal INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS seat_logs_ended ON seat_logs(ended_ns);
CREATE UNIQUE INDEX IF NOT EXISTS seat_logs_credential ON seat_logs(credential_hash) WHERE credential_hash!='';
CREATE TABLE IF NOT EXISTS seat_log_events (
 ordinal INTEGER PRIMARY KEY AUTOINCREMENT,
 seat_id TEXT NOT NULL REFERENCES seat_logs(seat_id) ON DELETE CASCADE,
 event_id TEXT NOT NULL,
 event_json BLOB NOT NULL,
 UNIQUE(seat_id,event_id)
);
CREATE INDEX IF NOT EXISTS seat_log_events_seat ON seat_log_events(seat_id,ordinal);
`

func seatCredentialHash(secret string) string {
	v := sha256.Sum256([]byte(secret))
	return hex.EncodeToString(v[:])
}

// AppendSeatEvent is the small shared L1 writer. Callers provide a stable event
// ID; a retry cannot duplicate or change evidence. Feature-specific authority
// is checked by the calling operation, never delegated to the event payload.
func (s *Store) AppendSeatEvent(ctx context.Context, event SeatLogEvent) error {
	w, err := s.beginWriteTransaction(ctx, nil)
	if err != nil {
		return err
	}
	defer w.rollback()
	if err := appendSeatEvent(ctx, w.tx, w.at, event); err != nil {
		return err
	}
	return w.commit()
}
func appendSeatEvent(ctx context.Context, q seatApprovalQuery, at time.Time, event SeatLogEvent) error {
	if !seatApprovalID.MatchString(event.SeatID) || event.EventID == "" || len(event.EventID) > 255 || len(event.Reason) > 8192 || len(event.Note) > 8192 || len(event.Path) > 4096 || (event.SizeBytes != nil && *event.SizeBytes < 0) {
		return protocolError(contract.ErrorInvalidRequest, "invalid Seat log event")
	}
	switch event.Kind {
	case SeatEventRequested, SeatEventApproved, SeatEventRefused, SeatEventStarting, SeatEventStarted, SeatEventReleased, SeatEventEnded, SeatEventOwnerStop, SeatEventOwnerRestart, SeatEventCommandStarted, SeatEventCommandFinished, SeatEventCopyIn, SeatEventCopyOut, SeatEventControlTaken, SeatEventControlReleased:
	default:
		return protocolError(contract.ErrorInvalidRequest, "unknown Seat log event kind")
	}
	suppliedTime := !event.OccurredAt.IsZero()
	if !suppliedTime {
		event.OccurredAt = at
	}
	event.OccurredAt = canonicalTime(event.OccurredAt)
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	var old []byte
	err = q.QueryRowContext(ctx, `SELECT event_json FROM seat_log_events WHERE seat_id=? AND event_id=?`, event.SeatID, event.EventID).Scan(&old)
	if err == nil {
		var previous SeatLogEvent
		if err := json.Unmarshal(old, &previous); err != nil {
			return err
		}
		// L1 supplies stored time. A retry with no timestamp keeps that time.
		if !suppliedTime {
			event.OccurredAt = previous.OccurredAt
		}
		data, err = json.Marshal(event)
		if err != nil {
			return err
		}
		if string(data) != string(old) {
			return protocolError(contract.ErrorIdempotencyConflict, "Seat event ID has different evidence")
		}
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	var exists bool
	if err := q.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM seat_logs WHERE seat_id=?)`, event.SeatID).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		return protocolError(contract.ErrorNotFound, "Seat log not found; a Seat request must create it first")
	}
	if _, err = q.ExecContext(ctx, `INSERT INTO seat_log_events(seat_id,event_id,event_json) VALUES(?,?,?)`, event.SeatID, event.EventID, data); err != nil {
		return err
	}
	if event.Kind == SeatEventStarted {
		if _, err := q.ExecContext(ctx, `UPDATE seat_logs SET ever_live=1 WHERE seat_id=?`, event.SeatID); err != nil {
			return err
		}
	}
	if event.Kind == SeatEventEnded || event.Kind == SeatEventRefused {
		_, err = q.ExecContext(ctx, `UPDATE seat_logs SET ended_ns=COALESCE(ended_ns,?),reason=? WHERE seat_id=?`, at.UnixNano(), event.Reason, event.SeatID)
	}
	return err
}

func seatLogCursor(id string, n int64) string {
	return base64.RawURLEncoding.EncodeToString([]byte(id + ":" + strconv.FormatInt(n, 10)))
}
func parseSeatLogCursor(id, cursor string) (int64, error) {
	if cursor == "" {
		return 0, nil
	}
	data, err := base64.RawURLEncoding.DecodeString(cursor)
	if err == nil {
		prefix, number, ok := strings.Cut(string(data), ":")
		n, e := strconv.ParseInt(number, 10, 64)
		if ok && prefix == id && e == nil && n >= 0 {
			return n, nil
		}
	}
	return 0, protocolError(contract.ErrorInvalidRequest, "invalid cursor for this Seat log")
}
func authorizeSeatLog(ctx context.Context, q queryer, id, caller, secret string) error {
	var owner, digest string
	err := q.QueryRowContext(ctx, `SELECT caller_identity,credential_hash FROM seat_logs WHERE seat_id=?`, id).Scan(&owner, &digest)
	if errors.Is(err, sql.ErrNoRows) {
		return protocolError(contract.ErrorNotFound, "Seat log not found")
	}
	if err != nil {
		return err
	}
	if secret != "" {
		if digest != "" && seatCredentialHash(secret) == digest {
			return nil
		}
	} else if owner != "" && caller == owner {
		return nil
	}
	return protocolError(contract.ErrorForbidden, "Seat log belongs to another caller or credential")
}

// GetSeatLogs uses only a read snapshot and never acknowledges unprinted data.
// Follow polling does not renew Seat activity. #1146 owns one-time call activity.
func (s *Store) GetSeatLogs(ctx context.Context, caller, id, secret, cursor string, limit int) (SeatLogPage, error) {
	page := SeatLogPage{SeatID: id, Events: []SeatLogEvent{}}
	n, err := parseSeatLogCursor(id, cursor)
	if err != nil {
		return page, err
	}
	if limit < 1 || limit > MaxLogPageLimit {
		return page, protocolError(contract.ErrorInvalidRequest, "invalid Seat log page limit")
	}
	err = s.withReadSnapshot(ctx, nil, func(ctx context.Context, reads readModel) error {
		r := reads.(*databaseReads)
		q := r.q
		if err := authorizeSeatLog(ctx, q, id, caller, secret); err != nil {
			return err
		}
		var ended sql.NullInt64
		var created int64
		var everLive bool
		if err := q.QueryRowContext(ctx, `SELECT created_ns,ended_ns,ever_live,reason FROM seat_logs WHERE seat_id=?`, id).Scan(&created, &ended, &everLive, &page.Reason); err != nil {
			return err
		}
		if seatLogRetentionExpired(r.now(), created, ended, everLive, s.computerTakeoverAuditRetentionAge) {
			return protocolError(contract.ErrorNotFound, "Seat log retention ended")
		}
		page.Ended = ended.Valid
		rows, err := q.QueryContext(ctx, `SELECT ordinal,event_json FROM seat_log_events WHERE seat_id=? AND ordinal>? ORDER BY ordinal LIMIT ?`, id, n, limit)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var data []byte
			if err := rows.Scan(&n, &data); err != nil {
				return err
			}
			var event SeatLogEvent
			if err := json.Unmarshal(data, &event); err != nil {
				return err
			}
			page.Events = append(page.Events, event)
		}
		page.NextCursor = seatLogCursor(id, n)
		return rows.Err()
	})
	return page, err
}

func (s *Store) MarkSeatLogRead(ctx context.Context, caller, id, secret, cursor string) error {
	n, err := parseSeatLogCursor(id, cursor)
	if err != nil {
		return err
	}
	w, err := s.beginWriteTransaction(ctx, nil)
	if err != nil {
		return err
	}
	defer w.rollback()
	if err := authorizeSeatLog(ctx, w.tx, id, caller, secret); err != nil {
		return err
	}
	var ended sql.NullInt64
	var created int64
	var everLive bool
	if err := w.tx.QueryRowContext(ctx, `SELECT created_ns,ended_ns,ever_live FROM seat_logs WHERE seat_id=?`, id).Scan(&created, &ended, &everLive); err != nil {
		return err
	}
	if seatLogRetentionExpired(w.at, created, ended, everLive, s.computerTakeoverAuditRetentionAge) {
		return protocolError(contract.ErrorNotFound, "Seat log retention ended")
	}
	var maximum int64
	if err := w.tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(ordinal),0) FROM seat_log_events WHERE seat_id=?`, id).Scan(&maximum); err != nil {
		return err
	}
	if n > maximum {
		return protocolError(contract.ErrorInvalidRequest, "Seat read cursor is beyond the log")
	}
	if _, err := w.tx.ExecContext(ctx, `UPDATE seat_logs SET read_ordinal=MAX(read_ordinal,?) WHERE seat_id=?`, n, id); err != nil {
		return err
	}
	return w.commit()
}

// Only request admission creates a log. Generic event writers cannot invent or
// revive one after retention. The caller's request transaction owns this insert.
func createSeatLog(ctx context.Context, q seatApprovalQuery, id string, at time.Time) error {
	_, err := q.ExecContext(ctx, `INSERT INTO seat_logs(seat_id,created_ns) VALUES(?,?) ON CONFLICT DO NOTHING`, id, at.UnixNano())
	return err
}

func seatLogRetentionExpired(at time.Time, created int64, ended sql.NullInt64, everLive bool, age time.Duration) bool {
	if !everLive {
		return !at.Before(time.Unix(0, created).Add(age))
	}
	return ended.Valid && !at.Before(time.Unix(0, ended.Int64).Add(age))
}

func (s *Store) pruneSeatLogs(ctx context.Context, tx *sql.Tx, now time.Time) (int64, error) {
	cutoff := now.Add(-s.computerTakeoverAuditRetentionAge).UnixNano()
	result, err := tx.ExecContext(ctx, `DELETE FROM seat_logs WHERE seat_id IN (
 SELECT seat_id FROM seat_logs WHERE ever_live=1 AND ended_ns<=?
 UNION ALL SELECT seat_id FROM seat_logs WHERE ever_live=0 AND created_ns<=?
 )`, cutoff, cutoff)
	if err != nil {
		return 0, internalError(err, "prune Seat log retention")
	}
	count, err := result.RowsAffected()
	if err != nil {
		return 0, internalError(err, "count pruned Seat logs")
	}
	return count, nil
}
