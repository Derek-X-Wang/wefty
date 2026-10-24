package l1

import (
	"context"
	"database/sql"
)

// Preserve a digest before #1143's terminal scrub removes the active secret.
// Old terminal credentials already scrubbed by an earlier binary cannot be
// reconstructed. Their original caller still owns the retained log.
func (s *Store) migrateSeatLogs(ctx context.Context) error {
	if err := s.ensureColumn(ctx, "seat_logs", "ever_live", "INTEGER NOT NULL DEFAULT 0 CHECK(ever_live IN (0,1))"); err != nil {
		return err
	}
	if err := s.ensureColumn(ctx, "computer_seat_identities", "ever_live", "INTEGER NOT NULL DEFAULT 0 CHECK(ever_live IN (0,1))"); err != nil {
		return err
	}
	w, err := s.beginWriteTransaction(ctx, nil)
	if err != nil {
		return err
	}
	defer w.rollback()
	if _, err := w.tx.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS seat_logs_never_live_age ON seat_logs(created_ns) WHERE ever_live=0`); err != nil {
		return err
	}
	var historyApplied bool
	if err := w.tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM l1_data_migrations WHERE name='seat_log_live_history')`).Scan(&historyApplied); err != nil {
		return err
	}
	if !historyApplied {
		// A pre-fix orphan's invented started event is not evidence of a real Seat.
		if _, err := w.tx.ExecContext(ctx, `UPDATE seat_logs SET ever_live=1 WHERE EXISTS(SELECT 1 FROM seat_log_events e WHERE e.seat_id=seat_logs.seat_id AND json_extract(e.event_json,'$.kind')='started') AND (EXISTS(SELECT 1 FROM seat_request_calls c WHERE c.seat_id=seat_logs.seat_id) OR EXISTS(SELECT 1 FROM seat_approvals a WHERE a.seat_id=seat_logs.seat_id) OR EXISTS(SELECT 1 FROM computer_seat_identities i WHERE i.seat_id=seat_logs.seat_id))`); err != nil {
			return err
		}
		if _, err := w.tx.ExecContext(ctx, `INSERT INTO l1_data_migrations(name,applied_ns) VALUES('seat_log_live_history',?)`, w.at.UnixNano()); err != nil {
			return err
		}
	}
	var applied bool
	if err := w.tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM l1_data_migrations WHERE name='seat_log_seed')`).Scan(&applied); err != nil {
		return err
	}
	var authorityApplied bool
	if err := w.tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM l1_data_migrations WHERE name='seat_log_authority_seed')`).Scan(&authorityApplied); err != nil {
		return err
	}
	if applied && authorityApplied {
		return w.commit()
	}
	rows, err := w.tx.QueryContext(ctx, `SELECT ids.seat_id,COALESCE(c.caller_identity,''),COALESCE(c.credential,''),COALESCE(c.created_ns,?),COALESCE(NULLIF(a.lifecycle_state,''),a.state,CASE WHEN i.ever_live=1 THEN 'live' ELSE 'requested' END),COALESCE(NULLIF(a.lifecycle_reason,''),a.reason,''),COALESCE(i.ever_live,0) OR COALESCE(c.start_finished,0)
 FROM (SELECT seat_id FROM seat_request_calls WHERE ?=0 UNION SELECT seat_id FROM seat_approvals WHERE ?=0 UNION SELECT seat_id FROM computer_seat_identities WHERE ?=0) ids
 LEFT JOIN seat_request_calls c USING(seat_id) LEFT JOIN seat_approvals a USING(seat_id) LEFT JOIN computer_seat_identities i USING(seat_id)
 WHERE NOT EXISTS(SELECT 1 FROM seat_logs l WHERE l.seat_id=ids.seat_id)`, w.at.UnixNano(), applied, authorityApplied, authorityApplied)
	if err != nil {
		return err
	}
	type entry struct {
		id, caller, secret, state, reason string
		created                           int64
		everLive                          bool
	}
	var entries []entry
	for rows.Next() {
		var e entry
		if err := rows.Scan(&e.id, &e.caller, &e.secret, &e.created, &e.state, &e.reason, &e.everLive); err != nil {
			rows.Close()
			return err
		}
		entries = append(entries, e)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, e := range entries {
		digest := ""
		if e.secret != "" {
			digest = seatCredentialHash(e.secret)
		}
		var ended sql.NullInt64
		if e.state == "ending" || e.state == "ended" || e.state == "refused" {
			ended = sql.NullInt64{Int64: w.at.UnixNano(), Valid: true}
		}
		if _, err := w.tx.ExecContext(ctx, `INSERT INTO seat_logs(seat_id,caller_identity,credential_hash,created_ns,ended_ns,reason,ever_live) VALUES(?,?,?,?,?,?,?)`, e.id, e.caller, digest, e.created, ended, e.reason, e.everLive || e.state == "live"); err != nil {
			return err
		}
		// This is a current-state receipt, not a reconstruction of missing history.
		kind := SeatEventKind(e.state)
		switch e.state {
		case "live":
			kind = SeatEventStarted
		case "ending", "ended":
			kind = SeatEventEnded
		}
		if err := appendSeatEvent(ctx, w.tx, w.at, SeatLogEvent{SeatID: e.id, EventID: "upgrade-state", Kind: kind, Reason: e.reason}); err != nil {
			return err
		}
	}
	if _, err := w.tx.ExecContext(ctx, `INSERT INTO l1_data_migrations(name,applied_ns) VALUES('seat_log_seed',?),('seat_log_authority_seed',?) ON CONFLICT DO NOTHING`, w.at.UnixNano(), w.at.UnixNano()); err != nil {
		return err
	}
	return w.commit()
}
