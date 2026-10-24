package l1

import (
	"context"
	"database/sql"
)

// Preserve a digest before #1143's terminal scrub removes the active secret.
// Old terminal credentials already scrubbed by an earlier binary cannot be
// reconstructed. Their original caller still owns the retained log.
func (s *Store) migrateSeatLogs(ctx context.Context) error {
	w, err := s.beginWriteTransaction(ctx, nil)
	if err != nil {
		return err
	}
	defer w.rollback()
	var applied bool
	if err := w.tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM l1_data_migrations WHERE name='seat_log_seed')`).Scan(&applied); err != nil {
		return err
	}
	if applied {
		return w.commit()
	}
	rows, err := w.tx.QueryContext(ctx, `SELECT c.seat_id,c.caller_identity,c.credential,c.created_ns,COALESCE(NULLIF(a.lifecycle_state,''),a.state,'requested'),COALESCE(NULLIF(a.lifecycle_reason,''),a.reason,'') FROM seat_request_calls c LEFT JOIN seat_approvals a ON a.seat_id=c.seat_id WHERE NOT EXISTS(SELECT 1 FROM seat_logs l WHERE l.seat_id=c.seat_id)`)
	if err != nil {
		return err
	}
	type entry struct {
		id, caller, secret, state, reason string
		created                           int64
	}
	var entries []entry
	for rows.Next() {
		var e entry
		if err := rows.Scan(&e.id, &e.caller, &e.secret, &e.created, &e.state, &e.reason); err != nil {
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
		if _, err := w.tx.ExecContext(ctx, `INSERT INTO seat_logs(seat_id,caller_identity,credential_hash,created_ns,ended_ns,reason) VALUES(?,?,?,?,?,?)`, e.id, e.caller, digest, e.created, ended, e.reason); err != nil {
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
	if _, err := w.tx.ExecContext(ctx, `INSERT INTO l1_data_migrations(name,applied_ns) VALUES('seat_log_seed',?)`, w.at.UnixNano()); err != nil {
		return err
	}
	return w.commit()
}
