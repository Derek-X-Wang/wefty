package l1

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/Derek-X-Wang/wefty/contract"
)

// Copy holds are separate from command cancellation IDs. A missing terminal
// receipt can delay idle expiry only by the bounded boot-relative lease.
func (s *Store) renewSeatCopyHold(ctx context.Context, id string, r SeatCopyReceipt) error {
	w, err := s.beginWriteTransaction(ctx, nil)
	if err != nil {
		return err
	}
	defer w.rollback()
	if err := s.authorizeLiveSeat(ctx, w.tx, id, r.Credential, w.at); err != nil {
		if errorCode(err) == "seat_ended" {
			if commitErr := w.commit(); commitErr != nil {
				return commitErr
			}
		}
		return err
	}
	var raw []byte
	if err := w.tx.QueryRowContext(ctx, `SELECT event_json FROM seat_log_events WHERE seat_id=? AND event_id=?`, id, "copy:"+r.CopyID).Scan(&raw); err != nil {
		return err
	}
	var event SeatLogEvent
	if err := json.Unmarshal(raw, &event); err != nil {
		return err
	}
	if event.NodeOutcome != "sending" {
		return nil
	}
	boot, ns, err := seatTime(s.clock)
	if err != nil {
		return err
	}
	var oldBoot string
	var until int64
	err = w.tx.QueryRowContext(ctx, `SELECT boot_id,lease_until_ns FROM seat_copy_holds WHERE seat_id=? AND copy_id=?`, id, r.CopyID).Scan(&oldBoot, &until)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if err == nil && (oldBoot != boot || until <= ns) {
		return protocolError(contract.ErrorConflict, "Seat copy hold lease expired")
	}
	if errors.Is(err, sql.ErrNoRows) {
		if err := s.seatCallHappenedTx(ctx, w.tx, id); err != nil {
			return err
		}
	}
	_, err = w.tx.ExecContext(ctx, `INSERT INTO seat_copy_holds(seat_id,copy_id,attempt_id,boot_id,lease_until_ns) VALUES(?,?,?,?,?) ON CONFLICT(seat_id,copy_id) DO UPDATE SET lease_until_ns=excluded.lease_until_ns`, id, r.CopyID, r.AttemptID, boot, ns+int64(contract.SeatCopyHoldLease))
	if err != nil {
		return err
	}
	return w.commit()
}
func (s *Store) releaseSeatCopyHold(ctx context.Context, id, copyID string) error {
	w, err := s.beginWriteTransaction(ctx, nil)
	if err != nil {
		return err
	}
	defer w.rollback()
	if err := s.checkSeatLifeTxIfLive(ctx, w.tx, id, w.at); err != nil {
		return err
	}
	result, err := w.tx.ExecContext(ctx, `DELETE FROM seat_copy_holds WHERE seat_id=? AND copy_id=?`, id, copyID)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n > 0 {
		if err := touchSeatActivity(ctx, w.tx, s.clock, id); err != nil {
			return err
		}
	}
	return w.commit()
}
