package l1

import (
	"context"
	"time"
)

func seatApprovalNeedsExpiry(ctx context.Context, q queryer, v SeatRequest, now time.Time, clock Clock) (bool, error) {
	if v.State != SeatRequested {
		return false, nil
	}
	var boot string
	var started int64
	if err := q.QueryRowContext(ctx, `SELECT wait_boot_id,wait_started_ns FROM seat_approvals WHERE seat_id=?`, v.SeatID).Scan(&boot, &started); err != nil {
		return false, err
	}
	// A pre-upgrade pending dialog has no boot-relative authority. Fail closed.
	if boot == "" {
		return true, nil
	}
	current, ns, err := seatTime(clock)
	if err != nil {
		return false, err
	}
	a, err := readSeatApproval(ctx, q, v.SeatID)
	if err != nil {
		return false, err
	}
	return current != boot || ns < started || time.Duration(ns-started) >= a.Request.AnswerWait, nil
}
func expireSeatApproval(ctx context.Context, q seatApprovalQuery, id string, now time.Time, clock Clock) error {
	v, err := readSeatRequest(ctx, q, id)
	if errorCodeForSeatLifecycle(err) == "not_found" {
		return nil
	}
	if err != nil {
		return err
	}
	expired, err := seatApprovalNeedsExpiry(ctx, q, v, now, clock)
	if err != nil || !expired {
		return err
	}
	return setSeatLifecycle(ctx, q, id, SeatRefused, "no answer from the owner", now)
}

// Normal reads remain read-only. An expired pending read enters a separate
// fenced write and checks node authority again before publishing the refusal.
func (s *Store) expireSeatApprovalRead(ctx context.Context, id, identity string, request *SeatNodeRequest) (SeatRequest, error) {
	write, err := s.beginWriteTransaction(ctx, nil)
	if err != nil {
		return SeatRequest{}, err
	}
	defer write.rollback()
	if request != nil {
		if err := authorizeSeatRequest(ctx, write.tx, identity, id, request.NodeID, request.BootSessionID); err != nil {
			return SeatRequest{}, err
		}
	}
	if err := expireSeatApproval(ctx, write.tx, id, write.at, s.clock); err != nil {
		return SeatRequest{}, err
	}
	v, err := readSeatRequest(ctx, write.tx, id)
	if err != nil {
		return v, err
	}
	return v, write.commit()
}
