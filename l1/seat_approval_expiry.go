package l1

import (
	"context"
	"time"
)

func seatApprovalNeedsExpiry(ctx context.Context, q queryer, v SeatRequest, now time.Time, clock Clock) (bool, error) {
	if (v.State == SeatApproved || v.State == SeatStarting) && !v.Deadline.IsZero() {
		return !now.Before(v.Deadline), nil
	}
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
	return current != boot || time.Duration(max(0, ns-started)) >= a.Request.AnswerWait, nil
}
func expireSeatApproval(ctx context.Context, q seatApprovalQuery, id string, now time.Time, clock Clock) error {
	if err := recoverSeatApprovalClocks(ctx, q, clock); err != nil {
		return err
	}
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
	reason := "no answer from the owner"
	if v.State != SeatRequested {
		reason, err = seatStartFailureReason(ctx, q, id)
		if err != nil {
			return err
		}
	}
	if err := setSeatLifecycle(ctx, q, id, SeatRefused, reason, now); err != nil {
		return err
	}
	if err := removeLifecycleSeat(ctx, q, v); err != nil {
		return err
	}
	return cleanNeverStartedSeat(ctx, q, v)
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
