package l1

import (
	"context"
	"time"
)

func seatApprovalNeedsExpiry(ctx context.Context, q queryer, v SeatRequest, now time.Time) (bool, error) {
	if v.State != SeatRequested {
		return false, nil
	}
	var deadline int64
	err := q.QueryRowContext(ctx, `SELECT deadline_ns FROM seat_approvals WHERE seat_id=?`, v.SeatID).Scan(&deadline)
	return now.UnixNano() >= deadline, err
}
func expireSeatApproval(ctx context.Context, q seatApprovalQuery, id string, now time.Time) error {
	_, err := q.ExecContext(ctx, `UPDATE seat_approvals SET state='refused',reason='no answer from the owner',lifecycle_state='refused',lifecycle_reason='no answer from the owner' WHERE seat_id=? AND COALESCE(NULLIF(lifecycle_state,''),state)='requested' AND deadline_ns<=?`, id, now.UnixNano())
	return err
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
	if err := expireSeatApproval(ctx, write.tx, id, write.at); err != nil {
		return SeatRequest{}, err
	}
	v, err := readSeatRequest(ctx, write.tx, id)
	if err != nil {
		return v, err
	}
	return v, write.commit()
}
