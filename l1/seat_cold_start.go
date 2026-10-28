package l1

import (
	"context"
	"fmt"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

const DefaultSeatStartWait = 3 * time.Minute

// Approval, including approval by a window, arms this once. The existing
// deadline column survives retries and L1 restarts without a second timer.
func (s *Store) armSeatCallDeadline(ctx context.Context, q seatApprovalQuery, value SeatApproval, at time.Time) error {
	if value.State != "approved" {
		return nil
	}
	_, err := q.ExecContext(ctx, `UPDATE seat_approvals SET seat_deadline_ns=? WHERE seat_id=? AND seat_deadline_ns=0 AND EXISTS(SELECT 1 FROM seat_request_calls c WHERE c.seat_id=seat_approvals.seat_id)`, at.Add(s.seatStartWait).UnixNano(), value.Request.SeatID)
	return err
}

func seatHostVMStarting(ctx context.Context, q queryer, id string) (bool, error) {
	var waiting bool
	err := q.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM seat_approvals a JOIN nodes n ON n.node_id=a.node_id WHERE a.seat_id=? AND COALESCE(NULLIF(a.lifecycle_state,''),a.state) IN ('approved','starting') AND json_extract(n.capabilities_json,'$."kind:oci"') IS NOT 1 AND n.os='darwin' AND n.capability_reason_code!='oci_intent_disabled')`, id).Scan(&waiting)
	return waiting, err
}

func seatStartFailureReason(ctx context.Context, q queryer, id string) (string, error) {
	var reason contract.CapabilityReasonCode
	err := q.QueryRowContext(ctx, `SELECT n.capability_reason_code FROM seat_approvals a JOIN nodes n ON n.node_id=a.node_id WHERE a.seat_id=?`, id).Scan(&reason)
	if err != nil {
		return "", err
	}
	if reason == contract.CapabilityReasonOCIIntentDisabled {
		return "the host VM is off", nil
	}
	if reason == "" {
		return "Seat start deadline exceeded; run wefty node doctor", nil
	}
	return fmt.Sprintf("Seat start deadline exceeded: %s; run wefty node doctor", reason), nil
}
