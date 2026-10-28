package l1

import (
	"context"
	"fmt"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

const DefaultSeatStartWait = 3 * time.Minute

// Approval records the potential cold-start limit once. It becomes a deadline
// only if this request encounters an idle host VM; warm starts keep their budget.
func (s *Store) armSeatCallDeadline(ctx context.Context, q seatApprovalQuery, value SeatApproval, at time.Time) error {
	if value.State != "approved" {
		return nil
	}
	_, err := q.ExecContext(ctx, `UPDATE seat_approvals SET host_vm_wait_deadline_ns=? WHERE seat_id=? AND host_vm_wait_deadline_ns=0 AND EXISTS(SELECT 1 FROM seat_request_calls c WHERE c.seat_id=seat_approvals.seat_id)`, at.Add(s.seatStartWait).UnixNano(), value.Request.SeatID)
	if err != nil {
		return err
	}
	return armIdleNodeSeatDeadlines(ctx, q, value.Request.NodeID)
}

// Run in the same transaction as approval or a node capability observation.
// Once armed, recovery and an L1 restart cannot extend the deadline.
func armIdleNodeSeatDeadlines(ctx context.Context, q seatApprovalQuery, node string) error {
	_, err := q.ExecContext(ctx, `UPDATE seat_approvals SET seat_deadline_ns=host_vm_wait_deadline_ns
 WHERE node_id=? AND COALESCE(NULLIF(lifecycle_state,''),state) IN ('approved','starting')
 AND host_vm_wait_deadline_ns>0 AND (seat_deadline_ns=0 OR seat_deadline_ns>host_vm_wait_deadline_ns)
 AND EXISTS(SELECT 1 FROM nodes n WHERE n.node_id=seat_approvals.node_id AND n.os='darwin'
 AND n.capability_reason_code='host_vm_idle' AND json_extract(n.capabilities_json,'$."kind:oci"') IS NOT 1)`, node)
	return err
}

func seatHostVMStarting(ctx context.Context, q queryer, id string) (bool, error) {
	var waiting bool
	err := q.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM seat_approvals a JOIN nodes n ON n.node_id=a.node_id WHERE a.seat_id=? AND COALESCE(NULLIF(a.lifecycle_state,''),a.state) IN ('approved','starting') AND json_extract(n.capabilities_json,'$."kind:oci"') IS NOT 1 AND n.os='darwin' AND n.capability_reason_code='host_vm_idle')`, id).Scan(&waiting)
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
