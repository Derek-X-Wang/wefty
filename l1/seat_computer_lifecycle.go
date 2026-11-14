package l1

import (
	"context"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

// seatEndHostVMOff names the owner's off switch. The off switch suppresses the
// Computer attempt's completion, so L1 learns of the end only at lease expiry.
const seatEndHostVMOff = "the owner turned the host VM off"

// endHostVMOffSeats ends a lost Computer attempt's Seats with the off-switch
// reason when its Node reports durable OCI intent off. A host VM restart keeps
// intent on and delivers its completion, so it keeps "the Computer restarted".
func endHostVMOffSeats(ctx context.Context, q seatApprovalQuery, nodeID, jobID, attemptID string, now time.Time) error {
	computer, err := computerForJob(ctx, q, jobID)
	if err != nil || computer == "" {
		return err
	}
	var off bool
	if err := q.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM nodes WHERE node_id=? AND capability_reason_code=?
 AND json_extract(capabilities_json,'$."kind:oci"') IS NOT 1)`, nodeID, contract.CapabilityReasonOCIIntentDisabled).Scan(&off); err != nil {
		return err
	}
	if !off {
		return nil
	}
	return endComputerSeats(ctx, q, computer, attemptID, seatEndHostVMOff, now)
}

// Authority loss and Seat ending commit with the Computer intent. Attempt
// completion is scoped to its old Seats, never to a replacement's new Seats.
func endComputerSeats(ctx context.Context, q seatApprovalQuery, computer, attempt, reason string, now time.Time) error {
	if attempt != "" {
		if _, err := q.ExecContext(ctx, `DELETE FROM seat_copy_holds WHERE attempt_id=?`, attempt); err != nil {
			return err
		}
	}
	query := `SELECT i.seat_id FROM computer_seat_identities i JOIN seat_approvals a ON a.seat_id=i.seat_id WHERE i.computer_id=? AND COALESCE(NULLIF(a.lifecycle_state,''),a.state) IN ('requested','approved','starting','live')`
	args := []any{computer}
	if attempt != "" {
		query += ` AND (i.first_attempt_id=? OR (i.first_attempt_id IS NULL AND EXISTS(SELECT 1 FROM attempts old JOIN computers c ON c.current_job_id=old.job_id WHERE old.attempt_id=? AND c.computer_id=i.computer_id)))`
		args = append(args, attempt, attempt)
	}
	ids, err := seatIDs(ctx, q, query, args...)
	if err != nil {
		return err
	}
	for _, id := range ids {
		v, err := readSeatRequest(ctx, q, id)
		if err != nil {
			return err
		}
		state := SeatRefused
		if v.State == SeatLive {
			state = SeatEnding
		}
		kind := SeatEventKind("")
		if reason == "computer_stopped" {
			kind = SeatEventOwnerStop
		} else if reason == "computer_restarted" {
			kind = SeatEventOwnerRestart
		} else if reason == "computer_reimaged" {
			kind = SeatEventOwnerReimage
		} else if reason == "computer_reset" {
			kind = SeatEventOwnerReset
		} else if reason == "computer_removed" {
			kind = SeatEventOwnerRemove
		}
		if kind != "" {
			if _, err := q.ExecContext(ctx, `UPDATE seat_approvals SET ended_computer_revision=(SELECT intent_revision FROM computers WHERE computer_id=?) WHERE seat_id=?`, computer, id); err != nil {
				return err
			}
		}
		if kind != "" {
			if err := appendSeatEvent(ctx, q, now, SeatLogEvent{SeatID: id, EventID: string(kind), Kind: kind, Reason: reason}); err != nil {
				return err
			}
		}
		if err := setSeatLifecycle(ctx, q, id, state, reason, now); err != nil {
			return err
		}
		if err := removeLifecycleSeat(ctx, q, v); err != nil {
			return err
		}
		if err := cleanNeverStartedSeat(ctx, q, v); err != nil {
			return err
		}
	}
	return nil
}
func seatIDs(ctx context.Context, q queryer, query string, args ...any) ([]string, error) {
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := []string{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// A positive exact Attempt removal/absence also cleans its Seats. Fresh
// per-place folder proof still fences reuse in the node agent/helper loop.
func cleanComputerSeatAttempt(ctx context.Context, q seatApprovalQuery, computer, attempt string, now time.Time) error {
	if err := endComputerSeats(ctx, q, computer, attempt, "the Computer restarted", now); err != nil {
		return err
	}
	ids, err := seatIDs(ctx, q, `SELECT i.seat_id FROM computer_seat_identities i JOIN seat_approvals a ON a.seat_id=i.seat_id WHERE i.computer_id=? AND (i.first_attempt_id=? OR (i.first_attempt_id IS NULL AND a.lifecycle_state IN ('refused','ending','ended')))`, computer, attempt)
	if err != nil {
		return err
	}
	for _, id := range ids {
		v, err := readSeatRequest(ctx, q, id)
		if err != nil {
			return err
		}
		if err := markSeatReservationClean(ctx, q, v); err != nil {
			return err
		}
		if v.State == SeatEnding {
			if err := setSeatLifecycle(ctx, q, id, SeatEnded, v.Reason, now); err != nil {
				return err
			}
		}
	}
	if _, err := q.ExecContext(ctx, `UPDATE computer_seat_identities SET first_attempt_absent=1 WHERE computer_id=? AND first_attempt_id=?`, computer, attempt); err != nil {
		return err
	}
	_, err = q.ExecContext(ctx, `UPDATE computer_room_limits SET memory_attempt_absent=1 WHERE computer_id=? AND json_extract(memory_attempt_authority_json,'$.attempt_id')=?`, computer, attempt)
	return err
}
func releaseAbsentComputerAttemptRoom(ctx context.Context, q seatApprovalQuery, computer, attempt string) error {
	_, err := q.ExecContext(ctx, `UPDATE computer_room_limits SET memory_bytes=COALESCE((SELECT SUM(memory_bytes) FROM computer_seat_room_reservations WHERE computer_id=? AND cleanup_complete=0),0),revision=revision+1 WHERE computer_id=? AND (memory_attempt_authority_json IS NULL OR (memory_attempt_absent=1 AND json_extract(memory_attempt_authority_json,'$.attempt_id')=?))`, computer, computer, attempt)
	return err
}
func cleanRemovedComputerSeats(ctx context.Context, q seatApprovalQuery, computer string, now time.Time) error {
	if err := endComputerSeats(ctx, q, computer, "", "the owner removed the Computer", now); err != nil {
		return err
	}
	ids, err := seatIDs(ctx, q, `SELECT i.seat_id FROM computer_seat_identities i JOIN seat_approvals a ON a.seat_id=i.seat_id WHERE i.computer_id=?`, computer)
	if err != nil {
		return err
	}
	for _, id := range ids {
		v, err := readSeatRequest(ctx, q, id)
		if err != nil {
			return err
		}
		if err := markSeatReservationClean(ctx, q, v); err != nil {
			return err
		}
		if v.State == SeatEnding {
			if err := setSeatLifecycle(ctx, q, id, SeatEnded, v.Reason, now); err != nil {
				return err
			}
		}
	}
	if _, err := q.ExecContext(ctx, `UPDATE computer_seat_identities SET first_attempt_absent=1 WHERE computer_id=?`, computer); err != nil {
		return err
	}
	_, err = q.ExecContext(ctx, `DELETE FROM computer_room_limits WHERE computer_id=?`, computer)
	return err
}
