package l1

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/Derek-X-Wang/wefty/contract"
)

func markSeatReservationClean(ctx context.Context, q seatApprovalQuery, value SeatRequest) error {
	result, err := q.ExecContext(ctx, `UPDATE computer_seat_room_reservations SET cleanup_complete=1 WHERE seat_id=? AND cleanup_complete=0`, value.SeatID)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed > 0 {
		_, err = q.ExecContext(ctx, `UPDATE computer_room_limits SET revision=revision+1 WHERE computer_id=?`, value.ComputerID)
	}
	return err
}

// No wanted directive can exist before starting. Cancel that reservation, but
// retain any possibly changed runtime charge once a helper caller has bound it.
func cleanNeverStartedSeat(ctx context.Context, q seatApprovalQuery, value SeatRequest) error {
	if !value.Deadline.IsZero() {
		return nil
	}
	if err := markSeatReservationClean(ctx, q, value); err != nil {
		return err
	}
	if value.ComputerID == "" {
		return nil
	}
	charge, err := initialComputerRuntimeCharge(ctx, q, value.ComputerID)
	if err != nil {
		return err
	}
	_, err = q.ExecContext(ctx, `UPDATE computer_room_limits SET memory_bytes=MAX(?,COALESCE((SELECT SUM(memory_bytes) FROM computer_seat_room_reservations WHERE computer_id=? AND cleanup_complete=0),0)) WHERE computer_id=? AND memory_attempt_authority_json IS NULL`, charge, value.ComputerID, value.ComputerID)
	return err
}
func initialComputerRuntimeCharge(ctx context.Context, q queryer, computer string) (int64, error) {
	var state string
	var raw []byte
	err := q.QueryRowContext(ctx, `SELECT j.state,j.spec_json FROM computers c JOIN jobs j ON j.job_id=c.current_job_id WHERE c.computer_id=?`, computer).Scan(&state, &raw)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if state != string(contract.JobClaimed) && state != string(contract.JobRunning) && state != string(contract.JobStopping) && state != string(contract.JobRemovalPending) && state != string(contract.JobAgentCleaned) {
		return 0, nil
	}
	var spec contract.JobSpec
	if err := json.Unmarshal(raw, &spec); err != nil {
		return 0, err
	}
	if spec.Execution.OCI != nil && spec.Execution.OCI.Limits != nil && spec.Execution.OCI.Limits.MemoryBytes != nil {
		return *spec.Execution.OCI.Limits.MemoryBytes, nil
	}
	return 0, nil
}

func computerCreationMemoryFloor(ctx context.Context, q queryer, computer string) (int64, error) {
	var raw []byte
	if err := q.QueryRowContext(ctx, `SELECT j.spec_json FROM computers c JOIN jobs j ON j.job_id=c.current_job_id WHERE c.computer_id=?`, computer).Scan(&raw); err != nil {
		return 0, err
	}
	var spec contract.JobSpec
	if err := json.Unmarshal(raw, &spec); err != nil {
		return 0, err
	}
	if spec.Execution.OCI != nil && spec.Execution.OCI.Limits != nil && spec.Execution.OCI.Limits.MemoryBytes != nil {
		return *spec.Execution.OCI.Limits.MemoryBytes, nil
	}
	return 0, nil
}
