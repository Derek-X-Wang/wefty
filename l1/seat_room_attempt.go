package l1

import (
	"context"
	"encoding/json"
	"fmt"
)

// The memory owner also covers cleanup-only Attempts that never receive a
// wanted Seat. Its cgroup can hold room after every Seat has been cleaned.
func seatRoomAttemptBindingNeeded(ctx context.Context, q queryer, computer, attempt string) (bool, error) {
	var count int
	err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM computer_room_limits WHERE computer_id=? AND (memory_attempt_authority_json IS NULL OR (memory_attempt_absent=1 AND json_extract(memory_attempt_authority_json,'$.attempt_id')!=?))`, computer, attempt).Scan(&count)
	return count != 0, err
}
func bindSeatRoomAttempt(ctx context.Context, q seatApprovalQuery, computer, job, attempt string, auth ComputerSeatAttemptRequest) error {
	raw, err := json.Marshal(ComputerSeatRetiredAttempt{NodeID: auth.NodeID, BootSessionID: auth.BootSessionID, JobID: job, AttemptID: attempt, FencingToken: auth.FencingToken, RemovalGeneration: fmt.Sprint(InitialServiceRemovalGeneration)})
	if err != nil {
		return err
	}
	_, err = q.ExecContext(ctx, `UPDATE computer_room_limits SET memory_attempt_authority_json=?,memory_attempt_absent=0 WHERE computer_id=? AND (memory_attempt_authority_json IS NULL OR (memory_attempt_absent=1 AND json_extract(memory_attempt_authority_json,'$.attempt_id')!=?))`, raw, computer, attempt)
	return err
}
func oldSeatRoomAttemptPending(ctx context.Context, q queryer, computer, attempt string) (bool, error) {
	var count int
	err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM computer_room_limits WHERE computer_id=? AND memory_attempt_absent=0 AND json_extract(memory_attempt_authority_json,'$.attempt_id')!=?`, computer, attempt).Scan(&count)
	return count != 0, err
}
