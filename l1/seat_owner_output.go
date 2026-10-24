package l1

import "context"

// ComputerEndedSeat is the durable list ended by the current owner intent.
// It remains stable on an idempotent replay, including after helper cleanup.
type ComputerEndedSeat struct {
	SeatID string `json:"seat_id"`
	Reason string `json:"reason"`
}

func readComputerEndedSeats(ctx context.Context, q queryer, computer string, revision int64) ([]ComputerEndedSeat, error) {
	rows, err := q.QueryContext(ctx, `SELECT a.seat_id,a.lifecycle_reason FROM seat_approvals a JOIN computer_seat_identities i ON i.seat_id=a.seat_id WHERE i.computer_id=? AND a.ended_computer_revision=? ORDER BY a.seat_id`, computer, revision)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []ComputerEndedSeat{}
	for rows.Next() {
		var v ComputerEndedSeat
		if err := rows.Scan(&v.SeatID, &v.Reason); err != nil {
			return nil, err
		}
		result = append(result, v)
	}
	return result, rows.Err()
}
