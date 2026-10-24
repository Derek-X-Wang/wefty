package l1

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/Derek-X-Wang/wefty/contract"
)

// Recovery bindings carry cleanup authority only. They never add wanted Seats.
type ComputerSeatRecovery struct {
	RetiredAttempts []ComputerSeatRetiredAttempt `json:"retired_attempts"`
	Bindings        []WantedComputerSeat         `json:"bindings"`
	Sequence        int64                        `json:"sequence"`
	Room            *SeatRoomMemoryReceipt       `json:"room,omitempty"`
}

func readComputerSeatRecovery(ctx context.Context, q queryer, computer, attempt string) (*ComputerSeatRecovery, error) {
	value := &ComputerSeatRecovery{Bindings: []WantedComputerSeat{}, RetiredAttempts: []ComputerSeatRetiredAttempt{}}
	rows, err := q.QueryContext(ctx, `SELECT i.seat_id,i.place FROM computer_seat_identities i
 LEFT JOIN computer_seat_observations o ON o.seat_id=i.seat_id AND o.attempt_id=?
 LEFT JOIN computer_seat_room_reservations r ON r.seat_id=i.seat_id
 LEFT JOIN seat_approvals a ON a.seat_id=i.seat_id
 WHERE i.computer_id=? AND ((i.first_attempt_id IS NULL AND a.lifecycle_state IN ('ending','ended','refused')) OR i.first_attempt_id=? OR (i.first_attempt_absent=1 AND r.seat_id IS NOT NULL)) AND COALESCE(o.cleanup_complete,0)=0 AND (COALESCE(r.cleanup_complete,0)=0 OR i.first_attempt_absent=1 OR (i.first_attempt_id IS NULL AND a.lifecycle_state IN ('refused','ended'))) ORDER BY i.place,CASE WHEN i.first_attempt_id=? THEN 1 ELSE 0 END,i.introduced_revision`, attempt, computer, attempt, attempt)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var binding WantedComputerSeat
		if err := rows.Scan(&binding.SeatID, &binding.Place); err != nil {
			rows.Close()
			return nil, err
		}
		value.Bindings = append(value.Bindings, binding)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	if err := q.QueryRowContext(ctx, `SELECT COALESCE(MAX(report_sequence),0) FROM computer_seat_observations WHERE attempt_id=?`, attempt).Scan(&value.Sequence); err != nil {
		return nil, err
	}
	value.RetiredAttempts, err = readRetiredSeatAttempts(ctx, q, computer, attempt)
	if err != nil {
		return nil, err
	}
	room, err := readComputerSeatRoomReceipt(ctx, q, computer)
	if err == nil {
		value.Room = &room
	} else if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	return value, nil
}
func readComputerSeatRoomReceipt(ctx context.Context, q queryer, computer string) (SeatRoomMemoryReceipt, error) {
	var room SeatRoomMemoryReceipt
	var specJSON []byte
	err := q.QueryRowContext(ctx, `SELECT l.memory_bytes,l.revision,
 COALESCE((SELECT SUM(memory_bytes) FROM computer_seat_room_reservations WHERE computer_id=l.computer_id AND cleanup_complete=0),0),j.spec_json
 FROM computer_room_limits l JOIN computers c ON c.computer_id=l.computer_id JOIN jobs j ON j.job_id=c.current_job_id WHERE l.computer_id=?`, computer).Scan(&room.HeldMemoryBytes, &room.RoomRevision, &room.TargetMemoryBytes, &specJSON)
	if err != nil {
		return room, err
	}
	// The creation declaration bounds the immutable scratch/tmpfs floor. An
	// empty Computer still needs that positive limit until runtime absence.
	var spec contract.JobSpec
	if err := json.Unmarshal(specJSON, &spec); err != nil {
		return room, err
	}
	if spec.Execution.OCI != nil && spec.Execution.OCI.Limits != nil && spec.Execution.OCI.Limits.MemoryBytes != nil {
		room.TargetMemoryBytes = max(room.TargetMemoryBytes, *spec.Execution.OCI.Limits.MemoryBytes)
	}
	return room, nil
}

// This immutable snapshot outlives prunable Attempt history. It is evidence
// identity for cleanup, never new runtime authority.
type ComputerSeatRetiredAttempt struct {
	NodeID            string `json:"node_id"`
	BootSessionID     string `json:"boot_session_id"`
	JobID             string `json:"job_id"`
	AttemptID         string `json:"attempt_id"`
	FencingToken      string `json:"fencing_token"`
	RemovalGeneration string `json:"removal_generation"`
}
type ComputerSeatAttemptAbsenceReport struct {
	ComputerSeatAttemptRequest
	Retired ComputerSeatRetiredAttempt `json:"retired"`
	Absent  bool                       `json:"absent"`
}

const seatAttemptSnapshotSQL = `(SELECT json_object('node_id',a.node_id,'boot_session_id',a.boot_session_id,'job_id',a.job_id,'attempt_id',a.attempt_id,'fencing_token',a.fencing_token,'removal_generation',?) FROM attempts a WHERE a.attempt_id=computer_seat_identities.first_attempt_id)`

func retainSeatAttemptAuthorities(ctx context.Context, q seatApprovalQuery, computer string) error {
	query := `UPDATE computer_seat_identities SET first_attempt_authority_json=` + seatAttemptSnapshotSQL + ` WHERE first_attempt_id IS NOT NULL AND first_attempt_authority_json IS NULL`
	args := []any{fmt.Sprint(InitialServiceRemovalGeneration)}
	if computer != "" {
		query += ` AND computer_id=?`
		args = append(args, computer)
	}
	_, err := q.ExecContext(ctx, query, args...)
	return err
}
func readRetiredSeatAttempts(ctx context.Context, q queryer, computer, current string) ([]ComputerSeatRetiredAttempt, error) {
	rows, err := q.QueryContext(ctx, `SELECT DISTINCT i.first_attempt_authority_json FROM computer_seat_identities i JOIN computer_seat_room_reservations r ON r.seat_id=i.seat_id
 WHERE i.computer_id=? AND i.first_attempt_id!=? AND i.first_attempt_absent=0
 UNION SELECT memory_attempt_authority_json FROM computer_room_limits WHERE computer_id=? AND memory_attempt_absent=0 AND json_extract(memory_attempt_authority_json,'$.attempt_id')!=? ORDER BY 1`, computer, current, computer, current)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	values := []ComputerSeatRetiredAttempt{}
	seen := map[string]ComputerSeatRetiredAttempt{}
	for rows.Next() {
		var raw []byte
		var value ComputerSeatRetiredAttempt
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(raw, &value); err != nil || value.NodeID == "" || value.BootSessionID == "" || value.JobID == "" || value.AttemptID == "" || value.FencingToken == "" || value.RemovalGeneration == "" {
			return nil, errors.New("earlier Seat Attempt identity is unavailable; room remains held")
		}
		if old, ok := seen[value.AttemptID]; ok {
			if old != value {
				return nil, errors.New("earlier Seat Attempt identity changed; room remains held")
			}
			continue
		}
		seen[value.AttemptID] = value
		values = append(values, value)
	}
	return values, rows.Err()
}
func (s *Store) ReportComputerSeatAttemptAbsence(ctx context.Context, identity, computer, job, current string, report ComputerSeatAttemptAbsenceReport) (ComputerSeatObservationReceipt, error) {
	if !report.Absent || report.Retired.AttemptID == "" || report.Retired.AttemptID == current {
		return ComputerSeatObservationReceipt{}, protocolError(contract.ErrorInvalidRequest, "positive earlier Attempt absence is required")
	}
	write, err := s.beginWriteTransaction(ctx, nil)
	if err != nil {
		return ComputerSeatObservationReceipt{}, err
	}
	defer write.rollback()
	q := write.tx
	if err := validateComputerSeatAttempt(ctx, q, write.at, identity, computer, job, current, report.ComputerSeatAttemptRequest); err != nil {
		return ComputerSeatObservationReceipt{}, err
	}
	var raw []byte
	if err := q.QueryRowContext(ctx, `SELECT first_attempt_authority_json FROM computer_seat_identities WHERE computer_id=? AND first_attempt_id=?
 UNION SELECT memory_attempt_authority_json FROM computer_room_limits WHERE computer_id=? AND json_extract(memory_attempt_authority_json,'$.attempt_id')=? LIMIT 1`, computer, report.Retired.AttemptID, computer, report.Retired.AttemptID).Scan(&raw); err != nil {
		return ComputerSeatObservationReceipt{}, protocolError(contract.ErrorConflict, "earlier Seat Attempt is unknown")
	}
	var retained ComputerSeatRetiredAttempt
	if json.Unmarshal(raw, &retained) != nil || retained != report.Retired || retained.NodeID != report.NodeID {
		return ComputerSeatObservationReceipt{}, protocolError(contract.ErrorConflict, "earlier Attempt identity changed")
	}
	if err := cleanComputerSeatAttempt(ctx, q, computer, retained.AttemptID, write.at); err != nil {
		return ComputerSeatObservationReceipt{}, err
	}

	return ComputerSeatObservationReceipt{Accepted: true}, write.commit()
}
