package l1

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"slices"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

// These are observations, not durable Seat lifecycle states.
type ComputerSeatObservedState string

const (
	ComputerSeatStarting ComputerSeatObservedState = "starting"
	ComputerSeatReady    ComputerSeatObservedState = "ready"
	ComputerSeatFailed   ComputerSeatObservedState = "failed"
	ComputerSeatRemoving ComputerSeatObservedState = "removing"
	ComputerSeatCleaned  ComputerSeatObservedState = "cleaned"
)

type ComputerSeatAttemptRequest struct {
	NodeID        string `json:"node_id"`
	BootSessionID string `json:"boot_session_id"`
	FencingToken  string `json:"fencing_token"`
}
type ComputerSeatObservation struct {
	SeatID          string                    `json:"seat_id"`
	Place           int                       `json:"place"`
	State           ComputerSeatObservedState `json:"state"`
	Reason          string                    `json:"reason"`
	CleanupComplete bool                      `json:"cleanup_complete"`
}
type ComputerSeatObservationReport struct {
	NodeID        string                    `json:"node_id"`
	BootSessionID string                    `json:"boot_session_id"`
	FencingToken  string                    `json:"fencing_token"`
	ListRevision  int64                     `json:"list_revision"`
	Generation    uint64                    `json:"generation"`
	Sequence      int64                     `json:"sequence"`
	Seats         []ComputerSeatObservation `json:"seats"`
}
type ComputerSeatObservationReceipt struct {
	Accepted bool `json:"accepted"`
}

// ComputerSeatEvidence retains only the latest observation per Seat and exact
// Attempt. It grants neither execution authority nor permission to return Room.
type ComputerSeatEvidence struct {
	ComputerSeatObservation
	AttemptID    string
	ListRevision int64
	Generation   uint64
	Sequence     int64
	ObservedAt   time.Time
}

func validateComputerSeatAttempt(ctx context.Context, q queryer, now time.Time, identity, computerID, jobID, attemptID string, request ComputerSeatAttemptRequest) error {
	a, err := readAttemptAuthority(ctx, q, attemptID)
	if err != nil {
		return err
	}
	if err := validateAttemptAuthority(identity, jobID, attemptID, request.FencingToken, a); err != nil {
		return err
	}
	if request.NodeID != a.nodeID || request.BootSessionID != a.bootSessionID {
		return protocolError(contract.ErrorNodeSessionReplaced, "Seat observation node registration does not match")
	}
	if (a.state != contract.AttemptClaimed && a.state != contract.AttemptRunning && a.state != contract.AttemptAwaitingInput) || !now.Before(a.leaseExpires) {
		return protocolError(contract.ErrorConflict, "Seat observation requires a live attempt")
	}
	var currentJob, desired string
	var limit int
	err = q.QueryRowContext(ctx, `SELECT COALESCE(current_job_id,''),desired_state,seat_limit FROM computers WHERE computer_id=?`, computerID).Scan(&currentJob, &desired, &limit)
	if errors.Is(err, sql.ErrNoRows) {
		return protocolError(contract.ErrorNotFound, "Computer not found")
	}
	if err != nil {
		return internalError(err, "read Computer Seat authority")
	}
	if currentJob != jobID || desired != string(contract.ServiceDesiredRunning) || limit < 1 || a.spec.Kind != contract.JobKindOCI || a.spec.Execution.OCI == nil || a.spec.Execution.OCI.Computer == nil {
		return protocolError(contract.ErrorConflict, "attempt is not this Seat-enabled Computer's current projection")
	}
	return nil
}
func (r *databaseReads) computerSeatListForAttempt(ctx context.Context, identity, computerID, jobID, attemptID string, request ComputerSeatAttemptRequest) (ComputerSeatList, error) {
	if err := validateComputerSeatAttempt(ctx, r.q, r.now(), identity, computerID, jobID, attemptID, request); err != nil {
		return ComputerSeatList{}, err
	}
	list, err := readComputerSeatList(ctx, r.q, computerID)
	if err != nil {
		return ComputerSeatList{}, err
	}
	for _, seat := range list.Seats {
		value, lifecycleErr := readSeatRequest(ctx, r.q, seat.SeatID)
		if lifecycleErr == nil && (value.State != SeatStarting && value.State != SeatLive || value.State == SeatStarting && !r.now().Before(value.Deadline)) {
			return ComputerSeatList{}, errComputerSeatBindingNeeded
		}
		if lifecycleErr != nil && errorCodeForSeatLifecycle(lifecycleErr) != contract.ErrorNotFound {
			return ComputerSeatList{}, lifecycleErr
		}
		var first sql.NullString
		if err := r.q.QueryRowContext(ctx, `SELECT first_attempt_id FROM computer_seat_identities WHERE seat_id=?`, seat.SeatID).Scan(&first); err != nil {
			return ComputerSeatList{}, err
		}
		if !first.Valid || first.String != attemptID {
			return ComputerSeatList{}, errComputerSeatBindingNeeded
		}
	}
	needed, err := seatRoomAttemptBindingNeeded(ctx, r.q, computerID, attemptID)
	if err != nil {
		return ComputerSeatList{}, err
	}
	if needed {
		return ComputerSeatList{}, errComputerSeatBindingNeeded
	}
	list.Recovery, err = readComputerSeatRecovery(ctx, r.q, computerID, attemptID)
	return list, err
}
func (s *Store) GetComputerSeatListForAttempt(ctx context.Context, identity, computerID, jobID, attemptID string, request ComputerSeatAttemptRequest) (ComputerSeatList, error) {
	var list ComputerSeatList
	err := s.withAgentReadSnapshot(ctx, func(ctx context.Context, reads readModel) error {
		var err error
		list, err = reads.computerSeatListForAttempt(ctx, identity, computerID, jobID, attemptID, request)
		return err
	})
	if errors.Is(err, errComputerSeatBindingNeeded) {
		return s.bindComputerSeatList(ctx, identity, computerID, jobID, attemptID, request)
	}
	return list, err
}

var errComputerSeatBindingNeeded = errors.New("Seat list needs attempt binding")

// Binding happens once, before a list can be delivered. Steady-state reads
// stay on the node read snapshot; they acquire no write lock. The immutable
// first Attempt is a tombstone value, not a foreign key into prunable history.
func (s *Store) bindComputerSeatList(ctx context.Context, identity, computerID, jobID, attemptID string, request ComputerSeatAttemptRequest) (ComputerSeatList, error) {
	write, err := s.beginWriteTransaction(ctx, nil)
	if err != nil {
		return ComputerSeatList{}, err
	}
	defer write.rollback()
	tx := write.tx
	if err := validateComputerSeatAttempt(ctx, tx, write.at, identity, computerID, jobID, attemptID, request); err != nil {
		return ComputerSeatList{}, err
	}
	list, err := readComputerSeatList(ctx, tx, computerID)
	if err != nil {
		return ComputerSeatList{}, err
	}
	removed := false
	for _, seat := range list.Seats {
		var first sql.NullString
		if err := tx.QueryRowContext(ctx, `SELECT first_attempt_id FROM computer_seat_identities WHERE seat_id=?`, seat.SeatID).Scan(&first); err != nil {
			return ComputerSeatList{}, err
		}
		value, lifecycleErr := readSeatRequest(ctx, tx, seat.SeatID)
		if lifecycleErr != nil && errorCodeForSeatLifecycle(lifecycleErr) != contract.ErrorNotFound {
			return ComputerSeatList{}, lifecycleErr
		}
		expired := lifecycleErr == nil && value.State == SeatStarting && !write.at.Before(value.Deadline)
		final := lifecycleErr == nil && value.State != SeatStarting && value.State != SeatLive
		if expired || final {
			if expired {
				if err := setSeatLifecycle(ctx, tx, seat.SeatID, SeatRefused, "Seat readiness deadline exceeded", write.at); err != nil {
					return ComputerSeatList{}, err
				}
			}
			if _, err := tx.ExecContext(ctx, `DELETE FROM computer_wanted_seats WHERE seat_id=?`, seat.SeatID); err != nil {
				return ComputerSeatList{}, err
			}
			removed = true
			continue
		}
		if first.Valid && first.String != attemptID {
			if lifecycleErr == nil {
				state := SeatEnding
				if value.State == SeatStarting {
					state = SeatRefused
				}
				if err := setSeatLifecycle(ctx, tx, seat.SeatID, state, "the Computer restarted", write.at); err != nil {
					return ComputerSeatList{}, err
				}
			}
			if _, err := tx.ExecContext(ctx, `DELETE FROM computer_wanted_seats WHERE computer_id=? AND seat_id=?`, computerID, seat.SeatID); err != nil {
				return ComputerSeatList{}, err
			}
			removed = true
		} else if !first.Valid {
			if _, err := tx.ExecContext(ctx, `UPDATE computer_seat_identities SET first_attempt_id=? WHERE seat_id=? AND first_attempt_id IS NULL`, attemptID, seat.SeatID); err != nil {
				return ComputerSeatList{}, err
			}
		}
	}
	if err := retainSeatAttemptAuthorities(ctx, tx, computerID); err != nil {
		return ComputerSeatList{}, err
	}
	if removed {
		if list.Revision == math.MaxInt64 {
			return ComputerSeatList{}, protocolError(contract.ErrorConflict, "wanted Seat revision exhausted")
		}
		if _, err := tx.ExecContext(ctx, `UPDATE computer_seat_lists SET revision=revision+1 WHERE computer_id=?`, computerID); err != nil {
			return ComputerSeatList{}, err
		}
	}
	list, err = readComputerSeatList(ctx, tx, computerID)
	if err != nil {
		return ComputerSeatList{}, err
	}
	if err := bindSeatRoomAttempt(ctx, tx, computerID, jobID, attemptID, request); err != nil {
		return ComputerSeatList{}, err
	}
	list.Recovery, err = readComputerSeatRecovery(ctx, tx, computerID, attemptID)
	if err != nil {
		return ComputerSeatList{}, err
	}
	if err := write.commit(); err != nil {
		return ComputerSeatList{}, err
	}
	return list, nil
}
func (r *databaseReads) computerSeatObservations(ctx context.Context, computerID string) ([]ComputerSeatEvidence, error) {
	return readComputerSeatObservations(ctx, r.q, computerID, "")
}
func (s *Store) GetComputerSeatObservations(ctx context.Context, computerID string) ([]ComputerSeatEvidence, error) {
	var result []ComputerSeatEvidence
	err := s.withReadSnapshot(ctx, nil, func(ctx context.Context, reads readModel) error {
		var err error
		result, err = reads.computerSeatObservations(ctx, computerID)
		return err
	})
	return result, err
}
func readComputerSeatObservations(ctx context.Context, q queryer, computerID, attemptID string) ([]ComputerSeatEvidence, error) {
	rows, err := q.QueryContext(ctx, `SELECT o.seat_id,i.place,o.attempt_id,o.list_revision,o.helper_generation,o.report_sequence,o.state,o.reason,o.cleanup_complete,o.observed_ns FROM computer_seat_observations o JOIN computer_seat_identities i ON i.seat_id=o.seat_id WHERE i.computer_id=? AND (?='' OR o.attempt_id=?) ORDER BY o.attempt_id,i.place,o.seat_id`, computerID, attemptID, attemptID)
	if err != nil {
		return nil, internalError(err, "read Seat observations")
	}
	defer rows.Close()
	result := []ComputerSeatEvidence{}
	for rows.Next() {
		var value ComputerSeatEvidence
		var ns int64
		if err := rows.Scan(&value.SeatID, &value.Place, &value.AttemptID, &value.ListRevision, &value.Generation, &value.Sequence, &value.State, &value.Reason, &value.CleanupComplete, &ns); err != nil {
			return nil, err
		}
		value.ObservedAt = time.Unix(0, ns).UTC()
		result = append(result, value)
	}
	return result, rows.Err()
}
func (s *Store) ReportComputerSeatObservations(ctx context.Context, identity, computerID, jobID, attemptID string, report ComputerSeatObservationReport) (ComputerSeatObservationReceipt, error) {
	if report.Generation == 0 || report.Generation > math.MaxInt64 || report.Sequence < 1 || report.ListRevision < 0 || len(report.Seats) < 1 || len(report.Seats) > 4 {
		return ComputerSeatObservationReceipt{}, protocolError(contract.ErrorInvalidRequest, "invalid Seat observation bounds")
	}
	report.Seats = slices.Clone(report.Seats)
	slices.SortFunc(report.Seats, func(a, b ComputerSeatObservation) int { return a.Place - b.Place })
	seen := map[int]bool{}
	for _, seat := range report.Seats {
		if !seatApprovalID.MatchString(seat.SeatID) || seat.Place < 1 || seat.Place > 4 || seen[seat.Place] || len(seat.Reason) > 8192 || seat.CleanupComplete != (seat.State == ComputerSeatCleaned) {
			return ComputerSeatObservationReceipt{}, protocolError(contract.ErrorInvalidRequest, "invalid Seat observation")
		}
		seen[seat.Place] = true
		switch seat.State {
		case ComputerSeatStarting, ComputerSeatReady, ComputerSeatRemoving, ComputerSeatCleaned:
		case ComputerSeatFailed:
			if seat.Reason == "" {
				return ComputerSeatObservationReceipt{}, protocolError(contract.ErrorInvalidRequest, "failed Seat requires a reason")
			}
		default:
			return ComputerSeatObservationReceipt{}, protocolError(contract.ErrorInvalidRequest, "unknown Seat observation state")
		}
	}
	write, err := s.beginWriteTransaction(ctx, nil)
	if err != nil {
		return ComputerSeatObservationReceipt{}, err
	}
	defer write.rollback()
	tx := write.tx
	now := canonicalTime(s.clock.Now())
	auth := ComputerSeatAttemptRequest{NodeID: report.NodeID, BootSessionID: report.BootSessionID, FencingToken: report.FencingToken}
	if err := validateComputerSeatAttempt(ctx, tx, now, identity, computerID, jobID, attemptID, auth); err != nil {
		return ComputerSeatObservationReceipt{}, err
	}
	list, err := readComputerSeatList(ctx, tx, computerID)
	if err != nil {
		return ComputerSeatObservationReceipt{}, err
	}
	if list.Revision != report.ListRevision {
		return ComputerSeatObservationReceipt{}, protocolErrorWithDetails(contract.ErrorConflict, map[string]any{"reason": "seat_list_revision_changed"}, "wanted Seat list revision changed")
	}
	wanted := map[string]bool{}
	for _, seat := range list.Seats {
		wanted[seat.SeatID] = true
	}
	previous, err := readComputerSeatObservations(ctx, tx, computerID, attemptID)
	if err != nil {
		return ComputerSeatObservationReceipt{}, err
	}
	var maxSequence int64
	var maxGeneration uint64
	byID := map[string]ComputerSeatEvidence{}
	for _, old := range previous {
		maxSequence = max(maxSequence, old.Sequence)
		maxGeneration = max(maxGeneration, old.Generation)
		byID[old.SeatID] = old
	}
	if report.Sequence < maxSequence || report.Generation < maxGeneration {
		return ComputerSeatObservationReceipt{}, protocolError(contract.ErrorConflict, "Seat observation is older than retained evidence")
	}
	replayCount := 0
	for _, seat := range report.Seats {
		var place int
		var first sql.NullString
		err := tx.QueryRowContext(ctx, `SELECT place,first_attempt_id FROM computer_seat_identities WHERE seat_id=? AND computer_id=?`, seat.SeatID, computerID).Scan(&place, &first)
		if errors.Is(err, sql.ErrNoRows) || err == nil && place != seat.Place {
			return ComputerSeatObservationReceipt{}, protocolError(contract.ErrorConflict, "Seat identity does not match this Computer place")
		}
		if err != nil {
			return ComputerSeatObservationReceipt{}, err
		}
		if ((seat.State == ComputerSeatReady || seat.State == ComputerSeatStarting) && !wanted[seat.SeatID]) || (seat.State == ComputerSeatCleaned && wanted[seat.SeatID]) {
			return ComputerSeatObservationReceipt{}, protocolError(contract.ErrorConflict, "Seat observation disagrees with wanted list")
		}
		if first.Valid && first.String != attemptID {
			var absent bool
			if err := tx.QueryRowContext(ctx, `SELECT first_attempt_absent FROM computer_seat_identities WHERE seat_id=?`, seat.SeatID).Scan(&absent); err != nil {
				return ComputerSeatObservationReceipt{}, err
			}
			if !absent || seat.State == ComputerSeatReady || seat.State == ComputerSeatStarting {
				return ComputerSeatObservationReceipt{}, protocolError(contract.ErrorConflict, "earlier Seat Attempt has no absence proof")
			}
		}
		if !first.Valid {
			if _, err := tx.ExecContext(ctx, `UPDATE computer_seat_identities SET first_attempt_id=? WHERE seat_id=? AND first_attempt_id IS NULL`, attemptID, seat.SeatID); err != nil {
				return ComputerSeatObservationReceipt{}, err
			}
		}
		old, known := byID[seat.SeatID]
		if known && old.State == ComputerSeatCleaned && seat.State != ComputerSeatCleaned {
			return ComputerSeatObservationReceipt{}, protocolError(contract.ErrorConflict, "cleaned Seat observation is final for this attempt")
		}
		if report.Sequence == maxSequence {
			if !known || old.Sequence != report.Sequence || old.Generation != report.Generation || old.ListRevision != report.ListRevision || old.ComputerSeatObservation != seat {
				return ComputerSeatObservationReceipt{}, protocolError(contract.ErrorConflict, "Seat report sequence was already used")
			}
			replayCount++
		}
	}
	if report.Sequence == maxSequence {
		count := 0
		for _, old := range previous {
			if old.Sequence == maxSequence {
				count++
			}
		}
		if count != replayCount {
			return ComputerSeatObservationReceipt{}, protocolError(contract.ErrorConflict, "Seat report retry changed its batch")
		}
	}
	if report.Sequence > maxSequence {
		for _, seat := range report.Seats {
			_, err := tx.ExecContext(ctx, `INSERT INTO computer_seat_observations(seat_id,attempt_id,list_revision,helper_generation,report_sequence,state,reason,cleanup_complete,observed_ns) VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(seat_id,attempt_id) DO UPDATE SET list_revision=excluded.list_revision,helper_generation=excluded.helper_generation,report_sequence=excluded.report_sequence,state=excluded.state,reason=excluded.reason,cleanup_complete=excluded.cleanup_complete,observed_ns=excluded.observed_ns`, seat.SeatID, attemptID, report.ListRevision, report.Generation, report.Sequence, seat.State, seat.Reason, seat.CleanupComplete, now.UnixNano())
			if err != nil {
				return ComputerSeatObservationReceipt{}, internalError(err, "retain Seat observation")
			}
		}
	}
	if err := retainSeatAttemptAuthorities(ctx, tx, computerID); err != nil {
		return ComputerSeatObservationReceipt{}, err
	}
	for _, seat := range report.Seats {
		if err := applySeatLifecycleObservation(ctx, tx, now, seat, s.clock); err != nil {
			return ComputerSeatObservationReceipt{}, err
		}
	}
	if err := write.commit(); err != nil {
		return ComputerSeatObservationReceipt{}, err
	}
	return ComputerSeatObservationReceipt{Accepted: true}, nil
}
