package l1

import (
	"context"
	"crypto/subtle"
	"database/sql"
	"errors"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

type SeatCopyTarget struct {
	Endpoint     string `json:"endpoint"`
	UnreadEvents int    `json:"unread_events"`
}
type SeatCopyAuthorityInput struct {
	ComputerSeatAttemptRequest
	ComputerID string `json:"computer_id"`
	JobID      string `json:"job_id"`
	AttemptID  string `json:"attempt_id"`
	Credential string `json:"credential"`
}
type SeatCopyAuthority struct {
	Generation uint64 `json:"generation"`
	Place      int    `json:"place"`
}
type SeatCopyReceipt struct {
	SeatCopyAuthorityInput
	CopyID    string `json:"copy_id"`
	Direction string `json:"direction"`
	Path      string `json:"path"`
	SizeBytes int64  `json:"size_bytes"`
	Outcome   string `json:"outcome,omitempty"`
	Reason    string `json:"reason,omitempty"`
}

func checkSeatCopyCredential(ctx context.Context, q queryer, id, credential string, requireLive bool) (SeatRequest, error) {
	if credential == "" {
		return SeatRequest{}, protocolError(contract.ErrorForbidden, "Seat credential is required")
	}
	var hash, reason string
	var ended sql.NullInt64
	err := q.QueryRowContext(ctx, `SELECT credential_hash,ended_ns,reason FROM seat_logs WHERE seat_id=?`, id).Scan(&hash, &ended, &reason)
	if errors.Is(err, sql.ErrNoRows) {
		return SeatRequest{}, protocolError(contract.ErrorForbidden, "invalid Seat credential")
	}
	if err != nil {
		return SeatRequest{}, err
	}
	if hash == "" || subtle.ConstantTimeCompare([]byte(hash), []byte(seatCredentialHash(credential))) != 1 {
		return SeatRequest{}, protocolError(contract.ErrorForbidden, "invalid Seat credential")
	}
	if requireLive && ended.Valid {
		return SeatRequest{}, protocolError(contract.ErrorCode("seat_ended"), "Seat ended: %s", reason)
	}
	value, err := readSeatRequest(ctx, q, id)
	if err != nil {
		return SeatRequest{}, err
	}
	if requireLive && value.State != SeatLive {
		if value.State == SeatEnding || value.State == SeatEnded || value.State == SeatRefused {
			return SeatRequest{}, protocolError(contract.ErrorCode("seat_ended"), "Seat ended: %s", value.Reason)
		}
		return SeatRequest{}, protocolError(contract.ErrorConflict, "Seat is not live")
	}
	return value, nil
}

func seatCopyBinding(ctx context.Context, q queryer, now time.Time, id string, value SeatRequest) (SeatCopyAuthority, string, error) {
	var generation uint64
	var attempt string
	err := q.QueryRowContext(ctx, `SELECT o.helper_generation,o.attempt_id FROM computer_seat_observations o JOIN computer_wanted_seats w ON w.seat_id=o.seat_id JOIN computer_seat_identities i ON i.seat_id=o.seat_id WHERE o.seat_id=? AND w.computer_id=? AND w.place=? AND o.state='ready' AND o.cleanup_complete=0 AND o.attempt_id=i.first_attempt_id`, id, value.ComputerID, value.Place).Scan(&generation, &attempt)
	if errors.Is(err, sql.ErrNoRows) {
		return SeatCopyAuthority{}, "", protocolError(contract.ErrorConflict, "Seat is not ready")
	}
	if err != nil {
		return SeatCopyAuthority{}, "", err
	}
	a, err := readAttemptAuthority(ctx, q, attempt)
	if err != nil {
		return SeatCopyAuthority{}, "", err
	}
	if err = validateComputerSeatAttempt(ctx, q, now, a.identityNodeID, value.ComputerID, a.jobID, attempt, ComputerSeatAttemptRequest{NodeID: a.nodeID, BootSessionID: a.bootSessionID, FencingToken: a.fencingToken}); err != nil {
		return SeatCopyAuthority{}, "", err
	}
	return SeatCopyAuthority{Generation: generation, Place: value.Place}, attempt, nil
}
func (s *Store) GetSeatCopyTarget(ctx context.Context, id, credential string) (target SeatCopyTarget, err error) {
	err = s.withReadSnapshot(ctx, nil, func(ctx context.Context, reads readModel) error {
		rd := reads.(*databaseReads)
		value, e := checkSeatCopyCredential(ctx, rd.q, id, credential, true)
		if e != nil {
			return e
		}
		_, attempt, e := seatCopyBinding(ctx, rd.q, rd.now(), id, value)
		if e != nil {
			return e
		}
		var endpoint sql.NullString
		e = rd.q.QueryRowContext(ctx, `SELECT j.display_endpoint FROM computers c JOIN service_jobs j ON j.job_id=c.current_job_id WHERE c.computer_id=? AND j.published_attempt_id=?`, value.ComputerID, attempt).Scan(&endpoint)
		if errors.Is(e, sql.ErrNoRows) {
			return protocolError(contract.ErrorUnavailable, "Seat copy endpoint is not published")
		}
		if e != nil {
			return e
		}
		if !endpoint.Valid || endpoint.String == "" {
			return protocolError(contract.ErrorUnavailable, "Seat copy endpoint is not published")
		}
		target.Endpoint = endpoint.String
		return rd.q.QueryRowContext(ctx, `SELECT COUNT(*) FROM seat_log_events e JOIN seat_logs l ON l.seat_id=e.seat_id WHERE e.seat_id=? AND e.ordinal>l.read_ordinal`, id).Scan(&target.UnreadEvents)
	})
	return
}
func (s *Store) AuthorizeSeatCopy(ctx context.Context, identity, id string, input SeatCopyAuthorityInput) (result SeatCopyAuthority, err error) {
	err = s.withAgentReadSnapshot(ctx, func(ctx context.Context, reads readModel) error {
		rd := reads.(*databaseReads)
		value, e := checkSeatCopyCredential(ctx, rd.q, id, input.Credential, true)
		if e != nil {
			return e
		}
		if value.ComputerID != input.ComputerID {
			return protocolError(contract.ErrorForbidden, "Seat belongs to another Computer")
		}
		if e = validateComputerSeatAttempt(ctx, rd.q, rd.now(), identity, input.ComputerID, input.JobID, input.AttemptID, input.ComputerSeatAttemptRequest); e != nil {
			return e
		}
		var attempt string
		result, attempt, e = seatCopyBinding(ctx, rd.q, rd.now(), id, value)
		if e != nil {
			return e
		}
		if attempt != input.AttemptID {
			return protocolError(contract.ErrorForbidden, "Seat belongs to another Attempt")
		}
		return nil
	})
	return
}
func (s *Store) RecordSeatCopy(ctx context.Context, identity, id string, input SeatCopyReceipt) error {
	if input.Outcome == "" {
		input.Outcome = "completed"
		if input.Direction == "out" {
			input.Outcome = "sent"
		}
	}
	if !validCopyReceipt(SeatCopyOutcomeReceipt{input.CopyID, input.Direction, input.Path, input.SizeBytes, input.Outcome, input.Reason}, true) {
		return protocolError(contract.ErrorInvalidRequest, "invalid Seat copy receipt")
	}
	// An already admitted copy may finish beside the Seat's end. Retain its
	// evidence without admitting another operation or changing removal proof.
	err := s.withAgentReadSnapshot(ctx, func(ctx context.Context, reads readModel) error {
		rd := reads.(*databaseReads)
		value, e := checkSeatCopyCredential(ctx, rd.q, id, input.Credential, false)
		if e != nil {
			return e
		}
		if value.ComputerID != input.ComputerID {
			return protocolError(contract.ErrorForbidden, "Seat belongs to another Computer")
		}
		a, e := readAttemptAuthority(ctx, rd.q, input.AttemptID)
		if e != nil {
			return e
		}
		if e = validateAttemptAuthority(identity, input.JobID, input.AttemptID, input.FencingToken, a); e != nil {
			return e
		}
		if input.NodeID != a.nodeID || input.BootSessionID != a.bootSessionID {
			return protocolError(contract.ErrorForbidden, "copy receipt node mismatch")
		}
		var first sql.NullString
		if e = rd.q.QueryRowContext(ctx, `SELECT first_attempt_id FROM computer_seat_identities WHERE seat_id=?`, id).Scan(&first); e != nil {
			return e
		}
		if !first.Valid || first.String != input.AttemptID {
			return protocolError(contract.ErrorForbidden, "copy receipt Attempt mismatch")
		}
		return nil
	})
	if err != nil {
		return err
	}
	return s.appendSeatCopyOutcome(ctx, id, SeatCopyOutcomeReceipt{input.CopyID, input.Direction, input.Path, input.SizeBytes, input.Outcome, input.Reason})
}

// SeatCopyStreamEvent is one bounded NDJSON line from the hosting node.
// A final size is success; EOF without that line is always a failed copy.
type SeatCopyStreamEvent struct {
	Data      []byte             `json:"data,omitempty"`
	CopyID    string             `json:"copy_id,omitempty"`
	SizeBytes *int64             `json:"size_bytes,omitempty"`
	Error     *contract.APIError `json:"error,omitempty"`
}
