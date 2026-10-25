package l1

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/Derek-X-Wang/wefty/contract"
)

type SeatScreenAuthorityInput struct {
	SeatCopyAuthorityInput
	Input  bool `json:"input"`
	Person bool `json:"person"`
}
type SeatScreenAuthority struct {
	SeatCopyAuthority
	ReleaseNote  string `json:"release_note"`
	UnreadEvents int    `json:"unread_events"`
}
type SeatScreenEventReceipt struct {
	Event    SeatLogEvent `json:"event"`
	Replayed bool         `json:"replayed"`
}

type SeatScreenEventRequest struct {
	SeatCopyAuthorityInput
	Event SeatLogEvent `json:"event"`
}

// This is node-only authority. A person path is still authenticated and
// authorized by the installed Computer policy at the front door. A supplied
// Seat credential always selects the AI agent path, including on person URLs.
func (s *Store) AuthorizeSeatScreen(ctx context.Context, identity, id string, input SeatScreenAuthorityInput) (result SeatScreenAuthority, err error) {
	w, err := s.beginWriteTransaction(ctx, nil)
	if err != nil {
		return result, err
	}
	defer w.rollback()
	ctx = w.ctx
	person := input.Person && input.Credential == ""
	var value SeatRequest
	if person {
		err = s.checkSeatLifeTxIfLive(ctx, w.tx, id, w.at)
		if err == nil {
			value, err = readSeatRequest(ctx, w.tx, id)
		}
		if err == nil && value.State != SeatLive {
			err = protocolError(contract.ErrorCode("seat_ended"), "Seat ended: %s", value.Reason)
		}
	} else {
		err = s.authorizeLiveSeat(ctx, w.tx, id, input.Credential, w.at)
		if err == nil {
			value, err = readSeatRequest(ctx, w.tx, id)
		}
	}
	if err != nil {
		if errorCode(err) == "seat_ended" {
			if commitErr := w.commit(); commitErr != nil {
				return result, commitErr
			}
		}
		return result, err
	}
	if value.ComputerID != input.ComputerID {
		return result, protocolError(contract.ErrorForbidden, "Seat belongs to another Computer")
	}
	if err = validateComputerSeatAttempt(ctx, w.tx, w.at, identity, input.ComputerID, input.JobID, input.AttemptID, input.ComputerSeatAttemptRequest); err != nil {
		return result, err
	}
	var attempt string
	result.SeatCopyAuthority, attempt, err = seatCopyBinding(ctx, w.tx, w.at, id, value)
	if err != nil {
		return result, err
	}
	if attempt != input.AttemptID {
		return result, protocolError(contract.ErrorForbidden, "Seat belongs to another Attempt")
	}
	if !person {
		if err = s.seatCallHappenedTx(ctx, w.tx, id); err != nil {
			return result, err
		}
	}
	var driving bool
	if err = w.tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM seat_activity_holds WHERE seat_id=? AND kind='person' AND active=1)`, id).Scan(&driving); err != nil {
		return result, err
	}
	if driving && input.Input && !person {
		if err = w.commit(); err != nil {
			return result, err
		}
		return result, protocolError(contract.ErrorCode("person_driving"), "a person is driving")
	}
	var data []byte
	err = w.tx.QueryRowContext(ctx, `SELECT event_json FROM seat_log_events WHERE seat_id=? AND json_extract(event_json,'$.kind')='control_released' ORDER BY ordinal DESC LIMIT 1`, id).Scan(&data)
	if err == nil {
		var event SeatLogEvent
		if err = json.Unmarshal(data, &event); err != nil {
			return result, err
		}
		result.ReleaseNote = event.Note
	} else if !errors.Is(err, sql.ErrNoRows) {
		return result, err
	}
	if err = w.tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM seat_log_events e JOIN seat_logs l ON l.seat_id=e.seat_id WHERE e.seat_id=? AND e.ordinal>l.read_ordinal`, id).Scan(&result.UnreadEvents); err != nil {
		return result, err
	}
	return result, w.commit()
}

// The Seat log is the single take-over record for Seat-enabled Computers.
// Tenure and the Seat-life person hold move in the same write transaction.
func (s *Store) RecordSeatScreenEvent(ctx context.Context, identity, id string, input SeatScreenEventRequest) (receipt SeatScreenEventReceipt, resultErr error) {
	e := input.Event
	if e.SeatID != id || e.EventID == "" || e.Path != "" || e.RunID != "" || e.ExitCode != nil || e.SizeBytes != nil || e.CopyID != "" || e.Outcome != "" || e.NodeOutcome != "" {
		return receipt, protocolError(contract.ErrorInvalidRequest, "invalid Seat screen evidence")
	}
	switch e.Kind {
	case SeatEventControlTaken, SeatEventControlReleased, SeatEventTakeover, SeatEventScreenInput:
	default:
		return receipt, protocolError(contract.ErrorInvalidRequest, "invalid Seat screen event kind")
	}
	if e.Note != "" && e.Kind != SeatEventControlReleased {
		return receipt, protocolError(contract.ErrorInvalidRequest, "only release may carry a note")
	}
	if e.Kind == SeatEventScreenInput && (e.Reason != "ai_agent" && e.Reason != "person") {
		return receipt, protocolError(contract.ErrorInvalidRequest, "screen input must name its path")
	}
	if e.Audit != nil {
		if err := validateComputerTakeoverAuditRequest(input.ComputerID, input.JobID, input.AttemptID, ComputerTakeoverAuditRequest{FencingToken: input.FencingToken, Event: *e.Audit}); err != nil {
			return receipt, err
		}
	}
	w, err := s.beginWriteTransaction(ctx, nil)
	if err != nil {
		return receipt, err
	}
	defer w.rollback()
	ctx = w.ctx
	a, err := readAttemptAuthority(ctx, w.tx, input.AttemptID)
	if err != nil {
		return receipt, err
	}
	if err = validateAttemptEvidence(identity, input.JobID, input.AttemptID, input.FencingToken, a); err != nil {
		return receipt, err
	}
	if a.nodeID != input.NodeID || a.bootSessionID != input.BootSessionID {
		return receipt, protocolError(contract.ErrorForbidden, "Seat screen node mismatch")
	}
	var computer, first string
	if err = w.tx.QueryRowContext(ctx, `SELECT computer_id,COALESCE(first_attempt_id,'') FROM computer_seat_identities WHERE seat_id=?`, id).Scan(&computer, &first); err != nil {
		return receipt, err
	}
	if computer != input.ComputerID || first != input.AttemptID {
		return receipt, protocolError(contract.ErrorForbidden, "Seat screen Attempt mismatch")
	}
	var replay bool
	if err = w.tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM seat_log_events WHERE seat_id=? AND event_id=?)`, id, e.EventID).Scan(&replay); err != nil {
		return receipt, err
	}
	if e.Audit != nil {
		audit := *e.Audit
		audit.AuthorityGeneration = a.authorityGeneration
		e.Audit = &audit
	}
	if !replay && (e.Kind == SeatEventControlTaken || e.Kind == SeatEventControlReleased) {
		if e.Kind == SeatEventControlTaken {
			if err = validateComputerSeatAttempt(ctx, w.tx, w.at, identity, input.ComputerID, input.JobID, input.AttemptID, input.ComputerSeatAttemptRequest); err != nil {
				return receipt, err
			}
		}
		if err = s.seatActivityHoldTx(ctx, w.tx, w.at, id, "person", "screen", e.Kind == SeatEventControlTaken); err != nil {
			if errorCode(err) == "seat_ended" {
				if commitErr := w.commit(); commitErr != nil {
					return receipt, commitErr
				}
			}
			return receipt, err
		}
	}
	if err = writeSeatEvent(ctx, w.tx, w.at, e); err != nil {
		return receipt, err
	}
	var stored []byte
	if err = w.tx.QueryRowContext(ctx, `SELECT event_json FROM seat_log_events WHERE seat_id=? AND event_id=?`, id, e.EventID).Scan(&stored); err != nil {
		return receipt, err
	}
	if err = json.Unmarshal(stored, &receipt.Event); err != nil {
		return receipt, err
	}
	receipt.Replayed = replay
	return receipt, w.commit()
}
