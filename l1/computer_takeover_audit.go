package l1

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

const maximumComputerTakeoverAuditFieldBytes = 255

// AppendComputerTakeoverAudit durably records one immutable, attempt-fenced
// take-over event. Replays are accepted only when the complete privacy-safe
// event is byte-for-byte equivalent after canonical timestamp normalization.
func (s *Store) AppendComputerTakeoverAudit(
	ctx context.Context,
	identityNodeID, computerID, jobID, attemptID string,
	request ComputerTakeoverAuditRequest,
) (ComputerTakeoverAuditReceipt, error) {
	request.Event.OccurredAt = canonicalTime(request.Event.OccurredAt)
	if err := validateComputerTakeoverAuditRequest(computerID, jobID, attemptID, request); err != nil {
		return ComputerTakeoverAuditReceipt{}, err
	}
	eventHash, err := computerTakeoverAuditHash(request.Event)
	if err != nil {
		return ComputerTakeoverAuditReceipt{}, internalError(err, "hash Computer take-over audit event")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ComputerTakeoverAuditReceipt{}, internalError(err, "begin Computer take-over audit append")
	}
	defer tx.Rollback()
	attempt, err := readAttemptAuthority(ctx, tx, attemptID)
	if err != nil {
		return ComputerTakeoverAuditReceipt{}, err
	}
	if err := validateAttemptEvidence(identityNodeID, jobID, attemptID, request.FencingToken, attempt); err != nil {
		return ComputerTakeoverAuditReceipt{}, err
	}
	if attempt.spec.Kind != contract.JobKindOCI || attempt.spec.Execution.OCI == nil || attempt.spec.Execution.OCI.Computer == nil {
		return ComputerTakeoverAuditReceipt{}, protocolError(contract.ErrorConflict, "take-over audit is applicable only to a Computer attempt")
	}
	var projectedComputerID string
	if err := tx.QueryRowContext(ctx, `SELECT computer_id FROM computer_job_projections WHERE job_id=?`, jobID).Scan(&projectedComputerID); errors.Is(err, sql.ErrNoRows) {
		return ComputerTakeoverAuditReceipt{}, protocolError(contract.ErrorNotFound, "Computer projection for job %q was not found", jobID)
	} else if err != nil {
		return ComputerTakeoverAuditReceipt{}, internalError(err, "read Computer take-over projection")
	}
	if projectedComputerID != computerID {
		return ComputerTakeoverAuditReceipt{}, protocolError(contract.ErrorAttemptMismatch, "Computer does not match the attempt projection")
	}

	stored, storedHash, readErr := readComputerTakeoverAudit(ctx, tx, attemptID, request.Event.EventID)
	if readErr == nil {
		if storedHash != eventHash {
			return ComputerTakeoverAuditReceipt{}, protocolError(contract.ErrorIdempotencyConflict,
				"Computer take-over audit event %q conflicts with its prior upload", request.Event.EventID)
		}
		if err := tx.Commit(); err != nil {
			return ComputerTakeoverAuditReceipt{}, internalError(err, "commit Computer take-over audit replay")
		}
		return ComputerTakeoverAuditReceipt{Event: stored, Replayed: true}, nil
	}
	if !errors.Is(readErr, sql.ErrNoRows) {
		return ComputerTakeoverAuditReceipt{}, internalError(readErr, "read Computer take-over audit replay")
	}

	event := request.Event
	event.AuthorityGeneration = attempt.authorityGeneration
	_, err = tx.ExecContext(ctx, `INSERT INTO computer_takeover_audit(
		attempt_id, event_id, event_kind, computer_id, job_id, session_id, fabric_id, user_id, device_id,
		authorized_role, admitted_mode, policy_revision, authority_generation, occurred_ns, reason, request_hash
	) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		event.AttemptID, event.EventID, event.Kind, event.ComputerID, event.JobID, event.SessionID,
		event.FabricID, event.UserID, event.DeviceID, event.AuthorizedRole, event.AdmittedMode,
		event.PolicyRevision, event.AuthorityGeneration, event.OccurredAt.UnixNano(), event.Reason, eventHash)
	if err != nil {
		return ComputerTakeoverAuditReceipt{}, internalError(err, "append Computer take-over audit event")
	}
	if err := tx.Commit(); err != nil {
		return ComputerTakeoverAuditReceipt{}, internalError(err, "commit Computer take-over audit event")
	}
	return ComputerTakeoverAuditReceipt{Event: event}, nil
}

func validateComputerTakeoverAuditRequest(computerID, jobID, attemptID string, request ComputerTakeoverAuditRequest) error {
	if request.FencingToken == "" {
		return protocolError(contract.ErrorInvalidRequest, "fencing_token is required")
	}
	if request.Event.AuthorityGeneration != 0 {
		return protocolError(contract.ErrorInvalidRequest, "authority_generation is derived by L1")
	}
	for name, value := range map[string]string{
		"computer_id": computerID, "job_id": jobID, "attempt_id": attemptID, "event_id": request.Event.EventID,
	} {
		if !boundedComputerAuditValue(value, true) {
			return protocolError(contract.ErrorInvalidRequest, "%s must contain between 1 and %d bytes", name, maximumComputerTakeoverAuditFieldBytes)
		}
	}
	if request.Event.ComputerID != computerID || request.Event.JobID != jobID || request.Event.AttemptID != attemptID {
		return protocolError(contract.ErrorAttemptMismatch, "take-over audit event does not match the request path")
	}
	if request.Event.OccurredAt.IsZero() {
		return protocolError(contract.ErrorInvalidRequest, "occurred_at is required")
	}
	for name, value := range map[string]string{
		"session_id": request.Event.SessionID, "fabric_id": request.Event.FabricID,
		"user_id": request.Event.UserID, "device_id": request.Event.DeviceID, "reason": request.Event.Reason,
	} {
		if !boundedComputerAuditValue(value, false) {
			return protocolError(contract.ErrorInvalidRequest, "%s must be at most %d bytes with no surrounding whitespace", name, maximumComputerTakeoverAuditFieldBytes)
		}
	}
	switch request.Event.Kind {
	case ComputerTakeoverAdmissionDenied:
		if request.Event.SessionID != "" || request.Event.AdmittedMode != "" || request.Event.Reason == "" {
			return protocolError(contract.ErrorInvalidRequest, "admission_denied requires a reason and no session or admitted mode")
		}
	case ComputerTakeoverSessionOpen, ComputerTakeoverSessionClose,
		ComputerTakeoverControlAcquired, ComputerTakeoverControlReleased, ComputerTakeoverAdminOverrode:
		if !boundedComputerAuditValue(request.Event.SessionID, true) ||
			!boundedComputerAuditValue(request.Event.FabricID, true) ||
			!boundedComputerAuditValue(request.Event.UserID, true) ||
			!boundedComputerAuditValue(request.Event.DeviceID, true) || request.Event.PolicyRevision < 1 {
			return protocolError(contract.ErrorInvalidRequest, "session audit requires session, person, device, and positive policy revision")
		}
		if request.Event.AuthorizedRole != ComputerGrantView && request.Event.AuthorizedRole != ComputerGrantControl {
			return protocolError(contract.ErrorInvalidRequest, "session audit authorized_role must be view or control")
		}
		if request.Event.AdmittedMode != "view" && request.Event.AdmittedMode != "controller" {
			return protocolError(contract.ErrorInvalidRequest, "session audit admitted_mode must be view or controller")
		}
		if request.Event.Kind == ComputerTakeoverSessionOpen && request.Event.AdmittedMode != "view" {
			return protocolError(contract.ErrorInvalidRequest, "every session must open in view mode")
		}
		if request.Event.Kind == ComputerTakeoverSessionClose && request.Event.Reason == "" {
			return protocolError(contract.ErrorInvalidRequest, "session_close requires a reason")
		}
	default:
		return protocolError(contract.ErrorInvalidRequest, "unknown Computer take-over audit event kind %q", request.Event.Kind)
	}
	return nil
}

func boundedComputerAuditValue(value string, required bool) bool {
	if required && value == "" {
		return false
	}
	return len(value) <= maximumComputerTakeoverAuditFieldBytes && value == strings.TrimSpace(value)
}

func computerTakeoverAuditHash(event ComputerTakeoverAuditEvent) (string, error) {
	event.AuthorityGeneration = 0
	payload, err := json.Marshal(event)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

func readComputerTakeoverAudit(ctx context.Context, q queryer, attemptID, eventID string) (ComputerTakeoverAuditEvent, string, error) {
	var event ComputerTakeoverAuditEvent
	var occurredNS int64
	var requestHash string
	err := q.QueryRowContext(ctx, `SELECT event_id, event_kind, computer_id, job_id, attempt_id, session_id,
		fabric_id, user_id, device_id, authorized_role, admitted_mode, policy_revision,
		authority_generation, occurred_ns, reason, request_hash
		FROM computer_takeover_audit WHERE attempt_id=? AND event_id=?`, attemptID, eventID).Scan(
		&event.EventID, &event.Kind, &event.ComputerID, &event.JobID, &event.AttemptID, &event.SessionID,
		&event.FabricID, &event.UserID, &event.DeviceID, &event.AuthorizedRole, &event.AdmittedMode,
		&event.PolicyRevision, &event.AuthorityGeneration, &occurredNS, &event.Reason, &requestHash)
	if err != nil {
		return ComputerTakeoverAuditEvent{}, "", err
	}
	event.OccurredAt = time.Unix(0, occurredNS).UTC()
	return event, requestHash, nil
}

func (kind ComputerTakeoverAuditEventKind) String() string { return string(kind) }

func (event ComputerTakeoverAuditEvent) String() string {
	return fmt.Sprintf("%s:%s", event.Kind, event.EventID)
}
