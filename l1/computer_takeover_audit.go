package l1

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

const maximumComputerTakeoverAuditFieldBytes = 255

type ComputerTakeoverAuditList struct {
	Events     []ComputerTakeoverAuditEvent `json:"events"`
	NextCursor string                       `json:"next_cursor,omitempty"`
}

type ComputerTakeoverSession struct {
	ComputerID     string                  `json:"computer_id"`
	JobID          string                  `json:"job_id"`
	AttemptID      string                  `json:"attempt_id"`
	SessionID      string                  `json:"session_id"`
	FabricID       string                  `json:"fabric_id"`
	UserID         string                  `json:"user_id"`
	DeviceID       string                  `json:"device_id"`
	AuthorizedRole ComputerGrantPermission `json:"authorized_role"`
	AdmittedMode   ComputerAdmittedMode    `json:"admitted_mode"`
	PolicyRevision int64                   `json:"policy_revision"`
	OpenedAt       time.Time               `json:"opened_at"`
	EvidenceState  string                  `json:"evidence_state"`
}

type ComputerTakeoverSessionList struct {
	Sessions            []ComputerTakeoverSession `json:"sessions"`
	ControllerSessionID string                    `json:"controller_session_id,omitempty"`
}

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
		authorized_role, admitted_mode, policy_revision, authority_generation, occurred_ns, reason, event_count, request_hash
	) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		event.AttemptID, event.EventID, event.Kind, event.ComputerID, event.JobID, event.SessionID,
		event.FabricID, event.UserID, event.DeviceID, event.AuthorizedRole, event.AdmittedMode,
		event.PolicyRevision, event.AuthorityGeneration, event.OccurredAt.UnixNano(), event.Reason, event.EventCount, eventHash)
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
		"user_id": request.Event.UserID, "device_id": request.Event.DeviceID, "reason": string(request.Event.Reason),
	} {
		if !boundedComputerAuditValue(value, false) {
			return protocolError(contract.ErrorInvalidRequest, "%s must be at most %d bytes with no surrounding whitespace", name, maximumComputerTakeoverAuditFieldBytes)
		}
	}
	switch request.Event.Kind {
	case ComputerTakeoverAdmissionDenied:
		if request.Event.SessionID != "" || request.Event.AdmittedMode != "" || request.Event.Reason == "" || request.Event.EventCount < 1 {
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
		if request.Event.EventCount != 0 {
			return protocolError(contract.ErrorInvalidRequest, "session audit cannot be a coalesced event")
		}
		if request.Event.AdmittedMode != ComputerAdmittedView && request.Event.AdmittedMode != ComputerAdmittedController {
			return protocolError(contract.ErrorInvalidRequest, "session audit admitted_mode must be view or controller")
		}
		if request.Event.Kind == ComputerTakeoverSessionOpen && request.Event.AdmittedMode != ComputerAdmittedView {
			return protocolError(contract.ErrorInvalidRequest, "every session must open in view mode")
		}
		if request.Event.Kind == ComputerTakeoverSessionClose && request.Event.Reason == "" {
			return protocolError(contract.ErrorInvalidRequest, "session_close requires a reason")
		}
		if (request.Event.Kind == ComputerTakeoverControlAcquired || request.Event.Kind == ComputerTakeoverControlReleased ||
			request.Event.Kind == ComputerTakeoverAdminOverrode) && request.Event.AdmittedMode != ComputerAdmittedController {
			return protocolError(contract.ErrorInvalidRequest, "control audit requires controller admitted mode")
		}
		if request.Event.Kind == ComputerTakeoverControlReleased && request.Event.Reason == "" {
			return protocolError(contract.ErrorInvalidRequest, "control_released requires a reason")
		}
	default:
		return protocolError(contract.ErrorInvalidRequest, "unknown Computer take-over audit event kind %q", request.Event.Kind)
	}
	if request.Event.Reason != "" && !validComputerTakeoverReason(request.Event.Reason) {
		return protocolError(contract.ErrorInvalidRequest, "unknown Computer take-over reason %q", request.Event.Reason)
	}
	return nil
}

func validComputerTakeoverReason(reason ComputerTakeoverReason) bool {
	switch reason {
	case ComputerTakeoverIdentityUnavailable, ComputerTakeoverInvalidRequestPath,
		ComputerTakeoverInvalidSubprotocol, ComputerTakeoverUnauthorizedIdentity,
		ComputerTakeoverAttemptAuthorityLost, ComputerTakeoverRevoked,
		ComputerTakeoverViewBackendUnavailable, ComputerTakeoverClientUpgradeFailed,
		ComputerTakeoverClientClosed, ComputerTakeoverViewBackendClosed,
		ComputerTakeoverRevalidationFailed, ComputerTakeoverSessionCapExpired,
		ComputerTakeoverExplicitRelease, ComputerTakeoverControllerOverridden,
		ComputerTakeoverControlBackendClosed, ComputerTakeoverControlBackendFailed:
		return true
	default:
		return false
	}
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
		authority_generation, occurred_ns, reason, event_count, request_hash
		FROM computer_takeover_audit WHERE attempt_id=? AND event_id=?`, attemptID, eventID).Scan(
		&event.EventID, &event.Kind, &event.ComputerID, &event.JobID, &event.AttemptID, &event.SessionID,
		&event.FabricID, &event.UserID, &event.DeviceID, &event.AuthorizedRole, &event.AdmittedMode,
		&event.PolicyRevision, &event.AuthorityGeneration, &occurredNS, &event.Reason, &event.EventCount, &requestHash)
	if err != nil {
		return ComputerTakeoverAuditEvent{}, "", err
	}
	event.OccurredAt = time.Unix(0, occurredNS).UTC()
	return event, requestHash, nil
}

func encodeComputerTakeoverAuditCursor(occurredNS int64, attemptID, eventID string) string {
	value := strconv.FormatInt(occurredNS, 10) + "\x00" + attemptID + "\x00" + eventID
	return base64.RawURLEncoding.EncodeToString([]byte(value))
}

func decodeComputerTakeoverAuditCursor(value string) (int64, string, string, error) {
	if value == "" {
		return 0, "", "", nil
	}
	payload, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return 0, "", "", protocolError(contract.ErrorInvalidRequest, "cursor is invalid")
	}
	parts := strings.Split(string(payload), "\x00")
	if len(parts) != 3 {
		return 0, "", "", protocolError(contract.ErrorInvalidRequest, "cursor is invalid")
	}
	occurredNS, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || occurredNS < 0 || !boundedComputerAuditValue(parts[1], true) || !boundedComputerAuditValue(parts[2], true) {
		return 0, "", "", protocolError(contract.ErrorInvalidRequest, "cursor is invalid")
	}
	return occurredNS, parts[1], parts[2], nil
}

func (s *Store) ListComputerTakeoverAudit(
	ctx context.Context,
	identity fabric.Identity,
	computerID, cursor string,
	limit int,
	tail bool,
) (ComputerTakeoverAuditList, error) {
	if err := requireCurrentAdmin(ctx, s.db, identity); err != nil {
		return ComputerTakeoverAuditList{}, err
	}
	if limit < 1 || limit > MaxJobPageLimit {
		return ComputerTakeoverAuditList{}, protocolError(contract.ErrorInvalidRequest, "limit must be between 1 and %d", MaxJobPageLimit)
	}
	if _, err := s.GetComputer(ctx, computerID); err != nil {
		return ComputerTakeoverAuditList{}, err
	}
	occurredNS, attemptID, eventID, err := decodeComputerTakeoverAuditCursor(cursor)
	if err != nil {
		return ComputerTakeoverAuditList{}, err
	}
	if tail {
		if cursor != "" {
			return ComputerTakeoverAuditList{}, protocolError(contract.ErrorInvalidRequest, "tail does not accept a cursor")
		}
		return s.listComputerTakeoverAuditTail(ctx, computerID, limit)
	}
	rows, err := s.db.QueryContext(ctx, `SELECT event_id, event_kind, computer_id, job_id, attempt_id, session_id,
		fabric_id, user_id, device_id, authorized_role, admitted_mode, policy_revision,
		authority_generation, occurred_ns, reason, event_count
		FROM computer_takeover_audit WHERE computer_id=? AND
		(occurred_ns>? OR (occurred_ns=? AND (attempt_id>? OR (attempt_id=? AND event_id>?))))
		ORDER BY occurred_ns, attempt_id, event_id LIMIT ?`,
		computerID, occurredNS, occurredNS, attemptID, attemptID, eventID, limit+1)
	if err != nil {
		return ComputerTakeoverAuditList{}, internalError(err, "list Computer take-over audit")
	}
	defer rows.Close()
	page := ComputerTakeoverAuditList{Events: []ComputerTakeoverAuditEvent{}}
	for rows.Next() {
		var event ComputerTakeoverAuditEvent
		var eventNS int64
		if err := rows.Scan(&event.EventID, &event.Kind, &event.ComputerID, &event.JobID, &event.AttemptID,
			&event.SessionID, &event.FabricID, &event.UserID, &event.DeviceID, &event.AuthorizedRole,
			&event.AdmittedMode, &event.PolicyRevision, &event.AuthorityGeneration, &eventNS,
			&event.Reason, &event.EventCount); err != nil {
			return ComputerTakeoverAuditList{}, internalError(err, "scan Computer take-over audit")
		}
		event.OccurredAt = time.Unix(0, eventNS).UTC()
		page.Events = append(page.Events, event)
	}
	if err := rows.Err(); err != nil {
		return ComputerTakeoverAuditList{}, internalError(err, "read Computer take-over audit")
	}
	if len(page.Events) > limit {
		page.Events = page.Events[:limit]
		last := page.Events[len(page.Events)-1]
		page.NextCursor = encodeComputerTakeoverAuditCursor(last.OccurredAt.UnixNano(), last.AttemptID, last.EventID)
	}
	return page, nil
}

func (s *Store) listComputerTakeoverAuditTail(ctx context.Context, computerID string, limit int) (ComputerTakeoverAuditList, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT event_id, event_kind, computer_id, job_id, attempt_id, session_id,
		fabric_id, user_id, device_id, authorized_role, admitted_mode, policy_revision,
		authority_generation, occurred_ns, reason, event_count
		FROM computer_takeover_audit WHERE computer_id=?
		ORDER BY occurred_ns DESC, rowid DESC LIMIT ?`, computerID, limit)
	if err != nil {
		return ComputerTakeoverAuditList{}, internalError(err, "tail Computer take-over audit")
	}
	defer rows.Close()
	page := ComputerTakeoverAuditList{Events: []ComputerTakeoverAuditEvent{}}
	for rows.Next() {
		var event ComputerTakeoverAuditEvent
		var eventNS int64
		if err := rows.Scan(&event.EventID, &event.Kind, &event.ComputerID, &event.JobID, &event.AttemptID,
			&event.SessionID, &event.FabricID, &event.UserID, &event.DeviceID, &event.AuthorizedRole,
			&event.AdmittedMode, &event.PolicyRevision, &event.AuthorityGeneration, &eventNS,
			&event.Reason, &event.EventCount); err != nil {
			return ComputerTakeoverAuditList{}, internalError(err, "scan Computer take-over audit tail")
		}
		event.OccurredAt = time.Unix(0, eventNS).UTC()
		page.Events = append(page.Events, event)
	}
	if err := rows.Err(); err != nil {
		return ComputerTakeoverAuditList{}, internalError(err, "read Computer take-over audit tail")
	}
	for left, right := 0, len(page.Events)-1; left < right; left, right = left+1, right-1 {
		page.Events[left], page.Events[right] = page.Events[right], page.Events[left]
	}
	return page, nil
}

func (s *Store) ListComputerTakeoverSessions(
	ctx context.Context,
	identity fabric.Identity,
	computerID string,
) (ComputerTakeoverSessionList, error) {
	if err := requireCurrentAdmin(ctx, s.db, identity); err != nil {
		return ComputerTakeoverSessionList{}, err
	}
	if _, err := s.GetComputer(ctx, computerID); err != nil {
		return ComputerTakeoverSessionList{}, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT event_kind, job_id, attempt_id, session_id, fabric_id, user_id,
		device_id, authorized_role, admitted_mode, policy_revision, occurred_ns
		FROM computer_takeover_audit WHERE computer_id=? AND session_id<>''
		ORDER BY occurred_ns, rowid`, computerID)
	if err != nil {
		return ComputerTakeoverSessionList{}, internalError(err, "list Computer take-over sessions")
	}
	defer rows.Close()
	type state struct {
		session ComputerTakeoverSession
		active  bool
	}
	states := map[string]*state{}
	controllerKey := ""
	for rows.Next() {
		var kind ComputerTakeoverAuditEventKind
		var jobID, attemptID, sessionID, fabricID, userID, deviceID string
		var role ComputerGrantPermission
		var mode ComputerAdmittedMode
		var revision, occurredNS int64
		if err := rows.Scan(&kind, &jobID, &attemptID, &sessionID, &fabricID, &userID, &deviceID,
			&role, &mode, &revision, &occurredNS); err != nil {
			return ComputerTakeoverSessionList{}, internalError(err, "scan Computer take-over sessions")
		}
		key := attemptID + "\x00" + sessionID
		switch kind {
		case ComputerTakeoverSessionOpen:
			states[key] = &state{active: true, session: ComputerTakeoverSession{
				ComputerID: computerID, JobID: jobID, AttemptID: attemptID, SessionID: sessionID,
				FabricID: fabricID, UserID: userID, DeviceID: deviceID, AuthorizedRole: role,
				AdmittedMode: ComputerAdmittedView, PolicyRevision: revision,
				OpenedAt: time.Unix(0, occurredNS).UTC(), EvidenceState: "open_without_close",
			}}
		case ComputerTakeoverSessionClose:
			if current := states[key]; current != nil {
				current.active = false
			}
			if controllerKey == key {
				controllerKey = ""
			}
		case ComputerTakeoverControlAcquired, ComputerTakeoverAdminOverrode:
			if current := states[key]; current != nil && current.active {
				if prior := states[controllerKey]; prior != nil {
					prior.session.AdmittedMode = ComputerAdmittedView
				}
				current.session.AdmittedMode = ComputerAdmittedController
				controllerKey = key
			}
		case ComputerTakeoverControlReleased:
			if current := states[key]; current != nil {
				current.session.AdmittedMode = ComputerAdmittedView
			}
			if controllerKey == key {
				controllerKey = ""
			}
		}
	}
	if err := rows.Err(); err != nil {
		return ComputerTakeoverSessionList{}, internalError(err, "read Computer take-over sessions")
	}
	result := ComputerTakeoverSessionList{Sessions: []ComputerTakeoverSession{}}
	for key, current := range states {
		if !current.active {
			continue
		}
		result.Sessions = append(result.Sessions, current.session)
		if key == controllerKey {
			result.ControllerSessionID = current.session.SessionID
		}
	}
	sort.Slice(result.Sessions, func(i, j int) bool {
		if result.Sessions[i].OpenedAt.Equal(result.Sessions[j].OpenedAt) {
			return result.Sessions[i].SessionID < result.Sessions[j].SessionID
		}
		return result.Sessions[i].OpenedAt.Before(result.Sessions[j].OpenedAt)
	})
	return result, nil
}

// pruneComputerTakeoverAudit applies the audit stream's own finite retention.
// It is deliberately independent from attempt/log-summary retention: deleting
// an attempt never deletes take-over evidence as a foreign-key side effect.
func (s *Store) pruneComputerTakeoverAudit(ctx context.Context, tx *sql.Tx, now time.Time) (int64, error) {
	cutoff := now.Add(-s.computerTakeoverAuditRetentionAge).UnixNano()
	result, err := tx.ExecContext(ctx, "DELETE FROM computer_takeover_audit WHERE occurred_ns<?", cutoff)
	if err != nil {
		return 0, internalError(err, "prune Computer take-over audit retention")
	}
	pruned, err := result.RowsAffected()
	if err != nil {
		return 0, internalError(err, "read Computer take-over audit retention result")
	}
	return pruned, nil
}

func (kind ComputerTakeoverAuditEventKind) String() string { return string(kind) }

func (event ComputerTakeoverAuditEvent) String() string {
	return fmt.Sprintf("%s:%s", event.Kind, event.EventID)
}
