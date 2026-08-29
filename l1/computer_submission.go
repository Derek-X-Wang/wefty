package l1

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

const DefaultComputerSubmitMaxInflight = 20

type ComputerSubmissionRequest struct {
	PolicyRevision       int64  `json:"policy_revision"`
	SubmitIntentRevision int64  `json:"submit_intent_revision"`
	SubmitEnabled        bool   `json:"submit_enabled"`
	SubmitMaxInflight    int    `json:"submit_max_inflight"`
	IdempotencyKey       string `json:"idempotency_key"`
}

type ComputerSubmissionAudit struct {
	ComputerID           string    `json:"computer_id"`
	SubmitIntentRevision int64     `json:"submit_intent_revision"`
	PolicyRevision       int64     `json:"policy_revision"`
	ActorFabricID        string    `json:"actor_fabric_id"`
	ActorUserID          string    `json:"actor_user_id"`
	ActorDeviceID        string    `json:"actor_device_id"`
	PreviousEnabled      bool      `json:"previous_enabled"`
	SubmitEnabled        bool      `json:"submit_enabled"`
	SubmitMaxInflight    int       `json:"submit_max_inflight"`
	IdempotencyKey       string    `json:"idempotency_key"`
	CreatedAt            time.Time `json:"created_at"`
}

type ComputerTokenRevocation struct {
	ComputerID              string `json:"computer_id"`
	NewSubmitIntentRevision int64  `json:"new_submit_intent_revision"`
	RevokeAll               bool   `json:"revoke_all,omitempty"`
	Reason                  string `json:"reason"`
}

type ComputerTokenRevoker interface {
	RevokeComputerTokens(context.Context, ComputerTokenRevocation) error
}

func validateComputerSubmissionRequest(request ComputerSubmissionRequest) error {
	if request.PolicyRevision < 1 || request.SubmitIntentRevision < 0 {
		return protocolError(contract.ErrorInvalidRequest, "policy_revision must be positive and submit_intent_revision non-negative")
	}
	if request.SubmitMaxInflight < 1 || request.SubmitMaxInflight > 1000 {
		return protocolError(contract.ErrorInvalidRequest, "submit_max_inflight must be between 1 and 1000")
	}
	if strings.TrimSpace(request.IdempotencyKey) == "" || len(request.IdempotencyKey) > 255 {
		return protocolError(contract.ErrorInvalidRequest, "idempotency_key must contain between 1 and 255 bytes")
	}
	return nil
}

func computerSubmissionRequestHash(request ComputerSubmissionRequest) (string, error) {
	payload, err := json.Marshal(struct {
		PolicyRevision       int64 `json:"policy_revision"`
		SubmitIntentRevision int64 `json:"submit_intent_revision"`
		SubmitEnabled        bool  `json:"submit_enabled"`
		SubmitMaxInflight    int   `json:"submit_max_inflight"`
	}{request.PolicyRevision, request.SubmitIntentRevision, request.SubmitEnabled, request.SubmitMaxInflight})
	if err != nil {
		return "", internalError(err, "encode Computer submission request")
	}
	hash := sha256.Sum256(payload)
	return hex.EncodeToString(hash[:]), nil
}

func (s *Store) PrepareComputerSubmissionMutation(ctx context.Context, identity fabric.Identity, computerID string, request ComputerSubmissionRequest) (Computer, bool, error) {
	if err := validateComputerSubmissionRequest(request); err != nil {
		return Computer{}, false, err
	}
	if err := requireCurrentAdmin(ctx, s.db, identity); err != nil {
		return Computer{}, false, err
	}
	requestHash, err := computerSubmissionRequestHash(request)
	if err != nil {
		return Computer{}, false, err
	}
	computer, err := readComputerAuthority(ctx, s.db, computerID, canonicalTime(s.clock.Now()))
	if errors.Is(err, sql.ErrNoRows) {
		return Computer{}, false, protocolError(contract.ErrorNotFound, "Computer %q was not found", computerID)
	}
	if err != nil {
		return Computer{}, false, internalError(err, "read Computer submission authority")
	}
	if computer.ReconfigurationPhase != ComputerReconfigurationStable {
		return Computer{}, false, protocolError(contract.ErrorConflict,
			"Computer submission authority cannot change during reconfiguration")
	}
	if computer.DesiredState == contract.ServiceDesiredRemoved {
		return Computer{}, false, protocolError(contract.ErrorConflict, "removed Computer cannot submit Runs")
	}
	var storedHash string
	if err := s.db.QueryRowContext(ctx, `SELECT request_hash FROM computer_submission_audit
		WHERE computer_id=? AND idempotency_key=?`, computerID, request.IdempotencyKey).Scan(&storedHash); err == nil {
		if storedHash != requestHash {
			return Computer{}, false, protocolError(contract.ErrorIdempotencyConflict, "Computer submission idempotency key was reused with different authority")
		}
		return computer, true, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return Computer{}, false, internalError(err, "read Computer submission replay")
	}
	if computer.SubmitIntentRevision != request.SubmitIntentRevision {
		return Computer{}, false, protocolError(contract.ErrorStalePolicyRevision, "Computer submission revision changed")
	}
	var policyRevision int64
	if err := s.db.QueryRowContext(ctx, `SELECT revision FROM admin_policy WHERE singleton=1`).Scan(&policyRevision); err != nil {
		return Computer{}, false, internalError(err, "read admin policy revision")
	}
	if err := validatePolicyRevision(policyRevision, request.PolicyRevision); err != nil {
		return Computer{}, false, err
	}
	return computer, false, nil
}

func (s *Store) MutateComputerSubmission(ctx context.Context, identity fabric.Identity, computerID string, request ComputerSubmissionRequest) (Computer, bool, error) {
	if err := validateComputerSubmissionRequest(request); err != nil {
		return Computer{}, false, err
	}
	requestHash, err := computerSubmissionRequestHash(request)
	if err != nil {
		return Computer{}, false, err
	}
	now := canonicalTime(s.clock.Now())
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Computer{}, false, internalError(err, "begin Computer submission mutation")
	}
	defer tx.Rollback()
	if err := requireCurrentAdmin(ctx, tx, identity); err != nil {
		return Computer{}, false, err
	}
	var storedHash string
	if err := tx.QueryRowContext(ctx, `SELECT request_hash FROM computer_submission_audit
		WHERE computer_id=? AND idempotency_key=?`, computerID, request.IdempotencyKey).Scan(&storedHash); err == nil {
		if storedHash != requestHash {
			return Computer{}, false, protocolError(contract.ErrorIdempotencyConflict, "Computer submission idempotency key was reused with different authority")
		}
		computer, readErr := readComputerAuthority(ctx, tx, computerID, now)
		return computer, true, readErr
	} else if !errors.Is(err, sql.ErrNoRows) {
		return Computer{}, false, internalError(err, "read Computer submission replay")
	}
	computer, err := readComputerAuthority(ctx, tx, computerID, now)
	if errors.Is(err, sql.ErrNoRows) {
		return Computer{}, false, protocolError(contract.ErrorNotFound, "Computer %q was not found", computerID)
	}
	if err != nil {
		return Computer{}, false, internalError(err, "read Computer submission authority")
	}
	if computer.SubmitIntentRevision != request.SubmitIntentRevision {
		return Computer{}, false, protocolErrorWithDetails(contract.ErrorStalePolicyRevision,
			map[string]any{"expected_revision": computer.SubmitIntentRevision, "observed_revision": request.SubmitIntentRevision},
			"Computer submission revision changed")
	}
	var policyRevision int64
	if err := tx.QueryRowContext(ctx, `SELECT revision FROM admin_policy WHERE singleton=1`).Scan(&policyRevision); err != nil {
		return Computer{}, false, internalError(err, "read admin policy revision")
	}
	if err := validatePolicyRevision(policyRevision, request.PolicyRevision); err != nil {
		return Computer{}, false, err
	}
	nextSubmitRevision, nextPolicyRevision := computer.SubmitIntentRevision+1, policyRevision+1
	result, err := tx.ExecContext(ctx, `UPDATE computers SET submit_enabled=?, submit_intent_revision=?,
		submit_max_inflight=?, submit_policy_revision=?, updated_ns=?
		WHERE computer_id=? AND submit_intent_revision=?`, request.SubmitEnabled, nextSubmitRevision,
		request.SubmitMaxInflight, nextPolicyRevision, now.UnixNano(), computerID, request.SubmitIntentRevision)
	if err != nil {
		return Computer{}, false, internalError(err, "update Computer submission authority")
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		return Computer{}, false, protocolError(contract.ErrorStalePolicyRevision, "Computer submission revision changed")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE admin_policy SET revision=?, updated_ns=? WHERE singleton=1 AND revision=?`,
		nextPolicyRevision, now.UnixNano(), policyRevision); err != nil {
		return Computer{}, false, internalError(err, "advance submission policy revision")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO computer_submission_audit(computer_id, submit_intent_revision,
		policy_revision, actor_fabric_id, actor_user_id, actor_device_id, previous_enabled, submit_enabled,
		submit_max_inflight, idempotency_key, request_hash, created_ns) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		computerID, nextSubmitRevision, nextPolicyRevision, identity.FabricID, identity.UserID, identity.DeviceID,
		computer.SubmitEnabled, request.SubmitEnabled, request.SubmitMaxInflight, request.IdempotencyKey, requestHash,
		now.UnixNano()); err != nil {
		return Computer{}, false, internalError(err, "append Computer submission audit")
	}
	computer, err = readComputerAuthority(ctx, tx, computerID, now)
	if err != nil {
		return Computer{}, false, internalError(err, "read mutated Computer submission authority")
	}
	if err := tx.Commit(); err != nil {
		return Computer{}, false, internalError(err, "commit Computer submission mutation")
	}
	s.notifyComputerPolicyChanged()
	return computer, false, nil
}

func (s *Store) ProveComputerTokenScope(ctx context.Context, computerID, attemptID, hostIdentityNodeID, hostNodeID string) (ComputerTokenScopeProof, error) {
	if computerID == "" || attemptID == "" || (hostIdentityNodeID == "") == (hostNodeID == "") {
		return ComputerTokenScopeProof{}, protocolError(contract.ErrorForbidden, "Computer token scope proof is bound to the hosting Node")
	}
	var proof ComputerTokenScopeProof
	var submitEnabled bool
	var leaseExpiresNS int64
	err := s.db.QueryRowContext(ctx, `SELECT c.computer_id, a.attempt_id, c.storage_generation,
		c.submit_intent_revision, host.identity_node_id, c.submit_max_inflight, c.submit_enabled, a.lease_expires_ns
		FROM computers c JOIN attempts a ON a.job_id=c.current_job_id
		JOIN nodes host ON host.node_id=a.node_id
		WHERE c.computer_id=? AND a.attempt_id=? AND c.current_job_id=a.job_id
		AND c.bound_node_id=a.node_id AND c.placement_node_id=a.node_id
		AND (?='' OR host.identity_node_id=?) AND (?='' OR host.node_id=?)
		AND c.desired_state<>'removed' AND c.reconfiguration_phase='stable'
		AND a.state IN ('claimed', 'running') AND EXISTS(
			SELECT 1 FROM nodes n JOIN computer_policy_installations i
			ON i.node_id=n.node_id AND i.boot_session_id=n.boot_session_id
			JOIN admin_policy p ON p.singleton=1
			WHERE n.node_id=a.node_id AND i.policy_generation=p.authority_generation
			AND i.policy_revision>=c.submit_policy_revision
		)`, computerID, attemptID, hostIdentityNodeID, hostIdentityNodeID, hostNodeID, hostNodeID).Scan(&proof.ComputerID,
		&proof.ComputerAttemptID, &proof.ComputerStorageGeneration, &proof.SubmitIntentRevision,
		&proof.HostNodeID, &proof.SubmitMaxInflight, &submitEnabled, &leaseExpiresNS)
	if errors.Is(err, sql.ErrNoRows) {
		return ComputerTokenScopeProof{}, protocolError(contract.ErrorForbidden, "Computer submission authority is not current")
	}
	if err != nil {
		return ComputerTokenScopeProof{}, internalError(err, "prove Computer token scope")
	}
	if !submitEnabled || leaseExpiresNS <= canonicalTime(s.clock.Now()).UnixNano() {
		return ComputerTokenScopeProof{}, protocolError(contract.ErrorForbidden, "Computer submission authority is not current")
	}
	return proof, nil
}
