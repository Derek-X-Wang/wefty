package l1

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

type AdminPolicyOperation string

const (
	AdminPolicyBootstrap AdminPolicyOperation = "bootstrap"
	AdminPolicyAdd       AdminPolicyOperation = "add"
	AdminPolicyRemove    AdminPolicyOperation = "remove"
	AdminPolicyReset     AdminPolicyOperation = "reset"
	MaxAdministrators                         = 32
)

type AdminPolicyActorKind string

const (
	AdminPolicyActorFabricPerson  AdminPolicyActorKind = "fabric_person"
	AdminPolicyActorLocalOperator AdminPolicyActorKind = "local_operator"
)

// AdminPolicy is the bounded current person-based administrator policy.
// Device evidence is deliberately retained only in immutable audit rows.
type AdminPolicy struct {
	Revision    int64                      `json:"revision"`
	Admins      []Admin                    `json:"admins,omitempty"`
	Revocations []ComputerPolicyRevocation `json:"revocations,omitempty"`
}

type Admin struct {
	FabricID      string    `json:"fabric_id"`
	UserID        string    `json:"user_id"`
	AddedRevision int64     `json:"added_revision"`
	AddedAt       time.Time `json:"added_at"`
}

type AdminPolicyAudit struct {
	Revision        int64                `json:"revision"`
	Operation       AdminPolicyOperation `json:"operation"`
	ActorKind       AdminPolicyActorKind `json:"actor_kind"`
	ActorFabricID   string               `json:"actor_fabric_id"`
	ActorUserID     string               `json:"actor_user_id"`
	ActorDeviceID   string               `json:"actor_device_id"`
	SubjectFabricID string               `json:"subject_fabric_id"`
	SubjectUserID   string               `json:"subject_user_id"`
	CreatedAt       time.Time            `json:"created_at"`
}

type AdminPolicyAuditList struct {
	Entries    []AdminPolicyAudit `json:"entries"`
	NextCursor string             `json:"next_cursor,omitempty"`
}

// AuthenticatedPerson is the stable person identity L1 most recently observed
// through a person-authenticated route. DeviceID remains evidence, not the key.
type AuthenticatedPerson struct {
	FabricID string    `json:"fabric_id"`
	UserID   string    `json:"user_id"`
	DeviceID string    `json:"device_id"`
	SeenAt   time.Time `json:"seen_at"`
}

// AdminBootstrapChallenge is returned only to the local process that opened
// the L1 database. The durable row stores a hash, never this bearer value.
type AdminBootstrapChallenge struct {
	Nonce     string    `json:"nonce"`
	ExpiresAt time.Time `json:"expires_at"`
}

type BootstrapAdminRequest struct {
	Nonce string `json:"nonce"`
}

type AdminPolicyMutationRequest struct {
	PolicyRevision int64 `json:"policy_revision"`
}

func validatePersonIdentity(identity fabric.Identity) error {
	if identity.Kind == fabric.IdentityKindMachine || strings.TrimSpace(identity.UserID) == "" ||
		strings.TrimSpace(identity.DeviceID) == "" || strings.TrimSpace(identity.FabricID) == "" ||
		identity.UserID != strings.TrimSpace(identity.UserID) ||
		identity.DeviceID != strings.TrimSpace(identity.DeviceID) ||
		identity.FabricID != strings.TrimSpace(identity.FabricID) {
		return protocolError(contract.ErrorPersonIdentityRequired,
			"fabric identity has no stable issuing Fabric, person, and device IDs")
	}
	if len(identity.UserID) > 255 || len(identity.DeviceID) > 255 || len(identity.FabricID) > 255 {
		return protocolError(contract.ErrorPersonIdentityRequired,
			"fabric issuer, person, or device identity exceeds 255 bytes")
	}
	return nil
}

func (s *Store) ObserveAuthenticatedPerson(ctx context.Context, identity fabric.Identity) (AuthenticatedPerson, error) {
	if err := validatePersonIdentity(identity); err != nil {
		return AuthenticatedPerson{}, err
	}
	now := canonicalTime(s.clock.Now())
	if _, err := s.db.ExecContext(ctx, `INSERT INTO authenticated_people(fabric_id, user_id, last_device_id, last_seen_ns)
		VALUES(?, ?, ?, ?) ON CONFLICT(fabric_id, user_id) DO UPDATE SET
		last_device_id=excluded.last_device_id, last_seen_ns=excluded.last_seen_ns`,
		identity.FabricID, identity.UserID, identity.DeviceID, now.UnixNano()); err != nil {
		return AuthenticatedPerson{}, internalError(err, "record authenticated person")
	}
	return AuthenticatedPerson{FabricID: identity.FabricID, UserID: identity.UserID,
		DeviceID: identity.DeviceID, SeenAt: now}, nil
}

func validateAdminUserID(userID string) (string, error) {
	if strings.TrimSpace(userID) == "" || userID != strings.TrimSpace(userID) || len(userID) > 255 {
		return "", protocolError(contract.ErrorInvalidRequest,
			"admin user_id must contain between 1 and 255 bytes")
	}
	return userID, nil
}

func bootstrapNonceHash(nonce, deploymentID string, authorityGeneration int64) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%d\x00%s", deploymentID, authorityGeneration, nonce)))
	return hex.EncodeToString(sum[:])
}

func deploymentIDHash(deploymentID string) string {
	sum := sha256.Sum256([]byte(deploymentID))
	return hex.EncodeToString(sum[:])
}

// InitiateAdminBootstrap creates or replaces the sole short-lived challenge.
// It is intentionally a Store method, not an HTTP route: callers must have
// local access to the control-plane database.
func (s *Store) InitiateAdminBootstrap(ctx context.Context) (AdminBootstrapChallenge, error) {
	now := canonicalTime(s.clock.Now())
	value := make([]byte, 32)
	if _, err := rand.Read(value); err != nil {
		return AdminBootstrapChallenge{}, internalError(err, "generate admin bootstrap nonce")
	}
	nonce := base64.RawURLEncoding.EncodeToString(value)
	expiresAt := canonicalTime(now.Add(s.adminBootstrapTTL))
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return AdminBootstrapChallenge{}, internalError(err, "begin admin bootstrap initiation")
	}
	defer tx.Rollback()
	var bootstrapOpen bool
	var authorityGeneration, adminCount int64
	if err := tx.QueryRowContext(ctx, `SELECT bootstrap_open, authority_generation,
		(SELECT COUNT(*) FROM admins) FROM admin_policy WHERE singleton=1`).
		Scan(&bootstrapOpen, &authorityGeneration, &adminCount); err != nil {
		return AdminBootstrapChallenge{}, internalError(err, "read admin bootstrap state")
	}
	if !bootstrapOpen || adminCount != 0 {
		return AdminBootstrapChallenge{}, protocolError(contract.ErrorAdminBootstrapClosed,
			"admin bootstrap is closed for the current authority generation")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO admin_bootstrap_challenges(
		singleton, nonce_hash, deployment_hash, authority_generation, created_ns, expires_ns
	) VALUES(1, ?, ?, ?, ?, ?) ON CONFLICT(singleton) DO UPDATE SET
		nonce_hash=excluded.nonce_hash, deployment_hash=excluded.deployment_hash,
		authority_generation=excluded.authority_generation,
		created_ns=excluded.created_ns, expires_ns=excluded.expires_ns`,
		bootstrapNonceHash(nonce, s.deploymentID, authorityGeneration), deploymentIDHash(s.deploymentID),
		authorityGeneration, now.UnixNano(), expiresAt.UnixNano()); err != nil {
		return AdminBootstrapChallenge{}, internalError(err, "store admin bootstrap challenge")
	}
	if err := tx.Commit(); err != nil {
		return AdminBootstrapChallenge{}, internalError(err, "commit admin bootstrap initiation")
	}
	return AdminBootstrapChallenge{Nonce: nonce, ExpiresAt: expiresAt}, nil
}

// BootstrapAdmin consumes one locally initiated challenge and records the
// WhoIs-authenticated person as the first durable administrator.
func (s *Store) BootstrapAdmin(ctx context.Context, identity fabric.Identity, nonce string) (AdminPolicy, error) {
	if err := validatePersonIdentity(identity); err != nil {
		return AdminPolicy{}, err
	}
	nonce = strings.TrimSpace(nonce)
	if nonce == "" {
		return AdminPolicy{}, protocolError(contract.ErrorInvalidRequest, "bootstrap nonce is required")
	}
	now := canonicalTime(s.clock.Now())
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return AdminPolicy{}, internalError(err, "begin admin bootstrap")
	}
	defer tx.Rollback()
	var revision, authorityGeneration, adminCount int64
	var bootstrapOpen bool
	if err := tx.QueryRowContext(ctx, `SELECT revision, bootstrap_open, authority_generation,
		(SELECT COUNT(*) FROM admins) FROM admin_policy WHERE singleton=1`).
		Scan(&revision, &bootstrapOpen, &authorityGeneration, &adminCount); err != nil {
		return AdminPolicy{}, internalError(err, "read admin policy")
	}
	if !bootstrapOpen || adminCount != 0 {
		return AdminPolicy{}, protocolError(contract.ErrorAdminBootstrapClosed,
			"admin bootstrap is closed for the current authority generation")
	}
	var expiresNS int64
	challengeErr := tx.QueryRowContext(ctx, `SELECT expires_ns FROM admin_bootstrap_challenges
		WHERE singleton=1 AND nonce_hash=? AND deployment_hash=? AND authority_generation=?`,
		bootstrapNonceHash(nonce, s.deploymentID, authorityGeneration), deploymentIDHash(s.deploymentID),
		authorityGeneration).Scan(&expiresNS)
	if errors.Is(challengeErr, sql.ErrNoRows) || challengeErr == nil && expiresNS <= now.UnixNano() {
		return AdminPolicy{}, protocolError(contract.ErrorAdminBootstrapInvalid,
			"admin bootstrap challenge is invalid or expired")
	}
	if challengeErr != nil {
		return AdminPolicy{}, internalError(challengeErr, "read admin bootstrap challenge")
	}
	nextRevision := revision + 1
	result, err := tx.ExecContext(ctx, `UPDATE admin_policy
		SET revision=?, bootstrap_open=0, updated_ns=?
		WHERE singleton=1 AND revision=? AND bootstrap_open=1 AND authority_generation=?`,
		nextRevision, now.UnixNano(), revision, authorityGeneration)
	if err != nil {
		return AdminPolicy{}, internalError(err, "advance bootstrapped admin policy")
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return AdminPolicy{}, internalError(err, "read bootstrapped admin policy CAS result")
	}
	if affected != 1 {
		return AdminPolicy{}, protocolError(contract.ErrorAdminBootstrapClosed,
			"admin bootstrap is closed for the current authority generation")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO admins(fabric_id, user_id, added_revision, added_ns)
		VALUES(?, ?, ?, ?)`, identity.FabricID, identity.UserID, nextRevision, now.UnixNano()); err != nil {
		return AdminPolicy{}, internalError(err, "store first admin")
	}
	if err := insertAdminPolicyAudit(ctx, tx, nextRevision, AdminPolicyBootstrap, identity,
		identity.UserID, now); err != nil {
		return AdminPolicy{}, err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM admin_bootstrap_challenges WHERE singleton=1`); err != nil {
		return AdminPolicy{}, internalError(err, "consume admin bootstrap challenge")
	}
	policy, err := readAdminPolicy(ctx, tx)
	if err != nil {
		return AdminPolicy{}, internalError(err, "read bootstrapped admin policy")
	}
	if err := tx.Commit(); err != nil {
		return AdminPolicy{}, internalError(err, "commit admin bootstrap")
	}
	s.notifyComputerPolicyChanged()
	return policy, nil
}

func (s *Store) GetAdminPolicy(ctx context.Context) (AdminPolicy, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return AdminPolicy{}, internalError(err, "begin admin policy read")
	}
	defer tx.Rollback()
	policy, err := readAdminPolicy(ctx, tx)
	if err != nil {
		return AdminPolicy{}, internalError(err, "read admin policy")
	}
	if err := tx.Commit(); err != nil {
		return AdminPolicy{}, internalError(err, "commit admin policy read")
	}
	return policy, nil
}

// GetVisibleAdminPolicy returns the roster only to a current administrator;
// every authenticated person may observe the revision needed for change
// detection without learning membership.
func (s *Store) GetVisibleAdminPolicy(ctx context.Context, identity fabric.Identity) (AdminPolicy, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return AdminPolicy{}, internalError(err, "begin visible admin policy read")
	}
	defer tx.Rollback()
	var revision int64
	if err := tx.QueryRowContext(ctx, `SELECT revision FROM admin_policy WHERE singleton=1`).Scan(&revision); err != nil {
		return AdminPolicy{}, internalError(err, "read admin policy revision")
	}
	if err := requireCurrentAdmin(ctx, tx, identity); err != nil {
		if errorCode(err) == contract.ErrorAdminRequired {
			if err := tx.Commit(); err != nil {
				return AdminPolicy{}, internalError(err, "commit redacted admin policy read")
			}
			return AdminPolicy{Revision: revision}, nil
		}
		return AdminPolicy{}, err
	}
	policy, err := readAdminPolicy(ctx, tx)
	if err != nil {
		return AdminPolicy{}, internalError(err, "read visible admin policy")
	}
	if err := tx.Commit(); err != nil {
		return AdminPolicy{}, internalError(err, "commit visible admin policy read")
	}
	return policy, nil
}

func readAdminPolicy(ctx context.Context, q queryer) (AdminPolicy, error) {
	var policy AdminPolicy
	if err := q.QueryRowContext(ctx, `SELECT revision FROM admin_policy WHERE singleton=1`).Scan(&policy.Revision); err != nil {
		return AdminPolicy{}, err
	}
	rows, err := q.QueryContext(ctx, `SELECT fabric_id, user_id, added_revision, added_ns
		FROM admins ORDER BY fabric_id, user_id`)
	if err != nil {
		return AdminPolicy{}, err
	}
	defer rows.Close()
	policy.Admins = []Admin{}
	for rows.Next() {
		var admin Admin
		var addedNS int64
		if err := rows.Scan(&admin.FabricID, &admin.UserID, &admin.AddedRevision, &addedNS); err != nil {
			return AdminPolicy{}, err
		}
		admin.AddedAt = time.Unix(0, addedNS).UTC()
		policy.Admins = append(policy.Admins, admin)
	}
	return policy, rows.Err()
}

func requireCurrentAdmin(ctx context.Context, q queryer, identity fabric.Identity) error {
	if err := validatePersonIdentity(identity); err != nil {
		return err
	}
	var current bool
	if err := q.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM admins WHERE fabric_id=? AND user_id=?)`,
		identity.FabricID, identity.UserID).Scan(&current); err != nil {
		return internalError(err, "read current admin")
	}
	if !current {
		return protocolError(contract.ErrorAdminRequired,
			"person %q is not a current administrator", identity.UserID)
	}
	return nil
}

func validatePolicyRevision(current, observed int64) error {
	if observed < 1 {
		return protocolError(contract.ErrorInvalidRequest, "policy_revision must be positive")
	}
	if current != observed {
		return protocolErrorWithDetails(contract.ErrorStalePolicyRevision,
			map[string]any{"expected_revision": current, "observed_revision": observed},
			"admin policy revision changed from %d to %d", observed, current)
	}
	return nil
}

func (s *Store) AddAdmin(
	ctx context.Context,
	identity fabric.Identity,
	userID string,
	observedRevision int64,
) (AdminPolicy, error) {
	userID, err := validateAdminUserID(userID)
	if err != nil {
		return AdminPolicy{}, err
	}
	return s.mutateAdmin(ctx, identity, userID, observedRevision, AdminPolicyAdd)
}

func (s *Store) RemoveAdmin(
	ctx context.Context,
	identity fabric.Identity,
	userID string,
	observedRevision int64,
) (AdminPolicy, error) {
	userID, err := validateAdminUserID(userID)
	if err != nil {
		return AdminPolicy{}, err
	}
	return s.mutateAdmin(ctx, identity, userID, observedRevision, AdminPolicyRemove)
}

func (s *Store) mutateAdmin(
	ctx context.Context,
	identity fabric.Identity,
	userID string,
	observedRevision int64,
	operation AdminPolicyOperation,
) (AdminPolicy, error) {
	now := canonicalTime(s.clock.Now())
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return AdminPolicy{}, internalError(err, "begin admin policy mutation")
	}
	defer tx.Rollback()
	if err := requireCurrentAdmin(ctx, tx, identity); err != nil {
		return AdminPolicy{}, err
	}
	var revision int64
	if err := tx.QueryRowContext(ctx, `SELECT revision FROM admin_policy WHERE singleton=1`).Scan(&revision); err != nil {
		return AdminPolicy{}, internalError(err, "read admin policy revision")
	}
	if err := validatePolicyRevision(revision, observedRevision); err != nil {
		return AdminPolicy{}, err
	}
	var exists bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM admins WHERE fabric_id=? AND user_id=?)`,
		identity.FabricID, userID).Scan(&exists); err != nil {
		return AdminPolicy{}, internalError(err, "read admin membership")
	}
	nextRevision := revision + 1
	var revocations []ComputerPolicyRevocation
	switch operation {
	case AdminPolicyAdd:
		if exists {
			return AdminPolicy{}, protocolError(contract.ErrorConflict,
				"person %q is already an administrator", userID)
		}
		var count int64
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM admins`).Scan(&count); err != nil {
			return AdminPolicy{}, internalError(err, "count administrators")
		}
		if count >= MaxAdministrators {
			return AdminPolicy{}, protocolError(contract.ErrorCapacityExhausted,
				"admin policy is limited to %d members", MaxAdministrators)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO admins(fabric_id, user_id, added_revision, added_ns)
			VALUES(?, ?, ?, ?)`, identity.FabricID, userID, nextRevision, now.UnixNano()); err != nil {
			return AdminPolicy{}, internalError(err, "add administrator")
		}
	case AdminPolicyRemove:
		if !exists {
			return AdminPolicy{}, protocolError(contract.ErrorConflict,
				"person %q is not an administrator", userID)
		}
		var count int64
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM admins`).Scan(&count); err != nil {
			return AdminPolicy{}, internalError(err, "count administrators")
		}
		if count == 1 {
			return AdminPolicy{}, protocolError(contract.ErrorFinalAdmin,
				"the final administrator cannot be removed")
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM admins WHERE fabric_id=? AND user_id=?`,
			identity.FabricID, userID); err != nil {
			return AdminPolicy{}, internalError(err, "remove administrator")
		}
		revocations, err = revokePersonAcrossComputers(ctx, tx, identity.FabricID, userID, nextRevision,
			ComputerPolicyAuditAdminRemove, AdminPolicyActorFabricPerson, identity, now)
		if err != nil {
			return AdminPolicy{}, err
		}
	default:
		return AdminPolicy{}, internalError(fmt.Errorf("unknown operation %q", operation),
			"mutate admin policy")
	}
	result, err := tx.ExecContext(ctx, `UPDATE admin_policy SET revision=?, updated_ns=?
		WHERE singleton=1 AND revision=?`, nextRevision, now.UnixNano(), observedRevision)
	if err != nil {
		return AdminPolicy{}, internalError(err, "advance admin policy revision")
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return AdminPolicy{}, internalError(err, "read admin policy CAS result")
	}
	if affected != 1 {
		return AdminPolicy{}, protocolError(contract.ErrorStalePolicyRevision,
			"admin policy revision changed from %d", observedRevision)
	}
	if err := insertAdminPolicyAudit(ctx, tx, nextRevision, operation, identity, userID, now); err != nil {
		return AdminPolicy{}, err
	}
	policy, err := readAdminPolicy(ctx, tx)
	if err != nil {
		return AdminPolicy{}, internalError(err, "read mutated admin policy")
	}
	if err := tx.Commit(); err != nil {
		return AdminPolicy{}, internalError(err, "commit admin policy mutation")
	}
	policy.Revocations = revocations
	s.notifyComputerPolicyChanged()
	return policy, nil
}

func revokePersonAcrossComputers(
	ctx context.Context,
	tx *sql.Tx,
	fabricID, userID string,
	revision int64,
	operation ComputerPolicyAuditOperation,
	actorKind AdminPolicyActorKind,
	actor fabric.Identity,
	now time.Time,
) ([]ComputerPolicyRevocation, error) {
	rows, err := tx.QueryContext(ctx, `SELECT computer_id FROM computers WHERE desired_state<>'removed' ORDER BY computer_id`)
	if err != nil {
		return nil, internalError(err, "list Computers for administrator revocation")
	}
	var computerIDs []string
	for rows.Next() {
		var computerID string
		if err := rows.Scan(&computerID); err != nil {
			rows.Close()
			return nil, internalError(err, "scan Computer for administrator revocation")
		}
		computerIDs = append(computerIDs, computerID)
	}
	if err := rows.Close(); err != nil {
		return nil, internalError(err, "close administrator revocation Computers")
	}
	revocations := make([]ComputerPolicyRevocation, 0, len(computerIDs))
	for _, computerID := range computerIDs {
		previous := ComputerGrantNone
		var previousValue string
		if err := tx.QueryRowContext(ctx, `SELECT permission FROM computer_grants
			WHERE computer_id=? AND fabric_id=? AND user_id=?`, computerID, fabricID, userID).Scan(&previousValue); err == nil {
			previous = ComputerGrantPermission(previousValue)
		} else if !errors.Is(err, sql.ErrNoRows) {
			return nil, internalError(err, "read administrator removal prior Computer grant")
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO computer_grants(
			computer_id, fabric_id, user_id, permission, policy_revision, updated_ns
		) VALUES(?, ?, ?, 'none', ?, ?) ON CONFLICT(computer_id, fabric_id, user_id) DO UPDATE SET
			permission='none', policy_revision=excluded.policy_revision, updated_ns=excluded.updated_ns`,
			computerID, fabricID, userID, revision, now.UnixNano()); err != nil {
			return nil, internalError(err, "store administrator removal Computer denial")
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO computer_policy_audit(
			policy_revision, computer_id, operation, actor_kind, actor_fabric_id, actor_user_id, actor_device_id,
			subject_fabric_id, subject_user_id, previous_permission, permission, idempotency_key, request_hash, created_ns
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'none', '', '', ?)`, revision, computerID, operation,
			actorKind, actor.FabricID, actor.UserID, actor.DeviceID, fabricID, userID, previous, now.UnixNano()); err != nil {
			return nil, internalError(err, "append administrator revocation audit")
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO computer_policy_revocations(
			policy_revision, computer_id, subject_fabric_id, subject_user_id, target_permission, created_ns
		) VALUES(?, ?, ?, ?, 'none', ?)`, revision, computerID, fabricID, userID, now.UnixNano()); err != nil {
			return nil, internalError(err, "record administrator policy revocation")
		}
		revocations = append(revocations, ComputerPolicyRevocation{PolicyRevision: revision, ComputerID: computerID,
			SubjectFabricID: fabricID, SubjectUserID: userID, TargetPermission: ComputerGrantNone,
			State: ComputerPolicyRevocationPending, CreatedAt: now})
	}
	return revocations, nil
}

func insertAdminPolicyAudit(
	ctx context.Context,
	tx *sql.Tx,
	revision int64,
	operation AdminPolicyOperation,
	identity fabric.Identity,
	subjectUserID string,
	now time.Time,
) error {
	if _, err := tx.ExecContext(ctx, `INSERT INTO admin_policy_audit(
		revision, operation, actor_kind, actor_fabric_id, actor_user_id, actor_device_id,
		subject_fabric_id, subject_user_id, created_ns
	) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)`, revision, operation, AdminPolicyActorFabricPerson,
		identity.FabricID, identity.UserID, identity.DeviceID, identity.FabricID,
		subjectUserID, now.UnixNano()); err != nil {
		return internalError(err, "append admin policy audit")
	}
	return nil
}

// ResetAdminPolicy is a local recovery operation for an unusable roster. It
// advances the durable policy and authority generation, clears membership and
// live challenges, reopens bootstrap, and appends an audit row atomically.
func (s *Store) ResetAdminPolicy(ctx context.Context) (AdminPolicy, error) {
	now := canonicalTime(s.clock.Now())
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return AdminPolicy{}, internalError(err, "begin admin policy reset")
	}
	defer tx.Rollback()
	var revision, authorityGeneration int64
	if err := tx.QueryRowContext(ctx, `SELECT revision, authority_generation
		FROM admin_policy WHERE singleton=1`).Scan(&revision, &authorityGeneration); err != nil {
		return AdminPolicy{}, internalError(err, "read admin policy reset state")
	}
	nextRevision := revision + 1
	adminRows, err := tx.QueryContext(ctx, `SELECT fabric_id, user_id FROM admins
		UNION SELECT DISTINCT fabric_id, user_id FROM computer_grants ORDER BY fabric_id, user_id`)
	if err != nil {
		return AdminPolicy{}, internalError(err, "list administrators for policy reset")
	}
	var resetSubjects []ComputerPolicyAdmin
	for adminRows.Next() {
		var admin ComputerPolicyAdmin
		if err := adminRows.Scan(&admin.FabricID, &admin.UserID); err != nil {
			adminRows.Close()
			return AdminPolicy{}, internalError(err, "scan administrator for policy reset")
		}
		resetSubjects = append(resetSubjects, admin)
	}
	if err := adminRows.Close(); err != nil {
		return AdminPolicy{}, internalError(err, "close policy reset administrators")
	}
	var revocations []ComputerPolicyRevocation
	for _, admin := range resetSubjects {
		entries, revokeErr := revokePersonAcrossComputers(ctx, tx, admin.FabricID, admin.UserID, nextRevision,
			ComputerPolicyAuditAdminReset, AdminPolicyActorLocalOperator, fabric.Identity{}, now)
		if revokeErr != nil {
			return AdminPolicy{}, revokeErr
		}
		revocations = append(revocations, entries...)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM admins`); err != nil {
		return AdminPolicy{}, internalError(err, "clear administrators")
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM admin_bootstrap_challenges`); err != nil {
		return AdminPolicy{}, internalError(err, "clear admin bootstrap challenge")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE admin_policy SET revision=?, bootstrap_open=1,
		authority_generation=?, updated_ns=? WHERE singleton=1 AND revision=?`,
		nextRevision, authorityGeneration+1, now.UnixNano(), revision); err != nil {
		return AdminPolicy{}, internalError(err, "advance reset admin policy")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO admin_policy_audit(
		revision, operation, actor_kind, actor_fabric_id, actor_user_id, actor_device_id,
		subject_fabric_id, subject_user_id, created_ns
	) VALUES(?, ?, ?, '', '', '', '', '', ?)`, nextRevision, AdminPolicyReset,
		AdminPolicyActorLocalOperator, now.UnixNano()); err != nil {
		return AdminPolicy{}, internalError(err, "append admin policy reset audit")
	}
	policy, err := readAdminPolicy(ctx, tx)
	if err != nil {
		return AdminPolicy{}, internalError(err, "read reset admin policy")
	}
	if err := tx.Commit(); err != nil {
		return AdminPolicy{}, internalError(err, "commit admin policy reset")
	}
	policy.Revocations = revocations
	s.notifyComputerPolicyChanged()
	return policy, nil
}

func encodeAdminAuditCursor(revision int64) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.FormatInt(revision, 10)))
}

func decodeAdminAuditCursor(value string) (int64, error) {
	if value == "" {
		return 0, nil
	}
	payload, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return 0, protocolError(contract.ErrorInvalidRequest, "cursor is invalid")
	}
	revision, err := strconv.ParseInt(string(payload), 10, 64)
	if err != nil || revision < 0 {
		return 0, protocolError(contract.ErrorInvalidRequest, "cursor is invalid")
	}
	return revision, nil
}

func (s *Store) ListAdminPolicyAudit(
	ctx context.Context,
	identity fabric.Identity,
	cursorValue string,
	limit int,
) (AdminPolicyAuditList, error) {
	if limit < 1 || limit > MaxJobPageLimit {
		return AdminPolicyAuditList{}, protocolError(contract.ErrorInvalidRequest,
			"limit must be between 1 and %d", MaxJobPageLimit)
	}
	afterRevision, err := decodeAdminAuditCursor(cursorValue)
	if err != nil {
		return AdminPolicyAuditList{}, err
	}
	if err := requireCurrentAdmin(ctx, s.db, identity); err != nil {
		return AdminPolicyAuditList{}, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT revision, operation, actor_kind, actor_fabric_id,
		actor_user_id, actor_device_id, subject_fabric_id, subject_user_id, created_ns FROM admin_policy_audit
		WHERE revision>? ORDER BY revision LIMIT ?`, afterRevision, limit+1)
	if err != nil {
		return AdminPolicyAuditList{}, internalError(err, "list admin policy audit")
	}
	defer rows.Close()
	page := AdminPolicyAuditList{Entries: []AdminPolicyAudit{}}
	for rows.Next() {
		var entry AdminPolicyAudit
		var createdNS int64
		if err := rows.Scan(&entry.Revision, &entry.Operation, &entry.ActorKind,
			&entry.ActorFabricID, &entry.ActorUserID, &entry.ActorDeviceID,
			&entry.SubjectFabricID, &entry.SubjectUserID, &createdNS); err != nil {
			return AdminPolicyAuditList{}, internalError(err, "scan admin policy audit")
		}
		entry.CreatedAt = time.Unix(0, createdNS).UTC()
		page.Entries = append(page.Entries, entry)
	}
	if err := rows.Err(); err != nil {
		return AdminPolicyAuditList{}, internalError(err, "read admin policy audit")
	}
	if len(page.Entries) > limit {
		page.Entries = page.Entries[:limit]
		page.NextCursor = encodeAdminAuditCursor(page.Entries[len(page.Entries)-1].Revision)
	}
	return page, nil
}
