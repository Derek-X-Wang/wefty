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
)

// AdminPolicy is the bounded current person-based administrator policy.
// Device evidence is deliberately retained only in immutable audit rows.
type AdminPolicy struct {
	Revision int64   `json:"revision"`
	Admins   []Admin `json:"admins"`
}

type Admin struct {
	UserID        string    `json:"user_id"`
	AddedRevision int64     `json:"added_revision"`
	AddedAt       time.Time `json:"added_at"`
}

type AdminPolicyAudit struct {
	Revision      int64                `json:"revision"`
	Operation     AdminPolicyOperation `json:"operation"`
	ActorUserID   string               `json:"actor_user_id"`
	ActorDeviceID string               `json:"actor_device_id"`
	SubjectUserID string               `json:"subject_user_id"`
	CreatedAt     time.Time            `json:"created_at"`
}

type AdminPolicyAuditList struct {
	Entries    []AdminPolicyAudit `json:"entries"`
	NextCursor string             `json:"next_cursor,omitempty"`
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
	if strings.TrimSpace(identity.UserID) == "" || strings.TrimSpace(identity.DeviceID) == "" ||
		identity.UserID != strings.TrimSpace(identity.UserID) ||
		identity.DeviceID != strings.TrimSpace(identity.DeviceID) {
		return protocolError(contract.ErrorPersonIdentityRequired,
			"fabric identity has no stable person and device IDs")
	}
	if len(identity.UserID) > 255 || len(identity.DeviceID) > 255 {
		return protocolError(contract.ErrorPersonIdentityRequired,
			"fabric person or device identity exceeds 255 bytes")
	}
	return nil
}

func validateAdminUserID(userID string) (string, error) {
	if strings.TrimSpace(userID) == "" || userID != strings.TrimSpace(userID) || len(userID) > 255 {
		return "", protocolError(contract.ErrorInvalidRequest,
			"admin user_id must contain between 1 and 255 bytes")
	}
	return userID, nil
}

func bootstrapNonceHash(nonce string) string {
	sum := sha256.Sum256([]byte(nonce))
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
	var revision, adminCount int64
	if err := tx.QueryRowContext(ctx, `SELECT revision, (SELECT COUNT(*) FROM admins)
		FROM admin_policy WHERE singleton=1`).Scan(&revision, &adminCount); err != nil {
		return AdminBootstrapChallenge{}, internalError(err, "read admin bootstrap state")
	}
	if revision != 0 || adminCount != 0 {
		return AdminBootstrapChallenge{}, protocolError(contract.ErrorAdminBootstrapClosed,
			"admin bootstrap is permanently closed")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO admin_bootstrap_challenges(
		singleton, nonce_hash, created_ns, expires_ns
	) VALUES(1, ?, ?, ?) ON CONFLICT(singleton) DO UPDATE SET
		nonce_hash=excluded.nonce_hash, created_ns=excluded.created_ns, expires_ns=excluded.expires_ns`,
		bootstrapNonceHash(nonce), now.UnixNano(), expiresAt.UnixNano()); err != nil {
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
	var revision, adminCount int64
	if err := tx.QueryRowContext(ctx, `SELECT revision, (SELECT COUNT(*) FROM admins)
		FROM admin_policy WHERE singleton=1`).Scan(&revision, &adminCount); err != nil {
		return AdminPolicy{}, internalError(err, "read admin policy")
	}
	if revision != 0 || adminCount != 0 {
		return AdminPolicy{}, protocolError(contract.ErrorAdminBootstrapClosed,
			"admin bootstrap is permanently closed")
	}
	var expiresNS int64
	challengeErr := tx.QueryRowContext(ctx, `SELECT expires_ns FROM admin_bootstrap_challenges
		WHERE singleton=1 AND nonce_hash=?`, bootstrapNonceHash(nonce)).Scan(&expiresNS)
	if errors.Is(challengeErr, sql.ErrNoRows) || challengeErr == nil && expiresNS <= now.UnixNano() {
		return AdminPolicy{}, protocolError(contract.ErrorAdminBootstrapInvalid,
			"admin bootstrap challenge is invalid or expired")
	}
	if challengeErr != nil {
		return AdminPolicy{}, internalError(challengeErr, "read admin bootstrap challenge")
	}
	result, err := tx.ExecContext(ctx, `UPDATE admin_policy SET revision=1, updated_ns=?
		WHERE singleton=1 AND revision=0`, now.UnixNano())
	if err != nil {
		return AdminPolicy{}, internalError(err, "advance bootstrapped admin policy")
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return AdminPolicy{}, internalError(err, "read bootstrapped admin policy CAS result")
	}
	if affected != 1 {
		return AdminPolicy{}, protocolError(contract.ErrorAdminBootstrapClosed,
			"admin bootstrap is permanently closed")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO admins(user_id, added_revision, added_ns)
		VALUES(?, 1, ?)`, identity.UserID, now.UnixNano()); err != nil {
		return AdminPolicy{}, internalError(err, "store first admin")
	}
	if err := insertAdminPolicyAudit(ctx, tx, 1, AdminPolicyBootstrap, identity,
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
	return policy, nil
}

func (s *Store) GetAdminPolicy(ctx context.Context) (AdminPolicy, error) {
	policy, err := readAdminPolicy(ctx, s.db)
	if err != nil {
		return AdminPolicy{}, internalError(err, "read admin policy")
	}
	return policy, nil
}

func readAdminPolicy(ctx context.Context, q queryer) (AdminPolicy, error) {
	var policy AdminPolicy
	if err := q.QueryRowContext(ctx, `SELECT revision FROM admin_policy WHERE singleton=1`).Scan(&policy.Revision); err != nil {
		return AdminPolicy{}, err
	}
	type rowsQueryer interface {
		QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	}
	rowsSource, ok := q.(rowsQueryer)
	if !ok {
		return AdminPolicy{}, fmt.Errorf("query source cannot list admins")
	}
	rows, err := rowsSource.QueryContext(ctx, `SELECT user_id, added_revision, added_ns
		FROM admins ORDER BY user_id`)
	if err != nil {
		return AdminPolicy{}, err
	}
	defer rows.Close()
	policy.Admins = []Admin{}
	for rows.Next() {
		var admin Admin
		var addedNS int64
		if err := rows.Scan(&admin.UserID, &admin.AddedRevision, &addedNS); err != nil {
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
	if err := q.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM admins WHERE user_id=?)`,
		identity.UserID).Scan(&current); err != nil {
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
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM admins WHERE user_id=?)`, userID).Scan(&exists); err != nil {
		return AdminPolicy{}, internalError(err, "read admin membership")
	}
	nextRevision := revision + 1
	switch operation {
	case AdminPolicyAdd:
		if exists {
			return AdminPolicy{}, protocolError(contract.ErrorConflict,
				"person %q is already an administrator", userID)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO admins(user_id, added_revision, added_ns)
			VALUES(?, ?, ?)`, userID, nextRevision, now.UnixNano()); err != nil {
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
		if _, err := tx.ExecContext(ctx, `DELETE FROM admins WHERE user_id=?`, userID); err != nil {
			return AdminPolicy{}, internalError(err, "remove administrator")
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
	return policy, nil
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
		revision, operation, actor_user_id, actor_device_id, subject_user_id, created_ns
	) VALUES(?, ?, ?, ?, ?, ?)`, revision, operation, identity.UserID, identity.DeviceID,
		subjectUserID, now.UnixNano()); err != nil {
		return internalError(err, "append admin policy audit")
	}
	return nil
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
	rows, err := s.db.QueryContext(ctx, `SELECT revision, operation, actor_user_id,
		actor_device_id, subject_user_id, created_ns FROM admin_policy_audit
		WHERE revision>? ORDER BY revision LIMIT ?`, afterRevision, limit+1)
	if err != nil {
		return AdminPolicyAuditList{}, internalError(err, "list admin policy audit")
	}
	defer rows.Close()
	page := AdminPolicyAuditList{Entries: []AdminPolicyAudit{}}
	for rows.Next() {
		var entry AdminPolicyAudit
		var createdNS int64
		if err := rows.Scan(&entry.Revision, &entry.Operation, &entry.ActorUserID,
			&entry.ActorDeviceID, &entry.SubjectUserID, &createdNS); err != nil {
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
