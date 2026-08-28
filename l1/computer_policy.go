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
	"strconv"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

const (
	DefaultComputerPolicyFreshness = 15 * time.Second
	DefaultComputerPolicyWatchWait = 5 * time.Second
)

type ComputerPolicyRevocationState string

const (
	ComputerPolicyRevocationPending   ComputerPolicyRevocationState = "pending"
	ComputerPolicyRevocationCompleted ComputerPolicyRevocationState = "completed"
)

type ComputerGrantMutationRequest struct {
	PolicyRevision int64                   `json:"policy_revision"`
	Permission     ComputerGrantPermission `json:"permission"`
	IdempotencyKey string                  `json:"idempotency_key"`
}

type ComputerGrantMutationResult struct {
	Grant      ComputerGrant             `json:"grant"`
	Replayed   bool                      `json:"replayed"`
	Revocation *ComputerPolicyRevocation `json:"revocation,omitempty"`
}

type ComputerGrantList struct {
	PolicyRevision int64           `json:"policy_revision"`
	Grants         []ComputerGrant `json:"grants"`
}

type ComputerPolicyAuditOperation string

const (
	ComputerPolicyAuditGrant       ComputerPolicyAuditOperation = "grant"
	ComputerPolicyAuditAdminRemove ComputerPolicyAuditOperation = "admin_remove"
	ComputerPolicyAuditAdminReset  ComputerPolicyAuditOperation = "admin_reset"
)

type ComputerPolicyAudit struct {
	PolicyRevision     int64                        `json:"policy_revision"`
	ComputerID         string                       `json:"computer_id"`
	Operation          ComputerPolicyAuditOperation `json:"operation"`
	ActorKind          AdminPolicyActorKind         `json:"actor_kind"`
	ActorFabricID      string                       `json:"actor_fabric_id"`
	ActorUserID        string                       `json:"actor_user_id"`
	ActorDeviceID      string                       `json:"actor_device_id"`
	SubjectFabricID    string                       `json:"subject_fabric_id"`
	SubjectUserID      string                       `json:"subject_user_id"`
	PreviousPermission ComputerGrantPermission      `json:"previous_permission"`
	Permission         ComputerGrantPermission      `json:"permission"`
	IdempotencyKey     string                       `json:"idempotency_key,omitempty"`
	CreatedAt          time.Time                    `json:"created_at"`
}

type ComputerPolicyAuditList struct {
	Entries    []ComputerPolicyAudit `json:"entries"`
	NextCursor string                `json:"next_cursor,omitempty"`
}

type ComputerPolicyRevocation struct {
	PolicyRevision   int64                         `json:"policy_revision"`
	ComputerID       string                        `json:"computer_id"`
	SubjectFabricID  string                        `json:"subject_fabric_id"`
	SubjectUserID    string                        `json:"subject_user_id"`
	TargetPermission ComputerGrantPermission       `json:"target_permission"`
	State            ComputerPolicyRevocationState `json:"state"`
	CreatedAt        time.Time                     `json:"created_at"`
}

type ComputerPolicyAdmin struct {
	FabricID string `json:"fabric_id"`
	UserID   string `json:"user_id"`
}

type ComputerPolicyComputer struct {
	ComputerID string          `json:"computer_id"`
	Grants     []ComputerGrant `json:"grants"`
}

// ComputerPolicySnapshot is issued only to the authenticated agent for the
// named current boot. It is deliberately an in-memory lease at the node.
type ComputerPolicySnapshot struct {
	PolicyGeneration int64                    `json:"policy_generation"`
	PolicyRevision   int64                    `json:"policy_revision"`
	NodeID           string                   `json:"node_id"`
	BootSessionID    string                   `json:"boot_session_id"`
	IssuedAt         time.Time                `json:"issued_at"`
	FreshUntil       time.Time                `json:"fresh_until"`
	Admins           []ComputerPolicyAdmin    `json:"admins"`
	Computers        []ComputerPolicyComputer `json:"computers"`
	SnapshotDigest   string                   `json:"snapshot_digest"`
}

type ComputerPolicyInstallAcknowledgement struct {
	NodeID           string `json:"node_id"`
	BootSessionID    string `json:"boot_session_id"`
	PolicyGeneration int64  `json:"policy_generation"`
	PolicyRevision   int64  `json:"policy_revision"`
	SnapshotDigest   string `json:"snapshot_digest"`
}

func (s *Store) notifyComputerPolicyChanged() {
	s.policyChangeMu.Lock()
	close(s.policyChanged)
	s.policyChanged = make(chan struct{})
	s.policyChangeMu.Unlock()
}

func (s *Store) computerPolicyChangeChannel() <-chan struct{} {
	s.policyChangeMu.Lock()
	defer s.policyChangeMu.Unlock()
	return s.policyChanged
}

func validateComputerGrantPermission(permission ComputerGrantPermission) error {
	switch permission {
	case ComputerGrantNone, ComputerGrantView, ComputerGrantControl:
		return nil
	default:
		return protocolError(contract.ErrorInvalidRequest, "permission must be none, view, or control")
	}
}

func computerGrantRank(permission ComputerGrantPermission) int {
	switch permission {
	case ComputerGrantControl:
		return 2
	case ComputerGrantView:
		return 1
	default:
		return 0
	}
}

func computerGrantRequestHash(identity fabric.Identity, computerID, userID string, request ComputerGrantMutationRequest) (string, error) {
	payload, err := json.Marshal(struct {
		ActorFabricID string                  `json:"actor_fabric_id"`
		ActorUserID   string                  `json:"actor_user_id"`
		ComputerID    string                  `json:"computer_id"`
		SubjectUserID string                  `json:"subject_user_id"`
		Revision      int64                   `json:"policy_revision"`
		Permission    ComputerGrantPermission `json:"permission"`
	}{identity.FabricID, identity.UserID, computerID, userID, request.PolicyRevision, request.Permission})
	if err != nil {
		return "", internalError(err, "encode Computer grant mutation")
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func validateGrantMutation(computerID, userID string, request ComputerGrantMutationRequest) error {
	if strings.TrimSpace(computerID) == "" || computerID != strings.TrimSpace(computerID) || len(computerID) > 255 {
		return protocolError(contract.ErrorInvalidRequest, "computer_id must contain between 1 and 255 bytes")
	}
	if _, err := validateAdminUserID(userID); err != nil {
		return err
	}
	if err := validateComputerGrantPermission(request.Permission); err != nil {
		return err
	}
	if request.PolicyRevision < 1 {
		return protocolError(contract.ErrorInvalidRequest, "policy_revision must be positive")
	}
	if strings.TrimSpace(request.IdempotencyKey) == "" || request.IdempotencyKey != strings.TrimSpace(request.IdempotencyKey) || len(request.IdempotencyKey) > 255 {
		return protocolError(contract.ErrorInvalidRequest, "idempotency_key must contain between 1 and 255 bytes")
	}
	return nil
}

func (s *Store) MutateComputerGrant(ctx context.Context, identity fabric.Identity, computerID, userID string, request ComputerGrantMutationRequest) (ComputerGrantMutationResult, error) {
	if err := validatePersonIdentity(identity); err != nil {
		return ComputerGrantMutationResult{}, err
	}
	if err := validateGrantMutation(computerID, userID, request); err != nil {
		return ComputerGrantMutationResult{}, err
	}
	requestHash, err := computerGrantRequestHash(identity, computerID, userID, request)
	if err != nil {
		return ComputerGrantMutationResult{}, err
	}
	now := canonicalTime(s.clock.Now())
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ComputerGrantMutationResult{}, internalError(err, "begin Computer grant mutation")
	}
	defer tx.Rollback()
	if err := requireCurrentAdmin(ctx, tx, identity); err != nil {
		return ComputerGrantMutationResult{}, err
	}
	if replay, replayErr := readComputerGrantReplay(ctx, tx, computerID, request.IdempotencyKey, requestHash); replayErr == nil {
		replay.Replayed = true
		replay.Revocation, err = readComputerPolicyRevocation(ctx, tx, replay.Grant.PolicyRevision, computerID, identity.FabricID, userID)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return ComputerGrantMutationResult{}, internalError(err, "read replayed Computer revocation")
		}
		if err := tx.Commit(); err != nil {
			return ComputerGrantMutationResult{}, internalError(err, "commit Computer grant replay")
		}
		if replay.Revocation != nil {
			completed, statusErr := s.computerPolicyRevisionInstalled(ctx, replay.Grant.PolicyRevision, computerID)
			if statusErr != nil {
				return ComputerGrantMutationResult{}, statusErr
			}
			if completed {
				replay.Revocation.State = ComputerPolicyRevocationCompleted
			}
		}
		return replay, nil
	} else if !errors.Is(replayErr, sql.ErrNoRows) {
		return ComputerGrantMutationResult{}, replayErr
	}
	var exists bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM computers WHERE computer_id=? AND desired_state<>'removed')`, computerID).Scan(&exists); err != nil {
		return ComputerGrantMutationResult{}, internalError(err, "read Computer grant target")
	}
	if !exists {
		return ComputerGrantMutationResult{}, protocolError(contract.ErrorNotFound, "Computer %q not found", computerID)
	}
	var revision int64
	if err := tx.QueryRowContext(ctx, `SELECT revision FROM admin_policy WHERE singleton=1`).Scan(&revision); err != nil {
		return ComputerGrantMutationResult{}, internalError(err, "read Computer policy revision")
	}
	if err := validatePolicyRevision(revision, request.PolicyRevision); err != nil {
		return ComputerGrantMutationResult{}, err
	}
	previous := ComputerGrantNone
	var previousValue string
	readErr := tx.QueryRowContext(ctx, `SELECT permission FROM computer_grants WHERE computer_id=? AND fabric_id=? AND user_id=?`,
		computerID, identity.FabricID, userID).Scan(&previousValue)
	if readErr == nil {
		previous = ComputerGrantPermission(previousValue)
	} else if !errors.Is(readErr, sql.ErrNoRows) {
		return ComputerGrantMutationResult{}, internalError(readErr, "read current Computer grant")
	}
	if previous == request.Permission {
		return ComputerGrantMutationResult{}, protocolError(contract.ErrorConflict,
			"Computer %q already has %s permission for person %q", computerID, request.Permission, userID)
	}
	nextRevision := revision + 1
	if _, err := tx.ExecContext(ctx, `INSERT INTO computer_grants(computer_id, fabric_id, user_id, permission, policy_revision, updated_ns)
		VALUES(?, ?, ?, ?, ?, ?) ON CONFLICT(computer_id, fabric_id, user_id) DO UPDATE SET
		permission=excluded.permission, policy_revision=excluded.policy_revision, updated_ns=excluded.updated_ns`,
		computerID, identity.FabricID, userID, request.Permission, nextRevision, now.UnixNano()); err != nil {
		return ComputerGrantMutationResult{}, internalError(err, "store current Computer grant")
	}
	result, err := tx.ExecContext(ctx, `UPDATE admin_policy SET revision=?, updated_ns=? WHERE singleton=1 AND revision=?`,
		nextRevision, now.UnixNano(), revision)
	if err != nil {
		return ComputerGrantMutationResult{}, internalError(err, "advance Computer policy revision")
	}
	if affected, rowsErr := result.RowsAffected(); rowsErr != nil || affected != 1 {
		if rowsErr != nil {
			return ComputerGrantMutationResult{}, internalError(rowsErr, "read Computer policy CAS result")
		}
		return ComputerGrantMutationResult{}, protocolError(contract.ErrorStalePolicyRevision,
			"Computer policy revision changed from %d", revision)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO computer_policy_audit(
		policy_revision, computer_id, operation, actor_kind, actor_fabric_id, actor_user_id, actor_device_id,
		subject_fabric_id, subject_user_id, previous_permission, permission, idempotency_key, request_hash, created_ns
	) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, nextRevision, computerID, ComputerPolicyAuditGrant,
		AdminPolicyActorFabricPerson, identity.FabricID, identity.UserID, identity.DeviceID,
		identity.FabricID, userID, previous, request.Permission, request.IdempotencyKey, requestHash, now.UnixNano()); err != nil {
		return ComputerGrantMutationResult{}, internalError(err, "append Computer policy audit")
	}
	var revocation *ComputerPolicyRevocation
	if computerGrantRank(request.Permission) < computerGrantRank(previous) {
		if _, err := tx.ExecContext(ctx, `INSERT INTO computer_policy_revocations(
			policy_revision, computer_id, subject_fabric_id, subject_user_id, target_permission, created_ns
		) VALUES(?, ?, ?, ?, ?, ?)`, nextRevision, computerID, identity.FabricID, userID, request.Permission, now.UnixNano()); err != nil {
			return ComputerGrantMutationResult{}, internalError(err, "record Computer policy revocation")
		}
		revocation = &ComputerPolicyRevocation{PolicyRevision: nextRevision, ComputerID: computerID,
			SubjectFabricID: identity.FabricID, SubjectUserID: userID, TargetPermission: request.Permission,
			State: ComputerPolicyRevocationPending, CreatedAt: now}
	}
	if err := tx.Commit(); err != nil {
		return ComputerGrantMutationResult{}, internalError(err, "commit Computer grant mutation")
	}
	s.notifyComputerPolicyChanged()
	return ComputerGrantMutationResult{Grant: ComputerGrant{FabricID: identity.FabricID, UserID: userID,
		Permission: request.Permission, PolicyRevision: nextRevision, UpdatedAt: now}, Revocation: revocation}, nil
}

func readComputerGrantReplay(ctx context.Context, q queryer, computerID, idempotencyKey, requestHash string) (ComputerGrantMutationResult, error) {
	var grant ComputerGrant
	var storedHash string
	var updatedNS int64
	err := q.QueryRowContext(ctx, `SELECT subject_fabric_id, subject_user_id, permission, policy_revision, created_ns, request_hash
		FROM computer_policy_audit WHERE computer_id=? AND idempotency_key=?`, computerID, idempotencyKey).
		Scan(&grant.FabricID, &grant.UserID, &grant.Permission, &grant.PolicyRevision, &updatedNS, &storedHash)
	if err != nil {
		return ComputerGrantMutationResult{}, err
	}
	if storedHash != requestHash {
		return ComputerGrantMutationResult{}, protocolError(contract.ErrorIdempotencyConflict,
			"idempotency key %q was already used for another Computer grant mutation", idempotencyKey)
	}
	grant.UpdatedAt = time.Unix(0, updatedNS).UTC()
	return ComputerGrantMutationResult{Grant: grant}, nil
}

func (s *Store) ListComputerGrants(ctx context.Context, identity fabric.Identity, computerID string) (ComputerGrantList, error) {
	if err := requireCurrentAdmin(ctx, s.db, identity); err != nil {
		return ComputerGrantList{}, err
	}
	var result ComputerGrantList
	if err := s.db.QueryRowContext(ctx, `SELECT revision FROM admin_policy WHERE singleton=1`).Scan(&result.PolicyRevision); err != nil {
		return ComputerGrantList{}, internalError(err, "read Computer grant list revision")
	}
	var exists bool
	if err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM computers WHERE computer_id=? AND desired_state<>'removed')`, computerID).Scan(&exists); err != nil {
		return ComputerGrantList{}, internalError(err, "read Computer grant list target")
	}
	if !exists {
		return ComputerGrantList{}, protocolError(contract.ErrorNotFound, "Computer %q not found", computerID)
	}
	grants, err := listComputerGrants(ctx, s.db, computerID)
	if err != nil {
		return ComputerGrantList{}, internalError(err, "list Computer grants")
	}
	result.Grants = grants
	return result, nil
}

func listComputerGrants(ctx context.Context, q queryer, computerID string) ([]ComputerGrant, error) {
	rows, err := q.QueryContext(ctx, `SELECT fabric_id, user_id, permission, policy_revision, updated_ns
		FROM computer_grants WHERE computer_id=? ORDER BY fabric_id, user_id`, computerID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	grants := []ComputerGrant{}
	for rows.Next() {
		var grant ComputerGrant
		var updatedNS int64
		if err := rows.Scan(&grant.FabricID, &grant.UserID, &grant.Permission, &grant.PolicyRevision, &updatedNS); err != nil {
			return nil, err
		}
		grant.UpdatedAt = time.Unix(0, updatedNS).UTC()
		grants = append(grants, grant)
	}
	return grants, rows.Err()
}

func encodeComputerPolicyAuditCursor(revision int64, computerID, fabricID, userID string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf("%d\x00%s\x00%s\x00%s", revision, computerID, fabricID, userID)))
}

func decodeComputerPolicyAuditCursor(value string) (int64, string, string, string, error) {
	if value == "" {
		return 0, "", "", "", nil
	}
	payload, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return 0, "", "", "", protocolError(contract.ErrorInvalidRequest, "cursor is invalid")
	}
	parts := strings.Split(string(payload), "\x00")
	if len(parts) != 4 {
		return 0, "", "", "", protocolError(contract.ErrorInvalidRequest, "cursor is invalid")
	}
	revision, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || revision < 0 {
		return 0, "", "", "", protocolError(contract.ErrorInvalidRequest, "cursor is invalid")
	}
	return revision, parts[1], parts[2], parts[3], nil
}

func (s *Store) ListComputerPolicyAudit(ctx context.Context, identity fabric.Identity, computerID, cursor string, limit int) (ComputerPolicyAuditList, error) {
	if err := requireCurrentAdmin(ctx, s.db, identity); err != nil {
		return ComputerPolicyAuditList{}, err
	}
	if limit < 1 || limit > MaxJobPageLimit {
		return ComputerPolicyAuditList{}, protocolError(contract.ErrorInvalidRequest, "limit must be between 1 and %d", MaxJobPageLimit)
	}
	revision, cursorComputer, fabricID, userID, err := decodeComputerPolicyAuditCursor(cursor)
	if err != nil {
		return ComputerPolicyAuditList{}, err
	}
	if cursorComputer != "" && cursorComputer != computerID {
		return ComputerPolicyAuditList{}, protocolError(contract.ErrorInvalidRequest, "cursor is for another Computer")
	}
	rows, err := s.db.QueryContext(ctx, `SELECT policy_revision, computer_id, operation, actor_kind,
		actor_fabric_id, actor_user_id, actor_device_id, subject_fabric_id, subject_user_id,
		previous_permission, permission, idempotency_key, created_ns
		FROM computer_policy_audit WHERE computer_id=? AND
		(policy_revision>? OR (policy_revision=? AND (subject_fabric_id>? OR (subject_fabric_id=? AND subject_user_id>?))))
		ORDER BY policy_revision, subject_fabric_id, subject_user_id LIMIT ?`, computerID, revision, revision, fabricID, fabricID, userID, limit+1)
	if err != nil {
		return ComputerPolicyAuditList{}, internalError(err, "list Computer policy audit")
	}
	defer rows.Close()
	page := ComputerPolicyAuditList{Entries: []ComputerPolicyAudit{}}
	for rows.Next() {
		var entry ComputerPolicyAudit
		var createdNS int64
		if err := rows.Scan(&entry.PolicyRevision, &entry.ComputerID, &entry.Operation, &entry.ActorKind,
			&entry.ActorFabricID, &entry.ActorUserID, &entry.ActorDeviceID, &entry.SubjectFabricID,
			&entry.SubjectUserID, &entry.PreviousPermission, &entry.Permission, &entry.IdempotencyKey, &createdNS); err != nil {
			return ComputerPolicyAuditList{}, internalError(err, "scan Computer policy audit")
		}
		entry.CreatedAt = time.Unix(0, createdNS).UTC()
		page.Entries = append(page.Entries, entry)
	}
	if err := rows.Err(); err != nil {
		return ComputerPolicyAuditList{}, internalError(err, "read Computer policy audit")
	}
	if len(page.Entries) > limit {
		page.Entries = page.Entries[:limit]
		last := page.Entries[len(page.Entries)-1]
		page.NextCursor = encodeComputerPolicyAuditCursor(last.PolicyRevision, last.ComputerID, last.SubjectFabricID, last.SubjectUserID)
	}
	return page, nil
}

func ComputeComputerPolicySnapshotDigest(snapshot ComputerPolicySnapshot) (string, error) {
	snapshot.SnapshotDigest = ""
	payload, err := json.Marshal(snapshot)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func ValidateComputerPolicySnapshot(snapshot ComputerPolicySnapshot) error {
	if snapshot.PolicyGeneration < 1 || snapshot.PolicyRevision < 0 || strings.TrimSpace(snapshot.NodeID) == "" ||
		strings.TrimSpace(snapshot.BootSessionID) == "" || snapshot.IssuedAt.IsZero() || !snapshot.FreshUntil.After(snapshot.IssuedAt) {
		return errors.New("invalid Computer policy snapshot authority")
	}
	admins := make(map[string]struct{}, len(snapshot.Admins))
	if len(snapshot.Admins) > MaxAdministrators {
		return fmt.Errorf("Computer policy has %d administrators, maximum is %d", len(snapshot.Admins), MaxAdministrators)
	}
	for i, admin := range snapshot.Admins {
		if strings.TrimSpace(admin.FabricID) == "" || strings.TrimSpace(admin.UserID) == "" {
			return fmt.Errorf("invalid Computer policy admin at index %d", i)
		}
		key := admin.FabricID + "\x00" + admin.UserID
		if _, duplicate := admins[key]; duplicate {
			return fmt.Errorf("duplicate Computer policy admin at index %d", i)
		}
		admins[key] = struct{}{}
	}
	computers := make(map[string]struct{}, len(snapshot.Computers))
	for i, computer := range snapshot.Computers {
		if strings.TrimSpace(computer.ComputerID) == "" {
			return fmt.Errorf("invalid Computer policy Computer at index %d", i)
		}
		if _, duplicate := computers[computer.ComputerID]; duplicate {
			return fmt.Errorf("duplicate Computer policy Computer at index %d", i)
		}
		computers[computer.ComputerID] = struct{}{}
		grants := make(map[string]struct{}, len(computer.Grants))
		for j, grant := range computer.Grants {
			if strings.TrimSpace(grant.FabricID) == "" || strings.TrimSpace(grant.UserID) == "" ||
				validateComputerGrantPermission(grant.Permission) != nil || grant.PolicyRevision < 1 ||
				grant.PolicyRevision > snapshot.PolicyRevision || grant.UpdatedAt.IsZero() {
				return fmt.Errorf("invalid Computer policy grant at index %d/%d", i, j)
			}
			key := grant.FabricID + "\x00" + grant.UserID
			if _, duplicate := grants[key]; duplicate {
				return fmt.Errorf("duplicate Computer policy grant at index %d/%d", i, j)
			}
			grants[key] = struct{}{}
		}
	}
	digest, err := ComputeComputerPolicySnapshotDigest(snapshot)
	if err != nil {
		return err
	}
	if snapshot.SnapshotDigest == "" || digest != snapshot.SnapshotDigest {
		return errors.New("Computer policy snapshot digest mismatch")
	}
	return nil
}

func (s *Store) IssueComputerPolicySnapshot(ctx context.Context, identityNodeID, nodeID, bootSessionID string, freshness time.Duration) (*ComputerPolicySnapshot, error) {
	if freshness <= 0 {
		freshness = DefaultComputerPolicyFreshness
	}
	now := canonicalTime(s.clock.Now())
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, internalError(err, "begin Computer policy snapshot")
	}
	defer tx.Rollback()
	var storedIdentity, storedBoot string
	if err := tx.QueryRowContext(ctx, `SELECT identity_node_id, boot_session_id FROM nodes WHERE node_id=?`, nodeID).
		Scan(&storedIdentity, &storedBoot); errors.Is(err, sql.ErrNoRows) {
		return nil, protocolError(contract.ErrorNodeNotRegistered, "node %q is not registered", nodeID)
	} else if err != nil {
		return nil, internalError(err, "read Computer policy node")
	}
	if storedIdentity != identityNodeID {
		return nil, protocolError(contract.ErrorIdentityBound, "node %q is bound to another Fabric identity", nodeID)
	}
	if storedBoot != bootSessionID {
		return nil, protocolError(contract.ErrorNodeSessionReplaced, "node %q boot session was replaced", nodeID)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM computer_policy_issued
		WHERE node_id=? AND (boot_session_id<>? OR expires_ns<=?)`, nodeID, bootSessionID, now.UnixNano()); err != nil {
		return nil, internalError(err, "expire old Computer policy issuances")
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM computer_policy_installations
		WHERE node_id=? AND boot_session_id<>?`, nodeID, bootSessionID); err != nil {
		return nil, internalError(err, "expire old Computer policy installations")
	}
	var generation, revision int64
	if err := tx.QueryRowContext(ctx, `SELECT authority_generation, revision FROM admin_policy WHERE singleton=1`).Scan(&generation, &revision); err != nil {
		return nil, internalError(err, "read Computer policy authority")
	}
	computerRows, err := tx.QueryContext(ctx, `SELECT computer_id FROM computers
		WHERE desired_state<>'removed' AND (placement_node_id=? OR bound_node_id=?) ORDER BY computer_id`, nodeID, nodeID)
	if err != nil {
		return nil, internalError(err, "list node Computers for policy")
	}
	var computerIDs []string
	for computerRows.Next() {
		var computerID string
		if err := computerRows.Scan(&computerID); err != nil {
			computerRows.Close()
			return nil, internalError(err, "scan node Computer policy")
		}
		computerIDs = append(computerIDs, computerID)
	}
	if err := computerRows.Close(); err != nil {
		return nil, internalError(err, "close node Computer policy rows")
	}
	var previouslyIssued bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM computer_policy_issued WHERE node_id=?)`, nodeID).Scan(&previouslyIssued); err != nil {
		return nil, internalError(err, "read prior Computer policy issuance")
	}
	if len(computerIDs) == 0 && !previouslyIssued {
		if err := tx.Commit(); err != nil {
			return nil, internalError(err, "commit empty Computer policy lookup")
		}
		return nil, nil
	}
	adminRows, err := tx.QueryContext(ctx, `SELECT fabric_id, user_id FROM admins ORDER BY fabric_id, user_id`)
	if err != nil {
		return nil, internalError(err, "list Computer policy administrators")
	}
	admins := []ComputerPolicyAdmin{}
	for adminRows.Next() {
		var admin ComputerPolicyAdmin
		if err := adminRows.Scan(&admin.FabricID, &admin.UserID); err != nil {
			adminRows.Close()
			return nil, internalError(err, "scan Computer policy administrator")
		}
		admins = append(admins, admin)
	}
	if err := adminRows.Close(); err != nil {
		return nil, internalError(err, "close Computer policy administrators")
	}
	computers := make([]ComputerPolicyComputer, 0, len(computerIDs))
	for _, computerID := range computerIDs {
		grants, err := listComputerGrants(ctx, tx, computerID)
		if err != nil {
			return nil, internalError(err, "list snapshot Computer grants")
		}
		computers = append(computers, ComputerPolicyComputer{ComputerID: computerID, Grants: grants})
	}
	snapshot := ComputerPolicySnapshot{PolicyGeneration: generation, PolicyRevision: revision,
		NodeID: nodeID, BootSessionID: bootSessionID, IssuedAt: now, FreshUntil: canonicalTime(now.Add(freshness)),
		Admins: admins, Computers: computers}
	snapshot.SnapshotDigest, err = ComputeComputerPolicySnapshotDigest(snapshot)
	if err != nil {
		return nil, internalError(err, "digest Computer policy snapshot")
	}
	if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO computer_policy_issued(
		node_id, boot_session_id, policy_generation, policy_revision, snapshot_digest, expires_ns, issued_ns
	) VALUES(?, ?, ?, ?, ?, ?, ?)`, nodeID, bootSessionID, generation, revision, snapshot.SnapshotDigest,
		snapshot.FreshUntil.UnixNano(), snapshot.IssuedAt.UnixNano()); err != nil {
		return nil, internalError(err, "record Computer policy issuance")
	}
	if err := tx.Commit(); err != nil {
		return nil, internalError(err, "commit Computer policy snapshot")
	}
	return &snapshot, nil
}

func (s *Store) AcknowledgeComputerPolicyInstallation(ctx context.Context, identityNodeID string, request ComputerPolicyInstallAcknowledgement) error {
	if strings.TrimSpace(request.NodeID) == "" || strings.TrimSpace(request.BootSessionID) == "" || request.PolicyGeneration < 1 ||
		request.PolicyRevision < 0 || len(request.SnapshotDigest) != sha256.Size*2 {
		return protocolError(contract.ErrorInvalidRequest, "complete Computer policy installation evidence is required")
	}
	now := canonicalTime(s.clock.Now())
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return internalError(err, "begin Computer policy acknowledgement")
	}
	defer tx.Rollback()
	var storedIdentity, storedBoot string
	if err := tx.QueryRowContext(ctx, `SELECT identity_node_id, boot_session_id FROM nodes WHERE node_id=?`, request.NodeID).
		Scan(&storedIdentity, &storedBoot); errors.Is(err, sql.ErrNoRows) {
		return protocolError(contract.ErrorNodeNotRegistered, "node %q is not registered", request.NodeID)
	} else if err != nil {
		return internalError(err, "read Computer policy acknowledgement node")
	}
	if storedIdentity != identityNodeID {
		return protocolError(contract.ErrorIdentityBound, "node %q is bound to another Fabric identity", request.NodeID)
	}
	if storedBoot != request.BootSessionID {
		return protocolError(contract.ErrorNodeSessionReplaced, "node %q boot session was replaced", request.NodeID)
	}
	var generation int64
	if err := tx.QueryRowContext(ctx, `SELECT authority_generation FROM admin_policy WHERE singleton=1`).Scan(&generation); err != nil {
		return internalError(err, "read Computer policy acknowledgement generation")
	}
	if generation != request.PolicyGeneration {
		return protocolError(contract.ErrorStalePolicyRevision, "Computer policy generation changed")
	}
	var expiresNS int64
	if err := tx.QueryRowContext(ctx, `SELECT expires_ns FROM computer_policy_issued WHERE
		node_id=? AND boot_session_id=? AND policy_generation=? AND policy_revision=? AND snapshot_digest=?`,
		request.NodeID, request.BootSessionID, request.PolicyGeneration, request.PolicyRevision, request.SnapshotDigest).
		Scan(&expiresNS); errors.Is(err, sql.ErrNoRows) {
		return protocolError(contract.ErrorInvalidRequest, "Computer policy acknowledgement has no matching issued snapshot")
	} else if err != nil {
		return internalError(err, "read issued Computer policy snapshot")
	}
	if expiresNS <= now.UnixNano() {
		return protocolError(contract.ErrorStalePolicyRevision, "Computer policy snapshot expired before acknowledgement")
	}
	var installedRevision int64
	installedErr := tx.QueryRowContext(ctx, `SELECT policy_revision FROM computer_policy_installations
		WHERE node_id=? AND boot_session_id=? AND policy_generation=?`, request.NodeID, request.BootSessionID,
		request.PolicyGeneration).Scan(&installedRevision)
	if installedErr == nil && installedRevision > request.PolicyRevision {
		return protocolErrorWithDetails(contract.ErrorStalePolicyRevision,
			map[string]any{"expected_revision": installedRevision, "observed_revision": request.PolicyRevision},
			"Computer policy installation revision regressed from %d to %d", installedRevision, request.PolicyRevision)
	}
	if installedErr != nil && !errors.Is(installedErr, sql.ErrNoRows) {
		return internalError(installedErr, "read current Computer policy installation")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO computer_policy_installations(
		node_id, boot_session_id, policy_generation, policy_revision, snapshot_digest, installed_ns
	) VALUES(?, ?, ?, ?, ?, ?) ON CONFLICT(node_id, boot_session_id, policy_generation) DO UPDATE SET
		policy_revision=excluded.policy_revision, snapshot_digest=excluded.snapshot_digest, installed_ns=excluded.installed_ns
		WHERE excluded.policy_revision>=computer_policy_installations.policy_revision`, request.NodeID, request.BootSessionID,
		request.PolicyGeneration, request.PolicyRevision, request.SnapshotDigest, now.UnixNano()); err != nil {
		return internalError(err, "record Computer policy installation")
	}
	if err := tx.Commit(); err != nil {
		return internalError(err, "commit Computer policy acknowledgement")
	}
	return nil
}

func readComputerPolicyRevocation(ctx context.Context, q queryer, revision int64, computerID, fabricID, userID string) (*ComputerPolicyRevocation, error) {
	var revocation ComputerPolicyRevocation
	var createdNS int64
	query := `SELECT policy_revision, computer_id, subject_fabric_id, subject_user_id,
		target_permission, created_ns FROM computer_policy_revocations WHERE policy_revision=? AND computer_id=?`
	args := []any{revision, computerID}
	if fabricID != "" || userID != "" {
		query += ` AND subject_fabric_id=? AND subject_user_id=?`
		args = append(args, fabricID, userID)
	}
	query += ` ORDER BY subject_fabric_id, subject_user_id LIMIT 1`
	err := q.QueryRowContext(ctx, query, args...).
		Scan(&revocation.PolicyRevision, &revocation.ComputerID, &revocation.SubjectFabricID,
			&revocation.SubjectUserID, &revocation.TargetPermission, &createdNS)
	if err != nil {
		return nil, err
	}
	revocation.CreatedAt = time.Unix(0, createdNS).UTC()
	revocation.State = ComputerPolicyRevocationPending
	return &revocation, nil
}

func (s *Store) GetComputerPolicyRevocation(ctx context.Context, identity fabric.Identity, revision int64, computerID, fabricID, userID string) (ComputerPolicyRevocation, error) {
	if err := requireCurrentAdmin(ctx, s.db, identity); err != nil {
		return ComputerPolicyRevocation{}, err
	}
	revocation, err := readComputerPolicyRevocation(ctx, s.db, revision, computerID, fabricID, userID)
	if errors.Is(err, sql.ErrNoRows) {
		return ComputerPolicyRevocation{}, protocolError(contract.ErrorNotFound, "Computer policy revocation not found")
	}
	if err != nil {
		return ComputerPolicyRevocation{}, internalError(err, "read Computer policy revocation")
	}
	completed, err := s.computerPolicyRevisionInstalled(ctx, revision, computerID)
	if err != nil {
		return ComputerPolicyRevocation{}, err
	}
	if completed {
		revocation.State = ComputerPolicyRevocationCompleted
	}
	return *revocation, nil
}

func (s *Store) computerPolicyRevisionInstalled(ctx context.Context, revision int64, computerID string) (bool, error) {
	var generation int64
	if err := s.db.QueryRowContext(ctx, `SELECT authority_generation FROM admin_policy WHERE singleton=1`).Scan(&generation); err != nil {
		return false, internalError(err, "read revocation policy generation")
	}
	rows, err := s.db.QueryContext(ctx, `SELECT DISTINCT COALESCE(NULLIF(bound_node_id, ''), placement_node_id)
		FROM computers WHERE desired_state<>'removed' AND (?='' OR computer_id=?)`, computerID, computerID)
	if err != nil {
		return false, internalError(err, "list revocation nodes")
	}
	defer rows.Close()
	var nodes []string
	for rows.Next() {
		var nodeID string
		if err := rows.Scan(&nodeID); err != nil {
			return false, internalError(err, "scan revocation node")
		}
		if nodeID != "" {
			nodes = append(nodes, nodeID)
		}
	}
	if err := rows.Err(); err != nil {
		return false, internalError(err, "read revocation nodes")
	}
	for _, nodeID := range nodes {
		var installed bool
		if err := s.db.QueryRowContext(ctx, `SELECT EXISTS(
			SELECT 1 FROM nodes n JOIN computer_policy_installations i
			ON i.node_id=n.node_id AND i.boot_session_id=n.boot_session_id
			WHERE n.node_id=? AND i.policy_generation=? AND i.policy_revision>=?
		)`, nodeID, generation, revision).Scan(&installed); err != nil {
			return false, internalError(err, "read revocation installation")
		}
		if !installed {
			return false, nil
		}
	}
	return true, nil
}
