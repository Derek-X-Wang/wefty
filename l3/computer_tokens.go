package l3

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strings"

	"github.com/Derek-X-Wang/wefty/contract"
)

var computerTokenPermissions = []ComputerPermission{ComputerPermissionObserve, ComputerPermissionSubmit}

func newComputerToken() (string, error) {
	var random [32]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", internalError(err, "mint Computer token entropy")
	}
	return "wcomputer_" + hex.EncodeToString(random[:]), nil
}

func validComputerScopeProof(proof ComputerTokenScopeProof) error {
	if strings.TrimSpace(proof.ComputerID) == "" || proof.ComputerID != strings.TrimSpace(proof.ComputerID) ||
		strings.TrimSpace(proof.ComputerAttemptID) == "" || proof.ComputerAttemptID != strings.TrimSpace(proof.ComputerAttemptID) ||
		strings.TrimSpace(proof.HostNodeID) == "" || proof.HostNodeID != strings.TrimSpace(proof.HostNodeID) ||
		len(proof.ComputerID) > 255 || len(proof.ComputerAttemptID) > 255 || len(proof.HostNodeID) > 255 ||
		proof.ComputerStorageGeneration < 1 || proof.SubmitIntentRevision < 1 || proof.SubmitMaxInflight < 1 {
		return protocolError(contract.ErrorInvalidRequest, "Computer token scope proof is incomplete")
	}
	return nil
}

func (s *Store) adoptComputerAuthorityInstance(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return internalError(err, "begin L3 Computer authority advance")
	}
	defer tx.Rollback()
	now := canonicalTime(s.clock.Now())
	var generation int64
	var currentInstance string
	if err := tx.QueryRowContext(ctx, `SELECT authority_generation, instance_id FROM computer_authority WHERE singleton=1`).Scan(&generation, &currentInstance); err != nil {
		return internalError(err, "read L3 Computer authority generation")
	}
	if currentInstance == s.computerAuthorityInstanceID && generation > 0 {
		return nil
	}
	if err := revokeComputerGrantRows(ctx, tx, `revoked_ns IS NULL`, nil, now.UnixNano(), "l3_authority_advanced"); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE computer_authority SET authority_generation=?, instance_id=?, updated_ns=? WHERE singleton=1`, generation+1, s.computerAuthorityInstanceID, now.UnixNano()); err != nil {
		return internalError(err, "advance L3 Computer authority generation")
	}
	if err := tx.Commit(); err != nil {
		return internalError(err, "commit L3 Computer authority advance")
	}
	return nil
}

// MintComputerToken persists only the digest and immutable audit. Minting a
// second pass revokes every older grant for the Computer before the fresh
// bearer is returned. This makes a new attempt a definitive authority fence.
func (s *Store) MintComputerToken(ctx context.Context, proof ComputerTokenScopeProof) (ComputerTokenGrant, error) {
	if err := validComputerScopeProof(proof); err != nil {
		return ComputerTokenGrant{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ComputerTokenGrant{}, internalError(err, "begin Computer token mint")
	}
	defer tx.Rollback()
	now := canonicalTime(s.clock.Now())
	if err := revokeComputerGrantRows(ctx, tx, `computer_id=? AND revoked_ns IS NULL`,
		[]any{proof.ComputerID}, now.UnixNano(), "regranted"); err != nil {
		return ComputerTokenGrant{}, err
	}
	var authorityGeneration, grantRevision int64
	if err := tx.QueryRowContext(ctx, `SELECT authority_generation, grant_revision FROM computer_authority WHERE singleton=1`).
		Scan(&authorityGeneration, &grantRevision); err != nil {
		return ComputerTokenGrant{}, internalError(err, "read Computer token authority")
	}
	grantRevision++
	token, err := newComputerToken()
	if err != nil {
		return ComputerTokenGrant{}, err
	}
	digest := sha256.Sum256([]byte(token))
	grantID := newID("computergrant")
	if _, err := tx.ExecContext(ctx, `INSERT INTO computer_token_grants(
		grant_id, computer_id, computer_attempt_id, computer_storage_generation, submit_intent_revision,
		host_node_id, l3_authority_generation, grant_revision, submit_max_inflight, token_hash, issued_ns
	) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, grantID, proof.ComputerID, proof.ComputerAttemptID,
		proof.ComputerStorageGeneration, proof.SubmitIntentRevision, proof.HostNodeID, authorityGeneration,
		grantRevision, proof.SubmitMaxInflight, digest[:], now.UnixNano()); err != nil {
		return ComputerTokenGrant{}, internalError(err, "store Computer token digest")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE computer_authority SET grant_revision=?, updated_ns=? WHERE singleton=1`, grantRevision, now.UnixNano()); err != nil {
		return ComputerTokenGrant{}, internalError(err, "advance Computer grant revision")
	}
	if err := appendComputerTokenAudit(ctx, tx, computerTokenAuditRow{
		GrantID: grantID, Operation: "issued", ComputerID: proof.ComputerID, ComputerAttemptID: proof.ComputerAttemptID,
		StorageGeneration: proof.ComputerStorageGeneration, SubmitIntentRevision: proof.SubmitIntentRevision,
		HostNodeID: proof.HostNodeID, AuthorityGeneration: authorityGeneration, GrantRevision: grantRevision,
		SubmitMaxInflight: proof.SubmitMaxInflight, TokenHash: digest[:], OccurredNS: now.UnixNano(),
	}); err != nil {
		return ComputerTokenGrant{}, err
	}
	if err := tx.Commit(); err != nil {
		return ComputerTokenGrant{}, internalError(err, "commit Computer token mint")
	}
	return ComputerTokenGrant{
		Token: token, ComputerID: proof.ComputerID, ComputerAttemptID: proof.ComputerAttemptID,
		ComputerStorageGeneration: proof.ComputerStorageGeneration, SubmitIntentRevision: proof.SubmitIntentRevision,
		HostNodeID: proof.HostNodeID, L3AuthorityGeneration: authorityGeneration, GrantRevision: grantRevision,
		SubmitMaxInflight: proof.SubmitMaxInflight, Permissions: append([]ComputerPermission(nil), computerTokenPermissions...),
	}, nil
}

// RevokeComputerTokens advances the authoritative grant record before callers
// close transport reachability or report an administrative disable complete.
func (s *Store) RevokeComputerTokens(ctx context.Context, request ComputerTokenRevocationRequest) error {
	request.ComputerID = strings.TrimSpace(request.ComputerID)
	request.Reason = strings.TrimSpace(request.Reason)
	if request.ComputerID == "" || len(request.ComputerID) > 255 || (!request.RevokeAll && request.SubmitIntentRevision < 1) || request.Reason == "" || len(request.Reason) > 255 {
		return protocolError(contract.ErrorInvalidRequest, "Computer token revocation is incomplete")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return internalError(err, "begin Computer token revocation")
	}
	defer tx.Rollback()
	now := canonicalTime(s.clock.Now())
	predicate := `computer_id=? AND submit_intent_revision<? AND revoked_ns IS NULL`
	args := []any{request.ComputerID, request.SubmitIntentRevision}
	if request.RevokeAll {
		predicate = `computer_id=? AND revoked_ns IS NULL`
		args = []any{request.ComputerID}
	}
	if err := revokeComputerGrantRows(ctx, tx, predicate, args, now.UnixNano(), request.Reason); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return internalError(err, "commit Computer token revocation")
	}
	return nil
}

func (s *Store) RevokeComputerTokenScope(ctx context.Context, scope ComputerTokenScope, reason string) error {
	if scope.GrantRevision < 1 || strings.TrimSpace(reason) == "" {
		return protocolError(contract.ErrorInvalidRequest, "Computer token scope revocation is incomplete")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return internalError(err, "begin scoped Computer token revocation")
	}
	defer tx.Rollback()
	now := canonicalTime(s.clock.Now())
	if err := revokeComputerGrantRows(ctx, tx, `grant_revision=? AND revoked_ns IS NULL`, []any{scope.GrantRevision}, now.UnixNano(), reason); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return internalError(err, "commit scoped Computer token revocation")
	}
	return nil
}

func (s *Store) RevokeComputerAttemptTokens(ctx context.Context, computerID, attemptID, hostNodeID, reason string) error {
	computerID, attemptID, hostNodeID, reason = strings.TrimSpace(computerID), strings.TrimSpace(attemptID), strings.TrimSpace(hostNodeID), strings.TrimSpace(reason)
	if computerID == "" || attemptID == "" || hostNodeID == "" || reason == "" {
		return protocolError(contract.ErrorInvalidRequest, "Computer attempt token revocation is incomplete")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return internalError(err, "begin Computer attempt token revocation")
	}
	defer tx.Rollback()
	now := canonicalTime(s.clock.Now())
	if err := revokeComputerGrantRows(ctx, tx,
		`computer_id=? AND computer_attempt_id=? AND host_node_id=? AND revoked_ns IS NULL`,
		[]any{computerID, attemptID, hostNodeID}, now.UnixNano(), reason); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return internalError(err, "commit Computer attempt token revocation")
	}
	return nil
}

func (s *Store) RevokeHostComputerTokens(ctx context.Context, hostNodeID, reason string) error {
	hostNodeID, reason = strings.TrimSpace(hostNodeID), strings.TrimSpace(reason)
	if hostNodeID == "" || reason == "" {
		return protocolError(contract.ErrorInvalidRequest, "host Computer token revocation is incomplete")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return internalError(err, "begin host Computer token revocation")
	}
	defer tx.Rollback()
	now := canonicalTime(s.clock.Now())
	if err := revokeComputerGrantRows(ctx, tx, `host_node_id=? AND revoked_ns IS NULL`, []any{hostNodeID}, now.UnixNano(), reason); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return internalError(err, "commit host Computer token revocation")
	}
	return nil
}

func validateComputerGrantTx(ctx context.Context, tx *sql.Tx, scope ComputerTokenScope) error {
	var active int
	err := tx.QueryRowContext(ctx, `SELECT EXISTS(
		SELECT 1 FROM computer_token_grants g JOIN computer_authority a ON a.singleton=1
		WHERE g.computer_id=? AND g.computer_attempt_id=? AND g.computer_storage_generation=?
		AND g.submit_intent_revision=? AND g.host_node_id=? AND g.l3_authority_generation=?
		AND g.grant_revision=? AND g.submit_max_inflight=? AND g.revoked_ns IS NULL
		AND g.l3_authority_generation=a.authority_generation
	)`, scope.ComputerID, scope.ComputerAttemptID, scope.ComputerStorageGeneration,
		scope.SubmitIntentRevision, scope.HostNodeID, scope.L3AuthorityGeneration,
		scope.GrantRevision, scope.SubmitMaxInflight).Scan(&active)
	if err != nil {
		return internalError(err, "revalidate Computer grant inside Run write")
	}
	if active != 1 {
		return protocolError(contract.ErrorUnauthorized, "Computer token grant is no longer current")
	}
	return nil
}

func (s *Store) AuthenticateComputerToken(ctx context.Context, token string) (ComputerTokenScope, error) {
	if token == "" {
		return ComputerTokenScope{}, protocolError(contract.ErrorUnauthorized, "Computer token is required")
	}
	digest := sha256.Sum256([]byte(token))
	var scope ComputerTokenScope
	var storedHash []byte
	var revokedNS sql.NullInt64
	var currentGeneration int64
	err := s.db.QueryRowContext(ctx, `SELECT g.computer_id, g.computer_attempt_id, g.computer_storage_generation,
		g.submit_intent_revision, g.host_node_id, g.l3_authority_generation, g.grant_revision,
		g.submit_max_inflight, g.token_hash, g.revoked_ns, a.authority_generation
	FROM computer_token_grants g CROSS JOIN computer_authority a
	WHERE g.token_hash=? AND a.singleton=1`, digest[:]).Scan(&scope.ComputerID, &scope.ComputerAttemptID,
		&scope.ComputerStorageGeneration, &scope.SubmitIntentRevision, &scope.HostNodeID,
		&scope.L3AuthorityGeneration, &scope.GrantRevision, &scope.SubmitMaxInflight, &storedHash, &revokedNS, &currentGeneration)
	if errors.Is(err, sql.ErrNoRows) {
		return ComputerTokenScope{}, protocolError(contract.ErrorUnauthorized, "Computer token is invalid")
	}
	if err != nil {
		return ComputerTokenScope{}, internalError(err, "authenticate Computer token")
	}
	if len(storedHash) != len(digest) || subtle.ConstantTimeCompare(storedHash, digest[:]) != 1 || revokedNS.Valid ||
		scope.L3AuthorityGeneration != currentGeneration {
		return ComputerTokenScope{}, protocolError(contract.ErrorUnauthorized, "Computer token is invalid")
	}
	scope.Permissions = append([]ComputerPermission(nil), computerTokenPermissions...)
	return scope, nil
}

func (s *Store) ComputerSelf(scope ComputerTokenScope) ComputerSelf {
	return ComputerSelf{ComputerID: scope.ComputerID, ComputerStorageGeneration: scope.ComputerStorageGeneration,
		GrantRevision: scope.GrantRevision, Permissions: append([]ComputerPermission(nil), scope.Permissions...)}
}

func (s *Store) CanComputerReadRun(ctx context.Context, scope ComputerTokenScope, runID string) (bool, error) {
	var allowed int
	err := s.db.QueryRowContext(ctx, `WITH RECURSIVE visible(run_id) AS (
		SELECT t.run_id FROM run_triggers t
		WHERE t.source='computer' AND t.computer_id=? AND t.computer_storage_generation=?
		UNION ALL
		SELECT r.run_id FROM runs r JOIN visible v ON r.parent_run_id=v.run_id
	) SELECT EXISTS(SELECT 1 FROM visible WHERE run_id=?)`, scope.ComputerID, scope.ComputerStorageGeneration, runID).Scan(&allowed)
	if err != nil {
		return false, internalError(err, "authorize Computer run read")
	}
	return allowed == 1, nil
}

type computerRunCursor struct {
	ComputerID                string `json:"computer_id"`
	ComputerStorageGeneration int64  `json:"computer_storage_generation"`
	IncludeDescendants        bool   `json:"include_descendants"`
	CreatedNS                 int64  `json:"created_ns"`
	RunID                     string `json:"run_id"`
}

func encodeComputerRunCursor(cursor computerRunCursor) string {
	payload, err := json.Marshal(cursor)
	if err != nil {
		panic("l3: encode Computer Run cursor: " + err.Error())
	}
	return base64.RawURLEncoding.EncodeToString(payload)
}

func decodeComputerRunCursor(value string, scope ComputerTokenScope, includeDescendants bool) (computerRunCursor, error) {
	if value == "" {
		return computerRunCursor{ComputerID: scope.ComputerID, ComputerStorageGeneration: scope.ComputerStorageGeneration,
			IncludeDescendants: includeDescendants}, nil
	}
	payload, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return computerRunCursor{}, protocolError(contract.ErrorInvalidRequest, "cursor is invalid")
	}
	var cursor computerRunCursor
	decoder := json.NewDecoder(strings.NewReader(string(payload)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cursor); err != nil || cursor.ComputerID != scope.ComputerID ||
		cursor.ComputerStorageGeneration != scope.ComputerStorageGeneration || cursor.IncludeDescendants != includeDescendants ||
		cursor.CreatedNS < 0 || strings.TrimSpace(cursor.RunID) == "" {
		return computerRunCursor{}, protocolError(contract.ErrorInvalidRequest, "cursor is invalid for this Computer Run scope")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return computerRunCursor{}, protocolError(contract.ErrorInvalidRequest, "cursor is invalid")
	}
	return cursor, nil
}

func (s *Store) ListComputerRuns(ctx context.Context, scope ComputerTokenScope, cursorValue string, limit int, includeDescendants bool) (ComputerRunPage, error) {
	if limit < 1 || limit > MaxComputerRunPageLimit {
		return ComputerRunPage{}, protocolError(contract.ErrorInvalidRequest, "limit must be between 1 and %d", MaxComputerRunPageLimit)
	}
	cursor, err := decodeComputerRunCursor(cursorValue, scope, includeDescendants)
	if err != nil {
		return ComputerRunPage{}, err
	}
	query := `SELECT r.run_id, r.created_ns FROM runs r JOIN run_triggers t ON t.run_id=r.run_id
		WHERE r.parent_run_id IS NULL AND t.source='computer' AND t.computer_id=? AND t.computer_storage_generation=?
			AND (r.created_ns>? OR (r.created_ns=? AND r.run_id>?))
		ORDER BY r.created_ns, r.run_id LIMIT ?`
	if includeDescendants {
		query = `WITH RECURSIVE visible(run_id) AS (
			SELECT r.run_id FROM runs r JOIN run_triggers t ON t.run_id=r.run_id
			WHERE r.parent_run_id IS NULL AND t.source='computer' AND t.computer_id=? AND t.computer_storage_generation=?
			UNION ALL
			SELECT child.run_id FROM runs child JOIN visible parent ON child.parent_run_id=parent.run_id
		) SELECT r.run_id, r.created_ns FROM runs r JOIN visible v ON v.run_id=r.run_id
		WHERE (r.created_ns>? OR (r.created_ns=? AND r.run_id>?))
		ORDER BY r.created_ns, r.run_id LIMIT ?`
	}
	rows, err := s.db.QueryContext(ctx, query, scope.ComputerID, scope.ComputerStorageGeneration,
		cursor.CreatedNS, cursor.CreatedNS, cursor.RunID, limit+1)
	if err != nil {
		return ComputerRunPage{}, internalError(err, "list Computer Run IDs")
	}
	type listedRun struct {
		runID     string
		createdNS int64
	}
	listed := make([]listedRun, 0, limit+1)
	for rows.Next() {
		var item listedRun
		if err := rows.Scan(&item.runID, &item.createdNS); err != nil {
			rows.Close()
			return ComputerRunPage{}, internalError(err, "scan Computer Run ID")
		}
		listed = append(listed, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return ComputerRunPage{}, internalError(err, "iterate Computer Run IDs")
	}
	if err := rows.Close(); err != nil {
		return ComputerRunPage{}, internalError(err, "close Computer Run list")
	}
	page := ComputerRunPage{Runs: []contract.RunRecord{}}
	hasMore := len(listed) > limit
	if hasMore {
		listed = listed[:limit]
	}
	for _, item := range listed {
		allowed, err := s.CanComputerReadRun(ctx, scope, item.runID)
		if err != nil {
			return ComputerRunPage{}, err
		}
		if !allowed {
			return ComputerRunPage{}, protocolError(contract.ErrorForbidden, "Computer Run list crossed its current scope")
		}
		record, err := s.GetRun(ctx, item.runID)
		if err != nil {
			return ComputerRunPage{}, err
		}
		page.Runs = append(page.Runs, record)
	}
	if hasMore && len(listed) > 0 {
		last := listed[len(listed)-1]
		page.NextCursor = encodeComputerRunCursor(computerRunCursor{ComputerID: scope.ComputerID,
			ComputerStorageGeneration: scope.ComputerStorageGeneration, IncludeDescendants: includeDescendants,
			CreatedNS: last.createdNS, RunID: last.runID})
	}
	return page, nil
}

type computerTokenAuditRow struct {
	GrantID, Operation, ComputerID, ComputerAttemptID, HostNodeID, Reason       string
	StorageGeneration, SubmitIntentRevision, AuthorityGeneration, GrantRevision int64
	SubmitMaxInflight                                                           int
	TokenHash                                                                   []byte
	OccurredNS                                                                  int64
}

func appendComputerTokenAudit(ctx context.Context, tx *sql.Tx, row computerTokenAuditRow) error {
	if _, err := tx.ExecContext(ctx, `INSERT INTO computer_token_audit(
		audit_id, grant_id, operation, computer_id, computer_attempt_id, computer_storage_generation,
		submit_intent_revision, host_node_id, l3_authority_generation, grant_revision, submit_max_inflight,
		token_hash, reason, occurred_ns
	) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, newID("computertokenaudit"), row.GrantID,
		row.Operation, row.ComputerID, row.ComputerAttemptID, row.StorageGeneration, row.SubmitIntentRevision,
		row.HostNodeID, row.AuthorityGeneration, row.GrantRevision, row.SubmitMaxInflight, row.TokenHash, row.Reason, row.OccurredNS); err != nil {
		return internalError(err, "append immutable Computer token audit")
	}
	return nil
}

func revokeComputerGrantRows(ctx context.Context, tx *sql.Tx, predicate string, args []any, revokedNS int64, reason string) error {
	query := `SELECT grant_id, computer_id, computer_attempt_id, computer_storage_generation, submit_intent_revision,
		host_node_id, l3_authority_generation, grant_revision, submit_max_inflight, token_hash
		FROM computer_token_grants WHERE ` + predicate
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return internalError(err, "list Computer grants for revocation")
	}
	var grants []computerTokenAuditRow
	for rows.Next() {
		var row computerTokenAuditRow
		if err := rows.Scan(&row.GrantID, &row.ComputerID, &row.ComputerAttemptID, &row.StorageGeneration,
			&row.SubmitIntentRevision, &row.HostNodeID, &row.AuthorityGeneration, &row.GrantRevision,
			&row.SubmitMaxInflight, &row.TokenHash); err != nil {
			rows.Close()
			return internalError(err, "scan Computer grant for revocation")
		}
		row.Operation, row.Reason, row.OccurredNS = "revoked", reason, revokedNS
		grants = append(grants, row)
	}
	if err := rows.Close(); err != nil {
		return internalError(err, "close Computer grants for revocation")
	}
	for _, grant := range grants {
		if _, err := tx.ExecContext(ctx, `UPDATE computer_token_grants SET revoked_ns=?, revocation_reason=?
			WHERE grant_id=? AND revoked_ns IS NULL`, revokedNS, reason, grant.GrantID); err != nil {
			return internalError(err, "revoke Computer token grant")
		}
		if err := appendComputerTokenAudit(ctx, tx, grant); err != nil {
			return err
		}
	}
	return nil
}
