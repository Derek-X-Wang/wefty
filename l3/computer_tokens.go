package l3

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/hex"
	"errors"
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
