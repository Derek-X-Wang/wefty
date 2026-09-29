package l1

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"
	"unicode/utf8"

	"github.com/Derek-X-Wang/wefty/contract"
)

// ComputerRevocationVerb names the authority-losing Computer mutation an owed
// revocation was written for.
type ComputerRevocationVerb string

const (
	ComputerRevocationVerbStop                ComputerRevocationVerb = "stop"
	ComputerRevocationVerbRestart             ComputerRevocationVerb = "restart"
	ComputerRevocationVerbReset               ComputerRevocationVerb = "reset"
	ComputerRevocationVerbProject             ComputerRevocationVerb = "project"
	ComputerRevocationVerbReimage             ComputerRevocationVerb = "reimage"
	ComputerRevocationVerbRemove              ComputerRevocationVerb = "remove"
	ComputerRevocationVerbGrowAcknowledgement ComputerRevocationVerb = "grow_acknowledgement"
	ComputerRevocationVerbAttemptCompletion   ComputerRevocationVerb = "attempt_completion"
)

// ComputerRevocationScope is what an owed revocation asks the run ledger to
// end: every grant the Computer held when the mutation committed, or exactly
// one completed attempt's grants. A row never changes scope.
type ComputerRevocationScope string

const (
	ComputerRevocationScopeRevokeAll ComputerRevocationScope = "revoke_all"
	ComputerRevocationScopeAttempt   ComputerRevocationScope = "attempt"
)

const (
	owedRevocationSettledRevoked     = "revoked"
	owedRevocationSettledNoRunLedger = "no_run_ledger"
	// MaxOwedRevocationsPerHeartbeat bounds the owed revocations one node
	// heartbeat settles. They share the heartbeat's revocation budget with
	// the pre-restore revocations; the rest wait for the next heartbeat.
	MaxOwedRevocationsPerHeartbeat = 16
	maxOwedRevocationFailureBytes  = 512
)

// OwedComputerRevocation is an explicit L3 revocation an authority-losing
// Computer mutation committed but the run ledger has not yet taken (#554).
// L3's live-scope check already refuses the Computer's old passes; this is
// the defense-in-depth revocation and its audit receipt, still outstanding.
type OwedComputerRevocation struct {
	RevocationID      int64                   `json:"revocation_id"`
	ComputerID        string                  `json:"computer_id"`
	HostNodeID        string                  `json:"host_node_id"`
	Verb              ComputerRevocationVerb  `json:"verb"`
	Reason            string                  `json:"reason"`
	Scope             ComputerRevocationScope `json:"scope"`
	ComputerAttemptID string                  `json:"computer_attempt_id,omitempty"`
	CreatedAt         time.Time               `json:"created_at"`
	SettleFailures    int                     `json:"settle_failures"`
	LastFailure       string                  `json:"last_failure,omitempty"`
	LastFailureAt     *time.Time              `json:"last_failure_at,omitempty"`
}

// owedRevocationWork is one owed row and the exact run-ledger request that
// settles it now.
type owedRevocationWork struct {
	owed    OwedComputerRevocation
	request ComputerTokenRevocation
	settled bool
}

type owedRevocationRecord struct {
	computerID string
	hostNodeID string
	verb       ComputerRevocationVerb
	reason     string
	attemptID  string
}

func computerHostNodeID(computer Computer) string {
	if computer.BoundNodeID != "" {
		return computer.BoundNodeID
	}
	return computer.PlacementNodeID
}

// recordOwedComputerRevocation writes the revocation an authority-losing
// mutation owes inside that mutation's own transaction, so the mutation
// cannot commit without it. A Computer-wide row also snapshots every attempt
// the Computer has at commit: those are the only attempts whose passes this
// authority loss ends. An attempt that begins later holds a pass the loss
// never covered, and a late settlement must leave it alone.
func recordOwedComputerRevocation(ctx context.Context, tx *sql.Tx, record owedRevocationRecord, now time.Time) (int64, error) {
	scope := ComputerRevocationScopeRevokeAll
	attemptsAtCommit := []string{}
	if record.attemptID != "" {
		scope = ComputerRevocationScopeAttempt
	} else {
		var err error
		attemptsAtCommit, err = computerAttemptIDs(ctx, tx, record.computerID)
		if err != nil {
			return 0, internalError(err, "snapshot Computer attempts for an owed revocation")
		}
	}
	snapshot, err := json.Marshal(attemptsAtCommit)
	if err != nil {
		return 0, internalError(err, "encode Computer attempts for an owed revocation")
	}
	result, err := tx.ExecContext(ctx, `INSERT INTO computer_owed_revocations(
		computer_id, host_node_id, verb, reason, scope, computer_attempt_id, attempts_at_commit_json, created_ns
	) VALUES(?, ?, ?, ?, ?, ?, ?, ?)`, record.computerID, record.hostNodeID, record.verb, record.reason, scope,
		record.attemptID, snapshot, now.UnixNano())
	if err != nil {
		return 0, internalError(err, "record owed Computer revocation")
	}
	revocationID, err := result.LastInsertId()
	if err != nil {
		return 0, internalError(err, "read owed Computer revocation identity")
	}
	return revocationID, nil
}

func computerAttemptIDs(ctx context.Context, q queryer, computerID string) ([]string, error) {
	rows, err := q.QueryContext(ctx, `SELECT attempts.attempt_id FROM attempts
		JOIN computer_job_projections ON computer_job_projections.job_id=attempts.job_id
		WHERE computer_job_projections.computer_id=? ORDER BY attempts.attempt_id`, computerID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	attemptIDs := []string{}
	for rows.Next() {
		var attemptID string
		if err := rows.Scan(&attemptID); err != nil {
			return nil, err
		}
		attemptIDs = append(attemptIDs, attemptID)
	}
	return attemptIDs, rows.Err()
}

const owedRevocationColumns = `revocation_id, computer_id, host_node_id, verb, reason, scope, computer_attempt_id,
	attempts_at_commit_json, created_ns, settle_failures, last_failure, last_failure_ns, settled_ns`

type owedRevocationRow struct {
	owed             OwedComputerRevocation
	attemptsAtCommit []byte
	settled          bool
}

func scanOwedRevocation(scanner interface{ Scan(...any) error }) (owedRevocationRow, error) {
	var row owedRevocationRow
	var createdNS int64
	var lastFailureNS, settledNS sql.NullInt64
	if err := scanner.Scan(&row.owed.RevocationID, &row.owed.ComputerID, &row.owed.HostNodeID, &row.owed.Verb,
		&row.owed.Reason, &row.owed.Scope, &row.owed.ComputerAttemptID, &row.attemptsAtCommit, &createdNS,
		&row.owed.SettleFailures, &row.owed.LastFailure, &lastFailureNS, &settledNS); err != nil {
		return owedRevocationRow{}, err
	}
	row.owed.CreatedAt = time.Unix(0, createdNS).UTC()
	if lastFailureNS.Valid {
		at := time.Unix(0, lastFailureNS.Int64).UTC()
		row.owed.LastFailureAt = &at
	}
	row.settled = settledNS.Valid
	return row, nil
}

// readOwedComputerRevocations lists a Computer's revocations still owed,
// oldest first, for the Computer read model.
func readOwedComputerRevocations(ctx context.Context, q queryer, computerID string) ([]OwedComputerRevocation, error) {
	rows, err := q.QueryContext(ctx, `SELECT `+owedRevocationColumns+` FROM computer_owed_revocations
		WHERE computer_id=? AND settled_ns IS NULL ORDER BY revocation_id`, computerID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var owed []OwedComputerRevocation
	for rows.Next() {
		row, err := scanOwedRevocation(rows)
		if err != nil {
			return nil, err
		}
		owed = append(owed, row.owed)
	}
	return owed, rows.Err()
}

// owedRevocationRequest builds the run-ledger request that settles one row
// now. An attempt row asks for exactly its attempt, never more. A
// Computer-wide row asks for a revoke-all that preserves every attempt the
// Computer began after the mutation committed: a restart or reimage may have
// minted the next attempt's pass while the revocation was owed, and the
// authority loss this row records never covered it (the #553 hazard, moved
// from completion to settlement time).
func owedRevocationRequest(ctx context.Context, q queryer, row owedRevocationRow) (ComputerTokenRevocation, error) {
	if row.owed.Scope == ComputerRevocationScopeAttempt {
		return ComputerTokenRevocation{ComputerID: row.owed.ComputerID, ComputerAttemptID: row.owed.ComputerAttemptID,
			Reason: row.owed.Reason}, nil
	}
	var atCommit []string
	if err := json.Unmarshal(row.attemptsAtCommit, &atCommit); err != nil {
		return ComputerTokenRevocation{}, internalError(err, "decode owed revocation attempt snapshot")
	}
	current, err := computerAttemptIDs(ctx, q, row.owed.ComputerID)
	if err != nil {
		return ComputerTokenRevocation{}, internalError(err, "read Computer attempts for an owed revocation")
	}
	var preserve []string
	for _, attemptID := range current {
		if !slices.Contains(atCommit, attemptID) {
			preserve = append(preserve, attemptID)
		}
	}
	if len(preserve) > MaxPreservedComputerAttempts {
		return ComputerTokenRevocation{}, fmt.Errorf("Computer %s began %d attempts after the revocation was owed, more than one revocation can preserve (%d)",
			row.owed.ComputerID, len(preserve), MaxPreservedComputerAttempts)
	}
	return ComputerTokenRevocation{ComputerID: row.owed.ComputerID, NewSubmitIntentRevision: 1, RevokeAll: true,
		PreserveComputerAttemptIDs: preserve, Reason: row.owed.Reason}, nil
}

// MaxPreservedComputerAttempts mirrors the run ledger's bound on one
// narrowed revoke-all.
const MaxPreservedComputerAttempts = 256

func (s *Store) owedComputerRevocationWork(ctx context.Context, revocationID int64) (owedRevocationWork, error) {
	row, err := scanOwedRevocation(s.db.QueryRowContext(ctx, `SELECT `+owedRevocationColumns+`
		FROM computer_owed_revocations WHERE revocation_id=?`, revocationID))
	if err != nil {
		return owedRevocationWork{}, internalError(err, "read owed Computer revocation")
	}
	if row.settled {
		return owedRevocationWork{owed: row.owed, settled: true}, nil
	}
	request, err := owedRevocationRequest(ctx, s.db, row)
	if err != nil {
		return owedRevocationWork{}, err
	}
	return owedRevocationWork{owed: row.owed, request: request}, nil
}

// listNodeOwedComputerRevocations returns the oldest revocations still owed
// for Computers hosted on nodeID, each with the request that settles it now.
// A row whose request cannot be built is reported through failed rather than
// failing the node's heartbeat.
func (s *Store) listNodeOwedComputerRevocations(ctx context.Context, nodeID string, limit int) ([]owedRevocationWork, map[int64]error, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+owedRevocationColumns+` FROM computer_owed_revocations
		WHERE host_node_id=? AND settled_ns IS NULL ORDER BY revocation_id LIMIT ?`, nodeID, limit)
	if err != nil {
		return nil, nil, internalError(err, "list owed Computer revocations")
	}
	var owed []owedRevocationRow
	for rows.Next() {
		row, scanErr := scanOwedRevocation(rows)
		if scanErr != nil {
			rows.Close()
			return nil, nil, internalError(scanErr, "scan owed Computer revocation")
		}
		owed = append(owed, row)
	}
	if err := rows.Close(); err != nil {
		return nil, nil, internalError(err, "close owed Computer revocations")
	}
	work := make([]owedRevocationWork, 0, len(owed))
	failed := map[int64]error{}
	for _, row := range owed {
		request, requestErr := owedRevocationRequest(ctx, s.db, row)
		if requestErr != nil {
			failed[row.owed.RevocationID] = requestErr
			continue
		}
		work = append(work, owedRevocationWork{owed: row.owed, request: request})
	}
	return work, failed, nil
}

// owedRevocationReceiptMatches proves the run ledger answered the request
// that was sent: the same Computer, the same scope, the same attempt, and for
// a narrowed revoke-all the same preserved attempts.
func owedRevocationReceiptMatches(owed OwedComputerRevocation, request ComputerTokenRevocation, receipt contract.ComputerTokenRevocationReceipt) error {
	switch {
	case receipt.ComputerID != owed.ComputerID || request.ComputerID != owed.ComputerID:
		return errors.New("the run ledger's receipt names a different Computer")
	case receipt.CommittedAt.IsZero():
		return errors.New("the run ledger's receipt has no commit time")
	case receipt.RestoreOperationRevision != 0:
		return errors.New("the run ledger's receipt is for a pre-restore revocation")
	case owed.Scope == ComputerRevocationScopeAttempt &&
		(request.RevokeAll || request.ComputerAttemptID != owed.ComputerAttemptID || receipt.ComputerAttemptID != owed.ComputerAttemptID):
		return errors.New("the run ledger's receipt does not revoke exactly the owed attempt")
	case owed.Scope == ComputerRevocationScopeRevokeAll &&
		(!request.RevokeAll || receipt.ComputerAttemptID != "" ||
			!slices.Equal(receipt.PreservedComputerAttemptIDs, request.PreserveComputerAttemptIDs)):
		return errors.New("the run ledger's receipt does not match the owed Computer-wide revocation")
	}
	return nil
}

// SettleOwedComputerRevocation records the run ledger's receipt as the audit
// record of one owed revocation. It settles a row at most once: a
// concurrent settlement that got there first keeps its receipt, and this
// call reports false. The run ledger's revocations are idempotent, so the
// second revocation that raced it changed nothing there either.
func (s *Store) SettleOwedComputerRevocation(ctx context.Context, revocationID int64, request ComputerTokenRevocation,
	receipt contract.ComputerTokenRevocationReceipt) (bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, internalError(err, "begin owed Computer revocation settlement")
	}
	defer tx.Rollback()
	row, err := scanOwedRevocation(tx.QueryRowContext(ctx, `SELECT `+owedRevocationColumns+`
		FROM computer_owed_revocations WHERE revocation_id=?`, revocationID))
	if errors.Is(err, sql.ErrNoRows) {
		return false, protocolError(contract.ErrorNotFound, "owed Computer revocation %d was not found", revocationID)
	}
	if err != nil {
		return false, internalError(err, "read owed Computer revocation for settlement")
	}
	if row.settled {
		return false, nil
	}
	if err := owedRevocationReceiptMatches(row.owed, request, receipt); err != nil {
		return false, err
	}
	payload, err := json.Marshal(receipt)
	if err != nil {
		return false, internalError(err, "encode owed Computer revocation receipt")
	}
	result, err := tx.ExecContext(ctx, `UPDATE computer_owed_revocations SET settlement=?, settled_ns=?, receipt_json=?
		WHERE revocation_id=? AND settled_ns IS NULL`, owedRevocationSettledRevoked,
		canonicalTime(s.clock.Now()).UnixNano(), payload, revocationID)
	if err != nil {
		return false, internalError(err, "settle owed Computer revocation")
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, internalError(err, "commit owed Computer revocation settlement")
	}
	return true, nil
}

// SettleOwedComputerRevocationWithoutRunLedger closes a row on an
// installation that names no run ledger. Such an installation mints no
// Computer passes, so there is nothing to revoke and nothing to wait for; the
// row says so rather than staying owed forever.
func (s *Store) SettleOwedComputerRevocationWithoutRunLedger(ctx context.Context, revocationID int64) (bool, error) {
	result, err := s.db.ExecContext(ctx, `UPDATE computer_owed_revocations SET settlement=?, settled_ns=?
		WHERE revocation_id=? AND settled_ns IS NULL`, owedRevocationSettledNoRunLedger,
		canonicalTime(s.clock.Now()).UnixNano(), revocationID)
	if err != nil {
		return false, internalError(err, "close owed Computer revocation without a run ledger")
	}
	changed, err := result.RowsAffected()
	return changed == 1, err
}

// RecordOwedComputerRevocationFailure notes why a settlement attempt did not
// land, for the operator reading the Computer. The row stays owed.
func (s *Store) RecordOwedComputerRevocationFailure(ctx context.Context, revocationID int64, cause string) error {
	if len(cause) > maxOwedRevocationFailureBytes {
		cause = cause[:maxOwedRevocationFailureBytes]
		for !utf8.ValidString(cause) {
			cause = cause[:len(cause)-1]
		}
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE computer_owed_revocations
		SET settle_failures=settle_failures+1, last_failure=?, last_failure_ns=?
		WHERE revocation_id=? AND settled_ns IS NULL`, cause, canonicalTime(s.clock.Now()).UnixNano(), revocationID); err != nil {
		return internalError(err, "record owed Computer revocation failure")
	}
	return nil
}
