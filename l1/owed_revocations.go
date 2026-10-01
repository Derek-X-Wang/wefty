package l1

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
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

// ComputerRevocationScope is what the revocation right after the mutation
// asks the run ledger to end: every grant of the Computer (`revoke_all`), or
// exactly one completed attempt's grants (`attempt`). A late settlement is
// always attempt-scoped, whatever the row's scope.
type ComputerRevocationScope string

const (
	ComputerRevocationScopeRevokeAll ComputerRevocationScope = "revoke_all"
	ComputerRevocationScopeAttempt   ComputerRevocationScope = "attempt"
)

const (
	owedRevocationSettledRevoked         = "revoked"
	owedRevocationSettledNothingToRevoke = "nothing_to_revoke"
	owedRevocationSettledNoRunLedger     = "no_run_ledger"
	// owedRevocationSettledHostDead closes a row whose host Node L1 marked
	// dead: no heartbeat will ever settle it. Its passes are already refused,
	// because every bearer use re-proves the live scope with L1, the mutation
	// that owed the row changed that scope, and that proof refuses every
	// attempt on a dead host whatever its lease says (#623). The host's next
	// boot revokes every grant an earlier boot of it minted (the
	// boot-session-fenced revoke-host).
	owedRevocationSettledHostDead = "host_dead"
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
// RecordedAttemptIDs are the attempts whose passes it ends: those that could
// hold one when the mutation began. RevokedAttemptIDs are the ones the run
// ledger has already taken.
type OwedComputerRevocation struct {
	RevocationID       int64                   `json:"revocation_id"`
	ComputerID         string                  `json:"computer_id"`
	HostNodeID         string                  `json:"host_node_id"`
	Verb               ComputerRevocationVerb  `json:"verb"`
	Reason             string                  `json:"reason"`
	Scope              ComputerRevocationScope `json:"scope"`
	ComputerAttemptID  string                  `json:"computer_attempt_id,omitempty"`
	RecordedAttemptIDs []string                `json:"recorded_attempt_ids"`
	RevokedAttemptIDs  []string                `json:"revoked_attempt_ids,omitempty"`
	CreatedAt          time.Time               `json:"created_at"`
	SettleFailures     int                     `json:"settle_failures"`
	LastFailure        string                  `json:"last_failure,omitempty"`
	LastFailureAt      *time.Time              `json:"last_failure_at,omitempty"`
}

// ComputerRevocationSettlement is the audit record a settled row keeps: the
// run ledger's receipt for the revoke-all sent right after the mutation, or
// one attempt-scoped receipt for every recorded attempt.
type ComputerRevocationSettlement struct {
	RevokeAll *contract.ComputerTokenRevocationReceipt  `json:"revoke_all,omitempty"`
	Attempts  []contract.ComputerTokenRevocationReceipt `json:"attempts,omitempty"`
}

// sqliteBusyTimeout is how long a connection of L1's main pool waits on
// SQLite's write lock. The driver does not interrupt that wait when a context
// ends, so no deadline can shorten it.
const sqliteBusyTimeout = 5 * time.Second

// owedRevocationWriteWait is the fixed lock wait of the handle owed-revocation
// settlement writes use on a heartbeat (Store.settlementDB). A heartbeat
// starts a write only while at least this wait plus owedRevocationWriteMargin
// of its budget is left, so a held lock costs it at most one such wait.
const (
	owedRevocationWriteWait   = 250 * time.Millisecond
	owedRevocationWriteMargin = 50 * time.Millisecond
)

// sqlSession is what owed-revocation writes run against: the main pool, or
// the settlement handle.
type sqlSession interface {
	BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

// owedRevocationWriteBudget admits heartbeat settlement writes while enough
// of the heartbeat's budget is left for one full lock wait.
type owedRevocationWriteBudget struct {
	deadline time.Time
	skipped  bool
}

func (budget *owedRevocationWriteBudget) allow() bool {
	if budget.skipped || time.Until(budget.deadline) < owedRevocationWriteWait+owedRevocationWriteMargin {
		budget.skipped = true
		return false
	}
	return true
}

type owedRevocationRecord struct {
	computerID string
	hostNodeID string
	verb       ComputerRevocationVerb
	reason     string
	// attemptID makes the row attempt-scoped: a completion ends exactly
	// this attempt.
	attemptID string
	// holdingAttempts are, for a Computer-wide row, the attempts that could
	// hold a pass when the mutation began (computerAttemptsHoldingAuthority).
	holdingAttempts []string
}

func computerHostNodeID(computer Computer) string {
	if computer.BoundNodeID != "" {
		return computer.BoundNodeID
	}
	return computer.PlacementNodeID
}

// computerAttemptsHoldingAuthority returns the attempts of a Computer's
// current Job that could hold an L3 pass. L3 mints a pass only against L1's
// live scope proof, which admits only a claimed or running attempt of the
// current Job (ProveComputerTokenScope), and re-proves that scope on every
// use. A verb calls this at the start of its transaction, before it marks
// those attempts lost or clears the Job's current attempt. A grant of an
// attempt already lost before the mutation is unusable and is not recorded:
// the agent's attempt-end revocation or its revoke-host on restart ends it.
func computerAttemptsHoldingAuthority(ctx context.Context, q queryer, jobID string) ([]string, error) {
	rows, err := q.QueryContext(ctx, `SELECT attempt_id FROM attempts WHERE job_id=? AND state IN (?, ?)
		ORDER BY attempt_id`, jobID, contract.AttemptClaimed, contract.AttemptRunning)
	if err != nil {
		return nil, internalError(err, "read Computer attempts holding authority")
	}
	defer rows.Close()
	attemptIDs := []string{}
	for rows.Next() {
		var attemptID string
		if err := rows.Scan(&attemptID); err != nil {
			return nil, internalError(err, "scan Computer attempt holding authority")
		}
		attemptIDs = append(attemptIDs, attemptID)
	}
	if err := rows.Err(); err != nil {
		return nil, internalError(err, "read Computer attempts holding authority")
	}
	return attemptIDs, nil
}

// recordOwedComputerRevocation writes the revocation an authority-losing
// mutation owes inside that mutation's own transaction, so the mutation
// cannot commit without it.
func recordOwedComputerRevocation(ctx context.Context, tx *sql.Tx, record owedRevocationRecord, now time.Time) (int64, error) {
	scope := ComputerRevocationScopeRevokeAll
	recorded := record.holdingAttempts
	if record.attemptID != "" {
		scope = ComputerRevocationScopeAttempt
		recorded = []string{record.attemptID}
	}
	if recorded == nil {
		recorded = []string{}
	}
	payload, err := json.Marshal(recorded)
	if err != nil {
		return 0, internalError(err, "encode owed Computer revocation attempts")
	}
	result, err := tx.ExecContext(ctx, `INSERT INTO computer_owed_revocations(
		computer_id, host_node_id, verb, reason, scope, computer_attempt_id, recorded_attempt_ids_json, created_ns
	) VALUES(?, ?, ?, ?, ?, ?, ?, ?)`, record.computerID, record.hostNodeID, record.verb, record.reason, scope,
		record.attemptID, payload, now.UnixNano())
	if err != nil {
		return 0, internalError(err, "record owed Computer revocation")
	}
	revocationID, err := result.LastInsertId()
	if err != nil {
		return 0, internalError(err, "read owed Computer revocation identity")
	}
	return revocationID, nil
}

const owedRevocationColumns = `revocation_id, computer_id, host_node_id, verb, reason, scope, computer_attempt_id,
	recorded_attempt_ids_json, attempt_receipts_json, created_ns, settle_failures, last_failure, last_failure_ns, settled_ns`

type owedRevocationRow struct {
	owed     OwedComputerRevocation
	receipts []contract.ComputerTokenRevocationReceipt
	settled  bool
}

// pending lists the recorded attempts the run ledger has not yet taken.
func (row owedRevocationRow) pending() []string {
	var pending []string
	for _, attemptID := range row.owed.RecordedAttemptIDs {
		if !slices.Contains(row.owed.RevokedAttemptIDs, attemptID) {
			pending = append(pending, attemptID)
		}
	}
	return pending
}

func scanOwedRevocation(scanner interface{ Scan(...any) error }) (owedRevocationRow, error) {
	var row owedRevocationRow
	var recorded, receipts []byte
	var createdNS int64
	var lastFailureNS, settledNS sql.NullInt64
	if err := scanner.Scan(&row.owed.RevocationID, &row.owed.ComputerID, &row.owed.HostNodeID, &row.owed.Verb,
		&row.owed.Reason, &row.owed.Scope, &row.owed.ComputerAttemptID, &recorded, &receipts, &createdNS,
		&row.owed.SettleFailures, &row.owed.LastFailure, &lastFailureNS, &settledNS); err != nil {
		return owedRevocationRow{}, err
	}
	if err := json.Unmarshal(recorded, &row.owed.RecordedAttemptIDs); err != nil {
		return owedRevocationRow{}, err
	}
	if err := json.Unmarshal(receipts, &row.receipts); err != nil {
		return owedRevocationRow{}, err
	}
	for _, receipt := range row.receipts {
		row.owed.RevokedAttemptIDs = append(row.owed.RevokedAttemptIDs, receipt.ComputerAttemptID)
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

func (s *Store) owedComputerRevocation(ctx context.Context, revocationID int64) (owedRevocationRow, error) {
	row, err := scanOwedRevocation(s.db.QueryRowContext(ctx, `SELECT `+owedRevocationColumns+`
		FROM computer_owed_revocations WHERE revocation_id=?`, revocationID))
	if errors.Is(err, sql.ErrNoRows) {
		return owedRevocationRow{}, protocolError(contract.ErrorNotFound, "owed Computer revocation %d was not found", revocationID)
	}
	if err != nil {
		return owedRevocationRow{}, internalError(err, "read owed Computer revocation")
	}
	return row, nil
}

// listNodeOwedComputerRevocations returns the oldest revocations still owed
// for Computers hosted on nodeID.
func (s *Store) listNodeOwedComputerRevocations(ctx context.Context, nodeID string, limit int) ([]owedRevocationRow, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+owedRevocationColumns+` FROM computer_owed_revocations
		WHERE host_node_id=? AND settled_ns IS NULL ORDER BY revocation_id LIMIT ?`, nodeID, limit)
	if err != nil {
		return nil, internalError(err, "list owed Computer revocations")
	}
	defer rows.Close()
	var owed []owedRevocationRow
	for rows.Next() {
		row, err := scanOwedRevocation(rows)
		if err != nil {
			return nil, internalError(err, "scan owed Computer revocation")
		}
		owed = append(owed, row)
	}
	if err := rows.Err(); err != nil {
		return nil, internalError(err, "list owed Computer revocations")
	}
	return owed, nil
}

// settleOwedRevocationTx runs one settlement write against an unsettled row.
// It reports false, writing nothing, when a concurrent settlement already
// closed the row: that earlier settlement's record stands.
func (s *Store) settleOwedRevocationTx(ctx context.Context, session sqlSession, revocationID int64,
	settle func(*sql.Tx, owedRevocationRow, int64) error) (bool, error) {
	tx, err := session.BeginTx(ctx, nil)
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
	if err := settle(tx, row, canonicalTime(s.clock.Now()).UnixNano()); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, internalError(err, "commit owed Computer revocation settlement")
	}
	return true, nil
}

func closeOwedRevocation(ctx context.Context, tx *sql.Tx, revocationID int64, settlement string, record *ComputerRevocationSettlement, nowNS int64) error {
	var payload []byte
	if record != nil {
		var err error
		if payload, err = json.Marshal(record); err != nil {
			return internalError(err, "encode owed Computer revocation settlement")
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE computer_owed_revocations SET settlement=?, settled_ns=?, receipt_json=?
		WHERE revocation_id=? AND settled_ns IS NULL`, settlement, nowNS, payload, revocationID); err != nil {
		return internalError(err, "settle owed Computer revocation")
	}
	return nil
}

// SettleOwedComputerRevocationRevokeAll records the receipt of the revoke-all
// a handler sent right after a Computer-wide row's mutation committed.
func (s *Store) SettleOwedComputerRevocationRevokeAll(ctx context.Context, revocationID int64,
	receipt contract.ComputerTokenRevocationReceipt) (bool, error) {
	return s.settleOwedRevocationTx(ctx, s.db, revocationID, func(tx *sql.Tx, row owedRevocationRow, nowNS int64) error {
		if row.owed.Scope != ComputerRevocationScopeRevokeAll || receipt.ComputerID != row.owed.ComputerID ||
			receipt.ComputerAttemptID != "" || receipt.RestoreOperationRevision != 0 || receipt.CommittedAt.IsZero() {
			return errors.New("the run ledger's receipt does not match the owed Computer-wide revocation")
		}
		return closeOwedRevocation(ctx, tx, revocationID, owedRevocationSettledRevoked,
			&ComputerRevocationSettlement{RevokeAll: &receipt}, nowNS)
	})
}

// RecordOwedComputerAttemptRevocations records attempt-scoped receipts
// against a row, and settles it once every recorded attempt has one. A
// receipt for an attempt the row did not record is refused: a settlement
// never reaches beyond the attempts the mutation ended.
func (s *Store) RecordOwedComputerAttemptRevocations(ctx context.Context, revocationID int64,
	receipts []contract.ComputerTokenRevocationReceipt) (bool, error) {
	return s.recordOwedComputerAttemptRevocations(ctx, s.db, revocationID, receipts)
}

func (s *Store) recordOwedComputerAttemptRevocations(ctx context.Context, session sqlSession, revocationID int64,
	receipts []contract.ComputerTokenRevocationReceipt) (bool, error) {
	settled := false
	_, err := s.settleOwedRevocationTx(ctx, session, revocationID, func(tx *sql.Tx, row owedRevocationRow, nowNS int64) error {
		merged := slices.Clone(row.receipts)
		for _, receipt := range receipts {
			if receipt.ComputerID != row.owed.ComputerID || receipt.RestoreOperationRevision != 0 || receipt.CommittedAt.IsZero() ||
				!slices.Contains(row.owed.RecordedAttemptIDs, receipt.ComputerAttemptID) {
				return errors.New("the run ledger's receipt does not revoke an attempt this revocation recorded")
			}
			if !slices.ContainsFunc(merged, func(have contract.ComputerTokenRevocationReceipt) bool {
				return have.ComputerAttemptID == receipt.ComputerAttemptID
			}) {
				merged = append(merged, receipt)
			}
		}
		slices.SortFunc(merged, func(a, b contract.ComputerTokenRevocationReceipt) int {
			switch {
			case a.ComputerAttemptID < b.ComputerAttemptID:
				return -1
			case a.ComputerAttemptID > b.ComputerAttemptID:
				return 1
			}
			return 0
		})
		payload, err := json.Marshal(merged)
		if err != nil {
			return internalError(err, "encode owed Computer attempt receipts")
		}
		if _, err := tx.ExecContext(ctx, `UPDATE computer_owed_revocations SET attempt_receipts_json=?
			WHERE revocation_id=? AND settled_ns IS NULL`, payload, revocationID); err != nil {
			return internalError(err, "record owed Computer attempt receipts")
		}
		if len(merged) < len(row.owed.RecordedAttemptIDs) {
			return nil
		}
		settled = true
		settlement := owedRevocationSettledRevoked
		var record *ComputerRevocationSettlement
		if len(merged) == 0 {
			settlement = owedRevocationSettledNothingToRevoke
		} else {
			record = &ComputerRevocationSettlement{Attempts: merged}
		}
		return closeOwedRevocation(ctx, tx, revocationID, settlement, record, nowNS)
	})
	return settled, err
}

// SettleOwedComputerRevocationWithoutRunLedger closes a row on an
// installation that names no run ledger. Such an installation mints no
// Computer passes, so there is nothing to revoke and nothing to wait for; the
// row says so rather than staying owed forever.
func (s *Store) SettleOwedComputerRevocationWithoutRunLedger(ctx context.Context, revocationID int64) (bool, error) {
	return s.settleOwedComputerRevocationWithoutRunLedger(ctx, s.db, revocationID)
}

func (s *Store) settleOwedComputerRevocationWithoutRunLedger(ctx context.Context, session sqlSession, revocationID int64) (bool, error) {
	return s.settleOwedRevocationTx(ctx, session, revocationID, func(tx *sql.Tx, _ owedRevocationRow, nowNS int64) error {
		return closeOwedRevocation(ctx, tx, revocationID, owedRevocationSettledNoRunLedger, nil, nowNS)
	})
}

// RecordOwedComputerRevocationFailure notes why a settlement attempt did not
// land, for the operator reading the Computer. The row stays owed.
func (s *Store) RecordOwedComputerRevocationFailure(ctx context.Context, revocationID int64, cause string) error {
	return s.recordOwedComputerRevocationFailure(ctx, s.db, revocationID, cause)
}

func (s *Store) recordOwedComputerRevocationFailure(ctx context.Context, session sqlSession, revocationID int64, cause string) error {
	if len(cause) > maxOwedRevocationFailureBytes {
		cause = cause[:maxOwedRevocationFailureBytes]
		for !utf8.ValidString(cause) {
			cause = cause[:len(cause)-1]
		}
	}
	if _, err := session.ExecContext(ctx, `UPDATE computer_owed_revocations
		SET settle_failures=settle_failures+1, last_failure=?, last_failure_ns=?
		WHERE revocation_id=? AND settled_ns IS NULL`, cause, canonicalTime(s.clock.Now()).UnixNano(), revocationID); err != nil {
		return internalError(err, "record owed Computer revocation failure")
	}
	return nil
}
