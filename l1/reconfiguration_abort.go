package l1

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"

	"github.com/Derek-X-Wang/wefty/contract"
)

func reconfigurationAbortRequestHash(request ComputerReconfigurationAbortRequest) (string, error) {
	payload, err := json.Marshal(struct {
		IntentRevision    int64  `json:"intent_revision"`
		StorageID         string `json:"storage_id"`
		StorageGeneration int64  `json:"storage_generation"`
		IdempotencyKey    string `json:"idempotency_key"`
		Actor             string `json:"actor"`
	}{request.IntentRevision, request.StorageID, request.StorageGeneration, request.IdempotencyKey, request.Actor})
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(payload)
	return hex.EncodeToString(hash[:]), nil
}

// AbortComputerReconfiguration is the operator escape hatch for an operation
// whose exact bound Node has died. It only releases L1 orchestration authority;
// it never claims that node-local bytes were removed and never fabricates a
// stop intent. Recovery remains an explicit restart or removal decision.
func (s *Store) AbortComputerReconfiguration(ctx context.Context, computerID string,
	request ComputerReconfigurationAbortRequest,
) (Computer, bool, error) {
	computerID = strings.TrimSpace(computerID)
	request.Actor = strings.TrimSpace(request.Actor)
	request.IdempotencyKey = strings.TrimSpace(request.IdempotencyKey)
	if computerID == "" || request.IdempotencyKey == "" {
		return Computer{}, false, protocolError(contract.ErrorInvalidRequest,
			"computer_id and idempotency_key are required")
	}
	requestHash, err := reconfigurationAbortRequestHash(request)
	if err != nil {
		return Computer{}, false, internalError(err, "encode Computer reconfiguration abort")
	}
	now := canonicalTime(s.clock.Now())
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Computer{}, false, internalError(err, "begin Computer reconfiguration abort")
	}
	defer tx.Rollback()
	computer, err := readComputerAuthority(ctx, tx, computerID, now)
	if errors.Is(err, sql.ErrNoRows) {
		return Computer{}, false, protocolError(contract.ErrorNotFound, "Computer %q was not found", computerID)
	}
	if err != nil {
		return Computer{}, false, internalError(err, "read Computer reconfiguration abort target")
	}
	var replayHash string
	if replayErr := tx.QueryRowContext(ctx, `SELECT request_hash FROM computer_reconfiguration_aborts
		WHERE computer_id=? AND idempotency_key=?`, computerID, request.IdempotencyKey).Scan(&replayHash); replayErr == nil {
		if replayHash != requestHash {
			return Computer{}, false, protocolError(contract.ErrorIdempotencyConflict,
				"Computer reconfiguration abort idempotency key was reused with different authority")
		}
		if err := tx.Commit(); err != nil {
			return Computer{}, false, internalError(err, "commit Computer reconfiguration abort replay")
		}
		return computer, true, nil
	} else if !errors.Is(replayErr, sql.ErrNoRows) {
		return Computer{}, false, internalError(replayErr, "read Computer reconfiguration abort replay")
	}
	if err := validateComputerPrecondition(computer, request.ComputerMutationPrecondition); err != nil {
		return Computer{}, false, err
	}
	if computer.ReconfigurationRevision == nil || *computer.ReconfigurationRevision != computer.IntentRevision {
		return Computer{}, false, protocolError(contract.ErrorConflict,
			"Computer %q has no abortable reconfiguration authority", computerID)
	}
	switch computer.ReconfigurationPhase {
	case ComputerReconfigurationBackingUp, ComputerReconfigurationResetting, ComputerReconfigurationReimaging,
		ComputerReconfigurationGrowing, ComputerReconfigurationExporting, ComputerReconfigurationImporting:
	default:
		return Computer{}, false, protocolError(contract.ErrorConflict,
			"Computer %q reconfiguration phase %q is not abortable", computerID, computer.ReconfigurationPhase)
	}
	boundNodeID := computer.BoundNodeID
	if boundNodeID == "" {
		boundNodeID = computer.CurrentJob.BoundNodeID
	}
	if boundNodeID == "" {
		var query string
		switch computer.ReconfigurationPhase {
		case ComputerReconfigurationBackingUp:
			query = `SELECT bound_node_id FROM computer_backup_operations WHERE computer_id=? AND operation_revision=?`
		case ComputerReconfigurationResetting:
			query = `SELECT bound_node_id FROM computer_storage_resets WHERE computer_id=? AND intent_revision=?`
		case ComputerReconfigurationReimaging:
			query = `SELECT bound_node_id FROM computer_reimage_operations WHERE computer_id=? AND operation_revision=?`
		case ComputerReconfigurationGrowing:
			query = `SELECT bound_node_id FROM computer_storage_grows WHERE computer_id=? AND operation_revision=?`
		case ComputerReconfigurationExporting:
			query = `SELECT bound_node_id FROM computer_custody_exports WHERE computer_id=? AND operation_revision=?`
		case ComputerReconfigurationImporting:
			query = `SELECT bound_node_id FROM computer_storage_copy_operations WHERE destination_computer_id=? AND operation_revision=? AND operation='import'`
		}
		if query != "" {
			if err := tx.QueryRowContext(ctx, query, computerID, computer.IntentRevision).Scan(&boundNodeID); err != nil {
				return Computer{}, false, internalError(err, "read aborted Computer operation binding")
			}
		}
	}
	if boundNodeID == "" {
		return Computer{}, false, protocolError(contract.ErrorConflict,
			"Computer %q has no bound Node whose loss can authorize abort", computerID)
	}
	var nodeState contract.NodeState
	if err := tx.QueryRowContext(ctx, `SELECT state FROM nodes WHERE node_id=?`, boundNodeID).Scan(&nodeState); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Computer{}, false, protocolError(contract.ErrorConflict, "bound node %q was not found", boundNodeID)
		}
		return Computer{}, false, internalError(err, "read aborted Computer bound Node")
	}
	if nodeState != contract.NodeDead {
		return Computer{}, false, protocolErrorWithDetails(contract.ErrorConflict, map[string]any{
			"computer_id": computerID, "bound_node_id": boundNodeID, "node_state": nodeState,
		}, "Computer reconfiguration abort requires a dead bound Node")
	}
	abortedRevision := computer.IntentRevision
	abortedPhase := computer.ReconfigurationPhase
	switch abortedPhase {
	case ComputerReconfigurationBackingUp:
		if _, err := tx.ExecContext(ctx, `UPDATE computer_backup_operations SET status='superseded'
			WHERE computer_id=? AND operation_revision=? AND status='planned'`, computerID, abortedRevision); err != nil {
			return Computer{}, false, internalError(err, "supersede aborted Computer Backup")
		}
	case ComputerReconfigurationResetting:
		if _, err := tx.ExecContext(ctx, `UPDATE computer_storage_resets SET status='superseded'
			WHERE computer_id=? AND intent_revision=? AND status IN ('reserved', 'prepared', 'published')`, computerID, abortedRevision); err != nil {
			return Computer{}, false, internalError(err, "supersede aborted Computer Storage reset")
		}
		if _, err := tx.ExecContext(ctx, `UPDATE computer_storage_generations SET phase='retired', retired_ns=?
			WHERE computer_id=? AND reset_revision=? AND phase='staging'`, now.UnixNano(), computerID, abortedRevision); err != nil {
			return Computer{}, false, internalError(err, "retire aborted Computer Storage staging generation")
		}
	case ComputerReconfigurationReimaging:
		if _, err := tx.ExecContext(ctx, `UPDATE computer_job_projections SET retired_ns=?, chown=0
			WHERE computer_id=? AND current=0 AND retired_ns IS NULL AND job_id=(
				SELECT job_id FROM computer_intent_history WHERE computer_id=? AND intent_revision=?)`,
			now.UnixNano(), computerID, computerID, abortedRevision); err != nil {
			return Computer{}, false, internalError(err, "retire aborted Computer reimage projection")
		}
		if _, err := tx.ExecContext(ctx, `UPDATE computer_reimage_operations SET status='superseded', completed_ns=?
			WHERE computer_id=? AND operation_revision=? AND status IN ('planned', 'preflight_verified')`,
			now.UnixNano(), computerID, abortedRevision); err != nil {
			return Computer{}, false, internalError(err, "supersede aborted Computer reimage")
		}
	case ComputerReconfigurationGrowing:
		if _, err := tx.ExecContext(ctx, `UPDATE computer_storage_grows SET status='superseded',
			completed_ns=? WHERE computer_id=? AND operation_revision=? AND status='planned'`,
			now.UnixNano(), computerID, abortedRevision); err != nil {
			return Computer{}, false, internalError(err, "supersede aborted Computer grow")
		}
	case ComputerReconfigurationExporting:
		if _, err := tx.ExecContext(ctx, `UPDATE computer_custody_exports SET status='failed',
			failure_code='aborted_dead_node', completed_ns=? WHERE computer_id=? AND operation_revision=? AND status='planned'`,
			now.UnixNano(), computerID, abortedRevision); err != nil {
			return Computer{}, false, internalError(err, "fail aborted Custody export")
		}
	case ComputerReconfigurationImporting:
		if _, err := tx.ExecContext(ctx, `UPDATE computer_storage_copy_operations SET status='superseded', preparation_outcome_json=NULL,
			preparation_acknowledgement_key=NULL, preparation_acknowledgement_hash=NULL,
			failure_code='aborted_dead_node', completed_ns=? WHERE destination_computer_id=? AND operation_revision=?
			AND operation='import' AND status IN ('reserved', 'prepared')`, now.UnixNano(), computerID, abortedRevision); err != nil {
			return Computer{}, false, internalError(err, "supersede aborted Custody import")
		}
		if _, err := tx.ExecContext(ctx, `UPDATE computer_storage_generations SET phase='retired', retired_ns=?
			WHERE computer_id=? AND reset_revision=? AND phase='staging'`, now.UnixNano(), computerID, abortedRevision); err != nil {
			return Computer{}, false, internalError(err, "retire aborted Custody import staging generation")
		}
		if _, err := tx.ExecContext(ctx, `UPDATE computers SET name='aborted-import-' || computer_id WHERE computer_id=?`, computerID); err != nil {
			return Computer{}, false, internalError(err, "release aborted Custody import name")
		}
	}
	if err := s.reconfigurationCheckpoint("abort_artifacts_superseded"); err != nil {
		return Computer{}, false, err
	}
	lastFailure, err := json.Marshal(contract.SpawnFailure{Code: contract.SpawnFailureReconfigurationAborted,
		Message: "Computer reconfiguration was aborted after its bound Node died", NodeID: boundNodeID})
	if err != nil {
		return Computer{}, false, internalError(err, "encode Computer reconfiguration abort observation")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE attempts SET state=?, lease_expires_ns=MIN(lease_expires_ns, ?),
		updated_ns=? WHERE job_id=? AND state IN (?, ?, ?)`, contract.AttemptLost, now.UnixNano(),
		now.UnixNano(), computer.CurrentJobID, contract.AttemptClaimed, contract.AttemptRunning,
		contract.AttemptAwaitingInput); err != nil {
		return Computer{}, false, internalError(err, "fence aborted Computer attempt")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE jobs SET state=?, current_attempt_id=NULL, updated_ns=? WHERE job_id=?`,
		contract.JobStopped, now.UnixNano(), computer.CurrentJobID); err != nil {
		return Computer{}, false, internalError(err, "stop aborted Computer projection")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE service_jobs SET next_restart_at=NULL, last_failure=?,
		published_attempt_id=NULL, healthy_since_ns=NULL WHERE job_id=?`, lastFailure, computer.CurrentJobID); err != nil {
		return Computer{}, false, internalError(err, "record Computer reconfiguration abort observation")
	}
	if err := s.reconfigurationCheckpoint("abort_projection_stopped"); err != nil {
		return Computer{}, false, err
	}
	nextRevision := abortedRevision + 1
	result, err := tx.ExecContext(ctx, `UPDATE computers SET intent_revision=?, applied_revision=?,
		reconfiguration_phase=?, reconfiguration_revision=NULL, updated_ns=?
		WHERE computer_id=? AND intent_revision=? AND reconfiguration_phase=? AND reconfiguration_revision=?`,
		nextRevision, nextRevision, ComputerReconfigurationStable, now.UnixNano(), computerID,
		abortedRevision, abortedPhase, abortedRevision)
	if err != nil {
		return Computer{}, false, internalError(err, "release aborted Computer reconfiguration authority")
	}
	if err := requireComputerCAS(result, computerID, abortedRevision); err != nil {
		return Computer{}, false, err
	}
	if err := insertComputerIntent(ctx, tx, computerID, nextRevision, ComputerIntentAbort,
		computer.DesiredState, computer.StorageID, computer.StorageGeneration, computer.CurrentJobID,
		computer.CurrentSpecRevision, request.Actor, now); err != nil {
		return Computer{}, false, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO computer_reconfiguration_aborts(
		computer_id, aborted_revision, intent_revision, aborted_phase, idempotency_key, request_hash, actor, created_ns
	) VALUES(?, ?, ?, ?, ?, ?, ?, ?)`, computerID, abortedRevision, nextRevision, abortedPhase,
		request.IdempotencyKey, requestHash, request.Actor, now.UnixNano()); err != nil {
		return Computer{}, false, internalError(err, "persist Computer reconfiguration abort")
	}
	if err := s.reconfigurationCheckpoint("abort_recorded"); err != nil {
		return Computer{}, false, err
	}
	updated, err := readComputerAuthority(ctx, tx, computerID, now)
	if err != nil {
		return Computer{}, false, internalError(err, "read aborted Computer reconfiguration")
	}
	if err := tx.Commit(); err != nil {
		return Computer{}, false, internalError(err, "commit Computer reconfiguration abort")
	}
	s.notifyComputerPolicyChanged()
	return updated, false, nil
}
