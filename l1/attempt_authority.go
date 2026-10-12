package l1

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

// Current execution authority checks identity, path, current binding, fence,
// then registration. Endpoints retain their phase, expiry and commit ordering;
// original-evidence provenance and accepted-completion replay stay separate.
func validateAttemptAuthority(identityNodeID, jobID, attemptID, fencingToken string, a attemptAuthority) error {
	if a.identityNodeID != identityNodeID {
		return protocolError(contract.ErrorAttemptNotOwned, "authenticated node does not own this attempt")
	}
	if a.jobID != jobID || a.attemptID != attemptID {
		return protocolError(contract.ErrorAttemptMismatch, "attempt does not match the request path")
	}
	if !a.currentAttempt.Valid || a.currentAttempt.String != attemptID {
		return protocolError(contract.ErrorAttemptMismatch, "attempt is not the job's current attempt")
	}
	if a.fencingToken != fencingToken {
		return protocolError(contract.ErrorStaleFence, "fencing token is stale")
	}
	if a.bootSessionID != a.currentBootSessionID || a.authorityGeneration != a.currentAuthorityGeneration {
		return protocolError(contract.ErrorNodeSessionReplaced, "attempt authority belongs to a replaced node registration")
	}
	return nil
}

type healthCause string

const (
	healthFromStarted     healthCause = "started"
	healthFromRenewal     healthCause = "renewal"
	healthFromLogs        healthCause = "logs"
	healthFromPublication healthCause = "publication"
)

// establishHealth applies the same health rule for Started, renewal, live
// process logs and ready publication, inside the triggering write transaction.
func establishHealth(ctx context.Context, tx *sql.Tx, attemptID string, cause healthCause, now time.Time) error {
	// Read only the health cause's inputs and target Job; phase, lease and
	// authority are checked in the UPDATE below, after the caller's writes.
	var jobID string
	var kind string
	var startedNS sql.NullInt64
	err := tx.QueryRowContext(ctx, `SELECT jobs.job_id,
		json_extract(CAST(jobs.spec_json AS TEXT), '$.kind'), attempts.started_ns
		FROM attempts JOIN jobs ON jobs.job_id=attempts.job_id
		WHERE attempts.attempt_id=?`, attemptID).Scan(&jobID, &kind, &startedNS)
	if errors.Is(err, sql.ErrNoRows) {
		return protocolError(contract.ErrorAttemptNotFound, "attempt %q was not found", attemptID)
	}
	if err != nil {
		return internalError(err, "read service health inputs")
	}
	switch cause {
	case healthFromRenewal:
		if kind == contract.JobKindOCI && !startedNS.Valid {
			return nil
		}
	case healthFromLogs:
		if kind == contract.JobKindOCI {
			return nil
		}
	case healthFromStarted, healthFromPublication:
	default:
		return internalError(fmt.Errorf("unknown health cause %q", cause), "record service stability")
	}
	// Keep current binding and registration checks in this effect even though
	// live callers also check them. Phase, lease and cancellation are checked
	// here too, so retaining historical logs cannot establish live health.
	if _, err := tx.ExecContext(ctx, `UPDATE service_jobs
		SET healthy_since_ns=?
		WHERE job_id=? AND healthy_since_ns IS NULL
			AND EXISTS (
				SELECT 1 FROM jobs
				JOIN attempts ON attempts.attempt_id=jobs.current_attempt_id
				JOIN nodes ON nodes.node_id=attempts.node_id
				WHERE jobs.job_id=service_jobs.job_id
					AND jobs.current_attempt_id=?
					AND attempts.state IN (?, ?, ?)
					AND attempts.lease_expires_ns>?
					AND jobs.outcome<>'canceled'
					AND ((service_jobs.published_port IS NULL AND NOT EXISTS (
						SELECT 1 FROM computer_job_projections
						WHERE computer_job_projections.job_id=service_jobs.job_id
					)) OR service_jobs.published_attempt_id=jobs.current_attempt_id)
					AND service_jobs.bound_node_id=attempts.node_id
					AND nodes.state=?
					AND attempts.boot_session_id=nodes.boot_session_id
					AND attempts.authority_generation=nodes.authority_generation
			)`, now.UnixNano(), jobID, attemptID, contract.AttemptClaimed, contract.AttemptRunning,
		contract.AttemptAwaitingInput, now.UnixNano(), contract.NodeAlive); err != nil {
		return internalError(err, "record service stability")
	}
	return nil
}

// promoteExecution acknowledges only a claimed Job bound to this exact
// Attempt. A stopping or awaiting-input Job retains its state. Completion's
// intermediate promotion is overwritten by settlement in the same transaction.
func promoteExecution(ctx context.Context, tx *sql.Tx, attemptID string, now time.Time) error {
	if _, err := tx.ExecContext(ctx, `UPDATE jobs SET state=?, updated_ns=?
		WHERE job_id=(SELECT job_id FROM attempts WHERE attempt_id=?)
			AND current_attempt_id=? AND state=?`, contract.JobRunning, now.UnixNano(), attemptID, attemptID, contract.JobClaimed); err != nil {
		return err
	}
	return nil
}

func interruptServiceStability(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx, `UPDATE service_jobs SET healthy_since_ns=NULL
		WHERE healthy_since_ns IS NOT NULL
			AND EXISTS (
				SELECT 1 FROM jobs
				JOIN attempts ON attempts.attempt_id=jobs.current_attempt_id
				JOIN nodes ON nodes.node_id=attempts.node_id
				WHERE jobs.job_id=service_jobs.job_id
					AND service_jobs.bound_node_id=attempts.node_id
					AND (nodes.state<>?
						OR attempts.boot_session_id<>nodes.boot_session_id
						OR attempts.authority_generation<>nodes.authority_generation)
			)`, contract.NodeAlive); err != nil {
		return internalError(err, "interrupt service stability on unavailable or replaced nodes")
	}
	return nil
}

func reconcileServiceStability(ctx context.Context, tx *sql.Tx, now time.Time, window time.Duration) (int64, error) {
	// Node liveness is resolved first at the same clock snapshot. In
	// particular, a node that reaches the equal two-minute dead/stability
	// boundary is dead and cannot reset a service's failure streak.
	resets, err := tx.ExecContext(ctx, `UPDATE service_jobs SET restart_streak=0
		WHERE restart_streak>0
			AND desired_state=?
			AND healthy_since_ns IS NOT NULL
			AND healthy_since_ns<=?
			AND EXISTS (
				SELECT 1 FROM jobs
				JOIN attempts ON attempts.attempt_id=jobs.current_attempt_id
				JOIN nodes ON nodes.node_id=attempts.node_id
				WHERE jobs.job_id=service_jobs.job_id
					AND jobs.state=?
					AND attempts.state=?
					AND attempts.lease_expires_ns>?
					AND nodes.state=?
					AND attempts.boot_session_id=nodes.boot_session_id
					AND attempts.authority_generation=nodes.authority_generation
					AND service_jobs.bound_node_id=attempts.node_id
			)`, contract.ServiceDesiredRunning, now.Add(-window).UnixNano(),
		contract.JobRunning, contract.AttemptRunning, now.UnixNano(), contract.NodeAlive)
	if err != nil {
		return 0, internalError(err, "reset stable service restart streaks")
	}
	count, err := resets.RowsAffected()
	if err != nil {
		return 0, internalError(err, "read stable service reset result")
	}

	return count, nil
}

type detachCause string

const (
	detachQueuedComputerStop detachCause = "queued-computer-stop"
	detachComputerAbort      detachCause = "computer-abort"
	detachImageLatch         detachCause = "image-latch"
	detachCompletion         detachCause = "completion"
	detachStorageDestination detachCause = "storage-destination"
)

// detachAttempt changes only the bindings. The caller owns Job settlement and
// timestamps in the same transaction; a detach grants no execution authority.
func detachAttempt(ctx context.Context, tx *sql.Tx, jobID, completedAttemptID string, cause detachCause) error {
	query := "UPDATE jobs SET current_attempt_id=NULL"
	args := []any{}
	switch cause {
	case detachQueuedComputerStop, detachComputerAbort, detachImageLatch:
		query += ", completion_replay_attempt_id=COALESCE(current_attempt_id, completion_replay_attempt_id)"
	case detachCompletion:
		if completedAttemptID == "" {
			return fmt.Errorf("completion detach requires an Attempt")
		}
		query += ", completion_replay_attempt_id=?"
		args = append(args, completedAttemptID)
	case detachStorageDestination:
		// Storage destinations clear the runtime binding without retaining it
		// for replay. Leave any existing replay binding untouched, as before.
	default:
		return fmt.Errorf("unknown detach cause %q", cause)
	}
	query += " WHERE job_id=?"
	args = append(args, jobID)
	_, err := tx.ExecContext(ctx, query, args...)
	return err
}

type invalidationCause string

const (
	invalidateComputerAbort   invalidationCause = "computer-abort"
	invalidateComputerRemoval invalidationCause = "computer-removal"
	invalidateServiceRemoval  invalidationCause = "service-removal"
	invalidateImageLatch      invalidationCause = "image-latch"
	invalidateLeaseSettlement invalidationCause = "lease-settlement"
)

// invalidateAttempts loses only active Attempts. Revocation caps their leases;
// inline lease/deadline settlement preserves its existing expiry and row count.
func invalidateAttempts(ctx context.Context, tx *sql.Tx, targetID string, cause invalidationCause, now time.Time) (sql.Result, error) {
	query := "UPDATE attempts SET state=?, updated_ns=?"
	args := []any{contract.AttemptLost, now.UnixNano()}
	if cause != invalidateLeaseSettlement {
		query += ", lease_expires_ns=MIN(lease_expires_ns, ?)"
		args = append(args, now.UnixNano())
	}
	switch cause {
	case invalidateComputerAbort, invalidateServiceRemoval:
		query += " WHERE job_id=?"
	case invalidateComputerRemoval:
		query += " WHERE job_id IN (SELECT job_id FROM computer_job_projections WHERE computer_id=?)"
	case invalidateImageLatch:
		query += " WHERE attempt_id=(SELECT current_attempt_id FROM jobs WHERE job_id=?)"
	case invalidateLeaseSettlement:
		// Expiry or cancellation-deadline accounting stays in expireAttempt;
		// this cause preserves its original lease even at an early deadline.
		query += " WHERE attempt_id=?"
	default:
		return nil, fmt.Errorf("unknown invalidation cause %q", cause)
	}
	query += " AND state IN (?, ?, ?)"
	args = append(args, targetID, contract.AttemptClaimed, contract.AttemptRunning, contract.AttemptAwaitingInput)
	return tx.ExecContext(ctx, query, args...)
}

// validateCompletionReplayBinding checks that the job still binds attemptID
// for completion replay. It deliberately does not check the node's current
// registration: it gates only an identical replay, which writes nothing, and a
// completion accepted under one registration stays accepted under the next.
// Identity, path, and fence are checked by validateAttemptEvidence first.
func validateCompletionReplayBinding(attemptID string, a attemptAuthority) error {
	if a.currentAttempt.Valid && a.currentAttempt.String != attemptID {
		return protocolError(contract.ErrorAttemptMismatch, "attempt is not the job's current attempt")
	}
	if !a.currentAttempt.Valid && (!a.completionReplayAttempt.Valid || a.completionReplayAttempt.String != attemptID) {
		return protocolError(contract.ErrorAttemptMismatch, "attempt is not the job's retained completion replay attempt")
	}
	return nil
}
