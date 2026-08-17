package l1

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

// Reconcile applies lease and node-liveness transitions using one snapshot of
// the injected clock. SQLite's immediate transaction lock serializes this pass
// with renewal, heartbeat, completion, and claim transactions, so the winner
// of an expiry-boundary race fully determines the durable result.
func (s *Store) Reconcile(ctx context.Context) (ReconcileResult, error) {
	now := canonicalTime(s.clock.Now())
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ReconcileResult{}, internalError(err, "begin L1 reconciliation")
	}
	defer tx.Rollback()

	result := ReconcileResult{}
	dead, err := tx.ExecContext(ctx, `UPDATE nodes SET state=?
		WHERE state IN (?, ?, ?) AND last_heartbeat_ns<=?`, contract.NodeDead,
		contract.NodeAlive, contract.NodeStale, contract.NodeDraining, now.Add(-s.nodeDeadAfter).UnixNano())
	if err != nil {
		return ReconcileResult{}, internalError(err, "mark dead nodes")
	}
	result.DeadNodes, err = dead.RowsAffected()
	if err != nil {
		return ReconcileResult{}, internalError(err, "read dead-node result")
	}

	stale, err := tx.ExecContext(ctx, `UPDATE nodes SET state=?
		WHERE state=? AND last_heartbeat_ns<=?`, contract.NodeStale, contract.NodeAlive,
		now.Add(-s.nodeStaleAfter).UnixNano())
	if err != nil {
		return ReconcileResult{}, internalError(err, "mark stale nodes")
	}
	result.StaleNodes, err = stale.RowsAffected()
	if err != nil {
		return ReconcileResult{}, internalError(err, "read stale-node result")
	}

	rows, err := tx.QueryContext(ctx, `UPDATE attempts SET state=?, updated_ns=?
		WHERE state IN (?, ?, ?) AND lease_expires_ns<=?
		AND EXISTS (
			SELECT 1 FROM jobs j
			WHERE j.job_id=attempts.job_id
			AND j.current_attempt_id=attempts.attempt_id
			AND j.state IN (?, ?, ?)
		)
		RETURNING attempt_id, job_id`, contract.AttemptLost, now.UnixNano(),
		contract.AttemptClaimed, contract.AttemptRunning, contract.AttemptAwaitingInput, now.UnixNano(),
		contract.JobClaimed, contract.JobRunning, contract.JobAwaitingInput)
	if err != nil {
		return ReconcileResult{}, internalError(err, "reap expired attempts")
	}
	type expiredAttempt struct{ attemptID, jobID string }
	var expired []expiredAttempt
	for rows.Next() {
		var attempt expiredAttempt
		if err := rows.Scan(&attempt.attemptID, &attempt.jobID); err != nil {
			rows.Close()
			return ReconcileResult{}, internalError(err, "read expired attempt")
		}
		expired = append(expired, attempt)
	}
	if err := rows.Close(); err != nil {
		return ReconcileResult{}, internalError(err, "close expired-attempt rows")
	}
	if err := rows.Err(); err != nil {
		return ReconcileResult{}, internalError(err, "iterate expired attempts")
	}
	for _, attempt := range expired {
		if _, err := tx.ExecContext(ctx, `UPDATE jobs SET state=?, updated_ns=?
			WHERE job_id=? AND current_attempt_id=? AND state IN (?, ?, ?)`, contract.JobFailed, now.UnixNano(),
			attempt.jobID, attempt.attemptID, contract.JobClaimed, contract.JobRunning, contract.JobAwaitingInput); err != nil {
			return ReconcileResult{}, internalError(err, "fail expired attempt's job")
		}
	}
	result.ExpiredAttempts = int64(len(expired))

	if err := tx.Commit(); err != nil {
		return ReconcileResult{}, internalError(err, "commit L1 reconciliation")
	}
	return result, nil
}

func (s *Store) reconcileClaimingNode(ctx context.Context, tx *sql.Tx, nodeID string, state contract.NodeState, heartbeat, now time.Time) (contract.NodeState, error) {
	next := state
	if state == contract.NodeAlive || state == contract.NodeStale {
		switch {
		case !heartbeat.Add(s.nodeDeadAfter).After(now):
			next = contract.NodeDead
		case state == contract.NodeAlive && !heartbeat.Add(s.nodeStaleAfter).After(now):
			next = contract.NodeStale
		}
	}
	if next == state {
		return state, nil
	}
	if _, err := tx.ExecContext(ctx, "UPDATE nodes SET state=? WHERE node_id=? AND state=?", next, nodeID, state); err != nil {
		return state, internalError(err, "reconcile claiming node liveness")
	}
	return next, nil
}

// DrainNode idempotently stops a registered boot session from claiming more
// jobs. Heartbeats and writes for already-running attempts remain authorized.
func (s *Store) DrainNode(ctx context.Context, identityNodeID, nodeID, bootSessionID string) (Node, error) {
	if bootSessionID == "" {
		return Node{}, protocolError(contract.ErrorInvalidRequest, "boot_session_id is required")
	}
	result, err := s.db.ExecContext(ctx, `UPDATE nodes SET state=?
		WHERE node_id=? AND identity_node_id=? AND boot_session_id=? AND state IN (?, ?, ?)`,
		contract.NodeDraining, nodeID, identityNodeID, bootSessionID,
		contract.NodeAlive, contract.NodeStale, contract.NodeDraining)
	if err != nil {
		return Node{}, internalError(err, "drain node")
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return Node{}, internalError(err, "read node drain result")
	}
	if changed == 0 {
		return Node{}, s.nodeSessionError(ctx, nodeID, identityNodeID, bootSessionID, "drain")
	}
	node, err := getNode(ctx, s.db, nodeID)
	if err != nil {
		return Node{}, internalError(err, "read draining node")
	}
	return node, nil
}

func (s *Store) nodeSessionError(ctx context.Context, nodeID, identityNodeID, bootSessionID, operation string) error {
	var storedIdentity, storedBoot string
	var state contract.NodeState
	err := s.db.QueryRowContext(ctx, "SELECT identity_node_id, boot_session_id, state FROM nodes WHERE node_id=?", nodeID).
		Scan(&storedIdentity, &storedBoot, &state)
	if errors.Is(err, sql.ErrNoRows) {
		return protocolError(contract.ErrorNodeNotRegistered, "node %q is not registered", nodeID)
	}
	if err != nil {
		return internalError(err, "classify node session")
	}
	if storedIdentity != identityNodeID {
		return protocolError(contract.ErrorIdentityBound, "stable node %q is bound to another Fabric identity", nodeID)
	}
	if storedBoot != bootSessionID {
		return protocolError(contract.ErrorNodeSessionReplaced, "node %q boot session has been replaced", nodeID)
	}
	switch state {
	case contract.NodeDead:
		return protocolError(contract.ErrorNodeDead, "node %q is dead", nodeID)
	case contract.NodeDraining:
		return protocolError(contract.ErrorNodeDraining, "node %q is draining", nodeID)
	default:
		return protocolError(contract.ErrorConflict, "node %q state does not permit %s", nodeID, operation)
	}
}

// SetNodeClaimsByOperator conditionally changes durable operator intent. The
// CAS is deliberately independent of node liveness so work can be forbidden
// while a node is dead, without fencing attempts already in progress.
func (s *Store) SetNodeClaimsByOperator(ctx context.Context, nodeID, actor string, request NodeIntentRequest) (Node, error) {
	if nodeID == "" {
		return Node{}, protocolError(contract.ErrorInvalidRequest, "node_id is required")
	}
	if request.IntentRevision < 0 {
		return Node{}, protocolError(contract.ErrorInvalidRequest, "intent_revision must be non-negative")
	}
	if strings.TrimSpace(request.Reason) == "" || strings.TrimSpace(actor) == "" {
		return Node{}, protocolError(contract.ErrorInvalidRequest, "intent reason and actor are required")
	}
	now := canonicalTime(s.clock.Now())
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Node{}, internalError(err, "begin operator intent mutation")
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `UPDATE nodes
		SET claims_enabled=?, intent_revision=intent_revision+1, intent_reason=?, intent_updated_at=?, intent_actor=?
		WHERE node_id=? AND intent_revision=?`, request.ClaimsEnabled, strings.TrimSpace(request.Reason), now.UnixNano(), strings.TrimSpace(actor), nodeID, request.IntentRevision)
	if err != nil {
		return Node{}, internalError(err, "write operator node intent")
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return Node{}, internalError(err, "read operator node intent result")
	}
	if changed == 0 {
		_, readErr := getNode(ctx, tx, nodeID)
		if errors.Is(readErr, sql.ErrNoRows) {
			return Node{}, protocolError(contract.ErrorNotFound, "node %q was not found", nodeID)
		}
		if readErr != nil {
			return Node{}, internalError(readErr, "read operator intent target")
		}
		return Node{}, protocolError(contract.ErrorConflict, "node %q intent revision has changed", nodeID)
	}
	node, err := getNode(ctx, tx, nodeID)
	if err != nil {
		return Node{}, internalError(err, "read updated operator intent")
	}
	if err := tx.Commit(); err != nil {
		return Node{}, internalError(err, "commit operator intent mutation")
	}
	return node, nil
}
