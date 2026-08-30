package l1

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

// ServiceJob is the service-only portion of a Job projection. It is embedded
// into Job so the HTTP representation remains flat while one-shot jobs omit
// every service field entirely.
type ServiceJob struct {
	DesiredState         contract.ServiceDesiredState `json:"desired_state"`
	BoundNodeID          string                       `json:"bound_node_id,omitempty"`
	NodeState            contract.NodeState           `json:"node_state,omitempty"`
	SlotHeld             bool                         `json:"holds_slot"`
	RestartSuppressed    string                       `json:"restart_suppressed_reason,omitempty"`
	RestartStreak        int                          `json:"restart_streak"`
	LifetimeRestartCount int                          `json:"lifetime_restart_count"`
	LeaseLossCount       int                          `json:"lease_loss_count"`
	NextRestartAt        *time.Time                   `json:"next_restart_at,omitempty"`
	PublishedPort        *int                         `json:"published_port,omitempty"`
	Ready                *bool                        `json:"ready,omitempty"`
	LastFailure          json.RawMessage              `json:"last_failure,omitempty"`
	HealthySinceAt       *time.Time                   `json:"healthy_since_at,omitempty"`
	PublishedAttemptID   string                       `json:"-"`
}

type serviceJobColumns struct {
	desiredState         sql.NullString
	boundNodeID          sql.NullString
	restartStreak        sql.NullInt64
	lifetimeRestartCount sql.NullInt64
	leaseLossCount       sql.NullInt64
	nextRestartNS        sql.NullInt64
	publishedPort        sql.NullInt64
	lastFailure          []byte
	healthySinceNS       sql.NullInt64
	publishedAttemptID   sql.NullString
	ready                sql.NullBool
}

func (columns *serviceJobColumns) scanDestinations() []any {
	return []any{
		&columns.desiredState, &columns.boundNodeID, &columns.restartStreak,
		&columns.lifetimeRestartCount, &columns.leaseLossCount, &columns.nextRestartNS, &columns.publishedPort,
		&columns.lastFailure, &columns.healthySinceNS, &columns.publishedAttemptID, &columns.ready,
	}
}

func (columns serviceJobColumns) projection() *ServiceJob {
	if !columns.desiredState.Valid {
		return nil
	}
	service := &ServiceJob{
		DesiredState:         contract.ServiceDesiredState(columns.desiredState.String),
		RestartStreak:        int(columns.restartStreak.Int64),
		LifetimeRestartCount: int(columns.lifetimeRestartCount.Int64),
		LeaseLossCount:       int(columns.leaseLossCount.Int64),
	}
	if columns.boundNodeID.Valid {
		service.BoundNodeID = columns.boundNodeID.String
	}
	if columns.nextRestartNS.Valid {
		value := time.Unix(0, columns.nextRestartNS.Int64).UTC()
		service.NextRestartAt = &value
	}
	if columns.publishedPort.Valid {
		value := int(columns.publishedPort.Int64)
		ready := columns.ready.Valid && columns.ready.Bool
		service.Ready = &ready
		if ready {
			service.PublishedPort = &value
		}
	}
	if columns.lastFailure != nil {
		service.LastFailure = json.RawMessage(columns.lastFailure)
	}
	if columns.healthySinceNS.Valid {
		value := time.Unix(0, columns.healthySinceNS.Int64).UTC()
		service.HealthySinceAt = &value
	}
	if columns.publishedAttemptID.Valid {
		service.PublishedAttemptID = columns.publishedAttemptID.String
	}
	return service
}

// HoldsSlot reports whether this binding currently occupies service capacity.
// Binding is the reservation: queued restart backoff, stopping, and an
// attestation-pending removal all hold. Stopped, latched failed, verified
// removal, and force-forget release without fabricating a slot identity.
func (service ServiceJob) HoldsSlot(state contract.JobState) bool {
	if service.BoundNodeID == "" {
		return false
	}
	switch state {
	case contract.JobQueued:
		return service.DesiredState == contract.ServiceDesiredRunning
	case contract.JobClaimed, contract.JobRunning, contract.JobStopping,
		contract.JobRemovalPending, contract.JobAgentCleaned:
		return true
	case contract.JobStopped, contract.JobFailed, contract.JobRemovedVerified,
		contract.JobForgottenCleanupUnverified:
		return false
	default:
		return false
	}
}

// RestartPending is an operator projection, never a persisted JobState.
func (service ServiceJob) RestartPending(state contract.JobState, now time.Time) bool {
	return state == contract.JobQueued &&
		service.DesiredState == contract.ServiceDesiredRunning &&
		service.NextRestartAt != nil && now.Before(*service.NextRestartAt)
}

func (s *Store) ProveServiceBinding(ctx context.Context, identityNodeID, jobID string, request ServiceBindingProofRequest) (bool, error) {
	if jobID == "" || request.NodeID == "" || request.BootSessionID == "" {
		return false, protocolError(contract.ErrorInvalidRequest, "service binding proof authority is incomplete")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, internalError(err, "begin service binding proof")
	}
	defer tx.Rollback()
	if err := validateImagePinNodeSession(ctx, tx, identityNodeID, request.NodeID, request.BootSessionID); err != nil {
		return false, err
	}
	var boundNodeID sql.NullString
	var state contract.JobState
	err = tx.QueryRowContext(ctx, `SELECT service_jobs.bound_node_id, jobs.state
		FROM jobs JOIN service_jobs ON service_jobs.job_id=jobs.job_id WHERE jobs.job_id=?`, jobID).
		Scan(&boundNodeID, &state)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, internalError(err, "read service binding proof")
	}
	switch state {
	case contract.JobQueued, contract.JobClaimed, contract.JobRunning, contract.JobStopping, contract.JobStopped, contract.JobFailed:
		return boundNodeID.Valid && boundNodeID.String == request.NodeID, nil
	default:
		return false, nil
	}
}

func (s *Store) LatchServiceImageReconciliationFailure(ctx context.Context, identityNodeID, jobID string, request ServiceImageReconciliationFailureRequest) (Job, error) {
	if jobID == "" || request.NodeID == "" || request.BootSessionID == "" || request.Failure.Code == "" || request.Failure.Message == "" {
		return Job{}, protocolError(contract.ErrorInvalidRequest, "service image reconciliation failure is incomplete")
	}
	result := ProcessResult{SpawnError: &request.Failure}
	if err := validateProcessResult(result); err != nil {
		return Job{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Job{}, internalError(err, "begin service image reconciliation failure")
	}
	defer tx.Rollback()
	if err := validateImagePinNodeSession(ctx, tx, identityNodeID, request.NodeID, request.BootSessionID); err != nil {
		return Job{}, err
	}
	job, err := getJobByID(ctx, tx, jobID, s.clock.Now())
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Job{}, protocolError(contract.ErrorNotFound, "service job %q was not found", jobID)
		}
		return Job{}, internalError(err, "read service image reconciliation job")
	}
	if job.ServiceJob == nil || job.BoundNodeID != request.NodeID {
		return Job{}, protocolError(contract.ErrorAttemptNotOwned, "authenticated node does not own service binding %q", jobID)
	}
	if job.State != contract.JobFailed {
		if err := transitionServiceJob(ctx, tx, jobID, job.DesiredState, contract.JobFailed, s.clock.Now()); err != nil {
			return Job{}, err
		}
	}
	payload, err := json.Marshal(result)
	if err != nil {
		return Job{}, internalError(err, "encode service image reconciliation failure")
	}
	if job.CurrentAttemptID != "" {
		if _, err := tx.ExecContext(ctx, `UPDATE attempts SET state=?, updated_ns=?
			WHERE attempt_id=? AND state IN (?, ?, ?)`, contract.AttemptLost, s.clock.Now().UnixNano(), job.CurrentAttemptID,
			contract.AttemptClaimed, contract.AttemptRunning, contract.AttemptAwaitingInput); err != nil {
			return Job{}, internalError(err, "fence attempt during service image reconciliation failure")
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE jobs SET current_attempt_id=NULL, updated_ns=? WHERE job_id=?`, s.clock.Now().UnixNano(), jobID); err != nil {
		return Job{}, internalError(err, "clear service attempt after image reconciliation failure")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE service_jobs SET next_restart_at=NULL, last_failure=?,
		healthy_since_ns=NULL, published_attempt_id=NULL WHERE job_id=?`, payload, jobID); err != nil {
		return Job{}, internalError(err, "record service image reconciliation failure")
	}
	job, err = getJobByID(ctx, tx, jobID, s.clock.Now())
	if err != nil {
		return Job{}, internalError(err, "read latched service image reconciliation job")
	}
	if err := tx.Commit(); err != nil {
		return Job{}, internalError(err, "commit service image reconciliation failure")
	}
	return job, nil
}

func validateImagePinNodeSession(ctx context.Context, q queryer, identityNodeID, nodeID, bootSessionID string) error {
	var storedIdentity, storedBoot string
	if err := q.QueryRowContext(ctx, `SELECT identity_node_id, boot_session_id FROM nodes WHERE node_id=?`, nodeID).
		Scan(&storedIdentity, &storedBoot); errors.Is(err, sql.ErrNoRows) {
		return protocolError(contract.ErrorNodeNotRegistered, "node %q is not registered", nodeID)
	} else if err != nil {
		return internalError(err, "read image-pin node session")
	}
	if storedIdentity != identityNodeID {
		return protocolError(contract.ErrorAttemptNotOwned, "authenticated node does not own node %q", nodeID)
	}
	if storedBoot != bootSessionID {
		return protocolError(contract.ErrorNodeSessionReplaced, "node %q boot session has been replaced", nodeID)
	}
	return nil
}

func validServiceStatePair(desired contract.ServiceDesiredState, state contract.JobState) bool {
	switch desired {
	case contract.ServiceDesiredRunning:
		switch state {
		case contract.JobQueued, contract.JobClaimed, contract.JobRunning, contract.JobFailed:
			return true
		}
	case contract.ServiceDesiredStopped:
		switch state {
		case contract.JobStopping, contract.JobStopped, contract.JobFailed:
			return true
		}
	}
	return false
}

// transitionServiceJob persists one service state-machine edge inside the
// caller's transaction. It intentionally leaves current_attempt_id untouched:
// stop completion can be replayed only while that attempt remains current.
// Callers own edge-specific side effects such as publication clearing and
// capacity reacquisition in this same transaction.
func transitionServiceJob(
	ctx context.Context,
	tx *sql.Tx,
	jobID string,
	desired contract.ServiceDesiredState,
	next contract.JobState,
	now time.Time,
) error {
	var current contract.JobState
	err := tx.QueryRowContext(ctx, `SELECT jobs.state
		FROM jobs JOIN service_jobs ON service_jobs.job_id=jobs.job_id
		WHERE jobs.job_id=?`, jobID).Scan(&current)
	if errors.Is(err, sql.ErrNoRows) {
		return protocolError(contract.ErrorNotFound, "service job %q was not found", jobID)
	}
	if err != nil {
		return internalError(err, "read service transition state")
	}
	if !contract.CanTransition(contract.ServiceJobTransitions, current, next) {
		return protocolError(contract.ErrorConflict, "service job cannot transition from %q to %q", current, next)
	}
	if !validServiceStatePair(desired, next) {
		return protocolError(contract.ErrorConflict, "service desired state %q cannot pair with observed state %q", desired, next)
	}
	clearPublication := next == contract.JobQueued || next == contract.JobStopping ||
		next == contract.JobStopped || next == contract.JobFailed
	if _, err := tx.ExecContext(ctx, `UPDATE service_jobs
		SET desired_state=?,
			published_attempt_id=CASE WHEN ? THEN NULL ELSE published_attempt_id END,
			healthy_since_ns=CASE WHEN ? THEN NULL ELSE healthy_since_ns END
		WHERE job_id=?`, desired, clearPublication, clearPublication, jobID); err != nil {
		return internalError(err, "update service desired state")
	}
	if _, err := tx.ExecContext(ctx, "UPDATE jobs SET state=?, updated_ns=? WHERE job_id=?", next, canonicalTime(now).UnixNano(), jobID); err != nil {
		return internalError(err, "update service observed state")
	}
	return nil
}
