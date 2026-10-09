package l1

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

// ServiceOperatorFacts is present only on caller-facing service reads, including
// removal tombstones. Store snapshots do not confer operator authority.
type ServiceOperatorFacts struct {
	AllowedActions []contract.AllowedAction `json:"allowed_actions"`
	LastCondition  *contract.Condition      `json:"last_condition"`
}

type serviceActionActor struct {
	Identity           fabric.Identity
	ClientPrincipalTag string
	AttemptCredential  bool
}

func (s *Server) serviceActionActor(r *http.Request) *serviceActionActor {
	return &serviceActionActor{Identity: identityFromRequest(r), ClientPrincipalTag: s.clientPrincipalTag,
		AttemptCredential: attemptCredentialFromRequest(r).JobID != ""}
}

// readMemo lives for exactly one read snapshot or mutation decision.
// Node facts include capabilities, authoritative tags and both occupancies.
// Ownership and failure evidence are read once per Job. Node-wide occupancy
// counts and managed roots are shared across a page, avoiding repeated scans of
// the same node's services. Errors are cached too, preserving refusal precedence.
type readMemo struct {
	nodes      map[string]nodeRead
	nodeRoots  map[string]rootRead
	nodeStates map[string]stateRead

	q         queryer
	computers map[string]serviceComputerRead
	failures  map[string]serviceFailureRead
	capacity  map[string]error
	roots     map[string]error
}

type serviceOperatorReads = readMemo

type serviceComputerRead struct {
	id     string
	mapped bool
	err    error
}

type serviceFailureRead struct {
	resumable bool
	err       error
}

func newServiceOperatorReads(q queryer) *serviceOperatorReads {
	return &readMemo{nodes: make(map[string]nodeRead), nodeRoots: make(map[string]rootRead), nodeStates: make(map[string]stateRead), q: q, computers: make(map[string]serviceComputerRead),
		failures: make(map[string]serviceFailureRead), capacity: make(map[string]error), roots: make(map[string]error)}
}

func (reads *serviceOperatorReads) computer(ctx context.Context, jobID string) (string, bool, error) {
	value, ok := reads.computers[jobID]
	if !ok {
		value.id, value.mapped, value.err = computerIDForJob(ctx, reads.q, jobID)
		reads.computers[jobID] = value
	}
	return value.id, value.mapped, value.err
}

func (reads *serviceOperatorReads) resumable(ctx context.Context, job Job) (bool, error) {
	value, ok := reads.failures[job.JobID]
	if !ok {
		_, value.resumable, value.err = neverAutomaticFailureCause(ctx, reads.q, job)
		reads.failures[job.JobID] = value
	}
	return value.resumable, value.err
}

func (reads *serviceOperatorReads) ensureCapacity(ctx context.Context, job Job) error {
	if job.ServiceJob == nil || job.BoundNodeID == "" {
		return nil
	}
	if err, ok := reads.capacity[job.BoundNodeID]; ok {
		return err
	}
	err := ensureBoundServiceCapacity(ctx, reads.q, job)
	reads.capacity[job.BoundNodeID] = err
	return err
}

func (reads *serviceOperatorReads) removalRoot(ctx context.Context, job Job) error {
	if err, ok := reads.roots[job.BoundNodeID]; ok {
		return err
	}
	var root sql.NullString
	err := reads.q.QueryRowContext(ctx, "SELECT root_instance_id FROM nodes WHERE node_id=?", job.BoundNodeID).Scan(&root)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		err = internalError(err, "read service removal root")
	} else if !root.Valid || strings.TrimSpace(root.String) == "" {
		err = protocolError(contract.ErrorConflict, "bound node %q has no registered managed-root instance", job.BoundNodeID)
	} else {
		err = nil
	}
	reads.roots[job.BoundNodeID] = err
	return err
}

// A nil actor is the existing trusted Store API, used by internal callers and
// store tests. HTTP reads and writes always supply the authenticated actor.
func serviceActionAuthority(ctx context.Context, reads readModel, job Job, verb string, actor *serviceActionActor) error {
	if actor != nil {
		if actor.AttemptCredential {
			return protocolError(contract.ErrorPrincipalForbidden, "an attempt credential may only submit a child job, list its own job and immediate children, read its own job, list or read its children, and cancel its children")
		}
		if err := taggedIdentityDecision(actor.Identity, actor.ClientPrincipalTag); err != nil {
			return err
		}
	}
	if job.ServiceJob == nil && job.Removal == nil {
		return protocolError(contract.ErrorNotFound, "service job %q was not found", job.JobID)
	}
	// Finalized tombstones have no mutable service row. Remove/forget remain
	// idempotent; start/restart retain the existing not-found answer.
	if job.ServiceJob == nil && (verb == "start" || verb == "stop" || verb == "restart") {
		return protocolError(contract.ErrorNotFound, "service job %q was not found", job.JobID)
	}
	if computerID, mapped, err := reads.serviceComputer(ctx, job.JobID); err != nil {
		return err
	} else if mapped {
		authority := "lifecycle"
		if verb == "start" || verb == "stop" {
			authority = "desired-state"
		}
		if verb == "remove" || verb == "forget" {
			authority = "removal"
		}
		return protocolErrorWithDetails(contract.ErrorComputerResourceRequired, map[string]any{"computer_id": computerID},
			"Computer %q is the sole %s authority for Job %q", computerID, authority, job.JobID)
	}
	return nil
}

// serviceActionDecision is the single source for advertised actions and the
// enforcing mutation. Chosen inputs are validated by their request decoder;
// restart describes a fresh key, while accepted key replays retain their path.
func serviceActionDecision(ctx context.Context, reads readModel, job Job, verb string, actor *serviceActionActor) error {
	if err := serviceActionAuthority(ctx, reads, job, verb, actor); err != nil {
		return err
	}
	if verb == "remove" || verb == "forget" {
		if job.Removal != nil || job.BoundNodeID == "" {
			return nil
		}
		return reads.serviceRemovalRoot(ctx, job)
	}
	if job.Removal != nil {
		return protocolError(contract.ErrorConflict, "service job %q is being removed", job.JobID)
	}
	switch verb {
	case "start":
		switch job.State {
		case contract.JobFailed, contract.JobStopped:
			resumable, err := reads.serviceResumable(ctx, job)
			if err != nil {
				return err
			}
			if job.State == contract.JobFailed && job.PolicyStop == nil && !resumable {
				return protocolError(contract.ErrorConflict, "service job %q is latched failed; use restart", job.JobID)
			}
			if !job.HoldsSlot(job.State) {
				return reads.ensureServiceCapacity(ctx, job)
			}
		case contract.JobStopping:
			return protocolError(contract.ErrorConflict, "service job %q is still stopping; wait for stopped before start", job.JobID)
		case contract.JobQueued, contract.JobClaimed, contract.JobRunning:
			if job.DesiredState != contract.ServiceDesiredRunning {
				return protocolError(contract.ErrorConflict, "service job %q has inconsistent desired state", job.JobID)
			}
		default:
			return protocolError(contract.ErrorConflict, "service job %q cannot be started from %q", job.JobID, job.State)
		}
	case "stop":
		switch job.State {
		case contract.JobQueued, contract.JobClaimed, contract.JobRunning, contract.JobFailed:
		case contract.JobStopped:
			if job.DesiredState != contract.ServiceDesiredStopped && job.PolicyStop == nil {
				return protocolError(contract.ErrorConflict, "service job %q has inconsistent desired state", job.JobID)
			}
		case contract.JobStopping:
			if job.DesiredState != contract.ServiceDesiredStopped {
				return protocolError(contract.ErrorConflict, "service job %q has inconsistent desired state", job.JobID)
			}
		default:
			return protocolError(contract.ErrorConflict, "service job %q cannot be stopped from %q", job.JobID, job.State)
		}
	case "restart":
		// Preserve capacity-before-state refusal precedence from the write.
		if !job.HoldsSlot(job.State) {
			if err := reads.ensureServiceCapacity(ctx, job); err != nil {
				return err
			}
		}
		switch job.State {
		case contract.JobStopped, contract.JobFailed, contract.JobQueued, contract.JobClaimed, contract.JobRunning:
		case contract.JobStopping:
			return protocolError(contract.ErrorConflict, "service job %q is still stopping; wait for stopped before restart", job.JobID)
		default:
			return protocolError(contract.ErrorConflict, "service job %q cannot be restarted from %q", job.JobID, job.State)
		}
	default:
		return protocolError(contract.ErrorInvalidRequest, "unknown service verb %q", verb)
	}
	return nil
}

func serviceAllowedActions(ctx context.Context, reads readModel, job Job, actor *serviceActionActor) []contract.AllowedAction {
	actions := make([]contract.AllowedAction, 0, 5)
	for _, verb := range []string{"start", "stop", "restart", "remove", "forget"} {
		action := contract.AllowedAction{Verb: verb, Requires: map[string]any{}}
		switch verb {
		case "start":
			action.Requires["desired_state"] = contract.ServiceDesiredRunning
		case "stop":
			action.Requires["desired_state"] = contract.ServiceDesiredStopped
		case "restart":
			action.Inputs = []contract.ActionInput{{Name: "idempotency_key", Type: "string", Required: true}}
		case "forget":
			action.Requires["force"] = true
		}
		action.RefusedBecause = apiErrorFromDecision(serviceActionDecision(ctx, reads, job, verb, actor))
		actions = append(actions, action)
	}
	return actions
}

// Closest persisted state-machine condition, rather than an invented event log.
// Its timestamp comes from the relevant existing row, never the read clock.
func serviceLastCondition(ctx context.Context, reads readModel, job Job) (*contract.Condition, error) {
	condition := &contract.Condition{Since: job.UpdatedAt, Details: map[string]any{"state": job.State}}
	if removal := job.Removal; removal != nil {
		condition.Code, condition.Scope, condition.Since = string(job.State), "service_removal", removal.RemovalRequestedAt
		condition.Details["cleanup_status"] = removal.CleanupStatus
		condition.Details["removal_outcome"] = removal.RemovalOutcome
		condition.Details["removal_generation"] = removal.RemovalGeneration
		if removal.CleanupAcknowledgedAt != nil {
			condition.Since = *removal.CleanupAcknowledgedAt
		}
		if removal.RemovedAt != nil {
			condition.Since = *removal.RemovedAt
		}
		if removal.StalledAt != nil {
			condition.Since = *removal.StalledAt
		}
		if removal.Stall != nil {
			condition.Details["stall"] = removal.Stall
		}
		return condition, nil
	}
	switch {
	case job.PolicyStop != nil:
		condition.Code, condition.Scope = "policy_stop", "service_restart"
		condition.Details["restart"] = job.Spec.Restart
		condition.Details["policy_stop"] = job.PolicyStop
	case job.State == contract.JobFailed:
		resumable, err := reads.serviceResumable(ctx, job)
		if err != nil {
			return nil, err
		}
		condition.Code, condition.Scope = "failure_latched", "service_restart"
		if resumable {
			condition.Code = "never_automatic_restart_suppressed"
		}
		condition.Details["restart_streak"] = job.RestartStreak
		if job.Spec.MaxRestartStreak != nil {
			condition.Details["max_restart_streak"] = *job.Spec.MaxRestartStreak
		}
		if job.FailureReason != "" {
			condition.Details["failure_reason"] = job.FailureReason
		}
	default:
		return nil, nil
	}
	if job.CurrentAttemptID != "" {
		since, err := reads.attemptConditionTime(ctx, job)
		if err != nil {
			return nil, internalError(err, "read service condition time")
		}
		condition.Since = since
	}
	return condition, nil
}

func projectServiceOperatorFacts(ctx context.Context, reads readModel, job Job, actor *serviceActionActor) (Job, error) {
	if job.ServiceJob == nil && job.Removal == nil {
		return job, nil
	}
	condition, err := serviceLastCondition(ctx, reads, job)
	if err != nil {
		return Job{}, err
	}
	job.ServiceOperatorFacts = &ServiceOperatorFacts{AllowedActions: serviceAllowedActions(ctx, reads, job, actor), LastCondition: condition}
	return job, nil
}

func (r *databaseReads) serviceComputer(ctx context.Context, id string) (string, bool, error) {
	return r.reads.computer(ctx, id)
}
func (r *databaseReads) serviceResumable(ctx context.Context, job Job) (bool, error) {
	return r.reads.resumable(ctx, job)
}
func (r *databaseReads) ensureServiceCapacity(ctx context.Context, job Job) error {
	return r.reads.ensureCapacity(ctx, job)
}
func (r *databaseReads) serviceRemovalRoot(ctx context.Context, job Job) error {
	return r.reads.removalRoot(ctx, job)
}
func (r *databaseReads) attemptConditionTime(ctx context.Context, job Job) (time.Time, error) {
	var ns int64
	err := r.q.QueryRowContext(ctx, "SELECT updated_ns FROM attempts WHERE attempt_id=? AND job_id=?", job.CurrentAttemptID, job.JobID).Scan(&ns)
	return time.Unix(0, ns).UTC(), err
}
