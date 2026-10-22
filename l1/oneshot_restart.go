package l1

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

const (
	CapabilityOneshotRestartable = "oneshot:restartable"
	MaximumOneshotRestarts       = 10
	MaximumOneshotRestartDelay   = 60 * time.Second
	// MinimumOneshotRestartDelay covers the node agent's default Guardian TERM
	// grace (5s), final reap bound (5s), and a 1s scheduling margin. Short Leases
	// must not allow a lease-loss successor ahead of ordinary EOF teardown.
	MinimumOneshotRestartDelay = 11 * time.Second
	RestartLimitReached        = "restart_limit_reached"
)

type InterruptionReason string

const (
	InterruptionUpgrade       InterruptionReason = "upgrade"
	InterruptionAgentShutdown InterruptionReason = "agent_shutdown"
	InterruptionLeaseExpired  InterruptionReason = "lease_expired"
)

// InterruptRestartableAttemptRequest names an exact Attempt and disabled Node
// intent. Replays never substitute a successor, even after admission changes.
type InterruptRestartableAttemptRequest struct {
	JobID          string             `json:"job_id"`
	BootSessionID  string             `json:"boot_session_id"`
	IntentRevision int64              `json:"intent_revision"`
	IdempotencyKey string             `json:"idempotency_key"`
	Reason         InterruptionReason `json:"reason"`
}

// OneshotRestartFacts are durable recovery facts from the same Read snapshot
// as their Job. Pending does not imply Node admission is enabled.
type OneshotRestartFacts struct {
	Count             int                 `json:"count"`
	Limit             int                 `json:"limit"`
	Pending           bool                `json:"pending"`
	LastReason        *InterruptionReason `json:"last_reason"`
	PreviousAttemptID *string             `json:"previous_attempt_id"`
	CurrentAttemptID  *string             `json:"current_attempt_id"`
	NextRestartAt     *time.Time          `json:"next_restart_at"`
	TerminalReason    *string             `json:"terminal_reason"`
}

func (s *Store) initializeOneshotRestart(ctx context.Context) error {
	for _, col := range []struct{ table, name, ddl string }{
		{"jobs", "restart_count", "INTEGER NOT NULL DEFAULT 0 CHECK(restart_count >= 0)"},
		{"jobs", "next_restart_at_ns", "INTEGER"},
		{"jobs", "restart_terminal_reason", "TEXT"},
		{"jobs", "restart_last_reason", "TEXT"},
		{"jobs", "restart_previous_attempt_id", "TEXT"},
		{"attempts", "interruption_reason", "TEXT"},
		{"attempts", "interruption_request_key", "TEXT"},
		{"attempts", "interruption_request_json", "BLOB"},
		{"attempts", "interruption_request_reason", "TEXT"},
		{"attempts", "interruption_requested_ns", "INTEGER"},
	} {
		if err := s.ensureColumn(ctx, col.table, col.name, col.ddl); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) InterruptRestartableAttempt(ctx context.Context, nodeID, attemptID string, request InterruptRestartableAttemptRequest) (Job, bool, error) {
	if strings.TrimSpace(request.JobID) == "" || strings.TrimSpace(request.BootSessionID) == "" || strings.TrimSpace(request.IdempotencyKey) == "" || request.IntentRevision < 0 || request.Reason != InterruptionUpgrade {
		return Job{}, false, protocolError(contract.ErrorInvalidRequest, "job_id, boot_session_id, intent_revision, idempotency_key and reason=upgrade are required")
	}
	encoded, err := json.Marshal(request)
	if err != nil {
		return Job{}, false, internalError(err, "encode interruption request")
	}
	write, err := s.beginWriteTransaction(ctx, nil)
	if err != nil {
		return Job{}, false, internalError(err, "begin interruption request")
	}
	defer write.rollback()
	tx, now := write.tx, write.at
	attempt, err := readAttemptAuthority(ctx, tx, attemptID)
	if err != nil {
		return Job{}, false, err
	}
	if attempt.nodeID != nodeID || attempt.jobID != request.JobID || attempt.bootSessionID != request.BootSessionID {
		return Job{}, false, protocolError(contract.ErrorAttemptMismatch, "interruption must name the exact Node, Job, Attempt and boot session")
	}
	if !attempt.spec.Restartable || attempt.spec.Kind != contract.JobKindProcess || attempt.spec.Class != contract.JobClassOneShot {
		return Job{}, false, protocolError(contract.ErrorConflict, "interruption requires a restartable process one-shot")
	}
	var prior []byte
	if err := tx.QueryRowContext(ctx, "SELECT interruption_request_json FROM attempts WHERE attempt_id=?", attemptID).Scan(&prior); err != nil {
		return Job{}, false, internalError(err, "read interruption replay")
	}
	if len(prior) > 0 {
		if string(prior) != string(encoded) {
			return Job{}, false, protocolError(contract.ErrorIdempotencyConflict, "Attempt already has a different interruption request")
		}
		job, err := getJobByID(ctx, tx, request.JobID, now)
		return job, true, err
	}
	var enabled bool
	var revision int64
	if err := tx.QueryRowContext(ctx, "SELECT claims_enabled, intent_revision FROM nodes WHERE node_id=?", nodeID).Scan(&enabled, &revision); err != nil {
		return Job{}, false, internalError(err, "read interruption Node intent")
	}
	if revision != request.IntentRevision {
		return Job{}, false, protocolErrorWithDetails(contract.ErrorStaleIntentRevision, map[string]any{"node_id": nodeID, "expected_revision": revision, "observed_revision": request.IntentRevision}, "Node intent revision changed")
	}
	if enabled {
		return Job{}, false, protocolError(contract.ErrorConflict, "interruption requires disabled Node claims")
	}
	if err := validateAttemptAuthority(attempt.identityNodeID, request.JobID, attemptID, attempt.fencingToken, attempt); err != nil {
		return Job{}, false, err
	}
	if attempt.state != contract.AttemptClaimed && attempt.state != contract.AttemptRunning && attempt.state != contract.AttemptAwaitingInput {
		return Job{}, false, protocolError(contract.ErrorConflict, "Attempt is terminal")
	}
	canceled, _, err := cancellationDeadline(ctx, tx, request.JobID)
	if err != nil {
		return Job{}, false, err
	}
	if canceled {
		return Job{}, false, protocolError(contract.ErrorConflict, "Job cancellation takes precedence")
	}
	if !now.Before(attempt.leaseExpires) {
		if err := expireAttempt(ctx, tx, attempt, now, s.restartJitter); err != nil {
			return Job{}, false, err
		}
		if err := tx.Commit(); err != nil {
			return Job{}, false, internalError(err, "commit interruption Lease expiry")
		}
		return Job{}, false, protocolError(contract.ErrorLeaseExpired, "Attempt Lease expired")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE attempts SET interruption_request_key=?, interruption_request_json=?, interruption_request_reason=?, interruption_requested_ns=? WHERE attempt_id=?`, request.IdempotencyKey, encoded, request.Reason, now.UnixNano(), attemptID); err != nil {
		return Job{}, false, internalError(err, "persist interruption request")
	}
	job, err := getJobByID(ctx, tx, request.JobID, now)
	if err != nil {
		return Job{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return Job{}, false, internalError(err, "commit interruption request")
	}
	return job, false, nil
}

func (s *Server) interruptRestartableAttempt(w http.ResponseWriter, r *http.Request) {
	var request InterruptRestartableAttemptRequest
	if err := decodeJSON(r, &request); err != nil {
		writeError(w, err)
		return
	}
	// The enclosing client router enforces tagged Fabric client authority.
	job, replayed, err := s.store.InterruptRestartableAttempt(r.Context(), r.PathValue("node_id"), r.PathValue("attempt_id"), request)
	if err != nil {
		writeError(w, err)
		return
	}
	if replayed {
		w.Header().Set("Idempotent-Replay", "true")
	}
	writeJSON(w, http.StatusOK, redactJob(job))
}

func classifyOneshotInterruption(ctx context.Context, tx *sql.Tx, attempt attemptAuthority, request CompletionRequest) (InterruptionReason, error) {
	if request.InterruptionReason == "" {
		if request.InterruptionRequestKey != "" {
			return "", protocolError(contract.ErrorInvalidRequest, "interruption_request_key requires interruption_reason")
		}
		return "", nil
	}
	if !attempt.spec.Restartable || attempt.spec.Kind != contract.JobKindProcess || attempt.spec.Class != contract.JobClassOneShot {
		return "", protocolError(contract.ErrorInvalidRequest, "interruption evidence requires a restartable process one-shot")
	}
	switch request.InterruptionReason {
	case InterruptionUpgrade:
		var key, reason sql.NullString
		if err := tx.QueryRowContext(ctx, "SELECT interruption_request_key, interruption_request_reason FROM attempts WHERE attempt_id=?", attempt.attemptID).Scan(&key, &reason); err != nil {
			return "", internalError(err, "read consumed interruption request")
		}
		if !key.Valid || key.String != request.InterruptionRequestKey || reason.String != string(request.InterruptionReason) {
			return "", protocolError(contract.ErrorConflict, "completion does not identify the exact interruption request")
		}
	case InterruptionAgentShutdown:
		if request.InterruptionRequestKey != "" {
			return "", protocolError(contract.ErrorInvalidRequest, "agent_shutdown does not consume an upgrade request")
		}
	default:
		return "", protocolError(contract.ErrorInvalidRequest, "completion interruption_reason must be upgrade or agent_shutdown")
	}
	// The payload's own exit, command limits and preparation/runtime failures
	// retain their verdict even if an interruption request raced with them.
	r := request.Result
	initiated := request.TerminationInitiator == contract.TerminationCauseAgent || request.TerminationInitiator == contract.TerminationCauseGuardian ||
		r.TerminationCause == contract.TerminationCauseAgent || r.TerminationCause == contract.TerminationCauseGuardian
	if !initiated || r.LimitReached != "" || r.SpawnError != nil || r.RuntimeFailure != nil || r.OOM || r.DiskExhausted {
		return "", nil
	}
	if request.RuntimeQuiescenceEvidence != RuntimeQuiescenceAttempt && request.RuntimeQuiescenceEvidence != RuntimeQuiescenceNoRuntime {
		return "", protocolError(contract.ErrorInvalidRequest, "controlled interruption requires positive process runtime quiescence evidence")
	}
	return request.InterruptionReason, nil
}

// recoverOneshot is the shared controlled-stop and Lease-loss decision. It
// never passes through a terminal Job while recovery remains possible.
func recoverOneshot(ctx context.Context, tx *sql.Tx, attempt attemptAuthority, reason InterruptionReason, now time.Time, jitter func(time.Duration) time.Duration) (contract.JobState, error) {
	var count int
	if err := tx.QueryRowContext(ctx, "SELECT restart_count FROM jobs WHERE job_id=?", attempt.jobID).Scan(&count); err != nil {
		return "", internalError(err, "read one-shot restart budget")
	}
	state := contract.JobQueued
	var next any = now.UnixNano()
	var terminal any
	if count >= MaximumOneshotRestarts {
		state = contract.JobFailed
		next = nil
		terminal = RestartLimitReached
	} else if reason == InterruptionLeaseExpired {
		next = now.Add(oneshotRestartDelay(count+1, jitter)).UnixNano()
	}
	if _, err := tx.ExecContext(ctx, `UPDATE jobs SET next_restart_at_ns=?, restart_terminal_reason=?, restart_last_reason=?, restart_previous_attempt_id=? WHERE job_id=?`, next, terminal, reason, attempt.attemptID, attempt.jobID); err != nil {
		return "", internalError(err, "persist one-shot restart decision")
	}
	return state, nil
}

func oneshotRestartDelay(restart int, jitter func(time.Duration) time.Duration) time.Duration {
	delay := time.Second
	for i := 1; i < restart && delay < MaximumOneshotRestartDelay; i++ {
		delay *= 2
	}
	if delay > MaximumOneshotRestartDelay {
		delay = MaximumOneshotRestartDelay
	}
	if jitter != nil {
		delay = jitter(delay)
	}
	if delay < MinimumOneshotRestartDelay {
		delay = MinimumOneshotRestartDelay
	}
	if delay > MaximumOneshotRestartDelay {
		delay = MaximumOneshotRestartDelay
	}
	return delay
}

func populateOneshotRestart(ctx context.Context, q queryer, job *Job) error {
	if !job.Spec.Restartable {
		return nil
	}
	facts := &OneshotRestartFacts{Limit: MaximumOneshotRestarts}
	if job.CurrentAttemptID != "" {
		facts.CurrentAttemptID = &job.CurrentAttemptID
	}
	var next sql.NullInt64
	var terminal, reason, previous sql.NullString
	err := q.QueryRowContext(ctx, `SELECT restart_count, next_restart_at_ns, restart_terminal_reason, restart_last_reason, restart_previous_attempt_id FROM jobs WHERE job_id=?`, job.JobID).Scan(&facts.Count, &next, &terminal, &reason, &previous)
	if errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if err != nil {
		return internalError(err, "read one-shot restart facts")
	}
	if terminal.Valid {
		facts.TerminalReason = &terminal.String
	}
	if reason.Valid {
		value := InterruptionReason(reason.String)
		facts.LastReason = &value
	}
	if previous.Valid {
		facts.PreviousAttemptID = &previous.String
	}
	facts.Pending = job.State == contract.JobQueued && next.Valid
	if next.Valid {
		value := time.Unix(0, next.Int64).UTC()
		facts.NextRestartAt = &value
	}
	if terminal.String == RestartLimitReached {
		job.FailureReason = "restart limit reached (10 restarts)"
	}
	job.RestartRecovery = facts
	return nil
}
