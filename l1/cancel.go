package l1

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"slices"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

// JobCancelCaller contains only authority resolved by the route. A credential
// never falls back to submitter authority, even though it inherits that ID.
type JobCancelCaller struct {
	Submitter string
	Person    *fabric.Identity
	Parent    *AttemptCredentialScope
}

// CancelSettlementTimeout bounds pending cancellation independently of renewals.
// It allows the normal five-second TERM grace and final evidence delivery.
const CancelSettlementTimeout = 30 * time.Second

// cancellationDeadline reads intent in the transaction that arbitrates work.
// A reserved outcome is independent of the attempt's actual process result.
func cancellationDeadline(ctx context.Context, q queryer, jobID string) (bool, time.Time, error) {
	var outcome string
	var deadline sql.NullInt64
	if err := q.QueryRowContext(ctx, `SELECT outcome, cancel_settle_by_ns FROM jobs WHERE job_id=?`, jobID).Scan(&outcome, &deadline); err != nil {
		return false, time.Time{}, internalError(err, "read cancellation intent")
	}
	return outcome == "canceled", time.Unix(0, deadline.Int64).UTC(), nil
}

// CancelJob reserves the outcome in the same immediate transaction as claim,
// start, child creation and completion. The first committed terminal intent wins.
func (s *Store) CancelJob(ctx context.Context, jobID string, caller JobCancelCaller) (Job, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Job{}, internalError(err, "begin job cancellation")
	}
	defer tx.Rollback()
	now := canonicalTime(s.clock.Now())
	if caller.Parent != nil {
		if err := revalidateAttemptCredential(ctx, tx, *caller.Parent, now.UnixNano()); err != nil {
			return Job{}, err
		}
	}
	job, err := getJobByID(ctx, tx, jobID, now)
	if errors.Is(err, sql.ErrNoRows) {
		tombstone, tombstoneErr := readServiceTombstoneByID(ctx, tx, jobID)
		if errors.Is(tombstoneErr, sql.ErrNoRows) {
			return Job{}, protocolError(contract.ErrorNotFound, "job was not found")
		}
		if tombstoneErr != nil {
			return Job{}, internalError(tombstoneErr, "read cancellation tombstone")
		}
		job = tombstone.job()
		job.Spec.Class = contract.JobClassService
		err = tx.QueryRowContext(ctx, `SELECT originating_submitter, parent_job_id FROM service_tombstones WHERE job_id=?`, jobID).Scan(&job.OriginatingSubmitter, &job.ParentJobID)
	}
	if err != nil {
		return Job{}, internalError(err, "read cancellation target")
	}
	allowed := false
	switch {
	case caller.Parent != nil:
		allowed = job.ParentJobID == caller.Parent.JobID
	case caller.Person != nil:
		err := requireCurrentAdmin(ctx, tx, *caller.Person)
		if err != nil && errorCode(err) != contract.ErrorAdminRequired && errorCode(err) != contract.ErrorPersonIdentityRequired {
			return Job{}, err
		}
		allowed = err == nil
	default:
		allowed = caller.Submitter != "" && caller.Submitter == job.OriginatingSubmitter
	}
	if !allowed {
		return Job{}, protocolError(contract.ErrorNotFound, "job was not found")
	}
	if job.Spec.Class == contract.JobClassService {
		// Point at the routes that actually govern this service: a Computer's
		// job answers only through its Computer resource, and the job routes
		// need the explicit service class selector.
		details := map[string]any{
			"desired_state_path": "/v1/jobs/" + jobID + "/desired-state?class=service",
			"remove_path":        "/v1/jobs/" + jobID + "/remove?class=service",
		}
		if computerID, mapped, mapErr := computerIDForJob(ctx, tx, jobID); mapErr != nil {
			return Job{}, mapErr
		} else if mapped {
			details = map[string]any{
				"computer_id":        computerID,
				"desired_state_path": "/v1/computers/" + computerID + "/desired-state",
				"remove_path":        "/v1/computers/" + computerID + "/remove",
			}
		}
		return Job{}, protocolErrorWithDetails(contract.ErrorCancelService, details,
			"services cannot be canceled; use desired state or remove")
	}
	switch job.State {
	case contract.JobSucceeded, contract.JobFailed:
		// A retry returns the first committed terminal outcome unchanged.
	case contract.JobQueued:
		if job.Spec.Class != contract.JobClassOneShot {
			return Job{}, protocolError(contract.ErrorCancelNotQueued, "only queued one-shots can be canceled")
		}
		if _, err := tx.ExecContext(ctx, `UPDATE jobs SET state='failed', outcome='canceled',
			current_attempt_id=NULL, prestart_terminal_reason=NULL, prestart_next_retry_at_ns=NULL,
			updated_ns=? WHERE job_id=? AND state='queued'`, now.UnixNano(), jobID); err != nil {
			return Job{}, internalError(err, "cancel queued job")
		}
		// Read back the trigger-scrubbed permanent record before committing.
		job, err = getJobByID(ctx, tx, jobID, now)
		if err != nil {
			return Job{}, internalError(err, "read canceled job")
		}
	case contract.JobClaimed, contract.JobRunning, contract.JobAwaitingInput:
		if job.Spec.Class != contract.JobClassOneShot {
			return Job{}, protocolErrorWithDetails(contract.ErrorCancelNotQueued, map[string]any{"state": job.State}, "only one-shots support active cancellation")
		}
		if job.Outcome != "canceled" {
			if _, err := tx.ExecContext(ctx, `UPDATE jobs SET outcome='canceled', cancel_settle_by_ns=?, updated_ns=? WHERE job_id=?`, now.Add(CancelSettlementTimeout).UnixNano(), now.UnixNano(), jobID); err != nil {
				return Job{}, internalError(err, "reserve canceled outcome")
			}
			job, err = getJobByID(ctx, tx, jobID, now)
			if err != nil {
				return Job{}, internalError(err, "read cancellation intent")
			}
		}
	default:
		return Job{}, protocolErrorWithDetails(contract.ErrorCancelNotQueued, map[string]any{"state": job.State}, "job does not support cancellation in this state")
	}
	if err := tx.Commit(); err != nil {
		return Job{}, internalError(err, "commit job cancellation")
	}
	return job, nil
}

// The cancel route alone admits untagged people; other job routes retain their
// existing client gate. An explicit bearer always selects credential scope.
func (s *Server) authorizeCancelProtocol(client, credential http.Handler) http.Handler {
	bare := s.authorize(cancelPrincipal, client)
	bearer := s.authorizeAttemptCredential(credential)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if presentedAttemptCredential(r) != "" {
			bearer.ServeHTTP(w, r)
			return
		}
		bare.ServeHTTP(w, r)
	})
}

func (s *Server) cancelJob(w http.ResponseWriter, r *http.Request) {
	identity := identityFromRequest(r)
	caller := JobCancelCaller{}
	if presentedAttemptCredential(r) != "" {
		scope := attemptCredentialFromRequest(r)
		caller.Parent = &scope
	} else if slices.Contains(NormalizeTags(identity.Tags), s.clientPrincipalTag) {
		caller.Submitter = identity.NodeID
	} else {
		caller.Person = &identity
	}
	job, err := s.store.CancelJob(r.Context(), r.PathValue("job_id"), caller)
	if err != nil {
		writeError(w, err)
		return
	}
	s.writeJobResource(w, r, job)
}

// ListNodeCancelDirectives keeps stop delivery available after logical settlement:
// a canceled job is never proof that an unreachable program has stopped. Exact
// attempt, fence and boot binding prevent stopping any successor execution.
func (s *Store) ListNodeCancelDirectives(ctx context.Context, identityNodeID, nodeID, bootSessionID string) ([]OneShotCancelDirective, error) {
	var directives []OneShotCancelDirective
	err := s.withReadSnapshot(ctx, nil, func(ctx context.Context, reads readModel) error {
		var err error
		directives, err = reads.nodeCancelDirectives(ctx, identityNodeID, nodeID, bootSessionID)
		return err
	})
	return directives, err
}

func (r *databaseReads) nodeCancelDirectives(ctx context.Context, identityNodeID, nodeID, bootSessionID string) ([]OneShotCancelDirective, error) {
	rows, err := r.q.QueryContext(ctx, `SELECT jobs.job_id, attempts.attempt_id, attempts.fencing_token
 FROM jobs JOIN attempts ON attempts.attempt_id=jobs.current_attempt_id
 JOIN nodes ON nodes.node_id=attempts.node_id
 WHERE jobs.outcome='canceled' AND attempts.node_id=? AND nodes.identity_node_id=?
 AND attempts.boot_session_id=? AND nodes.boot_session_id=attempts.boot_session_id
 AND nodes.authority_generation=attempts.authority_generation
 AND attempts.result_json IS NULL AND attempts.late_result_json IS NULL
 ORDER BY jobs.job_id`, nodeID, identityNodeID, bootSessionID)
	if err != nil {
		return nil, internalError(err, "list one-shot cancel directives")
	}
	defer rows.Close()
	directives := make([]OneShotCancelDirective, 0)
	for rows.Next() {
		var directive OneShotCancelDirective
		if err := rows.Scan(&directive.JobID, &directive.AttemptID, &directive.FencingToken); err != nil {
			return nil, internalError(err, "read one-shot cancel directive")
		}
		directives = append(directives, directive)
	}
	if err := rows.Err(); err != nil {
		return nil, internalError(err, "iterate one-shot cancel directives")
	}
	return directives, nil
}
