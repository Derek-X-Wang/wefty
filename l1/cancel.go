package l1

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"slices"

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

// CancelJob arbitrates authority and the queued-to-terminal transition in the
// same immediate transaction used by claim. Whichever commits first wins:
// claim cannot select a failed job, and cancel refuses a claimed job.
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
		return Job{}, protocolError(contract.ErrorNotFound, "job was not found")
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
	default:
		return Job{}, protocolErrorWithDetails(contract.ErrorCancelNotQueued, map[string]any{"state": job.State},
			"cancellation currently supports queued one-shots only; claimed, running and awaiting-input jobs are unchanged")
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
