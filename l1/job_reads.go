package l1

import (
	"context"
	"database/sql"
	"errors"
	"net/http"

	"github.com/Derek-X-Wang/wefty/contract"
)

func readJob(ctx context.Context, reads readModel, id string) (Job, error) {
	job, err := reads.job(ctx, id)
	if errors.Is(err, sql.ErrNoRows) {
		return Job{}, protocolError(contract.ErrorNotFound, "job %q was not found", id)
	}
	if err != nil {
		return Job{}, internalError(err, "read job")
	}
	return job, nil
}

func validateReadCredential(ctx context.Context, reads readModel) error {
	scope, _ := ctx.Value(attemptCredentialContextKey{}).(AttemptCredentialScope)
	if scope.JobID == "" {
		return nil
	}
	return reads.validateCredential(ctx, scope)
}

// readJobResource owns the entire answer, including authorization and selectors.
// Encoding happens after the snapshot has released its SQLite read lock.
func (s *Store) readJobResource(ctx context.Context, id string, actor *serviceActionActor, check func(Job) error) (job Job, err error) {
	err = s.withReadSnapshot(ctx, actor, func(ctx context.Context, reads readModel) error {
		if err := validateReadCredential(ctx, reads); err != nil {
			return err
		}
		job, err = readJob(ctx, reads, id)
		scope, _ := ctx.Value(attemptCredentialContextKey{}).(AttemptCredentialScope)
		if scope.JobID != "" && (errorCode(err) == contract.ErrorNotFound || err == nil && job.JobID != scope.JobID && job.ParentJobID != scope.JobID) {
			return protocolError(contract.ErrorForbidden, "an attempt credential may read only its own job and that job's children")
		}
		if err != nil {
			return err
		}
		if check != nil {
			if err := check(job); err != nil {
				return err
			}
		}
		job, err = projectJobWithReads(ctx, reads, job, projectJobAll)
		return err
	})
	return
}

// Mutations below have already committed. A fresh snapshot may observe a newer
// transition; failure to acquire or project never changes the committed result.
func (s *Server) writeChangedJob(w http.ResponseWriter, r *http.Request, job Job, status int) {
	projected, err := s.store.readJobResource(r.Context(), job.JobID, s.serviceActionActor(r), nil)
	if err != nil {
		details := map[string]any{"reason": "read_snapshot_post_change_failed", "mutation_applied": true, "job_id": job.JobID}
		details["read_reason"] = string(errorCode(err))
		snapshotUnavailable := false
		if api := apiErrorFromDecision(err); api != nil && api.Code == contract.ErrorUnavailable {
			if reason, ok := api.Details["reason"].(string); ok && (reason == "read_snapshot_admission_expired" || reason == "read_snapshot_expired") {
				details["read_reason"] = reason
				snapshotUnavailable = true
			}
		}
		writeError(w, &Error{Code: contract.ErrorUnavailable, Details: details,
			Message: "job change committed but its post-change read is unavailable", notRetryable: !snapshotUnavailable})
		return
	}
	writeJSON(w, status, redactJob(projected))
}
