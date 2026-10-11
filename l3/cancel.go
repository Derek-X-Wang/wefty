package l3

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"net/url"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

type runCancellation struct {
	projectedRun
	DispatchKey string
}

func (s *Store) finishRunCancellation(ctx context.Context, runID string) error {
	if _, err := s.db.ExecContext(ctx, `UPDATE run_cancellations SET completed_ns=?, last_error=NULL, refused=0 WHERE run_id=?`, s.recoveryNow().UnixNano(), runID); err != nil {
		return internalError(err, "finish run cancellation delivery")
	}
	return nil
}

// deferRunCancellation uses the same durable exponential schedule as dispatch
// recovery, including provisional dispatch absence. Explicit repeats reset it.
func (s *Store) deferRunCancellation(ctx context.Context, runID string, cause error) error {
	reason := contract.APIError{Code: contract.ErrorNotFound, Message: "dispatch absence remains provisional", Retryable: true}
	if cause != nil {
		reason = apiErrorFrom(cause)
	}
	payload, err := json.Marshal(reason)
	if err != nil {
		return internalError(err, "encode cancellation failure")
	}
	var failures int
	if err := s.db.QueryRowContext(ctx, `SELECT failures FROM run_cancellations WHERE run_id=?`, runID).Scan(&failures); err != nil {
		return internalError(err, "read cancellation failures")
	}
	return s.writeCancellationBackoff(ctx, runID, failures, s.recoveryNow().Add(unrecordedDispatchBackoff(failures)), string(payload))
}

func (s *Store) refuseRunCancellation(ctx context.Context, runID string, cause error) error {
	payload, err := json.Marshal(apiErrorFrom(cause))
	if err != nil {
		return internalError(err, "encode cancellation refusal")
	}
	_, err = s.db.ExecContext(ctx, `UPDATE run_cancellations SET completed_ns=?, last_error=?, refused=1 WHERE run_id=? AND completed_ns IS NULL`, s.recoveryNow().UnixNano(), string(payload), runID)
	if err != nil {
		return internalError(err, "record cancellation refusal")
	}
	return nil
}

func (s *Server) cancelRun(w http.ResponseWriter, req *http.Request) {
	if _, ok := runTokenFromRequest(req); ok {
		writeError(w, protocolError(contract.ErrorForbidden, "run tokens are not authorized to cancel runs"))
		return
	}
	if _, ok := computerTokenFromRequest(req); ok {
		writeError(w, protocolError(contract.ErrorForbidden, "Computer tokens are not authorized to cancel runs"))
		return
	}
	ctx := req.Context()
	runID := req.PathValue("run_id")
	if err := s.store.requestRunCancellation(ctx, runID, actorFromIdentity(identityFromRequest(req))); err != nil {
		writeError(w, err)
		return
	}
	runs, err := s.store.pendingRunCancellations(ctx, runID)
	if err != nil {
		writeError(w, err)
		return
	}
	for _, run := range runs {
		reconciler := s.reconciler
		if reconciler == nil {
			reconciler, err = NewReconciler(s.store, s.jobs, ReconcilerConfig{})
			if err != nil {
				writeError(w, internalError(err, "cancel run job"))
				return
			}
		}
		if err := reconciler.cancelRunJob(ctx, run); err != nil {
			// A remote delivery failure cannot turn an existing terminal Run
			// into a not-found/error response. Its durable intent still retries.
			record, readErr := s.store.GetRun(ctx, runID)
			if readErr != nil {
				writeError(w, readErr)
				return
			}
			if record.Status != contract.RunSucceeded && record.Status != contract.RunFailed {
				writeError(w, err)
				return
			}
		}
	}
	record, err := s.store.GetRun(ctx, runID)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, record)
}

// cancelRunJob uses the ledger's own client identity, the originating L1
// submitter, rather than forwarding the operator's identity or bearer.
// Delivery stays durable until L1 settles or returns a permanent refusal. Repeating it is an idempotent read
// of the first committed outcome, including completion that beat cancel.
func (r *Reconciler) cancelRunJob(ctx context.Context, cancellation runCancellation) error {
	remote, cancel := context.WithTimeout(ctx, r.budget)
	defer cancel()
	return r.deliverRunCancellation(ctx, remote, cancellation)
}

func (r *Reconciler) deliverRunCancellation(ctx, remote context.Context, cancellation runCancellation) error {
	err := r.cancelRunJobRemote(ctx, remote, cancellation)
	if err != nil {
		err = r.store.observeL1Error(ctx, err)
	}
	if err != nil && ctx.Err() == nil {
		// Keep the dependency/storage cause in server logs when HTTP exposes
		// only a retryable internal error. This also covers background retries.
		log.Printf("L3 run cancellation failed: run_id=%s error=%v cause=%v", cancellation.RunID, err, errors.Unwrap(err))
		return errors.Join(err, r.store.deferRunCancellation(ctx, cancellation.RunID, err))
	}
	return err
}

func (r *Reconciler) cancelRunJobRemote(ctx, remote context.Context, cancellation runCancellation) error {
	run, observed, ready, err := r.canceledRunJob(ctx, remote, cancellation)
	if err != nil || !ready {
		return err
	}
	canceler := r.jobs.(JobCancelClient)
	job, err := canceler.CancelJob(remote, run.JobID)
	if err != nil {
		a := classifyL1Answer(err)
		if a.Kind != l1WorkRefused || a.Method != http.MethodPost || a.Target != "/v1/jobs/"+url.PathEscape(run.JobID)+"/cancel" {
			return err
		}
		refusal := a.Response.protocol
		// A cancel 404 can also hide an ownership refusal. Only authoritative
		// GetJob absence establishes regression. The captured observation fences
		// both remote calls against newer terminal link evidence.
		if refusal.Code == contract.ErrorNotFound {
			_, getErr := r.jobs.GetJob(remote, run.JobID)
			if isMissingL1Job(getErr, run.JobID) {
				if run.State == contract.RunSucceeded || run.State == contract.RunFailed {
					return r.store.cancelAbsentStoredIdentity(ctx, run.RunID, observed)
				}
				if _, err := r.store.failMissingL1Job(ctx, run); err != nil {
					return err
				}
			}
		}
		return r.store.refuseRunCancellation(ctx, run.RunID, err)
	}
	if _, err := r.store.db.ExecContext(ctx, `UPDATE run_cancellations SET last_error=NULL, failures=0, retry_ns=0 WHERE run_id=? AND completed_ns IS NULL`, run.RunID); err != nil {
		return internalError(err, "clear cancellation delivery failure")
	}
	if err := r.projectObservedJob(ctx, run, job); err != nil {
		return err
	}
	if job.State == contract.JobSucceeded || job.State == contract.JobFailed {
		// A success first observed from queued must pass through running. Perform
		// the second legal transition now so the response reports the real outcome.
		record, err := r.store.GetRun(ctx, run.RunID)
		if err != nil {
			return err
		}
		run.State = record.Status
		if err := r.projectObservedJob(ctx, run, job); err != nil {
			return err
		}
		return r.store.finishRunCancellation(ctx, run.RunID)
	}
	return nil
}

// projectObservedJob preserves ordinary reconciliation's image evidence,
// node attribution and workflow gates when cancel observes a completed job.
func (r *Reconciler) projectObservedJob(ctx context.Context, run projectedRun, job l1.Job) error {
	if r.images != nil && (job.State == contract.JobSucceeded || job.State == contract.JobFailed) {
		evidence, err := r.images.GetJobImageEvidence(ctx, run.JobID)
		if err != nil {
			return err
		}
		for _, observation := range evidence {
			recorded, err := r.store.recordRunImageResolution(ctx, run.RunID, observation)
			if err != nil {
				return err
			}
			if recorded {
				break
			}
		}
	}
	nodeID, settled := jobNodeID(job)
	if err := r.store.recordRunNode(ctx, run.RunID, nodeID, settled); err != nil {
		return err
	}
	reason := ""
	if job.State == contract.JobFailed {
		reason = JobFailureReason(job)
	}
	return r.store.projectJobOutcome(ctx, run, job.State, reason)
}
