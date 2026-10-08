package l3

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

// requestRunCancellation arbitrates cancellation with the first dispatch
// attempt. A never-attempted run can settle locally. Once an attempt began,
// even without an acknowledgement, L1 is the authority on the job's outcome.
func (s *Store) requestRunCancellation(ctx context.Context, runID, actor string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return internalError(err, "begin run cancellation")
	}
	defer tx.Rollback()
	var state contract.RunState
	var submitter string
	var attempted sql.NullInt64
	var jobID sql.NullString
	err = tx.QueryRowContext(ctx, `SELECT r.status, t.actor, r.dispatch_attempt_ns, COALESCE(r.l1_job_id, o.job_id)
 FROM runs r JOIN run_triggers t ON t.run_id=r.run_id
 LEFT JOIN dispatch_outbox o ON o.run_id=r.run_id WHERE r.run_id=?`, runID).Scan(&state, &submitter, &attempted, &jobID)
	if errors.Is(err, sql.ErrNoRows) {
		return protocolError(contract.ErrorNotFound, "run was not found")
	}
	if err != nil {
		return internalError(err, "read run cancellation target")
	}
	if actor != submitter {
		// Follow parent links only: a rerun starts a new lineage with its own
		// submitter. Bound the walk and refuse corrupt or incomplete lineage.
		var rootActor string
		err := tx.QueryRowContext(ctx, `WITH RECURSIVE ancestors(run_id, parent_run_id, depth) AS (
 SELECT run_id, parent_run_id, 0 FROM runs WHERE run_id=?
 UNION ALL
 SELECT r.run_id, r.parent_run_id, a.depth+1 FROM runs r JOIN ancestors a ON r.run_id=a.parent_run_id WHERE a.depth<?
) SELECT t.actor FROM ancestors a JOIN run_triggers t ON t.run_id=a.run_id WHERE a.parent_run_id IS NULL`, runID, maxLineageTraversalDepth).Scan(&rootActor)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return internalError(err, "read cancellation lineage root")
		}
		if err != nil || actor != rootActor {
			return protocolError(contract.ErrorForbidden, "only the submitting actor or lineage root's submitting actor may cancel a run")
		}
	}
	if (state == contract.RunSucceeded || state == contract.RunFailed) && !jobID.Valid && !attempted.Valid {
		return nil
	}
	// Legacy acknowledgements may exist only in the outbox. Link them before
	// cancellation takes over recovery, preserving the terminal outcome.
	if jobID.Valid {
		if _, err := linkTerminalRunTx(ctx, tx, runID, jobID.String); err != nil {
			return err
		}
	}
	now := canonicalTime(s.clock.Now())
	if !attempted.Valid && !jobID.Valid {
		if err := failRunTx(ctx, tx, runID, now, s.tokenGrace, "the run was canceled before dispatch"); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO run_cancellations(run_id, requested_ns, completed_ns) VALUES(?, ?, ?) ON CONFLICT(run_id) DO NOTHING`, runID, now.UnixNano(), now.UnixNano()); err != nil {
			return internalError(err, "record local run cancellation")
		}
	} else {
		if _, err := tx.ExecContext(ctx, `INSERT INTO run_cancellations(run_id, requested_ns) VALUES(?, ?)
 ON CONFLICT(run_id) DO UPDATE SET failures=0, retry_ns=0, last_error=NULL, refused=0
 WHERE run_cancellations.completed_ns IS NULL`, runID, now.UnixNano()); err != nil {
			return internalError(err, "record run cancellation intent")
		}
	}
	if err := tx.Commit(); err != nil {
		return internalError(err, "commit run cancellation intent")
	}
	return nil
}

type runCancellation struct {
	projectedRun
	DispatchKey string
}

func (s *Store) pendingRunCancellations(ctx context.Context, runID string) ([]runCancellation, error) {
	query := `SELECT r.run_id, COALESCE(r.l1_job_id, ''), r.status, r.required_envelope, r.dispatch_key
 FROM run_cancellations c JOIN runs r ON r.run_id=c.run_id
 WHERE c.completed_ns IS NULL AND c.retry_ns<=?`
	args := []any{s.recoveryNow().UnixNano()}
	if runID != "" {
		query += " AND r.run_id=?"
		args = append(args, runID)
	}
	query += " ORDER BY c.retry_ns, c.requested_ns, r.run_id LIMIT ?"
	args = append(args, unrecordedDispatchBatch)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, internalError(err, "list run cancellations")
	}
	defer rows.Close()
	var runs []runCancellation
	for rows.Next() {
		var run runCancellation
		if err := rows.Scan(&run.RunID, &run.JobID, &run.State, &run.RequiredEnvelope, &run.DispatchKey); err != nil {
			return nil, internalError(err, "scan run cancellation")
		}
		runs = append(runs, run)
	}
	if err := rows.Err(); err != nil {
		return nil, internalError(err, "iterate run cancellations")
	}
	return runs, nil
}

// cancelAbsentDispatch accepts an absence only beyond the existing dispatch
// settlement horizon, and only if no acknowledgement landed meanwhile.
func (s *Store) cancelAbsentDispatch(ctx context.Context, runID string, lookupStarted time.Time) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return internalError(err, "begin absent dispatch cancellation")
	}
	defer tx.Rollback()
	var eligible bool
	if err := tx.QueryRowContext(ctx, `SELECT l1_job_id IS NULL AND dispatch_attempt_ns<=?
 AND NOT EXISTS (SELECT 1 FROM dispatch_outbox o WHERE o.run_id=runs.run_id AND o.job_id IS NOT NULL)
 FROM runs WHERE run_id=?`, canonicalTime(lookupStarted).Add(-unrecordedDispatchSettleHorizon).UnixNano(), runID).Scan(&eligible); err != nil {
		return internalError(err, "check canceled dispatch absence")
	}
	if !eligible {
		return nil
	}
	if err := failRunTx(ctx, tx, runID, canonicalTime(s.clock.Now()), s.tokenGrace, "the run was canceled before dispatch"); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE runs SET job_link_settled=1 WHERE run_id=?`, runID); err != nil {
		return internalError(err, "settle canceled dispatch absence")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE run_cancellations SET completed_ns=?, last_error=NULL, refused=0 WHERE run_id=?`, s.recoveryNow().UnixNano(), runID); err != nil {
		return internalError(err, "finish absent dispatch cancellation")
	}
	if err := tx.Commit(); err != nil {
		return internalError(err, "commit absent dispatch cancellation")
	}
	return nil
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
	_, err = s.db.ExecContext(ctx, `UPDATE run_cancellations SET failures=failures+1, retry_ns=?, last_error=? WHERE run_id=? AND completed_ns IS NULL AND failures=?`,
		s.recoveryNow().Add(unrecordedDispatchBackoff(failures)).UnixNano(), string(payload), runID, failures)
	if err != nil {
		return internalError(err, "defer run cancellation")
	}
	return nil
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
	if err != nil && ctx.Err() == nil {
		return errors.Join(err, r.store.deferRunCancellation(ctx, cancellation.RunID, err))
	}
	return err
}

func (r *Reconciler) cancelRunJobRemote(ctx, remote context.Context, cancellation runCancellation) error {
	run := cancellation.projectedRun
	if run.JobID == "" {
		if r.lookup == nil {
			return internalError(errors.New("L1 dispatch lookup client is not configured"), "resolve canceled dispatch")
		}
		lookupStarted := r.store.recoveryNow()
		job, err := r.lookup.LookupJobByDispatchKey(remote, cancellation.DispatchKey)
		if isMissingDispatch(err, cancellation.DispatchKey) {
			if err := r.store.cancelAbsentDispatch(ctx, run.RunID, lookupStarted); err != nil {
				return err
			}
			// If the absence was provisional, the row remains pending. Back
			// it off even though the lookup itself succeeded.
			return r.store.deferRunCancellation(ctx, run.RunID, err)
		}
		if err != nil {
			return err
		}
		if err := r.store.completeDispatch(ctx, run.RunID, job.JobID); err != nil {
			return err
		}
		record, err := r.store.GetRun(ctx, run.RunID)
		if err != nil {
			return err
		}
		run.JobID, run.State = record.L1JobID, record.Status
	}

	canceler, ok := r.jobs.(JobCancelClient)
	if !ok {
		return internalError(errors.New("L1 cancellation client is not configured"), "cancel run job")
	}
	job, err := canceler.CancelJob(remote, run.JobID)
	if err != nil {
		var refusal *Error
		var response *l1ResponseError
		hasProtocol := errors.As(err, &refusal)
		hasResponse := errors.As(err, &response)
		if hasProtocol && (refusal.Code == contract.ErrorUnauthorized || refusal.Code == contract.ErrorPersonIdentityRequired ||
			(hasResponse && response.status == http.StatusUnauthorized)) {
			// Identity lookup failure is not a decision about this Run's cancel.
			// Retain intent and advertise the same retryability to the caller.
			retry := *refusal
			retry.Retryable = true
			return &retry
		}
		typedRefusal := hasProtocol && !refusal.Retryable
		if hasResponse && !response.validEnvelope {
			typedRefusal = false
		}
		if !typedRefusal {
			return err
		}
		// A cancel 404 can also hide an ownership refusal. Only the existing
		// authoritative GetJob absence may fail the Run as an L1 regression.
		if refusal.Code == contract.ErrorNotFound {
			_, getErr := r.jobs.GetJob(remote, run.JobID)
			if isMissingL1Job(getErr, run.JobID) {
				if _, err := r.store.failMissingL1Job(ctx, run); err != nil {
					return err
				}
			}
		}
		return r.store.refuseRunCancellation(ctx, run.RunID, err)
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
