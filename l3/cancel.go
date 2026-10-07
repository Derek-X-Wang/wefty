package l3

import (
	"context"
	"database/sql"
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
	err = tx.QueryRowContext(ctx, `SELECT r.status, t.actor, r.dispatch_attempt_ns, r.l1_job_id
 FROM runs r JOIN run_triggers t ON t.run_id=r.run_id WHERE r.run_id=?`, runID).Scan(&state, &submitter, &attempted, &jobID)
	if errors.Is(err, sql.ErrNoRows) {
		return protocolError(contract.ErrorNotFound, "run was not found")
	}
	if err != nil {
		return internalError(err, "read run cancellation target")
	}
	if actor != submitter {
		return protocolError(contract.ErrorForbidden, "only the submitting actor may cancel a run")
	}
	if state == contract.RunSucceeded || state == contract.RunFailed {
		return nil
	}
	now := canonicalTime(s.clock.Now())
	if !attempted.Valid && !jobID.Valid {
		if err := failRunTx(ctx, tx, runID, now, s.tokenGrace, "the run was canceled before dispatch"); err != nil {
			return err
		}
	} else {
		if _, err := tx.ExecContext(ctx, `INSERT INTO run_cancellations(run_id, requested_ns) VALUES(?, ?) ON CONFLICT(run_id) DO NOTHING`, runID, now.UnixNano()); err != nil {
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
 FROM run_cancellations c JOIN runs r ON r.run_id=c.run_id`
	var args []any
	if runID != "" {
		query += " WHERE r.run_id=?"
		args = append(args, runID)
	}
	query += " ORDER BY c.requested_ns, r.run_id"
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
	if _, err := tx.ExecContext(ctx, `DELETE FROM run_cancellations WHERE run_id=?`, runID); err != nil {
		return internalError(err, "finish absent dispatch cancellation")
	}
	if err := tx.Commit(); err != nil {
		return internalError(err, "commit absent dispatch cancellation")
	}
	return nil
}

func (s *Store) finishRunCancellation(ctx context.Context, runID string) error {
	if _, err := s.db.ExecContext(ctx, `DELETE FROM run_cancellations WHERE run_id=?`, runID); err != nil {
		return internalError(err, "finish run cancellation delivery")
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
			writeError(w, err)
			return
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
// Delivery stays durable until L1 settles. Repeating it is an idempotent read
// of the first committed outcome, including completion that beat cancel.
func (r *Reconciler) cancelRunJob(ctx context.Context, cancellation runCancellation) error {
	run := cancellation.projectedRun
	if run.JobID == "" {
		if r.lookup == nil {
			return internalError(errors.New("L1 dispatch lookup client is not configured"), "resolve canceled dispatch")
		}
		lookupStarted := r.store.recoveryNow()
		job, err := r.lookup.LookupJobByDispatchKey(ctx, cancellation.DispatchKey)
		if isMissingDispatch(err, cancellation.DispatchKey) {
			return r.store.cancelAbsentDispatch(ctx, run.RunID, lookupStarted)
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
	job, err := canceler.CancelJob(ctx, run.JobID)
	if err != nil {
		return err
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
