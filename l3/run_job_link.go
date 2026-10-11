package l3

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

// Run job link owns confirmation, terminal association and cancel ownership.
// L1 observations are obtained before these short ledger writes. Adoption of
// dispatch, recovery, regression and readers continues in #779–#784.

// completeDispatch records L1's acknowledgement. A run that ended while its
// submit was in flight is linked here too, keeping its terminal state: the
// acknowledgement is L1's own answer for this dispatch key, so it also
// replaces a dispatch_not_found settlement recovery recorded before it
// arrived. A regression verdict contradicts that earlier commit, so a delayed
// acknowledgement cannot restore its link or erase the diagnostic.
func (s *Store) completeDispatch(ctx context.Context, runID, jobID string) error {
	return s.completeDispatchWithEvidence(ctx, runID, jobID, dispatchAcknowledgementEvidence, nil)
}

type dispatchLinkEvidence int

const (
	dispatchAcknowledgementEvidence dispatchLinkEvidence = iota
	dispatchStoredEvidence
	dispatchLookupEvidence
)

// dispatchObservation captures identity and verdict before an L1 lookup. A newer
// regression must not be overwritten by an answer to that earlier lookup.
type dispatchObservation struct {
	outboxJob, runJob, lastError sql.NullString
	dispatched, attempted        sql.NullInt64
	settled                      bool
	state                        contract.RunState
	reason, diagnosticJob        string
}

const dispatchObservationColumns = `o.job_id, r.l1_job_id, o.last_error, o.dispatched_ns, r.job_link_settled, r.status,
 COALESCE(CASE WHEN json_valid(o.last_error) THEN json_extract(o.last_error, '$.details.reason') END, ''),
 COALESCE(CASE WHEN json_valid(o.last_error) THEN json_extract(o.last_error, '$.details.l1_job_id') END, ''), r.dispatch_attempt_ns`

func readDispatchObservation(ctx context.Context, q queryRower, runID string) (dispatchObservation, error) {
	var observed dispatchObservation
	err := q.QueryRowContext(ctx, `SELECT `+dispatchObservationColumns+`
 FROM runs r JOIN dispatch_outbox o ON o.run_id=r.run_id WHERE r.run_id=?`, runID).
		Scan(&observed.outboxJob, &observed.runJob, &observed.lastError, &observed.dispatched, &observed.settled, &observed.state, &observed.reason, &observed.diagnosticJob, &observed.attempted)
	if err != nil {
		return observed, internalError(err, "read dispatch acknowledgement")
	}
	return observed, nil
}

func (s *Store) completeDispatchWithEvidence(ctx context.Context, runID, jobID string, evidence dispatchLinkEvidence, observed *dispatchObservation) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return internalError(err, "begin dispatch completion")
	}
	defer tx.Rollback()
	if err := s.completeDispatchWithEvidenceTx(ctx, tx, runID, jobID, evidence, observed); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return internalError(err, "commit dispatch completion")
	}
	return nil
}

func (s *Store) completeDispatchWithEvidenceTx(ctx context.Context, tx *sql.Tx, runID, jobID string, evidence dispatchLinkEvidence, observed *dispatchObservation) error {
	if jobID == "" {
		return internalError(errors.New("L1 dispatch acknowledgement has an empty job ID"), "complete dispatch")
	}
	now := canonicalTime(s.clock.Now())
	current, err := readDispatchObservation(ctx, tx, runID)
	if err != nil {
		return err
	}
	// An overlapping acknowledgement of the same Job can change identity,
	// dispatch stamps and Run state without changing the verdict. Recheck
	// identities below; only a newer regression makes this lookup stale.
	// Fresh positive evidence supersedes even a settlement written during lookup.
	if evidence == dispatchLookupEvidence && (observed == nil || (current.reason == l1RegressedReason &&
		(observed.reason != l1RegressedReason || current.diagnosticJob != observed.diagnosticJob || current.lastError != observed.lastError))) {
		return internalError(errors.New("dispatch evidence changed during lookup"), "complete dispatch")
	}
	if _, err := decodeRunJobLink(current); err != nil {
		return internalError(err, "complete dispatch")
	}
	outboxJob, runJob := current.outboxJob, current.runJob
	settled, reason, diagnosticJob := current.settled, current.reason, current.diagnosticJob
	if (outboxJob.Valid && outboxJob.String != jobID) || (runJob.Valid && runJob.String != jobID) ||
		(reason == l1RegressedReason && diagnosticJob != "" && diagnosticJob != jobID) {
		return internalError(errors.New("dispatch acknowledgement conflicts with recorded job"), "complete dispatch")
	}
	// Stored identity cannot undo a verdict. An acknowledgement delivered
	// after regression proves the earlier commit, not a later live Job. Only
	// a fresh, identity-bound lookup can supersede that diagnostic.
	if (evidence == dispatchStoredEvidence && (settled || reason == l1RegressedReason || reason == dispatchNotFoundReason)) ||
		(evidence == dispatchAcknowledgementEvidence && reason == l1RegressedReason) {
		return nil
	}
	result, err := tx.ExecContext(ctx, `UPDATE dispatch_outbox SET job_id=?, dispatched_ns=?, last_error=NULL, token_delivery=NULL WHERE run_id=? AND dispatched_ns IS NULL AND (job_id IS NULL OR job_id=?)`, jobID, now.UnixNano(), runID, jobID)
	if err != nil {
		return internalError(err, "complete dispatch outbox")
	}
	acknowledged, err := result.RowsAffected()
	if err != nil {
		return internalError(err, "read dispatch completion result")
	}
	if evidence == dispatchLookupEvidence {
		// A bound positive lookup is a new observation, even when the outbox
		// already recorded the earlier acknowledgement and its later absence.
		if _, err := tx.ExecContext(ctx, `UPDATE dispatch_outbox SET last_error=NULL, token_delivery=NULL WHERE run_id=? AND job_id=?`, runID, jobID); err != nil {
			return internalError(err, "record confirmed canceled dispatch")
		}
	}
	result, err = tx.ExecContext(ctx, `UPDATE runs SET l1_job_id=?, status=?, updated_ns=? WHERE run_id=? AND status IN (?, ?)`,
		jobID, contract.RunQueued, now.UnixNano(), runID, contract.RunPending, contract.RunDispatching)
	if err != nil {
		return internalError(err, "associate run with job")
	}
	queued, err := result.RowsAffected()
	if err != nil {
		return internalError(err, "read run association result")
	}
	if acknowledged == 1 || !runJob.Valid {
		// A first acknowledgement or newly confirmed association wakes delivery, even
		// when an older outbox-only acknowledgement was already recorded.
		if _, err := tx.ExecContext(ctx, `UPDATE run_cancellations SET failures=0, retry_ns=0, completed_ns=NULL, last_error=NULL, refused=0 WHERE run_id=?`, runID); err != nil {
			return internalError(err, "wake cancellation after dispatch acknowledgement")
		}
	}
	// Also repair a legacy outbox-only acknowledgement: it is already
	// dispatched, but the terminal Run may still lack its association.
	if queued == 0 {
		if _, err := linkTerminalRunTx(ctx, tx, runID, jobID); err != nil {
			return err
		}
	}
	if err := clearDispatchRetry(ctx, tx, runID); err != nil {
		return err
	}
	return nil
}

// linkTerminalRunTx records only the missing association of an ended run.
// The terminal state, reason and timestamps are deliberately left alone; the
// node is marked pending so the terminal-attribution pass can name it.
func linkTerminalRunTx(ctx context.Context, tx *sql.Tx, runID, jobID string) (bool, error) {
	result, err := tx.ExecContext(ctx, `UPDATE runs SET
  l1_job_id=?, job_link_settled=1,
  node_attribution_pending=CASE WHEN COALESCE(node_id, '')='' THEN 1 ELSE node_attribution_pending END
WHERE run_id=? AND l1_job_id IS NULL AND status IN (?, ?)`,
		jobID, runID, contract.RunSucceeded, contract.RunFailed)
	if err != nil {
		return false, internalError(err, "link terminal run with job")
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return false, internalError(err, "read terminal run link result")
	}
	return changed == 1, nil
}

// linkUnrecordedDispatch records only the missing association. The terminal
// state, reason and timestamps are deliberately outside both updates.
func (s *Store) linkUnrecordedDispatch(ctx context.Context, item unrecordedDispatch, jobID string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return internalError(err, "begin unrecorded dispatch link")
	}
	defer tx.Rollback()
	var cancelOwned bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM run_cancellations WHERE run_id=?)`, item.RunID).Scan(&cancelOwned); err != nil {
		return internalError(err, "check recovery cancellation ownership")
	}
	if cancelOwned {
		return nil
	}
	if jobID == "" {
		return internalError(errors.New("recovered dispatch has an empty job ID"), "link unrecorded dispatch")
	}
	var dispatchKey, outboxKey string
	var outboxJob, runJob sql.NullString
	var reason, diagnosticJob string
	if err := tx.QueryRowContext(ctx, `SELECT r.dispatch_key, o.dispatch_key, o.job_id, r.l1_job_id,
 COALESCE(CASE WHEN json_valid(o.last_error) THEN json_extract(o.last_error, '$.details.reason') END, ''),
 COALESCE(CASE WHEN json_valid(o.last_error) THEN json_extract(o.last_error, '$.details.l1_job_id') END, '')
 FROM runs r JOIN dispatch_outbox o ON o.run_id=r.run_id WHERE r.run_id=?`, item.RunID).
		Scan(&dispatchKey, &outboxKey, &outboxJob, &runJob, &reason, &diagnosticJob); err != nil {
		return internalError(err, "read recovered dispatch identity")
	}
	if dispatchKey != item.DispatchKey || outboxKey != dispatchKey ||
		(outboxJob.Valid && outboxJob.String != jobID) || (runJob.Valid && runJob.String != jobID) ||
		(reason == l1RegressedReason && diagnosticJob != "" && diagnosticJob != jobID) {
		return internalError(errRecoveredDispatchIdentityConflict, "link unrecorded dispatch")
	}
	if err := s.completeDispatchWithEvidenceTx(ctx, tx, item.RunID, jobID, dispatchLookupEvidence, &item.observed); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return internalError(err, "commit unrecorded dispatch link")
	}
	return nil
}

// cancelStoredJobID uses an outbox acknowledgement only while no settled absence
// or regression diagnostic contradicts it. The Run's own link remains primary.
const cancelStoredJobID = `COALESCE(r.l1_job_id, CASE WHEN r.job_link_settled=0
 AND COALESCE(CASE WHEN json_valid(o.last_error) THEN json_extract(o.last_error, '$.details.reason') END, '') NOT IN ('` + l1RegressedReason + `', '` + dispatchNotFoundReason + `')
 THEN o.job_id END)`

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
	err = tx.QueryRowContext(ctx, `SELECT r.status, t.actor, r.dispatch_attempt_ns, `+cancelStoredJobID+`
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
	// Uncontradicted legacy acknowledgements may exist only in the outbox. Link them before
	// cancellation takes over recovery, preserving the terminal outcome.
	if jobID.Valid {
		if _, err := linkTerminalRunTx(ctx, tx, runID, jobID.String); err != nil {
			return err
		}
	}
	now := canonicalTime(s.clock.Now())
	if !attempted.Valid && !jobID.Valid {
		if err := failRunTx(ctx, tx, runID, now, s.tokenGrace, "canceled before dispatch was confirmed"); err != nil {
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
	if err := failRunTx(ctx, tx, runID, canonicalTime(s.clock.Now()), s.tokenGrace, "canceled before dispatch was confirmed"); err != nil {
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

// cancelAbsentStoredIdentity settles only delivery for an already-terminal Run.
// Preserve its verdict, terminal facts and token expiry. Recheck the identity
// and verdict that authorized the remote read inside the write transaction.
func (s *Store) cancelAbsentStoredIdentity(ctx context.Context, runID string, observed dispatchObservation) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return internalError(err, "begin absent identity cancellation")
	}
	defer tx.Rollback()
	current, err := readDispatchObservation(ctx, tx, runID)
	if err != nil {
		return err
	}
	if current != observed {
		return internalError(errors.New("dispatch evidence changed during lookup"), "settle absent identity cancellation")
	}
	if !current.outboxJob.Valid || current.outboxJob.String == "" ||
		(current.runJob.Valid && current.runJob.String != current.outboxJob.String) ||
		(current.reason == l1RegressedReason && current.diagnosticJob != "" && current.diagnosticJob != current.outboxJob.String) {
		return internalError(errors.New("stored dispatch identity conflicts with recorded job"), "settle absent identity cancellation")
	}
	if current.state != contract.RunSucceeded && current.state != contract.RunFailed {
		// An active Run still follows the existing regression/absence rules.
		return nil
	}
	if _, err := tx.ExecContext(ctx, `UPDATE run_cancellations SET completed_ns=?, last_error=NULL, refused=0 WHERE run_id=? AND completed_ns IS NULL`, s.recoveryNow().UnixNano(), runID); err != nil {
		return internalError(err, "finish absent identity cancellation")
	}
	if err := tx.Commit(); err != nil {
		return internalError(err, "commit absent identity cancellation")
	}
	return nil
}

// acceptRecoveredCancellation hands a bound positive observation to cancel.
// The same write checks ownership and dispatch identity, links the Job using
// lookup evidence, and wakes delivery. L1 calls happen only after it commits.
func (s *Store) acceptRecoveredCancellation(ctx context.Context, item unrecordedDispatch, jobID string, observed *dispatchObservation) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return internalError(err, "begin recovered cancellation")
	}
	defer tx.Rollback()
	var key, outboxKey string
	err = tx.QueryRowContext(ctx, `SELECT r.dispatch_key, o.dispatch_key
 FROM runs r JOIN dispatch_outbox o ON o.run_id=r.run_id
 WHERE r.run_id=? AND EXISTS (SELECT 1 FROM run_cancellations c WHERE c.run_id=r.run_id)`, item.RunID).Scan(&key, &outboxKey)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return internalError(err, "read recovered cancellation ownership")
	}
	if key != item.DispatchKey || outboxKey != key {
		return internalError(errors.New("recovered cancellation conflicts with dispatch key"), "link recovered cancellation")
	}
	if err := s.completeDispatchWithEvidenceTx(ctx, tx, item.RunID, jobID, dispatchLookupEvidence, observed); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return internalError(err, "commit recovered cancellation")
	}
	return nil
}

func (s *Store) pendingRunCancellations(ctx context.Context, runID string) ([]runCancellation, error) {
	query := `SELECT r.run_id, COALESCE(` + cancelStoredJobID + `, ''), r.status, r.required_envelope, r.dispatch_key
 FROM run_cancellations c JOIN runs r ON r.run_id=c.run_id
 LEFT JOIN dispatch_outbox o ON o.run_id=r.run_id
 WHERE (c.completed_ns IS NULL OR (r.l1_job_id IS NULL AND r.job_link_settled=0 AND (r.dispatch_attempt_ns IS NOT NULL OR o.job_id IS NOT NULL)
 AND COALESCE(CASE WHEN json_valid(o.last_error) THEN json_extract(o.last_error, '$.details.reason') END, '') NOT IN ('` + l1RegressedReason + `', '` + dispatchNotFoundReason + `')))
 AND c.retry_ns<=?`
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

// Link knowledge is independent of Run outcome, submission eligibility and
// cancel delivery. Failure counters never imply a durable absence verdict.
type linkKnowledgeState string

const (
	linkUnknown              linkKnowledgeState = "unknown"
	linkAttemptedUnconfirmed linkKnowledgeState = "attempted-unconfirmed"
	linkLinked               linkKnowledgeState = "linked"
	linkMissingSettled       linkKnowledgeState = "missing-settled"
	linkRegressed            linkKnowledgeState = "regressed"
)

type runJobLinkKnowledge struct {
	state linkKnowledgeState
	// jobID keeps the original identity even when it is no longer a live link.
	jobID string
}

func (s *Store) runJobLink(ctx context.Context, runID string) (runJobLinkKnowledge, error) {
	observed, err := readDispatchObservation(ctx, s.db, runID)
	if err != nil {
		return runJobLinkKnowledge{}, err
	}
	knowledge, err := decodeRunJobLink(observed)
	if err != nil {
		return runJobLinkKnowledge{}, internalError(err, "decode Run job link")
	}
	return knowledge, nil
}

func decodeRunJobLink(observed dispatchObservation) (runJobLinkKnowledge, error) {
	knowledge := runJobLinkKnowledge{state: linkUnknown}
	identities := []sql.NullString{observed.runJob, observed.outboxJob}
	// Malformed historical errors carry no usable diagnostic, as in the existing
	// guarded SQL decoder. A present identity, however, cannot be empty or conflict.
	if observed.lastError.Valid {
		var diagnostic struct {
			Details map[string]json.RawMessage `json:"details"`
		}
		if json.Unmarshal([]byte(observed.lastError.String), &diagnostic) == nil {
			if raw, ok := diagnostic.Details["l1_job_id"]; ok {
				var identity string
				if err := json.Unmarshal(raw, &identity); err != nil || identity == "" {
					return knowledge, errors.New("link diagnostic has an invalid job identity")
				}
				identities = append(identities, sql.NullString{String: identity, Valid: true})
			}
		}
	}
	for _, identity := range identities {
		if !identity.Valid {
			continue
		}
		if identity.String == "" {
			return knowledge, errors.New("Run job link has an empty identity")
		}
		if knowledge.jobID != "" && knowledge.jobID != identity.String {
			return knowledge, errors.New("Run job link has conflicting identities")
		}
		knowledge.jobID = identity.String
	}
	switch {
	case observed.reason == l1RegressedReason:
		if knowledge.jobID == "" {
			return knowledge, errors.New("regressed Run job link has no original identity")
		}
		knowledge.state = linkRegressed
	case observed.runJob.Valid:
		knowledge.state = linkLinked
	case observed.settled || observed.reason == dispatchNotFoundReason:
		knowledge.state = linkMissingSettled
	case observed.outboxJob.Valid:
		knowledge.state = linkLinked
	case observed.attempted.Valid:
		knowledge.state = linkAttemptedUnconfirmed
	}
	return knowledge, nil
}

// writeCancellationBackoff also schedules unresolved, completed legacy links.
// Their old delivery outcome stays recorded until positive evidence wakes it.
func (s *Store) writeCancellationBackoff(ctx context.Context, runID string, failures int, retry time.Time, payload string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE run_cancellations SET failures=failures+1, retry_ns=?, last_error=? WHERE run_id=? AND failures=? AND (completed_ns IS NULL OR EXISTS
 (SELECT 1 FROM runs r WHERE r.run_id=run_cancellations.run_id AND r.l1_job_id IS NULL AND r.job_link_settled=0 AND (r.dispatch_attempt_ns IS NOT NULL OR EXISTS
 (SELECT 1 FROM dispatch_outbox a WHERE a.run_id=r.run_id AND a.job_id IS NOT NULL))
 AND NOT EXISTS (SELECT 1 FROM dispatch_outbox o WHERE o.run_id=r.run_id
 AND COALESCE(CASE WHEN json_valid(o.last_error) THEN json_extract(o.last_error, '$.details.reason') END, '') IN ('`+l1RegressedReason+`', '`+dispatchNotFoundReason+`'))))`, retry.UnixNano(), payload, runID, failures)
	if err != nil {
		return internalError(err, "defer run cancellation")
	}
	return nil
}
