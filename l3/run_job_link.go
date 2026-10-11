package l3

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

// Run job link owns dispatch creation and attempts, token staging and cleanup,
// confirmation, terminal recovery and cancel ownership. L1 observations are
// obtained before short ledger writes that recheck ownership and identity.
// Migration and its resumable validation live here too; adoption of active
// regression and readers continues in #782–#784.

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
		if _, err := tx.ExecContext(ctx, `UPDATE dispatch_outbox SET job_id=COALESCE(job_id, ?), last_error=NULL, token_delivery=NULL WHERE run_id=? AND (job_id IS NULL OR job_id=?)`, jobID, runID, jobID); err != nil {
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

// uncontradictedOutbox is the shared eligibility rule for stored evidence and
// unresolved legacy cancellation retries. Both queries bind runs r and outbox o.
const uncontradictedOutbox = `r.job_link_settled=0 AND COALESCE(CASE WHEN json_valid(o.last_error) THEN json_extract(o.last_error, '$.details.reason') END, '') NOT IN ('` + l1RegressedReason + `', '` + dispatchNotFoundReason + `')`

// cancelStoredJobID prefers the Run link over uncontradicted outbox evidence.
const cancelStoredJobID = `COALESCE(r.l1_job_id, CASE WHEN ` + uncontradictedOutbox + ` THEN o.job_id END)`

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
	var jobID, runJob sql.NullString
	err = tx.QueryRowContext(ctx, `SELECT r.status, t.actor, r.dispatch_attempt_ns, r.l1_job_id, `+cancelStoredJobID+`
 FROM runs r JOIN run_triggers t ON t.run_id=r.run_id
 LEFT JOIN dispatch_outbox o ON o.run_id=r.run_id WHERE r.run_id=?`, runID).Scan(&state, &submitter, &attempted, &runJob, &jobID)
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
	if jobID.Valid && !runJob.Valid {
		if err := s.completeDispatchWithEvidenceTx(ctx, tx, runID, jobID.String, dispatchStoredEvidence, nil); err != nil {
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

// cancelAbsentStoredIdentity settles delivery for an already-terminal Run.
// A linked identity records regression; an unlinked verdict is retained.
// Preserve terminal facts and token expiry. Recheck the identity
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
	if current.runJob.Valid && current.runJob.String != "" {
		if !current.dispatched.Valid {
			return internalError(errors.New("linked job has no dispatch acknowledgement"), "settle absent identity cancellation")
		}
		if err := markTerminalLinkRegressedTx(ctx, tx, runID, current.runJob.String); err != nil {
			return err
		}
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
 WHERE (c.completed_ns IS NULL OR (r.l1_job_id IS NULL AND (r.dispatch_attempt_ns IS NOT NULL OR o.job_id IS NOT NULL)
 AND ` + uncontradictedOutbox + `))
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
		if observed.outboxJob.Valid && observed.dispatched.Valid {
			// A legacy acknowledgement confirmed this Job before its absence.
			knowledge.state = linkRegressed
		} else {
			knowledge.state = linkMissingSettled
		}
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
	_, err := s.db.ExecContext(ctx, `UPDATE run_cancellations SET failures=failures+1, retry_ns=?, last_error=CASE WHEN completed_ns IS NULL THEN ? ELSE last_error END WHERE run_id=? AND failures=? AND (completed_ns IS NULL OR EXISTS
 (SELECT 1 FROM runs r LEFT JOIN dispatch_outbox o ON o.run_id=r.run_id
 WHERE r.run_id=run_cancellations.run_id AND r.l1_job_id IS NULL
 AND (r.dispatch_attempt_ns IS NOT NULL OR o.job_id IS NOT NULL)
 AND `+uncontradictedOutbox+`))`, retry.UnixNano(), payload, runID, failures)
	if err != nil {
		return internalError(err, "defer run cancellation")
	}
	return nil
}

// insertDispatchIntentTx creates the outbox entry in the same transaction as
// its Run or rerun, so a committed Run always carries its stable dispatch key.
func insertDispatchIntentTx(ctx context.Context, tx *sql.Tx, runID, dispatchKey string) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO dispatch_outbox(run_id, dispatch_key) VALUES(?, ?)`, runID, dispatchKey)
	return err
}

func (s *Store) ensureRunToken(ctx context.Context, runID string) (string, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", internalError(err, "begin run token mint")
	}
	defer tx.Rollback()
	token, err := ensureRunTokenTx(ctx, tx, runID, canonicalTime(s.clock.Now()))
	if err != nil {
		return "", err
	}
	if err := tx.Commit(); err != nil {
		return "", internalError(err, "commit run token mint")
	}
	return token, nil
}

// ensureRunTokenTx returns the bearer staged for the run's pending dispatch,
// minting and staging one on the first attempt.
func ensureRunTokenTx(ctx context.Context, tx *sql.Tx, runID string, now time.Time) (string, error) {
	var delivery sql.NullString
	err := tx.QueryRowContext(ctx, `SELECT token_delivery FROM dispatch_outbox WHERE run_id=? AND dispatched_ns IS NULL`, runID).Scan(&delivery)
	if errors.Is(err, sql.ErrNoRows) {
		return "", protocolError(contract.ErrorConflict, "run %q has no pending dispatch", runID)
	}
	if err != nil {
		return "", internalError(err, "read pending run token delivery")
	}
	if delivery.Valid && delivery.String != "" {
		return delivery.String, nil
	}
	token := newToken()
	digest := sha256.Sum256([]byte(token))
	attemptID := newID("runattempt")
	if _, err := tx.ExecContext(ctx, `INSERT INTO run_tokens(run_id, attempt_id, token_hash, minted_ns) VALUES(?, ?, ?, ?)`, runID, attemptID, digest[:], now.UnixNano()); err != nil {
		return "", internalError(err, "store run token hash")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE dispatch_outbox SET token_delivery=? WHERE run_id=? AND dispatched_ns IS NULL`, token, runID); err != nil {
		return "", internalError(err, "stage run token delivery")
	}
	return token, nil
}

// failRunTx fails a non-terminal run from the ledger side and records why.
// The reason is written only by the transition that fails the run, so the
// first cause is the one kept.
//
// The run's node attribution is cleared and marked pending when an L1 job
// exists. The ledger cannot tell which node wrote the failing evidence -- a
// run token names the run, not the L1 attempt -- and a node recorded while
// the job was live may be one an earlier attempt failed on before it was
// requeued. Once the job settles, the reconciler fills the node from the
// attempt that settled it, from empty only (#604 review).
func failRunTx(ctx context.Context, tx *sql.Tx, runID string, now time.Time, tokenGrace time.Duration, reason string) error {
	result, err := tx.ExecContext(ctx, `UPDATE runs SET status=?, failure_reason=?, updated_ns=?,
  started_ns=COALESCE(started_ns, ?), finished_ns=COALESCE(finished_ns, ?),
  node_id=CASE WHEN l1_job_id IS NULL THEN node_id ELSE NULL END,
  node_attribution_pending=CASE WHEN l1_job_id IS NULL THEN 0 ELSE 1 END
WHERE run_id=? AND status NOT IN (?, ?)`,
		contract.RunFailed, nullableReason(reason), now.UnixNano(), now.UnixNano(), now.UnixNano(), runID, contract.RunSucceeded, contract.RunFailed)
	if err != nil {
		return internalError(err, "fail run protocol")
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return internalError(err, "read protocol failure result")
	}
	if changed == 1 {
		expires := canonicalTime(now.Add(tokenGrace))
		if _, err := tx.ExecContext(ctx, `UPDATE run_tokens SET expires_ns=COALESCE(expires_ns, ?) WHERE run_id=?`, expires.UnixNano(), runID); err != nil {
			return internalError(err, "expire protocol-failed run token")
		}
	}
	return clearStagedTokenDelivery(ctx, tx, runID)
}

func clearDispatchRetry(ctx context.Context, tx *sql.Tx, runID string) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM dispatch_retry WHERE run_id=?`, runID); err != nil {
		return internalError(err, "clear dispatch retry")
	}
	return nil
}

// clearStagedTokenDelivery drops a terminal run's staged run-token bearer. A
// terminal run is never dispatched again, so a bearer still staged for a
// dispatch L3 never recorded -- L1 accepted the job, L3 crashed before
// completeDispatch, and the run then ended -- would otherwise stay in the
// outbox in plaintext forever (#52).
func clearStagedTokenDelivery(ctx context.Context, tx *sql.Tx, runID string) error {
	if err := clearDispatchRetry(ctx, tx, runID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE dispatch_outbox SET token_delivery=NULL WHERE run_id=? AND token_delivery IS NOT NULL`, runID); err != nil {
		return internalError(err, "clear terminal run's staged token delivery")
	}
	return nil
}

func (s *Store) pendingDispatches(ctx context.Context) ([]dispatchIntent, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT r.run_id, r.dispatch_key, COALESCE(r.parent_run_id, ''),
       CASE WHEN t.source='rerun' THEN t.source_run_id ELSE r.run_id END,
       s.content, s.sha256, s.interpreter_json, s.mode, i.program_json, r.tags_json, r.limits_json,
       r.params_json, r.dispatch_authority
FROM dispatch_outbox o JOIN runs r ON r.run_id=o.run_id LEFT JOIN run_scripts s ON s.run_id=r.run_id
LEFT JOIN run_images i ON i.run_id=r.run_id
JOIN run_triggers t ON t.run_id=r.run_id
WHERE o.dispatched_ns IS NULL AND r.status IN (?, ?)
AND NOT EXISTS (SELECT 1 FROM run_cancellations c WHERE c.run_id=r.run_id)
AND NOT EXISTS (SELECT 1 FROM dispatch_hold WHERE since_ns IS NOT NULL)
AND NOT EXISTS (SELECT 1 FROM dispatch_retry d WHERE d.run_id=r.run_id AND d.retry_ns>?)
ORDER BY r.created_ns, r.run_id`, contract.RunPending, contract.RunDispatching, s.recoveryNow().UnixNano())
	if err != nil {
		return nil, internalError(err, "list pending dispatches")
	}
	defer rows.Close()
	var intents []dispatchIntent
	for rows.Next() {
		var intent dispatchIntent
		var content, interpreterJSON, imageJSON, tagsJSON, limitsJSON, paramsJSON []byte
		var sha sql.NullString
		var mode sql.NullInt64
		if err := rows.Scan(&intent.RunID, &intent.DispatchKey, &intent.ParentRunID, &intent.HandoffOwnerID, &content, &sha, &interpreterJSON, &mode, &imageJSON, &tagsJSON, &limitsJSON, &paramsJSON, &intent.DispatchAuthority); err != nil {
			return nil, internalError(err, "scan pending dispatch")
		}
		if len(imageJSON) > 0 {
			intent.Image = &contract.ImageProgram{}
			if err := json.Unmarshal(imageJSON, intent.Image); err != nil {
				return nil, internalError(err, "decode dispatch image program")
			}
		} else {
			intent.Content = content
			intent.SHA256 = sha.String
			intent.Mode = uint32(mode.Int64)
			if err := json.Unmarshal(interpreterJSON, &intent.Interpreter); err != nil {
				return nil, internalError(err, "decode dispatch interpreter")
			}
		}
		if err := json.Unmarshal(tagsJSON, &intent.Tags); err != nil {
			return nil, internalError(err, "decode dispatch tags")
		}
		intent.Params = paramsJSON
		if len(limitsJSON) > 0 {
			intent.Limits = &contract.RunLimits{}
			if err := json.Unmarshal(limitsJSON, intent.Limits); err != nil {
				return nil, internalError(err, "decode dispatch limits")
			}
		}
		intents = append(intents, intent)
	}
	if err := rows.Err(); err != nil {
		return nil, internalError(err, "iterate pending dispatches")
	}
	return intents, nil
}

// beginDispatch starts one submit attempt and returns the bearer it carries.
// The run-status and cancellation guards, bearer hand-out and attempt count
// share one transaction. A run that ends or is canceled after this commit,
// while the submit is in flight, is linked by completeDispatch when L1
// acknowledges the job, or by lookup recovery when it does not.
func (s *Store) beginDispatch(ctx context.Context, runID string) (string, error) {
	now := canonicalTime(s.clock.Now())
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", internalError(err, "begin dispatch attempt")
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `UPDATE runs SET status=CASE WHEN status=? THEN ? ELSE status END, updated_ns=?, dispatch_attempt_ns=?
WHERE run_id=? AND status IN (?, ?)
AND NOT EXISTS (SELECT 1 FROM run_cancellations c WHERE c.run_id=runs.run_id)
AND NOT EXISTS (SELECT 1 FROM dispatch_hold WHERE since_ns IS NOT NULL)
AND NOT EXISTS (SELECT 1 FROM dispatch_retry d WHERE d.run_id=runs.run_id AND d.retry_ns>?)`,
		contract.RunPending, contract.RunDispatching, now.UnixNano(), now.UnixNano(), runID, contract.RunPending, contract.RunDispatching, now.UnixNano())
	if err != nil {
		return "", internalError(err, "mark dispatch attempt")
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return "", internalError(err, "read dispatch attempt result")
	}
	if changed == 0 {
		return "", errDispatchAbandoned
	}
	token, err := ensureRunTokenTx(ctx, tx, runID, now)
	if err != nil {
		return "", err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE dispatch_outbox SET attempt_count=attempt_count+1, last_error=NULL WHERE run_id=? AND dispatched_ns IS NULL`, runID); err != nil {
		return "", internalError(err, "increment dispatch attempt")
	}
	if err := tx.Commit(); err != nil {
		return "", internalError(err, "commit dispatch attempt")
	}
	return token, nil
}

func (s *Store) recordDispatchError(ctx context.Context, runID string, dispatchErr error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return internalError(err, "begin dispatch retry")
	}
	defer tx.Rollback()
	payload, err := json.Marshal(apiErrorFrom(dispatchErr))
	if err != nil {
		return internalError(err, "encode dispatch error")
	}
	// A submit error that returns after recovery settled the run must not
	// replace the settled diagnostic.
	_, err = tx.ExecContext(ctx, `UPDATE dispatch_outbox SET last_error=? WHERE run_id=? AND dispatched_ns IS NULL
  AND NOT EXISTS (SELECT 1 FROM runs r WHERE r.run_id=dispatch_outbox.run_id AND r.job_link_settled=1)`, string(payload), runID)
	if err != nil {
		return internalError(err, "record dispatch error")
	}
	var waiting bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM runs r JOIN dispatch_outbox o ON o.run_id=r.run_id
 WHERE r.run_id=? AND r.status IN ('pending','dispatching') AND o.dispatched_ns IS NULL AND r.job_link_settled=0
 AND NOT EXISTS(SELECT 1 FROM run_cancellations c WHERE c.run_id=r.run_id))`, runID).Scan(&waiting); err != nil {
		return internalError(err, "read dispatch retry eligibility")
	}
	if !waiting {
		return tx.Commit()
	}
	var failures int
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE((SELECT failures FROM dispatch_retry WHERE run_id=?),0)`, runID).Scan(&failures); err != nil {
		return internalError(err, "read dispatch retries")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO dispatch_retry(run_id,failures,retry_ns) VALUES(?,?,?) ON CONFLICT(run_id) DO UPDATE SET failures=excluded.failures,retry_ns=excluded.retry_ns`, runID, failures+1, s.recoveryNow().Add(dispatchRetryBackoff(failures)).UnixNano()); err != nil {
		return internalError(err, "schedule dispatch retry")
	}
	return tx.Commit()
}

func (s *Store) failDispatch(ctx context.Context, runID string, dispatchErr error) error {
	now := canonicalTime(s.clock.Now())
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return internalError(err, "begin permanent dispatch failure")
	}
	defer tx.Rollback()
	payload, err := json.Marshal(apiErrorFrom(dispatchErr))
	if err != nil {
		return internalError(err, "encode permanent dispatch error")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE dispatch_outbox SET last_error=CASE
    WHEN EXISTS (SELECT 1 FROM runs r WHERE r.run_id=dispatch_outbox.run_id AND r.job_link_settled=1) THEN last_error ELSE ? END,
  token_delivery=NULL
WHERE run_id=? AND dispatched_ns IS NULL`, string(payload), runID); err != nil {
		return internalError(err, "record permanent dispatch error")
	}
	if err := failRunTx(ctx, tx, runID, now, s.tokenGrace, dispatchFailureReason(dispatchErr)); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return internalError(err, "commit permanent dispatch failure")
	}
	return nil
}

type dispatchIntent struct {
	RunID          string
	DispatchKey    string
	ParentRunID    string
	HandoffOwnerID string
	// Params is the run's canonical parameter document. It reaches the job as
	// a file in the run mailbox, written by the node agent, so a workload can
	// read what it was submitted with while holding no credential.
	Params []byte
	// DispatchAuthority is the run's stored public declaration that its
	// workload dispatches child work. Dispatch converts it into both wire
	// labels — the positive dispatch_authority marker when it is true, the
	// withhold_workload_credentials instruction when it is false — which the
	// node agent reads alongside L1's own record of who submitted the job.
	DispatchAuthority bool
	Content           []byte
	SHA256            string
	Interpreter       []string
	Mode              uint32
	Image             *contract.ImageProgram
	Tags              []string
	Limits            *contract.RunLimits
}

func runDispatchHold(ctx context.Context, q holdReader, runID string) (*contract.DispatchHold, error) {
	var waiting bool
	err := q.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM runs r JOIN dispatch_outbox o ON o.run_id=r.run_id
 WHERE r.run_id=? AND r.status IN ('pending','dispatching') AND o.dispatched_ns IS NULL
 AND NOT EXISTS(SELECT 1 FROM run_cancellations c WHERE c.run_id=r.run_id))`, runID).Scan(&waiting)
	if err != nil {
		return nil, internalError(err, "read dispatch wait")
	}
	if !waiting {
		return nil, nil
	}
	return readDispatchHold(ctx, q)
}

// errDispatchAbandoned reports that a terminal transition or cancellation
// request won, or admission/retry is deferred. The caller must not submit:
// replay could create new work after the run ended or was told to stop.
var errDispatchAbandoned = errors.New("l3: dispatch attempt abandoned")

// migrateLegacyRunJobLinks commits the links, their validation work and the
// one-time marker together. The store's prerequisite backfills have run before
// this call. Store open never contacts L1.
func (s *Store) migrateLegacyRunJobLinks(ctx context.Context) error {
	var exists bool
	if err := s.db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM sqlite_schema WHERE name='run_job_link_migration' AND type='table')`).Scan(&exists); err != nil {
		return err
	}
	if exists {
		var done bool
		if err := s.db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM run_job_link_migration WHERE id=1)`).Scan(&done); err != nil {
			return err
		}
		if done {
			return nil
		}
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS run_job_link_migration (id INTEGER PRIMARY KEY CHECK(id=1));
CREATE TABLE IF NOT EXISTS run_job_link_validations (
 run_id TEXT PRIMARY KEY REFERENCES runs(run_id), dispatch_key TEXT NOT NULL, job_id TEXT NOT NULL,
 failures INTEGER NOT NULL DEFAULT 0, retry_ns INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS run_job_link_validations_due ON run_job_link_validations(retry_ns,run_id);`); err != nil {
		return err
	}
	// Recheck under the immediate write lock, after any competing opener commits.
	var done bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM run_job_link_migration WHERE id=1)`).Scan(&done); err != nil {
		return err
	}
	if done {
		return nil
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO run_job_link_validations(run_id, dispatch_key, job_id)
 SELECT r.run_id, r.dispatch_key, o.job_id FROM runs r JOIN dispatch_outbox o ON o.run_id=r.run_id
 WHERE r.status IN ('succeeded','failed') AND r.l1_job_id IS NULL AND r.job_link_settled=0
 AND r.dispatch_attempt_ns IS NOT NULL AND o.dispatch_key=r.dispatch_key
 AND o.job_id IS NOT NULL AND o.job_id<>'' AND o.dispatched_ns IS NOT NULL
 AND o.token_delivery IS NULL AND o.last_error IS NULL
 AND COALESCE(r.failure_reason,'') NOT IN ('the run was canceled before dispatch','canceled before dispatch','canceled before dispatch was confirmed')
 AND NOT EXISTS (SELECT 1 FROM run_cancellations c WHERE c.run_id=r.run_id)`); err != nil {
		return err
	}
	rows, err := tx.QueryContext(ctx, `SELECT run_id,job_id FROM run_job_link_validations`)
	if err != nil {
		return err
	}
	var links [][2]string
	for rows.Next() {
		var item [2]string
		if err := rows.Scan(&item[0], &item[1]); err != nil {
			rows.Close()
			return err
		}
		links = append(links, item)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, item := range links {
		if _, err := linkTerminalRunTx(ctx, tx, item[0], item[1]); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO run_job_link_migration(id) VALUES(1)`); err != nil {
		return err
	}
	return tx.Commit()
}

type migratedLinkValidation struct {
	runID, dispatchKey, jobID string
	failures                  int
	retryNS                   int64
	observed                  dispatchObservation
	owned                     bool
}

// This predicate also prevents obsolete validation work from holding node
// attribution after cancellation or newer link evidence takes ownership.
const migratedLinkOwnership = `r.dispatch_key=v.dispatch_key AND o.dispatch_key=v.dispatch_key
 AND r.l1_job_id=v.job_id AND o.job_id=v.job_id AND o.dispatched_ns IS NOT NULL
 AND r.status IN ('succeeded','failed') AND o.last_error IS NULL AND o.token_delivery IS NULL
 AND NOT EXISTS (SELECT 1 FROM run_cancellations c WHERE c.run_id=r.run_id)`

// Reading the due prefix bounds each pass even when some work is superseded.
// Superseded work retires without an L1 read or a link verdict.
func (s *Store) migratedLinkValidations(ctx context.Context) ([]migratedLinkValidation, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT v.run_id,v.dispatch_key,v.job_id,v.failures,v.retry_ns, `+dispatchObservationColumns+`, COALESCE((`+migratedLinkOwnership+`),0)
 FROM run_job_link_validations v JOIN runs r ON r.run_id=v.run_id JOIN dispatch_outbox o ON o.run_id=r.run_id
 WHERE v.retry_ns<=? ORDER BY v.retry_ns,v.run_id LIMIT ?`, s.recoveryNow().UnixNano(), unrecordedDispatchBatch)
	if err != nil {
		return nil, internalError(err, "list migrated link validations")
	}
	defer rows.Close()
	var pending []migratedLinkValidation
	for rows.Next() {
		var item migratedLinkValidation
		o := &item.observed
		if err := rows.Scan(&item.runID, &item.dispatchKey, &item.jobID, &item.failures, &item.retryNS, &o.outboxJob, &o.runJob, &o.lastError, &o.dispatched, &o.settled, &o.state, &o.reason, &o.diagnosticJob, &o.attempted, &item.owned); err != nil {
			return nil, internalError(err, "read migrated link validation")
		}
		pending = append(pending, item)
	}
	return pending, rows.Err()
}

type migratedLinkVerdict int

const (
	migratedLinkFound migratedLinkVerdict = iota
	migratedLinkAbsent
	migratedLinkRetry
	migratedLinkRetired
)

// completeMigratedLinkValidation rechecks both identity and ownership after
// the remote read. A crash before this commit leaves the lookup retryable.
// Concurrent validators cannot commit over a completed verdict or newer retry.
// The result is true only when this observation committed a verdict or retry.
func (s *Store) completeMigratedLinkValidation(ctx context.Context, item migratedLinkValidation, verdict migratedLinkVerdict) (bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, internalError(err, "begin migrated link validation")
	}
	defer tx.Rollback()
	var owned bool
	err = tx.QueryRowContext(ctx, `SELECT COALESCE((`+migratedLinkOwnership+`),0) FROM run_job_link_validations v
 JOIN runs r ON r.run_id=v.run_id JOIN dispatch_outbox o ON o.run_id=r.run_id
 WHERE v.run_id=? AND v.dispatch_key=? AND v.job_id=? AND v.failures=? AND v.retry_ns=?`, item.runID, item.dispatchKey, item.jobID, item.failures, item.retryNS).Scan(&owned)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, internalError(err, "recheck migrated link ownership")
	}
	if !owned {
		// The migrated identity or its recovery ownership no longer applies. This
		// retires work, not a completed validation, and writes no link verdict.
		if _, err := tx.ExecContext(ctx, `DELETE FROM run_job_link_validations WHERE run_id=?`, item.runID); err != nil {
			return false, internalError(err, "retire superseded link validation")
		}
		return false, tx.Commit()
	}
	if verdict == migratedLinkRetired {
		return false, nil
	}
	current, err := readDispatchObservation(ctx, tx, item.runID)
	if err != nil {
		return false, err
	}
	if current != item.observed {
		return false, nil
	}
	if verdict == migratedLinkRetry {
		if _, err := tx.ExecContext(ctx, `UPDATE run_job_link_validations SET failures=failures+1,retry_ns=? WHERE run_id=?`, s.recoveryNow().Add(unrecordedDispatchBackoff(item.failures)).UnixNano(), item.runID); err != nil {
			return false, internalError(err, "defer migrated link validation")
		}
	} else {
		if verdict == migratedLinkAbsent {
			if err := markTerminalLinkRegressedTx(ctx, tx, item.runID, item.jobID); err != nil {
				return false, err
			}
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM run_job_link_validations WHERE run_id=?`, item.runID); err != nil {
			return false, internalError(err, "complete migrated link validation")
		}
	}
	if err := tx.Commit(); err != nil {
		return false, internalError(err, "commit migrated link validation")
	}
	return true, nil
}

// markTerminalLinkRegressedTx is separate from active-run regression: terminal
// outcome, reason, timestamps and token expiry must never be rewritten. The
// caller has rechecked the captured acknowledgement and current ownership.
func markTerminalLinkRegressedTx(ctx context.Context, tx *sql.Tx, runID, jobID string) error {
	cause := contract.APIError{Code: contract.ErrorNotFound, Message: "L1 regressed: previously dispatched job is absent; work was not replayed", Details: map[string]any{"reason": l1RegressedReason, "l1_job_id": jobID}}
	payload, err := json.Marshal(cause)
	if err != nil {
		return internalError(err, "encode terminal link regression")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE dispatch_outbox SET last_error=? WHERE run_id=?`, string(payload), runID); err != nil {
		return internalError(err, "record terminal link regression")
	}
	return nil
}

// Node attribution must not bypass the validation queue's backoff and budget
// by making an extra, unbounded L1 read for each newly migrated link.
func (s *Store) pendingMigratedLinkValidation(ctx context.Context, runID string) (bool, error) {
	var pending bool
	err := s.db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM run_job_link_validations v
 JOIN runs r ON r.run_id=v.run_id JOIN dispatch_outbox o ON o.run_id=r.run_id
 WHERE v.run_id=? AND (`+migratedLinkOwnership+`))`, runID).Scan(&pending)
	return pending, err
}

// unrecordedDispatch is a terminal run for which at least one dispatch began
// but L3 never recorded the L1 job association.
type unrecordedDispatch struct {
	RunID, DispatchKey, OutboxJobID string
	observed                        dispatchObservation
}

// unrecordedDispatchesQuery repeats the partial-index predicate literally so
// SQLite walks only the due prefix of runs_job_link_recovery, in index order:
// rows never backed off first, oldest first, then backed-off rows as they
// come due. CROSS JOIN keeps runs the outer loop so LIMIT stops the walk.
const unrecordedDispatchesQuery = `SELECT r.run_id, r.dispatch_key, COALESCE(o.job_id, ''), ` + dispatchObservationColumns + `
FROM runs r INDEXED BY runs_job_link_recovery CROSS JOIN dispatch_outbox o ON o.run_id=r.run_id
WHERE r.l1_job_id IS NULL AND r.job_link_settled=0 AND r.status IN ('succeeded','failed') AND r.dispatch_attempt_ns IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM run_cancellations c WHERE c.run_id=r.run_id)
  AND r.job_link_retry_ns<=?
ORDER BY r.job_link_retry_ns, r.created_ns, r.run_id
LIMIT ?`

const (
	// unrecordedDispatchBatch bounds the rows one recovery pass reads.
	unrecordedDispatchBatch = 16
	// A row whose L1 reads failed transiently waits unrecordedDispatchRetryBase,
	// doubling per consecutive failure up to unrecordedDispatchRetryCap.
	unrecordedDispatchRetryBase = 30 * time.Second
	unrecordedDispatchRetryCap  = 30 * time.Minute
	// unrecordedDispatchSettleHorizon is how long after the run's last submit
	// attempt a dispatch-key absence stays provisional. L1 may commit a submit
	// after recovery first asks, with the response lost; only an absence at
	// or after the horizon settles the run.
	unrecordedDispatchSettleHorizon = time.Hour
)

func (s *Store) unrecordedDispatches(ctx context.Context, limit int) ([]unrecordedDispatch, error) {
	now := canonicalTime(s.clock.Now())
	rows, err := s.db.QueryContext(ctx, unrecordedDispatchesQuery, now.UnixNano(), limit)
	if err != nil {
		return nil, internalError(err, "list unrecorded dispatches")
	}
	defer rows.Close()
	var pending []unrecordedDispatch
	for rows.Next() {
		var item unrecordedDispatch
		if err := scanUnrecordedDispatch(rows, &item); err != nil {
			return nil, internalError(err, "scan unrecorded dispatch")
		}
		pending = append(pending, item)
	}
	if err := rows.Err(); err != nil {
		return nil, internalError(err, "iterate unrecorded dispatches")
	}
	return pending, nil
}

// deferUnrecordedDispatch backs one row off after a transient L1 failure.
func (s *Store) deferUnrecordedDispatch(ctx context.Context, runID string) error {
	now := canonicalTime(s.clock.Now())
	var failures int
	if err := s.db.QueryRowContext(ctx, `SELECT job_link_failures FROM runs WHERE run_id=?`, runID).Scan(&failures); err != nil {
		return internalError(err, "read unrecorded dispatch failures")
	}
	_, err := s.db.ExecContext(ctx, `UPDATE runs SET job_link_failures=job_link_failures+1, job_link_retry_ns=?
WHERE run_id=? AND l1_job_id IS NULL AND job_link_settled=0 AND job_link_failures=?
 AND NOT EXISTS (SELECT 1 FROM run_cancellations c WHERE c.run_id=runs.run_id)`,
		now.Add(unrecordedDispatchBackoff(failures)).UnixNano(), runID, failures)
	if err != nil {
		return internalError(err, "defer unrecorded dispatch")
	}
	return nil
}

// unrecordedDispatchBackoff is the wait after failures earlier consecutive
// transient failures plus this one.
func unrecordedDispatchBackoff(failures int) time.Duration {
	delay := unrecordedDispatchRetryBase
	for i := 0; i < failures && delay < unrecordedDispatchRetryCap; i++ {
		delay *= 2
	}
	return min(delay, unrecordedDispatchRetryCap)
}

// recoveryNow is the ledger clock reading recovery stamps on an L1 read before
// sending it.
func (s *Store) recoveryNow() time.Time { return canonicalTime(s.clock.Now()) }

// unrecordedDispatchFor rereads one run's recovery state after a settlement
// compare-and-set missed. It reports false once the run is no longer waiting.
func (s *Store) unrecordedDispatchFor(ctx context.Context, runID string) (unrecordedDispatch, bool, error) {
	return unrecordedDispatchForTx(ctx, s.db, runID)
}

type rowQuerier interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// scanUnrecordedDispatch captures the verdict in the same read that selects or
// rereads recovery's dispatch, before any L1 lookup can begin.
func scanUnrecordedDispatch(row interface{ Scan(...any) error }, item *unrecordedDispatch) error {
	observed := &item.observed
	return row.Scan(&item.RunID, &item.DispatchKey, &item.OutboxJobID,
		&observed.outboxJob, &observed.runJob, &observed.lastError, &observed.dispatched,
		&observed.settled, &observed.state, &observed.reason, &observed.diagnosticJob, &observed.attempted)
}

func unrecordedDispatchForTx(ctx context.Context, db rowQuerier, runID string) (unrecordedDispatch, bool, error) {
	item := unrecordedDispatch{RunID: runID}
	row := db.QueryRowContext(ctx, `SELECT r.run_id, r.dispatch_key, COALESCE(o.job_id, ''), `+dispatchObservationColumns+`
FROM runs r JOIN dispatch_outbox o ON o.run_id=r.run_id
WHERE r.run_id=? AND r.l1_job_id IS NULL AND r.job_link_settled=0 AND r.status IN (?, ?) AND r.dispatch_attempt_ns IS NOT NULL
 AND NOT EXISTS (SELECT 1 FROM run_cancellations c WHERE c.run_id=r.run_id)`,
		runID, contract.RunSucceeded, contract.RunFailed)
	err := scanUnrecordedDispatch(row, &item)
	if errors.Is(err, sql.ErrNoRows) {
		return unrecordedDispatch{}, false, nil
	}
	if err != nil {
		return unrecordedDispatch{}, false, internalError(err, "reread unrecorded dispatch")
	}
	return item, true, nil
}

var errRecoveredDispatchIdentityConflict = errors.New("recovered dispatch conflicts with recorded identity")

type settleOutcome int

const (
	settled settleOutcome = iota
	// settleChanged: the row changed since it was read; reread it.
	settleChanged
	// settleNotDue: an unacknowledged dispatch's absence is still inside the
	// settle horizon; ask again after the backoff.
	settleNotDue
)

// settleUnrecordedDispatch records one authoritative absence and stops future
// lookups without changing the already-terminal run. It settles nothing when
// the row changed since item was read, or when the dispatch was never
// acknowledged and lookupStarted, the time the absent answer was asked for,
// is earlier than the settle horizon after the run's last submit attempt. A
// lookup in flight across the horizon may have missed a submit L1 committed
// meanwhile, so the answer counts from when it was asked, not received.
func (s *Store) settleUnrecordedDispatch(ctx context.Context, item unrecordedDispatch, lookupStarted time.Time) (settleOutcome, error) {
	horizon := canonicalTime(lookupStarted).Add(-unrecordedDispatchSettleHorizon)
	cause := contract.APIError{
		Code: contract.ErrorNotFound, Message: "L1 did not find the attempted dispatch; work was not replayed",
		Details: map[string]any{"reason": dispatchNotFoundReason, "dispatch_key": item.DispatchKey},
	}
	if item.OutboxJobID != "" {
		cause = contract.APIError{
			Code: contract.ErrorNotFound, Message: "L1 regressed: previously dispatched job is absent; work was not replayed",
			Details: map[string]any{"reason": l1RegressedReason, "l1_job_id": item.OutboxJobID},
		}
	}
	payload, err := json.Marshal(cause)
	if err != nil {
		return settled, internalError(err, "encode unrecorded dispatch settlement")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return settled, internalError(err, "begin unrecorded dispatch settlement")
	}
	defer tx.Rollback()
	// Compare-and-set on the acknowledgement recovery read before its L1
	// reads: one recorded since then is L1's own answer, which the caller
	// links instead of settling over it. An acknowledged job's absence by ID
	// settles at once; L1 committed it before acknowledging it.
	result, err := tx.ExecContext(ctx, `UPDATE runs SET job_link_settled=1
WHERE run_id=? AND l1_job_id IS NULL AND job_link_settled=0 AND status IN (?, ?)
  AND NOT EXISTS (SELECT 1 FROM run_cancellations c WHERE c.run_id=runs.run_id)
  AND dispatch_key=?
  AND (SELECT COALESCE(o.job_id, '') FROM dispatch_outbox o WHERE o.run_id=runs.run_id)=?
  AND (?<>'' OR dispatch_attempt_ns<=?)`,
		item.RunID, contract.RunSucceeded, contract.RunFailed, item.DispatchKey, item.OutboxJobID, item.OutboxJobID, horizon.UnixNano())
	if err != nil {
		return settled, internalError(err, "settle unrecorded dispatch")
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return settled, internalError(err, "read unrecorded dispatch settlement result")
	}
	if changed == 0 {
		current, waiting, err := unrecordedDispatchForTx(ctx, tx, item.RunID)
		if err != nil {
			return settled, err
		}
		if waiting && current == item {
			return settleNotDue, nil
		}
		return settleChanged, nil
	}
	if _, err := tx.ExecContext(ctx, `UPDATE dispatch_outbox SET
  last_error=CASE WHEN last_error IS NULL OR json_extract(last_error, '$.retryable')=1 THEN ? ELSE last_error END,
  token_delivery=NULL
WHERE run_id=?`, string(payload), item.RunID); err != nil {
		return settled, internalError(err, "record unrecorded dispatch settlement")
	}
	if err := tx.Commit(); err != nil {
		return settled, internalError(err, "commit unrecorded dispatch settlement")
	}
	return settled, nil
}

// recoverUnrecordedDispatches links terminal runs by read-only L1 reads only.
// Replaying SubmitJob is impossible after #52 cleared the staged bearer, and
// unsafe when L1 has regressed because it could recreate side effects for an
// ended run.
//
// Each pass reads a bounded batch of the rows that are due, inside its budget,
// and stops starting L1 reads once that budget is spent. A row whose L1 reads
// fail transiently or reveal conflicting recorded identities, including one
// the budget cut short, backs off
// exponentially, so an unavailable L1 costs one budget per pass at most and
// later rows still get their turn.
func (r *Reconciler) recoverUnrecordedDispatches(ctx context.Context) []error {
	remote, cancel := context.WithTimeout(ctx, r.budget)
	defer cancel()
	return r.recoverUnrecordedDispatchesRemote(ctx, remote)
}

func (r *Reconciler) recoverUnrecordedDispatchesRemote(ctx, remote context.Context) []error {
	// Validation gets at most half the remaining budget. A slow migrated Job
	// must leave time for ordinary terminal dispatch recovery in this pass.
	validationBudget := r.budget / 2
	if deadline, ok := remote.Deadline(); ok {
		validationBudget = time.Until(deadline) / 2
	}
	validationRemote, cancel := context.WithTimeout(remote, validationBudget)
	passErrors := r.validateMigratedLinks(ctx, validationRemote)
	cancel()
	if r.lookup == nil || remote.Err() != nil {
		return passErrors
	}
	pending, err := r.store.unrecordedDispatches(remote, unrecordedDispatchBatch)
	if err != nil {
		return append(passErrors, err)
	}
	for _, item := range pending {
		if remote.Err() != nil {
			break
		}
		if err := r.recoverUnrecordedDispatch(ctx, remote, item); err != nil {
			passErrors = append(passErrors, err)
		}
	}
	return passErrors
}

type recoveryOutcome int

const (
	recoveryDone recoveryOutcome = iota
	// recoveryTransient is a remote failure or identity conflict: back the row off.
	recoveryTransient
	// recoveryStale is a settlement compare-and-set miss: reread the row.
	recoveryStale
	// recoveryNotDue is an unacknowledged dispatch's absence inside the
	// settle horizon: not an error yet, but asked again only after the
	// backoff, the same as a transient failure.
	recoveryNotDue
)

// maxRecoveryRereads bounds how often one row is reread after its settlement
// compare-and-set missed. An acknowledgement or cancellation ownership may
// have landed; a row still contended after that is left for the next pass.
const maxRecoveryRereads = 2

// recoverUnrecordedDispatch resolves one row. L1 reads use remote, bounded by
// the pass budget; ledger writes use ctx so a spent budget cannot drop them.
func (r *Reconciler) recoverUnrecordedDispatch(ctx, remote context.Context, item unrecordedDispatch) error {
	for rereads := 0; ; rereads++ {
		outcome, err := r.resolveUnrecordedDispatch(ctx, remote, item)
		switch {
		case outcome == recoveryTransient || outcome == recoveryNotDue:
			if ctx.Err() == nil {
				if deferErr := r.store.deferUnrecordedDispatch(ctx, item.RunID); deferErr != nil {
					err = errors.Join(err, deferErr)
				}
			}
			return err
		case outcome != recoveryStale || rereads == maxRecoveryRereads:
			return err
		}
		next, waiting, err := r.store.unrecordedDispatchFor(ctx, item.RunID)
		if err != nil || !waiting {
			return err
		}
		item = next
	}
}

// recoveredDispatchOutcome backs off corrupt identities without treating an
// unrelated ledger failure as a remote outage. Cancellation ownership still
// fences the recovery backoff write.
func recoveredDispatchOutcome(err error) (recoveryOutcome, error) {
	if errors.Is(err, errRecoveredDispatchIdentityConflict) {
		return recoveryTransient, err
	}
	return recoveryDone, err
}

// resolveUnrecordedDispatch links or settles one row from what L1 answers.
// It reports stale when the settlement compare-and-set found the row changed
// since it was read, so the caller rereads it rather than settle over an
// acknowledgement or cancellation ownership that arrived meanwhile. It reports
// not due when an unacknowledged dispatch's absence is still inside the
// settle horizon.
func (r *Reconciler) resolveUnrecordedDispatch(ctx, remote context.Context, item unrecordedDispatch) (recoveryOutcome, error) {
	lookupStarted := r.store.recoveryNow()
	job, err := r.lookup.LookupJobByDispatchKey(remote, item.DispatchKey)
	var absence error
	switch {
	case err == nil && (item.OutboxJobID == "" || job.JobID == item.OutboxJobID):
		return recoveredDispatchOutcome(r.linkRecoveredDispatch(ctx, remote, item, job.JobID, &item.observed))
	case err == nil:
		absence = fmt.Errorf("L1 dispatch %q resolved to job %q, not acknowledged job %q", item.DispatchKey, job.JobID, item.OutboxJobID)
	case isMissingDispatch(err, item.DispatchKey):
		absence = err
	default:
		return recoveryTransient, r.store.observeL1Error(ctx, err)
	}
	if item.OutboxJobID != "" {
		// L1 acknowledged this job, so only its authoritative absence by ID
		// is a regression, the same evidence failMissingL1Job requires. The
		// scoped lookup cannot see a job L1 stored before it recorded
		// run-ledger provenance, and that job is still this run's to link.
		_, err := r.jobs.GetJob(remote, item.OutboxJobID)
		if err == nil {
			return recoveredDispatchOutcome(r.linkRecoveredDispatch(ctx, remote, item, item.OutboxJobID, &item.observed))
		}
		if !isMissingL1Job(err, item.OutboxJobID) {
			return recoveryTransient, errors.Join(absence, r.store.observeL1Error(ctx, err))
		}
		absence = errors.Join(absence, err)
	}
	outcome, storeErr := r.store.settleUnrecordedDispatch(ctx, item, lookupStarted)
	switch {
	case storeErr != nil:
		return recoveryDone, errors.Join(absence, storeErr)
	case outcome == settleChanged:
		return recoveryStale, nil
	case outcome == settleNotDue:
		return recoveryNotDue, nil
	}
	return recoveryDone, absence
}

// linkRecoveredDispatch preserves discoveries when cancellation took ownership
// during L1's read. The recovery write is fenced; the cancel-owned write links
// and durably wakes delivery, even if the remaining remote budget is spent.
func (r *Reconciler) linkRecoveredDispatch(ctx, remote context.Context, item unrecordedDispatch, jobID string, observed *dispatchObservation) error {
	if err := r.store.linkUnrecordedDispatch(ctx, item, jobID); err != nil {
		return err
	}
	if err := r.store.acceptRecoveredCancellation(ctx, item, jobID, observed); err != nil {
		return err
	}
	cancellations, err := r.store.pendingRunCancellations(ctx, item.RunID)
	if err != nil {
		return err
	}
	for _, cancellation := range cancellations {
		if remote.Err() != nil {
			break
		}
		if err := r.deliverRunCancellation(ctx, remote, cancellation); err != nil {
			return err
		}
	}
	return nil
}

// validateMigratedLinks uses acknowledged Job identity, including old Jobs
// without dispatch-key provenance. All network calls precede ledger writes.
func (r *Reconciler) validateMigratedLinks(ctx, remote context.Context) []error {
	if remote.Err() != nil {
		return nil
	}
	pending, err := r.store.migratedLinkValidations(remote)
	if err != nil {
		return []error{err}
	}
	var passErrors []error
	for _, item := range pending {
		if remote.Err() != nil {
			break
		}
		if !item.owned {
			if _, err := r.store.completeMigratedLinkValidation(ctx, item, migratedLinkRetired); err != nil {
				passErrors = append(passErrors, err)
			}
			continue
		}
		job, readErr := r.jobs.GetJob(remote, item.jobID)
		verdict := migratedLinkFound
		switch {
		case isMissingL1Job(readErr, item.jobID):
			verdict = migratedLinkAbsent
		case readErr != nil:
			verdict = migratedLinkRetry
		case job.JobID != item.jobID:
			verdict = migratedLinkRetry
			readErr = fmt.Errorf("L1 validation returned Job %q for %q", job.JobID, item.jobID)
		}
		completed, err := r.store.completeMigratedLinkValidation(ctx, item, verdict)
		if err != nil {
			passErrors = append(passErrors, err)
		}
		if readErr != nil && (verdict != migratedLinkAbsent || completed) {
			passErrors = append(passErrors, r.store.observeL1Error(ctx, readErr))
		}
	}
	return passErrors
}
