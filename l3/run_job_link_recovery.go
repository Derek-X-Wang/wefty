package l3

import (
	"context"
	"errors"
	"fmt"
	"time"
)

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

// canceledRunJob resolves cancellation's private acknowledged-ID fallback and
// rechecks its stored identity before remote delivery. Reads of L1 happen before
// each short ledger write; the captured observation also fences terminal absence.
func (r *Reconciler) canceledRunJob(ctx, remote context.Context, cancellation runCancellation) (projectedRun, dispatchObservation, bool, error) {
	run := cancellation.projectedRun
	if run.JobID == "" {
		if r.lookup == nil {
			return run, dispatchObservation{}, false, internalError(errors.New("L1 dispatch lookup client is not configured"), "resolve canceled dispatch")
		}
		observed, err := readDispatchObservation(ctx, r.store.db, run.RunID)
		if err != nil {
			return run, dispatchObservation{}, false, err
		}
		lookupStarted := r.store.recoveryNow()
		job, err := r.lookup.LookupJobByDispatchKey(remote, cancellation.DispatchKey)
		if isMissingDispatch(err, cancellation.DispatchKey) {
			// A contradicted acknowledgement is not link evidence. Resolve its
			// identity through L1, outside the write, just as recovery does.
			if observed.outboxJob.Valid && observed.outboxJob.String != "" {
				job, err = r.jobs.GetJob(remote, observed.outboxJob.String)
				if isMissingL1Job(err, observed.outboxJob.String) {
					if err := r.store.cancelAbsentStoredIdentity(ctx, run.RunID, observed); err != nil {
						return run, dispatchObservation{}, false, err
					}
					return run, dispatchObservation{}, false, r.store.deferRunCancellation(ctx, run.RunID, nil)
				}
				if err != nil {
					return run, dispatchObservation{}, false, err
				}
				if job.JobID != observed.outboxJob.String {
					return run, dispatchObservation{}, false, internalError(errors.New("job lookup conflicts with stored identity"), "resolve canceled dispatch")
				}
			} else {
				if err := r.store.cancelAbsentDispatch(ctx, run.RunID, lookupStarted); err != nil {
					return run, dispatchObservation{}, false, err
				}
				return run, dispatchObservation{}, false, r.store.deferRunCancellation(ctx, run.RunID, err)
			}
		}
		if err != nil {
			return run, dispatchObservation{}, false, err
		}
		if err := r.store.completeDispatchWithEvidence(ctx, run.RunID, job.JobID, dispatchLookupEvidence, &observed); err != nil {
			return run, dispatchObservation{}, false, err
		}
		record, err := r.store.GetRun(ctx, run.RunID)
		if err != nil {
			return run, dispatchObservation{}, false, err
		}
		run.JobID, run.State = record.L1JobID, record.Status
	}

	// Recheck stored outbox acknowledgements before sending any cancellation.
	// A mismatched dispatch lookup must never become authority over another job.
	if err := r.store.completeDispatchWithEvidence(ctx, run.RunID, run.JobID, dispatchStoredEvidence, nil); err != nil {
		return run, dispatchObservation{}, false, err
	}
	record, err := r.store.GetRun(ctx, run.RunID)
	if err != nil {
		return run, dispatchObservation{}, false, err
	}
	if record.L1JobID != run.JobID || run.JobID == "" {
		return run, dispatchObservation{}, false, internalError(errors.New("canceled dispatch acknowledgement conflicts with run association"), "link canceled dispatch")
	}
	run.State = record.Status

	_, ok := r.jobs.(JobCancelClient)
	if !ok {
		return run, dispatchObservation{}, false, internalError(errors.New("L1 cancellation client is not configured"), "cancel run job")
	}
	observed, err := readDispatchObservation(ctx, r.store.db, run.RunID)
	if err != nil {
		return run, dispatchObservation{}, false, err
	}
	if !observed.runJob.Valid || observed.runJob.String != run.JobID ||
		!observed.outboxJob.Valid || observed.outboxJob.String != run.JobID {
		return run, dispatchObservation{}, false, internalError(errors.New("dispatch identity changed before cancellation"), "cancel run job")
	}
	run.State = observed.state
	return run, observed, true, nil
}
