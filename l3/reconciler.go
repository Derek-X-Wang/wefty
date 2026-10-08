package l3

import (
	"context"
	"errors"
	"fmt"
	"time"
)

const DefaultReconcileInterval = time.Second

// DefaultDispatchRecoveryBudget bounds the wall time one pass spends on
// lookup recovery, so an unavailable L1 cannot hold the next pass's dispatch
// and projection behind it.
const DefaultDispatchRecoveryBudget = 5 * time.Second

type ReconcilerConfig struct {
	Interval       time.Duration
	OnError        func(error)
	ImageEvidence  JobImageEvidenceClient
	DispatchLookup JobDispatchLookupClient
	// DispatchRecoveryBudget bounds lookup recovery per pass; zero selects
	// DefaultDispatchRecoveryBudget.
	DispatchRecoveryBudget time.Duration
}

// Reconciler drains durable dispatch intents and projects L1 job states. It is
// safe to run duplicate passes: L1's dispatch key is stable and idempotent.
type Reconciler struct {
	store    *Store
	jobs     JobClient
	images   JobImageEvidenceClient
	lookup   JobDispatchLookupClient
	budget   time.Duration
	interval time.Duration
	onError  func(error)
}

func NewReconciler(store *Store, jobs JobClient, config ReconcilerConfig) (*Reconciler, error) {
	if store == nil {
		return nil, fmt.Errorf("l3: store is required")
	}
	if jobs == nil {
		return nil, fmt.Errorf("l3: L1 job client is required")
	}
	interval := config.Interval
	if interval <= 0 {
		interval = DefaultReconcileInterval
	}
	images := config.ImageEvidence
	if images == nil {
		images, _ = jobs.(JobImageEvidenceClient)
	}
	lookup := config.DispatchLookup
	if lookup == nil {
		lookup, _ = jobs.(JobDispatchLookupClient)
	}
	budget := config.DispatchRecoveryBudget
	if budget <= 0 {
		budget = DefaultDispatchRecoveryBudget
	}
	return &Reconciler{store: store, jobs: jobs, images: images, lookup: lookup, budget: budget, interval: interval, onError: config.OnError}, nil
}

// ReconcileOnce makes one complete pass over every outstanding dispatch and
// every active run. Individual remote failures do not prevent other records
// from making progress.
func (r *Reconciler) ReconcileOnce(ctx context.Context) error {
	var passErrors []error
	if err := r.probeDispatchHold(ctx); err != nil {
		passErrors = append(passErrors, err)
	}
	intents, err := r.store.pendingDispatches(ctx)
	if err != nil {
		return err
	}
	for _, intent := range intents {
		runToken, err := r.store.beginDispatch(ctx, intent.RunID)
		if errors.Is(err, errDispatchAbandoned) {
			// The run ended, was canceled, or dispatch was deferred after
			// this pass listed it. A job
			// created by an earlier attempt is linked by lookup recovery,
			// never by a new submit.
			continue
		}
		if err != nil {
			passErrors = append(passErrors, err)
			continue
		}
		job, err := r.jobs.SubmitJob(ctx, intent.jobSpec(runToken))
		if err != nil {
			record := r.store.recordDispatchError
			if !retryableDispatchError(err) {
				record = r.store.failDispatch
			}
			err = r.store.observeL1Error(ctx, err)
			if recordErr := record(ctx, intent.RunID, err); recordErr != nil {
				passErrors = append(passErrors, errors.Join(err, recordErr))
			} else {
				passErrors = append(passErrors, err)
			}
			continue
		}
		if err := r.store.completeDispatch(ctx, intent.RunID, job.JobID); err != nil {
			passErrors = append(passErrors, err)
		}
	}

	runs, err := r.store.activeProjectedRuns(ctx)
	if err != nil {
		passErrors = append(passErrors, err)
		return errors.Join(passErrors...)
	}
	for _, run := range runs {
		job, err := r.jobs.GetJob(ctx, run.JobID)
		if err != nil {
			if isMissingL1Job(err, run.JobID) {
				changed, storeErr := r.store.failMissingL1Job(ctx, run)
				if storeErr != nil {
					passErrors = append(passErrors, errors.Join(err, storeErr))
				} else if changed {
					passErrors = append(passErrors, err)
				}
				continue
			}
			passErrors = append(passErrors, r.store.observeL1Error(ctx, err))
			continue
		}
		if err := r.projectObservedJob(ctx, run, job); err != nil {
			passErrors = append(passErrors, r.store.observeL1Error(ctx, err))
		}
	}
	passErrors = append(passErrors, r.settlePendingNodeAttributions(ctx)...)
	// Recovery runs last and within its budget: it serves ended runs, and must
	// not delay dispatch, projection or node attribution of live ones.
	remote, cancel := context.WithTimeout(ctx, r.budget)
	defer cancel()
	cancellations, err := r.store.pendingRunCancellations(ctx, "")
	if err != nil {
		passErrors = append(passErrors, err)
	} else {
		for _, run := range cancellations {
			if remote.Err() != nil {
				break
			}
			if err := r.deliverRunCancellation(ctx, remote, run); err != nil {
				passErrors = append(passErrors, err)
			}
		}
	}
	passErrors = append(passErrors, r.recoverUnrecordedDispatchesRemote(ctx, remote)...)
	return errors.Join(passErrors...)
}

// recoverUnrecordedDispatches links terminal runs by read-only L1 reads only.
// Replaying SubmitJob is impossible after #52 cleared the staged bearer, and
// unsafe when L1 has regressed because it could recreate side effects for an
// ended run.
//
// Each pass reads a bounded batch of the rows that are due, inside its budget,
// and stops starting L1 reads once that budget is spent. A row whose L1 reads
// fail transiently, including one the budget cut short, backs off
// exponentially, so an unavailable L1 costs one budget per pass at most and
// later rows still get their turn.
func (r *Reconciler) recoverUnrecordedDispatches(ctx context.Context) []error {
	if r.lookup == nil {
		return nil
	}
	remote, cancel := context.WithTimeout(ctx, r.budget)
	defer cancel()
	return r.recoverUnrecordedDispatchesRemote(ctx, remote)
}

func (r *Reconciler) recoverUnrecordedDispatchesRemote(ctx, remote context.Context) []error {
	if r.lookup == nil || remote.Err() != nil {
		return nil
	}
	pending, err := r.store.unrecordedDispatches(remote, unrecordedDispatchBatch)
	if err != nil {
		return []error{err}
	}
	var passErrors []error
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
	// recoveryTransient is a remote failure: back the row off.
	recoveryTransient
	// recoveryStale is a settlement compare-and-set miss: reread the row.
	recoveryStale
	// recoveryNotDue is an unacknowledged dispatch's absence inside the
	// settle horizon: not an error yet, but asked again only after the
	// backoff, the same as a transient failure.
	recoveryNotDue
)

// maxRecoveryRereads bounds how often one row is reread after its settlement
// compare-and-set missed. Each miss means an acknowledgement landed; a row
// still contended after that is left for the next pass.
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

// resolveUnrecordedDispatch links or settles one row from what L1 answers.
// It reports stale when the settlement compare-and-set found the row changed
// since it was read, so the caller rereads it rather than settle over an
// acknowledgement that arrived meanwhile, and not due when an unacknowledged
// dispatch's absence is still inside the settle horizon.
func (r *Reconciler) resolveUnrecordedDispatch(ctx, remote context.Context, item unrecordedDispatch) (recoveryOutcome, error) {
	lookupStarted := r.store.recoveryNow()
	job, err := r.lookup.LookupJobByDispatchKey(remote, item.DispatchKey)
	var absence error
	switch {
	case err == nil && (item.OutboxJobID == "" || job.JobID == item.OutboxJobID):
		return recoveryDone, r.store.linkUnrecordedDispatch(ctx, item, job.JobID)
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
			return recoveryDone, r.store.linkUnrecordedDispatch(ctx, item, item.OutboxJobID)
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

// settlePendingNodeAttributions names the node of runs the ledger failed while
// their L1 job was live. They are terminal, so the projection loop above no
// longer visits them; this waits for the job to settle and fills the node from
// the attempt that settled it. A job L1 no longer has ends the wait unnamed.
func (r *Reconciler) settlePendingNodeAttributions(ctx context.Context) []error {
	pending, err := r.store.pendingNodeAttributions(ctx)
	if err != nil {
		return []error{err}
	}
	var passErrors []error
	for _, item := range pending {
		job, err := r.jobs.GetJob(ctx, item.JobID)
		if err != nil {
			if isMissingL1Job(err, item.JobID) {
				if err := r.store.settleNodeAttribution(ctx, item.RunID, ""); err != nil {
					passErrors = append(passErrors, err)
				}
				continue
			}
			passErrors = append(passErrors, r.store.observeL1Error(ctx, err))
			continue
		}
		nodeID, settled := jobNodeID(job)
		if !settled {
			continue
		}
		if err := r.store.settleNodeAttribution(ctx, item.RunID, nodeID); err != nil {
			passErrors = append(passErrors, err)
		}
	}
	return passErrors
}

func retryableDispatchError(err error) bool {
	a := classifyL1Answer(err)
	return a.Kind != l1WorkRefused || a.Method != "POST" || a.Target != "/v1/jobs"
}

// Run reconciles immediately and then on a fixed cadence until cancellation.
// Transient pass failures are reported and retried rather than terminating the
// crash-recovery loop.
func (r *Reconciler) Run(ctx context.Context) error {
	r.reconcileAndReport(ctx)
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			r.reconcileAndReport(ctx)
		}
	}
}

func (r *Reconciler) reconcileAndReport(ctx context.Context) {
	if err := r.ReconcileOnce(ctx); err != nil && r.onError != nil && ctx.Err() == nil {
		r.onError(err)
	}
}
