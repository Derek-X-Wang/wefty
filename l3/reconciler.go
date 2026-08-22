package l3

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

const DefaultReconcileInterval = time.Second

type ReconcilerConfig struct {
	Interval      time.Duration
	OnError       func(error)
	ImageEvidence JobImageEvidenceClient
}

// Reconciler drains durable dispatch intents and projects L1 job states. It is
// safe to run duplicate passes: L1's dispatch key is stable and idempotent.
type Reconciler struct {
	store    *Store
	jobs     JobClient
	images   JobImageEvidenceClient
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
	return &Reconciler{store: store, jobs: jobs, images: images, interval: interval, onError: config.OnError}, nil
}

// ReconcileOnce makes one complete pass over every outstanding dispatch and
// every active run. Individual remote failures do not prevent other records
// from making progress.
func (r *Reconciler) ReconcileOnce(ctx context.Context) error {
	var passErrors []error
	intents, err := r.store.pendingDispatches(ctx)
	if err != nil {
		return err
	}
	for _, intent := range intents {
		runToken, err := r.store.ensureRunToken(ctx, intent.RunID)
		if err != nil {
			passErrors = append(passErrors, err)
			continue
		}
		if err := r.store.beginDispatch(ctx, intent.RunID); err != nil {
			passErrors = append(passErrors, err)
			continue
		}
		job, err := r.jobs.SubmitJob(ctx, intent.jobSpec(runToken))
		if err != nil {
			record := r.store.recordDispatchError
			if !retryableDispatchError(err) {
				record = r.store.failDispatch
			}
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
			passErrors = append(passErrors, err)
			continue
		}
		if r.images != nil && (job.State == contract.JobSucceeded || job.State == contract.JobFailed) {
			evidence, err := r.images.GetJobImageEvidence(ctx, run.JobID)
			if err != nil {
				passErrors = append(passErrors, err)
				continue
			}
			ingestionFailed := false
			for _, observation := range evidence {
				recorded, err := r.store.recordRunImageResolution(ctx, run.RunID, observation)
				if err != nil {
					passErrors = append(passErrors, err)
					ingestionFailed = true
					break
				}
				if recorded {
					break
				}
			}
			if ingestionFailed {
				continue
			}
		}
		if err := r.store.recordRunNode(ctx, run.RunID, job.NodeID); err != nil {
			passErrors = append(passErrors, err)
			continue
		}
		if err := r.store.projectJobState(ctx, run, job.State); err != nil {
			passErrors = append(passErrors, err)
		}
	}
	return errors.Join(passErrors...)
}

func retryableDispatchError(err error) bool {
	var dispatchErr *Error
	if errors.As(err, &dispatchErr) {
		return dispatchErr.Retryable
	}
	// Unknown failures include injected crashes and transport wrappers. They
	// are ambiguous, so keep the stable dispatch key eligible for replay.
	return true
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
