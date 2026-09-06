package agent

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

// OCIIntentObservation is the durable intent state that fenced completion
// publication observed. Revision is retained with the local receipt.
type OCIIntentObservation struct {
	Enabled  bool
	Revision uint64
}

// OCIIntentAuthorityUnavailableError means completion evidence was retained
// because the durable intent authority could not be read conclusively.
type OCIIntentAuthorityUnavailableError struct{ Err error }

func (err *OCIIntentAuthorityUnavailableError) Error() string {
	if err.Err == nil {
		return "agent: OCI intent authority is unavailable"
	}
	return fmt.Sprintf("agent: OCI intent authority is unavailable: %v", err.Err)
}

func (err *OCIIntentAuthorityUnavailableError) Unwrap() error { return err.Err }

// OCIIntentAuthorityRequiredError means an agent configuration offered OCI
// execution without the durable node-local control surface that fences it.
type OCIIntentAuthorityRequiredError struct{}

func (*OCIIntentAuthorityRequiredError) Error() string {
	return "agent: OCI runtime or capability requires an OCI intent authority"
}

// OCIIntentSuppressionPersistenceError means a disabled completion could not
// be durably classified before the bounded storage window closed. The stop
// barrier reports this error instead of claiming runtime quiescence.
type OCIIntentSuppressionPersistenceError struct {
	AttemptID      string
	IntentRevision uint64
	Err            error
}

func (err *OCIIntentSuppressionPersistenceError) Error() string {
	return fmt.Sprintf("agent: persist OCI intent suppression for attempt %q at revision %d: %v", err.AttemptID, err.IntentRevision, err.Err)
}

func (err *OCIIntentSuppressionPersistenceError) Unwrap() error { return err.Err }

// ociIntentCompletionGate orders same-process completion publication against
// the node-local stop controller. Readers hold the gate across one final
// durable observation and either one L1 response or one bounded suppression
// transaction; retries release it between calls. The suppression path's lock
// order is gate read side, then the spool's sole database connection. Its own
// deadline keeps that ordering within the operator-control drain budget.
type ociIntentCompletionGate struct {
	mu                 sync.RWMutex
	observe            func(context.Context) (OCIIntentObservation, error)
	suppressionTimeout time.Duration
	// disabledRevision is published immediately after the controller's durable
	// marker write, before it waits for pre-existing readers to drain.
	disabledRevision atomic.Uint64
	observed         func(OCIIntentObservation)
	failureMu        sync.Mutex
	failures         map[string]*OCIIntentSuppressionPersistenceError
}

func (gate *ociIntentCompletionGate) beginCompletion(ctx context.Context) (OCIIntentObservation, func(), error) {
	gate.mu.RLock()
	if gate.observe == nil {
		gate.mu.RUnlock()
		return OCIIntentObservation{}, nil, &OCIIntentAuthorityUnavailableError{}
	}
	observation, err := gate.observe(ctx)
	if err != nil {
		gate.mu.RUnlock()
		return OCIIntentObservation{}, nil, &OCIIntentAuthorityUnavailableError{Err: err}
	}
	if observation.Revision == 0 {
		gate.mu.RUnlock()
		return OCIIntentObservation{}, nil, &OCIIntentAuthorityUnavailableError{
			Err: errors.New("durable OCI intent marker is absent"),
		}
	}
	if gate.observed != nil {
		gate.observed(observation)
	}
	return observation, gate.mu.RUnlock, nil
}

func (gate *ociIntentCompletionGate) allows(observation OCIIntentObservation) bool {
	return observation.Enabled && observation.Revision > gate.disabledRevision.Load()
}

func (gate *ociIntentCompletionGate) beginSuppression(ctx context.Context) (context.Context, context.CancelFunc) {
	timeout := gate.suppressionTimeout
	if timeout <= 0 {
		timeout = DefaultOperationTimeout
	}
	return context.WithTimeout(context.WithoutCancel(ctx), timeout)
}

func (gate *ociIntentCompletionGate) finishSuppression(attemptID string, observation OCIIntentObservation, persistenceErr error) error {
	gate.failureMu.Lock()
	defer gate.failureMu.Unlock()
	if persistenceErr == nil {
		delete(gate.failures, attemptID)
		return nil
	}
	typed := &OCIIntentSuppressionPersistenceError{
		AttemptID: attemptID, IntentRevision: observation.Revision, Err: persistenceErr,
	}
	if gate.failures == nil {
		gate.failures = make(map[string]*OCIIntentSuppressionPersistenceError)
	}
	gate.failures[attemptID] = typed
	return typed
}

func (gate *ociIntentCompletionGate) suppressionFailure(revision uint64) error {
	gate.failureMu.Lock()
	defer gate.failureMu.Unlock()
	var failures []error
	for _, failure := range gate.failures {
		if failure.IntentRevision <= revision {
			failures = append(failures, failure)
		}
	}
	return errors.Join(failures...)
}

func (gate *ociIntentCompletionGate) beginStop(ctx context.Context, revision uint64) (func(), error) {
	gate.disabledRevision.Store(revision)
	acquired := make(chan struct{})
	canceled := make(chan struct{})
	go func() {
		// A queued writer prevents new readers from overtaking the drain. The
		// only existing readers span a bounded L1 request or suppression write.
		gate.mu.Lock()
		select {
		case acquired <- struct{}{}:
		case <-canceled:
			gate.mu.Unlock()
		}
	}()
	select {
	case <-acquired:
		if err := gate.suppressionFailure(revision); err != nil {
			gate.mu.Unlock()
			return nil, err
		}
		return gate.mu.Unlock, nil
	case <-ctx.Done():
		close(canceled)
		return nil, ctx.Err()
	}
}

func requiresOCIIntentFence(kind, class string) bool {
	return class == contract.JobClassService && (kind == contract.JobKindOCI || kind == legacyUnclassifiedKind)
}
