package agent

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

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

// ociIntentCompletionGate orders same-process completion publication against
// the node-local stop controller. Readers hold the gate only across one final
// durable observation and one L1 response; retries release it between calls.
type ociIntentCompletionGate struct {
	mu      sync.RWMutex
	observe func(context.Context) (OCIIntentObservation, error)
	// disabledRevision is published immediately after the controller's durable
	// marker write, before it waits for pre-existing readers to drain.
	disabledRevision atomic.Uint64
	observed         func(OCIIntentObservation)
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

func (gate *ociIntentCompletionGate) beginStop(ctx context.Context, revision uint64) (func(), error) {
	gate.disabledRevision.Store(revision)
	acquired := make(chan struct{})
	canceled := make(chan struct{})
	go func() {
		// A queued writer prevents new readers from overtaking the drain. The
		// only existing readers span an operation-timeout-bounded L1 request.
		gate.mu.Lock()
		select {
		case acquired <- struct{}{}:
		case <-canceled:
			gate.mu.Unlock()
		}
	}()
	select {
	case <-acquired:
		return gate.mu.Unlock, nil
	case <-ctx.Done():
		close(canceled)
		return nil, ctx.Err()
	}
}

func requiresOCIIntentFence(kind, class string) bool {
	return class == contract.JobClassService && (kind == contract.JobKindOCI || kind == legacyUnclassifiedKind)
}
