package agent

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
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
	if gate.observed != nil {
		gate.observed(observation)
	}
	return observation, gate.mu.RUnlock, nil
}

func (gate *ociIntentCompletionGate) allows(observation OCIIntentObservation) bool {
	return observation.Enabled && observation.Revision > gate.disabledRevision.Load()
}

func (gate *ociIntentCompletionGate) beginStop(revision uint64) func() {
	gate.disabledRevision.Store(revision)
	gate.mu.Lock()
	return gate.mu.Unlock
}
