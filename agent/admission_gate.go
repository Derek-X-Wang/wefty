package agent

import (
	"context"
	"sync"
)

// errorDestination is the scope that decides whether a failed operation is
// absorbed by its owner or propagated to the process-lifetime caller.
// Protocol operations receive a code-keyed destination; local failures and
// successful attempts remain unclassified. The session still preserves the
// existing propagation policy until the resilience cutover wires reactions.
type errorDestination uint8

const (
	errorDestinationUnclassified errorDestination = iota
	errorDestinationTransient
	errorDestinationAttemptAuthority
	errorDestinationNodeSession
)

// attemptExecution returns both the failure destination and the failure. A
// nil error makes the destination irrelevant.
type attemptExecution func(context.Context) (errorDestination, error)

// destinationErrorPolicy returns the error to propagate, or nil to absorb it
// at its destination. Keeping this decision outside the gate lets the session
// own error routing without coupling admission to protocol classification.
type destinationErrorPolicy func(errorDestination, error) error

// classAdmissionGate is the session-facing admission seam. Its interface
// deliberately carries both the failure destination and the routing policy:
// the classifier names the destination, while the session decides whether
// that destination propagates or absorbs the error.
type classAdmissionGate interface {
	execute(context.Context, attemptExecution, destinationErrorPolicy) (bool, error)
}

type admissionPolicy struct {
	limit int
}

// admissionGate owns only a policy and its occupancy count. In particular it
// never retains a claim, runner, output sink, or attempt lifecycle.
type admissionGate struct {
	mu       sync.Mutex
	policy   admissionPolicy
	occupied int
}

func newAdmissionGate(limit int) *admissionGate {
	return &admissionGate{policy: admissionPolicy{limit: limit}}
}

// execute admits work when capacity is available. The callback remains owned
// by the caller and is never retained by the gate. The returned bool reports
// whether work was admitted; a routed error is returned separately.
func (gate *admissionGate) execute(ctx context.Context, work attemptExecution, route destinationErrorPolicy) (bool, error) {
	if !gate.tryAcquire() {
		return false, nil
	}
	defer gate.release()

	destination, err := work(ctx)
	if err == nil {
		return true, nil
	}
	return true, route(destination, err)
}

func (gate *admissionGate) tryAcquire() bool {
	gate.mu.Lock()
	defer gate.mu.Unlock()
	if gate.policy.limit <= 0 || gate.occupied >= gate.policy.limit {
		return false
	}
	gate.occupied++
	return true
}

func (gate *admissionGate) release() {
	gate.mu.Lock()
	gate.occupied--
	gate.mu.Unlock()
}
