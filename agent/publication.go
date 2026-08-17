package agent

import (
	"context"
	"fmt"
	"sync"
	"time"
)

const (
	DefaultPublicationRecoveryWindow = 10 * time.Second
	DefaultPublicationRetryInterval  = 100 * time.Millisecond
)

type publicationRequest func(context.Context, bool) error

// publicationController is the sole writer of publication state for one
// attempt. Readiness callbacks only update its latest local observation, so a
// slow request cannot permit a second request or reorder absolute true/false
// mutations.
type publicationController struct {
	clock          Clock
	recoveryWindow time.Duration
	retryInterval  time.Duration
	publish        publicationRequest
	setForwarding  func(bool)
	notify         chan struct{}

	mu           sync.Mutex
	ready        bool
	readySince   time.Time
	everReady    bool
	withdrawn    bool
	clearPending bool
	stopping     bool
	revision     uint64
}

func newPublicationController(
	clock Clock,
	recoveryWindow, retryInterval time.Duration,
	publish publicationRequest,
	setForwarding func(bool),
) *publicationController {
	if clock == nil {
		clock = systemClock{}
	}
	if recoveryWindow <= 0 {
		recoveryWindow = DefaultPublicationRecoveryWindow
	}
	if retryInterval <= 0 {
		retryInterval = DefaultPublicationRetryInterval
	}
	if publish == nil {
		publish = func(context.Context, bool) error { return nil }
	}
	return &publicationController{
		clock: clock, recoveryWindow: recoveryWindow, retryInterval: retryInterval,
		publish: publish, setForwarding: setForwarding, notify: make(chan struct{}, 1),
	}
}

// Observe records one guardian readiness transition. Withdrawal closes local
// forwarding before the controller can attempt the fenced clear.
func (controller *publicationController) Observe(ready bool) {
	controller.mu.Lock()
	if controller.stopping || controller.ready == ready {
		controller.mu.Unlock()
		return
	}
	controller.ready = ready
	controller.revision++
	if ready {
		controller.everReady = true
		controller.readySince = controller.clock.Now()
	} else {
		controller.readySince = time.Time{}
		controller.withdrawn = controller.everReady
		controller.clearPending = controller.everReady
		controller.forward(false)
	}
	controller.signalLocked()
	controller.mu.Unlock()
}

// Stop withdraws forwarding synchronously and asks Run to drain the final
// absolute false mutation before returning. It is safe to call more than once.
func (controller *publicationController) Stop() {
	controller.mu.Lock()
	if !controller.stopping {
		controller.stopping = true
		controller.ready = false
		controller.readySince = time.Time{}
		controller.revision++
		controller.forward(false)
		controller.signalLocked()
	}
	controller.mu.Unlock()
}

func (controller *publicationController) Run(ctx context.Context) error {
	var acknowledged *bool
	requestedTrue := false
	for {
		snapshot := controller.snapshot()
		if snapshot.clearPending && acknowledged != nil && !*acknowledged {
			controller.markClearAcknowledged()
			snapshot.clearPending = false
		}
		desired, wait, done := controller.next(snapshot, acknowledged, requestedTrue)
		if done {
			return nil
		}
		if wait > 0 {
			if !controller.wait(ctx, wait, snapshot.revision) {
				controller.forward(false)
				return nil
			}
			continue
		}
		if desired == nil {
			select {
			case <-ctx.Done():
				controller.forward(false)
				return nil
			case <-controller.notify:
			}
			continue
		}

		requestedRevision := snapshot.revision
		if *desired {
			requestedTrue = true
		}
		err := controller.publish(ctx, *desired)
		if err != nil {
			classification := classifyAgentProtocolError(err)
			if classification.destination != errorDestinationTransient {
				controller.forward(false)
				return &routedDestinationError{
					destination: classification.destination,
					err:         fmt.Errorf("agent: set attempt publication to %t: %w", *desired, err),
				}
			}
			if !controller.wait(ctx, controller.retryInterval, requestedRevision) {
				controller.forward(false)
				return nil
			}
			continue
		}

		value := *desired
		acknowledged = &value
		if !value {
			controller.markClearAcknowledged()
		}
		if value {
			controller.enableForwardingIfCurrent(requestedRevision)
		}
	}
}

type publicationSnapshot struct {
	ready        bool
	readySince   time.Time
	withdrawn    bool
	clearPending bool
	stopping     bool
	revision     uint64
}

func (controller *publicationController) snapshot() publicationSnapshot {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	return publicationSnapshot{
		ready: controller.ready, readySince: controller.readySince,
		withdrawn: controller.withdrawn, clearPending: controller.clearPending,
		stopping: controller.stopping,
		revision: controller.revision,
	}
}

func (controller *publicationController) next(
	snapshot publicationSnapshot,
	acknowledged *bool,
	requestedTrue bool,
) (desired *bool, wait time.Duration, done bool) {
	if snapshot.stopping {
		if !requestedTrue && (acknowledged == nil || !*acknowledged) {
			return nil, 0, true
		}
		if acknowledged != nil && !*acknowledged {
			return nil, 0, true
		}
		value := false
		return &value, 0, false
	}
	if snapshot.clearPending && requestedTrue {
		value := false
		return &value, 0, false
	}
	if !snapshot.ready {
		if !requestedTrue || (acknowledged != nil && !*acknowledged) {
			return nil, 0, false
		}
		value := false
		return &value, 0, false
	}
	if acknowledged != nil && *acknowledged {
		return nil, 0, false
	}
	if snapshot.withdrawn {
		remaining := controller.recoveryWindow - controller.clock.Now().Sub(snapshot.readySince)
		if remaining > 0 {
			return nil, remaining, false
		}
	}
	value := true
	return &value, 0, false
}

func (controller *publicationController) enableForwardingIfCurrent(revision uint64) {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	if !controller.stopping && controller.ready && controller.revision == revision {
		controller.forward(true)
	}
}

func (controller *publicationController) markClearAcknowledged() {
	controller.mu.Lock()
	controller.clearPending = false
	controller.mu.Unlock()
}

func (controller *publicationController) wait(ctx context.Context, duration time.Duration, revision uint64) bool {
	timer := controller.clock.NewTimer(duration)
	defer stopTimer(timer)
	for {
		select {
		case <-ctx.Done():
			return false
		case <-controller.notify:
			if controller.snapshot().revision != revision {
				return true
			}
		case <-timer.C():
			return true
		}
	}
}

func (controller *publicationController) signalLocked() {
	select {
	case controller.notify <- struct{}{}:
	default:
	}
}

func (controller *publicationController) forward(enabled bool) {
	if controller.setForwarding != nil {
		controller.setForwarding(enabled)
	}
}
