package lima

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

const DefaultHostVMIdleWait = 10 * time.Minute
const DefaultHostVMIdleStopTimeout = 4 * time.Minute
const DefaultHostVMMinimumFreeBytes uint64 = 5 << 30

// DemandItem explains node-owned work that keeps the host VM running.
// It is an observation, never a field of the owner's durable intent.
type DemandItem struct {
	Kind string `json:"kind"`
	ID   string `json:"id,omitempty"`
}

type DemandSource interface {
	HostVMDemand(context.Context) ([]DemandItem, error)
}
type DemandSourceFunc func(context.Context) ([]DemandItem, error)

func (source DemandSourceFunc) HostVMDemand(ctx context.Context) ([]DemandItem, error) {
	return source(ctx)
}

func (supervisor *Supervisor) demand(ctx context.Context) ([]DemandItem, error) {
	if supervisor.config.Demand == nil {
		return nil, nil
	}
	return supervisor.config.Demand.HostVMDemand(ctx)
}

// IdleDue observes demand using the injected clock. An unreadable demand
// source keeps the host VM on; absence must be known before stopping it.
func (supervisor *Supervisor) IdleDue(ctx context.Context) bool {
	intent, err := supervisor.readIntent(ctx)
	items, demandErr := supervisor.demand(ctx)
	supervisor.mu.Lock()
	defer supervisor.mu.Unlock()
	supervisor.facts.Position = intent.PositionValue()
	supervisor.facts.Demand = append([]DemandItem(nil), items...)
	if err != nil || demandErr != nil || intent.PositionValue() != OCIPositionOnDemand || len(items) > 0 {
		supervisor.idleSince = time.Time{}
		return false
	}
	now := supervisor.config.now()
	if supervisor.idleSince.IsZero() {
		supervisor.idleSince = now
	}
	return now.Sub(supervisor.idleSince) >= supervisor.config.IdleWait
}

func (supervisor *Supervisor) idleStopped(ctx context.Context, intent OCIIntent, state InstanceState) bool {
	if intent.PositionValue() != OCIPositionOnDemand || state != InstanceStopped {
		return false
	}
	items, err := supervisor.demand(ctx)
	if err != nil || len(items) > 0 {
		return false
	}
	supervisor.record(state, true, false, contract.CapabilityReasonHostVMIdle, false)
	return true
}

// StopIdle is called only after capability withdrawal is acknowledged and the
// node's claim lock has rechecked demand. It rechecks at the Lima cycle too.
func (supervisor *Supervisor) StopIdle(ctx context.Context) error {
	supervisor.ensureMu.Lock()
	defer supervisor.ensureMu.Unlock()
	intent, err := supervisor.readIntent(ctx)
	if err != nil {
		return err
	}
	items, err := supervisor.demand(ctx)
	if err != nil {
		return err
	}
	if intent.PositionValue() != OCIPositionOnDemand || len(items) > 0 {
		return errIdleStopCanceled
	}
	return supervisor.gracefulIdleStop(ctx)
}

// IdleStop serializes the gentle stop with every other Lima/helper cycle.
// check runs after the cycle lock and before shutdown begins. A false result
// cancels shutdown; the caller restores capability after releasing claimMu.
func (barrier *SupervisedBootBarrier) IdleStop(ctx context.Context, check func() bool) (bool, error) {
	if err := lockSupervisorCycle(ctx, &barrier.cycleMu); err != nil {
		return false, err
	}
	defer barrier.cycleMu.Unlock()
	if !check() {
		return false, nil
	}
	// Detach deliberately before shutdown so the helper EOF cannot publish
	// helper_unreachable over the acknowledged host_vm_idle observation.
	barrier.Barrier.Invalidate()
	// StopIdle reads the cached node snapshot, never the claim lock.
	if err := barrier.Supervisor.StopIdle(ctx); err != nil {
		if errors.Is(err, errIdleStopCanceled) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

var errIdleStopCanceled = errors.New("host VM idle stop canceled by demand or owner switch")

func (supervisor *Supervisor) gracefulIdleStop(ctx context.Context) error {
	stopContext, cancel := supervisor.config.withTimeout(ctx, supervisor.config.IdleStopTimeout)
	_, err := supervisor.config.run(stopContext, supervisor.config.Limactl, "stop", supervisor.config.Instance)
	timedOut := errors.Is(err, context.DeadlineExceeded) || errors.Is(stopContext.Err(), context.DeadlineExceeded)
	cancel()
	if err != nil {
		if !timedOut || ctx.Err() != nil {
			return err
		}
		if err = supervisor.forceStop(ctx); err != nil {
			return err
		}
	}
	supervisor.record(InstanceStopped, true, false, contract.CapabilityReasonHostVMIdle, false)
	return nil
}

// DemandState is a node-owned snapshot. Reading it never takes the claim lock,
// so Lima recovery cannot invert the claim -> cycle lock order of idle stop.
type DemandState struct {
	mu    sync.RWMutex
	items []DemandItem
	err   error
}

func (state *DemandState) Replace(items []DemandItem, err error) {
	state.mu.Lock()
	defer state.mu.Unlock()
	state.items = append([]DemandItem(nil), items...)
	state.err = err
}
func (state *DemandState) HostVMDemand(context.Context) ([]DemandItem, error) {
	state.mu.RLock()
	defer state.mu.RUnlock()
	return append([]DemandItem(nil), state.items...), state.err
}

func (barrier *SupervisedBootBarrier) IdleDue(ctx context.Context) bool {
	return barrier.Supervisor.IdleDue(ctx)
}
func (barrier *SupervisedBootBarrier) IdleReason() contract.CapabilityReasonCode {
	return barrier.Supervisor.Facts().ReasonCode
}
