package agent

import (
	"context"
	"errors"
	"sort"
	"sync"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/runner/lima"
)

// QueuedOCIDemand is the #1144 claim-poll hint seam. Until it is supplied,
// the node uses NoQueuedOCIDemand; the operator socket cannot supply a hint.
type QueuedOCIDemand interface {
	QueuedOCIWork(context.Context) ([]lima.DemandItem, error)
}
type NoQueuedOCIDemand struct{}

func (NoQueuedOCIDemand) QueuedOCIWork(context.Context) ([]lima.DemandItem, error) { return nil, nil }

type hostVMState struct {
	mu              sync.Mutex
	directives      []lima.DemandItem
	loads           int
	directivePasses int
	snapshot        *lima.DemandState
	queued          QueuedOCIDemand
}

func (session *agentSession) observeHostVMDirectives(response l1.HeartbeatResponse) {
	session.claimMu.Lock()
	defer session.claimMu.Unlock()
	var items []lima.DemandItem
	for _, d := range response.RemovalDirectives {
		items = append(items, lima.DemandItem{Kind: "removal", ID: d.JobID})
	}
	for _, d := range response.BackupDirectives {
		items = append(items, lima.DemandItem{Kind: "backup", ID: d.BackupID})
	}
	for _, d := range response.BackupPruneDirectives {
		items = append(items, lima.DemandItem{Kind: "prune", ID: d.BackupID})
	}
	for _, d := range response.StorageResetDirectives {
		items = append(items, lima.DemandItem{Kind: "reset", ID: d.ComputerID})
	}
	for _, d := range response.StorageGrowDirectives {
		items = append(items, lima.DemandItem{Kind: "grow", ID: d.ComputerID})
	}
	for _, d := range response.StorageCopyDirectives {
		items = append(items, lima.DemandItem{Kind: d.Operation, ID: d.DestinationComputerID})
	}
	for _, d := range response.ReimageDirectives {
		items = append(items, lima.DemandItem{Kind: "reimage", ID: d.ComputerID})
	}
	session.hostVM.mu.Lock()
	session.hostVM.directives = items
	session.hostVM.mu.Unlock()
}

// OCIWork lists the OCI work ended by the off command, after admission closes.
func (a *Agent) OCIWork() []lima.DemandItem {
	if a == nil || a.session == nil {
		return nil
	}
	a.session.claimMu.Lock()
	defer a.session.claimMu.Unlock()
	return a.ociWorkLocked()
}
func (a *Agent) ociWorkLocked() []lima.DemandItem {
	var items []lima.DemandItem
	for id, kind := range a.session.residentKind {
		if kind == contract.JobKindOCI {
			items = append(items, lima.DemandItem{Kind: "oci", ID: id})
		}
	}
	sort.Slice(items, func(i, j int) bool { return items[i].ID < items[j].ID })
	return items
}

func (a *Agent) HostVMDemand(ctx context.Context) ([]lima.DemandItem, error) {
	if a == nil || a.session == nil {
		return nil, errors.New("node demand is unavailable")
	}
	a.session.claimMu.Lock()
	defer a.session.claimMu.Unlock()
	return a.hostVMDemandLocked(ctx)
}
func (a *Agent) hostVMDemandLocked(ctx context.Context) ([]lima.DemandItem, error) {
	items := a.ociWorkLocked()
	state := &a.session.hostVM
	state.mu.Lock()
	items = append(items, state.directives...)
	if state.directivePasses > 0 {
		items = append(items, lima.DemandItem{Kind: "helper-directive"})
	}
	if state.loads > 0 {
		items = append(items, lima.DemandItem{Kind: "load-image"})
	}
	queued := state.queued
	state.mu.Unlock()
	if queued == nil {
		queued = NoQueuedOCIDemand{}
	}
	waiting, err := queued.QueuedOCIWork(ctx)
	if err != nil {
		return nil, err
	}
	items = append(items, waiting...)
	// Durable removals count after restart, before another L1 directive arrives.
	if a.logSpool != nil {
		pending, err := a.logSpool.pendingRuntimeRemovals(ctx)
		if err != nil {
			return nil, err
		}
		for _, row := range pending {
			items = append(items, lima.DemandItem{Kind: "removal", ID: row.manifest.JobID})
		}
	}
	// A heartbeat can stop listing an acknowledged directive while its helper
	// call is still unwinding. In-flight cleanup remains demand until it joins.
	if c := a.session.backups; c != nil {
		c.mu.Lock()
		if len(c.inflight) > 0 {
			items = append(items, lima.DemandItem{Kind: "backup"})
		}
		c.mu.Unlock()
	}
	if c := a.session.storageCopies; c != nil {
		c.mu.Lock()
		if len(c.inflight) > 0 {
			items = append(items, lima.DemandItem{Kind: "storage-copy"})
		}
		c.mu.Unlock()
	}
	if c := a.session.storageResets; c != nil {
		c.mu.Lock()
		if len(c.inflight) > 0 {
			items = append(items, lima.DemandItem{Kind: "reset"})
		}
		c.mu.Unlock()
	}
	if c := a.session.storageGrows; c != nil {
		c.mu.Lock()
		if len(c.inflight) > 0 {
			items = append(items, lima.DemandItem{Kind: "grow"})
		}
		c.mu.Unlock()
	}
	if c := a.session.removals; c != nil {
		c.mu.Lock()
		if len(c.inflight) > 0 {
			items = append(items, lima.DemandItem{Kind: "removal"})
		}
		c.mu.Unlock()
	}
	if c := a.session.reimagePreflights; c != nil {
		c.mu.Lock()
		if len(c.inflight) > 0 {
			items = append(items, lima.DemandItem{Kind: "reimage"})
		}
		c.mu.Unlock()
	}
	if c := a.session.custody; c != nil {
		c.mu.Lock()
		if len(c.inflight) > 0 {
			items = append(items, lima.DemandItem{Kind: "custody-export"})
		}
		c.mu.Unlock()
	}
	return items, nil
}

func (session *agentSession) beginHostVMDirectivePass(response l1.HeartbeatResponse) func() {
	session.claimMu.Lock()
	session.hostVM.mu.Lock()
	session.hostVM.directivePasses++
	session.hostVM.mu.Unlock()
	session.claimMu.Unlock()
	session.observeHostVMDirectives(response)
	return func() {
		session.claimMu.Lock()
		defer session.claimMu.Unlock()
		session.hostVM.mu.Lock()
		session.hostVM.directivePasses--
		session.hostVM.mu.Unlock()
	}
}

// BeginHostVMLoadImage is an internal node hook, not an operator-supplied
// demand verb. It spans the import and its cleanup even when import fails.
func (a *Agent) BeginHostVMLoadImage() func() {
	a.session.claimMu.Lock()
	a.session.hostVM.mu.Lock()
	a.session.hostVM.loads++
	a.session.hostVM.mu.Unlock()
	a.refreshHostVMDemandLocked(context.Background())
	a.session.claimMu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			a.session.claimMu.Lock()
			defer a.session.claimMu.Unlock()
			a.session.hostVM.mu.Lock()
			a.session.hostVM.loads--
			a.session.hostVM.mu.Unlock()
			a.refreshHostVMDemandLocked(context.Background())
		})
	}
}

func (a *Agent) ConfigureHostVMDemand(snapshot *lima.DemandState, queued QueuedOCIDemand) {
	a.session.hostVM.mu.Lock()
	defer a.session.hostVM.mu.Unlock()
	a.session.hostVM.snapshot = snapshot
	a.session.hostVM.queued = queued
}
func (a *Agent) refreshHostVMDemandLocked(ctx context.Context) {
	items, err := a.hostVMDemandLocked(ctx)
	a.session.hostVM.mu.Lock()
	snapshot := a.session.hostVM.snapshot
	a.session.hostVM.mu.Unlock()
	if snapshot != nil {
		snapshot.Replace(items, err)
	}
}

// PollHostVM implements the idle withdrawal transaction. L1 acknowledgement
// precedes claimMu, which precedes the Lima cycle lock and graceful stop.
type HostVMIdleCycle interface {
	IdleDue(context.Context) bool
	IdleReason() contract.CapabilityReasonCode
	IdleStop(context.Context, func() bool) (bool, error)
}

func (a *Agent) PollHostVM(ctx context.Context, barrier HostVMIdleCycle) error {
	a.session.claimMu.Lock()
	a.refreshHostVMDemandLocked(ctx)
	a.session.claimMu.Unlock()
	if !barrier.IdleDue(ctx) {
		return nil
	}
	if barrier.IdleReason() == contract.CapabilityReasonHostVMIdle {
		return nil
	}
	if err := lockMutexContext(ctx, &a.session.ociRecoveryMu); err != nil {
		return err
	}
	recoveryLocked := true
	defer func() {
		if recoveryLocked {
			a.session.ociRecoveryMu.Unlock()
		}
	}()
	a.capabilities.suppressOCI(contract.CapabilityReasonHostVMIdle, errors.New("host VM idle wait elapsed"))
	response, err := a.session.publishCapabilityHeartbeatResponse(ctx, nil)
	if err != nil {
		return err
	}
	// Preserve cleanup first seen in the acknowledgement before rechecking.
	a.session.observeHostVMDirectives(response)
	if a.CapabilitySnapshot().PendingPublicationRevision != 0 {
		return errors.New("host VM idle capability withdrawal is not acknowledged")
	}
	a.session.claimMu.Lock()
	var demandErr error
	stopped, err := barrier.IdleStop(ctx, func() bool {
		items, e := a.hostVMDemandLocked(ctx)
		demandErr = e
		a.session.hostVM.mu.Lock()
		snapshot := a.session.hostVM.snapshot
		a.session.hostVM.mu.Unlock()
		if snapshot != nil {
			snapshot.Replace(items, e)
		}
		return e == nil && len(items) == 0
	})
	a.session.claimMu.Unlock()
	a.session.ociRecoveryMu.Unlock()
	recoveryLocked = false
	if !stopped {
		// Restore the earned capability after releasing both locks. A changed
		// switch is revalidated by ordinary recovery.
		restoreErr := a.RecoverOCIRuntimeCapabilities(ctx)
		return errors.Join(err, demandErr, restoreErr)
	}
	return err
}
