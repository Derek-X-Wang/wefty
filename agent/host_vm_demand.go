package agent

import (
	"context"
	"errors"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/runner/lima"
)

// QueuedOCIDemand reads cached claim-poll facts, never operator input.
type QueuedOCIDemand interface {
	QueuedOCIWork(context.Context) ([]lima.DemandItem, error)
}
type hostVMState struct {
	stopping        atomic.Bool
	idleLogged      atomic.Bool
	mu              sync.Mutex
	directives      []lima.DemandItem
	loads           int
	directivePasses int
	snapshot        *lima.DemandState
	queued          QueuedOCIDemand
	claimPoll       claimPollDemand
	refresh         func(context.Context)
}

type claimPollDemand struct{ waiting bool }

func (d claimPollDemand) QueuedOCIWork(context.Context) ([]lima.DemandItem, error) {
	if d.waiting {
		return []lima.DemandItem{{Kind: "queued-oci"}}, nil
	}
	return nil, nil
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
	if queued == nil {
		queued = state.claimPoll
	}
	state.mu.Unlock()
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
	if a.session.client != nil {
		// Configuration precedes serving claims; literal test clients may have
		// no negotiation state until host VM demand is configured.
		if a.session.client.claimHints == nil {
			a.session.client.claimHints = &claimHintState{}
		}
		a.session.client.claimHints.enabled.Store(a.session.registration.OS == "darwin" && snapshot != nil)
	}
	a.session.hostVM.mu.Lock()
	defer a.session.hostVM.mu.Unlock()
	a.session.hostVM.snapshot = snapshot
	a.session.hostVM.queued = queued
	a.session.hostVM.refresh = a.refreshHostVMDemandLocked
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
	MarkStopping(context.Context) error
	FinishStopping() error
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
	claimsLocked := true
	marked := false
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
		if e != nil || len(items) != 0 {
			return false
		}
		if e = barrier.MarkStopping(ctx); e != nil {
			demandErr = e
			return false
		}
		marked = true
		a.session.hostVM.stopping.Store(true)
		// The durable mark closes OCI recovery. Only the Lima cycle lock spans
		// the command; process claims, completion and doctor stay available.
		a.session.claimMu.Unlock()
		claimsLocked = false
		a.session.ociRecoveryMu.Unlock()
		recoveryLocked = false
		return true
	})
	if claimsLocked {
		a.session.claimMu.Unlock()
	}
	if recoveryLocked {
		a.session.ociRecoveryMu.Unlock()
		recoveryLocked = false
	}
	if marked {
		err = errors.Join(err, barrier.FinishStopping())
		a.session.hostVM.stopping.Store(false)
	}
	if !stopped {
		// Restore the earned capability after releasing both locks. A changed
		// switch is revalidated by ordinary recovery.
		restoreErr := a.restoreHostVMCapability(ctx)
		return errors.Join(err, demandErr, restoreErr)
	}
	return err
}

// Cancellation keeps an earned helper session. Only a lost generation needs
// takeover and a namespace sweep; live work must survive a canceled stop.
func (a *Agent) restoreHostVMCapability(ctx context.Context) error {
	session := a.session
	if err := lockMutexContext(ctx, &session.ociRecoveryMu); err != nil {
		return err
	}
	defer session.ociRecoveryMu.Unlock()
	if session.ociIntent != nil {
		observation, err := session.ociIntent(ctx)
		if err != nil {
			return err
		}
		if !observation.Enabled {
			session.capabilities.suppressOCIIntent(errOCIIntentDisabled)
			_, err = session.publishCapabilityHeartbeat(ctx, nil)
			return err
		}
	}
	if session.ociBootBarrier != nil {
		if _, ready := session.ociBootBarrier.Generation(); ready {
			_, err := session.publishRegistrationCapabilityPinnedLocked(ctx)
			return err
		}
	}
	generation, err := session.recoverOCIRuntimeLocked(ctx)
	if err != nil {
		return err
	}
	if session.ociBootBarrier == nil {
		return nil
	}
	_, err = session.publishCapabilityHeartbeat(ctx, &generation)
	return err
}

func (session *agentSession) skipHostVMRecovery(ctx context.Context) error {
	if session.hostVM.stopping.Load() {
		return &CapabilityProbeSkippedError{ReasonCode: contract.CapabilityReasonHostVMIdle}
	}
	if session.capabilities.ociIntentDisabled.Load() {
		return nil
	}
	skipper, ok := session.ociBootBarrier.(interface{ OCIRecoverySkip(context.Context) error })
	if !ok {
		return nil
	}
	err := skipper.OCIRecoverySkip(ctx)
	if err == nil {
		session.hostVM.idleLogged.Store(false)
		return nil
	}
	var reason interface {
		CapabilityReasonCode() contract.CapabilityReasonCode
	}
	if !errors.As(err, &reason) || reason.CapabilityReasonCode() != contract.CapabilityReasonHostVMIdle {
		return err
	}
	if session.capabilities.snapshot().ReasonCode != contract.CapabilityReasonHostVMIdle {
		session.capabilities.suppressOCI(contract.CapabilityReasonHostVMIdle, err)
	}
	if !session.hostVM.idleLogged.Swap(true) && session.logf != nil {
		session.logf("node agent: host VM idle; OCI recovery skipped")
	}
	return &CapabilityProbeSkippedError{ReasonCode: contract.CapabilityReasonHostVMIdle}
}

// EnableOnDemand clears a previous off latch without booting the host VM.
func (a *Agent) EnableOnDemand(ctx context.Context, revision uint64) error {
	s := a.session
	if err := lockMutexContext(ctx, &s.ociRecoveryMu); err != nil {
		return err
	}
	defer s.ociRecoveryMu.Unlock()
	if s.ociIntent == nil {
		return errors.New("OCI intent authority is unavailable")
	}
	observation, err := s.ociIntent(ctx)
	if err != nil {
		return err
	}
	if !observation.Enabled || observation.Revision != revision {
		return errors.New("OCI switch changed during on-demand publication")
	}
	if err := s.allowOCIIntentIfUnchanged(a.capabilities.ociSuppressionSequence.Load(), revision); err != nil {
		return err
	}
	if s.ociBootBarrier != nil {
		if _, ready := s.ociBootBarrier.Generation(); ready {
			_, err := s.publishRegistrationCapabilityPinnedLocked(ctx)
			return err
		}
	}
	a.capabilities.suppressOCI(contract.CapabilityReasonHostVMIdle, errors.New("host VM on demand; no runtime started"))
	_, err = s.publishCapabilityHeartbeat(ctx, nil)
	return err
}

// Owner recovery must distinguish an in-progress idle shutdown from failure.
func (session *agentSession) hostVMOwnerRecoveryRefusal() error {
	if session.hostVM.stopping.Load() {
		return &lima.HostVMStoppingError{}
	}
	return nil
}

// The claim caller holds claimMu. Keep all demand sources, including durable
// cleanup, when a successful poll refreshes the watchdog's snapshot.
func (session *agentSession) observeClaimPollDemandLocked(ctx context.Context, waiting bool) {
	if session.registration.OS != "darwin" {
		return
	}
	session.hostVM.mu.Lock()
	session.hostVM.claimPoll.waiting = waiting
	refresh := session.hostVM.refresh
	session.hostVM.mu.Unlock()
	if refresh != nil {
		refresh(ctx)
	}
}
