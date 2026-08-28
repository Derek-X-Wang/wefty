package ocihelper

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"
)

const defaultTakeoverRetryInterval = 25 * time.Millisecond

type BootBarrierConfig struct {
	Clock           Clock
	TakeoverTimeout time.Duration
	TakeoverRetry   time.Duration
}

// BootBarrier owns one exclusive helper session and proves the wefty runtime
// namespace empty before that session may be used. State readers never wait
// for takeover RPCs: ensureMu serializes retries while mu protects snapshots.
type BootBarrier struct {
	client  *Client
	request AcquireSessionRequest
	config  BootBarrierConfig

	ensureMu sync.Mutex
	mu       sync.RWMutex
	session  *Session
	prepared bool
	receipt  VerifiedSweepReceipt
	loss     func(HelperSession, error)
}

func NewBootBarrier(client *Client, request AcquireSessionRequest) (*BootBarrier, error) {
	return NewBootBarrierWithConfig(client, request, BootBarrierConfig{})
}

func NewBootBarrierWithConfig(client *Client, request AcquireSessionRequest, config BootBarrierConfig) (*BootBarrier, error) {
	if client == nil || client.Dial == nil {
		return nil, errors.New("OCI boot barrier requires a helper client")
	}
	if request.NodeID == "" || request.BootSessionID == "" {
		return nil, errors.New("OCI boot barrier requires node and boot session IDs")
	}
	if config.Clock == nil {
		config.Clock = systemClock{}
	}
	if config.TakeoverTimeout <= 0 {
		config.TakeoverTimeout = defaultReapTimeout
	}
	if config.TakeoverRetry <= 0 {
		config.TakeoverRetry = defaultTakeoverRetryInterval
	}
	return &BootBarrier{client: client, request: request, config: config}, nil
}

func (barrier *BootBarrier) Ready() bool {
	_, ok := barrier.Generation()
	return ok
}

func (barrier *BootBarrier) Generation() (HelperSession, bool) {
	if barrier == nil {
		return HelperSession{}, false
	}
	barrier.mu.RLock()
	defer barrier.mu.RUnlock()
	if !barrier.prepared || barrier.session == nil || barrier.session.HealthError() != nil {
		return HelperSession{}, false
	}
	return barrier.receipt.HelperSession, true
}

func (barrier *BootBarrier) SweepReceipt() (VerifiedSweepReceipt, bool) {
	if barrier == nil {
		return VerifiedSweepReceipt{}, false
	}
	barrier.mu.RLock()
	defer barrier.mu.RUnlock()
	if !barrier.prepared || barrier.session == nil || barrier.session.HealthError() != nil {
		return VerifiedSweepReceipt{}, false
	}
	return cloneVerifiedSweepReceipt(barrier.receipt), true
}

// ExecutionSnapshot atomically captures the session and the sweep receipt
// that authorized it. Adapters call this immediately before Run so recovery
// cannot pair an old session with a new sweep epoch (or vice versa).
func (barrier *BootBarrier) ExecutionSnapshot() (*Session, VerifiedSweepReceipt, error) {
	if barrier == nil {
		return nil, VerifiedSweepReceipt{}, errors.New("OCI boot barrier is unavailable")
	}
	barrier.mu.RLock()
	defer barrier.mu.RUnlock()
	if !barrier.prepared || barrier.session == nil {
		return nil, VerifiedSweepReceipt{}, errors.New("OCI boot barrier has not completed")
	}
	if err := barrier.session.HealthError(); err != nil {
		return nil, VerifiedSweepReceipt{}, err
	}
	return barrier.session, cloneVerifiedSweepReceipt(barrier.receipt), nil
}

func (barrier *BootBarrier) SetLossHandler(handler func(HelperSession, error)) {
	if barrier == nil {
		return
	}
	barrier.mu.Lock()
	barrier.loss = handler
	barrier.mu.Unlock()
}

func (barrier *BootBarrier) Ensure(ctx context.Context) error {
	if barrier == nil {
		return errors.New("OCI boot barrier is unavailable")
	}
	barrier.ensureMu.Lock()
	defer barrier.ensureMu.Unlock()
	if barrier.Ready() {
		return nil
	}
	barrier.detachSession()
	takeoverContext, cancel := context.WithTimeout(ctx, barrier.config.TakeoverTimeout)
	defer cancel()
	session, err := barrier.takeExclusiveSession(takeoverContext)
	if err != nil {
		return err
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = session.Close()
		}
	}()
	handshake := session.Handshake()
	if handshake.HelperInstanceID == "" || handshake.SessionGeneration == 0 || handshake.ReapTimeout <= 0 {
		return errors.New("OCI helper handshake omitted barrier authority")
	}
	barrierContext, barrierCancel := context.WithTimeout(ctx, handshake.ReapTimeout)
	defer barrierCancel()
	sweep, err := session.Sweep(barrierContext, SweepRequest{})
	if err != nil {
		return fmt.Errorf("sweep all OCI runtime state: %w", err)
	}
	verification, err := session.Verify(barrierContext, VerifyRequest{Scope: VerifyNamespace})
	if err != nil {
		return fmt.Errorf("verify OCI runtime namespace: %w", err)
	}
	if !verification.Absent || !inventoryEmpty(verification.Inventory) {
		return errors.New("verify OCI runtime namespace: residue remains after sweep")
	}
	receipt := VerifiedSweepReceipt{
		SweepEpoch:            sweep.SweepEpoch,
		HelperSession:         HelperSession{HelperInstanceID: handshake.HelperInstanceID, SessionGeneration: handshake.SessionGeneration},
		PriorBootSessionsSeen: slices.Clone(sweep.PriorBootSessionsSeen),
		SweptInventory:        cloneResourceInventory(sweep.Inventory),
		VerifiedInventory:     cloneResourceInventory(verification.Inventory),
		Attempts:              slices.Clone(sweep.Attempts),
	}
	if receipt.SweepEpoch == "" {
		return errors.New("sweep all OCI runtime state: helper omitted sweep epoch")
	}
	session.SetLossHandler(func(lossErr error) { barrier.sessionLost(session, receipt.HelperSession, lossErr) })
	if err := session.HealthError(); err != nil {
		return fmt.Errorf("OCI helper session lost after namespace verification: %w", err)
	}
	barrier.mu.Lock()
	barrier.session = session
	barrier.prepared = true
	barrier.receipt = receipt
	barrier.mu.Unlock()
	cleanup = false
	return nil
}

func (barrier *BootBarrier) takeExclusiveSession(ctx context.Context) (*Session, error) {
	for {
		session, err := barrier.client.OpenSession(ctx, barrier.request)
		if err == nil {
			return session, nil
		}
		var rpcErr *RPCError
		if !errors.As(err, &rpcErr) || rpcErr.Code != CodeSessionBusy {
			return nil, fmt.Errorf("acquire exclusive OCI helper session: %w", err)
		}
		timer := barrier.config.Clock.NewTimerAt(barrier.config.Clock.Now().Add(barrier.config.TakeoverRetry))
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, fmt.Errorf("acquire exclusive OCI helper session: %w", ctx.Err())
		case <-timer.C():
		}
	}
}

func (barrier *BootBarrier) Session() (*Session, error) {
	if barrier == nil {
		return nil, errors.New("OCI boot barrier is unavailable")
	}
	barrier.mu.RLock()
	defer barrier.mu.RUnlock()
	if !barrier.prepared || barrier.session == nil {
		return nil, errors.New("OCI boot barrier has not completed")
	}
	if err := barrier.session.HealthError(); err != nil {
		return nil, err
	}
	return barrier.session, nil
}

func (barrier *BootBarrier) Invalidate() {
	if barrier != nil {
		barrier.detachSession()
	}
}

func (barrier *BootBarrier) Close() error {
	if barrier == nil {
		return nil
	}
	barrier.ensureMu.Lock()
	defer barrier.ensureMu.Unlock()
	return barrier.detachSession()
}

func (barrier *BootBarrier) detachSession() error {
	barrier.mu.Lock()
	barrier.prepared = false
	barrier.receipt = VerifiedSweepReceipt{}
	session := barrier.session
	barrier.session = nil
	barrier.mu.Unlock()
	if session == nil {
		return nil
	}
	session.SetLossHandler(nil)
	return session.Close()
}

func (barrier *BootBarrier) sessionLost(session *Session, generation HelperSession, err error) {
	barrier.mu.Lock()
	if barrier.session != session || barrier.receipt.HelperSession != generation {
		barrier.mu.Unlock()
		return
	}
	barrier.prepared = false
	barrier.receipt = VerifiedSweepReceipt{}
	handler := barrier.loss
	barrier.mu.Unlock()
	if handler != nil {
		handler(generation, err)
	}
}

func cloneVerifiedSweepReceipt(receipt VerifiedSweepReceipt) VerifiedSweepReceipt {
	receipt.PriorBootSessionsSeen = slices.Clone(receipt.PriorBootSessionsSeen)
	receipt.SweptInventory = cloneResourceInventory(receipt.SweptInventory)
	receipt.VerifiedInventory = cloneResourceInventory(receipt.VerifiedInventory)
	receipt.Attempts = slices.Clone(receipt.Attempts)
	return receipt
}

func cloneResourceInventory(inventory ResourceInventory) ResourceInventory {
	inventory.Leases = slices.Clone(inventory.Leases)
	inventory.Snapshots = slices.Clone(inventory.Snapshots)
	inventory.Containers = slices.Clone(inventory.Containers)
	inventory.Tasks = slices.Clone(inventory.Tasks)
	inventory.Shims = slices.Clone(inventory.Shims)
	inventory.Cgroups = slices.Clone(inventory.Cgroups)
	inventory.LogSegments = slices.Clone(inventory.LogSegments)
	inventory.ManagedVolumes = slices.Clone(inventory.ManagedVolumes)
	return inventory
}

func inventoryEmpty(inventory ResourceInventory) bool {
	return len(inventory.Leases)+len(inventory.Snapshots)+len(inventory.Containers)+len(inventory.Tasks)+
		len(inventory.Shims)+len(inventory.Cgroups)+len(inventory.LogSegments)+len(inventory.ManagedVolumes) == 0
}
