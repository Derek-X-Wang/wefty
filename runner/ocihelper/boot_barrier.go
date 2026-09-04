package ocihelper

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"slices"
	"sync"
	"syscall"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

const defaultTakeoverRetryInterval = 25 * time.Millisecond

const (
	HelperUnitUnavailable  = "helper_unit_unavailable"
	HelperHandshakeStalled = "helper_handshake_stalled"
)

var errTakeoverWindowExpired = errors.New("OCI helper takeover window expired")

// TakeoverTimeoutForReap is the exclusive-session window: one reap interval
// for helper startup cleanup plus one for an incumbent authority to expire.
func TakeoverTimeoutForReap(reapTimeout time.Duration) time.Duration {
	return reapTimeout + reapTimeout
}

// VerifiedReadyTimeoutForReap composes takeover with the fresh admission
// sweep-and-verify interval that begins only after the handshake succeeds.
func VerifiedReadyTimeoutForReap(reapTimeout time.Duration) time.Duration {
	return TakeoverTimeoutForReap(reapTimeout) + reapTimeout
}

type BootBarrierConfig struct {
	Clock              Clock
	TakeoverTimeout    time.Duration
	MaximumReapTimeout time.Duration
	TakeoverRetry      time.Duration
}

type ReapTimeoutConfigurationError struct {
	AdvertisedReapTimeout time.Duration
	MaximumReapTimeout    time.Duration
}

func (err *ReapTimeoutConfigurationError) Error() string {
	return fmt.Sprintf("OCI helper reap timeout exceeds configured maximum: advertised=%s maximum=%s", err.AdvertisedReapTimeout, err.MaximumReapTimeout)
}

// HelperUnitUnavailableError means no handshake completed and the final dial
// in the complete takeover window positively proved the socket unit absent or
// refusing/resetting.
type HelperUnitUnavailableError struct {
	DialAttempts int
	Cause        error
}

type HelperHandshakeStalledError struct {
	DialAttempts int
	Cause        error
}

func (err *HelperHandshakeStalledError) Code() string { return HelperHandshakeStalled }
func (err *HelperHandshakeStalledError) Error() string {
	if err == nil || err.Cause == nil {
		return HelperHandshakeStalled
	}
	return fmt.Sprintf("%s: OCI helper completed neither a handshake nor session admission after %d takeover attempts: %v", HelperHandshakeStalled, err.DialAttempts, err.Cause)
}
func (err *HelperHandshakeStalledError) Unwrap() error { return err.Cause }

func (err *HelperUnitUnavailableError) Code() string { return HelperUnitUnavailable }

func (err *HelperUnitUnavailableError) Error() string {
	if err == nil || err.Cause == nil {
		return HelperUnitUnavailable
	}
	return fmt.Sprintf("%s: OCI helper completed no handshake after %d takeover attempts: %v", HelperUnitUnavailable, err.DialAttempts, err.Cause)
}

func (err *HelperUnitUnavailableError) Unwrap() error {
	if err == nil {
		return nil
	}
	return err.Cause
}

// NamespaceResidueError preserves the distinction between resources that
// block OCI admission and durable resources intentionally retained across a
// quiescent namespace verification.
type NamespaceResidueError struct {
	Operation         string
	Observed          ResourceInventory
	RuntimeResidue    ResourceInventory
	DurableRetained   ResourceInventory
	DurableRetentions []DurableRetention
}

// ResidueClassificationError means an engine returned an absence verdict that
// disagrees with its explicit runtime-residue projection. The barrier refuses
// to guess which observed resources are durable.
type ResidueClassificationError struct {
	Operation      string
	Absent         bool
	Observed       ResourceInventory
	RuntimeResidue ResourceInventory
}

func (err *ResidueClassificationError) Error() string {
	return fmt.Sprintf("%s: inconsistent runtime-residue classification: absent=%t runtime=%+v observed=%+v", err.Operation, err.Absent, err.RuntimeResidue, err.Observed)
}

func (err *NamespaceResidueError) Error() string {
	cgroupGuidance := ""
	if len(err.RuntimeResidue.Cgroups) > 0 {
		cgroupGuidance = fmt.Sprintf(" unbound_cgroups={paths:%v reason:%q}", err.RuntimeResidue.Cgroups, "unbound wefty-shaped cgroup; not helper-owned; remove manually or bind")
	}
	return fmt.Sprintf("%s: residue remains after sweep: runtime=%+v durable_retained=%+v retention_bindings=%+v observed=%+v%s", err.Operation, err.RuntimeResidue, err.DurableRetained, err.DurableRetentions, err.Observed, cgroupGuidance)
}

func validateNamespaceVerification(operation string, verification VerifyResponse) error {
	if verification.Absent != InventoryEmpty(verification.RuntimeResidue) {
		return &ResidueClassificationError{
			Operation: operation, Absent: verification.Absent,
			Observed: cloneResourceInventory(verification.Inventory), RuntimeResidue: cloneResourceInventory(verification.RuntimeResidue),
		}
	}
	observed := mergeResourceInventory(ResourceInventory{}, verification.Inventory)
	residue := mergeResourceInventory(ResourceInventory{}, verification.RuntimeResidue)
	retained := mergeResourceInventory(ResourceInventory{}, verification.DurableRetained)
	partition := mergeResourceInventory(residue, retained)
	if !inventoryIdentitySetsEqual(observed, partition) || !inventoryPartitionsDisjoint(residue, retained) {
		return fmt.Errorf("%s: observed inventory is not the exact disjoint runtime-residue/durable-retained partition: observed=%+v runtime=%+v durable_retained=%+v", operation, observed, residue, retained)
	}
	retainedTransient := append(slices.Clone(verification.DurableRetained.LogSegments), verification.DurableRetained.Cgroups...)
	slices.Sort(retainedTransient)
	boundTransient := make([]string, 0, len(verification.DurableRetentions))
	for _, retention := range verification.DurableRetentions {
		validClassReason := retention.Class == RemovalResourceLogSegments && retention.Reason == DurableRetentionReasonLogSpoolSealing && retention.State == DurableRetentionStateUnsealed ||
			retention.Class == RemovalResourceCgroup && retention.Reason == DurableRetentionReasonCgroupReaping && retention.State == DurableRetentionStatePopulated
		if !validClassReason || retention.ID == "" || retention.AttemptID == "" || retention.Owner != DurableRetentionOwnerOCIHelper ||
			retention.Bound <= 0 || retention.RecordedAt.IsZero() || retention.Deadline.IsZero() || !retention.Deadline.Equal(retention.RecordedAt.Add(retention.Bound)) {
			return fmt.Errorf("%s: invalid durable-retention binding: %+v", operation, retention)
		}
		boundTransient = append(boundTransient, retention.ID)
	}
	slices.Sort(boundTransient)
	if !slices.Equal(retainedTransient, boundTransient) {
		return fmt.Errorf("%s: transient durable-retained resources lack exact bindings: retained=%v bindings=%v", operation, retainedTransient, boundTransient)
	}
	return nil
}

func inventoryIdentitySetsEqual(left, right ResourceInventory) bool {
	return slices.Equal(left.Leases, right.Leases) && slices.Equal(left.Snapshots, right.Snapshots) &&
		slices.Equal(left.Containers, right.Containers) && slices.Equal(left.Tasks, right.Tasks) &&
		slices.Equal(left.Shims, right.Shims) && slices.Equal(left.Cgroups, right.Cgroups) &&
		slices.Equal(left.LogSegments, right.LogSegments) && slices.Equal(left.ImageSpools, right.ImageSpools) &&
		slices.Equal(left.ManagedVolumes, right.ManagedVolumes) && slices.Equal(left.ManagedVolumeRecords, right.ManagedVolumeRecords) &&
		slices.Equal(left.ComputerDiskImages, right.ComputerDiskImages) && slices.Equal(left.ComputerDiskAllocations, right.ComputerDiskAllocations) &&
		slices.Equal(left.ComputerDiskQuotas, right.ComputerDiskQuotas) && slices.Equal(left.ComputerDiskManifests, right.ComputerDiskManifests) &&
		slices.Equal(left.ComputerDiskMounts, right.ComputerDiskMounts) && slices.Equal(left.ComputerDiskLoops, right.ComputerDiskLoops) &&
		slices.Equal(left.ComputerAttachments, right.ComputerAttachments) && slices.Equal(left.ComputerResetManifests, right.ComputerResetManifests) &&
		slices.Equal(left.ComputerQuarantines, right.ComputerQuarantines) && slices.Equal(left.ComputerStorageDeferred, right.ComputerStorageDeferred) &&
		slices.Equal(left.ComputerStorageQuarantined, right.ComputerStorageQuarantined) && slices.Equal(left.ComputerDiskAnomalies, right.ComputerDiskAnomalies)
}

func inventoryPartitionsDisjoint(left, right ResourceInventory) bool {
	merged := mergeResourceInventory(left, right)
	return inventoryIdentityCountPortable(merged) == inventoryIdentityCountPortable(left)+inventoryIdentityCountPortable(right)
}

func inventoryIdentityCountPortable(inventory ResourceInventory) int {
	return len(inventory.Leases) + len(inventory.Snapshots) + len(inventory.Containers) + len(inventory.Tasks) + len(inventory.Shims) +
		len(inventory.Cgroups) + len(inventory.LogSegments) + len(inventory.ImageSpools) + len(inventory.ManagedVolumes) + len(inventory.ManagedVolumeRecords) +
		len(inventory.ComputerDiskImages) + len(inventory.ComputerDiskAllocations) + len(inventory.ComputerDiskQuotas) + len(inventory.ComputerDiskManifests) +
		len(inventory.ComputerDiskMounts) + len(inventory.ComputerDiskLoops) + len(inventory.ComputerAttachments) + len(inventory.ComputerResetManifests) +
		len(inventory.ComputerQuarantines) + len(inventory.ComputerStorageDeferred) + len(inventory.ComputerStorageQuarantined) + len(inventory.ComputerDiskAnomalies)
}

func namespaceResidueError(operation string, verification VerifyResponse) error {
	return &NamespaceResidueError{
		Operation:         operation,
		Observed:          cloneResourceInventory(verification.Inventory),
		RuntimeResidue:    cloneResourceInventory(verification.RuntimeResidue),
		DurableRetained:   cloneResourceInventory(verification.DurableRetained),
		DurableRetentions: slices.Clone(verification.DurableRetentions),
	}
}

// BootBarrier owns one exclusive helper session and proves the wefty runtime
// namespace empty before that session may be used. State readers never wait
// for takeover RPCs: ensureMu serializes retries while mu protects snapshots.
type BootBarrier struct {
	client  *Client
	request AcquireSessionRequest
	config  BootBarrierConfig

	ensureMu        sync.Mutex
	mu              sync.RWMutex
	session         *Session
	prepared        bool
	receipt         VerifiedSweepReceipt
	lastLossReceipt VerifiedSweepReceipt
	loss            func(HelperSession, error)
	reason          contract.CapabilityReasonCode
	stalledWindows  uint64
	lastLossAt      time.Time
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
	if config.MaximumReapTimeout <= 0 {
		config.MaximumReapTimeout = defaultReapTimeout
	}
	if config.TakeoverTimeout <= 0 {
		config.TakeoverTimeout = TakeoverTimeoutForReap(config.MaximumReapTimeout)
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

// LastLossSweepReceipt returns the latest successful sweep whose acquisition
// began after this barrier observed a helper-session loss. It survives later
// explicit acquisitions so incident evidence cannot be overwritten by a
// follow-up capability refresh.
func (barrier *BootBarrier) LastLossSweepReceipt() (VerifiedSweepReceipt, bool) {
	if barrier == nil {
		return VerifiedSweepReceipt{}, false
	}
	barrier.mu.RLock()
	defer barrier.mu.RUnlock()
	if barrier.lastLossReceipt.BarrierTimeline.HelperLossObservedAt.IsZero() {
		return VerifiedSweepReceipt{}, false
	}
	return cloneVerifiedSweepReceipt(barrier.lastLossReceipt), true
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

func (barrier *BootBarrier) Ensure(ctx context.Context) (ensureErr error) {
	if barrier == nil {
		return errors.New("OCI boot barrier is unavailable")
	}
	defer func() { barrier.recordCapabilityReason(ensureErr) }()
	barrier.ensureMu.Lock()
	defer barrier.ensureMu.Unlock()
	if barrier.Ready() {
		return nil
	}
	barrier.detachSession()
	takeoverStarted := time.Now()
	barrier.mu.RLock()
	lossObservedAt := barrier.lastLossAt
	barrier.mu.RUnlock()
	session, handshakeElapsed, sessionAdmissionElapsed, err := barrier.takeExclusiveSession(ctx, takeoverStarted)
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
	sweepStarted := time.Now()
	sweep, err := session.Sweep(barrierContext, SweepRequest{})
	if err != nil {
		return fmt.Errorf("sweep all OCI runtime state: %w", err)
	}
	sweepElapsed := time.Since(sweepStarted)
	verifyStarted := time.Now()
	verification, err := session.Verify(barrierContext, VerifyRequest{Scope: VerifyNamespace})
	if err != nil {
		return fmt.Errorf("verify OCI runtime namespace: %w", err)
	}
	if err := validateNamespaceVerification("verify OCI runtime namespace", verification); err != nil {
		return err
	}
	if !verification.Absent {
		return namespaceResidueError("verify OCI runtime namespace", verification)
	}
	verifiedReadyElapsed := time.Since(takeoverStarted)
	receipt := VerifiedSweepReceipt{
		SweepEpoch:                      sweep.SweepEpoch,
		HelperSession:                   HelperSession{HelperInstanceID: handshake.HelperInstanceID, SessionGeneration: handshake.SessionGeneration},
		PriorBootSessionsSeen:           slices.Clone(sweep.PriorBootSessionsSeen),
		SweptInventory:                  cloneResourceInventory(sweep.Inventory),
		VerifiedAbsent:                  verification.Absent,
		VerifiedInventory:               cloneResourceInventory(verification.Inventory),
		VerifiedResidue:                 cloneResourceInventory(verification.RuntimeResidue),
		VerifiedRetained:                cloneResourceInventory(verification.DurableRetained),
		ComputerStorageDeferredCount:    len(verification.DurableRetained.ComputerStorageDeferred),
		ComputerStorageQuarantinedCount: len(verification.DurableRetained.ComputerStorageQuarantined),
		DurableRetentions:               slices.Clone(verification.DurableRetentions),
		SweepEvidence:                   cloneSweepEvidence(sweep.Evidence),
		Attempts:                        slices.Clone(sweep.Attempts),
		BarrierTimeline: BootBarrierTimelineReceipt{
			AdvertisedReapTimeout:   handshake.ReapTimeout,
			TakeoverBound:           min(barrier.config.TakeoverTimeout, TakeoverTimeoutForReap(handshake.ReapTimeout)),
			VerifiedReadyBound:      min(barrier.config.TakeoverTimeout, TakeoverTimeoutForReap(handshake.ReapTimeout)) + handshake.ReapTimeout,
			HelperLossObservedAt:    lossObservedAt,
			BarrierStartedAt:        takeoverStarted,
			PrefaceCompletedAt:      takeoverStarted.Add(handshakeElapsed),
			SessionAdmittedAt:       takeoverStarted.Add(sessionAdmissionElapsed),
			VerifiedReadyAt:         takeoverStarted.Add(verifiedReadyElapsed),
			PrefacedDuringStartup:   handshake.StartupInProgress,
			HandshakeElapsed:        handshakeElapsed,
			SessionAdmissionElapsed: sessionAdmissionElapsed,
			SweepElapsed:            sweepElapsed,
			VerifyElapsed:           time.Since(verifyStarted),
			VerifiedReadyElapsed:    verifiedReadyElapsed,
		},
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
	if !lossObservedAt.IsZero() {
		barrier.lastLossReceipt = cloneVerifiedSweepReceipt(receipt)
	}
	if barrier.lastLossAt.Equal(lossObservedAt) {
		barrier.lastLossAt = time.Time{}
	}
	barrier.mu.Unlock()
	cleanup = false
	return nil
}

// CapabilityReasonCode exposes the last Ensure outcome through the agent's
// bounded capability-restriction surface. A healthy barrier has no reason.
func (barrier *BootBarrier) CapabilityReasonCode() contract.CapabilityReasonCode {
	if barrier == nil {
		return contract.CapabilityReasonBootSweepFailed
	}
	barrier.mu.RLock()
	defer barrier.mu.RUnlock()
	return barrier.reason
}

// HandshakeStalledWindows is the consecutive bounded takeover windows that
// connected to the helper socket without completing a handshake.
func (barrier *BootBarrier) HandshakeStalledWindows() uint64 {
	if barrier == nil {
		return 0
	}
	barrier.mu.RLock()
	defer barrier.mu.RUnlock()
	return barrier.stalledWindows
}

func (barrier *BootBarrier) recordCapabilityReason(err error) {
	reason := contract.CapabilityReasonCode("")
	if err != nil {
		var unavailable *HelperUnitUnavailableError
		if errors.As(err, &unavailable) {
			reason = contract.CapabilityReasonHelperUnitUnavailable
		} else if errors.As(err, new(*HelperHandshakeStalledError)) {
			reason = contract.CapabilityReasonHelperHandshakeStalled
		} else {
			reason = contract.CapabilityReasonBootSweepFailed
		}
	}
	barrier.mu.Lock()
	barrier.reason = reason
	if reason == contract.CapabilityReasonHelperHandshakeStalled {
		barrier.stalledWindows++
	} else {
		barrier.stalledWindows = 0
	}
	barrier.mu.Unlock()
}

func (barrier *BootBarrier) takeExclusiveSession(ctx context.Context, takeoverStarted time.Time) (*Session, time.Duration, time.Duration, error) {
	takeoverDeadline := takeoverStarted.Add(barrier.config.TakeoverTimeout)
	var advertisedReapTimeout time.Duration
	var handshakeElapsed time.Duration
	dialAttempts := 0
	completedHandshakes := 0
	lastDialConnected := false
	lastAdmissionStalled := false
	var lastUnavailableError error
	takeoverError := func(contextError error, windowExpired bool) error {
		if dialAttempts > 0 && lastAdmissionStalled && windowExpired {
			return &HelperHandshakeStalledError{DialAttempts: dialAttempts, Cause: contextError}
		}
		if dialAttempts > 0 && completedHandshakes == 0 && !lastDialConnected && lastUnavailableError != nil && windowExpired {
			return &HelperUnitUnavailableError{
				DialAttempts: dialAttempts,
				Cause:        errors.Join(contextError, lastUnavailableError),
			}
		}
		if dialAttempts > 0 && completedHandshakes == 0 && lastDialConnected && windowExpired {
			return &HelperHandshakeStalledError{DialAttempts: dialAttempts, Cause: contextError}
		}
		return fmt.Errorf("acquire exclusive OCI helper session: %w", contextError)
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, 0, 0, takeoverError(err, false)
		}
		if !time.Now().Before(takeoverDeadline) {
			return nil, 0, 0, takeoverError(errors.Join(errTakeoverWindowExpired, context.DeadlineExceeded), true)
		}
		dialAttempts++
		lastDialConnected = false
		lastAdmissionStalled = false
		lastUnavailableError = nil
		handshakeCompleted := false
		attemptHandshakeElapsed := time.Duration(0)
		session, err := barrier.client.openSession(ctx, barrier.request, &lastDialConnected, takeoverDeadline, func(handshake AcquireSessionResponse) (time.Time, error) {
			handshakeCompleted = true
			attemptHandshakeElapsed = time.Since(takeoverStarted)
			if advertisedReapTimeout == 0 {
				advertisedReapTimeout = handshake.ReapTimeout
			} else if handshake.ReapTimeout != advertisedReapTimeout {
				return time.Time{}, fmt.Errorf("OCI helper reap timeout changed during takeover: first=%s current=%s", advertisedReapTimeout, handshake.ReapTimeout)
			}
			if handshake.ReapTimeout > barrier.config.MaximumReapTimeout {
				return time.Time{}, &ReapTimeoutConfigurationError{
					AdvertisedReapTimeout: handshake.ReapTimeout,
					MaximumReapTimeout:    barrier.config.MaximumReapTimeout,
				}
			}
			advertisedDeadline := takeoverStarted.Add(TakeoverTimeoutForReap(advertisedReapTimeout))
			if advertisedDeadline.Before(takeoverDeadline) {
				takeoverDeadline = advertisedDeadline
			}
			return takeoverDeadline, nil
		})
		if handshakeCompleted {
			completedHandshakes++
		}
		if err == nil {
			handshakeElapsed = attemptHandshakeElapsed
			return session, handshakeElapsed, time.Since(takeoverStarted), nil
		}
		if contextError := ctx.Err(); contextError != nil {
			return nil, 0, 0, takeoverError(contextError, false)
		}
		// A socket-backlog connection can reach its I/O deadline immediately
		// before the context timer publishes Done. OpenSession applies this
		// context's deadline to the connection, so synchronize that edge before
		// classifying a handshake transport error.
		var admissionErr *sessionAdmissionTransportError
		lastAdmissionStalled = handshakeCompleted && errors.As(err, &admissionErr)
		if !time.Now().Before(takeoverDeadline) && errors.Is(err, os.ErrDeadlineExceeded) {
			return nil, 0, 0, takeoverError(errors.Join(errTakeoverWindowExpired, context.DeadlineExceeded), true)
		}
		var rpcErr *RPCError
		var dialErr *helperDialError
		if errors.As(err, &dialErr) && (errors.Is(dialErr, os.ErrNotExist) || errors.Is(dialErr, syscall.ECONNREFUSED) || errors.Is(dialErr, syscall.ECONNRESET)) {
			lastUnavailableError = err
		} else if errors.As(err, &rpcErr) && rpcErr.Code == CodeSessionBusy {
			lastUnavailableError = nil
		} else if errors.As(err, &admissionErr) {
			lastUnavailableError = nil
		} else if lastDialConnected && !handshakeCompleted && helperHandshakeTransportFailure(err) {
			lastUnavailableError = nil
		} else {
			return nil, 0, 0, fmt.Errorf("acquire exclusive OCI helper session: %w", err)
		}
		timer := barrier.config.Clock.NewTimerAt(barrier.config.Clock.Now().Add(barrier.config.TakeoverRetry))
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, 0, 0, takeoverError(ctx.Err(), false)
		case <-timer.C():
		}
	}
}

func helperHandshakeTransportFailure(err error) bool {
	var networkError net.Error
	return errors.As(err, &networkError) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrClosedPipe) ||
		errors.Is(err, net.ErrClosed) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE)
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
	barrier.lastLossAt = time.Now()
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
	receipt.VerifiedResidue = cloneResourceInventory(receipt.VerifiedResidue)
	receipt.VerifiedRetained = cloneResourceInventory(receipt.VerifiedRetained)
	receipt.DurableRetentions = slices.Clone(receipt.DurableRetentions)
	receipt.SweepEvidence = cloneSweepEvidence(receipt.SweepEvidence)
	receipt.Attempts = slices.Clone(receipt.Attempts)
	return receipt
}

func cloneSweepEvidence(evidence []SweepEvidence) []SweepEvidence {
	cloned := slices.Clone(evidence)
	for index := range cloned {
		cloned[index].PIDs = slices.Clone(cloned[index].PIDs)
	}
	return cloned
}

func cloneResourceInventory(inventory ResourceInventory) ResourceInventory {
	inventory.Leases = slices.Clone(inventory.Leases)
	inventory.Snapshots = slices.Clone(inventory.Snapshots)
	inventory.Containers = slices.Clone(inventory.Containers)
	inventory.Tasks = slices.Clone(inventory.Tasks)
	inventory.Shims = slices.Clone(inventory.Shims)
	inventory.Cgroups = slices.Clone(inventory.Cgroups)
	inventory.LogSegments = slices.Clone(inventory.LogSegments)
	inventory.ImageSpools = slices.Clone(inventory.ImageSpools)
	inventory.ManagedVolumes = slices.Clone(inventory.ManagedVolumes)
	inventory.ManagedVolumeRecords = slices.Clone(inventory.ManagedVolumeRecords)
	inventory.ComputerDiskImages = slices.Clone(inventory.ComputerDiskImages)
	inventory.ComputerDiskAllocations = slices.Clone(inventory.ComputerDiskAllocations)
	inventory.ComputerDiskQuotas = slices.Clone(inventory.ComputerDiskQuotas)
	inventory.ComputerDiskManifests = slices.Clone(inventory.ComputerDiskManifests)
	inventory.ComputerDiskMounts = slices.Clone(inventory.ComputerDiskMounts)
	inventory.ComputerDiskLoops = slices.Clone(inventory.ComputerDiskLoops)
	inventory.ComputerAttachments = slices.Clone(inventory.ComputerAttachments)
	inventory.ComputerResetManifests = slices.Clone(inventory.ComputerResetManifests)
	inventory.ComputerQuarantines = slices.Clone(inventory.ComputerQuarantines)
	inventory.ComputerStorageDeferred = slices.Clone(inventory.ComputerStorageDeferred)
	inventory.ComputerStorageQuarantined = slices.Clone(inventory.ComputerStorageQuarantined)
	inventory.ComputerDiskAnomalies = slices.Clone(inventory.ComputerDiskAnomalies)
	return inventory
}

// InventoryEmpty reports whether every runtime-owned namespace class is absent.
func InventoryEmpty(inventory ResourceInventory) bool {
	return len(inventory.Leases)+len(inventory.Snapshots)+len(inventory.Containers)+len(inventory.Tasks)+
		len(inventory.Shims)+len(inventory.Cgroups)+len(inventory.LogSegments)+len(inventory.ImageSpools)+len(inventory.ManagedVolumes)+len(inventory.ManagedVolumeRecords)+
		len(inventory.ComputerDiskImages)+len(inventory.ComputerDiskAllocations)+len(inventory.ComputerDiskQuotas)+len(inventory.ComputerDiskManifests)+len(inventory.ComputerDiskMounts)+len(inventory.ComputerDiskLoops)+len(inventory.ComputerAttachments)+len(inventory.ComputerResetManifests)+len(inventory.ComputerQuarantines)+len(inventory.ComputerStorageDeferred)+len(inventory.ComputerStorageQuarantined)+len(inventory.ComputerDiskAnomalies) == 0
}
