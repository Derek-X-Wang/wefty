package agent

import (
	"context"
	"errors"
	"fmt"
	rand "math/rand/v2"
	"sync"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

const (
	DefaultSessionBackoffBase = 250 * time.Millisecond
	DefaultSessionBackoffMax  = 30 * time.Second
)

type workloadClass uint8

const (
	workloadClassOneShot workloadClass = iota
	workloadClassService
)

type classPool struct {
	class    workloadClass
	selector string
}

var classPools = []classPool{
	{class: workloadClassOneShot, selector: contract.JobClassOneShot},
	{class: workloadClassService, selector: contract.JobClassService},
}

type sessionAttemptExecution func(context.Context, l1.Claim, time.Time) (errorDestination, error)

// agentSession owns control-plane connectivity, heartbeat error routing, and
// the barrier that joins every attempt before session resources may close.
type agentSession struct {
	client            *Client
	registration      contract.NodeRegistration
	heartbeatInterval time.Duration
	claimInterval     time.Duration
	clock             Clock
	observer          *lifecycleObserver
	routeError        destinationErrorPolicy
	gates             map[workloadClass]classAdmissionGate
	localLimits       map[workloadClass]int
	logf              func(string, ...any)
	capabilities      *capabilityState
	ociBootBarrier    OCIBootBarrier
	ociImagePins      workloadrunner.OCIImagePinRuntime
	ociRecoveryMu     sync.Mutex

	capacityMu      sync.Mutex
	poolTargets     map[workloadClass]int
	capacityChanged chan struct{}
	claimsEnabled   bool

	claimMu                   sync.Mutex
	residentJobID             map[string]struct{}
	residentKind              map[string]string
	resident                  map[string]*residentAttempt
	residentSuppressionErrors map[string]error
	serviceReaps              map[string]runtimeReapOutcome
	serviceBoots              map[string]string
	residentChanged           chan struct{}
	// ociStopBeforeResidentScan is a test seam for completing a resident in
	// the narrow interval after the controller fence is released and before
	// runtime teardown takes its first resident snapshot. Production leaves it nil.
	ociStopBeforeResidentScan func()
	reapPriorBoot             func(context.Context, string, string, []workloadrunner.RuntimeResourceManifest) (workloadrunner.ReapReceipt, error)
	removals                  *removalController
	storageResets             *storageResetController
	storageGrows              *storageGrowController
	reimagePreflights         *reimagePreflightController
	backups                   *backupController
	storageCopies             *storageCopyController
	custody                   *custodyController
	computerPolicy            *ComputerPolicyCache
	computerAcks              *computerPolicyAckController

	drainOnce      sync.Once
	drainRequested chan struct{}
	attempts       sync.WaitGroup
}

type residentAttempt struct {
	class         string
	kind          string
	cancel        context.CancelCauseFunc
	done          chan struct{}
	runtimeReaped chan runtimeReapOutcome
	completionErr error
}

type runtimeReapOutcome struct {
	receipt workloadrunner.ReapReceipt
	err     error
}

type destinationError struct {
	destination errorDestination
	err         error
}

type routedDestinationError struct {
	destination errorDestination
	err         error
}

func (err *routedDestinationError) Error() string { return err.err.Error() }
func (err *routedDestinationError) Unwrap() error { return err.err }

func newAgentSession(
	client *Client,
	registration contract.NodeRegistration,
	capabilities *capabilityState,
	heartbeatInterval, claimInterval time.Duration,
	clock Clock,
	observer *lifecycleObserver,
	logf func(string, ...any),
	maxOneshotSlots, maxServiceSlots int,
) *agentSession {
	session := &agentSession{
		client: client, registration: registration,
		heartbeatInterval: heartbeatInterval, claimInterval: claimInterval,
		clock: clock, observer: observer, logf: logf, capabilities: capabilities,
		routeError: func(destination errorDestination, err error) error {
			if destination == errorDestinationAttemptAuthority {
				return nil
			}
			return &routedDestinationError{destination: destination, err: err}
		},
		gates: map[workloadClass]classAdmissionGate{
			workloadClassOneShot: newAdmissionGate(maxOneshotSlots),
			workloadClassService: newAdmissionGate(maxServiceSlots),
		},
		localLimits: map[workloadClass]int{
			workloadClassOneShot: maxOneshotSlots,
			workloadClassService: maxServiceSlots,
		},
		poolTargets: map[workloadClass]int{
			workloadClassOneShot: maxOneshotSlots,
			workloadClassService: maxServiceSlots,
		},
		capacityChanged:           make(chan struct{}, 1),
		claimsEnabled:             true,
		residentJobID:             make(map[string]struct{}),
		residentKind:              make(map[string]string),
		resident:                  make(map[string]*residentAttempt),
		residentSuppressionErrors: make(map[string]error),
		serviceReaps:              make(map[string]runtimeReapOutcome),
		serviceBoots:              make(map[string]string),
		residentChanged:           make(chan struct{}, 1),
		drainRequested:            make(chan struct{}),
		computerPolicy:            NewComputerPolicyCache(clock, registration.NodeID, registration.BootSessionID),
	}
	session.computerAcks = newComputerPolicyAckController(client, clock, logf)
	return session
}

func (session *agentSession) close() {
	if session != nil && session.computerPolicy != nil {
		session.computerPolicy.Close()
	}
	if session != nil && session.client != nil {
		session.client.Close()
	}
}

func (session *agentSession) register(ctx context.Context) (l1.Node, error) {
	if session.computerPolicy != nil {
		session.computerPolicy.Invalidate(ComputerPolicyWatchLost)
	}
	if session.ociBootBarrier == nil {
		if err := session.capabilities.refresh(ctx); err != nil && session.logf != nil {
			session.logf("agent: capability probe before registration: %v", err)
		}
		return session.publishRegistration(ctx)
	}

	// Establish node authority first, but only with a restrictive observation.
	// The returned L1 projection is the atomic same-boot revision oracle.
	session.capabilities.suppressOCI(
		ociBootBarrierReason(session.ociBootBarrier),
		errors.New("OCI helper session requires a boot sweep"),
	)
	node, err := session.publishRegistration(ctx)
	if err != nil {
		return l1.Node{}, err
	}
	if err := session.capabilities.adoptRestrictive(node); err != nil {
		return l1.Node{}, err
	}
	registrationHeartbeat, err := session.heartbeat(ctx)
	if err != nil {
		return l1.Node{}, err
	}
	if err := session.installComputerPolicy(ctx, registrationHeartbeat.ComputerPolicy); err != nil {
		session.computerPolicy.Invalidate(ComputerPolicyWatchLost)
		if session.logf != nil {
			session.logf("agent: Computer policy heartbeat bootstrap failed closed: %v", err)
		}
	}

	barrierErr := session.ociBootBarrier.Ensure(ctx)
	if barrierErr != nil {
		session.capabilities.suppressOCI(ociBootBarrierReason(session.ociBootBarrier), barrierErr)
	}
	// ADR-0002 removal recovery is independent of OCI readiness and always runs
	// once registration authority and its restrictive N+1 are published.
	removalErr := errors.Join(session.resumePendingRemovals(ctx), session.processRemovalDirectives(ctx, registrationHeartbeat.RemovalDirectives),
		session.processStorageResetDirectives(ctx, registrationHeartbeat.StorageResetDirectives),
		session.processStorageGrowDirectives(ctx, registrationHeartbeat.StorageGrowDirectives),
		session.processReimageDirectives(ctx, registrationHeartbeat.ReimageDirectives),
		session.processBackupDirectives(ctx, registrationHeartbeat.BackupDirectives),
		session.processBackupPruneDirectives(ctx, registrationHeartbeat.BackupPruneDirectives),
		session.processStorageCopyDirectives(ctx, registrationHeartbeat.StorageCopyDirectives),
		session.processCustodyExportDirectives(ctx, registrationHeartbeat.CustodyExportDirectives))
	pinsErr := error(nil)
	if barrierErr == nil && removalErr == nil {
		pinsErr = session.reconcileOCIImagePins(ctx)
	}
	if barrierErr != nil || removalErr != nil || pinsErr != nil {
		if session.logf != nil {
			if barrierErr != nil {
				session.logf("agent: OCI boot barrier before registration: %v", barrierErr)
			}
			if removalErr != nil {
				session.logf("agent: resume pending removals before registration: %v", removalErr)
			}
			if pinsErr != nil {
				session.logf("agent: reconcile OCI binding pins before registration: %v", pinsErr)
			}
		}
		return node, nil
	}
	generation, ok := session.ociBootBarrier.Generation()
	if !ok {
		session.capabilities.suppressOCI(contract.CapabilityReasonBootSweepFailed, errors.New("OCI helper session lost after removal recovery"))
		return session.publishCapabilityHeartbeat(ctx, nil)
	}
	confirmed, confirmedOK := session.ociBootBarrier.Generation()
	if !confirmedOK || confirmed != generation {
		session.capabilities.suppressOCI(contract.CapabilityReasonBootSweepFailed, errors.New("OCI helper session changed before functional probe"))
		return session.publishCapabilityHeartbeat(ctx, nil)
	}
	if err := session.capabilities.refreshValidated(ctx, func() error {
		return session.validateOCIGeneration(generation)
	}); err != nil {
		if session.logf != nil {
			session.logf("agent: OCI functional probe before publication: %v", err)
		}
		return session.publishCapabilityHeartbeat(ctx, nil)
	}
	return session.publishCapabilityHeartbeat(ctx, &generation)
}

func (session *agentSession) processRemovalDirectives(ctx context.Context, directives []l1.RemovalDirective) error {
	if session.removals == nil {
		return nil
	}
	var failures []error
	for _, directive := range directives {
		if err := session.removals.process(ctx, directive); err != nil {
			failures = append(failures, fmt.Errorf("reconcile removed OCI binding %q: %w", directive.JobID, err))
		}
	}
	return errors.Join(failures...)
}

func (session *agentSession) processStorageResetDirectives(ctx context.Context, directives []l1.ComputerStorageResetDirective) error {
	if session.storageResets == nil {
		return nil
	}
	var failures []error
	for _, directive := range directives {
		if err := session.storageResets.process(ctx, directive); err != nil {
			failures = append(failures, fmt.Errorf("reconcile Computer Storage reset %q: %w", directive.ComputerID, err))
		}
	}
	return errors.Join(failures...)
}

func (session *agentSession) processStorageGrowDirectives(ctx context.Context, directives []l1.ComputerStorageGrowDirective) error {
	if session.storageGrows == nil {
		return nil
	}
	var failures []error
	for _, directive := range directives {
		if err := session.storageGrows.process(ctx, directive); err != nil {
			failures = append(failures, fmt.Errorf("reconcile Computer Storage grow %q: %w", directive.ComputerID, err))
		}
	}
	return errors.Join(failures...)
}

func (session *agentSession) processReimageDirectives(ctx context.Context, directives []l1.ComputerReimagePreflightDirective) error {
	if session.reimagePreflights == nil {
		return nil
	}
	var failures []error
	for _, directive := range directives {
		if err := session.reimagePreflights.process(ctx, directive); err != nil {
			failures = append(failures, fmt.Errorf("preflight Computer reimage %q: %w", directive.ComputerID, err))
		}
	}
	return errors.Join(failures...)
}

func (session *agentSession) processBackupDirectives(ctx context.Context, directives []l1.ComputerBackupDirective) error {
	if session.backups == nil {
		return nil
	}
	for _, directive := range directives {
		directive := directive
		session.backups.enqueue(ctx, "create\x00"+directive.CopyID,
			func(runContext context.Context) error { return session.backups.processCreate(runContext, directive) }, nil)
	}
	return nil
}

func (session *agentSession) processBackupPruneDirectives(ctx context.Context, directives []l1.ComputerBackupPruneDirective) error {
	if session.backups == nil {
		return nil
	}
	var failures []error
	for _, directive := range directives {
		if err := session.backups.processPrune(ctx, directive); err != nil {
			failures = append(failures, fmt.Errorf("reconcile Backup prune %q: %w", directive.CopyID, err))
		}
	}
	return errors.Join(failures...)
}

func (session *agentSession) processStorageCopyDirectives(ctx context.Context, directives []l1.ComputerStorageCopyDirective) error {
	if session.storageCopies == nil {
		return nil
	}
	for _, directive := range directives {
		session.storageCopies.enqueue(ctx, directive, nil)
	}
	return nil
}

func (session *agentSession) processCustodyExportDirectives(ctx context.Context, directives []l1.ComputerCustodyExportDirective) error {
	if session.custody == nil {
		return nil
	}
	for _, directive := range directives {
		session.custody.enqueue(ctx, directive, nil)
	}
	return nil
}

func (session *agentSession) reconcileOCIImagePins(ctx context.Context) error {
	if session.ociImagePins == nil {
		return nil
	}
	failures, err := session.ociImagePins.ReconcileOCIImagePins(ctx, func(proofContext context.Context, jobID string) (bool, error) {
		return session.client.ProveServiceBinding(proofContext, session.registration.NodeID, session.registration.BootSessionID, jobID)
	})
	var latchErrors []error
	for _, failure := range failures {
		if _, latchErr := session.client.LatchServiceImageReconciliationFailure(ctx, session.registration.NodeID, session.registration.BootSessionID, failure.JobID, failure.Failure); latchErr != nil {
			latchErrors = append(latchErrors, fmt.Errorf("latch service %q after image reconciliation failure: %w", failure.JobID, latchErr))
		}
	}
	if err != nil || len(failures) != 0 || len(latchErrors) != 0 {
		if invalidator, ok := session.ociBootBarrier.(ociBootBarrierInvalidator); ok {
			invalidator.Invalidate()
		}
		if len(failures) != 0 {
			latchErrors = append(latchErrors, fmt.Errorf("%d OCI service binding image deliveries failed", len(failures)))
		}
		return errors.Join(err, errors.Join(latchErrors...))
	}
	return nil
}

func (session *agentSession) publishRegistration(ctx context.Context) (l1.Node, error) {
	registration := applyCapabilityObservation(session.registration, session.capabilities.snapshot())
	registration.SupersedeCapabilityRevision = session.ociBootBarrier != nil
	node, err := session.client.Register(ctx, registration)
	if err == nil {
		session.capabilities.acknowledge(node)
		session.observeGrantedCapacity(node)
	}
	return node, err
}

func (session *agentSession) resumePendingRemovals(ctx context.Context) error {
	if session.removals != nil {
		if err := session.removals.resume(ctx); err != nil {
			session.capabilities.suppressOCI(contract.CapabilityReasonBootSweepFailed, err)
			return err
		}
	}
	return nil
}

// recoverOCIRuntime publishes a restrictive observation before reacquiring a
// lost helper generation, then performs removal resume, binding-pin
// reconciliation, and the functional probe. Ordinary healthy heartbeats do
// not scan removals.
func (session *agentSession) recoverOCIRuntime(ctx context.Context) (ocihelper.HelperSession, error) {
	return session.recoverOCIRuntimeValidated(ctx, nil)
}

func (session *agentSession) recoverOCIRuntimeValidated(ctx context.Context, validateIntent func() error) (ocihelper.HelperSession, error) {
	if err := lockMutexContext(ctx, &session.ociRecoveryMu); err != nil {
		return ocihelper.HelperSession{}, err
	}
	defer session.ociRecoveryMu.Unlock()
	if validateIntent != nil {
		if err := validateIntent(); err != nil {
			return ocihelper.HelperSession{}, err
		}
	}
	return session.recoverOCIRuntimeLocked(ctx)
}

func (session *agentSession) recoverOCIRuntimeAfterLoss(ctx context.Context, observed workloadrunner.RuntimeGeneration) error {
	if err := lockMutexContext(ctx, &session.ociRecoveryMu); err != nil {
		return err
	}
	defer session.ociRecoveryMu.Unlock()
	if observed.InstanceID != "" && observed.Generation != 0 && session.ociBootBarrier != nil {
		current, ready := session.ociBootBarrier.Generation()
		if ready && (current.HelperInstanceID != observed.InstanceID || current.SessionGeneration != observed.Generation) {
			return nil
		}
	}
	_, err := session.recoverOCIRuntimeLocked(ctx)
	return err
}

func lockMutexContext(ctx context.Context, mutex *sync.Mutex) error {
	if mutex.TryLock() {
		return nil
	}
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if mutex.TryLock() {
				return nil
			}
		}
	}
}

func (session *agentSession) recoverOCIRuntimeLocked(ctx context.Context) (ocihelper.HelperSession, error) {
	if session.ociBootBarrier == nil {
		return ocihelper.HelperSession{}, session.capabilities.refresh(ctx)
	}
	intentDisabled := session.capabilities.ociIntentDisabled.Load()
	if invalidator, ok := session.ociBootBarrier.(ociBootBarrierInvalidator); ok {
		invalidator.Invalidate()
	}
	if !intentDisabled {
		session.capabilities.suppressOCI(
			ociBootBarrierReason(session.ociBootBarrier),
			errors.New("OCI helper session requires a new boot sweep"),
		)
	}
	restrictiveResponse, err := session.publishCapabilityHeartbeatResponse(ctx, nil)
	if err != nil {
		return ocihelper.HelperSession{}, err
	}
	barrierErr := session.ociBootBarrier.Ensure(ctx)
	if barrierErr != nil && !intentDisabled {
		session.capabilities.suppressOCI(ociBootBarrierReason(session.ociBootBarrier), barrierErr)
	}
	removalErr := errors.Join(session.resumePendingRemovals(ctx),
		session.processRemovalDirectives(ctx, restrictiveResponse.RemovalDirectives),
		session.processStorageResetDirectives(ctx, restrictiveResponse.StorageResetDirectives),
		session.processStorageGrowDirectives(ctx, restrictiveResponse.StorageGrowDirectives),
		session.processReimageDirectives(ctx, restrictiveResponse.ReimageDirectives),
		session.processBackupDirectives(ctx, restrictiveResponse.BackupDirectives),
		session.processBackupPruneDirectives(ctx, restrictiveResponse.BackupPruneDirectives),
		session.processStorageCopyDirectives(ctx, restrictiveResponse.StorageCopyDirectives),
		session.processCustodyExportDirectives(ctx, restrictiveResponse.CustodyExportDirectives))
	if barrierErr != nil {
		return ocihelper.HelperSession{}, barrierErr
	}
	if removalErr != nil {
		return ocihelper.HelperSession{}, removalErr
	}
	if err := session.reconcileOCIImagePins(ctx); err != nil {
		return ocihelper.HelperSession{}, err
	}
	generation, ok := session.ociBootBarrier.Generation()
	if !ok {
		return ocihelper.HelperSession{}, errors.New("OCI helper session lost after boot sweep")
	}
	confirmed, confirmedOK := session.ociBootBarrier.Generation()
	if !confirmedOK || confirmed != generation {
		return ocihelper.HelperSession{}, errors.New("OCI helper session changed before functional probe")
	}
	if err := session.capabilities.refreshValidated(ctx, func() error {
		return session.validateOCIGeneration(generation)
	}); err != nil {
		return generation, err
	}
	if current, stillReady := session.ociBootBarrier.Generation(); !stillReady || current != generation {
		err := errors.New("OCI helper session changed before capability publication")
		session.capabilities.suppressOCI(contract.CapabilityReasonBootSweepFailed, err)
		return ocihelper.HelperSession{}, err
	}
	return generation, nil
}

func (session *agentSession) validateOCIGeneration(expected ocihelper.HelperSession) error {
	current, ok := session.ociBootBarrier.Generation()
	confirmed, confirmedOK := session.ociBootBarrier.Generation()
	if !ok || current != expected || !confirmedOK || confirmed != current {
		return errors.New("OCI helper session changed during functional probe")
	}
	return nil
}

func (session *agentSession) heartbeat(ctx context.Context) (l1.HeartbeatResponse, error) {
	observation := session.capabilities.snapshot()
	return session.client.Heartbeat(ctx, session.registration.NodeID, heartbeatRequest(session.registration.BootSessionID, observation))
}

func (session *agentSession) installComputerPolicy(ctx context.Context, snapshot *l1.ComputerPolicySnapshot) error {
	if snapshot == nil || session.computerPolicy == nil {
		return nil
	}
	receipt, err := session.computerPolicy.Install(*snapshot)
	if err != nil {
		return err
	}
	if session.computerAcks != nil {
		session.computerAcks.submit(receipt)
	}
	return nil
}

func (session *agentSession) publishCapabilityHeartbeat(ctx context.Context, pinned *ocihelper.HelperSession) (l1.Node, error) {
	response, err := session.publishCapabilityHeartbeatResponse(ctx, pinned)
	return response.Node, err
}

func (session *agentSession) publishCapabilityHeartbeatResponse(ctx context.Context, pinned *ocihelper.HelperSession) (l1.HeartbeatResponse, error) {
	session.capabilities.claimPublication.Lock()
	defer session.capabilities.claimPublication.Unlock()
	observation := session.capabilities.snapshot()
	if pinned != nil {
		current, ok := session.ociBootBarrier.Generation()
		confirmed, confirmedOK := session.ociBootBarrier.Generation()
		if !ok || current != *pinned || !confirmedOK || confirmed != current {
			lossErr := errors.New("OCI helper session changed before positive capability publication")
			session.capabilities.suppressOCILocked(contract.CapabilityReasonBootSweepFailed, lossErr)
			return l1.HeartbeatResponse{}, lossErr
		}
	}
	response, err := session.client.Heartbeat(
		ctx,
		session.registration.NodeID,
		heartbeatRequest(session.registration.BootSessionID, observation),
	)
	if err == nil && pinned != nil {
		current, ok := session.ociBootBarrier.Generation()
		if !ok || current != *pinned {
			lossErr := errors.New("OCI helper session changed during positive capability publication")
			session.capabilities.suppressOCILocked(contract.CapabilityReasonBootSweepFailed, lossErr)
			restrictive := session.capabilities.snapshot()
			restrictiveResponse, restrictiveErr := session.client.Heartbeat(
				ctx,
				session.registration.NodeID,
				heartbeatRequest(session.registration.BootSessionID, restrictive),
			)
			if restrictiveErr != nil {
				return restrictiveResponse, errors.Join(lossErr, restrictiveErr)
			}
			session.capabilities.acknowledge(restrictiveResponse.Node)
			return restrictiveResponse, lossErr
		}
	}
	if err == nil {
		session.capabilities.acknowledge(response.Node)
	}
	return response, err
}

func (session *agentSession) drain(ctx context.Context) (l1.Node, error) {
	session.observer.setSession(LifecycleDraining, 0, nil)
	session.drainOnce.Do(func() { close(session.drainRequested) })
	return session.client.Drain(ctx, session.registration.NodeID, session.registration.BootSessionID)
}

// run is process-lifetime supervision. Protocol failures are acted on at the
// narrowest destination; only local invariant failures escape after every
// resident attempt has returned.
func (session *agentSession) run(ctx context.Context, execute sessionAttemptExecution) error {
	runContext, stop := context.WithCancel(ctx)
	defer func() {
		stop()
		session.attempts.Wait()
		if session.removals != nil {
			session.removals.wait()
		}
		if session.backups != nil {
			session.backups.wait()
		}
		if session.storageResets != nil {
			session.storageResets.wait()
		}
		if session.storageCopies != nil {
			session.storageCopies.wait()
		}
		if session.storageGrows != nil {
			session.storageGrows.wait()
		}
		if session.reimagePreflights != nil {
			session.reimagePreflights.wait()
		}
		if session.custody != nil {
			session.custody.wait()
		}
	}()

	backoff := newSessionBackoff(DefaultSessionBackoffBase, DefaultSessionBackoffMax)
	state := LifecycleRegistering
	for {
		if runContext.Err() != nil {
			return nil
		}
		if session.isDraining() {
			session.observer.setSession(LifecycleDraining, 0, nil)
			return nil
		}
		session.observer.setSession(state, 0, nil)
		if _, err := session.register(runContext); err != nil {
			if runContext.Err() != nil {
				return nil
			}
			classification := classifyAgentProtocolError(err)
			switch classification.nodeSessionReaction {
			case nodeSessionDrain:
				session.observer.setSession(LifecycleDraining, 0, err)
				return nil
			case nodeSessionStopRecordAndEscalate:
				return session.quarantine(runContext, err)
			}
			if classification.destination != errorDestinationTransient && classification.nodeSessionReaction != nodeSessionReregister {
				return fmt.Errorf("agent: register node: %w", err)
			}
			delay := backoff.next()
			session.observer.setSession(LifecycleRejoining, delay, err)
			if err := session.waitWithoutHeartbeat(runContext, delay); err != nil {
				return nil
			}
			state = LifecycleRejoining
			continue
		}
		backoff.reset()
		session.markReady()
		if session.removals != nil && session.ociBootBarrier == nil {
			if err := session.removals.resume(runContext); err != nil && runContext.Err() == nil && session.logf != nil {
				session.logf("agent: resume service removals: %v", err)
			}
		}

		failure := session.serveRegistered(runContext, execute)
		if failure.err == nil {
			return nil
		}
		classification := classifyAgentProtocolError(failure.err)
		if failure.destination == errorDestinationNodeSession {
			classification.destination = failure.destination
		}
		switch classification.nodeSessionReaction {
		case nodeSessionReregister:
			state = LifecycleRejoining
			continue
		case nodeSessionDrain:
			session.observer.setSession(LifecycleDraining, 0, failure.err)
			return nil
		case nodeSessionStopRecordAndEscalate:
			return session.quarantine(runContext, failure.err)
		}
		if failure.destination == errorDestinationTransient {
			state = LifecycleRejoining
			continue
		}
		return failure.err
	}
}

func (session *agentSession) serveRegistered(ctx context.Context, execute sessionAttemptExecution) destinationError {
	sessionContext, stopSession := context.WithCancel(ctx)
	heartbeatErrors := make(chan destinationError, 1)
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		session.heartbeatLoop(sessionContext, heartbeatErrors)
	}()
	policyWatchDone := make(chan struct{})
	go func() {
		defer close(policyWatchDone)
		session.computerPolicyWatchLoop(sessionContext)
	}()
	policyAckDone := make(chan struct{})
	go func() {
		defer close(policyAckDone)
		session.computerAcks.run(sessionContext)
	}()
	type claimWorker struct {
		id       int
		pool     classPool
		retire   chan struct{}
		retiring bool
	}
	type workerResult struct {
		id      int
		failure destinationError
	}
	workers := make(map[int]*claimWorker)
	workerDone := make(chan workerResult)
	nextWorkerID := 1
	startWorker := func(pool classPool) {
		worker := &claimWorker{id: nextWorkerID, pool: pool, retire: make(chan struct{})}
		nextWorkerID++
		workers[worker.id] = worker
		go func() {
			workerDone <- workerResult{
				id: worker.id,
				failure: session.claimClassLoop(
					sessionContext, worker.retire, worker.pool.class, worker.pool.selector, execute,
				),
			}
		}()
	}
	reconcileWorkers := func() {
		for _, pool := range classPools {
			target := session.poolTarget(pool.class)
			var available []*claimWorker
			for _, worker := range workers {
				if worker.pool.class == pool.class && !worker.retiring {
					available = append(available, worker)
				}
			}
			for len(available) < target {
				startWorker(pool)
				available = append(available, workers[nextWorkerID-1])
			}
			// A service worker that already established a binding remains the
			// pull path for that reserved service across restart backoff. The
			// lowered gate and L1 transaction refuse newcomers; retaining idle
			// workers here prevents a capacity reduction from stranding a bound
			// service that L1 explicitly permits to restart while overcommitted.
			if pool.class == workloadClassService {
				continue
			}
			for len(available) > target {
				last := available[len(available)-1]
				available = available[:len(available)-1]
				last.retiring = true
				close(last.retire)
			}
		}
	}
	reconcileWorkers()
	defer func() {
		stopSession()
		<-heartbeatDone
		<-policyWatchDone
		<-policyAckDone
		for len(workers) > 0 {
			result := <-workerDone
			delete(workers, result.id)
		}
	}()

	drainRequested := session.drainRequested
	draining := false
	for {
		select {
		case <-sessionContext.Done():
			return destinationError{}
		case <-drainRequested:
			draining = true
			session.observer.setSession(LifecycleDraining, 0, nil)
			if session.logf != nil {
				session.logf("agent: draining; waiting for %d class claim loops and their resident attempts", len(workers))
			}
			drainRequested = nil
			if len(workers) == 0 {
				return destinationError{}
			}
		case result := <-workerDone:
			delete(workers, result.id)
			// A claim RPC already in flight can race the L1 drain mutation and
			// return node_draining before this select observes drainRequested.
			// Local admission is nevertheless already closed, so join the other
			// workers instead of letting that expected refusal cancel siblings.
			if session.isDraining() {
				draining = true
				session.observer.setSession(LifecycleDraining, 0, nil)
				if len(workers) == 0 {
					return destinationError{}
				}
				continue
			}
			if result.failure.err != nil {
				return result.failure
			}
			if draining {
				if len(workers) == 0 {
					return destinationError{}
				}
				continue
			}
			reconcileWorkers()
		case <-session.capacityChanged:
			if !draining {
				reconcileWorkers()
			}
		case failure := <-heartbeatErrors:
			return failure
		}
	}
}

func (session *agentSession) computerPolicyWatchLoop(ctx context.Context) {
	if session.computerPolicy == nil {
		return
	}
	backoff := newSessionBackoff(DefaultSessionBackoffBase, DefaultSessionBackoffMax)
	for ctx.Err() == nil {
		snapshot, err := session.client.WatchComputerPolicy(ctx, session.registration.NodeID,
			session.registration.BootSessionID, session.computerPolicy.Revision())
		if err == nil && snapshot != nil {
			err = session.installComputerPolicy(ctx, snapshot)
		}
		if err == nil {
			backoff.reset()
			continue
		}
		if ctx.Err() != nil {
			return
		}
		session.computerPolicy.Invalidate(ComputerPolicyWatchLost)
		if session.logf != nil {
			session.logf("agent: Computer policy watch failed closed: %v", err)
		}
		timer := session.clock.NewTimer(backoff.next())
		select {
		case <-ctx.Done():
			stopTimer(timer)
			return
		case <-timer.C():
		}
	}
}

// claimClassLoop owns one blocking claim-execute-wait path for one fixed
// workload class. The gate remains only a policy and occupancy counter; #85
// widens the number of loop instances after reading L1-granted capacity.
func (session *agentSession) claimClassLoop(
	ctx context.Context,
	retire <-chan struct{},
	gateKey workloadClass,
	selector string,
	execute sessionAttemptExecution,
) destinationError {
	backoff := newSessionBackoff(DefaultSessionBackoffBase, DefaultSessionBackoffMax)
	serviceReservation := false
	for {
		select {
		case <-ctx.Done():
			return destinationError{}
		case <-retire:
			return destinationError{}
		case <-session.drainRequested:
			session.observer.setSession(LifecycleDraining, 0, nil)
			return destinationError{}
		default:
		}

		claimStarted := session.clock.Now()
		if !session.claimsAllowed() {
			if err := session.waitForClaimWork(ctx, retire, session.claimInterval); err != nil {
				return destinationFromError(err)
			}
			continue
		}
		claim, admitted, err := session.claim(ctx, session.gates[gateKey], selector, serviceReservation)
		if !admitted {
			if err := session.waitForClaimWork(ctx, retire, session.claimInterval); err != nil {
				return destinationFromError(err)
			}
			continue
		}
		if err != nil {
			classification := classifyAgentProtocolError(err)
			failure := destinationError{
				destination: classification.destination,
				err:         fmt.Errorf("agent: claim %s job: %w", selector, err),
			}
			if failure.destination != errorDestinationTransient {
				return failure
			}
			delay := backoff.next()
			session.observer.setSession(LifecycleRejoining, delay, err)
			if waitErr := session.waitForClaimWork(ctx, retire, delay); waitErr != nil {
				return destinationFromError(waitErr)
			}
			continue
		}
		backoff.reset()
		session.markReady()
		if claim == nil {
			if err := session.waitForClaimWork(ctx, retire, session.claimInterval); err != nil {
				return destinationFromError(err)
			}
			continue
		}
		if gateKey == workloadClassService {
			serviceReservation = true
		}
		session.attempts.Add(1)
		destination, executeErr := session.executeResident(ctx, gateKey, *claim, claimStarted, execute)
		if executeErr != nil {
			if routed := session.routeError(destination, executeErr); routed != nil {
				return destinationFromError(routed)
			}
		}
	}
}

func (session *agentSession) executeResident(
	ctx context.Context,
	gateKey workloadClass,
	claim l1.Claim,
	claimStarted time.Time,
	execute sessionAttemptExecution,
) (destination errorDestination, executeErr error) {
	attemptContext, cancelAttempt := context.WithCancelCause(ctx)
	resident := &residentAttempt{
		class: claim.Job.Spec.Class, kind: claim.Job.Spec.Kind, cancel: cancelAttempt,
		done: make(chan struct{}), runtimeReaped: make(chan runtimeReapOutcome, 1),
	}
	session.claimMu.Lock()
	if claim.Job.Spec.Class == contract.JobClassService {
		delete(session.serviceReaps, claim.Job.JobID)
		session.serviceBoots[claim.Job.JobID] = session.registration.BootSessionID
	}
	session.resident[claim.Job.JobID] = resident
	session.notifyResidentChangedLocked()
	session.claimMu.Unlock()
	defer cancelAttempt(nil)
	defer session.attempts.Done()
	defer session.gates[gateKey].release()
	defer func() {
		session.claimMu.Lock()
		resident.completionErr = executeErr
		var persistenceErr *OCIIntentSuppressionPersistenceError
		if errors.As(executeErr, &persistenceErr) {
			session.residentSuppressionErrors[claim.Job.JobID] = executeErr
		}
		delete(session.resident, claim.Job.JobID)
		delete(session.residentJobID, claim.Job.JobID)
		delete(session.residentKind, claim.Job.JobID)
		close(resident.done)
		session.notifyResidentChangedLocked()
		session.claimMu.Unlock()
	}()
	return execute(attemptContext, claim, claimStarted)
}

var errOCIIntentDisabled = errors.New("OCI intent disabled by the node-local operator")

// stopOCIRuntime closes admission through the shared capability snapshot, then
// joins only resident OCI attempts. Process work remains available. Attempt
// completion owns front-door withdrawal and positive runtime quiescence, so the
// return is the ordering barrier before a Mac caller may stop Lima.
func (session *agentSession) stopOCIRuntime(ctx context.Context) error {
	if session == nil || session.capabilities == nil {
		return errors.New("agent: OCI runtime control is unavailable")
	}
	session.capabilities.suppressOCI(contract.CapabilityReasonOCIIntentDisabled, errOCIIntentDisabled)
	// The command must not wait on L1 reachability. Publish immediately when
	// possible, while the durable marker and local admission remain restrictive
	// even if this best-effort heartbeat fails.
	if session.client != nil {
		go func() {
			publishContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), DefaultOperationTimeout)
			defer cancel()
			if _, err := session.publishCapabilityHeartbeat(publishContext, nil); err != nil && session.logf != nil {
				session.logf("agent: publish OCI intent withdrawal: %v", err)
			}
		}()
	}
	if session.ociStopBeforeResidentScan != nil {
		session.ociStopBeforeResidentScan()
	}

	seen := make(map[string]struct{})
	var joinedErrors []error
	for {
		session.claimMu.Lock()
		pending := false
		var targets []struct {
			jobID    string
			resident *residentAttempt
		}
		for jobID, err := range session.residentSuppressionErrors {
			joinedErrors = append(joinedErrors, err)
			delete(session.residentSuppressionErrors, jobID)
		}
		for jobID, kind := range session.residentKind {
			if kind != contract.JobKindOCI {
				continue
			}
			resident, active := session.resident[jobID]
			if !active {
				pending = true
				continue
			}
			if _, already := seen[jobID]; !already {
				seen[jobID] = struct{}{}
				resident.cancel(errOCIIntentDisabled)
				targets = append(targets, struct {
					jobID    string
					resident *residentAttempt
				}{jobID: jobID, resident: resident})
			}
		}
		changed := session.residentChanged
		session.claimMu.Unlock()
		for _, target := range targets {
			resident := target.resident
			var outcome runtimeReapOutcome
			outcomePresent := true
			select {
			case <-ctx.Done():
				return errors.Join(append(joinedErrors, ctx.Err())...)
			case outcome = <-resident.runtimeReaped:
			case <-resident.done:
				select {
				case outcome = <-resident.runtimeReaped:
				default:
					outcomePresent = false
					joinedErrors = append(joinedErrors, errors.New("agent: OCI attempt completed without a runtime reap receipt"))
				}
			}
			if outcomePresent {
				if _, err := verifiedRuntimeReap("OCI attempt", outcome); err != nil {
					joinedErrors = append(joinedErrors, err)
				}
			}
			select {
			case <-ctx.Done():
				return errors.Join(append(joinedErrors, ctx.Err())...)
			case <-resident.done:
			}
			session.claimMu.Lock()
			completionErr := resident.completionErr
			delete(session.residentSuppressionErrors, target.jobID)
			session.claimMu.Unlock()
			var persistenceErr *OCIIntentSuppressionPersistenceError
			if errors.As(completionErr, &persistenceErr) {
				joinedErrors = append(joinedErrors, persistenceErr)
			}
		}
		if !pending {
			session.claimMu.Lock()
			remaining := false
			for jobID, kind := range session.residentKind {
				_, quiesced := seen[jobID]
				remaining = remaining || kind == contract.JobKindOCI && !quiesced
			}
			session.claimMu.Unlock()
			if !remaining {
				return errors.Join(joinedErrors...)
			}
		}
		select {
		case <-ctx.Done():
			return errors.Join(append(joinedErrors, ctx.Err())...)
		case <-changed:
		}
	}
}

func (session *agentSession) allowOCIIntentIfUnchanged(suppressionSequence uint64) error {
	// Keep the existing claimMu -> claimPublication lock order so no newly
	// admitted resident or later stop episode can publish an error between the
	// positive reopen and clearing failures owned by the prior disabled episode.
	session.claimMu.Lock()
	defer session.claimMu.Unlock()
	if err := session.capabilities.allowOCIIntentIfUnchanged(suppressionSequence); err != nil {
		return err
	}
	clear(session.residentSuppressionErrors)
	return nil
}

func (session *agentSession) heartbeatLoop(ctx context.Context, failures chan<- destinationError) {
	backoff := newSessionBackoff(DefaultSessionBackoffBase, DefaultSessionBackoffMax)
	nextDelay := session.heartbeatInterval
	for {
		timer := session.clock.NewTimer(nextDelay)
		select {
		case <-ctx.Done():
			stopTimer(timer)
			return
		case <-timer.C():
			var pinned *ocihelper.HelperSession
			if session.ociBootBarrier == nil {
				if err := session.capabilities.refresh(ctx); err != nil && session.logf != nil {
					session.logf("agent: capability probe before heartbeat: %v", err)
				}
			} else if generation, ready := session.ociBootBarrier.Generation(); ready {
				refreshErr := session.capabilities.refreshValidated(ctx, func() error {
					return session.validateOCIGeneration(generation)
				})
				if refreshErr == nil {
					pinned = &generation
				} else if !capabilityProbeWasSkipped(refreshErr) && session.logf != nil {
					session.logf("agent: OCI capability probe before heartbeat: %v", refreshErr)
				}
			} else {
				generation, recoverErr := session.recoverOCIRuntime(ctx)
				if recoverErr != nil {
					if !capabilityProbeWasSkipped(recoverErr) && session.logf != nil {
						session.logf("agent: OCI barrier recovery before heartbeat: %v", recoverErr)
					}
				} else {
					pinned = &generation
				}
			}
			response, err := session.publishCapabilityHeartbeatResponse(ctx, pinned)
			if err != nil {
				if pinned != nil {
					session.capabilities.suppressOCI(contract.CapabilityReasonBootSweepFailed, err)
				}
				classification := classifyAgentProtocolError(err)
				if classification.destination == errorDestinationTransient {
					nextDelay = backoff.next()
					session.observer.setSession(LifecycleRejoining, nextDelay, err)
					continue
				}
				select {
				case failures <- destinationError{destination: classification.destination, err: fmt.Errorf("agent: heartbeat: %w", err)}:
				default:
				}
				return
			}
			if session.computerPolicy != nil && !session.computerPolicy.Valid() && response.ComputerPolicy != nil {
				if policyErr := session.installComputerPolicy(ctx, response.ComputerPolicy); policyErr != nil {
					session.computerPolicy.Invalidate(ComputerPolicyWatchLost)
					if session.logf != nil {
						session.logf("agent: Computer policy heartbeat bootstrap failed closed: %v", policyErr)
					}
				}
			}
			session.observeGrantedCapacity(response.Node)
			if session.removals != nil {
				for _, directive := range response.RemovalDirectives {
					session.removals.enqueue(ctx, directive, failures)
				}
			}
			if session.storageResets != nil {
				for _, directive := range response.StorageResetDirectives {
					session.storageResets.enqueue(ctx, directive, failures)
				}
			}
			if session.storageGrows != nil {
				for _, directive := range response.StorageGrowDirectives {
					session.storageGrows.enqueue(ctx, directive, failures)
				}
			}
			if session.reimagePreflights != nil {
				for _, directive := range response.ReimageDirectives {
					session.reimagePreflights.enqueue(ctx, directive, failures)
				}
			}
			if session.backups != nil {
				for _, directive := range response.BackupDirectives {
					directive := directive
					session.backups.enqueue(ctx, "create\x00"+directive.CopyID,
						func(runContext context.Context) error { return session.backups.processCreate(runContext, directive) }, failures)
				}
				for _, directive := range response.BackupPruneDirectives {
					directive := directive
					session.backups.enqueue(ctx, "prune\x00"+directive.CopyID,
						func(runContext context.Context) error { return session.backups.processPrune(runContext, directive) }, failures)
				}
			}
			if session.storageCopies != nil {
				for _, directive := range response.StorageCopyDirectives {
					session.storageCopies.enqueue(ctx, directive, failures)
				}
			}
			if session.custody != nil {
				for _, directive := range response.CustodyExportDirectives {
					session.custody.enqueue(ctx, directive, failures)
				}
			}
			backoff.reset()
			nextDelay = session.heartbeatInterval
			session.markReady()
		}
	}
}

func (session *agentSession) claim(
	ctx context.Context,
	gate classAdmissionGate,
	selector string,
	serviceReservation bool,
) (*l1.Claim, bool, error) {
	// Serializing the claim snapshot with the winning response closes the only
	// local race: a second worker cannot send a stale exclusion set after L1
	// has requeued a job but before the first worker finishes local finalization.
	session.claimMu.Lock()
	defer session.claimMu.Unlock()
	endCapabilityClaim := session.capabilities.beginClaim()
	defer endCapabilityClaim()
	if !session.capabilities.allowsClaim() {
		return nil, false, nil
	}
	if !gate.canAcquire() && !serviceReservation {
		return nil, false, nil
	}
	excluded := make([]string, 0, len(session.residentJobID))
	for jobID := range session.residentJobID {
		excluded = append(excluded, jobID)
	}
	claim, err := session.client.Claim(
		ctx, session.registration.NodeID, session.registration.BootSessionID, selector, excluded...,
	)
	if err != nil || claim == nil {
		return claim, true, err
	}
	if _, exists := session.residentJobID[claim.Job.JobID]; exists {
		return nil, true, fmt.Errorf("agent: L1 returned locally resident job %q despite exclusion", claim.Job.JobID)
	}
	if !gate.tryAcquire() {
		if !serviceReservation || selector != contract.JobClassService {
			return nil, true, fmt.Errorf("agent: %s admission changed while claiming job %q", selector, claim.Job.JobID)
		}
		// L1 admits a service above the current limit only when its existing
		// binding already holds the slot. Reflect that execution locally
		// without treating the gate as the authority for service placement.
		gate.acquireReserved()
	}
	session.residentJobID[claim.Job.JobID] = struct{}{}
	session.residentKind[claim.Job.JobID] = claim.Job.Spec.Kind
	return claim, true, nil
}

func (session *agentSession) reapServiceForRemoval(ctx context.Context, jobID, kind string, attempts []workloadrunner.RuntimeResourceManifest) (workloadrunner.ReapReceipt, error) {
	for {
		session.claimMu.Lock()
		resident, active := session.resident[jobID]
		_, admitted := session.residentJobID[jobID]
		if active {
			if resident.class != contract.JobClassService {
				session.claimMu.Unlock()
				return workloadrunner.ReapReceipt{}, fmt.Errorf("agent: removal target %q is not a resident service", jobID)
			}
			if resident.kind != kind {
				session.claimMu.Unlock()
				return workloadrunner.ReapReceipt{}, fmt.Errorf("agent: removal target %q kind changed from %q to %q", jobID, resident.kind, kind)
			}
			resident.cancel(errServiceRemovalRequested)
			done := resident.done
			reaped := resident.runtimeReaped
			session.claimMu.Unlock()
			select {
			case <-ctx.Done():
				return workloadrunner.ReapReceipt{}, ctx.Err()
			case outcome := <-reaped:
				select {
				case <-ctx.Done():
					return workloadrunner.ReapReceipt{}, ctx.Err()
				case <-done:
				}
				return verifiedRuntimeReap(jobID, outcome)
			case <-done:
				continue
			}
		}
		if !admitted {
			outcome, found := session.serviceReaps[jobID]
			serviceBoot := session.serviceBoots[jobID]
			priorBootReap := session.reapPriorBoot
			bootSessionID := session.registration.BootSessionID
			session.claimMu.Unlock()
			if found {
				return verifiedRuntimeReap(jobID, outcome)
			}
			// Only after the admission and residency gates are both clear may a
			// current-helper Storage-only inventory prove that no guardian exists.
			if receipt, ok := storageOnlyAttemptsReceipt(jobID, bootSessionID, attempts); ok {
				return receipt, nil
			}
			if serviceBoot == session.registration.BootSessionID || priorBootReap == nil {
				return workloadrunner.ReapReceipt{}, fmt.Errorf("agent: service %q has no runtime reap receipt", jobID)
			}
			receipt, err := priorBootReap(ctx, jobID, kind, attempts)
			return verifiedRuntimeReap(jobID, runtimeReapOutcome{receipt: receipt, err: err})
		}
		changed := session.residentChanged
		session.claimMu.Unlock()
		select {
		case <-ctx.Done():
			return workloadrunner.ReapReceipt{}, ctx.Err()
		case <-changed:
		}
	}
}

func storageOnlyAttemptsReceipt(jobID, bootSessionID string, attempts []workloadrunner.RuntimeResourceManifest) (workloadrunner.ReapReceipt, bool) {
	if jobID == "" || bootSessionID == "" || len(attempts) == 0 {
		return workloadrunner.ReapReceipt{}, false
	}
	for _, attempt := range attempts {
		if !attempt.StorageOnly || attempt.NodeID == "" || attempt.BootSessionID != bootSessionID || attempt.JobID != jobID ||
			attempt.WorkloadClass != contract.JobClassService || attempt.ComputerStorage == nil ||
			(!attempt.StorageAbsent && (attempt.StoragePreparation == nil || !attempt.StoragePreparation.Valid() ||
				!contract.ValidStorageOnlyRemovalAttemptID(attempt.AttemptID, attempt.ComputerStorage.StorageGeneration))) ||
			(attempt.StorageAbsent && (attempt.StoragePreparation != nil ||
				!contract.ValidStorageAbsentRemovalAttemptID(attempt.AttemptID, attempt.ComputerStorage.StorageGeneration))) {
			return workloadrunner.ReapReceipt{}, false
		}
	}
	return workloadrunner.ReapReceipt{RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidenceNoRuntime, BootSessionID: bootSessionID}, true
}

func (session *agentSession) recordRuntimeReap(jobID string, receipt workloadrunner.ReapReceipt, err error) {
	session.claimMu.Lock()
	defer session.claimMu.Unlock()
	outcome := runtimeReapOutcome{receipt: receipt, err: err}
	resident, active := session.resident[jobID]
	if active {
		select {
		case resident.runtimeReaped <- outcome:
		default:
		}
	}
	if active && resident.class == contract.JobClassService {
		if session.serviceReaps == nil {
			session.serviceReaps = make(map[string]runtimeReapOutcome)
		}
		session.serviceReaps[jobID] = outcome
	}
}

func (session *agentSession) clearRuntimeReap(jobID string) {
	session.claimMu.Lock()
	delete(session.serviceReaps, jobID)
	delete(session.serviceBoots, jobID)
	session.claimMu.Unlock()
}

func verifiedRuntimeReap(jobID string, outcome runtimeReapOutcome) (workloadrunner.ReapReceipt, error) {
	if outcome.err != nil {
		return workloadrunner.ReapReceipt{}, fmt.Errorf("agent: service %q runtime reap: %w", jobID, outcome.err)
	}
	if !outcome.receipt.RuntimeQuiesced {
		return workloadrunner.ReapReceipt{}, fmt.Errorf("agent: service %q runtime did not verify quiescence", jobID)
	}
	if outcome.receipt.Evidence == "" {
		return workloadrunner.ReapReceipt{}, fmt.Errorf("agent: service %q runtime receipt has no evidence kind", jobID)
	}
	return outcome.receipt, nil
}

func (session *agentSession) notifyResidentChangedLocked() {
	select {
	case session.residentChanged <- struct{}{}:
	default:
	}
}

func (session *agentSession) observeGrantedCapacity(node l1.Node) {
	targets := map[workloadClass]int{
		workloadClassOneShot: min(session.localLimits[workloadClassOneShot], node.MaxOneshotSlots),
		workloadClassService: min(session.localLimits[workloadClassService], node.MaxServiceSlots),
	}
	session.claimMu.Lock()
	defer session.claimMu.Unlock()
	session.capacityMu.Lock()
	session.claimsEnabled = node.ClaimsEnabled
	changed := false
	for class, target := range targets {
		if session.poolTargets[class] != target {
			session.poolTargets[class] = target
			session.gates[class].setLimit(target)
			changed = true
		}
	}
	session.capacityMu.Unlock()
	if changed {
		select {
		case session.capacityChanged <- struct{}{}:
		default:
		}
	}
}

func (session *agentSession) claimsAllowed() bool {
	session.capacityMu.Lock()
	defer session.capacityMu.Unlock()
	return session.claimsEnabled && !session.isDraining()
}

func (session *agentSession) poolTarget(class workloadClass) int {
	session.capacityMu.Lock()
	defer session.capacityMu.Unlock()
	return session.poolTargets[class]
}

func (session *agentSession) waitForClaimWork(ctx context.Context, retire <-chan struct{}, duration time.Duration) error {
	timer := session.clock.NewTimer(duration)
	defer stopTimer(timer)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-retire:
		return context.Canceled
	case <-session.drainRequested:
		return context.Canceled
	case <-timer.C():
		return nil
	}
}

func (session *agentSession) waitWithoutHeartbeat(ctx context.Context, duration time.Duration) error {
	timer := session.clock.NewTimer(duration)
	defer stopTimer(timer)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-session.drainRequested:
		return context.Canceled
	case <-timer.C():
		return nil
	}
}

func (session *agentSession) quarantine(ctx context.Context, err error) error {
	session.observer.setSession(LifecycleQuarantined, DefaultSessionBackoffMax, err)
	select {
	case <-ctx.Done():
		return nil
	case <-session.drainRequested:
		session.observer.setSession(LifecycleDraining, 0, nil)
		return nil
	}
}

func (session *agentSession) isDraining() bool {
	select {
	case <-session.drainRequested:
		return true
	default:
		return false
	}
}

func (session *agentSession) markReady() {
	if session.isDraining() {
		session.observer.setSession(LifecycleDraining, 0, nil)
		return
	}
	session.observer.setSession(LifecycleReady, 0, nil)
}

func destinationFromError(err error) destinationError {
	if err == nil || errors.Is(err, context.Canceled) {
		return destinationError{}
	}
	var routed *routedDestinationError
	if errors.As(err, &routed) {
		return destinationError{destination: routed.destination, err: routed.err}
	}
	return destinationError{destination: errorDestinationUnclassified, err: err}
}

func workloadClassFor(class string) workloadClass {
	if class == contract.JobClassService {
		return workloadClassService
	}
	return workloadClassOneShot
}

type sessionBackoff struct {
	base    time.Duration
	maximum time.Duration
	current time.Duration
}

func newSessionBackoff(base, maximum time.Duration) *sessionBackoff {
	return &sessionBackoff{base: base, maximum: maximum}
}

func (backoff *sessionBackoff) next() time.Duration {
	if backoff.current == 0 {
		backoff.current = backoff.base
	} else {
		backoff.current = min(backoff.current*2, backoff.maximum)
	}
	half := backoff.current / 2
	if half <= 0 {
		return backoff.current
	}
	return half + time.Duration(rand.Int64N(int64(backoff.current-half)+1))
}

func (backoff *sessionBackoff) reset() { backoff.current = 0 }
