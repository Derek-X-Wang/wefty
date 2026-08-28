// Package agent implements the long-running wefty node agent.
package agent

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"runtime"
	"sync"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
	processrunner "github.com/Derek-X-Wang/wefty/runner/process"
)

const (
	DefaultHeartbeatInterval       = 15 * time.Second
	DefaultCapabilityProbeTimeout  = 10 * time.Second
	DefaultClaimInterval           = time.Second
	DefaultRenewalInterval         = 10 * time.Second
	DefaultHandoffRetention        = 24 * time.Hour
	DefaultLogBatchSize            = 32
	DefaultLogFlushInterval        = 100 * time.Millisecond
	DefaultLogRetryInterval        = 100 * time.Millisecond
	DefaultLogSpoolMaxBytes        = 64 << 20
	DefaultServiceLogSpoolMaxBytes = 32 << 20
)

// OutputSinkFactory creates an optional local output destination for a claimed
// attempt. It may be invoked concurrently and must return an attempt-isolated
// sink or a sink that internally synchronizes shared state. The agent always
// uploads logs to L1 in addition to this sink.
type OutputSinkFactory func(l1.Claim) processrunner.OutputSink

// Config contains the stable node identity, per-process boot identity, and
// independent heartbeat, claim, and lease-renewal cadences.
type Config struct {
	Fabric              fabric.Fabric
	ControlPlaneAddress string
	RunLedgerAddress    string
	NodeID              string
	BootSessionID       string
	Version             string
	OS                  string
	Architecture        string
	// Capabilities is the complete advertised execution set; nil advertises none.
	Capabilities map[string]bool
	// CapabilityProbe earns and continuously revalidates OCI-related
	// capabilities. The real runtime adapter is supplied by later M3 tickets.
	CapabilityProbe CapabilityProbe
	// CapabilityProbeTimeout bounds one functional probe. Zero uses ten seconds.
	CapabilityProbeTimeout time.Duration
	// OCIBootBarrier must prove exclusive sweep and namespace absence before
	// the functional probe can earn OCI capability publication.
	OCIBootBarrier       OCIBootBarrier
	HeartbeatInterval    time.Duration
	ClaimInterval        time.Duration
	RenewalInterval      time.Duration
	MaxOneshotSlots      int
	MaxServiceSlots      int
	OperationTimeout     time.Duration
	FinalizationTimeout  time.Duration
	LogBatchSize         int
	LogFlushInterval     time.Duration
	LogRetryInterval     time.Duration
	LogSpoolDirectory    string
	LogSpoolMaxBytes     int64
	ManagedRootDirectory string
	GuardianExecutable   string
	// WorkloadRuntimes supplies open kind adapters. kind=process is installed
	// by default and may be replaced through this map. A capability may be
	// advertised without a matching local adapter, in which case local
	// admission fails closed.
	WorkloadRuntimes map[string]WorkloadRuntime
	// AttemptDeadman queues successful OCI L1 renewals to the helper session.
	// Nil is the no-helper-session mode.
	AttemptDeadman AttemptDeadmanRenewer
	// OCIWorkflowBridgeBinder binds the guest-visible host bridge for Lima.
	// Nil preserves the native loopback bridge used by process and Linux OCI.
	OCIWorkflowBridgeBinder workloadrunner.WorkflowBridgeBinder
	OutputSinkFactory       OutputSinkFactory
	HandoffRoot             string
	HandoffRetention        time.Duration
	// Logf need not be goroutine-safe. Agent serializes calls made through it.
	Logf  func(string, ...any)
	Clock Clock
}

// Agent owns process-lifetime resources and starts one control-plane session.
type Agent struct {
	fabric              fabric.Fabric
	runLedgerAddr       string
	registration        contract.NodeRegistration
	renewalInterval     time.Duration
	finalizationTimeout time.Duration
	logRetryInterval    time.Duration
	session             *agentSession
	outbox              *evidenceOutbox
	// logSpool is a compatibility view used by existing package tests. The
	// process-lifetime evidenceOutbox is its sole owner.
	logSpool          *logSpool
	runtimes          workloadRuntimeSet
	managedResource   managedResourceManager
	outputSinkFactory OutputSinkFactory
	handoffs          *handoffManager
	logf              func(string, ...any)
	clock             Clock
	observer          *lifecycleObserver
	capabilities      *capabilityState
	attemptDeadman    AttemptDeadmanRenewer
	ociBridgeBinder   workloadrunner.WorkflowBridgeBinder
	nodeLock          nodeLock
}

func New(config Config) (*Agent, error) {
	if config.NodeID == "" {
		return nil, errors.New("agent: stable node ID is required")
	}
	if config.BootSessionID == "" {
		return nil, errors.New("agent: boot session ID is required")
	}
	if config.Version == "" {
		return nil, errors.New("agent: version is required")
	}
	if config.LogBatchSize < 0 || config.LogBatchSize > l1.MaxLogBatchEvents {
		return nil, fmt.Errorf("agent: log batch size must be between 1 and %d", l1.MaxLogBatchEvents)
	}
	if config.LogSpoolMaxBytes < 0 {
		return nil, errors.New("agent: log spool maximum bytes cannot be negative")
	}
	if config.MaxOneshotSlots < 0 || config.MaxServiceSlots < 0 {
		return nil, errors.New("agent: local slot limits cannot be negative")
	}
	if config.CapabilityProbe != nil && config.OCIBootBarrier == nil {
		return nil, errors.New("agent: OCI capability probe requires a boot barrier")
	}
	osName := config.OS
	if osName == "" {
		osName = runtime.GOOS
	}
	architecture := config.Architecture
	if architecture == "" {
		architecture = runtime.GOARCH
	}
	clock := config.Clock
	if clock == nil {
		clock = systemClock{}
	}
	client, err := newClient(config.Fabric, config.ControlPlaneAddress, durationOrDefault(config.OperationTimeout, DefaultOperationTimeout))
	if err != nil {
		return nil, err
	}
	runtimes, err := newWorkloadRuntimeSet(config.WorkloadRuntimes)
	if err != nil {
		client.Close()
		return nil, err
	}
	if _, configured := runtimes.selectKind(contract.JobKindProcess); !configured {
		runtimes[contract.JobKindProcess] = processrunner.NewAdapterForBoot(processrunner.New(processrunner.Config{
			Clock: processClockAdapter{clock: clock}, GuardianExecutable: config.GuardianExecutable,
		}), config.BootSessionID)
	}
	managedResource, err := initializeManagedResource(config.ManagedRootDirectory, config.NodeID, config.BootSessionID)
	if err != nil {
		client.Close()
		return nil, err
	}
	registration := contract.NodeRegistration{
		NodeID:        config.NodeID,
		BootSessionID: config.BootSessionID,
		ConnectHost:   config.Fabric.ConnectHost(),
		OS:            osName,
		Architecture:  architecture,
		AgentVersion:  config.Version,
		Capabilities:  cloneCapabilities(config.Capabilities),
	}
	if managedResource != nil {
		registration.RootInstanceID = managedResource.rootInstanceID()
	}
	heartbeatInterval := durationOrDefault(config.HeartbeatInterval, DefaultHeartbeatInterval)
	claimInterval := durationOrDefault(config.ClaimInterval, DefaultClaimInterval)
	logBatchSize := intOrDefault(config.LogBatchSize, DefaultLogBatchSize)
	logFlushInterval := durationOrDefault(config.LogFlushInterval, DefaultLogFlushInterval)
	logRetryInterval := durationOrDefault(config.LogRetryInterval, DefaultLogRetryInterval)
	logSpoolDirectory, err := resolveLogSpoolDirectory(config.LogSpoolDirectory)
	if err != nil {
		client.Close()
		return nil, err
	}
	stableNodeLock, err := acquireNodeLock(logSpoolDirectory, config.NodeID)
	if err != nil {
		client.Close()
		return nil, err
	}
	outbox, err := newEvidenceOutbox(
		logSpoolDirectory,
		config.NodeID,
		int64OrDefault(config.LogSpoolMaxBytes, DefaultLogSpoolMaxBytes),
		clock,
		logBatchSize,
		logFlushInterval,
		logRetryInterval,
	)
	if err != nil {
		_ = stableNodeLock.Close()
		client.Close()
		return nil, err
	}
	var ociImagePins workloadrunner.OCIImagePinRuntime
	var managedVolumeFinalizer workloadrunner.ManagedVolumeFinalizer
	var ociRemovalProof workloadrunner.RuntimeRemovalProofRuntime
	var computerStorageResetter workloadrunner.ComputerStorageResetter
	if runtimeAdapter, configured := runtimes.selectKind(contract.JobKindOCI); configured {
		if pinRuntime, supported := runtimeAdapter.(workloadrunner.OCIImagePinRuntime); supported {
			pinRuntime.SetOCIImageBindingPinLedger(outbox.spool)
			ociImagePins = pinRuntime
		}
		managedVolumeFinalizer, _ = runtimeAdapter.(workloadrunner.ManagedVolumeFinalizer)
		if proofRuntime, supported := runtimeAdapter.(workloadrunner.RuntimeRemovalProofRuntime); supported {
			ociRemovalProof = proofRuntime
		}
		computerStorageResetter, _ = runtimeAdapter.(workloadrunner.ComputerStorageResetter)
	}
	observer := newLifecycleObserver(clock)
	logf := serialLogf(config.Logf)
	capabilities := newCapabilityState(config.Capabilities, config.CapabilityProbe, clock, config.CapabilityProbeTimeout)
	registration = applyCapabilityObservation(registration, capabilities.snapshot())
	session := newAgentSession(
		client, registration, capabilities, heartbeatInterval, claimInterval, clock, observer, logf,
		intOrDefault(config.MaxOneshotSlots, l1.DefaultMaxOneshotSlots),
		intOrDefault(config.MaxServiceSlots, l1.DefaultMaxServiceSlots),
	)
	session.ociBootBarrier = config.OCIBootBarrier
	session.ociImagePins = ociImagePins
	if session.ociBootBarrier != nil {
		session.ociBootBarrier.SetLossHandler(func(_ ocihelper.HelperSession, lossErr error) {
			capabilities.suppressOCI(ociBootBarrierReason(session.ociBootBarrier), lossErr)
		})
	}
	if resource, ok := managedResource.(*processManagedResource); ok && resource.previousBootSessionID != "" {
		var priorBootReapers []workloadrunner.PriorBootReaper
		for _, kind := range []string{contract.JobKindOCI, contract.JobKindProcess} {
			if adapter, found := runtimes.selectKind(kind); found {
				if reaper, supported := adapter.(workloadrunner.PriorBootReaper); supported {
					priorBootReapers = append(priorBootReapers, reaper)
				}
			}
		}
		if len(priorBootReapers) > 0 {
			session.reapPriorBoot = func(ctx context.Context, jobID string) (workloadrunner.ReapReceipt, error) {
				request := workloadrunner.PriorBootReapRequest{NodeID: config.NodeID, JobID: jobID, PriorBootSessionID: resource.previousBootSessionID, CurrentBootSessionID: config.BootSessionID}
				var failures []error
				for _, reaper := range priorBootReapers {
					receipt, reapErr := reaper.ReapPriorBoot(ctx, request)
					if reapErr == nil {
						return receipt, nil
					}
					if !errors.Is(reapErr, workloadrunner.ErrPriorBootEvidenceUnavailable) {
						failures = append(failures, reapErr)
					}
				}
				if len(failures) > 0 {
					return workloadrunner.ReapReceipt{}, errors.Join(failures...)
				}
				return workloadrunner.ReapReceipt{}, workloadrunner.ErrPriorBootEvidenceUnavailable
			}
		}
	}
	session.removals = newRemovalController(
		client, outbox, managedResource, session,
		config.NodeID, config.BootSessionID, logf,
	)
	if ociImagePins != nil {
		session.removals.releaseImagePin = ociImagePins.ReleaseOCIImageBindingPin
	}
	if managedVolumeFinalizer != nil {
		session.removals.finalizeVolumes = managedVolumeFinalizer.FinalizeManagedVolumes
	}
	if ociRemovalProof != nil {
		session.removals.reconstructRuntime = ociRemovalProof.ReconstructRuntimeRemoval
		session.removals.deleteRuntimeData = ociRemovalProof.DeleteRuntimeRemovalData
		session.removals.attestRuntimeRemoval = ociRemovalProof.AttestRuntimeRemoval
	}
	session.storageResets = newStorageResetController(client, session, computerStorageResetter,
		config.NodeID, config.BootSessionID, logf)
	return &Agent{
		fabric: config.Fabric, runLedgerAddr: stringOrDefault(config.RunLedgerAddress, "wefty://run-ledger"),
		registration: registration, renewalInterval: durationOrDefault(config.RenewalInterval, DefaultRenewalInterval),
		finalizationTimeout: durationOrDefault(config.FinalizationTimeout, DefaultFinalizationTimeout),
		logRetryInterval:    logRetryInterval, session: session, outbox: outbox, logSpool: outbox.spool,
		runtimes: runtimes, managedResource: managedResource, outputSinkFactory: config.OutputSinkFactory,
		handoffs: newHandoffManager(config.HandoffRoot, durationOrDefault(config.HandoffRetention, DefaultHandoffRetention)),
		logf:     logf, clock: clock, observer: observer, capabilities: capabilities,
		attemptDeadman:  config.AttemptDeadman,
		ociBridgeBinder: config.OCIWorkflowBridgeBinder,
		nodeLock:        stableNodeLock,
	}, nil
}

type processClockAdapter struct{ clock Clock }

func (adapter processClockAdapter) Now() time.Time { return adapter.clock.Now() }
func (adapter processClockAdapter) NewTimer(duration time.Duration) processrunner.Timer {
	return adapter.clock.NewTimer(duration)
}

// Close releases idle protocol connections.
func (a *Agent) Close() {
	if a.session != nil {
		a.session.close()
	}
	if a.outbox != nil {
		if err := a.outbox.Close(); err != nil {
			a.log("close durable log spool: %v", err)
		}
	}
	if a.nodeLock != nil {
		if err := a.nodeLock.Close(); err != nil {
			a.log("release stable-node lock: %v", err)
		}
	}
	if a.session != nil {
		if a.session.ociBootBarrier != nil {
			if err := a.session.ociBootBarrier.Close(); err != nil {
				a.log("close OCI boot barrier: %v", err)
			}
		}
	}
}

// Register starts or replaces the stable node's current boot session.
func (a *Agent) Register(ctx context.Context) (l1.Node, error) {
	return a.session.register(ctx)
}

// Drain stops new claims locally and marks the current boot session draining
// at the control plane. Attempts already executing are not canceled; Run
// returns after both class loops finish their resident attempt, if any.
func (a *Agent) Drain(ctx context.Context) (l1.Node, error) {
	return a.session.drain(ctx)
}

// Run registers and then serves claims until the context is canceled or the
// control plane rejects a liveness or execution operation.
func (a *Agent) Run(ctx context.Context) error {
	if a.handoffs != nil {
		if err := a.handoffs.cleanupExpired(""); err != nil {
			return fmt.Errorf("agent: clean expired handoff directories: %w", err)
		}
	}
	if a.outbox != nil && a.session != nil {
		a.outbox.startRecovery(ctx, a.session.client, func(err error) {
			a.log("recover durable evidence: %v", err)
		})
	}
	return a.session.run(ctx, func(attemptContext context.Context, claim l1.Claim, claimStarted time.Time) (errorDestination, error) {
		return a.executeClaim(attemptContext, claim, claimStarted)
	})
}

func (a *Agent) recoverPendingLogs(ctx context.Context) error {
	if a.outbox == nil || a.session == nil {
		return nil
	}
	return a.outbox.recover(ctx, a.session.client)
}

func (a *Agent) executeClaim(ctx context.Context, claim l1.Claim, claimStarted time.Time) (errorDestination, error) {
	return a.newAttemptLifecycle().execute(ctx, claim, claimStarted)
}

func (a *Agent) runWorkload(ctx context.Context, claim l1.Claim) (contract.ProcessResult, error) {
	return a.newAttemptLifecycle().runWorkload(ctx, claim)
}

func (a *Agent) newAttemptLifecycle() *attemptLifecycle {
	var allowsStart func(contract.JobSpec) bool
	if a.capabilities != nil {
		allowsStart = a.capabilities.allows
	}
	return newAttemptLifecycle(attemptLifecycleDependencies{
		client: a.sessionClient(), runtimes: a.runtimes, outbox: a.outbox,
		watchdog: newAuthorityWatchdog(a.clock), clock: a.clock,
		renewalInterval: a.renewalInterval, completionRetry: a.logRetryInterval,
		finalizationTimeout: a.finalizationTimeout,
		outputSinkFactory:   a.outputSinkFactory, handoffs: a.handoffs,
		managedResource: a.managedResource,
		nodeID:          a.registration.NodeID, bootSessionID: a.registration.BootSessionID,
		workflowBridge: a.startWorkflowBridge, logf: a.logf,
		observer: a.observer, reservePublishedPort: a.reservePublishedPort,
		prepareServiceEndpoint: prepareProcessServiceEndpoint,
		prepareAuthorityLoss:   a.prepareAuthorityLoss,
		allowsStart:            allowsStart,
		currentOCIGeneration:   a.currentOCIRuntimeGeneration,
		embargoOCIRuntime:      a.embargoOCIRuntimeLoss,
		recoverOCIRuntime:      a.recoverOCIRuntimeAfterLoss,
		runtimeReaped:          a.recordRuntimeReap,
		attemptDeadman:         a.attemptDeadman,
	})
}

func (a *Agent) currentOCIRuntimeGeneration() (workloadrunner.RuntimeGeneration, bool) {
	if a.session == nil || a.session.ociBootBarrier == nil {
		return workloadrunner.RuntimeGeneration{}, false
	}
	generation, ok := a.session.ociBootBarrier.Generation()
	return workloadrunner.RuntimeGeneration{InstanceID: generation.HelperInstanceID, Generation: generation.SessionGeneration}, ok
}

func (a *Agent) embargoOCIRuntimeLoss(_ workloadrunner.RuntimeGeneration) {
	capabilities := a.capabilities
	if capabilities == nil && a.session != nil {
		capabilities = a.session.capabilities
	}
	if capabilities != nil {
		capabilities.suppressOCI(contract.CapabilityReasonBootSweepFailed, errors.New("OCI helper runtime loss requires a new boot sweep"))
	}
}

func (a *Agent) recoverOCIRuntimeAfterLoss(ctx context.Context, observed workloadrunner.RuntimeGeneration) error {
	if a.session == nil {
		return errors.New("OCI runtime recovery requires an agent session")
	}
	return a.session.recoverOCIRuntimeAfterLoss(ctx, observed)
}

func (a *Agent) recordRuntimeReap(jobID string, receipt workloadrunner.ReapReceipt, err error) {
	if a.session != nil {
		a.session.recordRuntimeReap(jobID, receipt, err)
	}
}

func (a *Agent) prepareAuthorityLoss(ctx context.Context, jobID string) error {
	if a.session == nil || a.session.removals == nil {
		return nil
	}
	return a.session.removals.prepareAuthorityLoss(ctx, jobID)
}

func (a *Agent) reservePublishedPort(claim l1.Claim) (net.Listener, *contract.SpawnFailure) {
	port := claim.Job.Spec.PublishedPort
	if claim.Job.Spec.Class != contract.JobClassService || port == nil {
		return nil, nil
	}
	if a.fabric == nil {
		return nil, &contract.SpawnFailure{
			Code: contract.SpawnFailureProcessRequest, Message: "Fabric is required for a portful service",
		}
	}
	listener, err := a.fabric.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		return nil, &contract.SpawnFailure{
			Code:    contract.SpawnFailurePublishedPortOccupied,
			Message: fmt.Sprintf("node %s could not reserve Fabric published port %d", a.registration.NodeID, *port),
		}
	}
	return listener, nil
}

// Status returns an agent-local lifecycle and occupancy snapshot. It does not
// mutate or reinterpret the control plane's contract.NodeState.
func (a *Agent) Status() Status {
	if a == nil || a.observer == nil || a.session == nil {
		return Status{Attempts: map[string]AttemptStatus{}}
	}
	return a.observer.snapshot(
		a.session.gates[workloadClassOneShot].occupancy(),
		a.session.gates[workloadClassService].occupancy(),
	)
}

// CapabilitySnapshot returns the same immutable observation used by local
// admission, registration, and heartbeat publication.
func (a *Agent) CapabilitySnapshot() CapabilitySnapshot {
	if a == nil || a.capabilities == nil {
		return CapabilitySnapshot{}
	}
	return a.capabilities.capabilitySnapshot()
}

// RecoverOCIRuntimeCapabilities performs the full event-triggered OCI recovery
// transaction: restrictive publication, barrier takeover, removal resumption,
// functional probe, generation recheck, and positive publication.
func (a *Agent) RecoverOCIRuntimeCapabilities(ctx context.Context) error {
	if a == nil || a.capabilities == nil || a.session == nil {
		return nil
	}
	if a.session.ociBootBarrier == nil {
		return a.capabilities.refresh(ctx)
	}
	generation, err := a.session.recoverOCIRuntime(ctx)
	if err != nil {
		return err
	}
	_, err = a.session.publishCapabilityHeartbeat(ctx, &generation)
	return err
}

// OCISweepReceipt returns a defensive copy of the currently pinned verified
// sweep proof for runtime adapters and removal validation.
func (a *Agent) OCISweepReceipt() (ocihelper.VerifiedSweepReceipt, bool) {
	if a == nil || a.session == nil || a.session.ociBootBarrier == nil {
		return ocihelper.VerifiedSweepReceipt{}, false
	}
	return a.session.ociBootBarrier.SweepReceipt()
}

func (a *Agent) sessionClient() *Client {
	if a.session == nil {
		return nil
	}
	return a.session.client
}

func stringOrDefault(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func cloneEnvironment(values map[string]string) map[string]string {
	cloned := make(map[string]string, len(values))
	for name, value := range values {
		cloned[name] = value
	}
	return cloned
}

func NewBootSessionID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", fmt.Errorf("agent: generate boot session ID: %w", err)
	}
	return "boot_" + hex.EncodeToString(value[:]), nil
}

func durationOrDefault(value, fallback time.Duration) time.Duration {
	if value <= 0 {
		return fallback
	}
	return value
}

func intOrDefault(value, fallback int) int {
	if value <= 0 {
		return fallback
	}
	return value
}

func int64OrDefault(value, fallback int64) int64 {
	if value <= 0 {
		return fallback
	}
	return value
}

func cloneCapabilities(capabilities map[string]bool) map[string]bool {
	cloned := make(map[string]bool, len(capabilities))
	for capability, enabled := range capabilities {
		cloned[capability] = enabled
	}
	return cloned
}

func serialLogf(logf func(string, ...any)) func(string, ...any) {
	if logf == nil {
		return nil
	}
	var mu sync.Mutex
	return func(format string, args ...any) {
		mu.Lock()
		defer mu.Unlock()
		logf(format, args...)
	}
}

func (a *Agent) log(format string, args ...any) {
	if a.logf != nil {
		a.logf(format, args...)
	}
}
