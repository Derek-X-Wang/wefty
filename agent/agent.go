// Package agent implements the long-running wefty node agent.
package agent

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/l1"
	processrunner "github.com/Derek-X-Wang/wefty/runner/process"
)

const (
	DefaultHeartbeatInterval       = 15 * time.Second
	DefaultClaimInterval           = time.Second
	DefaultRenewalInterval         = 10 * time.Second
	DefaultHandoffRetention        = 24 * time.Hour
	DefaultLogBatchSize            = 32
	DefaultLogFlushInterval        = 100 * time.Millisecond
	DefaultLogRetryInterval        = 100 * time.Millisecond
	DefaultLogSpoolMaxBytes        = 64 << 20
	DefaultServiceLogSpoolMaxBytes = 32 << 20
)

// ProcessRunner is the execution seam used by the node loop.
type ProcessRunner interface {
	Run(context.Context, processrunner.Request, processrunner.OutputSink) (contract.ProcessResult, error)
}

// OutputSinkFactory creates an optional local output destination for a claimed
// attempt. It may be invoked concurrently and must return an attempt-isolated
// sink or a sink that internally synchronizes shared state. The agent always
// uploads logs to L1 in addition to this sink.
type OutputSinkFactory func(l1.Claim) processrunner.OutputSink

// Config contains the stable node identity, per-process boot identity, and
// independent heartbeat, claim, and lease-renewal cadences.
type Config struct {
	Fabric               fabric.Fabric
	ControlPlaneAddress  string
	RunLedgerAddress     string
	NodeID               string
	BootSessionID        string
	Version              string
	OS                   string
	Architecture         string
	Capabilities         map[string]bool
	HeartbeatInterval    time.Duration
	ClaimInterval        time.Duration
	RenewalInterval      time.Duration
	OperationTimeout     time.Duration
	LogBatchSize         int
	LogFlushInterval     time.Duration
	LogRetryInterval     time.Duration
	LogSpoolDirectory    string
	LogSpoolMaxBytes     int64
	ManagedRootDirectory string
	GuardianExecutable   string
	Runner               ProcessRunner
	OutputSinkFactory    OutputSinkFactory
	HandoffRoot          string
	HandoffRetention     time.Duration
	// Logf need not be goroutine-safe. Agent serializes calls made through it.
	Logf  func(string, ...any)
	Clock Clock
}

// Agent owns process-lifetime resources and starts one control-plane session.
type Agent struct {
	fabric           fabric.Fabric
	runLedgerAddr    string
	registration     contract.NodeRegistration
	renewalInterval  time.Duration
	logRetryInterval time.Duration
	session          *agentSession
	outbox           *evidenceOutbox
	// logSpool is a compatibility view used by existing package tests. The
	// process-lifetime evidenceOutbox is its sole owner.
	logSpool          *logSpool
	runner            ProcessRunner
	managedResource   managedResourceManager
	outputSinkFactory OutputSinkFactory
	handoffs          *handoffManager
	logf              func(string, ...any)
	clock             Clock
	observer          *lifecycleObserver
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
	osName := config.OS
	if osName == "" {
		osName = runtime.GOOS
	}
	architecture := config.Architecture
	if architecture == "" {
		architecture = runtime.GOARCH
	}
	runner := config.Runner
	clock := config.Clock
	if clock == nil {
		clock = systemClock{}
	}
	client, err := newClient(config.Fabric, config.ControlPlaneAddress, durationOrDefault(config.OperationTimeout, DefaultOperationTimeout))
	if err != nil {
		return nil, err
	}
	if runner == nil {
		runner = processrunner.New(processrunner.Config{
			Clock: processClockAdapter{clock: clock}, GuardianExecutable: config.GuardianExecutable,
		})
	}
	managedResource, err := initializeManagedResource(config.ManagedRootDirectory, config.NodeID, config.BootSessionID)
	if err != nil {
		client.Close()
		return nil, err
	}
	registration := contract.NodeRegistration{
		NodeID:        config.NodeID,
		BootSessionID: config.BootSessionID,
		OS:            osName,
		Architecture:  architecture,
		AgentVersion:  config.Version,
		Capabilities:  cloneCapabilities(config.Capabilities),
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
	observer := newLifecycleObserver(clock)
	logf := serialLogf(config.Logf)
	session := newAgentSession(client, registration, heartbeatInterval, claimInterval, clock, observer, logf)
	return &Agent{
		fabric: config.Fabric, runLedgerAddr: stringOrDefault(config.RunLedgerAddress, "wefty://run-ledger"),
		registration: registration, renewalInterval: durationOrDefault(config.RenewalInterval, DefaultRenewalInterval),
		logRetryInterval: logRetryInterval, session: session, outbox: outbox, logSpool: outbox.spool,
		runner: runner, managedResource: managedResource, outputSinkFactory: config.OutputSinkFactory,
		handoffs: newHandoffManager(config.HandoffRoot, durationOrDefault(config.HandoffRetention, DefaultHandoffRetention)),
		logf:     logf, clock: clock, observer: observer,
		nodeLock: stableNodeLock,
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

func (a *Agent) runProcess(ctx context.Context, claim l1.Claim) (contract.ProcessResult, error) {
	return a.newAttemptLifecycle().runProcess(ctx, claim)
}

func (a *Agent) newAttemptLifecycle() *attemptLifecycle {
	return newAttemptLifecycle(attemptLifecycleDependencies{
		client: a.sessionClient(), runner: a.runner, outbox: a.outbox,
		watchdog: newAuthorityWatchdog(a.clock), clock: a.clock,
		renewalInterval: a.renewalInterval, completionRetry: a.logRetryInterval,
		outputSinkFactory: a.outputSinkFactory, handoffs: a.handoffs,
		managedResource: a.managedResource,
		nodeID:          a.registration.NodeID, workflowBridge: a.startWorkflowBridge, logf: a.logf,
		observer: a.observer, preflight: a.preflightPublishedPort,
	})
}

func (a *Agent) preflightPublishedPort(claim l1.Claim) *contract.SpawnFailure {
	port := claim.Job.Spec.PublishedPort
	if claim.Job.Spec.Class != contract.JobClassService || port == nil || a.fabric == nil {
		return nil
	}
	listener, err := a.fabric.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		return &contract.SpawnFailure{
			Code:    contract.SpawnFailurePublishedPortOccupied,
			Message: fmt.Sprintf("node %s published port %d is occupied", a.registration.NodeID, *port),
		}
	}
	_ = listener.Close()
	return nil
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
	if capabilities == nil {
		return map[string]bool{"process": true}
	}
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
