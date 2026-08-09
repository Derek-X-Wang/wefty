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
	DefaultHeartbeatInterval = 15 * time.Second
	DefaultClaimInterval     = time.Second
	DefaultRenewalInterval   = 10 * time.Second
	DefaultHandoffRetention  = 24 * time.Hour
	DefaultLogBatchSize      = 32
	DefaultLogFlushInterval  = 100 * time.Millisecond
	DefaultLogRetryInterval  = 100 * time.Millisecond
	DefaultLogSpoolMaxBytes  = 64 << 20
)

// ProcessRunner is the execution seam used by the node loop.
type ProcessRunner interface {
	Run(context.Context, processrunner.Request, processrunner.OutputSink) (contract.ProcessResult, error)
}

// OutputSinkFactory creates an optional local output destination for a claimed
// attempt. The agent always uploads logs to L1 in addition to this sink.
type OutputSinkFactory func(l1.Claim) processrunner.OutputSink

// Config contains the stable node identity, per-process boot identity, and
// independent heartbeat, claim, and lease-renewal cadences.
type Config struct {
	Fabric              fabric.Fabric
	ControlPlaneAddress string
	NodeID              string
	BootSessionID       string
	Version             string
	OS                  string
	Architecture        string
	Capabilities        map[string]bool
	HeartbeatInterval   time.Duration
	ClaimInterval       time.Duration
	RenewalInterval     time.Duration
	LogBatchSize        int
	LogFlushInterval    time.Duration
	LogRetryInterval    time.Duration
	LogSpoolDirectory   string
	LogSpoolMaxBytes    int64
	Runner              ProcessRunner
	OutputSinkFactory   OutputSinkFactory
	HandoffRoot         string
	HandoffRetention    time.Duration
	Logf                func(string, ...any)
	Clock               Clock
}

// Agent registers one boot session and serially executes claimed jobs while
// node heartbeats and attempt lease renewals proceed on distinct schedules.
type Agent struct {
	client            *Client
	registration      contract.NodeRegistration
	heartbeatInterval time.Duration
	claimInterval     time.Duration
	renewalInterval   time.Duration
	logBatchSize      int
	logFlushInterval  time.Duration
	logRetryInterval  time.Duration
	logSpool          *logSpool
	runner            ProcessRunner
	outputSinkFactory OutputSinkFactory
	handoffs          *handoffManager
	logf              func(string, ...any)
	clock             Clock
	drainOnce         sync.Once
	drainRequested    chan struct{}
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
	client, err := NewClient(config.Fabric, config.ControlPlaneAddress)
	if err != nil {
		return nil, err
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
	if runner == nil {
		runner = processrunner.New(processrunner.Config{Clock: processClockAdapter{clock: clock}})
	}
	spool, err := openLogSpool(config.LogSpoolDirectory, config.NodeID, int64OrDefault(config.LogSpoolMaxBytes, DefaultLogSpoolMaxBytes))
	if err != nil {
		client.Close()
		return nil, err
	}
	return &Agent{
		client: client,
		registration: contract.NodeRegistration{
			NodeID:        config.NodeID,
			BootSessionID: config.BootSessionID,
			OS:            osName,
			Architecture:  architecture,
			AgentVersion:  config.Version,
			Capabilities:  cloneCapabilities(config.Capabilities),
		},
		heartbeatInterval: durationOrDefault(config.HeartbeatInterval, DefaultHeartbeatInterval),
		claimInterval:     durationOrDefault(config.ClaimInterval, DefaultClaimInterval),
		renewalInterval:   durationOrDefault(config.RenewalInterval, DefaultRenewalInterval),
		logBatchSize:      intOrDefault(config.LogBatchSize, DefaultLogBatchSize),
		logFlushInterval:  durationOrDefault(config.LogFlushInterval, DefaultLogFlushInterval),
		logRetryInterval:  durationOrDefault(config.LogRetryInterval, DefaultLogRetryInterval),
		logSpool:          spool,
		runner:            runner,
		outputSinkFactory: config.OutputSinkFactory,
		handoffs:          newHandoffManager(config.HandoffRoot, durationOrDefault(config.HandoffRetention, DefaultHandoffRetention)),
		logf:              config.Logf,
		clock:             clock,
		drainRequested:    make(chan struct{}),
	}, nil
}

type processClockAdapter struct{ clock Clock }

func (adapter processClockAdapter) Now() time.Time { return adapter.clock.Now() }
func (adapter processClockAdapter) NewTimer(duration time.Duration) processrunner.Timer {
	return adapter.clock.NewTimer(duration)
}

// Close releases idle protocol connections.
func (a *Agent) Close() {
	a.client.Close()
	if a.logSpool != nil {
		if err := a.logSpool.Close(); err != nil {
			a.log("close durable log spool: %v", err)
		}
	}
}

// Register starts or replaces the stable node's current boot session.
func (a *Agent) Register(ctx context.Context) (l1.Node, error) {
	return a.client.Register(ctx, a.registration)
}

// Drain stops new claims locally and marks the current boot session draining
// at the control plane. An attempt already executing is not canceled; Run
// returns after that attempt has uploaded its fenced completion.
func (a *Agent) Drain(ctx context.Context) (l1.Node, error) {
	node, err := a.client.Drain(ctx, a.registration.NodeID, a.registration.BootSessionID)
	a.drainOnce.Do(func() { close(a.drainRequested) })
	return node, err
}

// Run registers and then serves claims until the context is canceled or the
// control plane rejects a liveness or execution operation.
func (a *Agent) Run(ctx context.Context) error {
	if a.handoffs != nil {
		if err := a.handoffs.cleanupExpired(""); err != nil {
			return fmt.Errorf("agent: clean expired handoff directories: %w", err)
		}
	}
	runContext, stop := context.WithCancel(ctx)
	defer stop()
	if err := a.recoverPendingLogs(runContext); err != nil {
		if runContext.Err() != nil {
			return nil
		}
		return fmt.Errorf("agent: recover durable logs: %w", err)
	}
	if _, err := a.Register(runContext); err != nil {
		return fmt.Errorf("agent: register node: %w", err)
	}
	if a.isDraining() {
		if _, err := a.client.Drain(runContext, a.registration.NodeID, a.registration.BootSessionID); err != nil {
			return fmt.Errorf("agent: drain node: %w", err)
		}
		return nil
	}
	heartbeatErrors := make(chan error, 1)
	go a.heartbeatLoop(runContext, heartbeatErrors)

	for {
		select {
		case <-runContext.Done():
			return nil
		case <-a.drainRequested:
			return nil
		case err := <-heartbeatErrors:
			return err
		default:
		}

		claim, err := a.client.Claim(runContext, a.registration.NodeID, a.registration.BootSessionID)
		if err != nil {
			if runContext.Err() != nil || a.isDraining() {
				return nil
			}
			if retryableAgentProtocolError(err) {
				if waitErr := a.wait(runContext, a.claimInterval, heartbeatErrors); waitErr != nil {
					if errors.Is(waitErr, context.Canceled) {
						return nil
					}
					return waitErr
				}
				continue
			}
			return fmt.Errorf("agent: claim job: %w", err)
		}
		if claim == nil {
			if err := a.wait(runContext, a.claimInterval, heartbeatErrors); err != nil {
				if errors.Is(err, context.Canceled) {
					return nil
				}
				return err
			}
			continue
		}
		if err := a.executeClaim(runContext, *claim); err != nil {
			if runContext.Err() != nil {
				return nil
			}
			return err
		}
	}
}

func (a *Agent) recoverPendingLogs(ctx context.Context) error {
	if a.logSpool == nil {
		return nil
	}
	attempts, err := a.logSpool.pendingAttempts(ctx)
	if err != nil {
		return err
	}
	for _, attempt := range attempts {
		claim := l1.Claim{
			Job:   l1.Job{JobID: attempt.jobID},
			Lease: l1.AttemptLease{AttemptID: attempt.attemptID, FencingToken: attempt.fencingToken},
		}
		uploader, err := newBatchingLogSink(ctx, a.client, claim, a.logSpool, a.clock, a.logBatchSize, a.logFlushInterval, a.logRetryInterval)
		if err != nil {
			return err
		}
		if err := uploader.Close(); err != nil {
			return fmt.Errorf("attempt %s: %w", attempt.attemptID, err)
		}
	}
	return nil
}

func (a *Agent) heartbeatLoop(ctx context.Context, failures chan<- error) {
	for {
		timer := a.clock.NewTimer(a.heartbeatInterval)
		select {
		case <-ctx.Done():
			stopTimer(timer)
			return
		case <-timer.C():
			if _, err := a.client.Heartbeat(ctx, a.registration.NodeID, a.registration.BootSessionID); err != nil {
				if retryableAgentProtocolError(err) {
					continue
				}
				select {
				case failures <- fmt.Errorf("agent: heartbeat: %w", err):
				default:
				}
				return
			}
		}
	}
}

type runOutcome struct {
	result contract.ProcessResult
	err    error
}

func (a *Agent) executeClaim(ctx context.Context, claim l1.Claim) error {
	executionContext, cancelExecution := context.WithCancel(ctx)
	defer cancelExecution()

	renewalErrors := make(chan error, 1)
	go a.renewalLoop(executionContext, claim, renewalErrors)

	completed := make(chan runOutcome, 1)
	go func() {
		result, err := a.runProcess(executionContext, claim)
		completed <- runOutcome{result: result, err: err}
	}()

	var outcome runOutcome
	select {
	case <-ctx.Done():
		cancelExecution()
		<-completed
		return ctx.Err()
	case err := <-renewalErrors:
		cancelExecution()
		<-completed
		return fmt.Errorf("agent: renew lease: %w", err)
	case outcome = <-completed:
		cancelExecution()
	}

	if outcome.err != nil {
		a.log("attempt %s execution: %v", claim.Lease.AttemptID, outcome.err)
	}
	request := l1.CompletionRequest{
		FencingToken:   claim.Lease.FencingToken,
		IdempotencyKey: "completion:" + claim.Lease.AttemptID,
		Result:         toL1Result(outcome.result),
	}
	if err := a.completeWithRetry(ctx, claim, request); err != nil {
		return fmt.Errorf("agent: complete attempt: %w", err)
	}
	if a.handoffs != nil {
		succeeded := outcome.err == nil && outcome.result.ExitCode != nil && *outcome.result.ExitCode == 0
		if err := a.handoffs.finish(claim.Job.Spec, a.registration.NodeID, succeeded); err != nil {
			return fmt.Errorf("agent: finish handoff lifecycle: %w", err)
		}
	}
	return nil
}

func (a *Agent) completeWithRetry(ctx context.Context, claim l1.Claim, request l1.CompletionRequest) error {
	for {
		if _, err := a.client.Complete(ctx, claim.Job.JobID, claim.Lease.AttemptID, request); err != nil {
			if !retryableAgentProtocolError(err) {
				return err
			}
			timer := a.clock.NewTimer(a.logRetryInterval)
			select {
			case <-ctx.Done():
				stopTimer(timer)
				return ctx.Err()
			case <-timer.C():
			}
			continue
		}
		return nil
	}
}

func (a *Agent) runProcess(ctx context.Context, claim l1.Claim) (contract.ProcessResult, error) {
	if err := contract.CheckExecutableKind(claim.Job.Spec.Kind); err != nil {
		return contract.ProcessResult{SpawnError: err.Error()}, err
	}
	if claim.Job.Spec.RuntimeHandler != "" {
		err := fmt.Errorf("runtime handler %q is not supported for process jobs", claim.Job.Spec.RuntimeHandler)
		return contract.ProcessResult{SpawnError: err.Error()}, err
	}
	if a.handoffs != nil {
		if err := a.handoffs.prepare(claim.Job.Spec, a.registration.NodeID); err != nil {
			return contract.ProcessResult{SpawnError: err.Error()}, err
		}
	}
	execution, cleanupExecutable, err := materializeExecutable(claim.Job.Spec.Execution, claim.Lease.AttemptID)
	if err != nil {
		return contract.ProcessResult{SpawnError: err.Error()}, err
	}
	defer cleanupExecutable()
	var uploader *batchingLogSink
	var sinks multiOutputSink
	if a.client != nil {
		uploader, err = newBatchingLogSink(ctx, a.client, claim, a.logSpool, a.clock, a.logBatchSize, a.logFlushInterval, a.logRetryInterval)
		if err != nil {
			return contract.ProcessResult{SpawnError: err.Error()}, err
		}
		sinks = append(sinks, uploader)
	}
	var localSink processrunner.OutputSink
	if a.outputSinkFactory != nil {
		localSink = a.outputSinkFactory(claim)
	}
	if localSink != nil {
		sinks = append(sinks, localSink)
	}
	var sink processrunner.OutputSink
	if len(sinks) == 1 {
		sink = sinks[0]
	} else if len(sinks) > 1 {
		sink = sinks
	}
	redactingSink := newRedactingOutputSink(sink, claim.Job.Spec.Execution.SensitiveEnv)
	if redactingSink != nil {
		sink = redactingSink
	}
	result, runErr := a.runner.Run(ctx, processrunner.Request{
		AttemptID: claim.Lease.AttemptID,
		Execution: execution,
		Limits:    claim.Job.Spec.Limits,
	}, sink)
	var outputErr error
	if redactingSink != nil {
		outputErr = redactingSink.Flush(ctx)
	}
	var uploadErr error
	if uploader != nil {
		uploadErr = uploader.Close()
	}
	if outputErr != nil {
		outputErr = fmt.Errorf("flush redacted output: %w", outputErr)
	}
	if uploadErr != nil {
		uploadErr = fmt.Errorf("upload logs: %w", uploadErr)
	}
	return result, errors.Join(runErr, outputErr, uploadErr)
}

func (a *Agent) renewalLoop(ctx context.Context, claim l1.Claim, failures chan<- error) {
	lease := claim.Lease
	nextDelay := renewalDelay(a.clock.Now(), lease.LeaseExpires, a.renewalInterval)
	for {
		timer := a.clock.NewTimer(nextDelay)
		select {
		case <-ctx.Done():
			stopTimer(timer)
			return
		case <-timer.C():
		}
		renewed, err := a.client.Renew(ctx, claim.Job.JobID, lease.AttemptID, lease.FencingToken)
		if err != nil {
			if retryableAgentProtocolError(err) {
				remaining := lease.LeaseExpires.Sub(a.clock.Now())
				if remaining > 0 {
					nextDelay = min(a.logRetryInterval, remaining)
					continue
				}
			}
			select {
			case failures <- err:
			default:
			}
			return
		}
		lease = renewed
		nextDelay = renewalDelay(a.clock.Now(), lease.LeaseExpires, a.renewalInterval)
	}
}

func renewalDelay(now, expires time.Time, configured time.Duration) time.Duration {
	halfRemaining := expires.Sub(now) / 2
	if halfRemaining <= 0 {
		return time.Millisecond
	}
	if configured < halfRemaining {
		return configured
	}
	return halfRemaining
}

func toL1Result(result contract.ProcessResult) l1.ProcessResult {
	return l1.ProcessResult{SpawnError: result.SpawnError, ExitCode: result.ExitCode, Signal: result.Signal}
}

func (a *Agent) wait(ctx context.Context, duration time.Duration, heartbeatErrors <-chan error) error {
	timer := a.clock.NewTimer(duration)
	defer stopTimer(timer)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-a.drainRequested:
		return context.Canceled
	case err := <-heartbeatErrors:
		return err
	case <-timer.C():
		return nil
	}
}

func (a *Agent) isDraining() bool {
	select {
	case <-a.drainRequested:
		return true
	default:
		return false
	}
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

func (a *Agent) log(format string, args ...any) {
	if a.logf != nil {
		a.logf(format, args...)
	}
}
