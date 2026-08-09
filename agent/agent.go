// Package agent implements the long-running wefty node agent.
package agent

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"runtime"
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
)

// ProcessRunner is the execution seam used by the node loop.
type ProcessRunner interface {
	Run(context.Context, processrunner.Request, processrunner.OutputSink) (contract.ProcessResult, error)
}

// OutputSinkFactory creates an output destination for a claimed attempt.
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
	Runner              ProcessRunner
	OutputSinkFactory   OutputSinkFactory
	HandoffRoot         string
	HandoffRetention    time.Duration
	Logf                func(string, ...any)
}

// Agent registers one boot session and serially executes claimed jobs while
// node heartbeats and attempt lease renewals proceed on distinct schedules.
type Agent struct {
	client            *Client
	registration      contract.NodeRegistration
	heartbeatInterval time.Duration
	claimInterval     time.Duration
	renewalInterval   time.Duration
	runner            ProcessRunner
	outputSinkFactory OutputSinkFactory
	handoffs          *handoffManager
	logf              func(string, ...any)
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
	if runner == nil {
		runner = processrunner.New(processrunner.Config{})
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
		runner:            runner,
		outputSinkFactory: config.OutputSinkFactory,
		handoffs:          newHandoffManager(config.HandoffRoot, durationOrDefault(config.HandoffRetention, DefaultHandoffRetention)),
		logf:              config.Logf,
	}, nil
}

// Close releases idle protocol connections.
func (a *Agent) Close() { a.client.Close() }

// Register starts or replaces the stable node's current boot session.
func (a *Agent) Register(ctx context.Context) (l1.Node, error) {
	return a.client.Register(ctx, a.registration)
}

// Run registers and then serves claims until the context is canceled or the
// control plane rejects a liveness or execution operation.
func (a *Agent) Run(ctx context.Context) error {
	if a.handoffs != nil {
		if err := a.handoffs.cleanupExpired(""); err != nil {
			return fmt.Errorf("agent: clean expired handoff directories: %w", err)
		}
	}
	if _, err := a.Register(ctx); err != nil {
		return fmt.Errorf("agent: register node: %w", err)
	}
	heartbeatErrors := make(chan error, 1)
	go a.heartbeatLoop(ctx, heartbeatErrors)

	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-heartbeatErrors:
			return err
		default:
		}

		claim, err := a.client.Claim(ctx, a.registration.NodeID, a.registration.BootSessionID)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("agent: claim job: %w", err)
		}
		if claim == nil {
			if err := wait(ctx, a.claimInterval, heartbeatErrors); err != nil {
				if errors.Is(err, context.Canceled) {
					return nil
				}
				return err
			}
			continue
		}
		if err := a.executeClaim(ctx, *claim); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
	}
}

func (a *Agent) heartbeatLoop(ctx context.Context, failures chan<- error) {
	ticker := time.NewTicker(a.heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := a.client.Heartbeat(ctx, a.registration.NodeID, a.registration.BootSessionID); err != nil {
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
	if _, err := a.client.Complete(ctx, claim.Job.JobID, claim.Lease.AttemptID, request); err != nil {
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
	var sink processrunner.OutputSink
	if a.outputSinkFactory != nil {
		sink = a.outputSinkFactory(claim)
	}
	sink = redactOutputSink(sink, claim.Job.Spec.Execution.SensitiveEnv)
	return a.runner.Run(ctx, processrunner.Request{
		AttemptID: claim.Lease.AttemptID,
		Execution: execution,
		Limits:    claim.Job.Spec.Limits,
	}, sink)
}

func redactOutputSink(sink processrunner.OutputSink, sensitive map[string]string) processrunner.OutputSink {
	if sink == nil || len(sensitive) == 0 {
		return sink
	}
	secrets := make([][]byte, 0, len(sensitive))
	for _, value := range sensitive {
		if value != "" {
			secrets = append(secrets, []byte(value))
		}
	}
	if len(secrets) == 0 {
		return sink
	}
	return processrunner.OutputSinkFunc(func(ctx context.Context, event contract.LogEvent) error {
		for _, secret := range secrets {
			event.Bytes = bytes.ReplaceAll(event.Bytes, secret, []byte("[REDACTED]"))
		}
		return sink.WriteOutput(ctx, event)
	})
}

func (a *Agent) renewalLoop(ctx context.Context, claim l1.Claim, failures chan<- error) {
	lease := claim.Lease
	for {
		delay := renewalDelay(time.Now(), lease.LeaseExpires, a.renewalInterval)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-timer.C:
		}
		renewed, err := a.client.Renew(ctx, claim.Job.JobID, lease.AttemptID, lease.FencingToken)
		if err != nil {
			select {
			case failures <- err:
			default:
			}
			return
		}
		lease = renewed
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

func wait(ctx context.Context, duration time.Duration, heartbeatErrors <-chan error) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-heartbeatErrors:
		return err
	case <-timer.C:
		return nil
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
