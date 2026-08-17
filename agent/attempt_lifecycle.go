package agent

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	processrunner "github.com/Derek-X-Wang/wefty/runner/process"
)

type attemptWatchdog interface {
	Start(context.Context, l1.AttemptLease) attemptWatch
}

type attemptWatch interface {
	Renewed(l1.AttemptLease)
	Failures() <-chan error
	Stop()
}

type disabledAttemptWatchdog struct{}

func (disabledAttemptWatchdog) Start(context.Context, l1.AttemptLease) attemptWatch {
	return disabledAttemptWatch{}
}

type disabledAttemptWatch struct{}

func (disabledAttemptWatch) Renewed(l1.AttemptLease) {}
func (disabledAttemptWatch) Failures() <-chan error  { return nil }
func (disabledAttemptWatch) Stop()                   {}

type attemptLifecycleDependencies struct {
	client            *Client
	runner            ProcessRunner
	outbox            *evidenceOutbox
	watchdog          attemptWatchdog
	clock             Clock
	renewalInterval   time.Duration
	completionRetry   time.Duration
	outputSinkFactory OutputSinkFactory
	handoffs          *handoffManager
	nodeID            string
	workflowBridge    func(context.Context, contract.ExecutionSpec) (*workflowBridge, error)
	logf              func(string, ...any)
}

// attemptLifecycle owns one attempt from renewal startup through process/log
// finalization, fenced completion, and handoff finalization. Every dependency
// is supplied by the process or session owner; the lifecycle creates no
// shared resource itself.
type attemptLifecycle struct {
	dependencies attemptLifecycleDependencies
}

func newAttemptLifecycle(dependencies attemptLifecycleDependencies) *attemptLifecycle {
	if dependencies.watchdog == nil {
		dependencies.watchdog = disabledAttemptWatchdog{}
	}
	return &attemptLifecycle{dependencies: dependencies}
}

type runOutcome struct {
	result contract.ProcessResult
	err    error
}

func (lifecycle *attemptLifecycle) execute(ctx context.Context, claim l1.Claim) error {
	attemptContext, cancelAttempt := context.WithCancel(ctx)
	defer cancelAttempt()
	executionContext, cancelExecution := context.WithCancel(attemptContext)
	defer cancelExecution()

	watch := lifecycle.dependencies.watchdog.Start(attemptContext, claim.Lease)
	defer watch.Stop()

	renewalErrors := make(chan error, 1)
	renewalDone := make(chan struct{})
	go func() {
		defer close(renewalDone)
		lifecycle.renewalLoop(attemptContext, claim, renewalErrors, watch.Renewed)
	}()

	completed := make(chan runOutcome, 1)
	go func() {
		result, err := lifecycle.runProcess(executionContext, claim)
		completed <- runOutcome{result: result, err: err}
	}()

	var outcome runOutcome
	select {
	case <-ctx.Done():
		cancelAttempt()
		<-completed
		<-renewalDone
		return ctx.Err()
	case err := <-renewalErrors:
		cancelAttempt()
		<-completed
		<-renewalDone
		return fmt.Errorf("agent: renew lease: %w", err)
	case err := <-watch.Failures():
		cancelAttempt()
		<-completed
		<-renewalDone
		return fmt.Errorf("agent: authority watchdog: %w", err)
	case outcome = <-completed:
		cancelExecution()
	}

	if outcome.err != nil {
		lifecycle.log("attempt %s execution: %v", claim.Lease.AttemptID, outcome.err)
	}
	request := l1.CompletionRequest{
		FencingToken:   claim.Lease.FencingToken,
		IdempotencyKey: "completion:" + claim.Lease.AttemptID,
		Result:         toL1Result(outcome.result),
	}
	completionDone := make(chan error, 1)
	go func() { completionDone <- lifecycle.completeWithRetry(attemptContext, claim, request) }()
	var completionErr error
	select {
	case completionErr = <-completionDone:
		cancelAttempt()
	case err := <-renewalErrors:
		cancelAttempt()
		completionErr = <-completionDone
		<-renewalDone
		if completionErr != nil {
			return fmt.Errorf("agent: renew lease while completing: %w", err)
		}
	case <-ctx.Done():
		cancelAttempt()
		<-completionDone
		<-renewalDone
		return ctx.Err()
	}
	<-renewalDone
	if completionErr != nil {
		return fmt.Errorf("agent: complete attempt: %w", completionErr)
	}
	if lifecycle.dependencies.handoffs != nil {
		succeeded := outcome.err == nil && outcome.result.ExitCode != nil && *outcome.result.ExitCode == 0
		if err := lifecycle.dependencies.handoffs.finish(claim.Job.Spec, lifecycle.dependencies.nodeID, succeeded); err != nil {
			return fmt.Errorf("agent: finish handoff lifecycle: %w", err)
		}
	}
	return nil
}

func (lifecycle *attemptLifecycle) completeWithRetry(ctx context.Context, claim l1.Claim, request l1.CompletionRequest) error {
	for {
		if _, err := lifecycle.dependencies.client.Complete(ctx, claim.Job.JobID, claim.Lease.AttemptID, request); err != nil {
			if !retryableAgentProtocolError(err) {
				return err
			}
			timer := lifecycle.dependencies.clock.NewTimer(lifecycle.dependencies.completionRetry)
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

func (lifecycle *attemptLifecycle) runProcess(ctx context.Context, claim l1.Claim) (contract.ProcessResult, error) {
	if err := contract.CheckExecutableKind(claim.Job.Spec.Kind); err != nil {
		return contract.ProcessResult{SpawnError: err.Error()}, err
	}
	if claim.Job.Spec.RuntimeHandler != "" {
		err := fmt.Errorf("runtime handler %q is not supported for process jobs", claim.Job.Spec.RuntimeHandler)
		return contract.ProcessResult{SpawnError: err.Error()}, err
	}
	if lifecycle.dependencies.handoffs != nil {
		if err := lifecycle.dependencies.handoffs.prepare(claim.Job.Spec, lifecycle.dependencies.nodeID); err != nil {
			return contract.ProcessResult{SpawnError: err.Error()}, err
		}
	}
	execution, cleanupExecutable, err := materializeExecutable(claim.Job.Spec.Execution, claim.Lease.AttemptID)
	if err != nil {
		return contract.ProcessResult{SpawnError: err.Error()}, err
	}
	defer cleanupExecutable()
	var bridge *workflowBridge
	if lifecycle.dependencies.workflowBridge != nil {
		bridge, err = lifecycle.dependencies.workflowBridge(ctx, execution)
		if err != nil {
			return contract.ProcessResult{SpawnError: err.Error()}, err
		}
	}
	if bridge != nil {
		defer bridge.close()
		execution.Env = cloneEnvironment(execution.Env)
		delete(execution.Env, "WEFTY_L1_ENDPOINT")
		execution.Env[contract.EnvL3Endpoint] = bridge.l3Endpoint
	}
	var uploader *batchingLogSink
	var sinks multiOutputSink
	if lifecycle.dependencies.client != nil && lifecycle.dependencies.outbox != nil {
		uploader, err = lifecycle.dependencies.outbox.newLogSink(ctx, lifecycle.dependencies.client, claim)
		if err != nil {
			return contract.ProcessResult{SpawnError: err.Error()}, err
		}
		sinks = append(sinks, uploader)
	}
	var localSink processrunner.OutputSink
	if lifecycle.dependencies.outputSinkFactory != nil {
		localSink = lifecycle.dependencies.outputSinkFactory(claim)
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
	result, runErr := lifecycle.dependencies.runner.Run(ctx, processrunner.Request{
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
	finalizationErr := errors.Join(outputErr, uploadErr)
	if finalizationErr != nil {
		result = contract.ProcessResult{OutputError: finalizationErr.Error()}
	}
	return result, errors.Join(runErr, finalizationErr)
}

func (lifecycle *attemptLifecycle) renewalLoop(ctx context.Context, claim l1.Claim, failures chan<- error, renewed func(l1.AttemptLease)) {
	lease := claim.Lease
	nextDelay := renewalDelay(lifecycle.dependencies.clock.Now(), lease.LeaseExpires, lifecycle.dependencies.renewalInterval)
	for {
		timer := lifecycle.dependencies.clock.NewTimer(nextDelay)
		select {
		case <-ctx.Done():
			stopTimer(timer)
			return
		case <-timer.C():
		}
		updated, err := lifecycle.dependencies.client.Renew(ctx, claim.Job.JobID, lease.AttemptID, lease.FencingToken)
		if err != nil {
			if retryableAgentProtocolError(err) {
				remaining := lease.LeaseExpires.Sub(lifecycle.dependencies.clock.Now())
				if remaining > 0 {
					nextDelay = min(lifecycle.dependencies.completionRetry, remaining)
					continue
				}
			}
			select {
			case failures <- err:
			default:
			}
			return
		}
		lease = updated
		renewed(lease)
		nextDelay = renewalDelay(lifecycle.dependencies.clock.Now(), lease.LeaseExpires, lifecycle.dependencies.renewalInterval)
	}
}

func (lifecycle *attemptLifecycle) log(format string, args ...any) {
	if lifecycle.dependencies.logf != nil {
		lifecycle.dependencies.logf(format, args...)
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
	return l1.ProcessResult{SpawnError: result.SpawnError, OutputError: result.OutputError, ExitCode: result.ExitCode, Signal: result.Signal}
}
