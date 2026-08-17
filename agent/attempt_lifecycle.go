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

func (lifecycle *attemptLifecycle) execute(ctx context.Context, claim l1.Claim) (errorDestination, error) {
	attemptContext, cancelAttempt := context.WithCancel(ctx)
	defer cancelAttempt()
	executionContext, cancelExecution := context.WithCancel(attemptContext)
	defer cancelExecution()

	watch := lifecycle.dependencies.watchdog.Start(attemptContext, claim.Lease)
	defer watch.Stop()

	renewalErrors := make(chan destinationError, 1)
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
		return errorDestinationUnclassified, ctx.Err()
	case failure := <-renewalErrors:
		cancelAttempt()
		<-completed
		<-renewalDone
		return failure.destination, fmt.Errorf("agent: renew lease: %w", failure.err)
	case err := <-watch.Failures():
		cancelAttempt()
		<-completed
		<-renewalDone
		return errorDestinationUnclassified, fmt.Errorf("agent: authority watchdog: %w", err)
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
	completionDone := make(chan destinationError, 1)
	go func() { completionDone <- lifecycle.completeWithRetry(attemptContext, claim, request) }()
	var completionFailure destinationError
	select {
	case completionFailure = <-completionDone:
		cancelAttempt()
	case renewalFailure := <-renewalErrors:
		cancelAttempt()
		completionFailure = <-completionDone
		<-renewalDone
		if completionFailure.err != nil {
			return renewalFailure.destination, fmt.Errorf("agent: renew lease while completing: %w", renewalFailure.err)
		}
	case <-ctx.Done():
		cancelAttempt()
		<-completionDone
		<-renewalDone
		return errorDestinationUnclassified, ctx.Err()
	}
	<-renewalDone
	if completionFailure.err != nil {
		return completionFailure.destination, fmt.Errorf("agent: complete attempt: %w", completionFailure.err)
	}
	if lifecycle.dependencies.handoffs != nil && claim.Job.Spec.Class == contract.JobClassOneShot {
		succeeded := outcome.err == nil && outcome.result.ExitCode != nil && *outcome.result.ExitCode == 0
		if err := lifecycle.dependencies.handoffs.finish(claim.Job.Spec, lifecycle.dependencies.nodeID, succeeded); err != nil {
			return errorDestinationUnclassified, fmt.Errorf("agent: finish handoff lifecycle: %w", err)
		}
	}
	return errorDestinationUnclassified, nil
}

func (lifecycle *attemptLifecycle) completeWithRetry(ctx context.Context, claim l1.Claim, request l1.CompletionRequest) destinationError {
	for {
		if _, err := lifecycle.dependencies.client.Complete(ctx, claim.Job.JobID, claim.Lease.AttemptID, request); err != nil {
			classification := classifyAgentProtocolError(err)
			if classification.destination != errorDestinationTransient {
				return destinationError{destination: classification.destination, err: err}
			}
			timer := lifecycle.dependencies.clock.NewTimer(lifecycle.dependencies.completionRetry)
			select {
			case <-ctx.Done():
				stopTimer(timer)
				return destinationError{destination: errorDestinationUnclassified, err: ctx.Err()}
			case <-timer.C():
			}
			continue
		}
		return destinationError{}
	}
}

func (lifecycle *attemptLifecycle) runProcess(ctx context.Context, claim l1.Claim) (contract.ProcessResult, error) {
	if err := contract.CheckWorkloadClass(claim.Job.Spec.Class); err != nil {
		return spawnFailure(contract.SpawnFailureUnsupportedClass, err), err
	}
	if err := contract.CheckExecutableKind(claim.Job.Spec.Kind); err != nil {
		return spawnFailure(contract.SpawnFailureUnsupportedKind, err), err
	}
	if claim.Job.Spec.RuntimeHandler != "" {
		err := fmt.Errorf("runtime handler %q is not supported for process jobs", claim.Job.Spec.RuntimeHandler)
		return spawnFailure(contract.SpawnFailureUnsupportedRuntimeHandler, err), err
	}
	if lifecycle.dependencies.handoffs != nil && claim.Job.Spec.Class == contract.JobClassOneShot {
		if err := lifecycle.dependencies.handoffs.prepare(claim.Job.Spec, lifecycle.dependencies.nodeID); err != nil {
			return spawnFailure(contract.SpawnFailureHandoffPreparation, err), err
		}
	}
	execution, cleanupExecutable, err := materializeExecutable(claim.Job.Spec.Execution, claim.Lease.AttemptID)
	if err != nil {
		return spawnFailure(contract.SpawnFailureExecutableMaterialization, err), err
	}
	defer cleanupExecutable()
	var bridge *workflowBridge
	if lifecycle.dependencies.workflowBridge != nil && claim.Job.Spec.Class == contract.JobClassOneShot {
		bridge, err = lifecycle.dependencies.workflowBridge(ctx, execution)
		if err != nil {
			return spawnFailure(contract.SpawnFailureWorkflowBridgeCreation, err), err
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
			return spawnFailure(contract.SpawnFailureLogSinkSetup, err), err
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

func (lifecycle *attemptLifecycle) renewalLoop(ctx context.Context, claim l1.Claim, failures chan<- destinationError, renewed func(l1.AttemptLease)) {
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
			classification := classifyAgentProtocolError(err)
			if classification.destination == errorDestinationTransient {
				remaining := lease.LeaseExpires.Sub(lifecycle.dependencies.clock.Now())
				if remaining > 0 {
					nextDelay = min(lifecycle.dependencies.completionRetry, remaining)
					continue
				}
			}
			select {
			case failures <- destinationError{destination: classification.destination, err: err}:
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

func spawnFailure(code contract.SpawnFailureCode, err error) contract.ProcessResult {
	return contract.ProcessResult{SpawnError: &contract.SpawnFailure{Code: code, Message: err.Error()}}
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
	return l1.ProcessResult{
		SpawnError: result.SpawnError, OutputError: result.OutputError, ExitCode: result.ExitCode,
		Signal: result.Signal, TerminationCause: result.TerminationCause,
	}
}
