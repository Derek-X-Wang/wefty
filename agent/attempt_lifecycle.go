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
	Start(context.Context, localAuthority, context.CancelCauseFunc) attemptWatch
}

type attemptWatch interface {
	Renewed(localAuthority)
	Failures() <-chan error
	Check() error
	Stop()
}

type disabledAttemptWatchdog struct{}

func (disabledAttemptWatchdog) Start(context.Context, localAuthority, context.CancelCauseFunc) attemptWatch {
	return disabledAttemptWatch{}
}

type disabledAttemptWatch struct{}

func (disabledAttemptWatch) Renewed(localAuthority) {}
func (disabledAttemptWatch) Failures() <-chan error { return nil }
func (disabledAttemptWatch) Check() error           { return nil }
func (disabledAttemptWatch) Stop()                  {}

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
	observer          *lifecycleObserver
	preflight         func(l1.Claim) *contract.SpawnFailure
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
	result        contract.ProcessResult
	err           error
	durabilityErr error
}

func (lifecycle *attemptLifecycle) execute(ctx context.Context, claim l1.Claim, claimStarted time.Time) (errorDestination, error) {
	attemptContext, cancelAttempt := context.WithCancelCause(ctx)
	defer cancelAttempt(nil)
	executionContext, cancelExecution := context.WithCancel(attemptContext)
	defer cancelExecution()
	attemptID := claim.Lease.AttemptID
	lifecycle.dependencies.observer.beginAttempt(attemptID, claim.Job.JobID, claim.Job.Spec.Class)
	defer lifecycle.dependencies.observer.finishAttempt(attemptID)
	if lifecycle.dependencies.outbox != nil {
		if err := lifecycle.dependencies.outbox.ensureAttempt(attemptContext, claim); err != nil {
			return errorDestinationUnclassified, fmt.Errorf("agent: persist attempt evidence identity: %w", err)
		}
	}

	authority := localAuthority{deadline: claimStarted.Add(claim.Lease.LeaseTTL)}
	watch := lifecycle.dependencies.watchdog.Start(attemptContext, authority, cancelAttempt)
	defer watch.Stop()

	renewalErrors := make(chan destinationError, 1)
	renewalDone := make(chan struct{})
	go func() {
		defer close(renewalDone)
		lifecycle.renewalLoop(attemptContext, claim, authority, renewalErrors, watch)
	}()

	completed := make(chan runOutcome, 1)
	go func() {
		lifecycle.dependencies.observer.setAttempt(attemptID, AttemptRunning, nil)
		result, err := lifecycle.runProcess(executionContext, claim)
		var durabilityErr error
		if lifecycle.dependencies.outbox != nil {
			durabilityErr = lifecycle.dependencies.outbox.storeCompletion(
				context.WithoutCancel(attemptContext), attemptID, toL1Result(result), lifecycle.dependencies.clock.Now(),
			)
		}
		completed <- runOutcome{result: result, err: err, durabilityErr: durabilityErr}
	}()

	var outcome runOutcome
	select {
	case <-ctx.Done():
		lifecycle.dependencies.observer.setAttempt(attemptID, AttemptReaping, ctx.Err())
		cancelAttempt(ctx.Err())
		<-completed
		<-renewalDone
		return errorDestinationUnclassified, ctx.Err()
	case failure := <-renewalErrors:
		lifecycle.dependencies.observer.setAttempt(attemptID, AttemptReaping, failure.err)
		cancelAttempt(failure.err)
		<-completed
		<-renewalDone
		return failure.destination, fmt.Errorf("agent: renew lease: %w", failure.err)
	case err := <-watch.Failures():
		lifecycle.dependencies.observer.setAttempt(attemptID, AttemptReaping, err)
		cancelAttempt(err)
		<-completed
		<-renewalDone
		return errorDestinationAttemptAuthority, fmt.Errorf("agent: authority watchdog: %w", err)
	case outcome = <-completed:
		cancelExecution()
	}
	if cause := context.Cause(attemptContext); errors.Is(cause, errAuthorityDeadlineExceeded) {
		lifecycle.dependencies.observer.setAttempt(attemptID, AttemptReaping, cause)
		<-renewalDone
		return errorDestinationAttemptAuthority, fmt.Errorf("agent: authority watchdog: %w", cause)
	}
	lifecycle.dependencies.observer.setAttempt(attemptID, AttemptFinalizing, outcome.err)
	if outcome.durabilityErr != nil {
		return errorDestinationUnclassified, fmt.Errorf("agent: persist durable completion: %w", outcome.durabilityErr)
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
		cancelAttempt(nil)
	case renewalFailure := <-renewalErrors:
		cancelAttempt(renewalFailure.err)
		completionFailure = <-completionDone
		<-renewalDone
		if completionFailure.err != nil {
			return renewalFailure.destination, fmt.Errorf("agent: renew lease while completing: %w", renewalFailure.err)
		}
	case err := <-watch.Failures():
		lifecycle.dependencies.observer.setAttempt(attemptID, AttemptReaping, err)
		cancelAttempt(err)
		<-completionDone
		<-renewalDone
		return errorDestinationAttemptAuthority, fmt.Errorf("agent: authority watchdog while completing: %w", err)
	case <-ctx.Done():
		lifecycle.dependencies.observer.setAttempt(attemptID, AttemptReaping, ctx.Err())
		cancelAttempt(ctx.Err())
		<-completionDone
		<-renewalDone
		return errorDestinationUnclassified, ctx.Err()
	}
	<-renewalDone
	if cause := context.Cause(attemptContext); errors.Is(cause, errAuthorityDeadlineExceeded) {
		lifecycle.dependencies.observer.setAttempt(attemptID, AttemptReaping, cause)
		return errorDestinationAttemptAuthority, fmt.Errorf("agent: authority watchdog while completing: %w", cause)
	}
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
			if protocolErrorCode(err) == contract.ErrorLeaseExpired {
				if lifecycle.dependencies.outbox != nil {
					if releaseErr := lifecycle.dependencies.outbox.completionDelivered(context.WithoutCancel(ctx), claim.Lease.AttemptID); releaseErr != nil {
						return destinationError{destination: errorDestinationUnclassified, err: releaseErr}
					}
				}
				return destinationError{destination: errorDestinationAttemptAuthority, err: err}
			}
			classification := classifyAgentProtocolError(err)
			if classification.destination != errorDestinationTransient {
				if classification.destination == errorDestinationAttemptAuthority && lifecycle.dependencies.outbox != nil {
					if sealErr := lifecycle.dependencies.outbox.sealAttemptEvidence(
						context.WithoutCancel(ctx), claim.Lease.AttemptID,
						"attempt authority no longer accepts completion evidence", protocolErrorCode(err),
					); sealErr != nil {
						return destinationError{destination: errorDestinationUnclassified, err: errors.Join(err, sealErr)}
					}
				}
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
		if lifecycle.dependencies.outbox != nil {
			if err := lifecycle.dependencies.outbox.completionDelivered(context.WithoutCancel(ctx), claim.Lease.AttemptID); err != nil {
				return destinationError{destination: errorDestinationUnclassified, err: err}
			}
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
	if lifecycle.dependencies.preflight != nil {
		if failure := lifecycle.dependencies.preflight(claim); failure != nil {
			return contract.ProcessResult{SpawnError: failure}, nil
		}
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
		if claim.Job.Spec.Class == contract.JobClassService {
			localSink = bestEffortOutputSink{
				sink: localSink,
				report: func(err error) {
					if lifecycle.dependencies.logf != nil {
						lifecycle.dependencies.logf("service console mirror: %v", err)
					}
				},
			}
		}
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
	idlePolicy := processrunner.MonitorIdle
	if claim.Job.Spec.Class == contract.JobClassService {
		idlePolicy = processrunner.IgnoreIdle
	}
	result, runErr := lifecycle.dependencies.runner.Run(ctx, processrunner.Request{
		AttemptID:  claim.Lease.AttemptID,
		Class:      claim.Job.Spec.Class,
		Execution:  execution,
		Limits:     claim.Job.Spec.Limits,
		IdlePolicy: idlePolicy,
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

func (lifecycle *attemptLifecycle) renewalLoop(ctx context.Context, claim l1.Claim, authority localAuthority, failures chan<- destinationError, watch attemptWatch) {
	lease := claim.Lease
	nextDelay := renewalDelay(authority.deadline.Sub(lifecycle.dependencies.clock.Now()), lifecycle.dependencies.renewalInterval)
	for {
		timer := lifecycle.dependencies.clock.NewTimer(nextDelay)
		select {
		case <-ctx.Done():
			stopTimer(timer)
			return
		case <-timer.C():
		}
		if err := watch.Check(); err != nil {
			return
		}
		requestStarted := lifecycle.dependencies.clock.Now()
		remaining := authority.deadline.Sub(requestStarted)
		if remaining <= 0 {
			return
		}
		timeout := renewalRequestTimeout(remaining, lifecycle.dependencies.client.operationTimeout)
		if timeout <= 0 {
			return
		}
		renewContext, cancelRenew := context.WithTimeout(ctx, timeout)
		updated, err := lifecycle.dependencies.client.Renew(renewContext, claim.Job.JobID, lease.AttemptID, lease.FencingToken)
		cancelRenew()
		if err != nil {
			classification := classifyAgentProtocolError(err)
			if classification.destination == errorDestinationTransient {
				remaining := authority.deadline.Sub(lifecycle.dependencies.clock.Now())
				if remaining > 0 {
					nextDelay = min(lifecycle.dependencies.completionRetry, remaining)
					continue
				}
				_ = watch.Check()
				return
			}
			select {
			case failures <- destinationError{destination: classification.destination, err: err}:
			default:
			}
			return
		}
		lease = updated
		authority = localAuthority{deadline: requestStarted.Add(updated.LeaseTTL)}
		watch.Renewed(authority)
		nextDelay = renewalDelay(authority.deadline.Sub(lifecycle.dependencies.clock.Now()), lifecycle.dependencies.renewalInterval)
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

func renewalDelay(remaining, configured time.Duration) time.Duration {
	halfRemaining := remaining / 2
	if halfRemaining <= 0 {
		return time.Millisecond
	}
	if configured < halfRemaining {
		return configured
	}
	return halfRemaining
}

func renewalRequestTimeout(remaining, operationTimeout time.Duration) time.Duration {
	timeout := remaining / 2
	if operationTimeout < timeout {
		timeout = operationTimeout
	}
	if timeout <= 0 {
		return 0
	}
	return timeout
}

func toL1Result(result contract.ProcessResult) l1.ProcessResult {
	return l1.ProcessResult{
		SpawnError: result.SpawnError, OutputError: result.OutputError, ExitCode: result.ExitCode,
		Signal: result.Signal, TerminationCause: result.TerminationCause,
	}
}
