package agent

import (
	"context"
	"errors"
	"fmt"
	"net"
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
	client                 *Client
	runner                 ProcessRunner
	outbox                 *evidenceOutbox
	watchdog               attemptWatchdog
	clock                  Clock
	renewalInterval        time.Duration
	completionRetry        time.Duration
	outputSinkFactory      OutputSinkFactory
	managedResource        managedResourceManager
	handoffs               *handoffManager
	nodeID                 string
	workflowBridge         func(context.Context, contract.ExecutionSpec) (*workflowBridge, error)
	logf                   func(string, ...any)
	observer               *lifecycleObserver
	reservePublishedPort   func(l1.Claim) (net.Listener, *contract.SpawnFailure)
	prepareServiceEndpoint func(context.Context) (serviceRuntimeEndpoint, error)
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

type handoffLockResult struct {
	unlock func()
	err    error
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

	if lifecycle.dependencies.handoffs != nil && claim.Job.Spec.Class == contract.JobClassOneShot {
		locked := make(chan handoffLockResult, 1)
		go func() {
			unlock, err := lifecycle.dependencies.handoffs.lock(attemptContext, claim.Job.Spec)
			locked <- handoffLockResult{unlock: unlock, err: err}
		}()
		select {
		case result := <-locked:
			if result.err != nil {
				<-renewalDone
				if errors.Is(result.err, errAuthorityDeadlineExceeded) {
					return errorDestinationAttemptAuthority, fmt.Errorf("agent: acquire handoff path: %w", result.err)
				}
				return errorDestinationUnclassified, fmt.Errorf("agent: acquire handoff path: %w", result.err)
			}
			defer result.unlock()
		case failure := <-renewalErrors:
			cancelAttempt(failure.err)
			releaseHandoffLock(<-locked)
			<-renewalDone
			return failure.destination, fmt.Errorf("agent: renew lease while acquiring handoff path: %w", failure.err)
		case err := <-watch.Failures():
			cancelAttempt(err)
			releaseHandoffLock(<-locked)
			<-renewalDone
			return errorDestinationAttemptAuthority, fmt.Errorf("agent: authority watchdog while acquiring handoff path: %w", err)
		case <-ctx.Done():
			cancelAttempt(ctx.Err())
			releaseHandoffLock(<-locked)
			<-renewalDone
			return errorDestinationUnclassified, ctx.Err()
		}
	}

	completed := make(chan runOutcome, 1)
	go func() {
		if claim.Job.Spec.Class == contract.JobClassOneShot {
			lifecycle.dependencies.observer.setAttempt(attemptID, AttemptRunning, nil)
		}
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
	var routed *routedDestinationError
	if errors.As(outcome.err, &routed) && routed.destination != errorDestinationUnclassified {
		lifecycle.dependencies.observer.setAttempt(attemptID, AttemptReaping, outcome.err)
		cancelAttempt(outcome.err)
		<-renewalDone
		return routed.destination, outcome.err
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

func releaseHandoffLock(result handoffLockResult) {
	if result.unlock != nil {
		result.unlock()
	}
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
	executionSpec := claim.Job.Spec.Execution
	runtimeDirectory := ""
	var publishedListener net.Listener
	var endpoint serviceRuntimeEndpoint
	portfulService := claim.Job.Spec.Class == contract.JobClassService && claim.Job.Spec.PublishedPort != nil
	if claim.Job.Spec.Class == contract.JobClassService {
		if lifecycle.dependencies.managedResource == nil {
			err := errors.New("managed resource is not configured for service jobs")
			return spawnFailure(contract.SpawnFailureManagedResourcePreparation, err), err
		}
		resource, cleanupResource, err := lifecycle.dependencies.managedResource.prepareAttempt(claim.Job.JobID, claim.Lease.AttemptID)
		if err != nil {
			return spawnFailure(contract.SpawnFailureManagedResourcePreparation, err), err
		}
		defer cleanupResource()
		executionSpec.Env = cloneEnvironment(executionSpec.Env)
		executionSpec.SensitiveEnv = cloneEnvironment(executionSpec.SensitiveEnv)
		delete(executionSpec.SensitiveEnv, contract.EnvServiceDir)
		executionSpec.Env[contract.EnvServiceDir] = resource.dataDirectory
		runtimeDirectory = resource.runtimeDirectory
	}
	if portfulService {
		if lifecycle.dependencies.reservePublishedPort == nil {
			err := errors.New("published-port reservation is not configured")
			return spawnFailure(contract.SpawnFailureProcessRequest, err), err
		}
		var failure *contract.SpawnFailure
		publishedListener, failure = lifecycle.dependencies.reservePublishedPort(claim)
		if failure != nil {
			return contract.ProcessResult{SpawnError: failure}, nil
		}
		if publishedListener == nil {
			err := errors.New("published-port reservation returned no listener")
			return spawnFailure(contract.SpawnFailureProcessRequest, err), err
		}
		defer publishedListener.Close()
		if lifecycle.dependencies.prepareServiceEndpoint == nil {
			err := errors.New("service runtime endpoint adapter is not configured")
			return spawnFailure(contract.SpawnFailureProcessRequest, err), err
		}
		var err error
		endpoint, err = lifecycle.dependencies.prepareServiceEndpoint(ctx)
		if err != nil {
			return spawnFailure(contract.SpawnFailureProcessRequest, err), err
		}
		executionSpec.Env = cloneEnvironment(executionSpec.Env)
		executionSpec.SensitiveEnv = cloneEnvironment(executionSpec.SensitiveEnv)
		for name, value := range endpoint.environment {
			delete(executionSpec.SensitiveEnv, name)
			executionSpec.Env[name] = value
		}
		lifecycle.dependencies.observer.configurePortfulAttempt(claim.Lease.AttemptID)
	}
	if lifecycle.dependencies.handoffs != nil && claim.Job.Spec.Class == contract.JobClassOneShot {
		if err := lifecycle.dependencies.handoffs.prepare(claim.Job.Spec, lifecycle.dependencies.nodeID); err != nil {
			return spawnFailure(contract.SpawnFailureHandoffPreparation, err), err
		}
	}
	execution, cleanupExecutable, err := materializeExecutable(executionSpec, claim.Lease.AttemptID, runtimeDirectory)
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
	request := processrunner.Request{
		AttemptID:  claim.Lease.AttemptID,
		Class:      claim.Job.Spec.Class,
		Execution:  execution,
		Limits:     claim.Job.Spec.Limits,
		IdlePolicy: idlePolicy,
	}
	if claim.Job.Spec.Class == contract.JobClassService && !portfulService {
		request.Started = func() {
			lifecycle.dependencies.observer.setAttempt(claim.Lease.AttemptID, AttemptRunning, nil)
		}
	}
	var result contract.ProcessResult
	var runErr error
	if portfulService {
		config := serviceSupervisorConfig{
			clock: lifecycle.dependencies.clock,
			onReadiness: func(startupSatisfied, ready bool) {
				lifecycle.dependencies.observer.setServiceReadiness(claim.Lease.AttemptID, startupSatisfied, false)
			},
			onForwarding: func(ready bool) {
				lifecycle.dependencies.observer.setServiceReadiness(claim.Lease.AttemptID, true, ready)
			},
		}
		if lifecycle.dependencies.client != nil {
			config.publish = func(ctx context.Context, ready bool) error {
				_, err := lifecycle.dependencies.client.SetAttemptPublication(
					ctx,
					claim.Job.JobID,
					claim.Lease.AttemptID,
					l1.PublicationRequest{FencingToken: claim.Lease.FencingToken, Ready: &ready},
				)
				return err
			}
		}
		result, runErr = runPortfulService(
			ctx, lifecycle.dependencies.runner, request, sink, publishedListener, endpoint, config,
		)
	} else {
		result, runErr = lifecycle.dependencies.runner.Run(ctx, request, sink)
	}
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
