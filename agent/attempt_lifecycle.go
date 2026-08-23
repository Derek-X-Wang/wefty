package agent

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
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

// AttemptDeadmanRenewer is the optional agent-to-OCI-helper renewal hook. Its
// implementation queues evidence into the helper client's heartbeat pump; it
// must never perform an independent L1 renewal.
type AttemptDeadmanRenewer interface {
	QueueSuccessfulRenewal(l1.Claim, time.Duration) error
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
	runtimes               workloadRuntimeSet
	outbox                 *evidenceOutbox
	watchdog               attemptWatchdog
	clock                  Clock
	renewalInterval        time.Duration
	completionRetry        time.Duration
	finalizationTimeout    time.Duration
	outputSinkFactory      OutputSinkFactory
	managedResource        managedResourceManager
	handoffs               *handoffManager
	nodeID                 string
	bootSessionID          string
	workflowBridge         func(context.Context, contract.ExecutionSpec) (*workflowBridge, error)
	logf                   func(string, ...any)
	observer               *lifecycleObserver
	reservePublishedPort   func(l1.Claim) (net.Listener, *contract.SpawnFailure)
	prepareServiceEndpoint func(context.Context) (serviceRuntimeEndpoint, error)
	prepareAuthorityLoss   func(context.Context, string) error
	allowsStart            func(contract.JobSpec) bool
	runtimeReaped          func(string, workloadrunner.ReapReceipt, error)
	attemptDeadman         AttemptDeadmanRenewer
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
	// A zero finalization timeout would produce an already-expired context and
	// fail every attempt's final log flush, so it defaults rather than trusting
	// each construction site to supply it.
	dependencies.finalizationTimeout = durationOrDefault(dependencies.finalizationTimeout, DefaultFinalizationTimeout)
	return &attemptLifecycle{dependencies: dependencies}
}

type runOutcome struct {
	result        contract.ProcessResult
	err           error
	durabilityErr error
}

// attemptFinalization keeps log delivery alive after execution cancellation,
// but does not start its timeout until the payload has actually returned and
// finalization begins. cancel remains available to authority-loss and removal
// paths, where L1 can no longer accept the attempt's pending evidence.
type attemptFinalization struct {
	context context.Context
	cancel  context.CancelCauseFunc
	timeout time.Duration
}

func newAttemptFinalization(parent context.Context, timeout time.Duration) *attemptFinalization {
	finalizationContext, cancel := context.WithCancelCause(context.WithoutCancel(parent))
	return &attemptFinalization{context: finalizationContext, cancel: cancel, timeout: timeout}
}

func (finalization *attemptFinalization) begin() (context.Context, func()) {
	// The bounded finalization anchor must remain usable for runtime reaping
	// even when removal or authority loss canceled the upload context. Existing
	// uploads still observe finalization.context cancellation directly.
	boundedContext, cancelBound := context.WithTimeout(context.WithoutCancel(finalization.context), finalization.timeout)
	stopPropagation := context.AfterFunc(boundedContext, func() {
		// A log upload may already be retrying on the attempt-long context.
		// Propagate the finalization deadline so that operation is bounded too.
		finalization.cancel(context.Cause(boundedContext))
	})
	return boundedContext, func() {
		if cause := context.Cause(boundedContext); cause != nil && stopPropagation() {
			// CloseContext may observe the deadline before AfterFunc runs. Make
			// cancellation of an upload already using the base context synchronous.
			finalization.cancel(cause)
		}
		cancelBound()
	}
}

func (finalization *attemptFinalization) stop() {
	finalization.cancel(context.Canceled)
}

var (
	errAttemptDirectiveStop    = errors.New("attempt directive: stop")
	errAttemptDirectiveRestart = errors.New("attempt directive: restart")
)

func (lifecycle *attemptLifecycle) execute(ctx context.Context, claim l1.Claim, _ time.Time) (errorDestination, error) {
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

	// Start the local monotonic lease window when the claim response is actually
	// available. Claim RPC and admission latency can otherwise burn
	// most of a short lease under instrumentation before renewal even begins.
	authority := localAuthority{deadline: lifecycle.dependencies.clock.Now().Add(claim.Lease.LeaseTTL)}
	watch := lifecycle.dependencies.watchdog.Start(attemptContext, authority, cancelAttempt)
	defer watch.Stop()

	renewalErrors := make(chan destinationError, 1)
	renewalDone := make(chan struct{})
	go func() {
		defer close(renewalDone)
		lifecycle.renewalLoop(attemptContext, claim, authority, renewalErrors, watch)
	}()

	// The attempt-long context is uncancelable by execution shutdown so final
	// output can still be flushed. Its timeout is deliberately not attached
	// yet: payload uptime is not part of the finalization phase. Authority loss
	// and removal can still cancel this context outright below.
	finalization := newAttemptFinalization(attemptContext, lifecycle.dependencies.finalizationTimeout)
	defer finalization.stop()

	var handoffUnlock func()
	defer func() {
		if handoffUnlock != nil {
			handoffUnlock()
		}
	}()
	completed := make(chan runOutcome, 1)
	go func() {
		if claim.Job.Spec.Class == contract.JobClassOneShot && claim.Job.Spec.Kind != contract.JobKindOCI {
			lifecycle.dependencies.observer.setAttempt(attemptID, AttemptRunning, nil)
		}
		result, err := lifecycle.runWorkloadContexts(executionContext, finalization, claim, func(unlock func()) {
			handoffUnlock = unlock
		})
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
		cause := context.Cause(ctx)
		lifecycle.dependencies.observer.setAttempt(attemptID, AttemptReaping, cause)
		cancelExecution()
		cancelAttempt(cause)
		if errors.Is(cause, errServiceRemovalRequested) {
			// L1 scrubbed this job's log rows in the same transaction that
			// accepted the removal request, so its pending events can never be
			// accepted again — the sequence-continuity check rejects them and
			// that rejection classifies as transient, so the uploader would
			// retry until the finalization bound expired. There is nothing to
			// deliver to a control plane that has already deleted the
			// destination; the events stay durable in the spool until the
			// removal controller purges it.
			finalization.stop()
		}
		outcome := <-completed
		<-renewalDone
		if errors.Is(cause, errServiceRemovalRequested) {
			// The removal controller persists its local removing record before
			// canceling this context and waits here for the guardian's positive
			// reap before it purges spool metadata or invokes managedroot.Remove.
			return errorDestinationUnclassified, nil
		}
		result := outcome.result
		if result.Signal == "" && result.SpawnError == nil && result.OutputError == "" {
			result = contract.ProcessResult{Signal: "terminated", TerminationCause: contract.TerminationCauseAgent}
		}
		request := l1.CompletionRequest{
			FencingToken:   claim.Lease.FencingToken,
			IdempotencyKey: "completion:" + claim.Lease.AttemptID,
			Result:         toL1Result(result),
		}
		finalizationContext, cancelFinalization := context.WithTimeout(context.WithoutCancel(ctx), lifecycle.dependencies.client.operationTimeout)
		defer cancelFinalization()
		failure := lifecycle.completeWithRetry(finalizationContext, claim, request)
		if failure.err != nil && protocolErrorCode(failure.err) != contract.ErrorLeaseExpired {
			return failure.destination, fmt.Errorf("agent: shutdown completion: %w", failure.err)
		}
		return errorDestinationUnclassified, ctx.Err()
	case failure := <-renewalErrors:
		if errors.Is(failure.err, errAttemptDirectiveStop) || errors.Is(failure.err, errAttemptDirectiveRestart) {
			lifecycle.dependencies.observer.setAttempt(attemptID, AttemptReaping, failure.err)
			cancelExecution()
			cancelAttempt(failure.err)
			outcome := <-completed
			<-renewalDone
			result := outcome.result
			if result.Signal == "" && result.SpawnError == nil && result.OutputError == "" {
				result = contract.ProcessResult{Signal: "terminated", TerminationCause: contract.TerminationCauseAgent}
			}
			request := l1.CompletionRequest{FencingToken: claim.Lease.FencingToken, IdempotencyKey: "completion:" + claim.Lease.AttemptID, Result: toL1Result(result)}
			finalizationContext, cancelFinalization := context.WithTimeout(context.WithoutCancel(ctx), lifecycle.dependencies.client.operationTimeout)
			defer cancelFinalization()
			completionFailure := lifecycle.completeWithRetry(finalizationContext, claim, request)
			if completionFailure.err != nil {
				return completionFailure.destination, fmt.Errorf("agent: directive completion: %w", completionFailure.err)
			}
			return errorDestinationUnclassified, nil
		}
		lifecycle.dependencies.observer.setAttempt(attemptID, AttemptReaping, failure.err)
		cancelAttempt(failure.err)
		// Authority is gone, so L1 will not accept more of this attempt's
		// output. Leaving finalization running would retry until its bound
		// expired — and after a removal the rejection is a sequence conflict,
		// which classifies as transient and retries indefinitely. The events
		// stay durable in the spool either way.
		finalization.stop()
		<-completed
		<-renewalDone
		return failure.destination, fmt.Errorf("agent: renew lease: %w", failure.err)
	case err := <-watch.Failures():
		lifecycle.dependencies.observer.setAttempt(attemptID, AttemptReaping, err)
		cancelAttempt(err)
		finalization.stop()
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
	completionContext := attemptContext
	if cause := context.Cause(attemptContext); errors.Is(cause, errAttemptDirectiveStop) || errors.Is(cause, errAttemptDirectiveRestart) {
		completionContext = context.WithoutCancel(attemptContext)
	}
	go func() { completionDone <- lifecycle.completeWithRetry(completionContext, claim, request) }()
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

func (lifecycle *attemptLifecycle) runWorkload(ctx context.Context, claim l1.Claim) (contract.ProcessResult, error) {
	finalization := newAttemptFinalization(ctx, lifecycle.dependencies.finalizationTimeout)
	defer finalization.stop()
	return lifecycle.runWorkloadContexts(ctx, finalization, claim, nil)
}

func (lifecycle *attemptLifecycle) runWorkloadContexts(
	ctx context.Context,
	finalization *attemptFinalization,
	claim l1.Claim,
	retainHandoffLock func(func()),
) (contract.ProcessResult, error) {
	if err := contract.CheckWorkloadClass(claim.Job.Spec.Class); err != nil {
		return spawnFailure(contract.SpawnFailureUnsupportedClass, err), err
	}
	if lifecycle.dependencies.allowsStart != nil && !lifecycle.dependencies.allowsStart(claim.Job.Spec) {
		return contract.ProcessResult{SpawnError: &contract.SpawnFailure{
			Code: contract.SpawnFailureRuntimeUnavailable, Message: "local capability observation suppresses this workload start",
		}}, nil
	}
	runtimeAdapter, found := lifecycle.dependencies.runtimes.selectKind(claim.Job.Spec.Kind)
	if !found {
		err := &contract.ExecutionError{Kind: claim.Job.Spec.Kind}
		return spawnFailure(contract.SpawnFailureUnsupportedKind, err), err
	}
	authority := workloadrunner.AttemptAuthority{
		NodeID: lifecycle.dependencies.nodeID, BootSessionID: lifecycle.dependencies.bootSessionID,
		JobID: claim.Job.JobID, AttemptID: claim.Lease.AttemptID, FencingToken: claim.Lease.FencingToken,
		WorkloadClass: claim.Job.Spec.Class, RemovalGeneration: "attempt",
	}
	idlePolicy := workloadrunner.MonitorIdle
	if claim.Job.Spec.Class == contract.JobClassService {
		idlePolicy = workloadrunner.IgnoreIdle
	}
	request := workloadrunner.Request{
		Authority: authority, RuntimeHandler: claim.Job.Spec.RuntimeHandler,
		Execution: claim.Job.Spec.Execution, Limits: claim.Job.Spec.Limits,
		IdlePolicy: idlePolicy, InitialDeadman: claim.Lease.LeaseTTL,
	}
	if claim.Job.Spec.Kind == contract.JobKindOCI {
		request.OCIImagePulling = func() {
			lifecycle.dependencies.observer.setAttempt(claim.Lease.AttemptID, AttemptPulling, nil)
		}
		request.OCIImageReady = func() {
			lifecycle.dependencies.observer.setAttempt(claim.Lease.AttemptID, AttemptStarting, nil)
		}
		request.OCIStarted = func(startContext context.Context, observation workloadrunner.OCIImageObservation) error {
			if lifecycle.dependencies.client == nil {
				return errors.New("OCI Started acknowledgement requires an L1 client")
			}
			_, err := lifecycle.dependencies.client.ObserveAttemptImage(startContext, claim.Job.JobID, claim.Lease.AttemptID, l1.ImageObservationRequest{
				FencingToken:           claim.Lease.FencingToken,
				SubmittedReference:     observation.SubmittedReference,
				TopLevelDigest:         observation.TopLevelDigest,
				TopLevelMediaType:      observation.TopLevelMediaType,
				IndexDigest:            observation.IndexDigest,
				PlatformManifestDigest: observation.PlatformManifestDigest,
				Platform:               l1.OCIPlatform{OS: observation.PlatformOS, Architecture: observation.PlatformArchitecture, Variant: observation.PlatformVariant},
				RuntimeHandler:         observation.RuntimeHandler, Snapshotter: observation.Snapshotter,
			})
			if err != nil {
				return fmt.Errorf("record OCI image observation: %w", err)
			}
			if _, err := lifecycle.dependencies.client.StartAttempt(startContext, claim.Job.JobID, claim.Lease.AttemptID, l1.StartedRequest{FencingToken: claim.Lease.FencingToken}); err != nil {
				return fmt.Errorf("acknowledge OCI Started: %w", err)
			}
			lifecycle.dependencies.observer.setAttempt(claim.Lease.AttemptID, AttemptRunning, nil)
			return nil
		}
	}
	if claim.Job.Spec.Class == contract.JobClassService {
		request.LifetimeBoundary = workloadrunner.AgentBootLifetime
	}
	portfulService := claim.Job.Spec.Class == contract.JobClassService && claim.Job.Spec.PublishedPort != nil
	if claim.Job.Spec.Class == contract.JobClassService && !portfulService {
		request.Started = func() {
			lifecycle.dependencies.observer.setAttempt(claim.Lease.AttemptID, AttemptRunning, nil)
		}
	}
	admission, preflightResult, preflightErr := runtimeAdapter.Preflight(ctx, request)
	if admission.Release != nil {
		defer admission.Release()
	}
	request = admission.Request
	var uploader *batchingLogSink
	var redactingSink *redactingOutputSink
	var managedResources workloadrunner.ManagedResources
	finish := func(result contract.ProcessResult, runErr error) (contract.ProcessResult, error) {
		finalizationContext, cancelFinalization := finalization.begin()
		defer cancelFinalization()
		reapReceipt, reapErr := runtimeAdapter.ReapAndVerify(finalizationContext, workloadrunner.ReapRequest{
			Authority: authority, ManagedResources: managedResources,
		})
		if reapErr == nil && !reapReceipt.RuntimeQuiesced {
			reapErr = errors.New("workload runtime did not verify quiescence")
		}
		if reapErr == nil && reapReceipt.Evidence == "" {
			reapErr = errors.New("workload runtime reap receipt has no evidence kind")
		}
		if lifecycle.dependencies.runtimeReaped != nil {
			lifecycle.dependencies.runtimeReaped(claim.Job.JobID, reapReceipt, reapErr)
		}
		if reapErr != nil {
			reapErr = fmt.Errorf("reap and verify workload runtime: %w", reapErr)
		}
		var outputErr error
		if redactingSink != nil {
			outputErr = redactingSink.Flush(finalizationContext)
		}
		var uploadErr error
		if uploader != nil {
			uploadErr = uploader.CloseContext(finalizationContext)
		}
		if outputErr != nil {
			outputErr = fmt.Errorf("flush redacted output: %w", outputErr)
		}
		if uploadErr != nil {
			uploadErr = fmt.Errorf("upload logs: %w", uploadErr)
		}
		finalizationErr := errors.Join(reapErr, outputErr, uploadErr)
		if finalizationErr != nil {
			result = contract.ProcessResult{OutputError: finalizationErr.Error()}
		}
		return result, errors.Join(runErr, finalizationErr)
	}
	if preflightErr != nil {
		return finish(preflightResult.Outcome, preflightErr)
	}
	if lifecycle.dependencies.handoffs != nil && claim.Job.Spec.Class == contract.JobClassOneShot {
		unlock, err := lifecycle.dependencies.handoffs.lock(ctx, claim.Job.Spec)
		if err != nil {
			return finish(spawnFailure(contract.SpawnFailureHandoffPreparation, err), err)
		}
		if retainHandoffLock != nil {
			retainHandoffLock(unlock)
		} else {
			defer unlock()
		}
	}
	executionSpec := request.Execution
	var publishedListener net.Listener
	var endpoint serviceRuntimeEndpoint
	if claim.Job.Spec.Class == contract.JobClassService {
		if lifecycle.dependencies.managedResource == nil {
			err := errors.New("managed resource is not configured for service jobs")
			return finish(spawnFailure(contract.SpawnFailureManagedResourcePreparation, err), err)
		}
		resource, cleanupResource, err := lifecycle.dependencies.managedResource.prepareAttempt(claim.Job.JobID, claim.Lease.AttemptID)
		if err != nil {
			return finish(spawnFailure(contract.SpawnFailureManagedResourcePreparation, err), err)
		}
		defer cleanupResource()
		managedResources = resource
		request.ManagedResources = resource
		executionSpec.Env = cloneEnvironment(executionSpec.Env)
		executionSpec.SensitiveEnv = cloneEnvironment(executionSpec.SensitiveEnv)
		delete(executionSpec.SensitiveEnv, contract.EnvServiceDir)
		executionSpec.Env[contract.EnvServiceDir] = resource.dataDirectory
	}
	if portfulService {
		if lifecycle.dependencies.reservePublishedPort == nil {
			err := errors.New("published-port reservation is not configured")
			return finish(spawnFailure(contract.SpawnFailureProcessRequest, err), err)
		}
		var failure *contract.SpawnFailure
		publishedListener, failure = lifecycle.dependencies.reservePublishedPort(claim)
		if failure != nil {
			return finish(contract.ProcessResult{SpawnError: failure}, nil)
		}
		if publishedListener == nil {
			err := errors.New("published-port reservation returned no listener")
			return finish(spawnFailure(contract.SpawnFailureProcessRequest, err), err)
		}
		defer publishedListener.Close()
		if lifecycle.dependencies.prepareServiceEndpoint == nil {
			err := errors.New("service runtime endpoint adapter is not configured")
			return finish(spawnFailure(contract.SpawnFailureProcessRequest, err), err)
		}
		var err error
		endpoint, err = lifecycle.dependencies.prepareServiceEndpoint(ctx)
		if err != nil {
			return finish(spawnFailure(contract.SpawnFailureProcessRequest, err), err)
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
			return finish(spawnFailure(contract.SpawnFailureHandoffPreparation, err), err)
		}
	}
	var err error
	var bridge *workflowBridge
	if lifecycle.dependencies.workflowBridge != nil && claim.Job.Spec.Class == contract.JobClassOneShot {
		bridge, err = lifecycle.dependencies.workflowBridge(ctx, executionSpec)
		if err != nil {
			return finish(spawnFailure(contract.SpawnFailureWorkflowBridgeCreation, err), err)
		}
	}
	if bridge != nil {
		defer bridge.close()
		executionSpec.Env = cloneEnvironment(executionSpec.Env)
		delete(executionSpec.Env, "WEFTY_L1_ENDPOINT")
		executionSpec.Env[contract.EnvL3Endpoint] = bridge.l3Endpoint
	}
	var sinks multiOutputSink
	if lifecycle.dependencies.client != nil && lifecycle.dependencies.outbox != nil {
		uploader, err = lifecycle.dependencies.outbox.newLogSink(finalization.context, lifecycle.dependencies.client, claim)
		if err != nil {
			return finish(spawnFailure(contract.SpawnFailureLogSinkSetup, err), err)
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
	redactingSink = newRedactingOutputSink(sink, claim.Job.Spec.Execution.SensitiveEnv)
	if redactingSink != nil {
		sink = redactingSink
	}
	request.Execution = executionSpec
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
			ctx, runtimeAdapter, request, sink, publishedListener, endpoint, config,
		)
	} else {
		runtimeResult, err := runtimeAdapter.Run(ctx, request, sink)
		result, runErr = runtimeResult.Outcome, err
	}
	return finish(result, runErr)
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
			if classification.destination == errorDestinationAttemptAuthority &&
				claim.Job.Spec.Class == contract.JobClassService && lifecycle.dependencies.prepareAuthorityLoss != nil {
				if prepareErr := lifecycle.dependencies.prepareAuthorityLoss(ctx, claim.Job.JobID); prepareErr != nil {
					lifecycle.log("prepare service %s authority loss for removal: %v", claim.Job.JobID, prepareErr)
				}
			}
			select {
			case failures <- destinationError{destination: classification.destination, err: err}:
			default:
			}
			return
		}
		lease = updated
		authority = localAuthority{deadline: lifecycle.dependencies.clock.Now().Add(updated.LeaseTTL)}
		watch.Renewed(authority)
		if err := queueHelperDeadman(lifecycle.dependencies.attemptDeadman, claim, updated); err != nil {
			select {
			case failures <- destinationError{destination: errorDestinationAttemptAuthority, err: err}:
			default:
			}
			return
		}
		if updated.Directive == l1.AttemptDirectiveStop {
			returnWithDirective(failures, errAttemptDirectiveStop)
			return
		}
		if updated.Directive == l1.AttemptDirectiveRestart {
			returnWithDirective(failures, errAttemptDirectiveRestart)
			return
		}
		nextDelay = renewalDelay(authority.deadline.Sub(lifecycle.dependencies.clock.Now()), lifecycle.dependencies.renewalInterval)
	}
}

func queueHelperDeadman(renewer AttemptDeadmanRenewer, claim l1.Claim, lease l1.AttemptLease) error {
	if renewer == nil || claim.Job.Spec.Kind != contract.JobKindOCI || lease.Directive != "" {
		return nil
	}
	return renewer.QueueSuccessfulRenewal(claim, lease.LeaseTTL)
}

func returnWithDirective(failures chan<- destinationError, directive error) {
	select {
	case failures <- destinationError{destination: errorDestinationUnclassified, err: directive}:
	default:
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
		SpawnError: result.SpawnError, RuntimeFailure: result.RuntimeFailure, OutputError: result.OutputError, ExitCode: result.ExitCode,
		Signal: result.Signal, TerminationCause: result.TerminationCause, OOM: result.OOM, LogEvidenceIncomplete: result.LogEvidenceIncomplete,
	}
}
