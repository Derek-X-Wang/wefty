package agent

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/l3"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
)

type computerServiceConfig struct {
	clock          Clock
	fabric         fabric.Fabric
	authorizer     *ComputerPolicyCache
	auditor        computerTakeoverAuditor
	computerTokens ComputerTokenMinter
	submission     ComputerSubmissionAuthority
	computerID     string
	jobID          string
	attemptID      string
	fencingToken   string
	dial           computerEndpointDial
	publish        func(context.Context, bool, string) error
}

// runComputerService is the production owner of the private Computer front
// door. The guest endpoints remain process-local dial capabilities; only the
// Fabric listener address can enter L1 publication.
func runComputerService(
	ctx context.Context,
	runtimeAdapter WorkloadRuntime,
	request workloadrunner.Request,
	sink workloadrunner.OutputSink,
	config computerServiceConfig,
) (contract.ProcessResult, error) {
	clock := config.clock
	if clock == nil {
		clock = systemClock{}
	}
	if config.fabric == nil || config.authorizer == nil || config.auditor == nil || config.dial == nil {
		err := errors.New("Computer service front door dependencies are incomplete")
		return spawnFailure(contract.SpawnFailureProcessRequest, err), err
	}
	controlRuntime, ok := runtimeAdapter.(workloadrunner.OCIComputerControlRuntime)
	if !ok {
		err := errors.New("Computer runtime does not expose the attempt-fenced control-state seam")
		return spawnFailure(contract.SpawnFailureProcessRequest, err), err
	}
	listener, err := config.fabric.Listen("tcp", ":0")
	if err != nil {
		return spawnFailure(contract.SpawnFailurePublishedListener, err), fmt.Errorf("listen on private Fabric: %w", err)
	}
	defer listener.Close()
	port, err := listenerPort(listener.Addr())
	if err != nil {
		return spawnFailure(contract.SpawnFailurePublishedListener, err), err
	}
	displayEndpoint := (&url.URL{Scheme: "ws", Host: net.JoinHostPort(config.fabric.ConnectHost(), strconv.Itoa(port)), Path: computerWebSocketPath}).String()

	runContext, cancelRun := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelRun()
	frontDoor, err := newComputerFrontDoor(computerFrontDoorConfig{
		authorityContext: runContext, fabric: config.fabric, authorizer: config.authorizer, auditor: config.auditor,
		clock: clock, computerID: config.computerID, jobID: config.jobID, attemptID: config.attemptID,
		fencingToken: config.fencingToken, dial: config.dial,
	})
	if err != nil {
		return spawnFailure(contract.SpawnFailureProcessRequest, err), err
	}
	tenure, err := newControllerTenure(controllerTenureConfig{
		authorityContext: runContext,
		clock:            clock,
		dial:             config.dial,
		setControlState: func(signalContext context.Context, humanDriving bool) error {
			return controlRuntime.SetComputerControlState(signalContext, request.Authority, humanDriving)
		},
		record: func(auditContext context.Context, event l1.ComputerTakeoverAuditEvent) (l1.ComputerTakeoverAuditReceipt, error) {
			return config.auditor.AppendComputerTakeoverAudit(auditContext, config.computerID, config.jobID, config.attemptID,
				l1.ComputerTakeoverAuditRequest{FencingToken: config.fencingToken, Event: event})
		},
		report: frontDoor.report,
		onUnconfirmedClear: func(clearErr error) {
			frontDoor.report(fmt.Errorf("Computer control signal could not be confirmed clear: %w", clearErr))
		},
	})
	if err != nil {
		return spawnFailure(contract.SpawnFailureProcessRequest, err), err
	}
	frontDoor.config.controlTenure = tenure
	httpServer := &http.Server{Handler: frontDoor}
	serverErrors := make(chan error, 1)
	go func() {
		serveErr := httpServer.Serve(listener)
		if errors.Is(serveErr, http.ErrServerClosed) || errors.Is(serveErr, net.ErrClosed) {
			serveErr = nil
		}
		serverErrors <- serveErr
	}()

	publication := newPublicationController(clock, 0, 0,
		func(publishContext context.Context, ready bool) error {
			if config.publish == nil {
				return nil
			}
			return config.publish(publishContext, ready, displayEndpoint)
		}, frontDoor.SetReady)
	publicationDone := make(chan error, 1)
	go func() { publicationDone <- publication.Run(runContext) }()

	started := make(chan time.Time, 1)
	runtimeStarted := make(chan struct{})
	startupSignalErrors := make(chan error, 1)
	var startedOnce sync.Once
	var helperStartedAt time.Time
	priorOCIStartedAt := request.OCIStartedAt
	request.OCIStartedAt = func(startedAt time.Time) {
		helperStartedAt = startedAt
		if priorOCIStartedAt != nil {
			priorOCIStartedAt(startedAt)
		}
	}
	priorStarted := request.Started
	request.Started = func() {
		startedOnce.Do(func() {
			if helperStartedAt.IsZero() {
				startupSignalErrors <- errors.New("Computer runtime omitted the helper Started timestamp")
				return
			}
			startupSignalContext, cancelStartupSignal := context.WithTimeout(context.WithoutCancel(runContext), controllerTenureFinalizationLimit)
			err := controlRuntime.SetComputerControlState(startupSignalContext, request.Authority, false)
			cancelStartupSignal()
			if err != nil {
				startupSignalErrors <- fmt.Errorf("initialize Computer control signal false: %w", err)
				return
			}
			if priorStarted != nil {
				priorStarted()
			}
			close(runtimeStarted)
			started <- helperStartedAt
		})
	}
	tokenSyncErrors := make(chan error, 1)
	stopTokenSync := func() {}
	if config.computerTokens != nil {
		initial, updates, unsubscribe := config.authorizer.SubscribeComputerSubmission(config.computerID)
		stopTokenSync = unsubscribe
		go func() {
			select {
			case <-runContext.Done():
				tokenSyncErrors <- nil
				return
			case <-runtimeStarted:
			}
			tokenSyncErrors <- syncComputerTokenFile(runContext, controlRuntime, request.Authority,
				config.computerTokens, config.computerID, config.attemptID, config.submission, initial, updates)
		}()
	}
	defer stopTokenSync()
	outcomes := make(chan serviceRunOutcome, 1)
	go func() {
		result, runErr := runtimeAdapter.Run(runContext, request, sink)
		outcomes <- serviceRunOutcome{result: result.Outcome, err: runErr}
	}()
	readinessErrors := make(chan error, 1)
	go func() {
		readinessErrors <- monitorComputerReadiness(runContext, clock, started, config.dial, publication.Observe)
	}()

	stop := func(publicationFinished bool, publicationErr error) error {
		publication.Stop()
		frontDoor.EndSessions(l1.ComputerTakeoverAttemptAuthorityLost)
		frontDoor.SetReady(false)
		_ = listener.Close()
		_ = httpServer.Close()
		frontDoor.WaitForSessions()
		frontDoorErr := frontDoor.takeErrors()
		cancelRun()
		if !publicationFinished {
			publicationErr = <-publicationDone
		}
		return errors.Join(publicationErr, frontDoorErr)
	}
	select {
	case <-ctx.Done():
		stopErr := stop(false, nil)
		outcome := <-outcomes
		return outcome.result, errors.Join(outcome.err, stopErr)
	case outcome := <-outcomes:
		stopErr := stop(false, nil)
		return outcome.result, errors.Join(outcome.err, stopErr)
	case publicationErr := <-publicationDone:
		stopErr := stop(true, publicationErr)
		outcome := <-outcomes
		return outcome.result, errors.Join(outcome.err, stopErr)
	case serveErr := <-serverErrors:
		if serveErr == nil {
			serveErr = errors.New("private Computer front door stopped unexpectedly")
		}
		stopErr := stop(false, nil)
		outcome := <-outcomes
		return spawnFailure(contract.SpawnFailurePublishedListener, serveErr), errors.Join(serveErr, outcome.err, stopErr)
	case readinessErr := <-readinessErrors:
		if readinessErr == nil {
			return (<-outcomes).result, nil
		}
		stopErr := stop(false, nil)
		outcome := <-outcomes
		var typed *computerReadinessError
		if errors.As(readinessErr, &typed) {
			return spawnFailure(typed.Code, typed), errors.Join(typed, outcome.err, stopErr)
		}
		return spawnFailure(contract.SpawnFailureRuntimeUnavailable, readinessErr), errors.Join(readinessErr, outcome.err, stopErr)
	case startupSignalErr := <-startupSignalErrors:
		stopErr := stop(false, nil)
		outcome := <-outcomes
		return spawnFailure(contract.SpawnFailureRuntimeUnavailable, startupSignalErr), errors.Join(startupSignalErr, outcome.err, stopErr)
	case tokenSyncErr := <-tokenSyncErrors:
		stopErr := stop(false, nil)
		outcome := <-outcomes
		if tokenSyncErr == nil {
			return outcome.result, errors.Join(outcome.err, stopErr)
		}
		return spawnFailure(contract.SpawnFailureRuntimeUnavailable, tokenSyncErr), errors.Join(tokenSyncErr, outcome.err, stopErr)
	case <-frontDoor.Errors():
		frontDoorErr := frontDoor.takeErrors()
		stopErr := stop(false, nil)
		outcome := <-outcomes
		failure := fmt.Errorf("private Computer front door failed: %w", frontDoorErr)
		return spawnFailure(contract.SpawnFailureRuntimeUnavailable, failure), errors.Join(failure, outcome.err, stopErr)
	}
}

func syncComputerTokenFile(
	ctx context.Context,
	runtime workloadrunner.OCIComputerControlRuntime,
	authority workloadrunner.AttemptAuthority,
	minter ComputerTokenMinter,
	computerID, attemptID string,
	last ComputerSubmissionAuthority,
	initial ComputerSubmissionAuthority,
	updates <-chan ComputerSubmissionAuthority,
) error {
	apply := func(next ComputerSubmissionAuthority) error {
		if next.Enabled == last.Enabled && next.SubmitIntentRevision == last.SubmitIntentRevision &&
			next.SubmitMaxInflight == last.SubmitMaxInflight {
			return nil
		}
		if err := runtime.SetComputerToken(ctx, authority, ""); err != nil {
			return fmt.Errorf("remove superseded Computer token file: %w", err)
		}
		last = next
		if !next.Enabled {
			return nil
		}
		var grant l3.ComputerTokenGrant
		var err error
		for attempt := 0; attempt < 3; attempt++ {
			grant, err = minter.MintComputerToken(ctx, l3.ComputerTokenMintRequest{ComputerID: computerID, ComputerAttemptID: attemptID})
			if err == nil {
				break
			}
			timer := time.NewTimer(time.Duration(attempt+1) * 100 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil
			case <-timer.C:
			}
		}
		if err != nil {
			return fmt.Errorf("re-mint Computer token after submission policy change: %w", err)
		}
		if grant.Token == "" || grant.ComputerID != computerID || grant.ComputerAttemptID != attemptID ||
			grant.SubmitIntentRevision != next.SubmitIntentRevision || grant.SubmitMaxInflight != next.SubmitMaxInflight {
			return errors.New("re-minted Computer token grant is outside current submission authority")
		}
		if err := runtime.SetComputerToken(ctx, authority, grant.Token); err != nil {
			return fmt.Errorf("publish re-minted Computer token file: %w", err)
		}
		return nil
	}
	if err := apply(initial); err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case next, ok := <-updates:
			if !ok {
				return nil
			}
			if err := apply(next); err != nil {
				return err
			}
		}
	}
}

func monitorComputerReadiness(ctx context.Context, clock Clock, started <-chan time.Time, dial computerEndpointDial, observe func(bool)) error {
	if clock == nil {
		clock = systemClock{}
	}
	select {
	case <-ctx.Done():
		return &computerReadinessError{Code: contract.SpawnFailureRuntimeUnavailable, Err: fmt.Errorf("Computer runtime ended before Started: %w", context.Cause(ctx))}
	case startedAt := <-started:
		if startedAt.IsZero() {
			return &computerReadinessError{Code: contract.SpawnFailureRuntimeUnavailable, Err: errors.New("Computer runtime reported an invalid Started timestamp")}
		}
		if err := probeComputerBackends(ctx, clock, startedAt, dial); err != nil {
			return err
		}
	}
	observe(true)
	ready := true
	for {
		timer := clock.NewTimer(DefaultComputerReadinessProbeInterval)
		select {
		case <-ctx.Done():
			stopTimer(timer)
			if ready {
				observe(false)
			}
			return nil
		case <-timer.C():
		}
		probeContext, cancel := contextWithClockTimeout(ctx, clock, DefaultComputerReadinessConnectTimeout)
		nextReady := probeComputerBackendPairOnce(probeContext, dial) == nil
		cancel()
		if nextReady != ready {
			ready = nextReady
			observe(ready)
		}
	}
}

func contextWithClockTimeout(parent context.Context, clock Clock, timeout time.Duration) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	timer := clock.NewTimer(timeout)
	go func() {
		select {
		case <-ctx.Done():
			stopTimer(timer)
		case <-timer.C():
			cancel()
		}
	}()
	return ctx, cancel
}

func probeComputerBackendPairOnce(ctx context.Context, dial computerEndpointDial) error {
	for _, name := range []string{workloadrunner.AttemptEndpointView, workloadrunner.AttemptEndpointControl} {
		connection, websocketConnection, _, err := dialComputerBackend(ctx, dial, name)
		if connection != nil {
			_ = connection.Close()
		}
		if websocketConnection != nil {
			_ = websocketConnection.CloseNow()
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func listenerPort(address net.Addr) (int, error) {
	_, portText, err := net.SplitHostPort(address.String())
	if err != nil {
		return 0, fmt.Errorf("read private Computer listener address: %w", err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return 0, errors.New("private Computer listener returned an invalid port")
	}
	return port, nil
}
