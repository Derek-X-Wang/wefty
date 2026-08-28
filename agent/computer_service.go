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
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
)

type computerServiceConfig struct {
	clock        Clock
	fabric       fabric.Fabric
	authorizer   *ComputerPolicyCache
	auditor      computerTakeoverAuditor
	computerID   string
	jobID        string
	attemptID    string
	fencingToken string
	dial         computerEndpointDial
	publish      func(context.Context, bool, string) error
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
	startupSignalErrors := make(chan error, 1)
	var startedOnce sync.Once
	priorStarted := request.Started
	request.Started = func() {
		startedOnce.Do(func() {
			// Capture the authoritative Started edge before any post-start guest
			// signal I/O. Scheduling or helper latency must not extend the exact
			// 60-second image-readiness budget.
			startedAt := clock.Now()
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
			started <- startedAt
		})
	}
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
	case <-frontDoor.Errors():
		frontDoorErr := frontDoor.takeErrors()
		stopErr := stop(false, nil)
		outcome := <-outcomes
		failure := fmt.Errorf("private Computer front door failed: %w", frontDoorErr)
		return spawnFailure(contract.SpawnFailureRuntimeUnavailable, failure), errors.Join(failure, outcome.err, stopErr)
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
		probeContext, cancel := context.WithTimeout(ctx, DefaultComputerReadinessConnectTimeout)
		nextReady := probeComputerBackendPairOnce(probeContext, dial) == nil
		cancel()
		if nextReady != ready {
			ready = nextReady
			observe(ready)
		}
	}
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
