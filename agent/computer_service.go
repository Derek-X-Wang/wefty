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

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
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
	if config.fabric == nil || config.authorizer == nil || config.auditor == nil || config.dial == nil {
		err := errors.New("Computer service front door dependencies are incomplete")
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
		clock: config.clock, computerID: config.computerID, jobID: config.jobID, attemptID: config.attemptID,
		fencingToken: config.fencingToken, dial: config.dial,
	})
	if err != nil {
		return spawnFailure(contract.SpawnFailureProcessRequest, err), err
	}
	httpServer := &http.Server{Handler: frontDoor}
	serverErrors := make(chan error, 1)
	go func() {
		serveErr := httpServer.Serve(listener)
		if errors.Is(serveErr, http.ErrServerClosed) || errors.Is(serveErr, net.ErrClosed) {
			serveErr = nil
		}
		serverErrors <- serveErr
	}()

	publication := newPublicationController(config.clock, 0, 0,
		func(publishContext context.Context, ready bool) error {
			if config.publish == nil {
				return nil
			}
			return config.publish(publishContext, ready, displayEndpoint)
		}, frontDoor.SetReady)
	publicationDone := make(chan error, 1)
	go func() { publicationDone <- publication.Run(runContext) }()

	started := make(chan struct{})
	var startedOnce sync.Once
	priorStarted := request.Started
	request.Started = func() {
		if priorStarted != nil {
			priorStarted()
		}
		startedOnce.Do(func() { close(started) })
	}
	outcomes := make(chan serviceRunOutcome, 1)
	go func() {
		result, runErr := runtimeAdapter.Run(runContext, request, sink)
		outcomes <- serviceRunOutcome{result: result.Outcome, err: runErr}
	}()
	readinessErrors := make(chan error, 1)
	go func() {
		readinessErrors <- monitorComputerReadiness(runContext, config.clock, started, config.dial, publication.Observe)
	}()

	stop := func() error {
		publication.Stop()
		frontDoor.SetReady(false)
		_ = listener.Close()
		_ = httpServer.Close()
		cancelRun()
		return <-publicationDone
	}
	select {
	case <-ctx.Done():
		publicationErr := stop()
		outcome := <-outcomes
		return outcome.result, errors.Join(outcome.err, publicationErr)
	case outcome := <-outcomes:
		publicationErr := stop()
		return outcome.result, errors.Join(outcome.err, publicationErr)
	case publicationErr := <-publicationDone:
		frontDoor.SetReady(false)
		_ = listener.Close()
		cancelRun()
		outcome := <-outcomes
		return outcome.result, errors.Join(outcome.err, publicationErr)
	case serveErr := <-serverErrors:
		if serveErr == nil {
			serveErr = errors.New("private Computer front door stopped unexpectedly")
		}
		_ = stop()
		outcome := <-outcomes
		return spawnFailure(contract.SpawnFailurePublishedListener, serveErr), errors.Join(serveErr, outcome.err)
	case readinessErr := <-readinessErrors:
		if readinessErr == nil {
			return (<-outcomes).result, nil
		}
		_ = stop()
		outcome := <-outcomes
		var typed *computerReadinessError
		if errors.As(readinessErr, &typed) {
			return spawnFailure(typed.Code, typed), errors.Join(typed, outcome.err)
		}
		return spawnFailure(contract.SpawnFailureRuntimeUnavailable, readinessErr), errors.Join(readinessErr, outcome.err)
	}
}

func monitorComputerReadiness(ctx context.Context, clock Clock, started <-chan struct{}, dial computerEndpointDial, observe func(bool)) error {
	if clock == nil {
		clock = systemClock{}
	}
	select {
	case <-ctx.Done():
		return &computerReadinessError{Code: contract.SpawnFailureRuntimeUnavailable, Err: fmt.Errorf("Computer runtime ended before Started: %w", context.Cause(ctx))}
	case <-started:
	}
	startedAt := clock.Now()
	if err := probeComputerBackends(ctx, clock, startedAt, dial); err != nil {
		return err
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
