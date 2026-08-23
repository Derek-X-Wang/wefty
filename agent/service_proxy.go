package agent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	processrunner "github.com/Derek-X-Wang/wefty/runner/process"
)

// serviceRuntimeEndpoint is supplied by the workload runtime adapter. It is
// deliberately process-local and never enters JobSpec, L1 state, or another
// durable contract. A future OCI adapter can supply a dial function whose
// target is not in the agent's network namespace.
type serviceRuntimeEndpoint struct {
	environment map[string]string
	address     string
	dial        func(context.Context) (net.Conn, error)
}

func prepareProcessServiceEndpoint(ctx context.Context) (serviceRuntimeEndpoint, error) {
	if err := ctx.Err(); err != nil {
		return serviceRuntimeEndpoint{}, err
	}
	reservation, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return serviceRuntimeEndpoint{}, fmt.Errorf("reserve process service endpoint: %w", err)
	}
	address := reservation.Addr().String()
	port := reservation.Addr().(*net.TCPAddr).Port
	if err := reservation.Close(); err != nil {
		return serviceRuntimeEndpoint{}, fmt.Errorf("release process service endpoint reservation: %w", err)
	}
	dialer := &net.Dialer{}
	return serviceRuntimeEndpoint{
		environment: map[string]string{contract.EnvServicePort: strconv.Itoa(port)},
		address:     address,
		dial: func(ctx context.Context) (net.Conn, error) {
			return dialer.DialContext(ctx, "tcp4", address)
		},
	}, nil
}

type serviceRunOutcome struct {
	result contract.ProcessResult
	err    error
}

type serviceSupervisorConfig struct {
	clock                     Clock
	publicationRecoveryWindow time.Duration
	publicationRetryInterval  time.Duration
	publish                   publicationRequest
	onReadiness               func(startupSatisfied, ready bool)
	onForwarding              func(ready bool)
}

// runPortfulService owns the Fabric front door while the process runtime and
// guardian own backend probing and the startup race.
func runPortfulService(
	ctx context.Context,
	runtimeAdapter WorkloadRuntime,
	request workloadrunner.Request,
	sink workloadrunner.OutputSink,
	listener net.Listener,
	endpoint serviceRuntimeEndpoint,
	config serviceSupervisorConfig,
) (contract.ProcessResult, error) {
	// Keep the guardian execution context independent from the caller's
	// cancellation long enough to withdraw local publication and close the
	// Fabric listener first. The explicit ctx.Done arm below owns that order.
	runContext, cancelRun := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelRun()
	frontDoor := newServiceFrontDoor(listener, endpoint.dial, processrunner.DefaultReadinessConnectTimeout)
	defer frontDoor.Close()
	go frontDoor.Serve(runContext)
	publication := newPublicationController(
		config.clock,
		config.publicationRecoveryWindow,
		config.publicationRetryInterval,
		config.publish,
		func(ready bool) {
			frontDoor.SetForwarding(ready)
			if config.onForwarding != nil {
				config.onForwarding(ready)
			}
		},
	)
	publicationDone := make(chan error, 1)
	go func() { publicationDone <- publication.Run(runContext) }()

	request.ServiceAddress = endpoint.address
	priorReadiness := request.ReadinessChanged
	request.ReadinessChanged = func(startupSatisfied, ready bool) {
		if ready && config.onReadiness != nil {
			config.onReadiness(startupSatisfied, ready)
		}
		publication.Observe(ready)
		if !ready && config.onReadiness != nil {
			config.onReadiness(startupSatisfied, ready)
		}
		if priorReadiness != nil {
			priorReadiness(startupSatisfied, ready)
		}
	}
	outcomes := make(chan serviceRunOutcome, 1)
	go func() {
		result, err := runtimeAdapter.Run(runContext, request, sink)
		outcomes <- serviceRunOutcome{result: result.Outcome, err: err}
	}()

	select {
	case <-ctx.Done():
		publication.Stop()
		frontDoor.Close()
		cancelRun()
		outcome := <-outcomes
		publicationErr := <-publicationDone
		return outcome.result, errors.Join(outcome.err, publicationErr)
	case outcome := <-outcomes:
		publication.Stop()
		if publicationErr := <-publicationDone; publicationErr != nil {
			return outcome.result, errors.Join(outcome.err, publicationErr)
		}
		return outcome.result, outcome.err
	case publicationErr := <-publicationDone:
		if publicationErr == nil {
			cancelRun()
			outcome := <-outcomes
			return outcome.result, outcome.err
		}
		publication.Stop()
		cancelRun()
		outcome := <-outcomes
		return outcome.result, errors.Join(outcome.err, publicationErr)
	case err := <-frontDoor.Errors():
		publication.Stop()
		cancelRun()
		<-publicationDone
		<-outcomes
		return spawnFailure(contract.SpawnFailurePublishedListener, err), err
	}
}

type serviceFrontDoor struct {
	listener       net.Listener
	dial           func(context.Context) (net.Conn, error)
	connectTimeout time.Duration
	errors         chan error

	mu         sync.Mutex
	forwarding bool
	closed     bool
	active     map[net.Conn]net.Conn
	closeOnce  sync.Once
}

func newServiceFrontDoor(
	listener net.Listener,
	dial func(context.Context) (net.Conn, error),
	connectTimeout time.Duration,
) *serviceFrontDoor {
	return &serviceFrontDoor{
		listener: listener, dial: dial, connectTimeout: connectTimeout,
		errors: make(chan error, 1), active: make(map[net.Conn]net.Conn),
	}
}

func (frontDoor *serviceFrontDoor) Errors() <-chan error { return frontDoor.errors }

func (frontDoor *serviceFrontDoor) Serve(ctx context.Context) {
	for {
		connection, err := frontDoor.listener.Accept()
		if err != nil {
			frontDoor.mu.Lock()
			closed := frontDoor.closed
			frontDoor.mu.Unlock()
			if !closed && !errors.Is(err, net.ErrClosed) {
				select {
				case frontDoor.errors <- errors.New("Fabric published listener stopped accepting connections"):
				default:
				}
			}
			return
		}
		go frontDoor.forward(ctx, connection)
	}
}

func (frontDoor *serviceFrontDoor) SetForwarding(enabled bool) {
	frontDoor.mu.Lock()
	defer frontDoor.mu.Unlock()
	if frontDoor.closed || frontDoor.forwarding == enabled {
		return
	}
	frontDoor.forwarding = enabled
}

func (frontDoor *serviceFrontDoor) Close() {
	frontDoor.closeOnce.Do(func() {
		frontDoor.mu.Lock()
		frontDoor.closed = true
		frontDoor.forwarding = false
		frontDoor.closeActiveLocked()
		frontDoor.mu.Unlock()
		_ = frontDoor.listener.Close()
	})
}

func (frontDoor *serviceFrontDoor) forward(ctx context.Context, published net.Conn) {
	frontDoor.mu.Lock()
	if frontDoor.closed || !frontDoor.forwarding {
		frontDoor.mu.Unlock()
		_ = published.Close()
		return
	}
	frontDoor.mu.Unlock()

	dialContext, cancel := context.WithTimeout(ctx, frontDoor.connectTimeout)
	backend, err := frontDoor.dial(dialContext)
	cancel()
	if err != nil {
		_ = published.Close()
		return
	}

	frontDoor.mu.Lock()
	if frontDoor.closed || !frontDoor.forwarding {
		frontDoor.mu.Unlock()
		_ = published.Close()
		_ = backend.Close()
		return
	}
	frontDoor.active[published] = backend
	frontDoor.mu.Unlock()

	var copies sync.WaitGroup
	copies.Add(2)
	go func() {
		defer copies.Done()
		_, _ = io.Copy(backend, published)
	}()
	go func() {
		defer copies.Done()
		_, _ = io.Copy(published, backend)
	}()
	copies.Wait()
	_ = published.Close()
	_ = backend.Close()

	frontDoor.mu.Lock()
	delete(frontDoor.active, published)
	frontDoor.mu.Unlock()
}

func (frontDoor *serviceFrontDoor) closeActiveLocked() {
	for published, backend := range frontDoor.active {
		_ = published.Close()
		_ = backend.Close()
		delete(frontDoor.active, published)
	}
}
