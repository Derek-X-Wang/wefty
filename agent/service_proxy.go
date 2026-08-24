package agent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
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

type runtimeEndpointLatch struct {
	mu     sync.Mutex
	values map[string]workloadrunner.AttemptEndpoint
	ready  map[string]chan struct{}
}

func newRuntimeEndpointLatch() *runtimeEndpointLatch {
	return &runtimeEndpointLatch{values: make(map[string]workloadrunner.AttemptEndpoint), ready: make(map[string]chan struct{})}
}

func (latch *runtimeEndpointLatch) publish(name string, endpoint workloadrunner.AttemptEndpoint) error {
	if name == "" {
		return errors.New("runtime supplied an unnamed attempt endpoint")
	}
	if endpoint.Port == 0 || endpoint.Dial == nil {
		return errors.New("runtime supplied an invalid attempt endpoint")
	}
	latch.mu.Lock()
	defer latch.mu.Unlock()
	if _, exists := latch.values[name]; exists {
		return fmt.Errorf("runtime supplied attempt endpoint %q more than once", name)
	}
	latch.values[name] = endpoint
	ready := latch.ready[name]
	if ready == nil {
		ready = make(chan struct{})
		latch.ready[name] = ready
	}
	close(ready)
	return nil
}

func (latch *runtimeEndpointLatch) endpoint(name string) serviceRuntimeEndpoint {
	return serviceRuntimeEndpoint{dial: func(ctx context.Context) (net.Conn, error) {
		latch.mu.Lock()
		endpoint, exists := latch.values[name]
		ready := latch.ready[name]
		if ready == nil {
			ready = make(chan struct{})
			latch.ready[name] = ready
		}
		latch.mu.Unlock()
		if !exists {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-ready:
			}
			latch.mu.Lock()
			endpoint = latch.values[name]
			latch.mu.Unlock()
		}
		return endpoint.Dial(ctx)
	}}
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

type opaqueReadinessResult struct {
	outcome    serviceRunOutcome
	hasOutcome bool
	err        error
}

type serviceSupervisorConfig struct {
	clock                     Clock
	startupReadinessDeadline  time.Duration
	readinessProbeInterval    time.Duration
	readinessConnectTimeout   time.Duration
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
	frontDoor := newServiceFrontDoor(listener, endpoint.dial, durationOrDefault(config.readinessConnectTimeout, processrunner.DefaultReadinessConnectTimeout))
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

	readinessChanged := func(startupSatisfied, ready bool) {
		if ready && config.onReadiness != nil {
			config.onReadiness(startupSatisfied, ready)
		}
		publication.Observe(ready)
		if !ready && config.onReadiness != nil {
			config.onReadiness(startupSatisfied, ready)
		}
	}
	priorReadiness := request.ReadinessChanged
	request.ReadinessChanged = func(startupSatisfied, ready bool) {
		readinessChanged(startupSatisfied, ready)
		if priorReadiness != nil {
			priorReadiness(startupSatisfied, ready)
		}
	}
	request.ServiceAddress = endpoint.address
	var started chan struct{}
	if endpoint.address == "" {
		if endpoint.dial == nil {
			err := errors.New("opaque service runtime endpoint has no dial function")
			publication.Stop()
			cancelRun()
			<-publicationDone
			return spawnFailure(contract.SpawnFailureProcessRequest, err), err
		}
		started = make(chan struct{})
		var startedOnce sync.Once
		priorStarted := request.Started
		request.Started = func() {
			if priorStarted != nil {
				priorStarted()
			}
			startedOnce.Do(func() { close(started) })
		}
	}
	outcomes := make(chan serviceRunOutcome, 1)
	go func() {
		result, err := runtimeAdapter.Run(runContext, request, sink)
		outcomes <- serviceRunOutcome{result: result.Outcome, err: err}
	}()
	var readinessDone <-chan opaqueReadinessResult
	runtimeOutcomes := (<-chan serviceRunOutcome)(outcomes)
	if started != nil {
		result := make(chan opaqueReadinessResult, 1)
		go func() {
			result <- monitorOpaqueServiceReadiness(
				runContext, config.clock, started, endpoint.dial,
				config.startupReadinessDeadline, config.readinessProbeInterval,
				config.readinessConnectTimeout, readinessChanged, outcomes,
			)
		}()
		readinessDone = result
		runtimeOutcomes = nil
	}
	waitOutcome := func() serviceRunOutcome {
		if readinessDone == nil {
			return <-outcomes
		}
		result := <-readinessDone
		if result.hasOutcome {
			return result.outcome
		}
		return <-outcomes
	}

	select {
	case <-ctx.Done():
		publication.Stop()
		frontDoor.Close()
		cancelRun()
		outcome := waitOutcome()
		publicationErr := <-publicationDone
		return outcome.result, errors.Join(outcome.err, publicationErr)
	case outcome := <-runtimeOutcomes:
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
		outcome := waitOutcome()
		return outcome.result, errors.Join(outcome.err, publicationErr)
	case err := <-frontDoor.Errors():
		publication.Stop()
		cancelRun()
		<-publicationDone
		_ = waitOutcome()
		return spawnFailure(contract.SpawnFailurePublishedListener, err), err
	case readiness := <-readinessDone:
		if readiness.hasOutcome {
			publication.Stop()
			publicationErr := <-publicationDone
			return readiness.outcome.result, errors.Join(readiness.outcome.err, publicationErr)
		}
		publication.Stop()
		frontDoor.Close()
		cancelRun()
		outcome := <-outcomes
		publicationErr := <-publicationDone
		return spawnFailure(contract.SpawnFailureStartupReadinessTimeout, readiness.err), errors.Join(readiness.err, outcome.err, publicationErr)
	}
}

func monitorOpaqueServiceReadiness(
	ctx context.Context,
	clock Clock,
	started <-chan struct{},
	dial func(context.Context) (net.Conn, error),
	startupDeadline, probeInterval, connectTimeout time.Duration,
	observe func(startupSatisfied, ready bool),
	outcomes <-chan serviceRunOutcome,
) opaqueReadinessResult {
	if clock == nil {
		clock = systemClock{}
	}
	startupDeadline = durationOrDefault(startupDeadline, processrunner.DefaultStartupReadinessDeadline)
	probeInterval = durationOrDefault(probeInterval, processrunner.DefaultReadinessProbeInterval)
	connectTimeout = durationOrDefault(connectTimeout, processrunner.DefaultReadinessConnectTimeout)
	select {
	case <-ctx.Done():
		return opaqueReadinessResult{outcome: <-outcomes, hasOutcome: true}
	case outcome := <-outcomes:
		return opaqueReadinessResult{outcome: outcome, hasOutcome: true}
	case <-started:
	}

	deadline := clock.NewTimer(startupDeadline)
	defer stopTimer(deadline)
	deadlineChannel := deadline.C()
	var interval Timer
	var intervalChannel <-chan time.Time
	startupSatisfied := false
	ready := false
	type probeResult struct{ err error }
	var probeDone <-chan probeResult
	var cancelProbe context.CancelFunc
	startProbe := func() {
		probeContext, cancel := context.WithTimeout(ctx, connectTimeout)
		cancelProbe = cancel
		result := make(chan probeResult, 1)
		probeDone = result
		go func() {
			connection, err := dial(probeContext)
			if connection != nil {
				_ = connection.Close()
			}
			result <- probeResult{err: err}
		}()
	}
	consumeOutcome := func() (serviceRunOutcome, bool) {
		select {
		case outcome := <-outcomes:
			return outcome, true
		default:
			return serviceRunOutcome{}, false
		}
	}
	timeoutResult := func() opaqueReadinessResult {
		if cancelProbe != nil {
			cancelProbe()
		}
		if outcome, ok := consumeOutcome(); ok {
			return opaqueReadinessResult{outcome: outcome, hasOutcome: true}
		}
		return opaqueReadinessResult{err: errors.New("runtime-local service endpoint did not accept connections before the startup deadline")}
	}
	startProbe()
	for {
		select {
		case <-ctx.Done():
			if cancelProbe != nil {
				cancelProbe()
			}
			return opaqueReadinessResult{outcome: <-outcomes, hasOutcome: true}
		case outcome := <-outcomes:
			if cancelProbe != nil {
				cancelProbe()
			}
			return opaqueReadinessResult{outcome: outcome, hasOutcome: true}
		case <-deadlineChannel:
			return timeoutResult()
		case probe := <-probeDone:
			cancelProbe()
			cancelProbe = nil
			probeDone = nil
			if outcome, ok := consumeOutcome(); ok {
				return opaqueReadinessResult{outcome: outcome, hasOutcome: true}
			}
			if !startupSatisfied {
				select {
				case <-deadlineChannel:
					return timeoutResult()
				default:
				}
			}
			nextReady := probe.err == nil
			if nextReady != ready {
				ready = nextReady
				if ready {
					startupSatisfied = true
					stopTimer(deadline)
					deadlineChannel = nil
				}
				observe(startupSatisfied, ready)
			}
			if interval == nil {
				interval = clock.NewTimer(probeInterval)
				defer stopTimer(interval)
				intervalChannel = interval.C()
			} else {
				interval.Reset(probeInterval)
				intervalChannel = interval.C()
			}
		case <-intervalChannel:
			intervalChannel = nil
			startProbe()
		}
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
	if !enabled {
		frontDoor.closeActiveLocked()
	}
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
		closeServiceWrite(backend)
	}()
	go func() {
		defer copies.Done()
		_, _ = io.Copy(published, backend)
		closeServiceWrite(published)
	}()
	copies.Wait()
	_ = published.Close()
	_ = backend.Close()

	frontDoor.mu.Lock()
	delete(frontDoor.active, published)
	frontDoor.mu.Unlock()
}

func closeServiceWrite(connection net.Conn) {
	if half, ok := connection.(interface{ CloseWrite() error }); ok {
		if err := half.CloseWrite(); err != nil && !errors.Is(err, net.ErrClosed) {
			log.Printf("service front door write-half close: %v", err)
		}
		return
	}
	log.Printf("service front door connection %T lacks CloseWrite; closing the full tunnel", connection)
	if err := connection.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		log.Printf("service front door full-close fallback: %v", err)
	}
}

func (frontDoor *serviceFrontDoor) closeActiveLocked() {
	for published, backend := range frontDoor.active {
		_ = published.Close()
		_ = backend.Close()
		delete(frontDoor.active, published)
	}
}
