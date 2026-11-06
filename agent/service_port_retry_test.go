package agent

import (
	"context"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/testwait"
	processrunner "github.com/Derek-X-Wang/wefty/runner/process"
)

// #1468: the agent releases a process service's backend port before the
// payload binds it. These tests take the released port before the payload
// starts and show that the agent retries exactly once on a fresh port, never
// routes to the process that took it, and fails typed when the retry loses
// too. The Guardian's own ownership check is in runner/process.

func TestTakenServicePortRetriesOnceOnAFreshPortAndNeverRoutesToTheTaker(t *testing.T) {
	taker, takerConnections := takeServicePort(t)
	fresh := releasedTestAddress(t)
	reallocations := atomic.Int32{}
	endpoint := releasedServiceEndpoint(taker.Addr().(*net.TCPAddr), func(context.Context) (serviceRuntimeEndpoint, error) {
		reallocations.Add(1)
		return releasedServiceEndpoint(fresh, nil), nil
	})
	published, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	type startedPayload struct{ address, port string }
	started := make(chan startedPayload, 2)
	payload := directiveContinuationRunner(func(ctx context.Context, request processrunner.Request, _ processrunner.OutputSink) (contract.ProcessResult, error) {
		started <- startedPayload{address: request.ServiceAddress, port: request.Execution.Env[contract.EnvServicePort]}
		backend, err := net.Listen("tcp4", request.ServiceAddress)
		if err != nil {
			return contract.ProcessResult{}, err
		}
		defer backend.Close()
		go serveTestEcho(backend)
		request.ReadinessChanged(true, true)
		<-ctx.Done()
		return contract.ProcessResult{Signal: "terminated", TerminationCause: contract.TerminationCauseAgent}, ctx.Err()
	})
	request := workloadRequest("taken-service-port-retry")
	request.Execution.Env = cloneEnvironment(endpoint.environment)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	finished := make(chan serviceRunOutcome, 1)
	go func() {
		result, err := runPortfulService(ctx, testProcessRuntime(payload), request, nil, published, endpoint,
			serviceSupervisorConfig{clock: systemClock{}})
		finished <- serviceRunOutcome{result: result, err: err}
	}()

	var payloadStart startedPayload
	select {
	case payloadStart = <-started:
	case <-time.After(testwait.Bound):
		t.Fatal("payload never started on a fresh port")
	}
	if payloadStart.address != fresh.String() || payloadStart.port != strconv.Itoa(fresh.Port) {
		t.Fatalf("payload started with address %q and %s=%q, want the fresh %s", payloadStart.address, contract.EnvServicePort, payloadStart.port, fresh)
	}
	waitForPublishedEcho(t, published.Addr().String(), true)
	if got := reallocations.Load(); got != 1 {
		t.Fatalf("reallocations = %d, want exactly one fast retry", got)
	}
	if got := takerConnections.Load(); got != 0 {
		t.Fatalf("the process that took the port received %d connections, want none", got)
	}
	cancel()
	select {
	case <-finished:
	case <-time.After(testwait.Bound):
		t.Fatal("service did not stop after cancellation")
	}
	select {
	case extra := <-started:
		t.Fatalf("payload started a second time on %q", extra.address)
	default:
	}
}

func TestTakenServicePortFailsTypedWhenTheRetryLosesToo(t *testing.T) {
	taker, takerConnections := takeServicePort(t)
	secondTaker, secondConnections := takeServicePort(t)
	reallocations := atomic.Int32{}
	endpoint := releasedServiceEndpoint(taker.Addr().(*net.TCPAddr), func(context.Context) (serviceRuntimeEndpoint, error) {
		reallocations.Add(1)
		return releasedServiceEndpoint(secondTaker.Addr().(*net.TCPAddr), func(context.Context) (serviceRuntimeEndpoint, error) {
			t.Error("the agent retried a second time")
			return serviceRuntimeEndpoint{}, nil
		}), nil
	})
	published, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	// The attempt lifecycle owns this listener and closes it on return.
	defer published.Close()
	ran := atomic.Bool{}
	payload := directiveContinuationRunner(func(context.Context, processrunner.Request, processrunner.OutputSink) (contract.ProcessResult, error) {
		ran.Store(true)
		return contract.ProcessResult{}, nil
	})
	publications := atomic.Int32{}
	request := workloadRequest("taken-service-port-twice")
	request.Execution.Env = cloneEnvironment(endpoint.environment)
	result, err := runPortfulService(t.Context(), testProcessRuntime(payload), request, nil, published, endpoint,
		serviceSupervisorConfig{clock: systemClock{}, publish: func(context.Context, bool) error {
			publications.Add(1)
			return nil
		}})
	if err != nil {
		t.Fatalf("taken port returned an agent error: %v", err)
	}
	if result.SpawnError == nil || result.SpawnError.Code != contract.SpawnFailureServicePortTaken {
		t.Fatalf("result = %#v, want service_port_taken", result)
	}
	if ran.Load() {
		t.Fatal("the payload started although both ports were taken")
	}
	if got := reallocations.Load(); got != 1 {
		t.Fatalf("reallocations = %d, want exactly one fast retry", got)
	}
	if publications.Load() != 0 || takerConnections.Load() != 0 || secondConnections.Load() != 0 {
		t.Fatalf("publications=%d taker connections=%d/%d, want nothing published or routed",
			publications.Load(), takerConnections.Load(), secondConnections.Load())
	}
	if publishedEchoForwarded(published.Addr().String()) {
		t.Fatal("the published listener forwarded traffic for a service that never started")
	}
}

// takeServicePort stands in for another local process that grabs a released
// port: it listens there and counts every connection it receives.
func takeServicePort(t *testing.T) (net.Listener, *atomic.Int32) {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	connections := &atomic.Int32{}
	go func() {
		for {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			connections.Add(1)
			_ = connection.Close()
		}
	}()
	return listener, connections
}

func releasedTestAddress(t *testing.T) *net.TCPAddr {
	t.Helper()
	reservation, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := reservation.Addr().(*net.TCPAddr)
	if err := reservation.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}
