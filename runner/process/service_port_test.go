//go:build darwin || linux

package process

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

// #1468: the agent releases a process service's backend port before the
// payload binds it. These tests take that released port from the test
// process, which is outside the payload's process group, the way any other
// local process could.

func TestServiceListenerInspectionAttributesProcessGroups(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	backend := mustServiceBackend(t, listener.Addr().String())
	group := syscall.Getpgrp()
	if verdict, err := inspectServiceListeners(backend, group); err != nil || verdict != serviceListenerOwned {
		t.Fatalf("own listener verdict = (%d, %v), want owned", verdict, err)
	}
	if verdict, err := inspectServiceListeners(backend, group+1_000_000); err != nil || verdict != serviceListenerForeign {
		t.Fatalf("listener of another group verdict = (%d, %v), want foreign", verdict, err)
	}
	if verdict, err := inspectServiceListeners(mustServiceBackend(t, releasedServiceAddress(t)), group); err != nil || verdict != serviceListenerUnseen {
		t.Fatalf("free port verdict = (%d, %v), want unseen", verdict, err)
	}
}

func TestServiceListenerInspectionIgnoresListenersThatCannotReceiveTheBackend(t *testing.T) {
	address := releasedServiceAddress(t)
	backend := mustServiceBackend(t, address)
	unrelated := listenIPv6Loopback(t, backend.port)
	defer unrelated.Close()
	if verdict, err := inspectServiceListeners(backend, syscall.Getpgrp()+1_000_000); err != nil || verdict != serviceListenerUnseen {
		t.Fatalf("verdict with only [::1]:%d listening = (%d, %v), want unseen", backend.port, verdict, err)
	}
}

func TestServiceBackendReceivesOnlyItsAddressAndWildcards(t *testing.T) {
	backend := mustServiceBackend(t, "127.0.0.1:8080")
	for _, test := range []struct {
		ip   string
		want bool
	}{
		{"127.0.0.1", true}, {"::ffff:127.0.0.1", true}, {"0.0.0.0", true}, {"::", true},
		{"::1", false}, {"127.0.0.2", false}, {"10.0.0.5", false}, {"fe80::1", false},
	} {
		if got := backend.receives(net.ParseIP(test.ip)); got != test.want {
			t.Errorf("listener on %s receives 127.0.0.1 = %t, want %t", test.ip, got, test.want)
		}
	}
	ipv6 := mustServiceBackend(t, "[::1]:8080")
	if ipv6.receives(net.ParseIP("0.0.0.0")) || !ipv6.receives(net.ParseIP("::")) {
		t.Error("an IPv6 backend must count the IPv6 wildcard and not the IPv4 one")
	}
}

func TestGuardianPublishesBesideAnUnrelatedListenerOnAnotherAddress(t *testing.T) {
	address := releasedServiceAddress(t)
	unrelated := listenIPv6Loopback(t, mustServiceBackend(t, address).port)
	defer unrelated.Close()
	assertGuardianPublishes(t, helperExecution("listen", address), address)
}

func TestGuardianPublishesAListenerOwnedByAChildInThePayloadGroup(t *testing.T) {
	address := releasedServiceAddress(t)
	assertGuardianPublishes(t, helperExecution("listen-child", address), address)
}

func TestGuardianWithdrawsAListenerTakeoverWithoutAFailedProbe(t *testing.T) {
	address := releasedServiceAddress(t)
	readiness := make(chan bool, 8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finished := runServiceAsync(ctx, serviceTestRunner(30*time.Second), Request{
		AttemptID: "reuseport-takeover", Guarded: true,
		Execution: helperExecution("listen-reuseport", address), IdlePolicy: IgnoreIdle, ServiceAddress: address,
		ReadinessChanged: func(_, ready bool) { readiness <- ready },
	})
	if !waitGuardianReadiness(t, readiness) {
		t.Fatal("the payload's own listener was not published")
	}
	// The payload keeps listening, so every probe keeps succeeding: only the
	// periodic ownership check can see the second, foreign listener.
	foreign := listenReusePort(t, address)
	if waitGuardianReadiness(t, readiness) {
		t.Fatal("guardian kept a foreign SO_REUSEPORT listener published")
	}
	assertStillRunning(t, finished)
	_ = foreign.Close()
	if !waitGuardianReadiness(t, readiness) {
		t.Fatal("guardian did not republish once only the payload listened again")
	}
	cancel()
	outcome := awaitRun(t, finished)
	if !errors.Is(outcome.err, context.Canceled) {
		t.Fatalf("guardian outcome after cancellation = (%#v, %v)", outcome.result, outcome.err)
	}
}

func TestGuardianStopsAServiceWhoseListenerStaysForeign(t *testing.T) {
	address := releasedServiceAddress(t)
	readiness := make(chan bool, 8)
	finished := runServiceAsync(context.Background(), serviceTestRunner(3*time.Second), Request{
		AttemptID: "lasting-takeover", Guarded: true,
		Execution: helperExecution("listen-reuseport", address), IdlePolicy: IgnoreIdle, ServiceAddress: address,
		ReadinessChanged: func(_, ready bool) { readiness <- ready },
	})
	if !waitGuardianReadiness(t, readiness) {
		t.Fatal("the payload's own listener was not published")
	}
	foreign := listenReusePort(t, address)
	defer foreign.Close()
	if waitGuardianReadiness(t, readiness) {
		t.Fatal("guardian kept a foreign SO_REUSEPORT listener published")
	}
	outcome := awaitRun(t, finished)
	if outcome.err != nil || outcome.result.SpawnError == nil || outcome.result.SpawnError.Code != contract.SpawnFailureServicePortTaken {
		t.Fatalf("lasting takeover outcome = (%#v, %v), want typed service_port_taken", outcome.result, outcome.err)
	}
}

func TestGuardianInspectionFailureIsNeverPublishedAndEndsTyped(t *testing.T) {
	injected := errors.New("injected listener inspection failure")
	var failing atomic.Bool
	priorInspector, priorInterval := serviceListenerInspector, serviceOwnershipRecheckInterval
	serviceListenerInspector = func(backend serviceBackend, group int) (serviceListenerVerdict, error) {
		if failing.Load() {
			return 0, injected
		}
		return inspectServiceListeners(backend, group)
	}
	serviceOwnershipRecheckInterval = 50 * time.Millisecond
	t.Cleanup(func() { serviceListenerInspector, serviceOwnershipRecheckInterval = priorInspector, priorInterval })

	t.Run("before startup", func(t *testing.T) {
		failing.Store(true)
		statuses := runInProcessServiceGuardian(t, releasedServiceAddress(t), time.Second)
		exited := nextGuardianStatus(t, statuses)
		if exited.Type != guardianMessageExited || exited.Result.SpawnError == nil ||
			exited.Result.SpawnError.Code != contract.SpawnFailureStartupReadinessTimeout ||
			!strings.Contains(exited.Result.SpawnError.Message, injected.Error()) {
			t.Fatalf("status = %#v, want startup_readiness_timeout naming the inspection failure", exited)
		}
	})
	t.Run("after startup", func(t *testing.T) {
		failing.Store(false)
		statuses := runInProcessServiceGuardian(t, releasedServiceAddress(t), time.Second)
		if ready := nextGuardianStatus(t, statuses); ready.Type != guardianMessageReadiness || !*ready.Ready {
			t.Fatalf("status = %#v, want readiness true", ready)
		}
		failing.Store(true)
		if withdrawn := nextGuardianStatus(t, statuses); withdrawn.Type != guardianMessageReadiness || *withdrawn.Ready {
			t.Fatalf("status = %#v, want readiness withdrawn", withdrawn)
		}
		exited := nextGuardianStatus(t, statuses)
		if exited.Type != guardianMessageExited || exited.Result.SpawnError == nil ||
			exited.Result.SpawnError.Code != contract.SpawnFailureServicePortTaken ||
			!strings.Contains(exited.Result.SpawnError.Message, injected.Error()) {
			t.Fatalf("status = %#v, want service_port_taken naming the inspection failure", exited)
		}
	})
}

// runInProcessServiceGuardian serves a Guardian in this process for a
// payload that owns address, and returns its statuses after Started.
func runInProcessServiceGuardian(t *testing.T, address string, startupDeadline time.Duration) <-chan guardianStatusMessage {
	t.Helper()
	agentEndpoint, guardianEndpoint, err := newGuardianSocketPair()
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- serveGuardian(guardianEndpoint, os.Stdout, os.Stderr) }()
	start := guardianTestStart("listen")
	start.Start.Args = append(start.Start.Args, address)
	start.Start.ServiceAddress = address
	start.Start.StartupReadinessDeadline = startupDeadline
	start.Start.ReadinessProbeInterval = 20 * time.Millisecond
	start.Start.ReadinessConnectTimeout = 100 * time.Millisecond
	if err := json.NewEncoder(agentEndpoint).Encode(start); err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(agentEndpoint)
	if started, err := decodeGuardianStatus(decoder); err != nil || started.Type != guardianMessageStarted {
		t.Fatalf("first status = (%#v, %v), want started", started, err)
	}
	statuses := make(chan guardianStatusMessage, 8)
	go func() {
		defer close(statuses)
		for {
			status, err := decodeGuardianStatus(decoder)
			if err != nil {
				return
			}
			statuses <- status
		}
	}()
	t.Cleanup(func() {
		_ = agentEndpoint.Close()
		if err := <-done; err != nil {
			t.Errorf("serveGuardian() error = %v", err)
		}
		_ = guardianEndpoint.Close()
	})
	return statuses
}

func nextGuardianStatus(t *testing.T, statuses <-chan guardianStatusMessage) guardianStatusMessage {
	t.Helper()
	select {
	case status, open := <-statuses:
		if !open {
			t.Fatal("guardian status stream ended")
		}
		return status
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for a guardian status")
		return guardianStatusMessage{}
	}
}

func assertGuardianPublishes(t *testing.T, execution contract.ExecutionSpec, address string) {
	t.Helper()
	readiness := make(chan bool, 8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finished := runServiceAsync(ctx, serviceTestRunner(20*time.Second), Request{
		AttemptID: "published-service", Guarded: true,
		Execution: execution, IdlePolicy: IgnoreIdle, ServiceAddress: address,
		ReadinessChanged: func(_, ready bool) { readiness <- ready },
	})
	select {
	case ready := <-readiness:
		if !ready {
			t.Fatal("first readiness transition was not ready")
		}
	case outcome := <-finished:
		t.Fatalf("service ended before it was published: (%#v, %v)", outcome.result, outcome.err)
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for publication")
	}
	cancel()
	_ = awaitRun(t, finished)
}

func runServiceAsync(ctx context.Context, runner *Runner, request Request) <-chan runOutcome {
	finished := make(chan runOutcome, 1)
	go func() {
		result, err := runner.Run(ctx, request, nil)
		finished <- runOutcome{result: result, err: err}
	}()
	return finished
}

func mustServiceBackend(t *testing.T, address string) serviceBackend {
	t.Helper()
	backend, err := parseServiceBackend(address)
	if err != nil {
		t.Fatal(err)
	}
	return backend
}

// listenIPv6Loopback holds [::1]:port, which cannot receive connections for
// 127.0.0.1:port.
func listenIPv6Loopback(t *testing.T, port int) net.Listener {
	t.Helper()
	listener, err := net.Listen("tcp6", net.JoinHostPort("::1", strconv.Itoa(port)))
	if err != nil {
		t.Skipf("IPv6 loopback is unavailable here: %v", err)
	}
	go serveAndCloseForTest(listener)
	return listener
}

// listenReusePort adds an SO_REUSEPORT listener beside the payload's.
func listenReusePort(t *testing.T, address string) net.Listener {
	t.Helper()
	config := net.ListenConfig{Control: func(_, _ string, connection syscall.RawConn) error {
		var optionErr error
		if err := connection.Control(func(descriptor uintptr) {
			optionErr = syscall.SetsockoptInt(int(descriptor), syscall.SOL_SOCKET, syscall.SO_REUSEPORT, 1)
		}); err != nil {
			return err
		}
		return optionErr
	}}
	listener, err := config.Listen(context.Background(), "tcp4", address)
	if err != nil {
		t.Fatal(err)
	}
	go serveAndCloseForTest(listener)
	return listener
}

func serveAndCloseForTest(listener net.Listener) {
	for {
		connection, err := listener.Accept()
		if err != nil {
			return
		}
		_ = connection.Close()
	}
}

func TestGuardianNeverPublishesAForeignListenerOnTheServicePort(t *testing.T) {
	address := releasedServiceAddress(t)
	foreign, err := net.Listen("tcp4", address)
	if err != nil {
		t.Fatal(err)
	}
	defer foreign.Close()
	accepted := make(chan struct{}, 64)
	go func() {
		for {
			connection, err := foreign.Accept()
			if err != nil {
				return
			}
			accepted <- struct{}{}
			_ = connection.Close()
		}
	}()
	published := make(chan bool, 4)
	began := time.Now()
	// The payload never binds, so only the foreign listener can answer.
	result, err := serviceTestRunner(20*time.Second).Run(context.Background(), Request{
		AttemptID: "foreign-service-listener", Guarded: true,
		Execution: helperExecution("hang"), IdlePolicy: IgnoreIdle, ServiceAddress: address,
		ReadinessChanged: func(_, ready bool) { published <- ready },
	}, nil)
	if err != nil {
		t.Fatalf("foreign listener returned supervision error: %v", err)
	}
	if result.SpawnError == nil || result.SpawnError.Code != contract.SpawnFailureServicePortTaken || result.ExitCode != nil || result.Signal != "" {
		t.Fatalf("foreign listener result = %#v, want sole service_port_taken", result)
	}
	if elapsed := time.Since(began); elapsed > 15*time.Second {
		t.Fatalf("foreign listener was refused after %s, want before the startup deadline", elapsed)
	}
	select {
	case ready := <-published:
		t.Fatalf("guardian reported readiness %t for a listener outside the payload group", ready)
	default:
	}
	select {
	case <-accepted:
	default:
		t.Fatal("the foreign listener never answered a probe; the test did not reach the ownership check")
	}
}

func TestGuardianTypesALostPayloadBindAsServicePortTaken(t *testing.T) {
	address := releasedServiceAddress(t)
	port := mustServiceBackend(t, address).port
	// A bound socket that does not listen: it blocks the payload's bind but
	// answers no probe, so only the payload's exit can report the loss.
	holder, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.Close(holder)
	if err := syscall.Bind(holder, &syscall.SockaddrInet4{Port: port, Addr: [4]byte{127, 0, 0, 1}}); err != nil {
		t.Fatal(err)
	}
	result, err := serviceTestRunner(20*time.Second).Run(context.Background(), Request{
		AttemptID: "lost-service-bind", Guarded: true,
		Execution: helperExecution("listen", address), IdlePolicy: IgnoreIdle, ServiceAddress: address,
		ReadinessChanged: func(_, ready bool) { t.Errorf("guardian reported readiness %t without a payload listener", ready) },
	}, nil)
	if err != nil {
		t.Fatalf("lost bind returned supervision error: %v", err)
	}
	if result.SpawnError == nil || result.SpawnError.Code != contract.SpawnFailureServicePortTaken || result.ExitCode != nil {
		t.Fatalf("lost bind result = %#v, want service_port_taken", result)
	}
}

func TestGuardianPublishesOnlyThePayloadsOwnListenerAfterStartup(t *testing.T) {
	address := releasedServiceAddress(t)
	payload := make(chan int, 1)
	readiness := make(chan bool, 8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finished := make(chan runOutcome, 1)
	go func() {
		result, err := serviceTestRunner(10*time.Second).Run(ctx, Request{
			AttemptID: "owned-service-listener", Guarded: true,
			Execution: helperExecution("listen-toggle", address), IdlePolicy: IgnoreIdle, ServiceAddress: address,
			ProcessStarted:   func(processGroupID int) error { payload <- processGroupID; return nil },
			ReadinessChanged: func(_, ready bool) { readiness <- ready },
		}, nil)
		finished <- runOutcome{result: result, err: err}
	}()
	if !waitGuardianReadiness(t, readiness) {
		t.Fatal("the payload's own listener was not published")
	}
	pid := <-payload
	if err := syscall.Kill(pid, syscall.SIGUSR1); err != nil {
		t.Fatal(err)
	}
	if waitGuardianReadiness(t, readiness) {
		t.Fatal("guardian did not withdraw readiness after the payload closed its listener")
	}

	// Another process takes the port while the payload is not listening.
	// Probes now succeed, yet nothing is published and the payload lives.
	foreign, err := net.Listen("tcp4", address)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			connection, err := foreign.Accept()
			if err != nil {
				return
			}
			_ = connection.Close()
		}
	}()
	select {
	case ready := <-readiness:
		t.Fatalf("guardian reported readiness %t for a listener outside the payload group", ready)
	case <-time.After(time.Second):
	}
	assertStillRunning(t, finished)
	_ = foreign.Close()

	if err := syscall.Kill(pid, syscall.SIGUSR2); err != nil {
		t.Fatal(err)
	}
	if !waitGuardianReadiness(t, readiness) {
		t.Fatal("guardian did not republish the payload's own listener")
	}
	cancel()
	outcome := awaitRun(t, finished)
	if !errors.Is(outcome.err, context.Canceled) || outcome.result.Signal == "" {
		t.Fatalf("guardian outcome after cancellation = (%#v, %v)", outcome.result, outcome.err)
	}
}

func serviceTestRunner(startupDeadline time.Duration) *Runner {
	return New(Config{
		GuardianExecutable: guardianAgentPath, TerminationGraceTime: 100 * time.Millisecond,
		StartupReadinessDeadline: startupDeadline,
		ReadinessProbeInterval:   20 * time.Millisecond, ReadinessConnectTimeout: 100 * time.Millisecond,
	})
}

// releasedServiceAddress picks a backend address the way the agent does:
// bind port 0, read the number, release it.
func releasedServiceAddress(t *testing.T) string {
	t.Helper()
	reservation, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := reservation.Addr().String()
	if err := reservation.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

func waitGuardianReadiness(t *testing.T, readiness <-chan bool) bool {
	t.Helper()
	select {
	case ready := <-readiness:
		return ready
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for guardian readiness transition")
		return false
	}
}
