//go:build darwin || linux

package process

import (
	"context"
	"errors"
	"net"
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
	port := listener.Addr().(*net.TCPAddr).Port
	group := syscall.Getpgrp()
	if verdict, err := inspectServiceListeners(port, group); err != nil || verdict != serviceListenerOwned {
		t.Fatalf("own listener verdict = (%d, %v), want owned", verdict, err)
	}
	if verdict, err := inspectServiceListeners(port, group+1_000_000); err != nil || verdict != serviceListenerForeign {
		t.Fatalf("listener of another group verdict = (%d, %v), want foreign", verdict, err)
	}
	unused := releasedServiceAddress(t)
	unusedPort, err := serviceAddressPort(unused)
	if err != nil {
		t.Fatal(err)
	}
	if verdict, err := inspectServiceListeners(unusedPort, group); err != nil || verdict != serviceListenerUnseen {
		t.Fatalf("free port verdict = (%d, %v), want unseen", verdict, err)
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
	port, err := serviceAddressPort(address)
	if err != nil {
		t.Fatal(err)
	}
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
