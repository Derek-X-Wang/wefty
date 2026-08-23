//go:build service_acceptance && (darwin || linux)

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

func TestServiceAcceptanceQuietProcessIgnoresIdleAndHonorsMaxRuntime(t *testing.T) {
	clock := newFakeClock(time.Unix(10_000, 0))
	sink := newCollectingSink()
	runner := New(Config{
		Clock:                clock,
		IdleTimeout:          10 * time.Second,
		CompletionTimeout:    60 * time.Second,
		TerminationGraceTime: 2 * time.Second,
	})
	finished := runAsync(runner, Request{
		AttemptID:  "service-acceptance-idle-policy",
		Execution:  helperExecution("hang"),
		IdlePolicy: IgnoreIdle,
		Limits:     &contract.JobLimits{MaxRuntimeSeconds: 120},
		// Services do not signal one-shot completion. A nil signal must not
		// start the completion countdown.
		CompletionSignal: nil,
	}, sink)

	pid := eventPID(t, sink.Next(t))
	clock.WaitForTimerCount(t, 1)
	clock.WaitForActiveDeadline(t, time.Unix(10_120, 0))
	clock.Advance(119 * time.Second)
	assertStillRunning(t, finished)
	if err := syscall.Kill(pid, 0); err != nil {
		t.Fatalf("quiet service is not alive after idle and completion deadlines: %v", err)
	}

	clock.Advance(time.Second)
	clock.WaitForTimerCount(t, 2)
	clock.Advance(2 * time.Second)

	outcome := awaitRun(t, finished)
	if !errors.Is(outcome.err, ErrMaxRuntime) {
		t.Fatalf("Run() error = %v, want %v", outcome.err, ErrMaxRuntime)
	}
	if outcome.result.Signal == "" {
		t.Fatalf("result = %#v, want signal death", outcome.result)
	}
}

func TestServiceAcceptanceStalledProcessWaitIsSurfaced(t *testing.T) {
	assertTerminateAndWaitSurfacesStalledProcessWait(t)
}

func TestServiceAcceptanceGuardianOwnsStartupTimeoutRace(t *testing.T) {
	reservation, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serviceAddress := reservation.Addr().String()
	if err := reservation.Close(); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{}, 1)
	result, err := New(Config{
		GuardianExecutable: guardianAgentPath, TerminationGraceTime: 100 * time.Millisecond,
		StartupReadinessDeadline: 150 * time.Millisecond,
		ReadinessProbeInterval:   20 * time.Millisecond, ReadinessConnectTimeout: 10 * time.Millisecond,
	}).Run(context.Background(), Request{
		AttemptID: "startup-timeout-arbiter", Guarded: true,
		Execution: helperExecution("hang"), IdlePolicy: IgnoreIdle,
		ServiceAddress: serviceAddress, Started: func() { started <- struct{}{} },
	}, nil)
	if err != nil {
		t.Fatalf("startup timeout returned supervision error: %v", err)
	}
	select {
	case <-started:
	default:
		t.Fatal("startup deadline began without a successful spawn")
	}
	if result.SpawnError == nil || result.SpawnError.Code != contract.SpawnFailureStartupReadinessTimeout ||
		result.ExitCode != nil || result.Signal != "" {
		t.Fatalf("startup timeout result = %#v, want sole startup_readiness_timeout reason", result)
	}
}

func TestServiceAcceptanceProcessExitWinsBeforeStartupDeadline(t *testing.T) {
	reservation, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serviceAddress := reservation.Addr().String()
	if err := reservation.Close(); err != nil {
		t.Fatal(err)
	}
	exitCode := 7
	result, err := New(Config{
		GuardianExecutable: guardianAgentPath, TerminationGraceTime: 100 * time.Millisecond,
		StartupReadinessDeadline: 500 * time.Millisecond,
		ReadinessProbeInterval:   20 * time.Millisecond, ReadinessConnectTimeout: 10 * time.Millisecond,
	}).Run(context.Background(), Request{
		AttemptID: "startup-exit-arbiter", Guarded: true,
		Execution: helperExecution("exit", "7"), IdlePolicy: IgnoreIdle, ServiceAddress: serviceAddress,
	}, nil)
	if err != nil {
		t.Fatalf("process exit returned supervision error: %v", err)
	}
	if result.SpawnError != nil || result.ExitCode == nil || *result.ExitCode != exitCode || result.Signal != "" {
		t.Fatalf("process exit result = %#v, want sole exit code %d reason", result, exitCode)
	}
}

func TestServiceAcceptanceProbeNeverKillsAfterStartup(t *testing.T) {
	backend := startProbeBackend(t, "127.0.0.1:0")
	address := backend.Addr().String()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	readiness := make(chan bool, 8)
	runner := New(Config{
		GuardianExecutable: guardianAgentPath, TerminationGraceTime: 100 * time.Millisecond,
		StartupReadinessDeadline: time.Second,
		ReadinessProbeInterval:   20 * time.Millisecond, ReadinessConnectTimeout: 10 * time.Millisecond,
	})
	request := Request{
		AttemptID: "readiness-recovery", Guarded: true,
		Execution: helperExecution("hang"), IdlePolicy: IgnoreIdle, ServiceAddress: address,
		ReadinessChanged: func(startupSatisfied, ready bool) {
			if !startupSatisfied {
				t.Error("guardian regressed startup_satisfied")
			}
			readiness <- ready
		},
	}
	finished := make(chan runOutcome, 1)
	go func() {
		result, err := runner.Run(ctx, request, nil)
		finished <- runOutcome{result: result, err: err}
	}()
	if ready := waitGuardianReadiness(t, readiness); !ready {
		t.Fatal("first guardian readiness transition was not ready")
	}
	_ = backend.Close()
	if ready := waitGuardianReadiness(t, readiness); ready {
		t.Fatal("guardian did not withdraw readiness after backend loss")
	}
	assertStillRunning(t, finished)

	backend = startProbeBackend(t, address)
	defer backend.Close()
	if ready := waitGuardianReadiness(t, readiness); !ready {
		t.Fatal("guardian did not recover readiness after backend returned")
	}
	assertStillRunning(t, finished)
	cancel()
	outcome := awaitRun(t, finished)
	if !errors.Is(outcome.err, context.Canceled) || outcome.result.Signal == "" {
		t.Fatalf("guardian outcome after explicit cancellation = (%#v, %v)", outcome.result, outcome.err)
	}
}

func startProbeBackend(t *testing.T, address string) net.Listener {
	t.Helper()
	listener, err := net.Listen("tcp4", address)
	if err != nil {
		t.Fatalf("listen on runtime-local service endpoint %q: %v", address, err)
	}
	go func() {
		for {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			_ = connection.Close()
		}
	}()
	return listener
}

func waitGuardianReadiness(t *testing.T, readiness <-chan bool) bool {
	t.Helper()
	select {
	case ready := <-readiness:
		return ready
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for guardian readiness transition")
		return false
	}
}
