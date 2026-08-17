//go:build service_acceptance && (darwin || linux)

package process

import (
	"errors"
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
