//go:build darwin || linux

package process

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

var (
	helperPath        string
	guardianAgentPath string
)

func TestMain(main *testing.M) {
	directory, err := os.MkdirTemp("", "wefty-process-helper-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	helperPath = filepath.Join(directory, "processhelper")
	guardianAgentPath = filepath.Join(directory, "wefty-agent")
	if runtime.GOOS == "windows" {
		helperPath += ".exe"
		guardianAgentPath += ".exe"
	}
	for _, build := range []struct {
		name, output, packagePath string
	}{
		{name: "process helper", output: helperPath, packagePath: "./testdata/processhelper"},
		{name: "guardian agent", output: guardianAgentPath, packagePath: "../../cmd/wefty-agent"},
	} {
		command := exec.Command("go", "build", "-o", build.output, build.packagePath)
		if output, buildErr := command.CombinedOutput(); buildErr != nil {
			fmt.Fprintf(os.Stderr, "build %s: %v\n%s", build.name, buildErr, output)
			os.Exit(1)
		}
	}

	code := main.Run()
	if err := os.RemoveAll(directory); err != nil && code == 0 {
		fmt.Fprintf(os.Stderr, "remove process helper directory: %v\n", err)
		code = 1
	}
	os.Exit(code)
}

func TestRunUsesDirectArgvConstructedEnvironmentAndWorkingDirectory(t *testing.T) {
	workingDirectory := t.TempDir()
	sink := newCollectingSink()
	runner := New(Config{BaseEnvironment: []string{"BASE=from-base", "OVERRIDE=base"}})

	result, err := runner.Run(context.Background(), Request{
		AttemptID: "attempt-direct",
		Execution: contract.ExecutionSpec{
			Executable:       contract.ExecutableSpec{Path: helperPath},
			Argv:             []string{"custom-argv-zero", "stdout", "@cwd", "@env:BASE", "@env:OVERRIDE", "@env:SECRET"},
			Env:              map[string]string{"OVERRIDE": "public"},
			SensitiveEnv:     map[string]string{"OVERRIDE": "sensitive", "SECRET": "hidden"},
			WorkingDirectory: workingDirectory,
		},
	}, sink)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	assertExitCode(t, result, 0)

	events := sink.Events()
	assertOrderedStream(t, events, contract.LogStdout)
	resolvedWorkingDirectory, err := filepath.EvalSymlinks(workingDirectory)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(joinStream(events, contract.LogStdout)), resolvedWorkingDirectory+"\nfrom-base\nsensitive\nhidden\n"; got != want {
		t.Fatalf("stdout = %q, want %q", got, want)
	}
	for _, event := range events {
		if event.AttemptID != "attempt-direct" {
			t.Fatalf("event attempt ID = %q", event.AttemptID)
		}
	}
}

func TestRunEmitsOrderedEventsForEachStream(t *testing.T) {
	for _, stream := range []contract.LogStream{contract.LogStdout, contract.LogStderr} {
		t.Run(string(stream), func(t *testing.T) {
			payload := strings.Repeat(string(stream)+"-", 24_000)
			sink := newCollectingSink()
			result, err := New(Config{}).Run(context.Background(), Request{
				AttemptID: "attempt-ordered",
				// The payload rides a helper directive rather than argv:
				// Linux rejects any single argument over 128 KiB (E2BIG).
				Execution: helperExecution(string(stream), fmt.Sprintf("@repeat:%s-:24000", stream)),
			}, sink)
			if err != nil {
				t.Fatalf("Run() error = %v", err)
			}
			assertExitCode(t, result, 0)

			events := sink.Events()
			assertOrderedStream(t, events, stream)
			if len(events) < 2 {
				t.Fatalf("got %d output event, want multiple chunks", len(events))
			}
			if got, want := string(joinStream(events, stream)), payload+"\n"; got != want {
				t.Fatalf("joined output length = %d, want %d", len(got), len(want))
			}
		})
	}
}

func TestServiceRunSelfExecsGuardianAndPreservesRawEvents(t *testing.T) {
	sink := newCollectingSink()
	result, err := New(Config{GuardianExecutable: guardianAgentPath}).Run(context.Background(), Request{
		AttemptID:  "attempt-guardian-raw",
		Class:      contract.JobClassService,
		Execution:  helperExecution("raw-output"),
		IdlePolicy: IgnoreIdle,
	}, sink)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	assertExitCode(t, result, 0)
	events := sink.Events()
	assertOrderedStream(t, events, contract.LogStdout)
	want := append(bytes.Repeat([]byte{'x'}, 70*1024), []byte("partial-without-newline")...)
	want = append(want, 0xff, 0xfe, 0x00, '\n')
	if got := joinStream(events, contract.LogStdout); !bytes.Equal(got, want) {
		t.Fatalf("guardian stream length = %d, want exact %d raw bytes", len(got), len(want))
	}
}

func TestRunDistinguishesProcessResults(t *testing.T) {
	t.Run("spawn error", func(t *testing.T) {
		result, err := New(Config{}).Run(context.Background(), Request{
			AttemptID: "attempt-spawn",
			Execution: contract.ExecutionSpec{
				Executable:       contract.ExecutableSpec{Path: filepath.Join(t.TempDir(), "missing")},
				Argv:             []string{"missing"},
				WorkingDirectory: t.TempDir(),
			},
		}, nil)
		if err != nil {
			t.Fatalf("spawn error returned as supervision error: %v", err)
		}
		if result.SpawnError == nil || result.ExitCode != nil || result.Signal != "" {
			t.Fatalf("result = %#v, want spawn error only", result)
		}
		if result.SpawnError.Code != contract.SpawnFailureProcessSpawn {
			t.Fatalf("spawn failure code = %q, want %q", result.SpawnError.Code, contract.SpawnFailureProcessSpawn)
		}
	})

	t.Run("nonzero exit", func(t *testing.T) {
		result, err := New(Config{}).Run(context.Background(), Request{
			AttemptID: "attempt-exit",
			Execution: helperExecution("exit", "7"),
		}, nil)
		if err != nil {
			t.Fatalf("Run() error = %v", err)
		}
		assertExitCode(t, result, 7)
	})

	t.Run("signal death", func(t *testing.T) {
		sink := newCollectingSink()
		finished := runAsync(New(Config{}), Request{
			AttemptID: "attempt-signal",
			Execution: helperExecution("hang"),
		}, sink)
		pid := eventPID(t, sink.Next(t))
		if err := syscall.Kill(pid, syscall.SIGKILL); err != nil {
			t.Fatalf("kill helper: %v", err)
		}
		outcome := awaitRun(t, finished)
		if outcome.err != nil {
			t.Fatalf("Run() error = %v", outcome.err)
		}
		if outcome.result.Signal == "" || outcome.result.ExitCode != nil || outcome.result.SpawnError != nil {
			t.Fatalf("result = %#v, want signal only", outcome.result)
		}
		if outcome.result.TerminationCause != contract.TerminationCauseSpontaneous {
			t.Fatalf("termination cause = %q, want %q", outcome.result.TerminationCause, contract.TerminationCauseSpontaneous)
		}
	})
}

func TestIdleTimeoutResetsOnOutputActivity(t *testing.T) {
	clock := newFakeClock(time.Unix(1_000, 0))
	sink := newCollectingSink()
	runner := New(Config{
		Clock:                clock,
		IdleTimeout:          10 * time.Second,
		CompletionTimeout:    time.Minute,
		TerminationGraceTime: 2 * time.Second,
	})
	finished := runAsync(runner, Request{
		AttemptID: "attempt-idle-reset",
		Execution: helperExecution("stdout", "@signal"),
	}, sink)

	pid := eventPID(t, sink.Next(t))
	clock.WaitForTimerCount(t, 1)
	clock.Advance(9 * time.Second)
	if err := syscall.Kill(pid, syscall.SIGUSR1); err != nil {
		t.Fatalf("signal output helper: %v", err)
	}
	if event := sink.Next(t); !bytes.Contains(event.Bytes, []byte("tick")) {
		t.Fatalf("output after SIGUSR1 = %q", event.Bytes)
	}
	clock.WaitForActiveDeadline(t, time.Unix(1_019, 0))

	clock.Advance(9 * time.Second)
	assertStillRunning(t, finished)
	clock.Advance(time.Second)
	clock.WaitForTimerCount(t, 2)
	clock.Advance(2 * time.Second)

	outcome := awaitRun(t, finished)
	if !errors.Is(outcome.err, ErrIdleTimeout) {
		t.Fatalf("Run() error = %v, want %v", outcome.err, ErrIdleTimeout)
	}
	if outcome.result.Signal == "" {
		t.Fatalf("result = %#v, want signal death", outcome.result)
	}
}

func TestIgnoreIdlePolicyLeavesQuietProcessRunning(t *testing.T) {
	clock := newFakeClock(time.Unix(1_500, 0))
	sink := newCollectingSink()
	runner := New(Config{
		Clock:                clock,
		IdleTimeout:          10 * time.Second,
		TerminationGraceTime: 2 * time.Second,
	})
	finished := runAsync(runner, Request{
		AttemptID:  "attempt-ignore-idle",
		Execution:  helperExecution("hang"),
		IdlePolicy: IgnoreIdle,
		Limits:     &contract.JobLimits{MaxRuntimeSeconds: 120},
	}, sink)

	pid := eventPID(t, sink.Next(t))
	clock.WaitForTimerCount(t, 1)
	clock.WaitForActiveDeadline(t, time.Unix(1_620, 0))
	clock.Advance(119 * time.Second)
	assertStillRunning(t, finished)
	if err := syscall.Kill(pid, 0); err != nil {
		t.Fatalf("quiet process is not alive after ignored idle deadline: %v", err)
	}

	if err := syscall.Kill(pid, syscall.SIGKILL); err != nil {
		t.Fatalf("kill helper: %v", err)
	}
	outcome := awaitRun(t, finished)
	if outcome.err != nil {
		t.Fatalf("Run() error = %v", outcome.err)
	}
}

func TestRunRejectsUnsupportedIdlePolicyBeforeSpawn(t *testing.T) {
	result, err := New(Config{}).Run(context.Background(), Request{
		AttemptID:  "attempt-invalid-idle-policy",
		Execution:  helperExecution("exit", "0"),
		IdlePolicy: IdlePolicy(2),
	}, nil)
	if err == nil || !strings.Contains(err.Error(), "unsupported idle policy") || result.SpawnError == nil {
		t.Fatalf("Run() = (%#v, %v), want idle policy validation error", result, err)
	}
}

func TestCompletionTimeoutStartsOnSignal(t *testing.T) {
	clock := newFakeClock(time.Unix(2_000, 0))
	completion := make(chan struct{})
	sink := newCollectingSink()
	runner := New(Config{
		Clock:                clock,
		IdleTimeout:          10 * time.Second,
		CompletionTimeout:    10 * time.Second,
		TerminationGraceTime: 2 * time.Second,
	})
	finished := runAsync(runner, Request{
		AttemptID:        "attempt-completion",
		Execution:        helperExecution("hang"),
		CompletionSignal: completion,
	}, sink)

	_ = eventPID(t, sink.Next(t))
	clock.WaitForTimerCount(t, 1)
	clock.WaitForActiveDeadline(t, time.Unix(2_010, 0))
	clock.Advance(9 * time.Second)
	assertStillRunning(t, finished)
	close(completion)
	clock.WaitForTimerCount(t, 2)
	clock.WaitForActiveDeadline(t, time.Unix(2_019, 0))
	clock.Advance(time.Second)
	assertStillRunning(t, finished)
	clock.Advance(9 * time.Second)
	clock.WaitForTimerCount(t, 3)
	clock.Advance(2 * time.Second)

	outcome := awaitRun(t, finished)
	if !errors.Is(outcome.err, ErrCompletionTimeout) {
		t.Fatalf("Run() error = %v, want %v", outcome.err, ErrCompletionTimeout)
	}
}

func TestMaxRuntimeUsesInjectedClock(t *testing.T) {
	clock := newFakeClock(time.Unix(2_500, 0))
	sink := newCollectingSink()
	runner := New(Config{
		Clock:                clock,
		IdleTimeout:          10 * time.Minute,
		TerminationGraceTime: 2 * time.Second,
	})
	finished := runAsync(runner, Request{
		AttemptID: "attempt-runtime",
		Execution: helperExecution("hang"),
		Limits:    &contract.JobLimits{MaxRuntimeSeconds: 10},
	}, sink)

	_ = eventPID(t, sink.Next(t))
	clock.WaitForTimerCount(t, 2)
	clock.Advance(10 * time.Second)
	clock.WaitForTimerCount(t, 3)
	clock.Advance(2 * time.Second)

	outcome := awaitRun(t, finished)
	if !errors.Is(outcome.err, ErrMaxRuntime) {
		t.Fatalf("Run() error = %v, want %v", outcome.err, ErrMaxRuntime)
	}
}

func TestTimeoutTerminatesThenKillsEntireProcessGroup(t *testing.T) {
	clock := newFakeClock(time.Unix(3_000, 0))
	sink := newCollectingSink()
	runner := New(Config{
		Clock:                clock,
		IdleTimeout:          10 * time.Second,
		TerminationGraceTime: 2 * time.Second,
	})
	finished := runAsync(runner, Request{
		AttemptID: "attempt-group",
		Execution: helperExecution("spawn-child"),
	}, sink)

	childPID := eventPID(t, sink.Next(t))
	clock.WaitForTimerCount(t, 1)
	clock.Advance(10 * time.Second)
	if event := sink.Next(t); !bytes.Contains(event.Bytes, []byte("term")) {
		t.Fatalf("graceful termination output = %q, want term", event.Bytes)
	}
	clock.WaitForTimerCount(t, 2)
	clock.Advance(2 * time.Second)

	outcome := awaitRun(t, finished)
	if !errors.Is(outcome.err, ErrIdleTimeout) {
		t.Fatalf("Run() error = %v, want %v", outcome.err, ErrIdleTimeout)
	}
	if outcome.result.Signal == "" {
		t.Fatalf("result = %#v, want signal death", outcome.result)
	}
	waitForProcessGone(t, childPID)
}

func TestRunPassesWorkingDirectoryStraightToProcessSpawn(t *testing.T) {
	file := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	result, err := New(Config{}).Run(context.Background(), Request{
		AttemptID: "attempt-workdir",
		Execution: contract.ExecutionSpec{
			Executable:       contract.ExecutableSpec{Path: helperPath},
			Argv:             []string{"helper", "exit", "0"},
			WorkingDirectory: file,
		},
	}, nil)
	if err != nil || result.SpawnError == nil || result.SpawnError.Code != contract.SpawnFailureProcessSpawn {
		t.Fatalf("Run() = (%#v, %v), want process spawn failure", result, err)
	}
}

func TestTerminateAndWaitSurfacesStalledProcessWait(t *testing.T) {
	assertTerminateAndWaitSurfacesStalledProcessWait(t)
}

func assertTerminateAndWaitSurfacesStalledProcessWait(t *testing.T) {
	t.Helper()
	clock := newFakeClock(time.Unix(4_000, 0))
	runner := New(Config{
		Clock: clock, TerminationGraceTime: 2 * time.Second, ProcessReapTimeout: 3 * time.Second,
	})
	wait := make(chan waitResult)
	done := make(chan waitResult, 1)
	go func() { done <- runner.terminateAndWait(1<<30, wait) }()
	clock.WaitForTimerCount(t, 1)
	clock.Advance(2 * time.Second)
	clock.WaitForTimerCount(t, 2)
	select {
	case outcome := <-done:
		t.Fatalf("terminateAndWait returned before reap bound: %#v", outcome)
	default:
	}
	clock.Advance(3 * time.Second)
	select {
	case outcome := <-done:
		if !errors.Is(outcome.err, ErrProcessReapTimeout) || outcome.state != nil {
			t.Fatalf("terminateAndWait outcome = %#v, want explicit reap timeout", outcome)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("terminateAndWait silently stalled past reap bound")
	}
}

func helperExecution(mode string, arguments ...string) contract.ExecutionSpec {
	argv := []string{"process-helper", mode}
	argv = append(argv, arguments...)
	return contract.ExecutionSpec{
		Executable:       contract.ExecutableSpec{Path: helperPath},
		Argv:             argv,
		WorkingDirectory: os.TempDir(),
	}
}

func assertExitCode(t *testing.T, result contract.ProcessResult, want int) {
	t.Helper()
	if result.ExitCode == nil || *result.ExitCode != want || result.SpawnError != nil || result.Signal != "" {
		t.Fatalf("result = %#v, want exit code %d only", result, want)
	}
}

func assertOrderedStream(t *testing.T, events []contract.LogEvent, stream contract.LogStream) {
	t.Helper()
	var sequence uint64
	for _, event := range events {
		if event.Stream != stream {
			continue
		}
		if event.Sequence != sequence {
			t.Fatalf("%s sequence = %d, want %d", stream, event.Sequence, sequence)
		}
		sequence++
	}
	if sequence == 0 {
		t.Fatalf("no %s events", stream)
	}
}

func joinStream(events []contract.LogEvent, stream contract.LogStream) []byte {
	var joined []byte
	for _, event := range events {
		if event.Stream == stream {
			joined = append(joined, event.Bytes...)
		}
	}
	return joined
}

type collectingSink struct {
	mu     sync.Mutex
	events []contract.LogEvent
	next   chan contract.LogEvent
}

func newCollectingSink() *collectingSink {
	return &collectingSink{next: make(chan contract.LogEvent, 32)}
}

func (sink *collectingSink) WriteOutput(_ context.Context, event contract.LogEvent) error {
	sink.mu.Lock()
	sink.events = append(sink.events, event)
	sink.mu.Unlock()
	sink.next <- event
	return nil
}

func (sink *collectingSink) Events() []contract.LogEvent {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	return append([]contract.LogEvent(nil), sink.events...)
}

func (sink *collectingSink) Next(t *testing.T) contract.LogEvent {
	t.Helper()
	select {
	case event := <-sink.next:
		return event
	// A failure budget, not a success-path delay: it costs nothing when the
	// test passes, and 5s was too tight on a loaded shared macOS runner once
	// the guardian tests started spawning real processes alongside these.
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for process output")
		return contract.LogEvent{}
	}
}

type runOutcome struct {
	result contract.ProcessResult
	err    error
}

func runAsync(runner *Runner, request Request, sink OutputSink) <-chan runOutcome {
	finished := make(chan runOutcome, 1)
	go func() {
		result, err := runner.Run(context.Background(), request, sink)
		finished <- runOutcome{result: result, err: err}
	}()
	return finished
}

func awaitRun(t *testing.T, finished <-chan runOutcome) runOutcome {
	t.Helper()
	select {
	case outcome := <-finished:
		return outcome
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for runner")
		return runOutcome{}
	}
}

func assertStillRunning(t *testing.T, finished <-chan runOutcome) {
	t.Helper()
	select {
	case outcome := <-finished:
		t.Fatalf("runner finished early: result=%#v error=%v", outcome.result, outcome.err)
	default:
	}
}

func eventPID(t *testing.T, event contract.LogEvent) int {
	t.Helper()
	pid, err := strconv.Atoi(strings.TrimSpace(string(event.Bytes)))
	if err != nil {
		t.Fatalf("parse PID from %q: %v", event.Bytes, err)
	}
	return pid
}

func waitForProcessGone(t *testing.T, pid int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		err := syscall.Kill(pid, 0)
		if errors.Is(err, syscall.ESRCH) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("process %d still exists after group kill", pid)
}

type fakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
}

func newFakeClock(now time.Time) *fakeClock {
	return &fakeClock{now: now}
}

func (clock *fakeClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

func (clock *fakeClock) NewTimer(duration time.Duration) Timer {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	timer := &fakeTimer{
		clock:    clock,
		channel:  make(chan time.Time, 1),
		deadline: clock.now.Add(duration),
		active:   true,
	}
	clock.timers = append(clock.timers, timer)
	return timer
}

func (clock *fakeClock) Advance(duration time.Duration) {
	clock.mu.Lock()
	clock.now = clock.now.Add(duration)
	now := clock.now
	var ready []*fakeTimer
	for _, timer := range clock.timers {
		if timer.active && !timer.deadline.After(now) {
			timer.active = false
			ready = append(ready, timer)
		}
	}
	clock.mu.Unlock()

	for _, timer := range ready {
		timer.channel <- now
	}
}

func (clock *fakeClock) WaitForTimerCount(t *testing.T, count int) {
	t.Helper()
	waitForCondition(t, func() bool {
		clock.mu.Lock()
		defer clock.mu.Unlock()
		return len(clock.timers) >= count
	}, fmt.Sprintf("%d timers", count))
}

func (clock *fakeClock) WaitForActiveDeadline(t *testing.T, deadline time.Time) {
	t.Helper()
	waitForCondition(t, func() bool {
		clock.mu.Lock()
		defer clock.mu.Unlock()
		for _, timer := range clock.timers {
			if timer.active && timer.deadline.Equal(deadline) {
				return true
			}
		}
		return false
	}, "active timer deadline "+deadline.String())
}

type fakeTimer struct {
	clock    *fakeClock
	channel  chan time.Time
	deadline time.Time
	active   bool
}

func (timer *fakeTimer) C() <-chan time.Time { return timer.channel }

func (timer *fakeTimer) Stop() bool {
	timer.clock.mu.Lock()
	defer timer.clock.mu.Unlock()
	wasActive := timer.active
	timer.active = false
	return wasActive
}

func (timer *fakeTimer) Reset(duration time.Duration) bool {
	timer.clock.mu.Lock()
	defer timer.clock.mu.Unlock()
	wasActive := timer.active
	timer.deadline = timer.clock.now.Add(duration)
	timer.active = true
	return wasActive
}

func waitForCondition(t *testing.T, condition func() bool, description string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", description)
}
