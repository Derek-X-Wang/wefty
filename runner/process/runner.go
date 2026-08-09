// Package process executes native one-shot workloads without a shell.
package process

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

const (
	DefaultIdleTimeout          = 600 * time.Second
	DefaultCompletionTimeout    = 60 * time.Second
	DefaultTerminationGraceTime = 5 * time.Second
)

var (
	ErrIdleTimeout       = errors.New("process idle timeout exceeded")
	ErrCompletionTimeout = errors.New("process completion timeout exceeded")
	ErrMaxRuntime        = errors.New("process maximum runtime exceeded")
)

// OutputSink receives raw output events. Calls are serialized within a stream,
// but stdout and stderr may call the sink concurrently.
type OutputSink interface {
	WriteOutput(context.Context, contract.LogEvent) error
}

// OutputSinkFunc adapts a function into an OutputSink.
type OutputSinkFunc func(context.Context, contract.LogEvent) error

func (function OutputSinkFunc) WriteOutput(ctx context.Context, event contract.LogEvent) error {
	return function(ctx, event)
}

// Request describes one native process execution. Closing or sending on
// CompletionSignal replaces the idle clock with the completion clock; a nil
// channel leaves the runner in its idle-timeout phase until the process exits.
type Request struct {
	AttemptID        string
	Execution        contract.ExecutionSpec
	Limits           *contract.JobLimits
	CompletionSignal <-chan struct{}
}

// Config holds process-runner dependencies and defaults. A nil Clock uses the
// wall clock. A nil BaseEnvironment inherits the runner process environment.
type Config struct {
	Clock                Clock
	BaseEnvironment      []string
	IdleTimeout          time.Duration
	CompletionTimeout    time.Duration
	TerminationGraceTime time.Duration
}

// Runner executes kind=process workloads.
type Runner struct {
	clock                Clock
	baseEnvironment      []string
	idleTimeout          time.Duration
	completionTimeout    time.Duration
	terminationGraceTime time.Duration
}

// New creates a process runner with defaults for every zero-valued duration.
func New(config Config) *Runner {
	clock := config.Clock
	if clock == nil {
		clock = realClock{}
	}

	baseEnvironment := config.BaseEnvironment
	if baseEnvironment == nil {
		baseEnvironment = os.Environ()
	}

	return &Runner{
		clock:                clock,
		baseEnvironment:      append([]string(nil), baseEnvironment...),
		idleTimeout:          durationOrDefault(config.IdleTimeout, DefaultIdleTimeout),
		completionTimeout:    durationOrDefault(config.CompletionTimeout, DefaultCompletionTimeout),
		terminationGraceTime: durationOrDefault(config.TerminationGraceTime, DefaultTerminationGraceTime),
	}
}

// Run executes one process and waits for it and all captured output to finish.
// Process outcomes, including spawn failures and nonzero exits, are represented
// in ProcessResult. The returned error reports supervision failures such as a
// timeout, context cancellation, or output-sink failure.
func (runner *Runner) Run(ctx context.Context, request Request, sink OutputSink) (contract.ProcessResult, error) {
	if sink == nil {
		sink = OutputSinkFunc(func(context.Context, contract.LogEvent) error { return nil })
	}

	if err := validateRequest(ctx, request); err != nil {
		return contract.ProcessResult{SpawnError: err.Error()}, err
	}

	idleTimeout, completionTimeout, maxRuntime, err := runner.timeouts(request.Limits)
	if err != nil {
		return contract.ProcessResult{SpawnError: err.Error()}, err
	}

	command := &exec.Cmd{
		Path: request.Execution.Executable.Path,
		Args: append([]string(nil), request.Execution.Argv...),
		Dir:  request.Execution.WorkingDirectory,
		Env:  buildEnvironment(runner.baseEnvironment, request.Execution.Env, request.Execution.SensitiveEnv),
	}
	if err := configureProcessGroup(command); err != nil {
		return contract.ProcessResult{SpawnError: err.Error()}, err
	}

	activity := newActivityTracker(runner.clock.Now())
	failure := newSinkFailure()
	command.Stdout = &eventWriter{
		ctx:       ctx,
		clock:     runner.clock,
		attemptID: request.AttemptID,
		stream:    contract.LogStdout,
		sink:      sink,
		activity:  activity,
		failure:   failure,
	}
	command.Stderr = &eventWriter{
		ctx:       ctx,
		clock:     runner.clock,
		attemptID: request.AttemptID,
		stream:    contract.LogStderr,
		sink:      sink,
		activity:  activity,
		failure:   failure,
	}

	if err := command.Start(); err != nil {
		return contract.ProcessResult{SpawnError: err.Error()}, nil
	}

	wait := make(chan waitResult, 1)
	go func() {
		err := command.Wait()
		wait <- waitResult{err: err, state: command.ProcessState}
	}()

	idleTimer := runner.clock.NewTimer(idleTimeout)
	idleTimerChannel := idleTimer.C()
	defer stopTimer(idleTimer)

	var maxRuntimeTimer Timer
	var maxRuntimeChannel <-chan time.Time
	if maxRuntime > 0 {
		maxRuntimeTimer = runner.clock.NewTimer(maxRuntime)
		maxRuntimeChannel = maxRuntimeTimer.C()
		defer stopTimer(maxRuntimeTimer)
	}

	var completionTimer Timer
	var completionTimerChannel <-chan time.Time
	completionSignal := request.CompletionSignal

	for {
		select {
		case outcome := <-wait:
			runner.cleanupRemainingGroup(command.Process.Pid)
			result := resultFromWait(outcome.err, outcome.state)
			if sinkErr := failure.Err(); sinkErr != nil {
				return result, fmt.Errorf("output sink: %w", sinkErr)
			}
			return result, nil

		case <-activity.Changed():
			if idleTimerChannel == nil {
				continue
			}
			remaining := activity.Remaining(runner.clock.Now(), idleTimeout)
			if remaining <= 0 {
				outcome := runner.terminateAndWait(command.Process.Pid, wait)
				return resultFromWait(outcome.err, outcome.state), ErrIdleTimeout
			}
			resetTimer(idleTimer, remaining)

		case <-idleTimerChannel:
			remaining := activity.Remaining(runner.clock.Now(), idleTimeout)
			if remaining > 0 {
				resetTimer(idleTimer, remaining)
				continue
			}
			outcome := runner.terminateAndWait(command.Process.Pid, wait)
			return resultFromWait(outcome.err, outcome.state), ErrIdleTimeout

		case <-completionSignal:
			completionSignal = nil
			stopTimer(idleTimer)
			idleTimerChannel = nil
			completionTimer = runner.clock.NewTimer(completionTimeout)
			completionTimerChannel = completionTimer.C()
			defer stopTimer(completionTimer)

		case <-completionTimerChannel:
			outcome := runner.terminateAndWait(command.Process.Pid, wait)
			return resultFromWait(outcome.err, outcome.state), ErrCompletionTimeout

		case <-maxRuntimeChannel:
			outcome := runner.terminateAndWait(command.Process.Pid, wait)
			return resultFromWait(outcome.err, outcome.state), ErrMaxRuntime

		case <-failure.Changed():
			outcome := runner.terminateAndWait(command.Process.Pid, wait)
			return resultFromWait(outcome.err, outcome.state), fmt.Errorf("output sink: %w", failure.Err())

		case <-ctx.Done():
			outcome := runner.terminateAndWait(command.Process.Pid, wait)
			return resultFromWait(outcome.err, outcome.state), ctx.Err()
		}
	}
}

func (runner *Runner) timeouts(limits *contract.JobLimits) (time.Duration, time.Duration, time.Duration, error) {
	idleTimeout := runner.idleTimeout
	completionTimeout := runner.completionTimeout
	var maxRuntime time.Duration
	if limits == nil {
		return idleTimeout, completionTimeout, maxRuntime, nil
	}

	if limits.IdleTimeoutSeconds < 0 || limits.CompletionTimeoutSeconds < 0 || limits.MaxRuntimeSeconds < 0 {
		return 0, 0, 0, errors.New("process limits cannot be negative")
	}
	if limits.IdleTimeoutSeconds > 0 {
		idleTimeout = time.Duration(limits.IdleTimeoutSeconds) * time.Second
	}
	if limits.CompletionTimeoutSeconds > 0 {
		completionTimeout = time.Duration(limits.CompletionTimeoutSeconds) * time.Second
	}
	if limits.MaxRuntimeSeconds > 0 {
		maxRuntime = time.Duration(limits.MaxRuntimeSeconds) * time.Second
	}
	return idleTimeout, completionTimeout, maxRuntime, nil
}

func (runner *Runner) terminateAndWait(processGroupID int, wait <-chan waitResult) waitResult {
	_ = terminateProcessGroup(processGroupID)
	graceTimer := runner.clock.NewTimer(runner.terminationGraceTime)
	defer stopTimer(graceTimer)

	var completed *waitResult
	for {
		if completed != nil && !processGroupAlive(processGroupID) {
			return *completed
		}

		select {
		case outcome := <-wait:
			completed = &outcome
		case <-graceTimer.C():
			_ = killProcessGroup(processGroupID)
			if completed != nil {
				return *completed
			}
			return <-wait
		}
	}
}

func (runner *Runner) cleanupRemainingGroup(processGroupID int) {
	if !processGroupAlive(processGroupID) {
		return
	}
	_ = terminateProcessGroup(processGroupID)
	graceTimer := runner.clock.NewTimer(runner.terminationGraceTime)
	defer stopTimer(graceTimer)
	<-graceTimer.C()
	_ = killProcessGroup(processGroupID)
}

func validateRequest(ctx context.Context, request Request) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if request.AttemptID == "" {
		return errors.New("attempt ID is required")
	}
	if request.Execution.Executable.Path == "" {
		return errors.New("process executable path is required")
	}
	if len(request.Execution.Argv) == 0 || request.Execution.Argv[0] == "" {
		return errors.New("process argv must contain a non-empty argv[0]")
	}
	if request.Execution.WorkingDirectory == "" {
		return errors.New("process working directory is required")
	}
	info, err := os.Stat(request.Execution.WorkingDirectory)
	if err != nil {
		return fmt.Errorf("validate working directory: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("validate working directory: %q is not a directory", request.Execution.WorkingDirectory)
	}
	return nil
}

func buildEnvironment(base []string, public, sensitive map[string]string) []string {
	environment := make(map[string]string, len(base)+len(public)+len(sensitive))
	for _, entry := range base {
		key, value, found := strings.Cut(entry, "=")
		if found {
			environment[key] = value
		}
	}
	for key, value := range public {
		environment[key] = value
	}
	for key, value := range sensitive {
		environment[key] = value
	}

	keys := make([]string, 0, len(environment))
	for key := range environment {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	result := make([]string, 0, len(keys))
	for _, key := range keys {
		result = append(result, key+"="+environment[key])
	}
	return result
}

func durationOrDefault(value, fallback time.Duration) time.Duration {
	if value == 0 {
		return fallback
	}
	return value
}

type waitResult struct {
	err   error
	state *os.ProcessState
}

type eventWriter struct {
	ctx       context.Context
	clock     Clock
	attemptID string
	stream    contract.LogStream
	sink      OutputSink
	activity  *activityTracker
	failure   *sinkFailure
	sequence  uint64
}

func (writer *eventWriter) Write(bytes []byte) (int, error) {
	if len(bytes) == 0 {
		return 0, nil
	}

	timestamp := writer.clock.Now()
	writer.activity.Mark(timestamp)
	if writer.failure.Err() != nil {
		return len(bytes), nil
	}

	event := contract.LogEvent{
		AttemptID: writer.attemptID,
		Stream:    writer.stream,
		Sequence:  writer.sequence,
		Timestamp: timestamp,
		Bytes:     append([]byte(nil), bytes...),
	}
	writer.sequence++
	if err := writer.sink.WriteOutput(writer.ctx, event); err != nil {
		writer.failure.Set(err)
	}
	return len(bytes), nil
}

var _ io.Writer = (*eventWriter)(nil)

type activityTracker struct {
	mu      sync.Mutex
	last    time.Time
	changed chan struct{}
}

func newActivityTracker(now time.Time) *activityTracker {
	return &activityTracker{last: now, changed: make(chan struct{}, 1)}
}

func (tracker *activityTracker) Mark(now time.Time) {
	tracker.mu.Lock()
	tracker.last = now
	tracker.mu.Unlock()
	select {
	case tracker.changed <- struct{}{}:
	default:
	}
}

func (tracker *activityTracker) Changed() <-chan struct{} { return tracker.changed }

func (tracker *activityTracker) Remaining(now time.Time, timeout time.Duration) time.Duration {
	tracker.mu.Lock()
	last := tracker.last
	tracker.mu.Unlock()
	return timeout - now.Sub(last)
}

type sinkFailure struct {
	mu      sync.Mutex
	err     error
	changed chan struct{}
}

func newSinkFailure() *sinkFailure {
	return &sinkFailure{changed: make(chan struct{}, 1)}
}

func (failure *sinkFailure) Set(err error) {
	failure.mu.Lock()
	if failure.err == nil {
		failure.err = err
	}
	failure.mu.Unlock()
	select {
	case failure.changed <- struct{}{}:
	default:
	}
}

func (failure *sinkFailure) Err() error {
	failure.mu.Lock()
	defer failure.mu.Unlock()
	return failure.err
}

func (failure *sinkFailure) Changed() <-chan struct{} { return failure.changed }
