//go:build darwin || linux

package process

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"syscall"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

type guardianDecodeResult struct {
	message guardianControlMessage
	err     error
}

type guardianStatusResult struct {
	message guardianStatusMessage
	err     error
}

func newGuardianSocketPair() (*os.File, *os.File, error) {
	descriptors, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		return nil, nil, err
	}
	syscall.CloseOnExec(descriptors[0])
	syscall.CloseOnExec(descriptors[1])
	return os.NewFile(uintptr(descriptors[0]), "wefty-agent-control"),
		os.NewFile(uintptr(descriptors[1]), "wefty-guardian-control"), nil
}

func (runner *Runner) runGuarded(
	ctx context.Context,
	request Request,
	sink OutputSink,
	idleTimeout, completionTimeout, maxRuntime time.Duration,
) (contract.ProcessResult, error) {
	agentEndpoint, guardianEndpoint, err := newGuardianSocketPair()
	if err != nil {
		return spawnFailure(contract.SpawnFailureProcessGroupSetup, err), err
	}
	defer agentEndpoint.Close()

	activity := newActivityTracker(runner.clock.Now())
	failure := newSinkFailure()
	command := exec.Command(runner.guardianExecutable, guardianArg)
	command.ExtraFiles = []*os.File{guardianEndpoint}
	command.Stdout = &eventWriter{
		ctx: ctx, clock: runner.clock, attemptID: request.AttemptID,
		stream: contract.LogStdout, sink: sink, activity: activity, failure: failure,
	}
	command.Stderr = &eventWriter{
		ctx: ctx, clock: runner.clock, attemptID: request.AttemptID,
		stream: contract.LogStderr, sink: sink, activity: activity, failure: failure,
	}
	if err := command.Start(); err != nil {
		_ = guardianEndpoint.Close()
		return spawnFailure(contract.SpawnFailureProcessSpawn, err), nil
	}
	_ = guardianEndpoint.Close()

	guardianWait := make(chan waitResult, 1)
	go func() {
		err := command.Wait()
		guardianWait <- waitResult{err: err, state: command.ProcessState}
	}()
	stopGuardian := func() {
		_ = json.NewEncoder(agentEndpoint).Encode(guardianControlMessage{Type: guardianMessageStop})
	}
	failGuardian := func(err error) (contract.ProcessResult, error) {
		_ = agentEndpoint.Close()
		select {
		case <-guardianWait:
		case <-time.After(runner.terminationGraceTime + time.Second):
			_ = command.Process.Kill()
			<-guardianWait
		}
		return spawnFailure(contract.SpawnFailureProcessWait, err), err
	}

	start := guardianControlMessage{Type: guardianMessageStart, Start: &guardianStart{
		Path: request.Execution.Executable.Path, Args: append([]string(nil), request.Execution.Argv...),
		Directory:        request.Execution.WorkingDirectory,
		Environment:      buildEnvironment(runner.baseEnvironment, request.Execution.Env, request.Execution.SensitiveEnv),
		TerminationGrace: runner.terminationGraceTime,
	}}
	if err := json.NewEncoder(agentEndpoint).Encode(start); err != nil {
		return failGuardian(fmt.Errorf("send guardian start: %w", err))
	}
	statusDecoder := json.NewDecoder(agentEndpoint)
	started, err := decodeGuardianStatus(statusDecoder)
	if err != nil {
		return failGuardian(fmt.Errorf("receive guardian started: %w", err))
	}
	if started.Type == guardianMessageExited {
		<-guardianWait
		return *started.Result, nil
	}

	statuses := make(chan guardianStatusResult, 1)
	go func() {
		message, err := decodeGuardianStatus(statusDecoder)
		statuses <- guardianStatusResult{message: message, err: err}
	}()

	var idleTimer Timer
	var idleTimerChannel <-chan time.Time
	if request.IdlePolicy == MonitorIdle {
		idleTimer = runner.clock.NewTimer(idleTimeout)
		idleTimerChannel = idleTimer.C()
		defer stopTimer(idleTimer)
	}
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
	var supervisionErr error
	stopping := false
	finish := func(status guardianStatusResult, guardianOutcome waitResult) (contract.ProcessResult, error) {
		if status.err != nil {
			return spawnFailure(contract.SpawnFailureProcessWait, status.err), fmt.Errorf("receive guardian result: %w", status.err)
		}
		if status.message.Type != guardianMessageExited {
			err := fmt.Errorf("guardian sent unexpected %q message", status.message.Type)
			return spawnFailure(contract.SpawnFailureProcessWait, err), err
		}
		if guardianOutcome.err != nil {
			return spawnFailure(contract.SpawnFailureProcessWait, guardianOutcome.err), guardianOutcome.err
		}
		result := *status.message.Result
		if sinkErr := failure.Err(); sinkErr != nil {
			return result, fmt.Errorf("output sink: %w", sinkErr)
		}
		return result, supervisionErr
	}

	for {
		select {
		case status := <-statuses:
			if status.err != nil {
				return failGuardian(fmt.Errorf("receive guardian result: %w", status.err))
			}
			if status.message.Type != guardianMessageExited {
				return failGuardian(fmt.Errorf("guardian sent unexpected %q message", status.message.Type))
			}
			guardianOutcome := <-guardianWait
			return finish(status, guardianOutcome)

		case outcome := <-guardianWait:
			// The guardian writes its result before exiting, but the Wait goroutine
			// can win this select before the decoder goroutine is scheduled.
			return finish(<-statuses, outcome)

		case <-activity.Changed():
			if idleTimerChannel == nil || stopping {
				continue
			}
			remaining := activity.Remaining(runner.clock.Now(), idleTimeout)
			if remaining <= 0 {
				supervisionErr = ErrIdleTimeout
				stopping = true
				stopGuardian()
				continue
			}
			resetTimer(idleTimer, remaining)

		case <-idleTimerChannel:
			if stopping {
				continue
			}
			remaining := activity.Remaining(runner.clock.Now(), idleTimeout)
			if remaining > 0 {
				resetTimer(idleTimer, remaining)
				continue
			}
			supervisionErr = ErrIdleTimeout
			stopping = true
			stopGuardian()

		case <-completionSignal:
			completionSignal = nil
			stopTimer(idleTimer)
			idleTimerChannel = nil
			completionTimer = runner.clock.NewTimer(completionTimeout)
			completionTimerChannel = completionTimer.C()
			defer stopTimer(completionTimer)

		case <-completionTimerChannel:
			if !stopping {
				supervisionErr = ErrCompletionTimeout
				stopping = true
				stopGuardian()
			}

		case <-maxRuntimeChannel:
			if !stopping {
				supervisionErr = ErrMaxRuntime
				stopping = true
				stopGuardian()
			}

		case <-failure.Changed():
			if !stopping {
				supervisionErr = fmt.Errorf("output sink: %w", failure.Err())
				stopping = true
				stopGuardian()
			}

		case <-ctx.Done():
			if !stopping {
				supervisionErr = ctx.Err()
				stopping = true
				stopGuardian()
			}
		}
	}
}

func serveGuardian(endpoint *os.File, stdout, stderr io.Writer) error {
	decoder := json.NewDecoder(endpoint)
	encoder := json.NewEncoder(endpoint)
	var message guardianControlMessage
	if err := decoder.Decode(&message); err != nil {
		return fmt.Errorf("receive guardian start: %w", err)
	}
	if message.Type != guardianMessageStart || message.Start == nil {
		return errors.New("guardian expected one structured start message")
	}
	start := message.Start
	if start.Path == "" || len(start.Args) == 0 || start.Args[0] == "" || start.Directory == "" || start.TerminationGrace <= 0 {
		return errors.New("guardian start message is invalid")
	}
	command := &exec.Cmd{
		Path: start.Path, Args: append([]string(nil), start.Args...), Dir: start.Directory,
		Env: append([]string(nil), start.Environment...), Stdout: stdout, Stderr: stderr,
	}
	if err := configureProcessGroup(command); err != nil {
		result := spawnFailure(contract.SpawnFailureProcessGroupSetup, err)
		_ = encoder.Encode(guardianStatusMessage{Type: guardianMessageExited, Result: &result})
		return nil
	}
	if err := command.Start(); err != nil {
		result := spawnFailure(contract.SpawnFailureProcessSpawn, err)
		_ = encoder.Encode(guardianStatusMessage{Type: guardianMessageExited, Result: &result})
		return nil
	}
	processGroupID := command.Process.Pid
	wait := make(chan waitResult, 1)
	go func() {
		err := command.Wait()
		wait <- waitResult{err: err, state: command.ProcessState}
	}()
	if err := encoder.Encode(guardianStatusMessage{
		Type: guardianMessageStarted, PID: command.Process.Pid, ProcessGroupID: processGroupID,
	}); err != nil {
		_ = guardianTerminateAndWait(processGroupID, start.TerminationGrace, wait)
		return nil
	}
	control := make(chan guardianDecodeResult, 1)
	go func() {
		var next guardianControlMessage
		err := decoder.Decode(&next)
		control <- guardianDecodeResult{message: next, err: err}
	}()

	var outcome waitResult
	cause := contract.TerminationCauseSpontaneous
	disconnected := false
	select {
	case outcome = <-wait:
		guardianCleanupRemainingGroup(processGroupID, start.TerminationGrace)
	case next := <-control:
		if next.err != nil && !errors.Is(next.err, io.EOF) {
			disconnected = true
		} else if next.err == nil && next.message.Type != guardianMessageStop {
			disconnected = true
		} else if errors.Is(next.err, io.EOF) {
			disconnected = true
		}
		cause = contract.TerminationCauseGuardian
		outcome = guardianTerminateAndWait(processGroupID, start.TerminationGrace, wait)
	}
	result := resultFromWait(outcome.err, outcome.state, cause)
	if disconnected {
		_ = encoder.Encode(guardianStatusMessage{Type: guardianMessageExited, Result: &result})
		return nil
	}
	return encoder.Encode(guardianStatusMessage{Type: guardianMessageExited, Result: &result})
}

func guardianTerminateAndWait(processGroupID int, grace time.Duration, wait <-chan waitResult) waitResult {
	_ = terminateProcessGroup(processGroupID)
	timer := time.NewTimer(grace)
	defer timer.Stop()
	var completed *waitResult
	for {
		if completed != nil && !processGroupAlive(processGroupID) {
			return *completed
		}
		select {
		case outcome := <-wait:
			completed = &outcome
		case <-timer.C:
			_ = killProcessGroup(processGroupID)
			if completed != nil {
				return *completed
			}
			return <-wait
		}
	}
}

func guardianCleanupRemainingGroup(processGroupID int, grace time.Duration) {
	if !processGroupAlive(processGroupID) {
		return
	}
	_ = terminateProcessGroup(processGroupID)
	timer := time.NewTimer(grace)
	defer timer.Stop()
	<-timer.C
	_ = killProcessGroup(processGroupID)
}
