//go:build darwin || linux

package process

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
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
		Directory:                request.Execution.WorkingDirectory,
		Environment:              buildEnvironment(runner.baseEnvironment, request.Execution.Env, request.Execution.SensitiveEnv),
		TerminationGrace:         runner.terminationGraceTime,
		ServiceAddress:           request.ServiceAddress,
		StartupReadinessDeadline: runner.startupReadinessDeadline,
		ReadinessProbeInterval:   runner.readinessProbeInterval,
		ReadinessConnectTimeout:  runner.readinessConnectTimeout,
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
	if request.Started != nil {
		request.Started()
	}

	statuses := make(chan guardianStatusResult, 1)
	go func() {
		for {
			message, err := decodeGuardianStatus(statusDecoder)
			statuses <- guardianStatusResult{message: message, err: err}
			if err != nil || message.Type == guardianMessageExited {
				return
			}
		}
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
	handleReadiness := func(status guardianStatusResult) (bool, error) {
		if status.err != nil {
			return false, status.err
		}
		if status.message.Type != guardianMessageReadiness {
			return false, nil
		}
		if request.ReadinessChanged != nil {
			request.ReadinessChanged(status.message.StartupSatisfied, *status.message.Ready)
		}
		return true, nil
	}
	waitForExitedStatus := func() guardianStatusResult {
		for {
			status := <-statuses
			handled, err := handleReadiness(status)
			if err != nil || !handled {
				return status
			}
		}
	}

	for {
		select {
		case status := <-statuses:
			handled, err := handleReadiness(status)
			if err != nil {
				return failGuardian(fmt.Errorf("receive guardian result: %w", status.err))
			}
			if handled {
				continue
			}
			if status.message.Type != guardianMessageExited {
				return failGuardian(fmt.Errorf("guardian sent unexpected %q message", status.message.Type))
			}
			guardianOutcome := <-guardianWait
			return finish(status, guardianOutcome)

		case outcome := <-guardianWait:
			// The guardian writes its result before exiting, but the Wait goroutine
			// can win this select before the decoder goroutine is scheduled.
			return finish(waitForExitedStatus(), outcome)

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
	if start.ServiceAddress != "" && (start.StartupReadinessDeadline <= 0 || start.ReadinessProbeInterval <= 0 || start.ReadinessConnectTimeout <= 0) {
		return errors.New("guardian readiness configuration is invalid")
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

	probeContext, cancelProbes := context.WithCancel(context.Background())
	defer cancelProbes()
	var deadline *time.Timer
	var deadlineChannel <-chan time.Time
	var probes <-chan bool
	if start.ServiceAddress != "" {
		// Constructed immediately after successful spawn: time.Timer carries a
		// monotonic component and does not depend on wall-clock adjustments.
		deadline = time.NewTimer(start.StartupReadinessDeadline)
		deadlineChannel = deadline.C
		probes = guardianServiceProbes(
			probeContext, start.ServiceAddress, start.ReadinessProbeInterval, start.ReadinessConnectTimeout,
		)
		defer deadline.Stop()
	}
	startupSatisfied := false
	ready := false
	writeReadiness := func(value bool) error {
		readyValue := value
		return encoder.Encode(guardianStatusMessage{
			Type: guardianMessageReadiness, StartupSatisfied: true, Ready: &readyValue,
		})
	}
	writeExit := func(outcome waitResult, cause contract.TerminationCause) error {
		result := resultFromWait(outcome.err, outcome.state, cause)
		return encoder.Encode(guardianStatusMessage{Type: guardianMessageExited, Result: &result})
	}
	writeSpontaneousExit := func(outcome waitResult) error {
		guardianCleanupRemainingGroup(processGroupID, start.TerminationGrace)
		return writeExit(outcome, contract.TerminationCauseSpontaneous)
	}

	for {
		select {
		case outcome := <-wait:
			return writeSpontaneousExit(outcome)
		case next := <-control:
			disconnected := next.err != nil || next.message.Type != guardianMessageStop
			outcome := guardianTerminateAndWait(processGroupID, start.TerminationGrace, wait)
			if disconnected {
				_ = writeExit(outcome, contract.TerminationCauseGuardian)
				return nil
			}
			return writeExit(outcome, contract.TerminationCauseGuardian)
		case probeReady := <-probes:
			if !startupSatisfied {
				if !probeReady {
					continue
				}
				// If exit is already observable, it wins. Otherwise this first
				// successful local probe is the sole starting -> serving event.
				select {
				case outcome := <-wait:
					return writeSpontaneousExit(outcome)
				default:
				}
				startupSatisfied = true
				ready = true
				if deadline != nil {
					deadline.Stop()
					deadlineChannel = nil
				}
				if err := writeReadiness(true); err != nil {
					_ = guardianTerminateAndWait(processGroupID, start.TerminationGrace, wait)
					return nil
				}
				continue
			}
			if probeReady == ready {
				continue
			}
			ready = probeReady
			if err := writeReadiness(ready); err != nil {
				_ = guardianTerminateAndWait(processGroupID, start.TerminationGrace, wait)
				return nil
			}
		case <-deadlineChannel:
			// One guardian arbiter owns the process-exit/deadline race. An exit
			// already queued at the boundary wins; otherwise timeout owns the
			// termination and is the only reported reason.
			select {
			case outcome := <-wait:
				return writeSpontaneousExit(outcome)
			default:
			}
			_ = guardianTerminateAndWait(processGroupID, start.TerminationGrace, wait)
			result := spawnFailure(
				contract.SpawnFailureStartupReadinessTimeout,
				errors.New("runtime-local service endpoint did not accept connections before the startup deadline"),
			)
			return encoder.Encode(guardianStatusMessage{Type: guardianMessageExited, Result: &result})
		}
	}
}

func guardianServiceProbes(ctx context.Context, address string, interval, timeout time.Duration) <-chan bool {
	results := make(chan bool, 1)
	go func() {
		dialer := &net.Dialer{}
		for {
			probeContext, cancel := context.WithTimeout(ctx, timeout)
			connection, err := dialer.DialContext(probeContext, "tcp", address)
			cancel()
			if connection != nil {
				_ = connection.Close()
			}
			select {
			case results <- err == nil:
			case <-ctx.Done():
				return
			}
			timer := time.NewTimer(interval)
			select {
			case <-timer.C:
			case <-ctx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				return
			}
		}
	}()
	return results
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
