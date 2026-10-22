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

	// Preserve trailing output on ordinary cancellation. A separate context
	// interrupts sink writes when Guardian output must be cut off.
	outputContext, cancelOutput := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelOutput()
	activity := newActivityTracker(runner.clock.Now())
	failure := newSinkFailure()
	command := exec.Command(runner.guardianExecutable, guardianArg)
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	// WaitDelay bounds inherited stdout/stderr after the Guardian dies.
	// Allow one complete TERM grace plus the final group-reap bound and
	// a scheduling margin. Cancel sink output one second before pipe closure.
	command.WaitDelay = runner.terminationGraceTime + runner.processReapTimeout + time.Second
	command.ExtraFiles = []*os.File{guardianEndpoint}
	command.Stdout = &eventWriter{
		ctx: outputContext, clock: runner.clock, attemptID: request.AttemptID,
		stream: contract.LogStdout, sink: sink, activity: activity, failure: failure,
	}
	command.Stderr = &eventWriter{
		ctx: outputContext, clock: runner.clock, attemptID: request.AttemptID,
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
	awaitGuardianWait := func() waitResult {
		timer := time.AfterFunc(command.WaitDelay-time.Second, cancelOutput)
		defer timer.Stop()
		return <-guardianWait
	}
	guardianFailure := func(err error) (contract.ProcessResult, error) {
		result := spawnFailure(contract.SpawnFailureProcessWait, err)
		// Guardian loss cannot establish whether all runtime output was captured.
		result.LogEvidenceIncomplete = true
		return result, errors.Join(err, ErrGuardianLost)
	}
	stopGuardian := func(cause error) {
		if request.StopInitiated != nil {
			request.StopInitiated(cause)
		}
		_ = json.NewEncoder(agentEndpoint).Encode(guardianControlMessage{Type: guardianMessageStop})
	}
	failGuardian := func(err error) (contract.ProcessResult, error) {
		if request.StopInitiated != nil {
			request.StopInitiated(err)
		}
		_ = agentEndpoint.Close()
		cancelOutput()
		select {
		case <-guardianWait:
		case <-time.After(runner.terminationGraceTime + runner.processReapTimeout + time.Second):
			_ = command.Process.Kill()
			_ = awaitGuardianWait()
		}
		// Guardian Wait alone cannot prove that its payload was reaped.
		return guardianFailure(err)
	}

	start := guardianControlMessage{Type: guardianMessageStart, Start: &guardianStart{
		Path: request.Execution.Executable.Path, Args: append([]string(nil), request.Execution.Argv...),
		Directory:                request.Execution.WorkingDirectory,
		Environment:              buildEnvironment(runner.baseEnvironment, request.Execution.Env, request.Execution.SensitiveEnv),
		TerminationGrace:         runner.terminationGraceTime,
		ReapTimeout:              runner.processReapTimeout,
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
		outcome := awaitGuardianWait()
		if outcome.err != nil {
			return guardianFailure(outcome.err)
		}
		return *started.Result, nil
	}
	if request.ProcessStarted != nil {
		if err := request.ProcessStarted(started.ProcessGroupID); err != nil {
			stopGuardian(err)
			return failGuardian(fmt.Errorf("persist guardian started group: %w", err))
		}
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
			return guardianFailure(fmt.Errorf("receive guardian result: %w", status.err))
		}
		if status.message.Type != guardianMessageExited {
			err := fmt.Errorf("guardian sent unexpected %q message", status.message.Type)
			return guardianFailure(err)
		}
		if guardianOutcome.err != nil && !errors.Is(guardianOutcome.err, exec.ErrWaitDelay) {
			// A failed Guardian exit still makes its report suspect. ErrWaitDelay
			// means a clean exit with incomplete output, not loss of payload proof.
			return guardianFailure(guardianOutcome.err)
		}
		result := *status.message.Result
		result.LogEvidenceIncomplete = result.LogEvidenceIncomplete || outputContext.Err() != nil || errors.Is(guardianOutcome.err, exec.ErrWaitDelay)
		// A later output error must not hide which limit stopped the payload.
		markReachedLimit(&result, supervisionErr)
		if sinkErr := failure.Err(); sinkErr != nil && !(outputContext.Err() != nil && errors.Is(sinkErr, context.Canceled)) {
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
			guardianOutcome := awaitGuardianWait()
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
				stopGuardian(supervisionErr)
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
			stopGuardian(supervisionErr)

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
				stopGuardian(supervisionErr)
			}

		case <-maxRuntimeChannel:
			if !stopping {
				supervisionErr = ErrMaxRuntime
				stopping = true
				stopGuardian(supervisionErr)
			}

		case <-failure.Changed():
			if !stopping {
				supervisionErr = fmt.Errorf("output sink: %w", failure.Err())
				stopping = true
				stopGuardian(supervisionErr)
			}

		case <-ctx.Done():
			if !stopping {
				supervisionErr = ctx.Err()
				stopping = true
				stopGuardian(supervisionErr)
			}
		}
	}
}

func serveGuardian(endpoint *os.File, stdout, stderr io.Writer) error {
	// ExtraFiles clears close-on-exec when passing this endpoint to the
	// Guardian. Restore it before spawning a payload so only the Guardian
	// can write status reports or keep the agent's control stream open.
	syscall.CloseOnExec(int(endpoint.Fd()))
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
	if start.ReapTimeout == 0 {
		start.ReapTimeout = DefaultProcessReapTimeout
	}
	if start.Path == "" || len(start.Args) == 0 || start.Args[0] == "" || start.Directory == "" || start.TerminationGrace <= 0 || start.ReapTimeout <= 0 {
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
		_, _, _ = guardianTerminateAndWaitBounded(processGroupID, start.TerminationGrace, start.ReapTimeout, wait)
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
		completed := make(chan waitResult, 1)
		completed <- outcome
		if _, _, err := guardianTerminateAndWaitBounded(processGroupID, start.TerminationGrace, start.ReapTimeout, completed); err != nil {
			return err
		}
		return writeExit(outcome, contract.TerminationCauseSpontaneous)
	}

	for {
		select {
		case outcome := <-wait:
			return writeSpontaneousExit(outcome)
		case next := <-control:
			disconnected := next.err != nil || next.message.Type != guardianMessageStop
			outcome, delivered, reapErr := guardianTerminateAndWaitBounded(processGroupID, start.TerminationGrace, start.ReapTimeout, wait)
			if reapErr != nil {
				return reapErr
			}
			cause := terminationCause(delivered, contract.TerminationCauseGuardian)
			if disconnected {
				_ = writeExit(outcome, cause)
				return nil
			}
			return writeExit(outcome, cause)
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
					_, _, _ = guardianTerminateAndWaitBounded(processGroupID, start.TerminationGrace, start.ReapTimeout, wait)
					return nil
				}
				continue
			}
			if probeReady == ready {
				continue
			}
			ready = probeReady
			if err := writeReadiness(ready); err != nil {
				_, _, _ = guardianTerminateAndWaitBounded(processGroupID, start.TerminationGrace, start.ReapTimeout, wait)
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
			if _, _, err := guardianTerminateAndWaitBounded(processGroupID, start.TerminationGrace, start.ReapTimeout, wait); err != nil {
				return err
			}
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

// guardianTerminateAndWait retains the internal test caller's default bound.
func guardianTerminateAndWait(processGroupID int, grace time.Duration, wait <-chan waitResult) (waitResult, bool) {
	outcome, delivered, err := guardianTerminateAndWaitBounded(processGroupID, grace, DefaultProcessReapTimeout, wait)
	if err != nil {
		outcome.err = errors.Join(outcome.err, err)
	}
	return outcome, delivered
}

// A terminal report requires BOTH a reaped leader and kernel ESRCH for the
// entire payload group. Query failures, including EPERM, never prove absence.
// After final SIGKILL, a real wall-clock timer bounds even a missing Wait.
func guardianTerminateAndWaitBounded(processGroupID int, grace, reapBound time.Duration, wait <-chan waitResult) (waitResult, bool, error) {
	return guardianTerminateAndWaitWithGroupOps(processGroupID, grace, reapBound, wait, deliverTermination, killProcessGroup, queryProcessGroup)
}

func guardianTerminateAndWaitWithGroupOps(processGroupID int, grace, reapBound time.Duration, wait <-chan waitResult, term func(int) bool, kill func(int) error, query func(int) error) (waitResult, bool, error) {
	if processGroupID <= 0 {
		return waitResult{}, false, errors.Join(ErrGuardianLost, errors.New("invalid payload group"))
	}
	var completed *waitResult
	select {
	case outcome := <-wait:
		completed = &outcome
	default:
	}
	delivered := term(processGroupID) && completed == nil
	graceTimer := time.NewTimer(grace)
	defer graceTimer.Stop()
	var reapTimer *time.Timer
	var reapDeadline <-chan time.Time
	var killErr error
	defer func() {
		if reapTimer != nil {
			reapTimer.Stop()
		}
	}()
	poll := time.NewTicker(10 * time.Millisecond)
	defer poll.Stop()
	for {
		groupErr := query(processGroupID)
		if isMissingProcessGroup(groupErr) && completed != nil {
			return *completed, delivered, nil
		}
		// A query error is never positive evidence. Retry within the same bound:
		// Wait may still be reaping the leader while the kernel reports EPERM for
		// its dying group. Only ESRCH plus Wait can complete this path.
		select {
		case outcome := <-wait:
			completed = &outcome
		case <-poll.C:
		case <-graceTimer.C:
			// A failed signal does not prove absence. Keep waiting within the
			// final bound: the payload may already be exiting or being reaped.
			killErr = kill(processGroupID)
			reapTimer = time.NewTimer(reapBound)
			reapDeadline = reapTimer.C
		case <-reapDeadline:
			if isMissingProcessGroup(groupErr) {
				groupErr = nil
			}
			return waitResult{}, delivered, errors.Join(fmt.Errorf("%w: payload group %d reap proof incomplete", ErrGuardianLost, processGroupID), groupErr, killErr)
		}
	}
}
