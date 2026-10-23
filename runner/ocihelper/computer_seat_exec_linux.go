//go:build linux

package ocihelper

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"time"

	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/pkg/cio"
	specs "github.com/opencontainers/runtime-spec/specs-go"
)

func (engine *ContainerdEngine) ExecComputerSeat(ctx context.Context, request ExecComputerSeatRequest, emit func(SeatCommandEvent) error) error {
	return engine.execComputerSeat(ctx, request, emit, confirmSeatCommandReady)
}
func (engine *ContainerdEngine) execComputerSeat(ctx context.Context, request ExecComputerSeatRequest, emit func(SeatCommandEvent) error, confirm func(computerSeatAttemptSnapshot, ExecComputerSeatRequest) error) (resultErr error) {
	if err := request.validate(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeoutCause(ctx, request.MaxRuntime, ErrSeatCommandRuntimeLimit)
	defer cancel()
	defer func() {
		if resultErr != nil && errors.Is(context.Cause(ctx), ErrSeatCommandRuntimeLimit) && errors.Is(resultErr, ctx.Err()) && !seatCommandCleanupUnconfirmed(resultErr) {
			resultErr = errors.Join(resultErr, ErrSeatCommandRuntimeLimit)
		}
	}()
	stream := newSeatCommandStream(ctx, emit)
	waitCtx, stopWait := context.WithCancel(context.WithoutCancel(ctx))
	defer stopWait()
	process, wait, err := engine.startSeatCommand(ctx, waitCtx, request, stream, cancel, confirm)
	if err != nil {
		return err
	}
	// Delete only after positive wrapper exit evidence, including escalation.
	terminal := false
	defer func() {
		if terminal {
			err := deleteSeatCommandProcess(ctx, process)
			if err != nil {
				resultErr = &seatCommandCleanupError{errors.Join(resultErr, err)}
			}
		}
	}()
	var result containerd.ExitStatus
	select {
	case status, ok := <-wait:
		if !ok {
			return errors.New("Seat command exit evidence missing")
		}
		result = status
		terminal = result.Error() == nil
	case <-ctx.Done():
		var stopErr error
		result, terminal, stopErr = stopSeatCommandProcess(ctx, process, wait)
		if stopErr != nil {
			return &seatCommandCleanupError{errors.Join(ctx.Err(), stopErr)}
		}
	}
	if err := result.Error(); err != nil {
		// A failed wait is not an exit. Recover its observation with a short
		// registration bound, then stop/reap this exec before deleting it.
		registerCtx, stopRegister := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		recovered, registerErr := registerSeatCommandWait(registerCtx, waitCtx, process)
		stopRegister()
		if registerErr != nil {
			cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
			defer stop()
			signalErr := process.Kill(cleanup, syscall.SIGTERM)
			return &seatCommandCleanupError{errors.Join(err, registerErr, signalErr)}
		}
		_, confirmed, stopErr := stopSeatCommandProcess(ctx, process, recovered)
		terminal = confirmed
		return &seatCommandCleanupError{errors.Join(err, stopErr)}
	}
	if err := waitSeatCommandIO(ctx, process.IO()); err != nil {
		return &seatCommandCleanupError{err}
	}
	if err := stream.finish(); err != nil {
		return err
	}
	if result.ExitCode() != 0 {
		return fmt.Errorf("Seat command wrapper exited %d", result.ExitCode())
	}
	return nil
}

// Only preparation and start hold seatsMu, so removal can end a running
// command. No containerd call holds the attempt's lifecycle/control mutex.
func (engine *ContainerdEngine) startSeatCommand(ctx, waitCtx context.Context, request ExecComputerSeatRequest, stream *seatCommandStream, cancel context.CancelFunc, confirm func(computerSeatAttemptSnapshot, ExecComputerSeatRequest) error) (resultProcess containerd.Process, resultWait <-chan containerd.ExitStatus, resultErr error) {
	attempt, err := engine.attempt(request.Authority)
	if err != nil {
		return nil, nil, err
	}
	operationCtx := ctx
	ctx, stopStart := context.WithTimeoutCause(ctx, seatCommandStartTimeout, errSeatCommandStartTimeout)
	defer stopStart()
	defer func() {
		if resultErr != nil && errors.Is(context.Cause(ctx), errSeatCommandStartTimeout) {
			resultErr = errors.Join(resultErr, errSeatCommandStartTimeout)
		}
	}()
	attempt.seatsMu.Lock()
	locked := true
	unlock := func() {
		if locked {
			attempt.seatsMu.Unlock()
			locked = false
		}
	}
	defer unlock()
	snapshot, err := snapshotComputerSeatAttempt(ctx, attempt)
	if err != nil {
		return nil, nil, err
	}
	defer snapshot.namespace.close()
	config, err := snapshot.seats.command(request)
	if err != nil {
		return nil, nil, err
	}
	if err := confirm(snapshot, request); err != nil {
		return nil, nil, err
	}
	if err := engine.validateComputerSeatAttempt(ctx, request.Authority, attempt, snapshot); err != nil {
		return nil, nil, err
	}
	id, err := randomCapability()
	if err != nil {
		return nil, nil, err
	}
	// All fields are fresh. In particular, no image/root supplementary groups,
	// capabilities, environment or working directory can enter this process.
	spec := &specs.Process{
		User: specs.User{UID: config.UID, GID: config.GID, AdditionalGids: []uint32{}},
		Cwd:  config.Directory, Env: config.Environment,
		Args:            append([]string{"/usr/bin/python3", "-I", "-S", "-c", seatCommandWrapper}, request.Args...),
		NoNewPrivileges: true, Capabilities: &specs.LinuxCapabilities{},
		Rlimits: []specs.POSIXRlimit{{Type: "RLIMIT_NOFILE", Hard: 1024, Soft: 1024}},
	}
	// Do not publish command output before the post-start authority check.
	gate := make(chan struct{})
	authorized := false
	gated := newSeatCommandStream(operationCtx, func(e SeatCommandEvent) error {
		select {
		case <-gate:
		case <-operationCtx.Done():
			return operationCtx.Err()
		}
		if !authorized {
			return errors.New("Seat command lost start authority")
		}
		data, err := marshalSeatCommandEvent(e)
		if err != nil {
			return err
		}
		_, err = stream.Write(data)
		return err
	})
	process, err := snapshot.task.Exec(ctx, seatCommandExecID(id), spec, cio.NewCreator(cio.WithStreams(nil, seatCommandDrain{gated, cancel}, io.Discard)))
	if err != nil {
		close(gate)
		return nil, nil, err
	}
	ok := false
	startAttempted := false
	var wait <-chan containerd.ExitStatus
	defer func() {
		if !ok {
			unlock()
			cancel()
			close(gate)
			if cleanupErr := cleanupSeatCommandStart(ctx, process, wait, startAttempted); cleanupErr != nil {
				resultErr = &seatCommandCleanupError{errors.Join(resultErr, cleanupErr)}
			}
		}
	}()
	wait, err = registerSeatCommandWait(ctx, waitCtx, process)
	if err != nil {
		return nil, nil, err
	}
	if err := engine.validateComputerSeatAttempt(ctx, request.Authority, attempt, snapshot); err != nil {
		return nil, nil, err
	}
	startAttempted = true
	if err := process.Start(ctx); err != nil {
		return nil, nil, err
	}
	if err := engine.validateComputerSeatAttempt(ctx, request.Authority, attempt, snapshot); err != nil {
		return nil, nil, err
	}
	authorized, ok = true, true
	close(gate)
	return process, wait, nil
}

// A previous ready observation is necessary, but a stale or replaced image
// answer cannot authorize a new command. Reuse the strict status reader.
func confirmSeatCommandReady(snapshot computerSeatAttemptSnapshot, request ExecComputerSeatRequest) error {
	process, err := os.Open(fmt.Sprintf("/proc/%d", snapshot.task.Pid()))
	if err != nil {
		return err
	}
	defer process.Close()
	proc := fmt.Sprintf("/proc/self/fd/%d", process.Fd())
	if err := seatProcessMatchesNetwork(proc, snapshot.namespace); err != nil {
		return err
	}
	mountinfo, err := os.ReadFile(filepath.Join(proc, "mountinfo"))
	if err != nil {
		return err
	}
	directory, err := OpenValidatedSeatStatusDirectory(filepath.Join(proc, "root"), mountinfo)
	if err != nil {
		return err
	}
	defer directory.Close()
	payload, err := ReadSeatStatusAnswer(directory, fmt.Sprintf("%d.json", request.Place))
	var answer computerSeatAnswer
	if err != nil || decodeSeatDocument(payload, &answer) != nil || answer.Version != 1 || answer.Generation != request.Generation || answer.Place != request.Place || answer.State != "ready" || answer.Reason != "" || answer.CleanupComplete {
		return &SeatOperationRefusalError{SeatRefusalCommandNotReady}
	}
	return nil
}
