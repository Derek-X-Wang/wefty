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
	ctx, cancel := context.WithTimeout(ctx, request.MaxRuntime)
	defer cancel()
	stream := newSeatCommandStream(ctx, emit)
	waitCtx, stopWait := context.WithCancel(context.WithoutCancel(ctx))
	defer stopWait()
	process, wait, err := engine.startSeatCommand(ctx, waitCtx, request, stream, cancel, confirm)
	if err != nil {
		return err
	}
	// Wait and cleanup retain the containerd namespace but outlive the caller.
	defer func() {
		cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer stop()
		_, err := process.Delete(cleanup)
		resultErr = errors.Join(resultErr, err)
	}()
	var result containerd.ExitStatus
	select {
	case result = <-wait:
	case <-ctx.Done():
		cleanup, cancelCleanup := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancelCleanup()
		if err := process.Kill(cleanup, syscall.SIGTERM); err != nil {
			// A terminal race is resolved by Wait, never by killing the Computer.
			status, statusErr := process.Status(cleanup)
			if statusErr != nil || status.Status != containerd.Stopped {
				return errors.Join(ctx.Err(), err)
			}
		}
		select {
		case result = <-wait:
		case <-cleanup.Done():
			// Only the wrapper is addressed. WithKillAll would kill the Computer.
			last, lastCancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
			defer lastCancel()
			_ = process.Kill(last, syscall.SIGKILL)
			return errors.Join(ctx.Err(), errors.New("Seat command wrapper did not stop"))
		}
	}
	if err := result.Error(); err != nil {
		cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer stop()
		_ = process.Kill(cleanup, syscall.SIGTERM)
		return err
	}
	if process.IO() != nil {
		process.IO().Wait()
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
	attempt.seatsMu.Lock()
	defer attempt.seatsMu.Unlock()
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
	gated := newSeatCommandStream(ctx, func(e SeatCommandEvent) error {
		select {
		case <-gate:
		case <-ctx.Done():
			return ctx.Err()
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
	process, err := snapshot.task.Exec(ctx, "seat-command-"+id, spec, cio.NewCreator(cio.WithStreams(nil, seatCommandDrain{gated, cancel}, io.Discard)))
	if err != nil {
		close(gate)
		return nil, nil, err
	}
	ok := false
	var wait <-chan containerd.ExitStatus
	defer func() {
		if !ok {
			cancel()
			close(gate)
			cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			defer stop()
			// A pre-start refusal has no command tree and cannot produce a
			// Wait result. Delete that created exec directly.
			status, statusErr := process.Status(cleanup)
			if statusErr == nil && status.Status == containerd.Created {
				_, deleteErr := process.Delete(cleanup)
				resultErr = errors.Join(resultErr, deleteErr)
				return
			}
			_ = process.Kill(cleanup, syscall.SIGTERM)
			// Let the wrapper handle TERM before deleting it. Immediate KILL
			// here could strand descendants after a lost post-start fence.
			if wait != nil {
				select {
				case status := <-wait:
					resultErr = errors.Join(resultErr, status.Error())
				case <-cleanup.Done():
					resultErr = errors.Join(resultErr, cleanup.Err())
					return
				}
			}
			_, deleteErr := process.Delete(cleanup)
			resultErr = errors.Join(resultErr, deleteErr)
		}
	}()
	wait, err = process.Wait(waitCtx)
	if err != nil {
		return nil, nil, err
	}
	if err := engine.validateComputerSeatAttempt(ctx, request.Authority, attempt, snapshot); err != nil {
		return nil, nil, err
	}
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
