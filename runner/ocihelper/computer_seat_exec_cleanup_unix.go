//go:build linux || darwin

package ocihelper

import (
	"context"
	"errors"
	"fmt"
	"syscall"
	"time"

	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/pkg/cio"
)

// These bounds apply after wrapper exit and are independent of the caller.
const seatCommandOutputDrainTimeout = 5 * time.Second
const seatCommandDeleteTimeout = 5 * time.Second

func waitSeatCommandIO(ctx context.Context, output cio.IO) error {
	if output == nil {
		return nil
	}
	cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), seatCommandOutputDrainTimeout)
	defer stop()
	done := make(chan struct{})
	go func() { output.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-cleanup.Done():
		// Cancel closes the containerd FIFO readers even when a stopped discard
		// process retains a writer. Delete also has its own bound below.
		output.Cancel()
		return fmt.Errorf("Seat command output drain unconfirmed: %w", cleanup.Err())
	}
}

func deleteSeatCommandProcess(ctx context.Context, process containerd.Process) error {
	cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), seatCommandDeleteTimeout)
	defer stop()
	output := process.IO()
	if output != nil {
		output.Cancel()
	}
	done := make(chan error, 1)
	// containerd Delete also calls IO.Wait without consulting its RPC context.
	// Bound the entire call, rather than only its RPCs.
	go func() {
		_, err := process.Delete(cleanup)
		if err != nil && output != nil {
			err = errors.Join(err, output.Close())
		}
		done <- err
	}()
	select {
	case err := <-done:
		return err
	case <-cleanup.Done():
		if output != nil {
			_ = output.Close()
		}
		return fmt.Errorf("Seat command deletion unconfirmed: %w", cleanup.Err())
	}
}

func cleanupSeatCommandStart(ctx context.Context, process containerd.Process, wait <-chan containerd.ExitStatus, startAttempted bool) (cleanupErr error) {
	// A failed wait registration cannot have started this exec. Try Delete even
	// if Status is unavailable; containerd still enforces its deletion rules.
	if !startAttempted {
		return deleteSeatCommandProcess(ctx, process)
	}
	cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer stop()
	status, statusErr := process.Status(cleanup)
	terminal := statusErr == nil && (status.Status == containerd.Created || status.Status == containerd.Stopped)
	if !terminal && wait != nil {
		_, confirmed, stopErr := stopSeatCommandProcess(cleanup, process, wait)
		terminal = confirmed
		cleanupErr = errors.Join(cleanupErr, stopErr)
	}
	if terminal {
		return errors.Join(cleanupErr, deleteSeatCommandProcess(ctx, process))
	}
	return errors.Join(cleanupErr, statusErr, errors.New("Seat command failed-start cleanup unconfirmed"))
}

type seatCommandWaitRegistration struct {
	wait <-chan containerd.ExitStatus
	err  error
}

func registerSeatCommandWait(registrationCtx, lifetimeCtx context.Context, process containerd.Process) (<-chan containerd.ExitStatus, error) {
	registration := make(chan seatCommandWaitRegistration, 1)
	go func() { wait, err := process.Wait(lifetimeCtx); registration <- seatCommandWaitRegistration{wait, err} }()
	select {
	case result := <-registration:
		return result.wait, result.err
	case <-registrationCtx.Done():
		return nil, registrationCtx.Err()
	}
}

// The wrapper gives its process group five seconds after TERM. The helper
// gives the wrapper ten seconds, then forces only that exec process down and
// waits another two seconds. A stopped wrapper is required before Delete.
func stopSeatCommandProcess(ctx context.Context, process containerd.Process, wait <-chan containerd.ExitStatus) (containerd.ExitStatus, bool, error) {
	cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer stop()
	if err := process.Kill(cleanup, syscall.SIGTERM); err != nil {
		status, statusErr := process.Status(cleanup)
		if statusErr != nil || status.Status != containerd.Stopped {
			return containerd.ExitStatus{}, false, err
		}
	}
	select {
	case status, ok := <-wait:
		if !ok {
			return containerd.ExitStatus{}, false, errors.New("Seat command exit evidence missing")
		}
		return status, status.Error() == nil, status.Error()
	case <-cleanup.Done():
	}
	forced, stopForced := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer stopForced()
	if err := process.Kill(forced, syscall.SIGKILL); err != nil {
		status, statusErr := process.Status(forced)
		if statusErr != nil || status.Status != containerd.Stopped {
			return containerd.ExitStatus{}, false, err
		}
	}
	select {
	case status, ok := <-wait:
		if !ok {
			return containerd.ExitStatus{}, false, errors.New("Seat command forced-exit evidence missing")
		}
		return status, status.Error() == nil, errors.Join(status.Error(), errors.New("Seat command wrapper required KILL; process-tree cleanup unconfirmed"))
	case <-forced.Done():
		return containerd.ExitStatus{}, false, errors.New("Seat command wrapper did not exit after KILL")
	}
}
