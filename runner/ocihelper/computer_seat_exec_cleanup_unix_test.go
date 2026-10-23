//go:build linux || darwin

package ocihelper

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/pkg/cio"
)

type seatCommandHeldIO struct {
	cio.IO
	release  chan struct{}
	canceled chan struct{}
	once     sync.Once
}

func (output *seatCommandHeldIO) Wait() { <-output.release }
func (output *seatCommandHeldIO) Cancel() {
	output.once.Do(func() { close(output.canceled) })
}
func (output *seatCommandHeldIO) Close() error { return nil }

func TestSeatCommandOutputWaitIsBounded(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		output := &seatCommandHeldIO{release: make(chan struct{}), canceled: make(chan struct{})}
		defer close(output.release)
		done := make(chan error, 1)
		go func() { done <- waitSeatCommandIO(t.Context(), output) }()
		time.Sleep(6 * time.Second)
		synctest.Wait()
		select {
		case err := <-done:
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("held output was not reported: %v", err)
			}
		default:
			t.Error("held output retained the command past its five-second drain bound")
		}
		select {
		case <-output.canceled:
		default:
			t.Error("held command I/O was not canceled")
		}
	})
}

type seatCommandDeleteProcess struct {
	containerd.Process
	output  cio.IO
	release <-chan struct{}
	deleted bool
}

func (process *seatCommandDeleteProcess) IO() cio.IO { return process.output }
func (process *seatCommandDeleteProcess) Status(context.Context) (containerd.Status, error) {
	return containerd.Status{}, errors.New("fixture status unavailable")
}
func (process *seatCommandDeleteProcess) Delete(ctx context.Context, _ ...containerd.ProcessDeleteOpts) (*containerd.ExitStatus, error) {
	process.deleted = true
	if process.release != nil {
		<-process.release // Model containerd's IO.Wait, which ignores RPC context.
	}
	return containerd.NewExitStatus(0, time.Now(), nil), nil
}

func TestSeatCommandDeleteWaitIsBounded(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		defer close(release)
		output := &seatCommandHeldIO{canceled: make(chan struct{})}
		process := &seatCommandDeleteProcess{release: release, output: output}
		done := make(chan error, 1)
		go func() { done <- deleteSeatCommandProcess(t.Context(), process) }()
		time.Sleep(6 * time.Second)
		synctest.Wait()
		select {
		case err := <-done:
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("held Delete was not reported: %v", err)
			}
		default:
			t.Error("Delete retained the command past its five-second bound")
		}
		select {
		case <-output.canceled:
		default:
			t.Error("Delete did not cancel command I/O")
		}
	})
}

func TestSeatCommandNeverStartedDeletesWhenStatusUnavailable(t *testing.T) {
	process := &seatCommandDeleteProcess{}
	if err := cleanupSeatCommandStart(t.Context(), process, nil, false); err != nil {
		t.Fatal(err)
	}
	if !process.deleted {
		t.Fatal("never-started exec leaked after unavailable status and missing wait")
	}
}
