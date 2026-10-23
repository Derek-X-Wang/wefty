//go:build linux

package ocihelper

import (
	"context"
	"errors"
	"os"
	"slices"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/pkg/cio"
	specs "github.com/opencontainers/runtime-spec/specs-go"
)

type seatExecTask struct {
	seatLockTask
	exec func(context.Context, string, *specs.Process, cio.Creator) (containerd.Process, error)
}

func (t *seatExecTask) Exec(ctx context.Context, id string, p *specs.Process, c cio.Creator) (containerd.Process, error) {
	return t.exec(ctx, id, p, c)
}

func TestSeatExecBuildsUnprivilegedProcessAndKeepsOwnerLocksFree(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		authority := testAuthority()
		authority.Class = "service"
		namespace, err := pinTaskNetworkNamespace(uint32(os.Getpid()))
		if err != nil {
			t.Fatal(err)
		}
		defer namespace.close()
		directory := t.TempDir()
		seats := newComputerSeats(nil, directory)
		seats.apply([]ComputerSeat{{1, "seat1", 7102, 7103}})
		seats.entries[1].ready = true
		seats.entries[1].readyGeneration = 1
		sentinel := errors.New("fixture exec refused")
		entered, release := make(chan struct{}), make(chan struct{})
		called := 0
		task := &seatExecTask{exec: func(ctx context.Context, id string, p *specs.Process, c cio.Creator) (containerd.Process, error) {
			called++
			if id == "" || p.User.UID != 2001 || p.User.GID != 2001 || len(p.User.AdditionalGids) != 0 || p.Cwd != "/wefty/service/seats/1" || !p.NoNewPrivileges || p.Capabilities == nil || len(p.Capabilities.Effective) != 0 || len(p.Capabilities.Bounding) != 0 || !slices.Contains(p.Env, "DISPLAY=:1") {
				t.Errorf("unsafe process spec: %+v", p)
			}
			if len(p.Args) < 7 || p.Args[0] != "/usr/bin/python3" || p.Args[len(p.Args)-1] != "true" {
				t.Errorf("argv: %v", p.Args)
			}
			close(entered)
			<-release
			return nil, sentinel
		}}
		attempt := &containerdAttempt{authority: authority, task: task, networkNamespace: namespace, controlDirectory: directory, seats: seats}
		engine := &ContainerdEngine{attempts: map[string]*containerdAttempt{authority.key(): attempt}}
		request := ExecComputerSeatRequest{authority, 1, 1, []string{"true"}, 1000000000}
		wrong := request
		wrong.Authority.FencingToken = "stale"
		if err := engine.ExecComputerSeat(t.Context(), wrong, nil); err == nil {
			t.Fatal("wrong authority accepted")
		}
		done := make(chan error, 1)
		go func() {
			done <- engine.execComputerSeat(t.Context(), request, nil, func(computerSeatAttemptSnapshot, ExecComputerSeatRequest) error { return nil })
		}()
		<-entered
		ownerDone := make(chan struct{})
		go func() {
			attempt.controlMu.Lock()
			attempt.mu.Lock()
			attempt.deleted = true
			attempt.mu.Unlock()
			attempt.controlMu.Unlock()
			close(ownerDone)
		}()
		synctest.Wait()
		select {
		case <-ownerDone:
		default:
			t.Error("exec held owner lifecycle/control lock")
		}
		close(release)
		if err := <-done; !errors.Is(err, sentinel) {
			t.Fatalf("exec failure: %v", err)
		}
		if called != 1 {
			t.Fatalf("wrong authority reached exec: %d", called)
		}
	})
}

type seatExecDeadlineProcess struct {
	containerd.Process
	exited  chan containerd.ExitStatus
	killed  []syscall.Signal
	deleted bool
}

func (p *seatExecDeadlineProcess) Start(context.Context) error { return nil }
func (p *seatExecDeadlineProcess) Wait(context.Context) (<-chan containerd.ExitStatus, error) {
	return p.exited, nil
}
func (p *seatExecDeadlineProcess) Kill(ctx context.Context, signal syscall.Signal, opts ...containerd.KillOpts) error {
	if len(opts) != 0 {
		return errors.New("command cancel requested task-wide kill")
	}
	p.killed = append(p.killed, signal)
	p.exited <- *containerd.NewExitStatus(0, time.Now(), nil)
	return nil
}
func (p *seatExecDeadlineProcess) IO() cio.IO { return nil }
func (p *seatExecDeadlineProcess) Delete(ctx context.Context, opts ...containerd.ProcessDeleteOpts) (*containerd.ExitStatus, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.deleted = true
	return containerd.NewExitStatus(0, time.Now(), nil), nil
}
func TestSeatExecRuntimeLimitCancelsOnlyExecWithInjectedClock(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		authority := testAuthority()
		authority.Class = "service"
		namespace, err := pinTaskNetworkNamespace(uint32(os.Getpid()))
		if err != nil {
			t.Fatal(err)
		}
		defer namespace.close()
		directory := t.TempDir()
		seats := newComputerSeats(nil, directory)
		seats.apply([]ComputerSeat{{1, "seat1", 7102, 7103}})
		seats.entries[1].ready = true
		seats.entries[1].readyGeneration = 1
		process := &seatExecDeadlineProcess{exited: make(chan containerd.ExitStatus, 1)}
		task := &seatExecTask{exec: func(context.Context, string, *specs.Process, cio.Creator) (containerd.Process, error) {
			return process, nil
		}}
		attempt := &containerdAttempt{authority: authority, task: task, networkNamespace: namespace, controlDirectory: directory, seats: seats}
		engine := &ContainerdEngine{attempts: map[string]*containerdAttempt{authority.key(): attempt}}
		err = engine.execComputerSeat(t.Context(), ExecComputerSeatRequest{authority, 1, 1, []string{"sleep", "7200"}, time.Hour}, nil, func(computerSeatAttemptSnapshot, ExecComputerSeatRequest) error { return nil })
		if !errors.Is(err, context.DeadlineExceeded) || !slices.Equal(process.killed, []syscall.Signal{syscall.SIGTERM}) || !process.deleted {
			t.Fatalf("deadline cleanup: err=%v kills=%v deleted=%v", err, process.killed, process.deleted)
		}
		if attempt.deleted || attempt.lost {
			t.Fatal("command deadline ended the Computer")
		}
	})
}
