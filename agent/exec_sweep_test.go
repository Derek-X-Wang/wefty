//go:build darwin || linux

package agent

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/testwait"
)

func TestExecCaptureSweepRejectsPathSwapAfterProbe(t *testing.T) {
	mailbox, _, _ := newTestMailbox(t, captureSweepAppender(), `{}`)
	if err := mailbox.root.MkdirAll("wefty-exec-work/work", 0700); err != nil {
		t.Fatal(err)
	}
	outside := t.TempDir()
	outsideWork := filepath.Join(outside, "wefty-exec-work", "work")
	if err := os.MkdirAll(outsideWork, 0700); err != nil {
		t.Fatal(err)
	}
	// Default TERM handling makes any accidental sweep signal observable as
	// process exit, including a TERM that never reaches the KILL stage.
	child := exec.Command("/bin/sleep", "60")
	child.Dir = outsideWork
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { _ = child.Wait(); close(done) }()
	t.Cleanup(func() { _ = child.Process.Kill(); waitExecSweepProcess(t, done) })
	var diagnostics strings.Builder
	mailbox.captureStderr = func(_ context.Context, output []byte) error {
		_, _ = diagnostics.Write(output)
		return nil
	}
	probe := func(root *os.Root) (execSweepDirectory, error) {
		target, err := probeExecSweepDirectory(root)
		if err != nil {
			return target, err
		}
		// Swap only after the real Go identity check. Bash must reject the
		// outside directory reached through the now-replaced physical path.
		if err := os.Rename(mailbox.directory, mailbox.directory+"-moved"); err != nil {
			return target, err
		}
		return target, os.Symlink(outside, mailbox.directory)
	}
	stopped := mailbox.sweepExecCaptureRootWithProbe(t.Context(), mailbox.root, false, 5*time.Second, probe)
	if len(stopped) != 0 {
		t.Fatalf("path swap produced stopped evidence: %+v", stopped)
	}
	if !strings.Contains(diagnostics.String(), "wefty: warning: could not verify retained exec mailbox identity") {
		t.Fatalf("shell identity mismatch did not warn: %s", &diagnostics)
	}
	select {
	case <-done:
		t.Fatal("shell signaled a process outside the retained mailbox")
	default:
	}
}

func TestExecCaptureSweepBoundsBlockedProbeAndClosesLateHandle(t *testing.T) {
	for _, fail := range []bool{false, true} {
		name := "late_success"
		if fail {
			name = "late_error"
		}
		t.Run(name, func(t *testing.T) {
			mailbox, _, _ := newTestMailbox(t, captureSweepAppender(), `{}`)
			var diagnostics strings.Builder
			mailbox.captureStderr = func(_ context.Context, output []byte) error {
				_, _ = diagnostics.Write(output)
				return nil
			}
			entered := make(chan *os.File, 1)
			release := make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			defer unblock()
			probe := func(root *os.Root) (execSweepDirectory, error) {
				directory, err := root.Open(".")
				entered <- directory
				if err != nil {
					return execSweepDirectory{}, err
				}
				<-release
				if fail {
					err = errors.New("late probe failure")
				}
				return execSweepDirectory{directory: directory}, err
			}
			done := make(chan []contract.StoppedProcess, 1)
			go func() {
				done <- mailbox.sweepExecCaptureRootWithProbe(t.Context(), mailbox.root, false, 100*time.Millisecond, probe)
			}()
			var directory *os.File
			select {
			case directory = <-entered:
				if directory == nil {
					t.Fatal("probe could not open directory")
				}
			case <-time.After(testwait.Bound):
				t.Fatal("probe did not enter")
			}
			select {
			case stopped := <-done:
				if len(stopped) != 0 {
					t.Fatalf("blocked probe produced evidence: %+v", stopped)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("sweep did not return after its 100 ms probe deadline")
			}
			if !strings.Contains(diagnostics.String(), "wefty: warning: exec storage process sweep probes exceeded deadline") {
				t.Fatalf("blocked probe did not warn: %s", &diagnostics)
			}
			if _, err := directory.Stat(); err != nil {
				t.Fatalf("caller closed the handle still owned by blocked probe: %v", err)
			}
			unblock()
			deadline := time.Now().Add(testwait.Bound)
			for {
				if _, err := directory.Stat(); errors.Is(err, os.ErrClosed) {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("late probe result leaked its directory handle")
				}
				time.Sleep(time.Millisecond)
			}
		})
	}
}
