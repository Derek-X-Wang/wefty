package ocihelper

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestTerminalPublicationWaitsForExitedTaskRelease(t *testing.T) {
	ready := make(chan struct{})
	releaseEntered := make(chan struct{})
	allowRelease := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- publishTerminalAfterTaskRelease(time.Second, func(context.Context) error {
			close(releaseEntered)
			<-allowRelease
			return nil
		}, nil, func(sealReason string) {
			if sealReason != "" {
				t.Errorf("clean release recorded seal reason %q", sealReason)
			}
			close(ready)
		})
	}()
	<-releaseEntered
	select {
	case <-ready:
		t.Fatal("terminal became observable before the exited task released its logger pipes")
	case <-time.After(20 * time.Millisecond):
	}
	close(allowRelease)
	<-ready
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// errTaskStillRunning stands in for containerd's failed-precondition refusal to
// delete a task the shim still reports running.
var errTaskStillRunning = errors.New("task must be stopped before deletion: running: failed precondition")

// TestTerminalPublicationRetriesReleaseWhileTaskIsStillReportedRunning is the
// regression for issue #423. The exit status arrives on Wait before the
// runtime's task state leaves running, so the first deletion is refused. A
// single attempt published the terminal anyway, which started both stream seal
// deadlines while the shim's logger pipes were still open -- no pipe EOF, no
// seal, and an ordinary exit-0 payload carried incomplete log evidence.
func TestTerminalPublicationRetriesReleaseWhileTaskIsStillReportedRunning(t *testing.T) {
	var attempts atomic.Int64
	ready := make(chan struct{})
	var recorded string
	err := publishTerminalAfterTaskRelease(2*time.Second, func(context.Context) error {
		if attempts.Add(1) < 4 {
			return errTaskStillRunning
		}
		return nil
	}, func(err error) bool { return errors.Is(err, errTaskStillRunning) }, func(sealReason string) {
		recorded = sealReason
		if attempts.Load() < 4 {
			t.Errorf("terminal was published after %d release attempts, before the task stopped", attempts.Load())
		}
		close(ready)
	})
	if err != nil {
		t.Fatalf("release that eventually stopped returned %v", err)
	}
	<-ready
	if recorded != "" {
		t.Fatalf("released task recorded seal reason %q, want none", recorded)
	}
	if got := attempts.Load(); got != 4 {
		t.Fatalf("release attempts = %d, want 4", got)
	}
}

// TestTerminalPublicationRecordsTypedReasonWhenTaskNeverStops proves the bound
// still exists and that its expiry is named, not silently folded into a bare
// runtime precondition string.
func TestTerminalPublicationRecordsTypedReasonWhenTaskNeverStops(t *testing.T) {
	ready := make(chan struct{})
	var recorded string
	started := time.Now()
	err := publishTerminalAfterTaskRelease(120*time.Millisecond, func(context.Context) error {
		return errTaskStillRunning
	}, func(err error) bool { return errors.Is(err, errTaskStillRunning) }, func(sealReason string) {
		recorded = sealReason
		close(ready)
	})
	<-ready
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("release exceeded its budget by %v", elapsed)
	}
	var neverStopped *TaskNeverStoppedError
	if !errors.As(err, &neverStopped) {
		t.Fatalf("release error = %v, want a typed never-stopped failure", err)
	}
	if !errors.Is(err, errTaskStillRunning) {
		t.Fatalf("release error %v dropped the runtime refusal", err)
	}
	if recorded != TaskNeverStoppedSealReason {
		t.Fatalf("recorded seal reason = %q, want %q", recorded, TaskNeverStoppedSealReason)
	}
}
