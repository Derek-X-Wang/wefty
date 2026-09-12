//go:build linux

package ocihelper

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/errdefs"
)

func TestContainerdTerminalPublicationReleasesTaskSealsLogsAndRetainsOOM(t *testing.T) {
	root := t.TempDir()
	cgroupID := "attempt-cgroup"
	cgroupPath := filepath.Join(root, cgroupID)
	if err := os.MkdirAll(cgroupPath, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cgroupPath, "memory.events"), []byte("oom 1\noom_kill 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	stdout := filepath.Join(root, "stdout.log")
	stderr := filepath.Join(root, "stderr.log")
	for _, path := range []string{stdout, stderr} {
		if err := os.WriteFile(path, nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}

	authority := testAuthority()
	ready := make(chan struct{})
	taskDeleteEntered := make(chan struct{})
	allowTaskDelete := make(chan struct{})
	loggerDone := make(chan struct{}, 2)
	var orderMu sync.Mutex
	var order []string
	record := func(event string) {
		orderMu.Lock()
		order = append(order, event)
		orderMu.Unlock()
	}
	appendSeal := func(stream, path, payload string) {
		<-taskDeleteEntered
		record("logger EOF " + stream)
		file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
		if err == nil {
			err = writeLogRecord(file, logFrameMagic, 0, []byte(payload))
		}
		if err == nil {
			err = writeLogRecord(file, logSealMagic, 1, nil)
		}
		if file != nil {
			if closeErr := file.Close(); err == nil {
				err = closeErr
			}
		}
		if err != nil {
			t.Errorf("seal %s log: %v", stream, err)
		}
		record(stream + " seal")
		loggerDone <- struct{}{}
	}
	go appendSeal("stdout", stdout, "stdout complete")
	go appendSeal("stderr", stderr, "stderr complete")

	attempt := &containerdAttempt{
		authority:       authority,
		resources:       ResourceIdentity{CgroupID: cgroupID},
		stdout:          stdout,
		stderr:          stderr,
		terminalReady:   ready,
		logAcknowledged: make(map[string]uint64),
		cancel: func() {
			record("cancel Wait context")
		},
		releaseTask: func(context.Context) error {
			record("Task.Delete")
			close(taskDeleteEntered)
			<-allowTaskDelete
			if err := os.RemoveAll(cgroupPath); err != nil {
				return err
			}
			<-loggerDone
			<-loggerDone
			return nil
		},
	}
	engine := &ContainerdEngine{
		config:   NativeEngineConfig{CgroupRoot: root, LogSealTimeout: time.Second},
		attempts: map[string]*containerdAttempt{authority.key(): attempt},
	}
	wait := make(chan containerd.ExitStatus, 1)
	var events []WatchEvent
	watchDone := make(chan error, 1)
	go func() {
		watchDone <- engine.Watch(t.Context(), WatchRequest{Authority: authority}, func(event WatchEvent) error {
			events = append(events, event)
			if event.Result != nil {
				record("terminal completion")
			}
			return nil
		})
	}()
	record("Wait")
	wait <- *containerd.NewExitStatus(137, time.Now(), nil)
	close(wait)
	go attempt.cacheTerminal(wait, root, time.Second)
	<-taskDeleteEntered
	select {
	case <-ready:
		t.Fatal("terminal was published before Task.Delete and logger seals completed")
	case err := <-watchDone:
		t.Fatalf("Watch completed before Task.Delete was released: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(allowTaskDelete)
	if err := <-watchDone; err != nil {
		t.Fatal(err)
	}

	var result *WatchResponse
	seals := map[string]bool{}
	logs := map[string]string{}
	for _, event := range events {
		if event.Log != nil && event.Log.Gap == nil {
			logs[event.Log.Stream] += string(event.Log.Bytes)
		}
		if event.Seal != nil {
			seals[event.Seal.Stream] = event.Seal.Complete
		}
		if event.Result != nil {
			result = event.Result
		}
	}
	if result == nil || result.ExitCode == nil || *result.ExitCode != 137 || result.Signal != "" || !result.OutOfMemory || result.LogEvidenceIncomplete {
		t.Fatalf("terminal result = %+v", result)
	}
	if !seals["stdout"] || !seals["stderr"] || logs["stdout"] != "stdout complete" || logs["stderr"] != "stderr complete" {
		t.Fatalf("log evidence = seals=%v logs=%v", seals, logs)
	}
	orderMu.Lock()
	observedOrder := append([]string(nil), order...)
	orderMu.Unlock()
	for _, event := range []string{"Wait", "cancel Wait context", "Task.Delete", "logger EOF stdout", "stdout seal", "logger EOF stderr", "stderr seal", "terminal completion"} {
		if !slices.Contains(observedOrder, event) {
			t.Fatalf("terminal ordering omitted %q: %v", event, observedOrder)
		}
	}
	index := func(event string) int { return slices.Index(observedOrder, event) }
	if index("Wait") > index("cancel Wait context") || index("cancel Wait context") > index("Task.Delete") || index("Task.Delete") > index("stdout seal") || index("Task.Delete") > index("stderr seal") || index("stdout seal") > index("terminal completion") || index("stderr seal") > index("terminal completion") {
		t.Fatalf("terminal ordering = %v", observedOrder)
	}
}

// sealedLogSegments writes one framed record plus a pipe-EOF seal to each
// stream, which is what the binary-v2 logger does once the shim closes its
// write end -- that is, once the exited task is actually deleted.
func sealedLogSegments(t *testing.T, paths map[string]string) {
	t.Helper()
	for stream, path := range paths {
		file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
		if err != nil {
			t.Fatalf("open %s segment: %v", stream, err)
		}
		if err := writeLogRecord(file, logFrameMagic, 0, []byte(stream+" complete")); err != nil {
			t.Fatalf("write %s frame: %v", stream, err)
		}
		if err := writeLogRecord(file, logSealMagic, 1, nil); err != nil {
			t.Fatalf("seal %s: %v", stream, err)
		}
		if err := file.Close(); err != nil {
			t.Fatalf("close %s segment: %v", stream, err)
		}
	}
}

func emptyLogSegments(t *testing.T, root string) map[string]string {
	t.Helper()
	paths := map[string]string{"stdout": filepath.Join(root, "stdout.log"), "stderr": filepath.Join(root, "stderr.log")}
	for _, path := range paths {
		if err := os.WriteFile(path, nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return paths
}

func watchTerminalEvidence(t *testing.T, engine *ContainerdEngine, authority AttemptAuthority) (*WatchResponse, map[string]LogSeal) {
	t.Helper()
	var events []WatchEvent
	if err := engine.Watch(t.Context(), WatchRequest{Authority: authority}, func(event WatchEvent) error {
		events = append(events, event)
		return nil
	}); err != nil {
		t.Fatalf("watch: %v", err)
	}
	seals := map[string]LogSeal{}
	var result *WatchResponse
	for _, event := range events {
		if event.Seal != nil {
			seals[event.Seal.Stream] = *event.Seal
		}
		if event.Result != nil {
			result = event.Result
		}
	}
	return result, seals
}

// TestContainerdSealsLogsWhenExitedTaskIsBrieflyStillReportedRunning is the
// engine-level regression for issue #423. containerd delivers the exit status
// on Wait before the shim's task state leaves running, so the first Task.Delete
// is refused with a failed precondition. Publishing the terminal on that
// refusal left both logger pipes open, so neither stream ever reached pipe EOF
// and a clean exit-0 payload reported log_evidence_incomplete.
func TestContainerdSealsLogsWhenExitedTaskIsBrieflyStillReportedRunning(t *testing.T) {
	root := t.TempDir()
	paths := emptyLogSegments(t, root)
	authority := testAuthority()
	var releases atomic.Int64
	attempt := &containerdAttempt{
		authority:       authority,
		stdout:          paths["stdout"],
		stderr:          paths["stderr"],
		terminalReady:   make(chan struct{}),
		logAcknowledged: make(map[string]uint64),
		releaseTask: func(context.Context) error {
			if releases.Add(1) < 3 {
				return fmt.Errorf("task must be stopped before deletion: running: %w", errdefs.ErrFailedPrecondition)
			}
			sealedLogSegments(t, paths)
			return nil
		},
	}
	engine := &ContainerdEngine{
		config:   NativeEngineConfig{CgroupRoot: root, LogSealTimeout: 2 * time.Second},
		attempts: map[string]*containerdAttempt{authority.key(): attempt},
	}
	wait := make(chan containerd.ExitStatus, 1)
	wait <- *containerd.NewExitStatus(0, time.Now(), nil)
	close(wait)
	go attempt.cacheTerminal(wait, root, 2*time.Second)

	result, seals := watchTerminalEvidence(t, engine, authority)
	if result == nil || result.ExitCode == nil || *result.ExitCode != 0 {
		t.Fatalf("terminal result = %+v, want exit 0", result)
	}
	if result.LogEvidenceIncomplete {
		t.Fatalf("clean exit 0 reported incomplete log evidence: seals=%+v", seals)
	}
	for _, stream := range []string{"stdout", "stderr"} {
		if !seals[stream].Complete {
			t.Fatalf("%s seal = %+v, want complete", stream, seals[stream])
		}
	}
	if got := releases.Load(); got < 2 {
		t.Fatalf("release attempts = %d, want the refusal to be retried", got)
	}
}

// TestContainerdNamesTheCauseWhenExitedTaskNeverStops keeps the bound honest:
// when the task truly never stops the evidence still says so, but it says it
// with a typed reason an agent can separate from a real log gap.
func TestContainerdNamesTheCauseWhenExitedTaskNeverStops(t *testing.T) {
	root := t.TempDir()
	paths := emptyLogSegments(t, root)
	authority := testAuthority()
	attempt := &containerdAttempt{
		authority:       authority,
		stdout:          paths["stdout"],
		stderr:          paths["stderr"],
		terminalReady:   make(chan struct{}),
		logAcknowledged: make(map[string]uint64),
		releaseTask: func(context.Context) error {
			return fmt.Errorf("task must be stopped before deletion: running: %w", errdefs.ErrFailedPrecondition)
		},
	}
	engine := &ContainerdEngine{
		config:   NativeEngineConfig{CgroupRoot: root, LogSealTimeout: 50 * time.Millisecond},
		attempts: map[string]*containerdAttempt{authority.key(): attempt},
	}
	wait := make(chan containerd.ExitStatus, 1)
	wait <- *containerd.NewExitStatus(0, time.Now(), nil)
	close(wait)
	go attempt.cacheTerminal(wait, root, 50*time.Millisecond)

	result, seals := watchTerminalEvidence(t, engine, authority)
	if result == nil || !result.LogEvidenceIncomplete {
		t.Fatalf("unreleased task produced result %+v, want incomplete log evidence", result)
	}
	for _, stream := range []string{"stdout", "stderr"} {
		seal := seals[stream]
		if seal.Complete || !strings.HasPrefix(seal.Reason, TaskNeverStoppedSealReason+": ") {
			t.Fatalf("%s seal = %+v, want an incomplete seal named %q", stream, seal, TaskNeverStoppedSealReason)
		}
	}
}
