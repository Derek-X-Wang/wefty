//go:build linux

package ocihelper

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	containerd "github.com/containerd/containerd/v2/client"
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
	for _, event := range []string{"Wait", "Task.Delete", "logger EOF stdout", "stdout seal", "logger EOF stderr", "stderr seal", "terminal completion"} {
		if !slices.Contains(observedOrder, event) {
			t.Fatalf("terminal ordering omitted %q: %v", event, observedOrder)
		}
	}
	index := func(event string) int { return slices.Index(observedOrder, event) }
	if index("Wait") > index("Task.Delete") || index("Task.Delete") > index("stdout seal") || index("Task.Delete") > index("stderr seal") || index("stdout seal") > index("terminal completion") || index("stderr seal") > index("terminal completion") {
		t.Fatalf("terminal ordering = %v", observedOrder)
	}
}
