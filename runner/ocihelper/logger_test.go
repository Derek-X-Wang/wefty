package ocihelper

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

const loggerChildEnvironment = "WEFTY_LOGGER_EXTRA_FILES_CHILD"

func TestLoggerExtraFilesChild(t *testing.T) {
	if os.Getenv(loggerChildEnvironment) == "" {
		return
	}
	err := RunLoggerInvocation([]string{
		"wefty-agent", "mode", LoggerInvocationArg,
		"stdout", os.Getenv("WEFTY_LOGGER_STDOUT_SEGMENT"),
		"stderr", os.Getenv("WEFTY_LOGGER_STDERR_SEGMENT"),
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestLoggerFramesDataAndPipeEOFSeal(t *testing.T) {
	var segment bytes.Buffer
	if err := copyLogFrames(bytes.NewBufferString("hello"), &segment); err != nil {
		t.Fatal(err)
	}
	kind, sequence, payload, err := readLogRecord(&segment)
	if err != nil || kind != logRecordData || sequence != 0 || string(payload) != "hello" {
		t.Fatalf("data record kind=%v sequence=%d payload=%q err=%v", kind, sequence, payload, err)
	}
	kind, sequence, payload, err = readLogRecord(&segment)
	if err != nil || kind != logRecordSeal || sequence != 1 || len(payload) != 0 {
		t.Fatalf("seal record kind=%v sequence=%d payload=%q err=%v", kind, sequence, payload, err)
	}
	if _, _, _, err := readLogRecord(&segment); err != io.EOF {
		t.Fatalf("record tail error = %v, want EOF", err)
	}
}

func TestLoggerTerminationPublishesIncompleteSealsWithoutSealTimeout(t *testing.T) {
	stdoutSource, stdoutWriter, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer stdoutWriter.Close()
	stderrSource, stderrWriter, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer stderrWriter.Close()
	readyReader, readyWriter, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer readyReader.Close()
	directory := t.TempDir()
	segments := map[string]string{
		"stdout": filepath.Join(directory, "stdout.frames"),
		"stderr": filepath.Join(directory, "stderr.frames"),
	}
	command := exec.Command(os.Args[0], "-test.run=^TestLoggerExtraFilesChild$")
	command.Env = append(os.Environ(),
		loggerChildEnvironment+"=1",
		"WEFTY_LOGGER_STDOUT_SEGMENT="+segments["stdout"],
		"WEFTY_LOGGER_STDERR_SEGMENT="+segments["stderr"],
	)
	// This is the containerd launch shape: StartProcess materializes these
	// descriptors through ExtraFiles before the child recreates fds 3/4.
	command.ExtraFiles = []*os.File{stdoutSource, stderrSource, readyWriter}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	_ = stdoutSource.Close()
	_ = stderrSource.Close()
	_ = readyWriter.Close()
	if err := readyReader.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	ready := make([]byte, 1)
	if _, err := io.ReadFull(readyReader, ready); err != nil {
		_ = command.Process.Kill()
		t.Fatalf("logger did not acknowledge readiness after installing SIGTERM handling: %v", err)
	}
	started := time.Now()
	if err := command.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	waitForLoggerIncompleteSegments(t, segments, time.Second)
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		_ = command.Process.Kill()
		t.Fatal("logger termination waited for the post-terminal seal timeout")
	}
	if elapsed := time.Since(started); elapsed < loggerTerminationDrainTimeout || elapsed >= 2*time.Second {
		t.Fatalf("logger termination elapsed %s, want drain deadline honored and prompt exit", elapsed)
	}
}

func waitForLoggerIncompleteSegments(t *testing.T, segments map[string]string, timeout time.Duration) {
	t.Helper()
	confirmed := make(map[string]bool, len(segments))
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for name, path := range segments {
			if confirmed[name] {
				continue
			}
			target, err := os.Open(path)
			if err != nil {
				continue
			}
			kind, sequence, payload, readErr := readLogRecord(target)
			_ = target.Close()
			var evidence loggerIncompleteEvidence
			if readErr == nil && kind == logRecordIncomplete && sequence == 0 && len(payload) > 0 && json.Unmarshal(payload, &evidence) == nil && evidence.Reason != "" {
				confirmed[name] = true
			}
		}
		if len(confirmed) == len(segments) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("logger incomplete seals within %s = %v, want both streams", timeout, confirmed)
}

func TestLoggerRecordsIncompleteSourceWithoutLosingWrittenBytes(t *testing.T) {
	var segment bytes.Buffer
	source := io.MultiReader(bytes.NewBufferString("kept"), failingReader{})
	if err := copyLogFrames(source, &segment); err != nil {
		t.Fatal(err)
	}
	kind, _, payload, err := readLogRecord(&segment)
	if err != nil || kind != logRecordData || string(payload) != "kept" {
		t.Fatalf("data record kind=%v payload=%q err=%v", kind, payload, err)
	}
	kind, sequence, payload, err := readLogRecord(&segment)
	if err != nil || kind != logRecordIncomplete || sequence != 1 || len(payload) == 0 {
		t.Fatalf("incomplete record kind=%v sequence=%d payload=%q err=%v", kind, sequence, payload, err)
	}
}

func TestLoggerRejectsRecordLargerThanProtocolBound(t *testing.T) {
	var segment bytes.Buffer
	err := writeLogRecord(&segment, logFrameMagic, 0, make([]byte, MaxFrameBytes-logFrameHeaderBytes+1))
	if err == nil || segment.Len() != 0 {
		t.Fatalf("oversized record error=%v bytes=%d", err, segment.Len())
	}
}

type failingReader struct{}

func (failingReader) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }
