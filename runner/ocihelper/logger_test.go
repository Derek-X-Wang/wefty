package ocihelper

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

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
	stdoutTarget, err := os.Create(filepath.Join(t.TempDir(), "stdout.frames"))
	if err != nil {
		t.Fatal(err)
	}
	defer stdoutTarget.Close()
	stderrTarget, err := os.Create(filepath.Join(t.TempDir(), "stderr.frames"))
	if err != nil {
		t.Fatal(err)
	}
	defer stderrTarget.Close()

	interrupt := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- copyLoggerStreams([]loggerStream{
			{"stdout", stdoutSource, stdoutTarget},
			{"stderr", stderrSource, stderrTarget},
		}, interrupt)
	}()
	close(interrupt)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("logger termination waited for the post-terminal seal timeout")
	}
	for name, target := range map[string]*os.File{"stdout": stdoutTarget, "stderr": stderrTarget} {
		if _, err := target.Seek(0, io.SeekStart); err != nil {
			t.Fatal(err)
		}
		kind, sequence, payload, err := readLogRecord(target)
		if err != nil || kind != logRecordIncomplete || sequence != 0 || len(payload) == 0 {
			t.Fatalf("%s terminal record kind=%v sequence=%d payload=%q err=%v", name, kind, sequence, payload, err)
		}
	}
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
