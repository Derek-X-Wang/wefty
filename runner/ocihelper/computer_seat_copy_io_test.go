package ocihelper

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

func TestSeatCopyInputClosesRuntimeStdinAfterLastWrite(t *testing.T) {
	var delivered bytes.Buffer
	closed := 0
	input := &seatCommandInput{reader: &copyLastRead{data: []byte("archive and commit")}, closeStdin: func() error {
		closed++
		if delivered.String() != "archive and commit" {
			t.Errorf("stdin closed before final FIFO write: %q", delivered.String())
		}
		return nil
	}}
	if _, err := io.Copy(&delivered, input); err != nil {
		t.Fatal(err)
	}
	if closed != 1 {
		t.Fatalf("runtime stdin closes=%d, want 1 (FIFO EOF alone leaves the shim handle open)", closed)
	}
	if _, err := input.Read(make([]byte, 1)); err != io.EOF || closed != 1 {
		t.Fatalf("repeated EOF: %v, closes=%d", err, closed)
	}
}

func TestSeatCopyInputClosesRuntimeStdinOnFailedSource(t *testing.T) {
	failure := errors.New("source failed")
	closed := false
	input := &seatCommandInput{reader: &copyLastRead{err: failure}, closeStdin: func() error { closed = true; return nil }}
	if _, err := io.Copy(io.Discard, input); !errors.Is(err, failure) || !closed {
		t.Fatalf("failed input: %v, closed=%v", err, closed)
	}
}

type copyLastRead struct {
	data []byte
	err  error
}

func (r *copyLastRead) Read(p []byte) (int, error) {
	n := copy(p, r.data)
	r.data = r.data[n:]
	if len(r.data) != 0 {
		return n, nil
	}
	if r.err != nil {
		return n, r.err
	}
	return n, io.EOF
}
