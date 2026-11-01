package main

import (
	"context"
	"errors"
	"io"
	"os"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/testwait"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestConsoleOutputCancellationReleasesBlockedPipe(t *testing.T) {
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()
	defer write.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sink := newConsoleOutputSink(write, write, l1.Claim{Job: l1.Job{JobID: "job"}, Lease: l1.AttemptLease{AttemptID: "attempt"}})
	done := make(chan error, 1)
	go func() {
		done <- sink.WriteOutput(ctx, contract.LogEvent{Stream: contract.LogStdout, Bytes: make([]byte, 2<<20)})
	}()
	// Observing the first prefix byte proves the real pipe write has begun.
	ready := make(chan error, 1)
	go func() { _, err := io.ReadFull(read, make([]byte, 1)); ready <- err }()
	select {
	case err := <-ready:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(testwait.Bound):
		t.Fatal("console worker did not start pipe write")
	}
	select {
	case err := <-done:
		t.Fatalf("large unread pipe write unexpectedly ended: %v", err)
	default:
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("write err=%v", err)
		}
	case <-time.After(testwait.Bound):
		t.Error("cancelled console sink stayed blocked")
	}
	// Closing the owned reader releases the single console worker as well.
	_ = read.Close()
	barrier := make(chan error, 1)
	go func() {
		barrier <- newConsoleOutputSink(io.Discard, io.Discard, l1.Claim{}).WriteOutput(context.Background(), contract.LogEvent{Stream: contract.LogStdout, Bytes: []byte("barrier")})
	}()
	select {
	case err := <-barrier:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(testwait.Bound):
		t.Fatal("console worker did not release after owned pipe close")
	}
}
