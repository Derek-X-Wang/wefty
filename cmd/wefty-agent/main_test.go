package main

import (
	"bytes"
	"context"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestConsoleOutputIsAttributedAndSerialized(t *testing.T) {
	assertConsoleOutputIsAttributedAndSerialized(t)
}

func assertConsoleOutputIsAttributedAndSerialized(t *testing.T) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	claimOne := l1.Claim{Job: l1.Job{JobID: "job-one"}, Lease: l1.AttemptLease{AttemptID: "attempt-one"}}
	claimTwo := l1.Claim{Job: l1.Job{JobID: "job-two"}, Lease: l1.AttemptLease{AttemptID: "attempt-two"}}
	sinkOne := newConsoleOutputSink(&stdout, &stderr, claimOne)
	sinkTwo := newConsoleOutputSink(&stdout, &stderr, claimTwo)
	done := make(chan error, 2)
	go func() {
		done <- sinkOne.WriteOutput(context.Background(), contract.LogEvent{Stream: contract.LogStdout, Bytes: []byte("alpha\n")})
	}()
	go func() {
		done <- sinkTwo.WriteOutput(context.Background(), contract.LogEvent{Stream: contract.LogStdout, Bytes: []byte("beta\n")})
	}()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	one := "[job=job-one attempt=attempt-one stream=stdout] alpha\n"
	two := "[job=job-two attempt=attempt-two stream=stdout] beta\n"
	if got := stdout.String(); got != one+two && got != two+one {
		t.Fatalf("concurrent console output = %q, want two intact attributed records", got)
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}
