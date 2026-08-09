package agent

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestLogSpoolPersistsPendingEventsAndAcknowledgementHighWater(t *testing.T) {
	directory := t.TempDir()
	claim := spoolTestClaim("attempt-persist")
	spool := openTestLogSpool(t, directory, "node-persist", 1024)
	if err := spool.ensureAttempt(context.Background(), claim); err != nil {
		t.Fatal(err)
	}
	for _, event := range []contract.LogEvent{
		spoolTestEvent(claim.Lease.AttemptID, contract.LogStdout, 0, "out-0"),
		spoolTestEvent(claim.Lease.AttemptID, contract.LogStderr, 0, "err-0"),
		spoolTestEvent(claim.Lease.AttemptID, contract.LogStdout, 1, "out-1"),
	} {
		if err := spool.append(context.Background(), event); err != nil {
			t.Fatal(err)
		}
	}
	if err := spool.acknowledge(context.Background(), claim.Lease.AttemptID, map[contract.LogStream]uint64{contract.LogStdout: 0}); err != nil {
		t.Fatal(err)
	}
	if err := spool.Close(); err != nil {
		t.Fatal(err)
	}

	spool = openTestLogSpool(t, directory, "node-persist", 1024)
	defer spool.Close()
	highWater, present, err := spool.highWater(context.Background(), claim.Lease.AttemptID, contract.LogStdout)
	if err != nil {
		t.Fatal(err)
	}
	if !present || highWater != 0 {
		t.Fatalf("stdout acknowledgement = (%d, %t), want persisted sequence zero", highWater, present)
	}
	if _, present, err := spool.highWater(context.Background(), claim.Lease.AttemptID, contract.LogStderr); err != nil || present {
		t.Fatalf("stderr acknowledgement = (present=%t, err=%v), want absent", present, err)
	}
	pending, err := spool.pending(context.Background(), claim.Lease.AttemptID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 2 || pending[0].Stream != contract.LogStderr || pending[0].Sequence != 0 || pending[1].Stream != contract.LogStdout || pending[1].Sequence != 1 {
		t.Fatalf("pending after reopen = %#v", pending)
	}
}

func TestLogSpoolRetentionOverflowStopsWithoutEviction(t *testing.T) {
	spool := openTestLogSpool(t, t.TempDir(), "node-overflow", 4)
	defer spool.Close()
	claim := spoolTestClaim("attempt-overflow")
	if err := spool.ensureAttempt(context.Background(), claim); err != nil {
		t.Fatal(err)
	}
	first := spoolTestEvent(claim.Lease.AttemptID, contract.LogStdout, 0, "1234")
	if err := spool.append(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	err := spool.append(context.Background(), spoolTestEvent(claim.Lease.AttemptID, contract.LogStdout, 1, "5"))
	if !errors.Is(err, ErrLogSpoolFull) {
		t.Fatalf("overflow error = %v, want ErrLogSpoolFull", err)
	}
	pending, err := spool.pending(context.Background(), claim.Lease.AttemptID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || string(pending[0].Bytes) != "1234" {
		t.Fatalf("overflow changed retained events: %#v", pending)
	}
}

func TestLogSpoolKeepsRestartedAttemptsSeparate(t *testing.T) {
	spool := openTestLogSpool(t, t.TempDir(), "node-attempts", 1024)
	defer spool.Close()
	oldClaim := spoolTestClaim("attempt-old")
	newClaim := spoolTestClaim("attempt-new")
	for _, claim := range []l1.Claim{oldClaim, newClaim} {
		if err := spool.ensureAttempt(context.Background(), claim); err != nil {
			t.Fatal(err)
		}
	}
	if err := spool.append(context.Background(), spoolTestEvent(oldClaim.Lease.AttemptID, contract.LogStdout, 0, "old")); err != nil {
		t.Fatal(err)
	}
	if pending, err := spool.pending(context.Background(), newClaim.Lease.AttemptID, 10); err != nil || len(pending) != 0 {
		t.Fatalf("new attempt inherited old output: pending=%#v err=%v", pending, err)
	}
	attempts, err := spool.pendingAttempts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(attempts) != 1 || attempts[0].attemptID != oldClaim.Lease.AttemptID {
		t.Fatalf("recovery attempts = %#v, want only original attempt", attempts)
	}
}

func openTestLogSpool(t *testing.T, directory, nodeID string, maxBytes int64) *logSpool {
	t.Helper()
	spool, err := openLogSpool(directory, nodeID, maxBytes)
	if err != nil {
		t.Fatal(err)
	}
	return spool
}

func spoolTestClaim(attemptID string) l1.Claim {
	return l1.Claim{
		Job:   l1.Job{JobID: "job-" + attemptID},
		Lease: l1.AttemptLease{AttemptID: attemptID, FencingToken: "fence-" + attemptID},
	}
}

func spoolTestEvent(attemptID string, stream contract.LogStream, sequence uint64, payload string) contract.LogEvent {
	return contract.LogEvent{
		AttemptID: attemptID,
		Stream:    stream,
		Sequence:  sequence,
		Timestamp: time.Date(2026, 8, 9, 12, 0, int(sequence), 0, time.UTC),
		Bytes:     []byte(payload),
	}
}
