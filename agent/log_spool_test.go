package agent

import (
	"context"
	"encoding/json"
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

func TestLogSpoolPersistsFinalizedCompletionAcrossRestart(t *testing.T) {
	directory := t.TempDir()
	claim := spoolTestClaim("attempt-completion")
	spool := openTestLogSpool(t, directory, "node-completion", 1024)
	if err := spool.ensureAttempt(context.Background(), claim); err != nil {
		t.Fatal(err)
	}
	exitCode := 0
	result := l1.ProcessResult{ExitCode: &exitCode}
	finishedAt := time.Date(2026, 8, 17, 3, 4, 5, 6, time.UTC)
	if err := spool.storeCompletion(context.Background(), claim.Lease.AttemptID, result, finishedAt); err != nil {
		t.Fatal(err)
	}
	if err := spool.Close(); err != nil {
		t.Fatal(err)
	}

	spool = openTestLogSpool(t, directory, "node-completion", 1024)
	defer spool.Close()
	stored, storedFinishedAt, present, err := spool.completion(context.Background(), claim.Lease.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	if !present || stored.ExitCode == nil || *stored.ExitCode != 0 || !storedFinishedAt.Equal(finishedAt) {
		t.Fatalf("durable completion = (%#v, %s, %t)", stored, storedFinishedAt, present)
	}
	attempts, err := spool.pendingAttempts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(attempts) != 1 || attempts[0].attemptID != claim.Lease.AttemptID {
		t.Fatalf("completion recovery attempts = %#v", attempts)
	}
	if err := spool.completionDelivered(context.Background(), claim.Lease.AttemptID); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := spool.db.QueryRow("SELECT COUNT(*) FROM spool_attempts WHERE attempt_id=?", claim.Lease.AttemptID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("delivered completion retained %d attempt rows", count)
	}
}

func TestLogSpoolReplacesRejectedRawReplayWithTruthfulGap(t *testing.T) {
	spool := openTestLogSpool(t, t.TempDir(), "node-rejected", 1024)
	defer spool.Close()
	claim := spoolTestClaim("attempt-rejected")
	if err := spool.ensureAttempt(context.Background(), claim); err != nil {
		t.Fatal(err)
	}
	for sequence, payload := range []string{"one", "two"} {
		if err := spool.append(context.Background(), spoolTestEvent(claim.Lease.AttemptID, contract.LogStdout, uint64(sequence), payload)); err != nil {
			t.Fatal(err)
		}
	}
	batch, err := spool.pendingBatch(context.Background(), claim.Lease.AttemptID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if err := spool.replaceBatchWithReplayGaps(context.Background(), claim.Lease.AttemptID, batch); err != nil {
		t.Fatal(err)
	}
	events, err := spool.pending(context.Background(), claim.Lease.AttemptID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Gap == nil {
		t.Fatalf("replacement events = %#v", events)
	}
	gap := events[0].Gap
	if gap.Reason != contract.LogGapReplayRejected || gap.ThroughSequence != 1 || gap.LostEventCount != 2 || gap.LostByteCount != 6 || len(events[0].Bytes) != 0 {
		t.Fatalf("replacement gap = %#v", events[0])
	}
}

func TestLogSpoolIncompleteTombstoneReleasesRawEvidence(t *testing.T) {
	assertLogSpoolIncompleteTombstoneReleasesRawEvidence(t)
}

func assertLogSpoolIncompleteTombstoneReleasesRawEvidence(t *testing.T) {
	t.Helper()
	spool := openTestLogSpool(t, t.TempDir(), "node-incomplete", 1024)
	defer spool.Close()
	claim := spoolTestClaim("attempt-incomplete")
	if err := spool.ensureAttempt(context.Background(), claim); err != nil {
		t.Fatal(err)
	}
	for sequence, payload := range []string{"one", "two"} {
		if err := spool.append(context.Background(), spoolTestEvent(claim.Lease.AttemptID, contract.LogStdout, uint64(sequence), payload)); err != nil {
			t.Fatal(err)
		}
	}
	batch, err := spool.pendingBatch(context.Background(), claim.Lease.AttemptID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if err := spool.replaceBatchWithReplayGaps(context.Background(), claim.Lease.AttemptID, batch); err != nil {
		t.Fatal(err)
	}
	exitCode := 0
	finishedAt := time.Date(2026, 8, 17, 4, 5, 6, 0, time.UTC)
	if err := spool.storeCompletion(context.Background(), claim.Lease.AttemptID, l1.ProcessResult{ExitCode: &exitCode}, finishedAt); err != nil {
		t.Fatal(err)
	}
	if err := spool.sealIncomplete(context.Background(), claim.Lease.AttemptID, "attempt missing", contract.ErrorAttemptNotFound, finishedAt.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	var eventCount int
	var resultJSON, tombstoneJSON []byte
	if err := spool.db.QueryRow(`SELECT
  (SELECT COUNT(*) FROM spool_events WHERE attempt_id=spool_attempts.attempt_id),
  result_json, incomplete_json
FROM spool_attempts WHERE attempt_id=?`, claim.Lease.AttemptID).Scan(&eventCount, &resultJSON, &tombstoneJSON); err != nil {
		t.Fatal(err)
	}
	if eventCount != 0 || len(resultJSON) != 0 {
		t.Fatalf("sealed evidence retained raw payload: events=%d result=%q", eventCount, resultJSON)
	}
	var tombstone incompleteEvidenceTombstone
	if err := json.Unmarshal(tombstoneJSON, &tombstone); err != nil {
		t.Fatal(err)
	}
	if tombstone.Kind != "incomplete" || tombstone.ErrorCode != contract.ErrorAttemptNotFound || tombstone.LostEventCount != 2 || tombstone.LostByteCount != 6 || !tombstone.CompletionUndelivered || tombstone.FinishedAt == nil || !tombstone.FinishedAt.Equal(finishedAt) {
		t.Fatalf("incomplete tombstone = %#v", tombstone)
	}
	attempts, err := spool.pendingAttempts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(attempts) != 0 {
		t.Fatalf("sealed attempt remained replayable: %#v", attempts)
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
