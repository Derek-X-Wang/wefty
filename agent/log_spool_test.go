package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestComputerControlTokenKeyPersistsAcrossAgentRestart(t *testing.T) {
	directory := t.TempDir()
	spool := openTestLogSpool(t, directory, "node-control-token", 1024)
	keyBefore, err := spool.loadOrCreateSecret(t.Context(), computerControlTokenKeyName, computerControlTokenKeySize)
	if err != nil {
		t.Fatal(err)
	}
	if err := spool.Close(); err != nil {
		t.Fatal(err)
	}
	spool = openTestLogSpool(t, directory, "node-control-token", 1024)
	defer spool.Close()
	keyAfter, err := spool.loadOrCreateSecret(t.Context(), computerControlTokenKeyName, computerControlTokenKeySize)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(keyBefore, keyAfter) {
		t.Fatal("Computer control token key changed across durable spool reopen")
	}
	issuer, err := newComputerControlTokenCodec(keyBefore)
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := newComputerControlTokenCodec(keyAfter)
	if err != nil {
		t.Fatal(err)
	}
	identity := fabric.Identity{FabricID: "fabric-1", UserID: "person-1", DeviceID: "device-1"}
	token, err := issuer.issue(computerControlTokenClaims{ComputerID: "computer-1", StorageID: "storage-1", StorageGeneration: 1,
		AttemptID: "attempt-1", FabricID: identity.FabricID, UserID: identity.UserID, DeviceID: identity.DeviceID,
		CanTake: true, PolicyRevision: 1, Nonce: "nonce-1"})
	if err != nil {
		t.Fatal(err)
	}
	if claims, ok := verifier.authenticate(token, "computer-1", "storage-1", identity); !ok || claims.AttemptID != "attempt-1" ||
		claims.StorageGeneration != 1 {
		t.Fatalf("reopened token verifier = ok=%t claims=%+v", ok, claims)
	}
	if _, ok := verifier.authenticate(token, "computer-1", "different-storage", identity); ok {
		t.Fatal("Computer control token crossed its Storage lineage")
	}
}

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

func TestLogSpoolBudgetsAreClassScopedAndServiceRingEvictsAcrossAttempts(t *testing.T) {
	assertLogSpoolBudgetsAreClassScopedAndServiceRingEvictsAcrossAttempts(t)
}

func assertLogSpoolBudgetsAreClassScopedAndServiceRingEvictsAcrossAttempts(t *testing.T) {
	t.Helper()
	directory := t.TempDir()
	spool, err := openLogSpoolWithBudgets(directory, "node-class-budgets", 4, 4)
	if err != nil {
		t.Fatal(err)
	}
	oneshotA := spoolTestClaim("oneshot-a")
	oneshotB := spoolTestClaim("oneshot-b")
	quietService := serviceSpoolTestClaim("service-quiet")
	noisyService := serviceSpoolTestClaim("service-noisy")
	for _, claim := range []l1.Claim{oneshotA, oneshotB, quietService, noisyService} {
		if err := spool.ensureAttempt(context.Background(), claim); err != nil {
			t.Fatal(err)
		}
	}
	if err := spool.append(context.Background(), spoolTestEvent(oneshotA.Lease.AttemptID, contract.LogStdout, 0, "1234")); err != nil {
		t.Fatal(err)
	}
	if err := spool.append(context.Background(), spoolTestEvent(quietService.Lease.AttemptID, contract.LogStdout, 0, "abcd")); err != nil {
		t.Fatal(err)
	}
	if err := spool.append(context.Background(), spoolTestEvent(oneshotB.Lease.AttemptID, contract.LogStdout, 0, "5")); !errors.Is(err, ErrLogSpoolFull) {
		t.Fatalf("sibling one-shot overflow = %v, want ErrLogSpoolFull", err)
	}
	if err := spool.append(context.Background(), spoolTestEvent(noisyService.Lease.AttemptID, contract.LogStdout, 0, "z")); err != nil {
		t.Fatalf("service capacity append = %v, want ring eviction without producer error", err)
	}

	quiet, err := spool.pending(context.Background(), quietService.Lease.AttemptID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(quiet) != 1 || quiet[0].Gap == nil || quiet[0].Gap.Reason != contract.LogGapSpoolEviction ||
		quiet[0].Gap.ThroughSequence != 0 || quiet[0].Gap.LostEventCount != 1 || quiet[0].Gap.LostByteCount != 4 || len(quiet[0].Bytes) != 0 {
		t.Fatalf("quiet service after sibling eviction = %#v", quiet)
	}
	noisy, err := spool.pending(context.Background(), noisyService.Lease.AttemptID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(noisy) != 1 || string(noisy[0].Bytes) != "z" || noisy[0].Gap != nil {
		t.Fatalf("triggering service event = %#v", noisy)
	}
	oneshot, err := spool.pending(context.Background(), oneshotA.Lease.AttemptID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(oneshot) != 1 || string(oneshot[0].Bytes) != "1234" || oneshot[0].Gap != nil {
		t.Fatalf("service ring touched one-shot evidence: %#v", oneshot)
	}
	if err := spool.Close(); err != nil {
		t.Fatal(err)
	}

	spool, err = openLogSpoolWithBudgets(directory, "node-class-budgets", 4, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer spool.Close()
	attempts, err := spool.pendingAttempts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	classes := make(map[string]string, len(attempts))
	for _, attempt := range attempts {
		classes[attempt.attemptID] = attempt.class
	}
	if classes[oneshotA.Lease.AttemptID] != contract.JobClassOneShot ||
		classes[quietService.Lease.AttemptID] != contract.JobClassService ||
		classes[noisyService.Lease.AttemptID] != contract.JobClassService {
		t.Fatalf("recovered attempt classes = %#v", classes)
	}
}

func TestLogSpoolServiceRingDeclaresPerStreamGaps(t *testing.T) {
	assertLogSpoolServiceRingDeclaresPerStreamGaps(t)
}

func assertLogSpoolServiceRingDeclaresPerStreamGaps(t *testing.T) {
	t.Helper()
	spool, err := openLogSpoolWithBudgets(t.TempDir(), "node-stream-gaps", 64, 3)
	if err != nil {
		t.Fatal(err)
	}
	defer spool.Close()
	claim := serviceSpoolTestClaim("service-streams")
	if err := spool.ensureAttempt(context.Background(), claim); err != nil {
		t.Fatal(err)
	}
	for _, event := range []contract.LogEvent{
		spoolTestEvent(claim.Lease.AttemptID, contract.LogStdout, 0, "a"),
		spoolTestEvent(claim.Lease.AttemptID, contract.LogStderr, 0, "b"),
		spoolTestEvent(claim.Lease.AttemptID, contract.LogStdout, 1, "c"),
		spoolTestEvent(claim.Lease.AttemptID, contract.LogStdout, 2, "def"),
	} {
		if err := spool.append(context.Background(), event); err != nil {
			t.Fatal(err)
		}
	}
	pending, err := spool.pending(context.Background(), claim.Lease.AttemptID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 3 || pending[0].Stream != contract.LogStdout || pending[0].Sequence != 0 || pending[0].Gap == nil ||
		pending[0].Gap.ThroughSequence != 1 || pending[0].Gap.LostEventCount != 2 || pending[0].Gap.LostByteCount != 2 ||
		pending[1].Stream != contract.LogStderr || pending[1].Sequence != 0 || pending[1].Gap == nil ||
		pending[1].Gap.ThroughSequence != 0 || pending[1].Gap.LostEventCount != 1 || pending[1].Gap.LostByteCount != 1 ||
		pending[2].Stream != contract.LogStdout || pending[2].Sequence != 2 || string(pending[2].Bytes) != "def" || pending[2].Gap != nil {
		t.Fatalf("per-stream ring events = %#v", pending)
	}
}

func TestLogSpoolServiceOversizedEventBecomesGap(t *testing.T) {
	assertLogSpoolServiceOversizedEventBecomesGap(t)
}

func assertLogSpoolServiceOversizedEventBecomesGap(t *testing.T) {
	t.Helper()
	spool, err := openLogSpoolWithBudgets(t.TempDir(), "node-oversized-service", 64, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer spool.Close()
	claim := serviceSpoolTestClaim("service-oversized")
	if err := spool.ensureAttempt(context.Background(), claim); err != nil {
		t.Fatal(err)
	}
	event := spoolTestEvent(claim.Lease.AttemptID, contract.LogStdout, 0, "12345")
	if err := spool.append(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if err := spool.append(context.Background(), event); err != nil {
		t.Fatalf("idempotent oversized replay = %v", err)
	}
	pending, err := spool.pending(context.Background(), claim.Lease.AttemptID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || pending[0].Gap == nil || pending[0].Gap.Reason != contract.LogGapOversizedEvent ||
		pending[0].Gap.ThroughSequence != 0 || pending[0].Gap.LostEventCount != 1 || pending[0].Gap.LostByteCount != 5 || len(pending[0].Bytes) != 0 {
		t.Fatalf("oversized service event = %#v", pending)
	}
}

func TestLogSpoolServiceEvictionRollsBackWhenTriggeringInsertFails(t *testing.T) {
	assertLogSpoolServiceEvictionRollsBackWhenTriggeringInsertFails(t)
}

func assertLogSpoolServiceEvictionRollsBackWhenTriggeringInsertFails(t *testing.T) {
	t.Helper()
	spool, err := openLogSpoolWithBudgets(t.TempDir(), "node-atomic-ring", 64, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer spool.Close()
	claim := serviceSpoolTestClaim("service-atomic")
	if err := spool.ensureAttempt(context.Background(), claim); err != nil {
		t.Fatal(err)
	}
	if err := spool.append(context.Background(), spoolTestEvent(claim.Lease.AttemptID, contract.LogStdout, 0, "1234")); err != nil {
		t.Fatal(err)
	}
	if _, err := spool.db.Exec(`CREATE TRIGGER reject_service_trigger
BEFORE INSERT ON spool_events
WHEN NEW.attempt_id='service-atomic' AND NEW.sequence=1
BEGIN SELECT RAISE(ABORT, 'injected triggering insert failure'); END`); err != nil {
		t.Fatal(err)
	}
	if err := spool.append(context.Background(), spoolTestEvent(claim.Lease.AttemptID, contract.LogStdout, 1, "5")); err == nil {
		t.Fatal("triggering service append unexpectedly succeeded")
	}
	pending, err := spool.pending(context.Background(), claim.Lease.AttemptID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || pending[0].Gap != nil || pending[0].Sequence != 0 || string(pending[0].Bytes) != "1234" {
		t.Fatalf("failed triggering insert committed eviction: %#v", pending)
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
	claim := serviceSpoolTestClaim("attempt-completion")
	spool := openTestLogSpool(t, directory, "node-completion", 1024)
	if err := spool.ensureAttempt(context.Background(), claim); err != nil {
		t.Fatal(err)
	}
	if err := spool.storeRuntimeResourceManifest(context.Background(), testRuntimeResourceManifest(claim.Job.JobID, claim.Lease.AttemptID), time.Now()); err != nil {
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
	var disposition, reason string
	if err := spool.db.QueryRow(`SELECT disposition, reason FROM spool_completion_receipts WHERE attempt_id=?`, claim.Lease.AttemptID).
		Scan(&disposition, &reason); err != nil {
		t.Fatal(err)
	}
	if disposition != "delivered" || reason != "acknowledged_by_l1" {
		t.Fatalf("completion receipt = %q/%q, want delivered/acknowledged_by_l1", disposition, reason)
	}
	inspection := spool.inspectCompletion(context.Background(), claim.Lease.AttemptID)
	if inspection.State != "delivered" || inspection.Reason != "acknowledged_by_l1" || inspection.EventCount != 0 {
		t.Fatalf("delivered completion inspection = %+v", inspection)
	}
	if err := spool.db.QueryRow("SELECT COUNT(*) FROM runtime_attempt_manifests WHERE attempt_id=?", claim.Lease.AttemptID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("delivered completion retained %d runtime manifest rows", count)
	}
	if err := spool.db.QueryRow("SELECT COUNT(*) FROM runtime_service_manifests WHERE job_id=?", claim.Job.JobID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("delivered service completion retained %d current service manifests, want one bounded removal source", count)
	}
}

func TestLogSpoolInspectionDistinguishesSuppressedAndNeverPersistedCompletion(t *testing.T) {
	spool := openTestLogSpool(t, t.TempDir(), "node-completion-disposition", 1024)
	defer spool.Close()
	claim := serviceSpoolTestClaim("attempt-suppressed")
	if err := spool.ensureAttempt(context.Background(), claim); err != nil {
		t.Fatal(err)
	}
	before := spool.inspectCompletion(context.Background(), claim.Lease.AttemptID)
	if before.State != "never_persisted" {
		t.Fatalf("pre-completion inspection = %+v", before)
	}
	if err := spool.suppressCompletion(context.Background(), claim.Lease.AttemptID); err != nil {
		t.Fatal(err)
	}
	after := spool.inspectCompletion(context.Background(), claim.Lease.AttemptID)
	if after.State != "suppressed" || after.Reason != "service_intent_stop" {
		t.Fatalf("suppressed completion inspection = %+v", after)
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
		Job:   l1.Job{JobID: "job-" + attemptID, Spec: contract.JobSpec{Class: contract.JobClassOneShot}},
		Lease: l1.AttemptLease{AttemptID: attemptID, FencingToken: "fence-" + attemptID},
	}
}

func serviceSpoolTestClaim(attemptID string) l1.Claim {
	claim := spoolTestClaim(attemptID)
	claim.Job.Spec.Class = contract.JobClassService
	return claim
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
