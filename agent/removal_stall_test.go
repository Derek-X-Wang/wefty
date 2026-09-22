package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

// wedgeRefusal is the exact helper answer that pinned both node service Slots
// in the first Mac Computer run (#450): the helper no longer holds the attempt
// live, so no number of Deletes can ever turn it positive.
func wedgeRefusal() error {
	return &ocihelper.RPCError{
		Code: ocihelper.CodeUnauthorizedAttempt, Message: "attempt authority does not match a live attempt",
	}
}

// TestWedgedRemovalDeclaresAStallInsteadOfRetryingForever reproduces the wedge
// end to end: a directive redelivered on every heartbeat whose cleanup is
// refused identically every time. Before this change the loop had no exit and
// the Slot stayed pinned; the removal must now end, with the refusal recorded.
func TestWedgedRemovalDeclaresAStallInsteadOfRetryingForever(t *testing.T) {
	spool := openTestLogSpool(t, t.TempDir(), "wedged-removal-node", 1024)
	defer spool.Close()
	removal := testRuntimeRemoval("wedged-job")
	prepared := time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC)
	if err := spool.storeRuntimeResourceManifest(t.Context(), testRuntimeResourceManifest(removal.jobID, "attempt-a"), prepared); err != nil {
		t.Fatal(err)
	}
	if err := spool.beginRemoval(t.Context(), removal, prepared); err != nil {
		t.Fatal(err)
	}

	now := prepared
	controller := &removalController{nodeID: "wedged-removal-node", bootSessionID: "boot-1"}
	controller.now = func() time.Time { return now }
	controller.stallBound = l1.DefaultRemovalStallBound
	controller.loadRuntimeRemoval = spool.runtimeRemoval
	controller.recordRemovalFailure = func(ctx context.Context, target localRemoval, code, detail string) error {
		return spool.recordRuntimeRemovalFailure(ctx, target, code, detail, controller.bootSessionID, now)
	}
	controller.recordStallDeclared = func(ctx context.Context, target localRemoval) error {
		return spool.recordRuntimeRemovalStallDeclared(ctx, target, now)
	}
	var declarations []l1.ServiceRemovalStallEvidence
	controller.ackRemovalStall = func(_ context.Context, target localRemoval, record runtimeRemovalRecord) error {
		declarations = append(declarations, l1.ServiceRemovalStallEvidence{
			Kind: l1.ServiceRemovalStallEvidenceKind, JobID: target.jobID, Phase: string(record.phase),
			LastRefusalCode: record.lastRefusalCode, LastRefusalDetail: record.lastRefusalDetail,
			Attempts: record.failedAttempts, PreparedAt: record.preparedAt,
		})
		return nil
	}
	directive := l1.RemovalDirective{
		JobID: removal.jobID, BoundNodeID: controller.nodeID, Kind: contract.JobKindOCI,
		RemovalGeneration: removal.generation, CleanupFence: removal.cleanupFence,
		RootInstanceID: removal.rootInstanceID,
	}

	// Forty heartbeats is the ten-minute bound at the agent's ordinary
	// fifteen-second cadence, which is what the run-1 log showed happening;
	// the beat that lands exactly on the bound is the one that may declare.
	for beat := 0; beat <= 40; beat++ {
		_, _ = controller.noteRemovalFailure(t.Context(), directive, wedgeRefusal())
		now = now.Add(15 * time.Second)
	}
	if len(declarations) != 1 {
		t.Fatalf("stall declarations = %d, want exactly one (the removal never ended)", len(declarations))
	}
	declared := declarations[0]
	if declared.LastRefusalCode != string(ocihelper.CodeUnauthorizedAttempt) ||
		declared.LastRefusalDetail != "attempt authority does not match a live attempt" {
		t.Fatalf("declared refusal = %q/%q", declared.LastRefusalCode, declared.LastRefusalDetail)
	}
	if declared.Attempts < l1.MinimumServiceRemovalStallAttempts || !declared.PreparedAt.Equal(prepared) {
		t.Fatalf("declared evidence = %#v", declared)
	}
	record, found, err := spool.runtimeRemoval(t.Context(), removal.jobID)
	if err != nil || !found || record.stallDeclaredAt == nil {
		t.Fatalf("durable record after declaration = %+v found=%t err=%v", record, found, err)
	}
	if record.completedAt != nil {
		t.Fatal("a declared stall must never mark the removal complete")
	}
}

func TestDeclaredRemovalRetriesWithDurableBackoffAndLogsStateChanges(t *testing.T) {
	spool := openTestLogSpool(t, t.TempDir(), "backoff-node", 1024)
	defer spool.Close()
	removal := testRuntimeRemoval("backoff-job")
	prepared := time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC)
	if err := spool.storeRuntimeResourceManifest(t.Context(), testRuntimeResourceManifest(removal.jobID, "attempt"), prepared); err != nil {
		t.Fatal(err)
	}
	if err := spool.beginRemoval(t.Context(), removal, prepared); err != nil {
		t.Fatal(err)
	}
	now := prepared.Add(l1.DefaultRemovalStallBound)
	for attempt := 0; attempt < l1.MinimumServiceRemovalStallAttempts; attempt++ {
		if err := spool.recordRuntimeRemovalFailure(t.Context(), removal, string(ocihelper.CodeUnauthorizedAttempt),
			"attempt authority does not match a live attempt", "boot", now); err != nil {
			t.Fatal(err)
		}
	}
	declaration, err := json.Marshal(l1.ServiceRemovalStallEvidence{
		Kind: l1.ServiceRemovalStallEvidenceKind, JobID: removal.jobID,
		LastRefusalCode: string(ocihelper.CodeUnauthorizedAttempt), Attempts: l1.MinimumServiceRemovalStallAttempts,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := spool.freezeRuntimeRemovalStallDeclaration(t.Context(), removal, declaration, "stall-key"); err != nil {
		t.Fatal(err)
	}
	if err := spool.recordRuntimeRemovalStallDeclared(t.Context(), removal, now); err != nil {
		t.Fatal(err)
	}
	var logs []string
	helperAttempts := 0
	controller := &removalController{
		nodeID: "backoff-node", bootSessionID: "boot", stallBound: l1.DefaultRemovalStallBound,
		now: func() time.Time { return now }, logf: func(format string, args ...any) {
			logs = append(logs, format)
		},
	}
	controller.beginRemoval = func(context.Context, localRemoval) error { return nil }
	controller.loadRuntimeRemoval = spool.runtimeRemoval
	controller.recordRemovalFailure = func(ctx context.Context, target localRemoval, code, detail string) error {
		return spool.recordRuntimeRemovalFailure(ctx, target, code, detail, controller.bootSessionID, now)
	}
	controller.reapService = func(context.Context, string, string, []workloadrunner.RuntimeResourceManifest) (workloadrunner.ReapReceipt, error) {
		helperAttempts++
		return workloadrunner.ReapReceipt{}, wedgeRefusal()
	}
	directive := l1.RemovalDirective{
		JobID: removal.jobID, BoundNodeID: controller.nodeID, Kind: removal.kind,
		RemovalGeneration: removal.generation, CleanupFence: removal.cleanupFence,
		RootInstanceID: removal.rootInstanceID,
	}
	const heartbeats = 40
	for beat := 0; beat < heartbeats; beat++ {
		if err := controller.reconcile(t.Context(), directive); err != nil {
			t.Fatalf("declared retry %d: %v", beat, err)
		}
		now = now.Add(15 * time.Second)
	}
	if helperAttempts == 0 || helperAttempts >= heartbeats/2 {
		t.Fatalf("helper attempts after %d heartbeats = %d, want bounded retries", heartbeats, helperAttempts)
	}
	if len(logs) > 2 {
		t.Fatalf("declared removal emitted %d log lines for identical refusals: %v", len(logs), logs)
	}
	record, found, err := spool.runtimeRemoval(t.Context(), removal.jobID)
	if err != nil || !found || record.lastAttemptedAt == nil {
		t.Fatalf("durable retry state = %+v found=%t err=%v", record, found, err)
	}
	if record.stallRetryAttempts == 0 {
		t.Fatal("declared retry cadence was not persisted independently of the refusal streak")
	}
}

func TestCompleteDeclaredRemovalUsesBackoffForSameBootAcknowledgementConflicts(t *testing.T) {
	spool := openTestLogSpool(t, t.TempDir(), "complete-backoff-node", 1024)
	defer spool.Close()
	removal := testRuntimeRemoval("complete-backoff-job")
	prepared := time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC)
	manifest := testRuntimeResourceManifest(removal.jobID, "attempt")
	if err := spool.storeRuntimeResourceManifest(t.Context(), manifest, prepared); err != nil {
		t.Fatal(err)
	}
	if err := spool.beginRemoval(t.Context(), removal, prepared); err != nil {
		t.Fatal(err)
	}
	lastAttempt := prepared.Add(l1.DefaultRemovalStallBound)
	for range l1.MinimumServiceRemovalStallAttempts {
		if err := spool.recordRuntimeRemovalFailure(t.Context(), removal, string(ocihelper.CodeUnauthorizedAttempt), "A", "same-boot", lastAttempt); err != nil {
			t.Fatal(err)
		}
	}
	declaration, _ := json.Marshal(l1.ServiceRemovalStallEvidence{Kind: l1.ServiceRemovalStallEvidenceKind,
		JobID: removal.jobID, LastRefusalCode: string(ocihelper.CodeUnauthorizedAttempt)})
	if _, _, err := spool.freezeRuntimeRemovalStallDeclaration(t.Context(), removal, declaration, "stall-key"); err != nil {
		t.Fatal(err)
	}
	if err := spool.recordRuntimeRemovalStallDeclared(t.Context(), removal, lastAttempt); err != nil {
		t.Fatal(err)
	}
	receipt := workloadrunner.ReapReceipt{RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidenceAttempt, BootSessionID: "same-boot"}
	if err := spool.recordRuntimeQuiesced(t.Context(), removal, receipt, lastAttempt); err != nil {
		t.Fatal(err)
	}
	if err := spool.recordRuntimeAttested(t.Context(), removal,
		testRuntimeRemovalAttestation(runtimeRemovalManifest{Version: 1, JobID: removal.jobID,
			RemovalGeneration: removal.generation, Attempts: []workloadrunner.RuntimeResourceManifest{manifest}}), lastAttempt); err != nil {
		t.Fatal(err)
	}

	now := lastAttempt
	acknowledgements := 0
	controller := &removalController{nodeID: "complete-backoff-node", bootSessionID: receipt.BootSessionID,
		now: func() time.Time { return now }}
	controller.beginRemoval = func(context.Context, localRemoval) error { return nil }
	controller.loadRuntimeRemoval = spool.runtimeRemoval
	controller.recordRemovalFailure = func(ctx context.Context, target localRemoval, code, detail string) error {
		return spool.recordRuntimeRemovalFailure(ctx, target, code, detail, controller.bootSessionID, now)
	}
	controller.purgeJob = func(context.Context, string) error { return nil }
	controller.removeResource = func(context.Context, localRemoval) error { return nil }
	controller.ackRemoval = func(context.Context, localRemoval) error {
		acknowledgements++
		return &ProtocolError{APIError: contract.APIError{Code: contract.ErrorConflict,
			Message: "Computer removal requires positive absence for every Backup copy"}}
	}
	controller.finishRemoval = func(context.Context, localRemoval) error {
		t.Fatal("a refused acknowledgement finished the removal")
		return nil
	}
	directive := l1.RemovalDirective{JobID: removal.jobID, BoundNodeID: controller.nodeID, Kind: removal.kind,
		RemovalGeneration: removal.generation, CleanupFence: removal.cleanupFence, RootInstanceID: removal.rootInstanceID}
	const heartbeats = 40
	for range heartbeats {
		_ = controller.reconcile(t.Context(), directive)
		now = now.Add(15 * time.Second)
	}
	if acknowledgements == 0 || acknowledgements >= heartbeats/2 {
		t.Fatalf("complete-phase acknowledgement attempts after %d heartbeats = %d, want bounded retries", heartbeats, acknowledgements)
	}
	record, found, err := spool.runtimeRemoval(t.Context(), removal.jobID)
	if err != nil || !found || record.phase != runtimeRemovalComplete || record.stallRetryAttempts == 0 {
		t.Fatalf("complete-phase durable retry state = %+v found=%t err=%v", record, found, err)
	}
}

func TestCompleteDeclaredRemovalGetsOneImmediateAcknowledgementReplayPerBoot(t *testing.T) {
	directory := t.TempDir()
	spool := openTestLogSpool(t, directory, "complete-replay-node", 1024)
	removal := testRuntimeRemoval("complete-replay-job")
	prepared := time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC)
	manifest := testRuntimeResourceManifest(removal.jobID, "attempt")
	if err := spool.storeRuntimeResourceManifest(t.Context(), manifest, prepared); err != nil {
		t.Fatal(err)
	}
	if err := spool.beginRemoval(t.Context(), removal, prepared); err != nil {
		t.Fatal(err)
	}
	lastAttempt := prepared.Add(l1.DefaultRemovalStallBound)
	if err := spool.recordRuntimeRemovalFailure(t.Context(), removal, string(ocihelper.CodeUnauthorizedAttempt), "A", "boot-a", lastAttempt); err != nil {
		t.Fatal(err)
	}
	declaration, _ := json.Marshal(l1.ServiceRemovalStallEvidence{Kind: l1.ServiceRemovalStallEvidenceKind,
		JobID: removal.jobID, LastRefusalCode: string(ocihelper.CodeUnauthorizedAttempt)})
	if _, _, err := spool.freezeRuntimeRemovalStallDeclaration(t.Context(), removal, declaration, "stall-key"); err != nil {
		t.Fatal(err)
	}
	if err := spool.recordRuntimeRemovalStallDeclared(t.Context(), removal, lastAttempt); err != nil {
		t.Fatal(err)
	}
	receipt := workloadrunner.ReapReceipt{RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidenceAttempt, BootSessionID: "boot-a"}
	if err := spool.recordRuntimeQuiesced(t.Context(), removal, receipt, lastAttempt); err != nil {
		t.Fatal(err)
	}
	if err := spool.recordRuntimeAttested(t.Context(), removal,
		testRuntimeRemovalAttestation(runtimeRemovalManifest{Version: 1, JobID: removal.jobID,
			RemovalGeneration: removal.generation, Attempts: []workloadrunner.RuntimeResourceManifest{manifest}}), lastAttempt); err != nil {
		t.Fatal(err)
	}

	acknowledgements := 0
	newController := func(bootSessionID string) *removalController {
		controller := &removalController{nodeID: "complete-replay-node", bootSessionID: bootSessionID,
			now: func() time.Time { return lastAttempt }}
		controller.beginRemoval = func(context.Context, localRemoval) error { return nil }
		controller.loadRuntimeRemoval = spool.runtimeRemoval
		controller.recordRemovalFailure = func(ctx context.Context, target localRemoval, code, detail string) error {
			return spool.recordRuntimeRemovalFailure(ctx, target, code, detail, bootSessionID, lastAttempt)
		}
		controller.purgeJob = func(context.Context, string) error { return nil }
		controller.removeResource = func(context.Context, localRemoval) error { return nil }
		controller.ackRemoval = func(context.Context, localRemoval) error {
			acknowledgements++
			return &ProtocolError{APIError: contract.APIError{Code: contract.ErrorConflict, Message: "persistent conflict"}}
		}
		return controller
	}
	directive := l1.RemovalDirective{JobID: removal.jobID, BoundNodeID: "complete-replay-node", Kind: removal.kind,
		RemovalGeneration: removal.generation, CleanupFence: removal.cleanupFence, RootInstanceID: removal.rootInstanceID}
	controller := newController("boot-b")
	_ = controller.reconcile(t.Context(), directive)
	_ = controller.reconcile(t.Context(), directive)
	if acknowledgements != 1 {
		t.Fatalf("boot B acknowledgement attempts = %d, want exactly one immediate replay", acknowledgements)
	}
	if err := spool.Close(); err != nil {
		t.Fatal(err)
	}
	spool = openTestLogSpool(t, directory, "complete-replay-node", 1024)
	defer spool.Close()
	controller = newController("boot-b")
	_ = controller.reconcile(t.Context(), directive)
	if acknowledgements != 1 {
		t.Fatalf("reopened boot B acknowledgement attempts = %d, want durable deferral", acknowledgements)
	}
	controller = newController("boot-c")
	_ = controller.reconcile(t.Context(), directive)
	_ = controller.reconcile(t.Context(), directive)
	if acknowledgements != 2 {
		t.Fatalf("boot C acknowledgement attempts = %d, want exactly one more immediate replay", acknowledgements)
	}
}

func TestRemovalFailureSpoolErrorDoesNotSuppressCleanupCause(t *testing.T) {
	removal := testRuntimeRemoval("spool-error-job")
	var logs []string
	controller := &removalController{nodeID: "node", bootSessionID: "boot",
		managed: &recordingResumeResource{}, outbox: &evidenceOutbox{}, inflight: make(map[string]struct{}),
		logf: func(format string, args ...any) { logs = append(logs, fmt.Sprintf(format, args...)) }}
	controller.beginRemoval = func(context.Context, localRemoval) error { return nil }
	controller.loadRuntimeRemoval = func(context.Context, string) (runtimeRemovalRecord, bool, error) {
		return runtimeRemovalRecord{removal: removal, phase: runtimeRemovalPrepared}, true, nil
	}
	controller.reapService = func(context.Context, string, string, []workloadrunner.RuntimeResourceManifest) (workloadrunner.ReapReceipt, error) {
		return workloadrunner.ReapReceipt{}, wedgeRefusal()
	}
	controller.recordRemovalFailure = func(context.Context, localRemoval, string, string) error {
		return errors.New("sqlite write failed")
	}
	controller.enqueue(t.Context(), l1.RemovalDirective{JobID: removal.jobID, BoundNodeID: controller.nodeID,
		Kind: removal.kind, RemovalGeneration: removal.generation, CleanupFence: removal.cleanupFence,
		RootInstanceID: removal.rootInstanceID}, make(chan destinationError, 1))
	controller.wait()
	if len(logs) != 2 || !strings.Contains(logs[0], "sqlite write failed") ||
		!strings.Contains(logs[1], string(ocihelper.CodeUnauthorizedAttempt)) {
		t.Fatalf("spool and cleanup failure logs = %v, want both facts", logs)
	}
}

func TestDeclaredRemovalRetryCadenceAndLogsSurviveRefusalTransitions(t *testing.T) {
	spool := openTestLogSpool(t, t.TempDir(), "transition-node", 1024)
	defer spool.Close()
	removal := testRuntimeRemoval("transition-job")
	prepared := time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC)
	if err := spool.storeRuntimeResourceManifest(t.Context(), testRuntimeResourceManifest(removal.jobID, "attempt"), prepared); err != nil {
		t.Fatal(err)
	}
	if err := spool.beginRemoval(t.Context(), removal, prepared); err != nil {
		t.Fatal(err)
	}
	now := prepared.Add(l1.DefaultRemovalStallBound)
	for range l1.MinimumServiceRemovalStallAttempts {
		if err := spool.recordRuntimeRemovalFailure(t.Context(), removal, string(ocihelper.CodeUnauthorizedAttempt), "A", "boot", now); err != nil {
			t.Fatal(err)
		}
	}
	declaration, err := json.Marshal(l1.ServiceRemovalStallEvidence{
		Kind: l1.ServiceRemovalStallEvidenceKind, JobID: removal.jobID,
		LastRefusalCode: string(ocihelper.CodeUnauthorizedAttempt), Attempts: l1.MinimumServiceRemovalStallAttempts,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := spool.freezeRuntimeRemovalStallDeclaration(t.Context(), removal, declaration, "stall-key"); err != nil {
		t.Fatal(err)
	}
	var logs []string
	controller := &removalController{nodeID: "transition-node", bootSessionID: "boot", stallBound: l1.DefaultRemovalStallBound,
		managed: &recordingResumeResource{}, outbox: &evidenceOutbox{}, inflight: make(map[string]struct{}),
		now: func() time.Time { return now }, logf: func(format string, args ...any) { logs = append(logs, fmt.Sprintf(format, args...)) }}
	controller.beginRemoval = func(context.Context, localRemoval) error { return nil }
	controller.loadRuntimeRemoval = spool.runtimeRemoval
	controller.recordRemovalFailure = func(ctx context.Context, target localRemoval, code, detail string) error {
		return spool.recordRuntimeRemovalFailure(ctx, target, code, detail, controller.bootSessionID, now)
	}
	controller.ackRemovalStall = func(context.Context, localRemoval, runtimeRemovalRecord) error { return nil }
	controller.recordStallDeclared = func(ctx context.Context, target localRemoval) error {
		return spool.recordRuntimeRemovalStallDeclared(ctx, target, now)
	}
	directive := l1.RemovalDirective{JobID: removal.jobID, BoundNodeID: controller.nodeID, Kind: contract.JobKindOCI,
		RemovalGeneration: removal.generation, CleanupFence: removal.cleanupFence, RootInstanceID: removal.rootInstanceID}
	if suppressed, err := controller.noteRemovalFailure(t.Context(), directive, wedgeRefusal()); err != nil || !suppressed {
		t.Fatalf("declare A refusal = suppressed:%t err:%v", suppressed, err)
	}

	causes := []error{
		&ocihelper.RPCError{Code: ocihelper.CodeSessionStale, Message: "B"},
		&ocihelper.RPCError{Code: ocihelper.CodeSessionStale, Message: "B"},
		wedgeRefusal(),
	}
	causeIndex := 0
	controller.reapService = func(context.Context, string, string, []workloadrunner.RuntimeResourceManifest) (workloadrunner.ReapReceipt, error) {
		cause := causes[causeIndex]
		causeIndex++
		return workloadrunner.ReapReceipt{}, cause
	}
	delays := []time.Duration{15 * time.Second, 30 * time.Second, time.Minute}
	failures := make(chan destinationError, len(causes))
	for index := range causes {
		record, _, err := spool.runtimeRemoval(t.Context(), removal.jobID)
		if err != nil {
			t.Fatal(err)
		}
		controller.now = func() time.Time { return record.lastAttemptedAt.Add(delays[index] - time.Second) }
		if !controller.declaredRemovalRetryDeferred(record) {
			t.Fatalf("retry %d ran before its %s durable delay", index+1, delays[index])
		}
		now = record.lastAttemptedAt.Add(delays[index])
		controller.now = func() time.Time { return now }
		if controller.declaredRemovalRetryDeferred(record) {
			t.Fatalf("retry %d stayed deferred at its %s deadline", index+1, delays[index])
		}
		controller.enqueue(t.Context(), directive, failures)
		controller.wait()
		record, _, err = spool.runtimeRemoval(t.Context(), removal.jobID)
		if err != nil {
			t.Fatal(err)
		}
		if record.stallRetryAttempts != index+1 {
			t.Fatalf("retry %d durable cadence = %d", index+1, record.stallRetryAttempts)
		}
		controller.now = func() time.Time { return record.lastAttemptedAt.Add(time.Second) }
		if !controller.declaredRemovalRetryDeferred(record) {
			t.Fatalf("retry %d did not retain a bounded delay after refusal transition", index+1)
		}
		controller.now = func() time.Time { return now }
	}
	if len(failures) != 0 {
		t.Fatalf("refusal transitions reached the session fence: %+v", <-failures)
	}
	if len(logs) != 3 {
		t.Fatalf("declaration and refusal transitions logged %d times, want declaration, B, A: %v", len(logs), logs)
	}
	if !strings.Contains(logs[0], "removal declared stalled") ||
		!strings.Contains(logs[1], string(ocihelper.CodeSessionStale)) ||
		!strings.Contains(logs[2], string(ocihelper.CodeUnauthorizedAttempt)) {
		t.Fatalf("declaration/refusal transition logs = %v, want declaration then B then A", logs)
	}
	record, _, err := spool.runtimeRemoval(t.Context(), removal.jobID)
	if err != nil {
		t.Fatal(err)
	}
	if record.failedAttempts != 1 || record.lastRefusalCode != string(ocihelper.CodeUnauthorizedAttempt) {
		t.Fatalf("qualifying streak after A,A,A -> B,B -> A = %d/%q", record.failedAttempts, record.lastRefusalCode)
	}
	record.stallRetryAttempts = 99
	controller.now = func() time.Time { return record.lastAttemptedAt.Add(declaredRemovalRetryMax - time.Second) }
	if !controller.declaredRemovalRetryDeferred(record) {
		t.Fatal("declared retry delay was not capped at three minutes")
	}
}

// TestAgentDeclaresAStallOnlyAfterRepeatedRefusalsPastTheBound is the agent
// half of the two-sided bound. L1 can see how long a directive has stood; only
// the node knows whether anything was actually retried during that time.
func TestAgentDeclaresAStallOnlyAfterRepeatedRefusalsPastTheBound(t *testing.T) {
	prepared := time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC)
	pastBound := prepared.Add(l1.DefaultRemovalStallBound)
	for name, testCase := range map[string]struct {
		record  runtimeRemovalRecord
		now     time.Time
		declare bool
	}{
		"past the bound with a repeated refusal": {
			record:  stallRecord(prepared, l1.MinimumServiceRemovalStallAttempts, "unauthorized_attempt"),
			now:     pastBound,
			declare: true,
		},
		"inside the bound": {
			record: stallRecord(prepared, 9, "unauthorized_attempt"),
			now:    prepared.Add(l1.DefaultRemovalStallBound - time.Second),
		},
		"one bad try": {
			record: stallRecord(prepared, 1, "unauthorized_attempt"),
			now:    pastBound,
		},
		"never retried": {
			record: func() runtimeRemovalRecord {
				record := stallRecord(prepared, 0, "")
				record.lastAttemptedAt = nil
				return record
			}(),
			now: pastBound,
		},
		"already completed": {
			record: func() runtimeRemovalRecord {
				record := stallRecord(prepared, 5, "unauthorized_attempt")
				completed := pastBound
				record.completedAt = &completed
				return record
			}(),
			now: pastBound,
		},
	} {
		t.Run(name, func(t *testing.T) {
			controller := &removalController{
				stallBound: l1.DefaultRemovalStallBound,
				now:        func() time.Time { return testCase.now },
			}
			if got := controller.removalIsStalled(t.Context(), testCase.record); got != testCase.declare {
				t.Fatalf("removalIsStalled = %t, want %t", got, testCase.declare)
			}
		})
	}
}

// TestAnUntypedRemovalFailureNeverBecomesAStall keeps a transport hiccup from
// buying a permanent unverified outcome: it is never counted into the streak
// and it never declares, however long the directive has stood.
func TestAnUntypedRemovalFailureNeverBecomesAStall(t *testing.T) {
	controller := &removalController{nodeID: "node", stallBound: l1.DefaultRemovalStallBound}
	controller.now = func() time.Time { return time.Now().Add(24 * time.Hour) }
	counted := 0
	reset := 0
	controller.recordRemovalFailure = func(context.Context, localRemoval, string, string) error {
		counted++
		return nil
	}
	controller.recordUntypedFailure = func(context.Context, localRemoval) error {
		reset++
		return nil
	}
	controller.ackRemovalStall = func(context.Context, localRemoval, runtimeRemovalRecord) error {
		t.Fatal("an untyped failure declared a stall")
		return nil
	}
	controller.loadRuntimeRemoval = func(context.Context, string) (runtimeRemovalRecord, bool, error) {
		return runtimeRemovalRecord{failedAttempts: 99, lastRefusalCode: "unauthorized_attempt"}, true, nil
	}
	if declared, err := controller.noteRemovalFailure(t.Context(),
		l1.RemovalDirective{JobID: "job", BoundNodeID: "node"}, context.DeadlineExceeded); err != nil || declared {
		t.Fatal("an untyped failure reported the removal as declared stalled")
	}
	if counted != 0 || reset != 1 {
		t.Fatalf("untyped failure counted=%d reset=%d, want 0/1", counted, reset)
	}
}

// Node-wide Computer disk admission contention says nothing about the resource
// this removal asks about -- only that another Computer's operation held the
// Node while this one waited. Counting it would let unrelated traffic declare
// a healthy removal stalled, so it ends the streak like an untyped failure.
func TestAdmissionContentionNeverBecomesARemovalStall(t *testing.T) {
	controller := &removalController{nodeID: "node", stallBound: l1.DefaultRemovalStallBound}
	controller.now = func() time.Time { return time.Now().Add(24 * time.Hour) }
	counted := 0
	reset := 0
	controller.recordRemovalFailure = func(context.Context, localRemoval, string, string) error {
		counted++
		return nil
	}
	controller.recordUntypedFailure = func(context.Context, localRemoval) error {
		reset++
		return nil
	}
	controller.ackRemovalStall = func(context.Context, localRemoval, runtimeRemovalRecord) error {
		t.Fatal("admission contention declared a stall")
		return nil
	}
	controller.loadRuntimeRemoval = func(context.Context, string) (runtimeRemovalRecord, bool, error) {
		return runtimeRemovalRecord{failedAttempts: 99, lastRefusalCode: "unauthorized_attempt"}, true, nil
	}
	busy := &ocihelper.RPCError{Code: ocihelper.CodeComputerStorageBusy,
		Message: "Computer Storage root admission is contended: context deadline exceeded"}
	for range l1.MinimumServiceRemovalStallAttempts {
		declared, err := controller.noteRemovalFailure(t.Context(),
			l1.RemovalDirective{JobID: "job", BoundNodeID: "node"},
			fmt.Errorf("delete Computer disk resource: %w", busy))
		if err != nil || declared {
			t.Fatalf("admission contention reported the removal as declared stalled: declared=%t err=%v", declared, err)
		}
	}
	if counted != 0 || reset != l1.MinimumServiceRemovalStallAttempts {
		t.Fatalf("admission contention counted=%d reset=%d, want 0/%d", counted, reset, l1.MinimumServiceRemovalStallAttempts)
	}
}

// TestARemovalRefusalStreakResetsWhenTheRefusalChanges separates a removal
// that cannot proceed from one still working through distinct causes.
func TestARemovalRefusalStreakResetsWhenTheRefusalChanges(t *testing.T) {
	spool := openTestLogSpool(t, t.TempDir(), "streak-node", 1024)
	defer spool.Close()
	removal := testRuntimeRemoval("streak-job")
	prepared := time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC)
	if err := spool.storeRuntimeResourceManifest(t.Context(), testRuntimeResourceManifest(removal.jobID, "attempt-a"), prepared); err != nil {
		t.Fatal(err)
	}
	if err := spool.beginRemoval(t.Context(), removal, prepared); err != nil {
		t.Fatal(err)
	}
	for index, code := range []string{"unauthorized_attempt", "unauthorized_attempt", "session_stale"} {
		if err := spool.recordRuntimeRemovalFailure(t.Context(), removal, code, "detail", "boot",
			prepared.Add(time.Duration(index)*time.Minute)); err != nil {
			t.Fatal(err)
		}
	}
	record, found, err := spool.runtimeRemoval(t.Context(), removal.jobID)
	if err != nil || !found {
		t.Fatalf("record = %+v found=%t err=%v", record, found, err)
	}
	if record.failedAttempts != 1 || record.lastRefusalCode != "session_stale" {
		t.Fatalf("streak after a changed refusal = %d/%q, want 1/session_stale", record.failedAttempts, record.lastRefusalCode)
	}
}

func stallRecord(prepared time.Time, attempts int, refusal string) runtimeRemovalRecord {
	lastAttempted := prepared.Add(time.Minute)
	return runtimeRemovalRecord{
		removal: testRuntimeRemoval("stall-job"), phase: runtimeRemovalPrepared, preparedAt: prepared,
		failedAttempts: attempts, lastRefusalCode: refusal, lastAttemptedAt: &lastAttempted,
	}
}

// TestAnUntypedFailureBreaksTheTypedRefusalStreak keeps the bound honest: the
// rule is three consecutive attempts that produced the same typed refusal, so
// an attempt that produced no typed refusal ends the streak instead of being
// invisible to it.
func TestAnUntypedFailureBreaksTheTypedRefusalStreak(t *testing.T) {
	spool := openTestLogSpool(t, t.TempDir(), "bridge-node", 1024)
	defer spool.Close()
	removal := testRuntimeRemoval("bridge-job")
	prepared := time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC)
	if err := spool.storeRuntimeResourceManifest(t.Context(), testRuntimeResourceManifest(removal.jobID, "attempt-a"), prepared); err != nil {
		t.Fatal(err)
	}
	if err := spool.beginRemoval(t.Context(), removal, prepared); err != nil {
		t.Fatal(err)
	}
	now := prepared
	controller := &removalController{nodeID: "bridge-node", stallBound: l1.DefaultRemovalStallBound}
	controller.now = func() time.Time { return now }
	controller.loadRuntimeRemoval = spool.runtimeRemoval
	controller.removalStartedAt = spool.removalStartedAt
	controller.recordRemovalFailure = func(ctx context.Context, target localRemoval, code, detail string) error {
		return spool.recordRuntimeRemovalFailure(ctx, target, code, detail, controller.bootSessionID, now)
	}
	controller.recordUntypedFailure = func(ctx context.Context, target localRemoval) error {
		return spool.recordRuntimeRemovalUntypedFailure(ctx, target, controller.bootSessionID, now)
	}
	declarations := 0
	controller.ackRemovalStall = func(context.Context, localRemoval, runtimeRemovalRecord) error {
		declarations++
		return nil
	}
	controller.recordStallDeclared = func(ctx context.Context, target localRemoval) error {
		return spool.recordRuntimeRemovalStallDeclared(ctx, target, now)
	}
	directive := l1.RemovalDirective{
		JobID: removal.jobID, BoundNodeID: controller.nodeID, Kind: contract.JobKindOCI,
		RemovalGeneration: removal.generation, CleanupFence: removal.cleanupFence,
		RootInstanceID: removal.rootInstanceID,
	}
	// Well past the bound throughout, so only the streak decides.
	now = prepared.Add(l1.DefaultRemovalStallBound + time.Hour)
	for index, cause := range []error{wedgeRefusal(), wedgeRefusal(), context.DeadlineExceeded} {
		_, _ = controller.noteRemovalFailure(t.Context(), directive, cause)
		if declarations != 0 {
			t.Fatalf("declared after %d failures ending in an untyped one", index+1)
		}
	}
	record, _, err := spool.runtimeRemoval(t.Context(), removal.jobID)
	if err != nil {
		t.Fatal(err)
	}
	if record.failedAttempts != 0 || record.lastRefusalCode != "" {
		t.Fatalf("streak after an untyped failure = %d/%q, want 0/empty", record.failedAttempts, record.lastRefusalCode)
	}
	for beat := 0; beat < l1.MinimumServiceRemovalStallAttempts; beat++ {
		_, _ = controller.noteRemovalFailure(t.Context(), directive, wedgeRefusal())
	}
	if declarations != 1 {
		t.Fatalf("declarations after a rebuilt streak = %d, want 1", declarations)
	}
}

// TestRepeatedRestartsCannotPostponeAStallDeclaration pins the bound to the
// immutable moment the node accepted the directive. The frozen manifest's
// prepared time is refreshed whenever a Storage-only inventory is
// reconstructed, so measuring against that would reset the clock on every boot.
func TestRepeatedRestartsCannotPostponeAStallDeclaration(t *testing.T) {
	spool := openTestLogSpool(t, t.TempDir(), "restart-node", 1024)
	defer spool.Close()
	removal := testRuntimeRemoval("restart-job")
	started := time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC)
	if err := spool.storeRuntimeResourceManifest(t.Context(), testRuntimeResourceManifest(removal.jobID, "attempt-a"), started); err != nil {
		t.Fatal(err)
	}
	if err := spool.beginRemoval(t.Context(), removal, started); err != nil {
		t.Fatal(err)
	}
	record, found, err := spool.runtimeRemoval(t.Context(), removal.jobID)
	if err != nil || !found {
		t.Fatalf("record = %+v found=%t err=%v", record, found, err)
	}
	// A later boot reconstructed the inventory, so the record's own prepared
	// time is now recent even though the directive is hours old.
	record.preparedAt = started.Add(11 * time.Hour)
	lastAttempted := record.preparedAt
	record.lastAttemptedAt = &lastAttempted
	record.failedAttempts = l1.MinimumServiceRemovalStallAttempts
	record.lastRefusalCode = "unauthorized_attempt"

	controller := &removalController{
		stallBound: l1.DefaultRemovalStallBound, removalStartedAt: spool.removalStartedAt,
		now: func() time.Time { return started.Add(11 * time.Hour) },
	}
	if !controller.removalIsStalled(t.Context(), record) {
		t.Fatal("a refreshed prepared time postponed the declaration past the immutable removal start")
	}
	controller.now = func() time.Time { return started.Add(time.Minute) }
	if controller.removalIsStalled(t.Context(), record) {
		t.Fatal("a removal younger than the bound was declared stalled")
	}
}

// TestStallAccountingRefusesARemovalWithNoRuntimeRecord states the scope out
// loud. Stall accounting lives on the runtime removal record, which only a
// runtime removal has; a process service must be refused visibly rather than
// pass through an UPDATE that silently changes nothing.
func TestStallAccountingRefusesARemovalWithNoRuntimeRecord(t *testing.T) {
	spool := openTestLogSpool(t, t.TempDir(), "process-node", 1024)
	defer spool.Close()
	removal := localRemoval{jobID: "process-job", kind: contract.JobKindProcess,
		generation: l1.InitialServiceRemovalGeneration, cleanupFence: "cleanup-fence", rootInstanceID: "root-instance"}
	started := time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC)
	if err := spool.beginRemoval(t.Context(), removal, started); err != nil {
		t.Fatal(err)
	}
	err := spool.recordRuntimeRemovalFailure(t.Context(), removal, "unauthorized_attempt", "detail", "boot", started)
	if err == nil || !strings.Contains(err.Error(), "only a runtime removal keeps stall accounting") {
		t.Fatalf("process-service stall accounting = %v, want a loud refusal", err)
	}
	if err := spool.recordRuntimeRemovalUntypedFailure(t.Context(), removal, "boot", started); err == nil {
		t.Fatal("process-service streak reset silently succeeded")
	}
}

// TestADeclaredStallSuppressesOnlyItsOwnRefusal keeps the boot-unblocking
// exception narrow. The declaration says one thing -- that this helper refusal
// keeps happening -- so only that refusal is expected afterwards. A replaced
// node session, a different helper refusal, or an untyped local failure is new
// information, and swallowing it would leave an obsolete session unfenced.
func TestADeclaredStallSuppressesOnlyItsOwnRefusal(t *testing.T) {
	declared := time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC)
	declaration, err := json.Marshal(l1.ServiceRemovalStallEvidence{
		Kind: l1.ServiceRemovalStallEvidenceKind, JobID: "declared-job",
		LastRefusalCode: "unauthorized_attempt", Attempts: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	for name, testCase := range map[string]struct {
		cause    error
		suppress bool
	}{
		"the refusal the declaration stands for": {cause: wedgeRefusal(), suppress: true},
		"a different helper refusal": {cause: &ocihelper.RPCError{
			Code: ocihelper.CodeSessionStale, Message: "session is stale"}},
		"a replaced node session": {cause: &ProtocolError{APIError: contract.APIError{
			Code: contract.ErrorNodeSessionReplaced, Message: "node boot session has been replaced"}}},
		"an untyped local failure": {cause: context.DeadlineExceeded},
	} {
		t.Run(name, func(t *testing.T) {
			controller := &removalController{nodeID: "node", stallBound: l1.DefaultRemovalStallBound}
			controller.now = func() time.Time { return declared.Add(time.Hour) }
			controller.recordRemovalFailure = func(context.Context, localRemoval, string, string) error { return nil }
			controller.recordUntypedFailure = func(context.Context, localRemoval) error { return nil }
			controller.loadRuntimeRemoval = func(context.Context, string) (runtimeRemovalRecord, bool, error) {
				return runtimeRemovalRecord{
					phase: runtimeRemovalPrepared, preparedAt: declared, failedAttempts: 9,
					lastRefusalCode: "unauthorized_attempt", stallDeclaration: declaration,
					stallDeclaredAt: &declared,
				}, true, nil
			}
			controller.ackRemovalStall = func(context.Context, localRemoval, runtimeRemovalRecord) error {
				t.Fatal("an already-declared removal declared again")
				return nil
			}
			got, err := controller.noteRemovalFailure(t.Context(),
				l1.RemovalDirective{JobID: "declared-job", BoundNodeID: "node"}, testCase.cause)
			if err != nil {
				t.Fatal(err)
			}
			if got != testCase.suppress {
				t.Fatalf("suppressed = %t, want %t", got, testCase.suppress)
			}
		})
	}
}

func TestFrozenDeclarationAcceptanceStillPropagatesCurrentDifferentRefusal(t *testing.T) {
	prepared := time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC)
	removal := testRuntimeRemoval("lost-response-different-refusal")
	declaration, err := json.Marshal(l1.ServiceRemovalStallEvidence{
		Kind: l1.ServiceRemovalStallEvidenceKind, JobID: removal.jobID,
		LastRefusalCode: string(ocihelper.CodeUnauthorizedAttempt), Attempts: l1.MinimumServiceRemovalStallAttempts,
	})
	if err != nil {
		t.Fatal(err)
	}
	record := runtimeRemovalRecord{removal: removal, phase: runtimeRemovalPrepared, preparedAt: prepared,
		failedAttempts: l1.MinimumServiceRemovalStallAttempts, lastRefusalCode: string(ocihelper.CodeUnauthorizedAttempt),
		stallDeclaration: declaration, stallDeclarationKey: "frozen-key"}
	now := prepared.Add(l1.DefaultRemovalStallBound)
	var logs []string
	controller := &removalController{nodeID: "node", bootSessionID: "boot", stallBound: l1.DefaultRemovalStallBound,
		managed: &recordingResumeResource{}, outbox: &evidenceOutbox{}, inflight: make(map[string]struct{}),
		now: func() time.Time { return now }, logf: func(format string, args ...any) { logs = append(logs, fmt.Sprintf(format, args...)) }}
	controller.beginRemoval = func(context.Context, localRemoval) error { return nil }
	controller.loadRuntimeRemoval = func(context.Context, string) (runtimeRemovalRecord, bool, error) { return record, true, nil }
	controller.recordRemovalFailure = func(_ context.Context, _ localRemoval, code, detail string) error {
		if record.stallDeclaredAt != nil {
			record.stallRetryAttempts++
		}
		record.failedAttempts = 1
		record.lastRefusalCode = code
		record.lastRefusalDetail = detail
		attempted := now
		record.lastAttemptedAt = &attempted
		return nil
	}
	controller.reapService = func(context.Context, string, string, []workloadrunner.RuntimeResourceManifest) (workloadrunner.ReapReceipt, error) {
		return workloadrunner.ReapReceipt{}, &ocihelper.RPCError{Code: ocihelper.CodeSessionStale, Message: "B"}
	}
	replayed := false
	controller.ackRemovalStall = func(_ context.Context, _ localRemoval, got runtimeRemovalRecord) error {
		replayed = declaredRefusalCode(got) == string(ocihelper.CodeUnauthorizedAttempt)
		return nil
	}
	controller.recordStallDeclared = func(context.Context, localRemoval) error {
		declared := controller.now()
		record.stallDeclaredAt = &declared
		return nil
	}
	directive := l1.RemovalDirective{
		JobID: removal.jobID, BoundNodeID: controller.nodeID, Kind: contract.JobKindOCI,
		RemovalGeneration: removal.generation, CleanupFence: removal.cleanupFence, RootInstanceID: removal.rootInstanceID,
	}
	failures := make(chan destinationError, 1)
	controller.enqueue(t.Context(), directive, failures)
	controller.wait()
	now = now.Add(declaredRemovalRetryBase)
	controller.enqueue(t.Context(), directive, failures)
	controller.wait()
	if !replayed || record.stallDeclaredAt == nil || len(failures) != 0 {
		t.Fatalf("frozen A acceptance under current B = replayed:%t declared:%t failures:%d",
			replayed, record.stallDeclaredAt != nil, len(failures))
	}
	if len(logs) != 2 || !strings.Contains(logs[0], "removal declared stalled") ||
		strings.Count(strings.Join(logs, "\n"), string(ocihelper.CodeSessionStale)) != 1 {
		t.Fatalf("frozen A/current B logs = %v, want declaration A and exactly one B", logs)
	}
}

func TestStallAcknowledgementSessionReplacementReachesEnqueueFence(t *testing.T) {
	prepared := time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC)
	removal := testRuntimeRemoval("ack-session-replaced")
	record := stallRecord(prepared, l1.MinimumServiceRemovalStallAttempts-1, string(ocihelper.CodeUnauthorizedAttempt))
	record.removal = removal
	record.manifest = runtimeRemovalManifest{Version: 1, JobID: removal.jobID, RemovalGeneration: removal.generation,
		Attempts: []workloadrunner.RuntimeResourceManifest{testRuntimeResourceManifest(removal.jobID, "attempt")}}
	controller := &removalController{
		nodeID: "node", bootSessionID: "boot", stallBound: l1.DefaultRemovalStallBound,
		managed: &recordingResumeResource{resume: func() {}}, outbox: &evidenceOutbox{},
		inflight: make(map[string]struct{}),
		now:      func() time.Time { return prepared.Add(l1.DefaultRemovalStallBound + time.Minute) },
	}
	controller.beginRemoval = func(context.Context, localRemoval) error { return nil }
	controller.loadRuntimeRemoval = func(context.Context, string) (runtimeRemovalRecord, bool, error) {
		return record, true, nil
	}
	controller.reapService = func(context.Context, string, string, []workloadrunner.RuntimeResourceManifest) (workloadrunner.ReapReceipt, error) {
		return workloadrunner.ReapReceipt{}, wedgeRefusal()
	}
	controller.recordRemovalFailure = func(_ context.Context, _ localRemoval, code, detail string) error {
		record.failedAttempts++
		record.lastRefusalCode = code
		record.lastRefusalDetail = detail
		attempted := controller.now()
		record.lastAttemptedAt = &attempted
		return nil
	}
	controller.removalStartedAt = func(context.Context, string) (time.Time, error) { return prepared, nil }
	controller.ackRemovalStall = func(context.Context, localRemoval, runtimeRemovalRecord) error {
		return &ProtocolError{APIError: contract.APIError{
			Code: contract.ErrorNodeSessionReplaced, Message: "node boot session has been replaced",
		}}
	}
	controller.recordStallDeclared = func(context.Context, localRemoval) error {
		t.Fatal("a rejected stall acknowledgement was marked declared")
		return nil
	}
	failures := make(chan destinationError, 1)
	controller.enqueue(t.Context(), l1.RemovalDirective{
		JobID: removal.jobID, BoundNodeID: controller.nodeID, Kind: removal.kind,
		RemovalGeneration: removal.generation, CleanupFence: removal.cleanupFence,
		RootInstanceID: removal.rootInstanceID,
	}, failures)
	controller.wait()
	select {
	case failure := <-failures:
		var protocolErr *ProtocolError
		if failure.destination != errorDestinationNodeSession || !errors.As(failure.err, &protocolErr) ||
			protocolErr.APIError.Code != contract.ErrorNodeSessionReplaced {
			t.Fatalf("enqueue failure = %+v", failure)
		}
	default:
		t.Fatal("stall acknowledgement session replacement never reached the enqueue fence")
	}
}

// TestTransientBackupCopyRefusalRetriesInsteadOfStalling is the other half of
// sharing one streak: a Backup-copy deletion that fails once and then succeeds
// is a removal still working through a cause, not a wedge. The bound is
// already elapsed here, so only the streak length separates the two -- exactly
// as it does for a refused runtime reap.
func TestTransientBackupCopyRefusalRetriesInsteadOfStalling(t *testing.T) {
	spool := openTestLogSpool(t, t.TempDir(), "transient-backup-node", 1024)
	defer spool.Close()
	removal := testRuntimeRemoval("transient-backup-job")
	prepared := time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC)
	manifest := testRuntimeResourceManifest(removal.jobID, "attempt")
	if err := spool.storeRuntimeResourceManifest(t.Context(), manifest, prepared); err != nil {
		t.Fatal(err)
	}
	if err := spool.beginRemoval(t.Context(), removal, prepared); err != nil {
		t.Fatal(err)
	}

	now := prepared.Add(l1.DefaultRemovalStallBound + time.Minute)
	deletions := 0
	acknowledged := 0
	controller := &removalController{nodeID: "transient-backup-node", bootSessionID: "boot",
		stallBound: l1.DefaultRemovalStallBound, now: func() time.Time { return now }, logf: t.Logf}
	controller.beginRemoval = func(context.Context, localRemoval) error { return nil }
	controller.loadRuntimeRemoval = spool.runtimeRemoval
	controller.removalStartedAt = spool.removalStartedAt
	controller.recordRemovalFailure = func(ctx context.Context, target localRemoval, code, detail string) error {
		return spool.recordRuntimeRemovalFailure(ctx, target, code, detail, controller.bootSessionID, now)
	}
	controller.recordUntypedFailure = func(ctx context.Context, target localRemoval) error {
		return spool.recordRuntimeRemovalUntypedFailure(ctx, target, controller.bootSessionID, now)
	}
	controller.freezeStall = spool.freezeRuntimeRemovalStallDeclaration
	controller.recordStallDeclared = func(ctx context.Context, target localRemoval) error {
		return spool.recordRuntimeRemovalStallDeclared(ctx, target, now)
	}
	controller.ackRemovalStall = func(context.Context, localRemoval, runtimeRemovalRecord) error {
		t.Fatal("one transient Backup-copy refusal declared the removal stalled")
		return nil
	}
	controller.removeBackupCopies = func(context.Context, []l1.ComputerBackupPruneDirective) error {
		deletions++
		if deletions == 1 {
			return &ocihelper.RPCError{Code: ocihelper.CodeEngineFailure, Message: "Backup copy busy"}
		}
		return nil
	}
	controller.reapService = func(context.Context, string, string, []workloadrunner.RuntimeResourceManifest) (workloadrunner.ReapReceipt, error) {
		return workloadrunner.ReapReceipt{RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidenceAttempt,
			BootSessionID: controller.bootSessionID}, nil
	}
	controller.recordRuntimeQuiesced = func(ctx context.Context, target localRemoval, receipt workloadrunner.ReapReceipt) error {
		return spool.recordRuntimeQuiesced(ctx, target, receipt, now)
	}
	controller.recordRuntimeAttested = func(ctx context.Context, target localRemoval, attestation workloadrunner.RuntimeRemovalAttestation) error {
		return spool.recordRuntimeAttested(ctx, target, attestation, now)
	}
	controller.purgeJob = func(context.Context, string) error { return nil }
	controller.removeResource = func(context.Context, localRemoval) error { return nil }
	controller.deleteRuntimeData = func(context.Context, workloadrunner.RuntimeRemovalProofRequest) error { return nil }
	controller.attestRuntimeRemoval = func(_ context.Context, request workloadrunner.RuntimeRemovalProofRequest) (workloadrunner.RuntimeRemovalAttestation, error) {
		return testRuntimeRemovalAttestation(runtimeRemovalManifest{Version: 1, JobID: request.JobID,
			RemovalGeneration: request.RemovalGeneration, Attempts: request.Attempts}), nil
	}
	controller.ackRemoval = func(context.Context, localRemoval) error { acknowledged++; return nil }
	controller.finishRemoval = func(ctx context.Context, target localRemoval) error {
		return spool.completeRemoval(ctx, target)
	}
	directive := l1.RemovalDirective{JobID: removal.jobID, BoundNodeID: controller.nodeID, Kind: removal.kind,
		RemovalGeneration: removal.generation, CleanupFence: removal.cleanupFence, RootInstanceID: removal.rootInstanceID,
		ComputerBackupCopies: &l1.ComputerBackupCopyClaims{Copies: []l1.ComputerBackupPruneDirective{{
			BackupID: "backup", CopyID: "copy", ComputerID: "computer", StorageID: "storage",
			StorageGeneration: 1, BoundNodeID: controller.nodeID, RootInstanceID: removal.rootInstanceID,
			OperationRevision: 1, CleanupFence: removal.cleanupFence,
		}}},
	}

	if err := controller.reconcile(t.Context(), directive); err == nil {
		t.Fatal("the refused Backup-copy deletion was reported as a success")
	}
	// The refusal was counted, not swallowed: that is what would have let a
	// repeat of it end the removal.
	refused, found, err := spool.runtimeRemoval(t.Context(), removal.jobID)
	if err != nil || !found || refused.failedAttempts != 1 ||
		refused.lastRefusalCode != string(ocihelper.CodeEngineFailure) {
		t.Fatalf("durable record after one Backup-copy refusal = %+v found=%t err=%v", refused, found, err)
	}
	now = now.Add(15 * time.Second)
	if err := controller.reconcile(t.Context(), directive); err != nil {
		t.Fatalf("the retried Backup-copy deletion did not finish the removal: %v", err)
	}
	if deletions != 2 || acknowledged != 1 {
		t.Fatalf("Backup-copy deletions = %d, acknowledgements = %d, want 2 and 1", deletions, acknowledged)
	}
	if _, found, err := spool.runtimeRemoval(t.Context(), removal.jobID); err != nil || found {
		t.Fatalf("completed removal kept its durable record: found=%t err=%v", found, err)
	}
}
