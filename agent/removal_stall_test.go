package agent

import (
	"context"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
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
		return spool.recordRuntimeRemovalFailure(ctx, target, code, detail, now)
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
		controller.noteRemovalFailure(t.Context(), directive, wedgeRefusal())
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
			if got := controller.removalIsStalled(testCase.record); got != testCase.declare {
				t.Fatalf("removalIsStalled = %t, want %t", got, testCase.declare)
			}
		})
	}
}

// TestAnUntypedRemovalFailureNeverBecomesAStall keeps a transport hiccup from
// buying a permanent unverified outcome.
func TestAnUntypedRemovalFailureNeverBecomesAStall(t *testing.T) {
	controller := &removalController{nodeID: "node"}
	recorded := 0
	controller.recordRemovalFailure = func(context.Context, localRemoval, string, string) error {
		recorded++
		return nil
	}
	controller.loadRuntimeRemoval = func(context.Context, string) (runtimeRemovalRecord, bool, error) {
		t.Fatal("an untyped failure must not even be weighed against the bound")
		return runtimeRemovalRecord{}, false, nil
	}
	controller.noteRemovalFailure(t.Context(), l1.RemovalDirective{JobID: "job", BoundNodeID: "node"},
		context.DeadlineExceeded)
	if recorded != 0 {
		t.Fatalf("untyped failures recorded = %d, want 0", recorded)
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
		if err := spool.recordRuntimeRemovalFailure(t.Context(), removal, code, "detail",
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
