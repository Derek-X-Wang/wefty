package l1

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

// failureReporter sends Storage operation failure reports for one directive,
// minting a fresh report_id per attempt as the agent does.
type failureReporter struct {
	t        *testing.T
	store    *Store
	node     Node
	computer string
	base     ComputerStorageOperationFailureRequest
	next     int
}

func newFailureReporter(t *testing.T, h *integrationHarness, node Node, computerID string, verb ComputerStorageOperationVerb,
	revision int64, stage ComputerStorageOperationStage, fence string) *failureReporter {
	return &failureReporter{t: t, store: h.store, node: node, computer: computerID, base: ComputerStorageOperationFailureRequest{
		NodeID: node.NodeID, BootSessionID: node.BootSessionID, Verb: verb, OperationRevision: revision, Stage: stage,
		Fence: fence, Code: "helper_call", HelperGeneration: 7}}
}

func (r *failureReporter) request(class ComputerStorageOperationFailureClass) ComputerStorageOperationFailureRequest {
	r.next++
	request := r.base
	request.ReportID = fmt.Sprintf("%032x", r.next)
	request.Class = class
	return request
}

func (r *failureReporter) send(request ComputerStorageOperationFailureRequest) (ComputerStorageOperationFailureResponse, error) {
	return r.store.ReportComputerStorageOperationFailure(context.Background(), "fabric-computer-node", r.computer, request)
}

// report sends one new failed attempt and requires the disposition.
func (r *failureReporter) report(class ComputerStorageOperationFailureClass, want ComputerStorageOperationFailureDisposition) ComputerStorageOperationFailureResponse {
	r.t.Helper()
	response, err := r.send(r.request(class))
	if err != nil || response.Disposition != want {
		r.t.Fatalf("report %d (%s) = %#v err=%v, want %s", r.next, class, response, err, want)
	}
	return response
}

// spend reports engine failures until the budget is spent: four in the first
// minute, then one more after the window. It returns the exhausting answer.
func (r *failureReporter) spend(h *integrationHarness) ComputerStorageOperationFailureResponse {
	r.t.Helper()
	for range MaximumStorageOperationFailures - 1 {
		r.report(ComputerStorageOperationFailureEngine, ComputerStorageOperationFailureCounted)
	}
	h.clock.Advance(StorageOperationFailureWindow)
	return r.report(ComputerStorageOperationFailureEngine, ComputerStorageOperationFailureExhausted)
}

func latchedFailureCode(t *testing.T, h *integrationHarness, jobID string) contract.SpawnFailureCode {
	t.Helper()
	var payload []byte
	if err := h.store.db.QueryRow(`SELECT last_failure FROM service_jobs WHERE job_id=?`, jobID).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	var failure contract.SpawnFailure
	if len(payload) != 0 {
		if err := json.Unmarshal(payload, &failure); err != nil {
			t.Fatal(err)
		}
	}
	return failure.Code
}

func beginRevokedRestore(t *testing.T, h *integrationHarness, node Node, computer Computer, backup Backup, key string) ComputerStorageCopyDirective {
	t.Helper()
	reserved, _, err := h.store.BeginComputerRestore(context.Background(), computer.ComputerID,
		ComputerRestoreRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"),
			BackupID: backup.BackupID, KeepOldBackup: true, IdempotencyKey: key})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.store.RecordComputerRestoreAuthorityRevoked(context.Background(), computer.ComputerID, reserved.IntentRevision,
		testRestoreRevocationEvidence(computer.ComputerID, reserved.IntentRevision)); err != nil {
		t.Fatal(err)
	}
	directives, err := h.store.ListNodeComputerStorageCopyDirectives(context.Background(), "fabric-computer-node", node.NodeID, node.BootSessionID)
	if err != nil || len(directives) != 1 || directives[0].OperationRevision != reserved.IntentRevision {
		t.Fatalf("restore directives = %#v err=%v", directives, err)
	}
	return directives[0]
}

func storageCopyDirectiveCount(t *testing.T, h *integrationHarness, node Node) int {
	t.Helper()
	directives, err := h.store.ListNodeComputerStorageCopyDirectives(context.Background(), "fabric-computer-node", node.NodeID, node.BootSessionID)
	if err != nil {
		t.Fatal(err)
	}
	return len(directives)
}

// The one rule (#1608), driven through a restore before publication: the
// budget needs both the count and the window on the L1 clock, survives an L1
// restart, answers a retried report without counting it twice, refuses a
// foreign Node or fence, and ignores a report it no longer owns. Once spent,
// the restore records `failed` and the Computer returns to `stable` on its
// old generation, and removal still plans the uncataloged predecessor copy.
func TestStorageOperationFailureBudgetEndsRestoreBeforePublication(t *testing.T) {
	ctx := context.Background()
	h, node, computer, backup, _ := publishedBackupForStorageCopy(t, 3)
	directive := beginRevokedRestore(t, h, node, computer, backup, "budget-restore")
	reporter := newFailureReporter(t, h, node, computer.ComputerID, ComputerStorageOperationRestore,
		directive.OperationRevision, ComputerStorageOperationPrepare, directive.CleanupFence)

	// Malformed reports are refused before any authority is read.
	for name, mutate := range map[string]func(*ComputerStorageOperationFailureRequest){
		"report id": func(r *ComputerStorageOperationFailureRequest) { r.ReportID = "not-hex" },
		"class":     func(r *ComputerStorageOperationFailureRequest) { r.Class = "unknown" },
		"code":      func(r *ComputerStorageOperationFailureRequest) { r.Code = "Has Spaces" },
		"stage":     func(r *ComputerStorageOperationFailureRequest) { r.Stage = "publish" },
		"verb stage": func(r *ComputerStorageOperationFailureRequest) {
			r.Verb = ComputerStorageOperationBackup
			r.Stage = ComputerStorageOperationRetire
		},
		"long detail": func(r *ComputerStorageOperationFailureRequest) {
			r.Detail = string(make([]byte, MaximumStorageOperationFailureDetail+1))
		},
		"outcome on grow": func(r *ComputerStorageOperationFailureRequest) {
			r.Verb, r.PreparationOutcome = ComputerStorageOperationGrow, &ComputerStoragePreparationOutcome{}
		},
	} {
		request := reporter.request(ComputerStorageOperationFailureEngine)
		mutate(&request)
		if _, err := reporter.send(request); errorCode(err) != contract.ErrorInvalidRequest {
			t.Fatalf("%s: error = %v, want %q", name, err, contract.ErrorInvalidRequest)
		}
	}
	// A foreign Node and a foreign fence are refused and count nothing.
	otherRegistration := node.NodeRegistration
	otherRegistration.NodeID, otherRegistration.BootSessionID, otherRegistration.RootInstanceID = "other-node", "other-boot", "other-root"
	other, err := h.store.RegisterNode(ctx, fabric.Identity{NodeID: "fabric-computer-node"}, otherRegistration,
		NodePolicy{MaxOneshotSlots: 1, MaxServiceSlots: 1}, false)
	if err != nil {
		t.Fatal(err)
	}
	foreignNode := reporter.request(ComputerStorageOperationFailureEngine)
	foreignNode.NodeID, foreignNode.BootSessionID = other.NodeID, other.BootSessionID
	if _, err := reporter.send(foreignNode); errorCode(err) != contract.ErrorAttemptNotOwned {
		t.Fatalf("foreign node error = %v", err)
	}
	foreignFence := reporter.request(ComputerStorageOperationFailureEngine)
	foreignFence.Fence = "other-fence"
	if _, err := reporter.send(foreignFence); errorCode(err) != contract.ErrorStaleFence {
		t.Fatalf("foreign fence error = %v", err)
	}
	// The retire stage of a restore that has not published is not running.
	retire := reporter.request(ComputerStorageOperationFailureEngine)
	retire.Stage = ComputerStorageOperationRetire
	if response, err := reporter.send(retire); err != nil || response.Disposition != ComputerStorageOperationFailureSuperseded {
		t.Fatalf("retire report before publication = %#v err=%v", response, err)
	}

	// A deferral is recorded, not counted.
	if deferred := reporter.report(ComputerStorageOperationFailureResumeDeferred, ComputerStorageOperationFailureRecorded); deferred.FailedAttempts != 0 ||
		deferred.FirstFailedAt != nil || deferred.OperationStatus != "reserved" {
		t.Fatalf("deferral answer = %#v", deferred)
	}
	first := reporter.report(ComputerStorageOperationFailureEngine, ComputerStorageOperationFailureCounted)
	if first.FailedAttempts != 1 || first.FirstFailedAt == nil || !first.FirstFailedAt.Equal(h.clock.Now()) || first.RetryAfterSeconds != 15 {
		t.Fatalf("first counted answer = %#v", first)
	}
	// A retried report is answered from the durable row and not counted; the
	// same report_id with another body is refused.
	replay := reporter.base
	replay.ReportID, replay.Class = fmt.Sprintf("%032x", reporter.next), ComputerStorageOperationFailureEngine
	if replayed, err := reporter.send(replay); err != nil || replayed.Disposition != ComputerStorageOperationFailureReplayed || replayed.FailedAttempts != 1 {
		t.Fatalf("replayed answer = %#v err=%v", replayed, err)
	}
	replay.Code = "another_call"
	if _, err := reporter.send(replay); errorCode(err) != contract.ErrorIdempotencyConflict {
		t.Fatalf("changed replay error = %v", err)
	}
	// The count survives an L1 restart: a second store on the same database
	// continues it.
	var path string
	var sequence int
	var name string
	if err := h.store.db.QueryRow("PRAGMA database_list").Scan(&sequence, &name, &path); err != nil {
		t.Fatal(err)
	}
	resumed, err := openTestStore(t, path, StoreOptions{Clock: h.clock, LeaseDuration: 3 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer resumed.Close()
	reporter.store = resumed
	for want := int64(2); want <= MaximumStorageOperationFailures; want++ {
		h.clock.Advance(time.Minute)
		if counted := reporter.report(ComputerStorageOperationFailureEngine, ComputerStorageOperationFailureCounted); counted.FailedAttempts != want {
			t.Fatalf("counted answer = %#v, want %d attempts", counted, want)
		}
	}
	// N is reached before T: the operation still runs until the window from
	// the first counted failure has passed.
	if storageCopyDirectiveCount(t, h, node) != 1 {
		t.Fatal("restore ended before its window")
	}
	h.clock.Advance(StorageOperationFailureWindow - 4*time.Minute - time.Second)
	reporter.report(ComputerStorageOperationFailureRuntimeLoss, ComputerStorageOperationFailureCounted)
	h.clock.Advance(time.Second)
	exhausted := reporter.report(ComputerStorageOperationFailureRuntimeLoss, ComputerStorageOperationFailureExhausted)
	if exhausted.OperationStatus != "failed" || exhausted.FailedAttempts != MaximumStorageOperationFailures+2 || exhausted.RetryAfterSeconds != 0 {
		t.Fatalf("exhausted answer = %#v", exhausted)
	}
	reporter.store = h.store
	ended, err := h.store.GetComputer(ctx, computer.ComputerID)
	if err != nil || ended.ReconfigurationPhase != ComputerReconfigurationStable || ended.StorageGeneration != directive.OldGeneration ||
		ended.AppliedRevision != directive.OperationRevision {
		t.Fatalf("ended restore left Computer = %#v err=%v", ended, err)
	}
	operation, err := h.store.ComputerRestoreOperation(ctx, computer.ComputerID, directive.OperationRevision)
	if err != nil || operation.Status != "failed" || operation.FailureCode != ComputerRestoreFailureRuntimeLoss || operation.Failure == nil ||
		operation.Failure.Class != ComputerStorageOperationFailureRuntimeLoss || operation.Failure.FailedAttempts != MaximumStorageOperationFailures+2 ||
		operation.Failure.Stage != ComputerStorageOperationPrepare {
		t.Fatalf("ended restore operation = %#v err=%v", operation, err)
	}
	if storageCopyDirectiveCount(t, h, node) != 0 {
		t.Fatal("ended restore is still listed")
	}
	// The exhausting report replays; a new report changes nothing.
	exhausting := reporter.base
	exhausting.ReportID, exhausting.Class = fmt.Sprintf("%032x", reporter.next), ComputerStorageOperationFailureRuntimeLoss
	if replayed, err := reporter.send(exhausting); err != nil || replayed.Disposition != ComputerStorageOperationFailureReplayed ||
		replayed.OperationStatus != "failed" {
		t.Fatalf("exhausting replay = %#v err=%v", replayed, err)
	}
	if late := reporter.report(ComputerStorageOperationFailureEngine, ComputerStorageOperationFailureSuperseded); late.OperationStatus != "failed" {
		t.Fatalf("late report = %#v", late)
	}

	// Removal plans the uncataloged predecessor copy of the budget-ended
	// restore and accepts its absence.
	if _, err := h.store.RemoveComputer(ctx, computer.ComputerID,
		ComputerRemoveRequest{ComputerMutationPrecondition: computerPrecondition(ended, "operator-remove")}); err != nil {
		t.Fatal(err)
	}
	removals, err := h.store.ListNodeRemovalDirectives(ctx, "fabric-computer-node", node.NodeID, node.BootSessionID)
	if err != nil || len(removals) != 1 || removals[0].ComputerBackupCopies == nil {
		t.Fatalf("removal directive = %#v err=%v", removals, err)
	}
	var precommit *ComputerBackupPruneDirective
	for index := range removals[0].ComputerBackupCopies.Copies {
		if removals[0].ComputerBackupCopies.Copies[index].CopyID == directive.OldCopyID {
			precommit = &removals[0].ComputerBackupCopies.Copies[index]
		}
	}
	if precommit == nil {
		t.Fatalf("removal omitted the predecessor copy: %#v", removals[0].ComputerBackupCopies.Copies)
	}
	if _, err := h.store.AcknowledgeComputerBackupPrune(ctx, "fabric-computer-node", computer.ComputerID,
		ComputerBackupPruneAcknowledgementRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID,
			IdempotencyKey: "precommit-removed", Receipt: backupRemovalReceipt(*precommit)}); err != nil {
		t.Fatalf("predecessor copy absence: %v", err)
	}
}

// A restore whose predecessor copy failed by receipt already proved that copy
// absent, so removal does not plan it again (a deliberate narrowing of the
// design's "any failed restore" predicate).
func TestRemovalSkipsPredecessorCopyProvenAbsentByReceipt(t *testing.T) {
	for _, code := range []ComputerRestoreFailureCode{ComputerRestoreFailureInsufficientDisk, ComputerRestoreFailureDigestMismatch,
		ComputerRestoreFailureSourceNeverDetached} {
		if restorePredecessorMayBeUncataloged("failed", string(code)) {
			t.Fatalf("%s restore plans a proven-absent predecessor copy", code)
		}
	}
	for _, code := range []ComputerRestoreFailureCode{ComputerRestoreFailureStorageCopyEngineFailure, ComputerRestoreFailureEngine,
		ComputerRestoreFailureQuarantined} {
		if !restorePredecessorMayBeUncataloged("failed", string(code)) {
			t.Fatalf("%s restore does not plan its predecessor copy", code)
		}
	}
	if !restorePredecessorMayBeUncataloged("superseded", "") || restorePredecessorMayBeUncataloged("retired", "") ||
		restorePredecessorMayBeUncataloged("cleanup_owed", string(ComputerRestoreFailureEngine)) {
		t.Fatal("restore predecessor predicate misclassifies a status")
	}
	ctx := context.Background()
	h, node, computer, backup, _ := publishedBackupForStorageCopy(t, 3)
	directive := beginRevokedRestore(t, h, node, computer, backup, "receipt-failed")
	receipt := successfulOldGenerationBackupReceipt(directive)
	receipt.Kind, receipt.FailureCode, receipt.CopyAbsent, receipt.ContentDigest = computerBackupFailureReceiptKind,
		string(ComputerBackupFailureInsufficientDisk), true, ""
	if _, err := h.store.AcknowledgeComputerStorageCopy(ctx, "fabric-computer-node", computer.ComputerID,
		ComputerStorageCopyAcknowledgementRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID,
			IdempotencyKey: "predecessor-failed", Receipt: successfulStorageCopyReceipt(directive), OldBackupReceipt: &receipt}); errorCode(err) != contract.ErrorConflict {
		t.Fatalf("predecessor failure receipt = %v", err)
	}
	stable, err := h.store.GetComputer(ctx, computer.ComputerID)
	if err != nil || stable.ReconfigurationPhase != ComputerReconfigurationStable {
		t.Fatalf("receipt-failed restore left Computer = %#v err=%v", stable, err)
	}
	if _, err := h.store.RemoveComputer(ctx, computer.ComputerID,
		ComputerRemoveRequest{ComputerMutationPrecondition: computerPrecondition(stable, "operator-remove")}); err != nil {
		t.Fatal(err)
	}
	removals, err := h.store.ListNodeRemovalDirectives(ctx, "fabric-computer-node", node.NodeID, node.BootSessionID)
	if err != nil || len(removals) != 1 || removals[0].ComputerBackupCopies == nil {
		t.Fatalf("removal directive = %#v err=%v", removals, err)
	}
	for _, copy := range removals[0].ComputerBackupCopies.Copies {
		if copy.CopyID == directive.OldCopyID {
			t.Fatalf("removal plans a predecessor copy its receipt proved absent: %#v", copy)
		}
	}
}

// After publication a restore that cannot delete its predecessor ends
// `cleanup_owed`: the Computer is stable on the new generation, held stopped
// with the failure latched, and the next restore is admitted.
func TestStorageOperationFailureBudgetEndsPublishedRestoreCleanupOwed(t *testing.T) {
	ctx := context.Background()
	h, node, computer, backup, _ := publishedBackupForStorageCopy(t, 3)
	directive := beginRevokedRestore(t, h, node, computer, backup, "owed-restore")
	receipt := successfulStorageCopyReceipt(directive)
	oldReceipt := successfulOldGenerationBackupReceipt(directive)
	if _, err := h.store.AcknowledgeComputerStorageCopy(ctx, "fabric-computer-node", computer.ComputerID,
		ComputerStorageCopyAcknowledgementRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID,
			IdempotencyKey: receipt.ReceiptID, Receipt: receipt, OldBackupReceipt: &oldReceipt}); err != nil {
		t.Fatal(err)
	}
	reporter := newFailureReporter(t, h, node, computer.ComputerID, ComputerStorageOperationRestore,
		directive.OperationRevision, ComputerStorageOperationRetire, directive.CleanupFence)
	// The prepare stage no longer runs.
	prepare := reporter.request(ComputerStorageOperationFailureEngine)
	prepare.Stage = ComputerStorageOperationPrepare
	if response, err := reporter.send(prepare); err != nil || response.Disposition != ComputerStorageOperationFailureSuperseded {
		t.Fatalf("prepare report after publication = %#v err=%v", response, err)
	}
	if exhausted := reporter.spend(h); exhausted.OperationStatus != "cleanup_owed" {
		t.Fatalf("exhausted answer = %#v", exhausted)
	}
	ended, err := h.store.GetComputer(ctx, computer.ComputerID)
	if err != nil || ended.ReconfigurationPhase != ComputerReconfigurationStable || ended.StorageGeneration != directive.DestinationGeneration ||
		ended.AppliedRevision != directive.OperationRevision || ended.CurrentJob.State != contract.JobStopped {
		t.Fatalf("cleanup-owed restore left Computer = %#v err=%v", ended, err)
	}
	if code := latchedFailureCode(t, h, ended.CurrentJobID); code != contract.SpawnFailureComputerStorageOperationFailed {
		t.Fatalf("latched failure = %q", code)
	}
	operation, err := h.store.ComputerRestoreOperation(ctx, computer.ComputerID, directive.OperationRevision)
	if err != nil || operation.Status != "cleanup_owed" || operation.FailureCode != ComputerRestoreFailureEngine ||
		operation.Failure == nil || operation.Failure.Stage != ComputerStorageOperationRetire || operation.CompletedAt == nil {
		t.Fatalf("cleanup-owed restore operation = %#v err=%v", operation, err)
	}
	retirements, err := h.store.ListNodeComputerStorageCopyDirectives(ctx, "fabric-computer-node", node.NodeID, node.BootSessionID)
	if err != nil || len(retirements) != 0 {
		t.Fatalf("cleanup-owed restore still dispatches = %#v err=%v", retirements, err)
	}
	beginRevokedRestore(t, h, node, ended, backup, "after-owed")
}

func beginStoppedReset(t *testing.T, h *integrationHarness, node Node, computer Computer, key string) ComputerStorageResetDirective {
	t.Helper()
	if _, _, err := h.store.BeginComputerStorageReset(context.Background(), computer.ComputerID,
		ComputerStorageResetRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"), IdempotencyKey: key}); err != nil {
		t.Fatal(err)
	}
	directives, err := h.store.ListNodeComputerStorageResetDirectives(context.Background(), "fabric-computer-node", node.NodeID, node.BootSessionID)
	if err != nil || len(directives) != 1 {
		t.Fatalf("reset directives = %#v err=%v", directives, err)
	}
	return directives[0]
}

// Owner decision D1: until a fenced reset can heal (#1609), a reset is never
// ended before publication, whatever the class -- quarantine included. Its
// failures are counted and shown. With the switch on, the reset ends `failed`
// on the old generation and the next reset is admitted, which needs the
// rebuilt active-reset index.
func TestStorageOperationFailureBudgetCountsButKeepsResetBeforePublication(t *testing.T) {
	ctx := context.Background()
	h, node, computer, _, _ := publishedBackupForStorageCopy(t, 3)
	directive := beginStoppedReset(t, h, node, computer, "counted-reset")
	reporter := newFailureReporter(t, h, node, computer.ComputerID, ComputerStorageOperationReset,
		directive.IntentRevision, ComputerStorageOperationPrepare, directive.CleanupFence)
	for range MaximumStorageOperationFailures + 2 {
		h.clock.Advance(3 * time.Minute)
		reporter.report(ComputerStorageOperationFailureEngine, ComputerStorageOperationFailureCounted)
	}
	reporter.report(ComputerStorageOperationFailureQuarantined, ComputerStorageOperationFailureCounted)
	resetting, err := h.store.GetComputer(ctx, computer.ComputerID)
	if err != nil || resetting.ReconfigurationPhase != ComputerReconfigurationResetting || resetting.LastResetOperation == nil ||
		resetting.LastResetOperation.Status != "reserved" || resetting.LastResetOperation.Failure == nil ||
		resetting.LastResetOperation.Failure.FailedAttempts != MaximumStorageOperationFailures+3 ||
		resetting.LastResetOperation.Failure.Class != ComputerStorageOperationFailureQuarantined {
		t.Fatalf("counted reset = %#v err=%v", resetting.LastResetOperation, err)
	}
	if directives, err := h.store.ListNodeComputerStorageResetDirectives(ctx, "fabric-computer-node", node.NodeID, node.BootSessionID); err != nil || len(directives) != 1 {
		t.Fatalf("counted reset no longer dispatches = %#v err=%v", directives, err)
	}

	storageResetPrepareEndsByBudget = true
	t.Cleanup(func() { storageResetPrepareEndsByBudget = false })
	if exhausted := reporter.report(ComputerStorageOperationFailureEngine, ComputerStorageOperationFailureExhausted); exhausted.OperationStatus != "failed" {
		t.Fatalf("exhausted reset = %#v", exhausted)
	}
	ended, err := h.store.GetComputer(ctx, computer.ComputerID)
	if err != nil || ended.ReconfigurationPhase != ComputerReconfigurationStable || ended.StorageGeneration != directive.OldGeneration ||
		ended.LastResetOperation == nil || ended.LastResetOperation.Status != "failed" || ended.LastResetOperation.FailureCode != "engine_failure" ||
		ended.LastResetOperation.CompletedAt == nil {
		t.Fatalf("failed reset left Computer = %#v err=%v", ended, err)
	}
	if code := latchedFailureCode(t, h, ended.CurrentJobID); code != contract.SpawnFailureComputerStorageOperationFailed {
		t.Fatalf("latched failure = %q", code)
	}
	next := beginStoppedReset(t, h, node, ended, "after-failed-reset")
	if next.NewGeneration != directive.NewGeneration+1 {
		t.Fatalf("next reset generation = %d after %d", next.NewGeneration, directive.NewGeneration)
	}
}

// A reset that published but cannot delete its predecessor ends
// `cleanup_owed` on the new generation, held stopped.
func TestStorageOperationFailureBudgetEndsPublishedResetCleanupOwed(t *testing.T) {
	ctx := context.Background()
	h, node, computer, _, _ := publishedBackupForStorageCopy(t, 3)
	directive := beginStoppedReset(t, h, node, computer, "owed-reset")
	receipt := resetReceipt(directive)
	if _, err := h.store.AcknowledgeComputerStorageReset(ctx, "fabric-computer-node", computer.ComputerID,
		ComputerStorageResetAcknowledgementRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID,
			IdempotencyKey: receipt.ReceiptID, Receipt: receipt}); err != nil {
		t.Fatal(err)
	}
	reporter := newFailureReporter(t, h, node, computer.ComputerID, ComputerStorageOperationReset,
		directive.IntentRevision, ComputerStorageOperationRetire, directive.CleanupFence)
	if exhausted := reporter.spend(h); exhausted.OperationStatus != "cleanup_owed" {
		t.Fatalf("exhausted answer = %#v", exhausted)
	}
	ended, err := h.store.GetComputer(ctx, computer.ComputerID)
	if err != nil || ended.ReconfigurationPhase != ComputerReconfigurationStable || ended.StorageGeneration != directive.NewGeneration ||
		ended.CurrentJob.State != contract.JobStopped || ended.LastResetOperation == nil || ended.LastResetOperation.Status != "cleanup_owed" ||
		ended.LastResetOperation.Failure == nil || ended.LastResetOperation.Failure.Stage != ComputerStorageOperationRetire {
		t.Fatalf("cleanup-owed reset left Computer = %#v err=%v", ended, err)
	}
	if directives, err := h.store.ListNodeComputerStorageResetDirectives(ctx, "fabric-computer-node", node.NodeID, node.BootSessionID); err != nil || len(directives) != 0 {
		t.Fatalf("cleanup-owed reset still dispatches = %#v err=%v", directives, err)
	}
	beginStoppedReset(t, h, node, ended, "after-owed-reset")
}

// A Backup that spends its budget ends `failed` and releases the Computer;
// its planned copy is owed to removal, which now plans it and accepts its
// absence (the prune acknowledgement accepts a failed row with no receipt).
func TestStorageOperationFailureBudgetEndsBackupAndRemovalPlansItsCopy(t *testing.T) {
	ctx := context.Background()
	h, node, computer, _, _ := publishedBackupForStorageCopy(t, 3)
	if _, _, err := h.store.BeginComputerBackup(ctx, computer.ComputerID, ComputerBackupCreateRequest{
		ComputerMutationPrecondition: computerPrecondition(computer, "operator"), IdempotencyKey: "budget-backup"}); err != nil {
		t.Fatal(err)
	}
	directives, err := h.store.ListNodeComputerBackupDirectives(ctx, "fabric-computer-node", node.NodeID, node.BootSessionID)
	if err != nil || len(directives) != 1 {
		t.Fatalf("Backup directives = %#v err=%v", directives, err)
	}
	directive := directives[0]
	reporter := newFailureReporter(t, h, node, computer.ComputerID, ComputerStorageOperationBackup,
		directive.OperationRevision, ComputerStorageOperationPrepare, directive.CleanupFence)
	// T before N: the window passes first, then the count completes.
	reporter.report(ComputerStorageOperationFailureAgentRefused, ComputerStorageOperationFailureCounted)
	h.clock.Advance(StorageOperationFailureWindow + time.Minute)
	for range MaximumStorageOperationFailures - 2 {
		reporter.report(ComputerStorageOperationFailureAgentRefused, ComputerStorageOperationFailureCounted)
	}
	if exhausted := reporter.report(ComputerStorageOperationFailureAgentRefused, ComputerStorageOperationFailureExhausted); exhausted.OperationStatus != "failed" {
		t.Fatalf("exhausted Backup = %#v", exhausted)
	}
	ended, err := h.store.GetComputer(ctx, computer.ComputerID)
	if err != nil || ended.ReconfigurationPhase != ComputerReconfigurationStable || ended.LastBackupOperation == nil ||
		ended.LastBackupOperation.Status != "failed" || ended.LastBackupOperation.FailureCode != ComputerBackupFailureAgentRefused ||
		ended.LastBackupOperation.Failure == nil || ended.LastBackupOperation.Failure.FailedAttempts != MaximumStorageOperationFailures {
		t.Fatalf("failed Backup left Computer = %#v err=%v", ended.LastBackupOperation, err)
	}
	if directives, err := h.store.ListNodeComputerBackupDirectives(ctx, "fabric-computer-node", node.NodeID, node.BootSessionID); err != nil || len(directives) != 0 {
		t.Fatalf("failed Backup still dispatches = %#v err=%v", directives, err)
	}
	if _, err := h.store.RemoveComputer(ctx, computer.ComputerID,
		ComputerRemoveRequest{ComputerMutationPrecondition: computerPrecondition(ended, "operator-remove")}); err != nil {
		t.Fatal(err)
	}
	removals, err := h.store.ListNodeRemovalDirectives(ctx, "fabric-computer-node", node.NodeID, node.BootSessionID)
	if err != nil || len(removals) != 1 || removals[0].ComputerBackupCopies == nil {
		t.Fatalf("removal directive = %#v err=%v", removals, err)
	}
	var planned *ComputerBackupPruneDirective
	for index := range removals[0].ComputerBackupCopies.Copies {
		if removals[0].ComputerBackupCopies.Copies[index].CopyID == directive.CopyID {
			planned = &removals[0].ComputerBackupCopies.Copies[index]
		}
	}
	if planned == nil || !planned.Superseded {
		t.Fatalf("removal omitted the failed Backup's copy: %#v", removals[0].ComputerBackupCopies.Copies)
	}
	if _, err := h.store.AcknowledgeComputerBackupPrune(ctx, "fabric-computer-node", computer.ComputerID,
		ComputerBackupPruneAcknowledgementRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID,
			IdempotencyKey: "failed-copy-removed", Receipt: backupRemovalReceipt(*planned)}); err != nil {
		t.Fatalf("failed Backup copy absence: %v", err)
	}
	var outstanding int
	if err := h.store.db.QueryRow(`SELECT COUNT(*) FROM computer_backup_operations WHERE computer_id=?
		AND status IN ('superseded', 'failed') AND acknowledgement_key IS NULL`, computer.ComputerID).Scan(&outstanding); err != nil || outstanding != 0 {
		t.Fatalf("outstanding failed Backup copies = %d err=%v", outstanding, err)
	}
}

// A grow that spends its budget ends `failed` with the disk budget unchanged
// and the failure latched.
func TestStorageOperationFailureBudgetEndsGrow(t *testing.T) {
	ctx := context.Background()
	h, node, computer, _, _ := publishedBackupForStorageCopy(t, 3)
	if _, _, err := h.store.BeginComputerGrow(ctx, computer.ComputerID, ComputerGrowRequest{
		ComputerMutationPrecondition: computerPrecondition(computer, "operator"), DiskBytes: computer.DesiredDiskBytes + (1 << 30),
		IdempotencyKey: "budget-grow"}); err != nil {
		t.Fatal(err)
	}
	directives, err := h.store.ListNodeComputerStorageGrowDirectives(ctx, "fabric-computer-node", node.NodeID, node.BootSessionID)
	if err != nil || len(directives) != 1 {
		t.Fatalf("grow directives = %#v err=%v", directives, err)
	}
	directive := directives[0]
	reporter := newFailureReporter(t, h, node, computer.ComputerID, ComputerStorageOperationGrow,
		directive.OperationRevision, ComputerStorageOperationPrepare, directive.OperationFence)
	reporter.spend(h)
	ended, err := h.store.GetComputer(ctx, computer.ComputerID)
	if err != nil || ended.ReconfigurationPhase != ComputerReconfigurationStable || ended.DesiredDiskBytes != computer.DesiredDiskBytes ||
		ended.LastGrowOperation == nil || ended.LastGrowOperation.Status != "failed" || ended.LastGrowOperation.FailureCode != "engine_failure" ||
		ended.LastGrowOperation.Failure == nil {
		t.Fatalf("failed grow left Computer = %#v err=%v", ended.LastGrowOperation, err)
	}
	if code := latchedFailureCode(t, h, ended.CurrentJobID); code != contract.SpawnFailureComputerStorageOperationFailed {
		t.Fatalf("latched failure = %q", code)
	}
	if directives, err := h.store.ListNodeComputerStorageGrowDirectives(ctx, "fabric-computer-node", node.NodeID, node.BootSessionID); err != nil || len(directives) != 0 {
		t.Fatalf("failed grow still dispatches = %#v err=%v", directives, err)
	}
}

// A quarantine ends a clone at once through the shared terminal tail; the
// report's preparation outcome is kept as the clone's evidence.
func TestStorageOperationFailureQuarantineEndsCloneAtOnce(t *testing.T) {
	ctx := context.Background()
	h, node, source, backup, _ := publishedBackupForStorageCopy(t, 2)
	clone, _, err := h.store.BeginComputerClone(ctx, ComputerCloneRequest{BackupID: backup.BackupID,
		ComputerMutationPrecondition: computerPrecondition(source, "operator"), Name: "quarantined-clone",
		DiskBytes: backup.AllocatedSize, IdempotencyKey: "quarantined", Actor: "operator"})
	if err != nil {
		t.Fatal(err)
	}
	directives, err := h.store.ListNodeComputerStorageCopyDirectives(ctx, "fabric-computer-node", node.NodeID, node.BootSessionID)
	if err != nil || len(directives) != 1 {
		t.Fatalf("clone directives = %#v err=%v", directives, err)
	}
	directive := directives[0]
	reporter := newFailureReporter(t, h, node, clone.ComputerID, ComputerStorageOperationClone,
		directive.OperationRevision, ComputerStorageOperationPrepare, directive.CleanupFence)
	foreign := reporter.request(ComputerStorageOperationFailureQuarantined)
	foreign.PreparationOutcome = clonePreparationOutcome(directive, ComputerStoragePreparationQuarantined, 11, h.clock.Now())
	foreign.PreparationOutcome.DestinationGeneration++
	if _, err := reporter.send(foreign); errorCode(err) != contract.ErrorStorageReferenceConflict {
		t.Fatalf("foreign preparation outcome = %v", err)
	}
	quarantined := reporter.request(ComputerStorageOperationFailureQuarantined)
	quarantined.PreparationOutcome = clonePreparationOutcome(directive, ComputerStoragePreparationQuarantined, 11, h.clock.Now())
	if response, err := reporter.send(quarantined); err != nil || response.Disposition != ComputerStorageOperationFailureExhausted ||
		response.FailedAttempts != 1 || response.OperationStatus != "failed" {
		t.Fatalf("quarantine answer = %#v err=%v", response, err)
	}
	ended, err := h.store.GetComputerWithCloneOperation(ctx, clone.ComputerID, directive.OperationRevision)
	if err != nil || ended.ReconfigurationPhase != ComputerReconfigurationStable || ended.CurrentJob.State != contract.JobFailed ||
		ended.CloneOperation == nil || ended.CloneOperation.Status != "failed" ||
		ended.CloneOperation.FailureCode != ComputerStoragePreparationQuarantined || ended.CloneOperation.Failure == nil {
		t.Fatalf("quarantined clone = %#v err=%v", ended.CloneOperation, err)
	}
	if code := latchedFailureCode(t, h, ended.CurrentJobID); code != contract.SpawnFailureComputerStorageOperationFailed {
		t.Fatalf("latched failure = %q", code)
	}
	var outcome []byte
	if err := h.store.db.QueryRow(`SELECT preparation_outcome_json FROM computer_storage_copy_operations
		WHERE destination_computer_id=? AND operation_revision=?`, clone.ComputerID, directive.OperationRevision).Scan(&outcome); err != nil || len(outcome) == 0 {
		t.Fatalf("clone preparation evidence = %s err=%v", outcome, err)
	}
	if storageCopyDirectiveCount(t, h, node) != 0 {
		t.Fatal("quarantined clone is still listed")
	}
}

// A Custody import counts within the budget instead of ending on its first
// engine failure (owner decision D2), shows the count on the import
// observation, and latches its destination failed once the budget is spent.
func TestStorageOperationFailureBudgetEndsCustodyImport(t *testing.T) {
	ctx := context.Background()
	h, node, source, backup, _ := publishedBackupForStorageCopy(t, 3)
	export, exportDirective := beginCustodyExport(t, h, node, source, backup, "budget-import")
	exportReceipt := successfulCustodyExportReceipt(exportDirective)
	if _, err := h.store.AcknowledgeComputerCustodyExport(ctx, "fabric-computer-node", source.ComputerID,
		ComputerCustodyExportAcknowledgementRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID,
			IdempotencyKey: exportReceipt.ReceiptID, Receipt: exportReceipt}); err != nil {
		t.Fatal(err)
	}
	operation, _, err := h.store.BeginComputerCustodyImport(ctx, export.ExportID, ComputerCustodyImportRequest{
		Name: "budget-import", DiskBytes: backup.AllocatedSize, NodeID: node.NodeID,
		ExternalPath: exportDirective.ExternalPath, Manifest: custodyManifest(exportDirective),
		ManifestDigest: exportReceipt.ManifestDigest, IdempotencyKey: "budget-import", Actor: "operator"})
	if err != nil {
		t.Fatal(err)
	}
	directives, err := h.store.ListNodeComputerStorageCopyDirectives(ctx, "fabric-computer-node", node.NodeID, node.BootSessionID)
	if err != nil || len(directives) != 1 || directives[0].Operation != "import" {
		t.Fatalf("import directives = %#v err=%v", directives, err)
	}
	reporter := newFailureReporter(t, h, node, operation.DestinationComputerID, ComputerStorageOperationImport,
		operation.OperationRevision, ComputerStorageOperationPrepare, directives[0].CleanupFence)
	reporter.report(ComputerStorageOperationFailureEngine, ComputerStorageOperationFailureCounted)
	observed, err := h.store.GetComputerCustodyImport(ctx, operation.DestinationComputerID)
	if err != nil || observed.Status != "reserved" || observed.Failure == nil || observed.Failure.FailedAttempts != 1 {
		t.Fatalf("counting import observation = %#v err=%v", observed, err)
	}
	for range MaximumStorageOperationFailures - 2 {
		reporter.report(ComputerStorageOperationFailureEngine, ComputerStorageOperationFailureCounted)
	}
	h.clock.Advance(StorageOperationFailureWindow)
	reporter.report(ComputerStorageOperationFailureEngine, ComputerStorageOperationFailureExhausted)
	observed, err = h.store.GetComputerCustodyImport(ctx, operation.DestinationComputerID)
	if err != nil || observed.Status != "failed" || observed.FailureCode != "engine_failure" || observed.Failure == nil ||
		observed.Failure.FailedAttempts != MaximumStorageOperationFailures {
		t.Fatalf("failed import observation = %#v err=%v", observed, err)
	}
	ended, err := h.store.GetComputer(ctx, operation.DestinationComputerID)
	if err != nil || ended.ReconfigurationPhase != ComputerReconfigurationStable || ended.CurrentJob.State != contract.JobFailed {
		t.Fatalf("failed import destination = %#v err=%v", ended, err)
	}
	if storageCopyDirectiveCount(t, h, node) != 0 {
		t.Fatal("failed import is still listed")
	}
}

// A report for an operation that never existed writes nothing.
func TestStorageOperationFailureReportForUnknownOperationIsSuperseded(t *testing.T) {
	h, node, computer, _, _ := publishedBackupForStorageCopy(t, 2)
	reporter := newFailureReporter(t, h, node, computer.ComputerID, ComputerStorageOperationGrow, 99,
		ComputerStorageOperationPrepare, "no-fence")
	if response := reporter.report(ComputerStorageOperationFailureEngine, ComputerStorageOperationFailureSuperseded); response.OperationStatus != "" {
		t.Fatalf("unknown operation answer = %#v", response)
	}
	var rows int
	if err := h.store.db.QueryRow(`SELECT COUNT(*) FROM computer_storage_operation_failures`).Scan(&rows); err != nil || rows != 0 {
		t.Fatalf("unknown operation wrote %d failure rows err=%v", rows, err)
	}
}

// A database written before the failure budget (#1608) opens with every
// rebuilt table widened in place: live rows survive, the new statuses and
// abort phases insert, the active-reset index no longer counts an ended reset
// as active, and a second open changes nothing.
func TestStorageOperationFailureMigrationWidensDurableConstraints(t *testing.T) {
	ctx := context.Background()
	h, node, computer, backup, _ := publishedBackupForStorageCopy(t, 2)
	// A live clone row, a live reset row, and an abort row populate all
	// three rebuilt tables.
	clone, _, err := h.store.BeginComputerClone(ctx, ComputerCloneRequest{BackupID: backup.BackupID,
		ComputerMutationPrecondition: computerPrecondition(computer, "operator"), Name: "legacy-clone",
		DiskBytes: backup.AllocatedSize, IdempotencyKey: "legacy-clone", Actor: "operator"})
	if err != nil {
		t.Fatal(err)
	}
	computer, err = h.store.GetComputer(ctx, computer.ComputerID)
	if err != nil {
		t.Fatal(err)
	}
	directive := beginStoppedReset(t, h, node, computer, "legacy-reset")
	if _, err := h.store.db.Exec(`INSERT INTO computer_reconfiguration_aborts(computer_id, aborted_revision, intent_revision,
		aborted_phase, idempotency_key, request_hash, actor, created_ns) VALUES(?, 40, 41, 'resetting', 'legacy-abort', 'hash', 'operator', 1)`,
		computer.ComputerID); err != nil {
		t.Fatal(err)
	}
	// The legacy reset table also predates failure_code and completed_ns,
	// which the open adds back before the rebuild.
	legacy := []struct {
		table, index string
		replacements map[string]string
	}{
		{"computer_storage_resets",
			`CREATE UNIQUE INDEX computer_storage_reset_active ON computer_storage_resets(computer_id) WHERE status NOT IN ('retired', 'superseded')`,
			map[string]string{
				"status IN ('reserved', 'prepared', 'published', 'retired', 'failed', 'cleanup_owed', 'superseded')": "status IN ('reserved', 'prepared', 'published', 'retired', 'superseded')",
				"  failure_code TEXT NOT NULL DEFAULT '',\n":                                                         "",
				"\n  completed_ns INTEGER,":                                                                          "",
			}},
		{"computer_storage_copy_operations",
			`CREATE UNIQUE INDEX computer_storage_copy_active ON computer_storage_copy_operations(destination_computer_id) WHERE status IN ('reserved', 'prepared', 'published')`,
			map[string]string{"status IN ('reserved', 'prepared', 'published', 'retired', 'complete', 'failed', 'cleanup_owed', 'superseded')": "status IN ('reserved', 'prepared', 'published', 'retired', 'complete', 'failed', 'superseded')"}},
		{"computer_reconfiguration_aborts", "",
			map[string]string{"'backing_up', 'resetting', 'restoring', 'cloning', 'exporting', 'importing', 'reimaging', 'growing'": "'backing_up', 'resetting', 'exporting', 'importing', 'reimaging', 'growing'"}},
	}
	connection, err := h.store.db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := connection.ExecContext(ctx, "PRAGMA foreign_keys=OFF"); err != nil {
		t.Fatal(err)
	}
	for _, table := range legacy {
		var current string
		if err := connection.QueryRowContext(ctx, `SELECT sql FROM sqlite_master WHERE type='table' AND name=?`, table.table).Scan(&current); err != nil {
			t.Fatal(err)
		}
		createSQL, err := migratedSQLiteCreateTable(current, table.table+"_legacy", table.replacements)
		if err != nil {
			t.Fatalf("fixture error: %v", err)
		}
		if _, err := connection.ExecContext(ctx, createSQL); err != nil {
			t.Fatal(err)
		}
		rows, err := connection.QueryContext(ctx, `SELECT name FROM pragma_table_info(?)`, table.table+"_legacy")
		if err != nil {
			t.Fatal(err)
		}
		var columns []string
		for rows.Next() {
			var column string
			if err := rows.Scan(&column); err != nil {
				t.Fatal(err)
			}
			columns = append(columns, column)
		}
		rows.Close()
		list := strings.Join(columns, ", ")
		statements := `INSERT INTO ` + table.table + `_legacy(` + list + `) SELECT ` + list + ` FROM ` + table.table + `; DROP TABLE ` + table.table +
			`; ALTER TABLE ` + table.table + `_legacy RENAME TO ` + table.table
		if table.index != "" {
			statements += "; " + table.index
		}
		if _, err := connection.ExecContext(ctx, statements); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := connection.ExecContext(ctx, "PRAGMA foreign_keys=ON"); err != nil {
		t.Fatal(err)
	}
	connection.Close()
	var path string
	var sequence int
	var name string
	if err := h.store.db.QueryRow("PRAGMA database_list").Scan(&sequence, &name, &path); err != nil {
		t.Fatal(err)
	}
	schema := func(store *Store) string {
		t.Helper()
		rows, err := store.db.Query(`SELECT name, sql FROM sqlite_master WHERE tbl_name IN
			('computer_storage_resets', 'computer_storage_copy_operations', 'computer_reconfiguration_aborts') ORDER BY name`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var text string
		for rows.Next() {
			var objectName string
			var objectSQL any
			if err := rows.Scan(&objectName, &objectSQL); err != nil {
				t.Fatal(err)
			}
			text += fmt.Sprintf("%s=%v\n", objectName, objectSQL)
		}
		return text
	}
	migrated, err := openTestStore(t, path, StoreOptions{Clock: h.clock, LeaseDuration: 3 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer migrated.Close()
	after := schema(migrated)
	for _, want := range []string{"'cleanup_owed', 'superseded')", "'restoring', 'cloning'",
		"computer_storage_reset_active=CREATE UNIQUE INDEX computer_storage_reset_active ON computer_storage_resets(computer_id)\n\t\t\t\tWHERE status IN ('reserved', 'prepared', 'published')",
		"computer_storage_copy_active="} {
		if !strings.Contains(after, want) {
			t.Fatalf("migrated schema lacks %q:\n%s", want, after)
		}
	}
	var status, failureCode string
	var completed sql.NullInt64
	if err := migrated.db.QueryRow(`SELECT status, failure_code, completed_ns FROM computer_storage_resets WHERE computer_id=? AND intent_revision=?`,
		computer.ComputerID, directive.IntentRevision).Scan(&status, &failureCode, &completed); err != nil || status != "reserved" ||
		failureCode != "" || completed.Valid {
		t.Fatalf("live reset after migration = %q %q %v err=%v", status, failureCode, completed, err)
	}
	if err := migrated.db.QueryRow(`SELECT status FROM computer_storage_copy_operations WHERE destination_computer_id=?`,
		clone.ComputerID).Scan(&status); err != nil || status != "reserved" {
		t.Fatalf("live clone after migration = %q err=%v", status, err)
	}
	var aborts int
	if err := migrated.db.QueryRow(`SELECT COUNT(*) FROM computer_reconfiguration_aborts WHERE idempotency_key='legacy-abort'`).Scan(&aborts); err != nil || aborts != 1 {
		t.Fatalf("abort rows after migration = %d err=%v", aborts, err)
	}
	if _, err := migrated.db.Exec(`UPDATE computer_storage_copy_operations SET status='cleanup_owed' WHERE destination_computer_id=?`,
		clone.ComputerID); err != nil {
		t.Fatalf("migrated copy operations refuse cleanup_owed: %v", err)
	}
	if _, err := migrated.db.Exec(`UPDATE computer_storage_resets SET status='failed' WHERE computer_id=? AND intent_revision=?`,
		computer.ComputerID, directive.IntentRevision); err != nil {
		t.Fatalf("migrated reset refuses failed: %v", err)
	}
	if _, err := migrated.db.Exec(`INSERT INTO computer_storage_resets(computer_id, intent_revision, storage_id, old_generation,
		new_generation, disk_bytes, bound_node_id, root_instance_id, job_id, cleanup_fence, idempotency_key, request_hash,
		status, requested_ns) SELECT computer_id, intent_revision+1, storage_id, old_generation, new_generation+1, disk_bytes,
		bound_node_id, root_instance_id, job_id, cleanup_fence, 'next-reset', request_hash, 'reserved', requested_ns
		FROM computer_storage_resets WHERE computer_id=? AND intent_revision=?`, computer.ComputerID, directive.IntentRevision); err != nil {
		t.Fatalf("a failed reset still blocks the next one: %v", err)
	}
	if _, err := migrated.db.Exec(`UPDATE computer_storage_resets SET status='cleanup_owed' WHERE computer_id=? AND idempotency_key='next-reset'`,
		computer.ComputerID); err != nil {
		t.Fatalf("migrated reset refuses cleanup_owed: %v", err)
	}
	if _, err := migrated.db.Exec(`INSERT INTO computer_reconfiguration_aborts(computer_id, aborted_revision, intent_revision,
		aborted_phase, idempotency_key, request_hash, actor, created_ns) VALUES(?, 1, 2, 'restoring', 'abort-restoring', 'hash', 'operator', 1),
		(?, 2, 3, 'cloning', 'abort-cloning', 'hash', 'operator', 1)`, computer.ComputerID, computer.ComputerID); err != nil {
		t.Fatalf("migrated aborts refuse the new phases: %v", err)
	}
	again, err := openTestStore(t, path, StoreOptions{Clock: h.clock, LeaseDuration: 3 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer again.Close()
	if reopened := schema(again); reopened != after {
		t.Fatalf("second open changed the schema:\n%s\nwas:\n%s", reopened, after)
	}
}

func beginGrowDirective(t *testing.T, h *integrationHarness, node Node, computer Computer, key string) ComputerStorageGrowDirective {
	t.Helper()
	if _, _, err := h.store.BeginComputerGrow(context.Background(), computer.ComputerID, ComputerGrowRequest{
		ComputerMutationPrecondition: computerPrecondition(computer, "operator"), DiskBytes: computer.DesiredDiskBytes + (1 << 30),
		IdempotencyKey: key}); err != nil {
		t.Fatal(err)
	}
	directives, err := h.store.ListNodeComputerStorageGrowDirectives(context.Background(), "fabric-computer-node", node.NodeID, node.BootSessionID)
	if err != nil || len(directives) != 1 {
		t.Fatalf("grow directives = %#v err=%v", directives, err)
	}
	return directives[0]
}

// Every accepted report_id stays replayable, not only the latest: a retry of
// an older report after newer ones (A, B, A) is answered `replayed` and not
// counted, also from a restarted L1, and the same report_id with another body
// is refused.
func TestStorageOperationFailureReplayOfAnOlderReportIsNotCounted(t *testing.T) {
	h, node, computer, _, _ := publishedBackupForStorageCopy(t, 2)
	directive := beginGrowDirective(t, h, node, computer, "replay-grow")
	reporter := newFailureReporter(t, h, node, computer.ComputerID, ComputerStorageOperationGrow,
		directive.OperationRevision, ComputerStorageOperationPrepare, directive.OperationFence)
	a := reporter.request(ComputerStorageOperationFailureEngine)
	if response, err := reporter.send(a); err != nil || response.FailedAttempts != 1 {
		t.Fatalf("report A = %#v err=%v", response, err)
	}
	reporter.report(ComputerStorageOperationFailureRuntimeLoss, ComputerStorageOperationFailureCounted)
	if replayed, err := reporter.send(a); err != nil || replayed.Disposition != ComputerStorageOperationFailureReplayed || replayed.FailedAttempts != 2 {
		t.Fatalf("replay of A after B = %#v err=%v", replayed, err)
	}
	changed := a
	changed.Code = "another_call"
	if _, err := reporter.send(changed); errorCode(err) != contract.ErrorIdempotencyConflict {
		t.Fatalf("changed replay of A after B = %v", err)
	}
	var path string
	var sequence int
	var name string
	if err := h.store.db.QueryRow("PRAGMA database_list").Scan(&sequence, &name, &path); err != nil {
		t.Fatal(err)
	}
	resumed, err := openTestStore(t, path, StoreOptions{Clock: h.clock, LeaseDuration: 3 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer resumed.Close()
	reporter.store = resumed
	if replayed, err := reporter.send(a); err != nil || replayed.Disposition != ComputerStorageOperationFailureReplayed || replayed.FailedAttempts != 2 {
		t.Fatalf("replay of A after restart = %#v err=%v", replayed, err)
	}
}

// A deferral never spends the budget, even when the count and the window are
// already met: it is recorded, the operation keeps running, and the next
// counted report ends it with a counted class as failure_code.
func TestStorageOperationFailureDeferralNeverEndsTheOperation(t *testing.T) {
	h, node, computer, _, _ := publishedBackupForStorageCopy(t, 2)
	directive := beginGrowDirective(t, h, node, computer, "deferral-grow")
	reporter := newFailureReporter(t, h, node, computer.ComputerID, ComputerStorageOperationGrow,
		directive.OperationRevision, ComputerStorageOperationPrepare, directive.OperationFence)
	for range MaximumStorageOperationFailures {
		reporter.report(ComputerStorageOperationFailureEngine, ComputerStorageOperationFailureCounted)
	}
	h.clock.Advance(StorageOperationFailureWindow)
	if deferred := reporter.report(ComputerStorageOperationFailureResumeDeferred, ComputerStorageOperationFailureRecorded); deferred.OperationStatus != "planned" ||
		deferred.FailedAttempts != MaximumStorageOperationFailures {
		t.Fatalf("deferral after the window = %#v", deferred)
	}
	if exhausted := reporter.report(ComputerStorageOperationFailureAcknowledgementRefused, ComputerStorageOperationFailureExhausted); exhausted.OperationStatus != "failed" {
		t.Fatalf("counted report after the deferral = %#v", exhausted)
	}
	ended, err := h.store.GetComputer(context.Background(), computer.ComputerID)
	if err != nil || ended.LastGrowOperation == nil || ended.LastGrowOperation.FailureCode != "acknowledgement_refused" {
		t.Fatalf("ended grow = %#v err=%v", ended.LastGrowOperation, err)
	}
}

// A reset of a running Computer that published but cannot delete its
// predecessor ends `cleanup_owed` without resuming: the reset contract
// resumes only after the predecessor is attested gone. Its service stays
// desired stopped, and a late retirement acknowledgement changes nothing.
func TestCleanupOwedResetOfARunningComputerStaysStopped(t *testing.T) {
	ctx := context.Background()
	h, node, computer := backupHarness(t, 2, nil)
	computer, claim := startBackupComputer(t, h, node, computer)
	if _, _, err := h.store.BeginComputerStorageReset(ctx, computer.ComputerID,
		ComputerStorageResetRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"), IdempotencyKey: "running-reset", TerminateSessions: true}); err != nil {
		t.Fatal(err)
	}
	finishBackupQuiescence(t, h, claim, "running-reset-stop")
	directives, err := h.store.ListNodeComputerStorageResetDirectives(ctx, "fabric-computer-node", node.NodeID, node.BootSessionID)
	if err != nil || len(directives) != 1 {
		t.Fatalf("reset directives = %#v err=%v", directives, err)
	}
	directive := directives[0]
	var resume bool
	if err := h.store.db.QueryRow(`SELECT resume_desired_running FROM computer_storage_resets WHERE computer_id=? AND intent_revision=?`,
		computer.ComputerID, directive.IntentRevision).Scan(&resume); err != nil || !resume {
		t.Fatalf("reset resume intent = %t err=%v", resume, err)
	}
	receipt := resetReceipt(directive)
	if _, err := h.store.AcknowledgeComputerStorageReset(ctx, "fabric-computer-node", computer.ComputerID,
		ComputerStorageResetAcknowledgementRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID,
			IdempotencyKey: receipt.ReceiptID, Receipt: receipt}); err != nil {
		t.Fatal(err)
	}
	reporter := newFailureReporter(t, h, node, computer.ComputerID, ComputerStorageOperationReset,
		directive.IntentRevision, ComputerStorageOperationRetire, directive.CleanupFence)
	reporter.spend(h)
	late := RemovalAcknowledgementRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID,
		RemovalGeneration: uint64(directive.IntentRevision), CleanupFence: directive.CleanupFence,
		RootInstanceID: directive.RootInstanceID, IdempotencyKey: "late-retired"}
	if _, err := h.store.AcknowledgeComputerStorageRetirement(ctx, "fabric-computer-node", computer.ComputerID, late); errorCode(err) != contract.ErrorStaleIntentRevision {
		t.Fatalf("late retirement acknowledgement = %v", err)
	}
	ended, err := h.store.GetComputer(ctx, computer.ComputerID)
	if err != nil || ended.ReconfigurationPhase != ComputerReconfigurationStable || ended.StorageGeneration != directive.NewGeneration ||
		ended.CurrentJob.State != contract.JobStopped || ended.LastResetOperation == nil || ended.LastResetOperation.Status != "cleanup_owed" {
		t.Fatalf("cleanup-owed running reset = %#v err=%v", ended, err)
	}
	var serviceDesired string
	if err := h.store.db.QueryRow(`SELECT desired_state FROM service_jobs WHERE job_id=?`, ended.CurrentJobID).Scan(&serviceDesired); err != nil ||
		serviceDesired != string(contract.ServiceDesiredStopped) {
		t.Fatalf("cleanup-owed reset service desired state = %q err=%v", serviceDesired, err)
	}
}
