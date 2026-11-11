package l1

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

func restoreEngineFailureAcknowledgement(node Node, d ComputerStorageCopyDirective, helperGeneration uint64,
	recordedAt time.Time) ComputerStorageCopyAcknowledgementRequest {
	return ComputerStorageCopyAcknowledgementRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID,
		IdempotencyKey: fmt.Sprintf("preparation-%s-%d-", ComputerStoragePreparationInterrupted, helperGeneration),
		PreparationOutcome: &ComputerStoragePreparationOutcome{Code: ComputerStoragePreparationInterrupted,
			DestinationComputerID: d.DestinationComputerID, DestinationStorageID: d.DestinationStorageID,
			DestinationGeneration: d.DestinationGeneration, IntentRevision: d.OperationRevision,
			DiskBytes: d.DestinationSize, HelperGeneration: helperGeneration, Operation: "computer_storage_copy",
			Reason: ComputerStoragePreparationReasonEngineFailure, RecordedAt: &recordedAt}}
}

// A restore whose copy kept failing with an engine failure on a live helper
// session is aborted once the agent's bound is spent (#1591): the restore
// records `failed` with storage_copy_engine_failure, the old generation stays
// current, the directive is no longer listed, and a replay after the abort --
// even after a newer restore started -- stays aborted. Removal still plans the
// partial copy's generation and the predecessor Backup copy.
func TestRestoreCopyEngineFailureAbortsTheRestore(t *testing.T) {
	ctx := context.Background()
	h, node, computer, source, _ := publishedBackupForStorageCopy(t, 3)
	begin := func(precondition Computer, key string) Computer {
		t.Helper()
		reserved, _, err := h.store.BeginComputerRestore(ctx, computer.ComputerID,
			ComputerRestoreRequest{ComputerMutationPrecondition: computerPrecondition(precondition, "operator"),
				BackupID: source.BackupID, KeepOldBackup: true, IdempotencyKey: key})
		if err != nil {
			t.Fatalf("begin restore %s: %v", key, err)
		}
		if err := h.store.RecordComputerRestoreAuthorityRevoked(ctx, computer.ComputerID, reserved.IntentRevision,
			testRestoreRevocationEvidence(computer.ComputerID, reserved.IntentRevision)); err != nil {
			t.Fatal(err)
		}
		return reserved
	}
	directives := func() []ComputerStorageCopyDirective {
		t.Helper()
		listed, err := h.store.ListNodeComputerStorageCopyDirectives(ctx, "fabric-computer-node", node.NodeID, node.BootSessionID)
		if err != nil {
			t.Fatal(err)
		}
		return listed
	}
	acknowledge := func(request ComputerStorageCopyAcknowledgementRequest) (Computer, error) {
		return h.store.AcknowledgeComputerStorageCopy(ctx, "fabric-computer-node", computer.ComputerID, request)
	}

	reservedA := begin(computer, "restore-engine-failure-a")
	listed := directives()
	if len(listed) != 1 || listed[0].OperationRevision != reservedA.IntentRevision {
		t.Fatalf("restore A directives = %#v", listed)
	}
	a := listed[0]
	recordedAt := time.Date(2026, 11, 10, 20, 10, 0, 0, time.UTC)

	// Every other restore preparation outcome stays rejected.
	for name, mutate := range map[string]func(*ComputerStoragePreparationOutcome){
		"runtime loss": func(o *ComputerStoragePreparationOutcome) { o.Reason = ComputerStoragePreparationReasonRuntimeLoss },
		"deferral": func(o *ComputerStoragePreparationOutcome) {
			o.Code, o.SweepEpoch, o.DiskName, o.Reason = ComputerStoragePreparationResumeDeferred, "epoch", "disk", "operational_failure"
		},
		"quarantine": func(o *ComputerStoragePreparationOutcome) {
			o.Code, o.SweepEpoch, o.DiskName, o.Reason = ComputerStoragePreparationQuarantined, "epoch", "disk", "operational_failure"
		},
	} {
		request := restoreEngineFailureAcknowledgement(node, a, 13, recordedAt)
		request.IdempotencyKey = "other-" + name
		mutate(request.PreparationOutcome)
		if _, err := acknowledge(request); errorCode(err) != contract.ErrorInvalidRequest {
			t.Fatalf("restore %s outcome = %v, want %q", name, err, contract.ErrorInvalidRequest)
		}
	}
	if still := directives(); len(still) != 1 {
		t.Fatalf("a rejected outcome ended the restore: %#v", still)
	}

	abortA := restoreEngineFailureAcknowledgement(node, a, 13, recordedAt)
	aborted, err := acknowledge(abortA)
	if err != nil {
		t.Fatalf("engine failure abort: %v", err)
	}
	if aborted.ReconfigurationPhase != ComputerReconfigurationStable || aborted.AppliedRevision != a.OperationRevision ||
		aborted.StorageGeneration != a.OldGeneration {
		t.Fatalf("aborted restore left Computer = %#v", aborted)
	}
	operation, err := h.store.ComputerRestoreOperation(ctx, computer.ComputerID, a.OperationRevision)
	if err != nil || operation.Status != "failed" || operation.FailureCode != ComputerRestoreFailureStorageCopyEngineFailure ||
		operation.CompletedAt == nil {
		t.Fatalf("aborted restore operation = %#v err=%v", operation, err)
	}
	var stagingPhase string
	if err := h.store.db.QueryRow(`SELECT phase FROM computer_storage_generations WHERE computer_id=? AND storage_generation=?`,
		computer.ComputerID, a.DestinationGeneration).Scan(&stagingPhase); err != nil || stagingPhase != "retired" {
		t.Fatalf("aborted staging generation phase = %q err=%v", stagingPhase, err)
	}
	if after := directives(); len(after) != 0 {
		t.Fatalf("aborted restore is still listed: %#v", after)
	}

	// An identical replay reads the same answer; a different one is refused.
	if replayed, err := acknowledge(abortA); err != nil || replayed.ReconfigurationPhase != ComputerReconfigurationStable {
		t.Fatalf("identical replay = %#v err=%v", replayed, err)
	}
	different := restoreEngineFailureAcknowledgement(node, a, 14, recordedAt.Add(time.Minute))
	if _, err := acknowledge(different); errorCode(err) != contract.ErrorConflict {
		t.Fatalf("different outcome after the abort = %v, want %q", err, contract.ErrorConflict)
	}

	// A later restore reserves a generation past the retired staging row
	// instead of colliding with it, and the replay of A still reads A.
	reservedB := begin(aborted, "restore-engine-failure-b")
	listed = directives()
	if len(listed) != 1 || listed[0].OperationRevision != reservedB.IntentRevision ||
		listed[0].DestinationGeneration != a.DestinationGeneration+1 || listed[0].OldGeneration != a.OldGeneration {
		t.Fatalf("restore B directives = %#v (A staged %d)", listed, a.DestinationGeneration)
	}
	b := listed[0]
	if replayed, err := acknowledge(abortA); err != nil || replayed.ReconfigurationPhase != ComputerReconfigurationRestoring ||
		replayed.IntentRevision != b.OperationRevision {
		t.Fatalf("replay of A after B started = %#v err=%v", replayed, err)
	}
	if operation, err := h.store.ComputerRestoreOperation(ctx, computer.ComputerID, b.OperationRevision); err != nil ||
		operation.Status != "reserved" {
		t.Fatalf("replay of A touched restore B: %#v err=%v", operation, err)
	}
	if operation, err := h.store.ComputerRestoreOperation(ctx, computer.ComputerID, a.OperationRevision); err != nil ||
		operation.Status != "failed" || operation.FailureCode != ComputerRestoreFailureStorageCopyEngineFailure {
		t.Fatalf("replay reopened restore A: %#v err=%v", operation, err)
	}

	// Abort B as well, then remove the Computer: removal plans both partial
	// copies' generations and both predecessor Backup copies.
	if _, err := acknowledge(restoreEngineFailureAcknowledgement(node, b, 13, recordedAt.Add(time.Hour))); err != nil {
		t.Fatalf("restore B abort: %v", err)
	}
	stable, err := testReads(h.store).GetComputer(ctx, computer.ComputerID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.RemoveComputer(ctx, computer.ComputerID,
		ComputerRemoveRequest{ComputerMutationPrecondition: computerPrecondition(stable, "operator-remove")}); err != nil {
		t.Fatal(err)
	}
	removals, err := h.store.ListNodeRemovalDirectives(ctx, "fabric-computer-node", node.NodeID, node.BootSessionID)
	if err != nil || len(removals) != 1 || removals[0].ComputerStorageGenerations == nil || removals[0].ComputerBackupCopies == nil {
		t.Fatalf("removal directive = %#v err=%v", removals, err)
	}
	generations := map[int64]bool{}
	for _, generation := range removals[0].ComputerStorageGenerations.Generations {
		generations[generation.StorageGeneration] = true
	}
	if !generations[a.DestinationGeneration] || !generations[b.DestinationGeneration] {
		t.Fatalf("removal omitted an aborted restore's generation: %#v", removals[0].ComputerStorageGenerations.Generations)
	}
	precommits := map[string]ComputerBackupPruneDirective{}
	for _, copy := range removals[0].ComputerBackupCopies.Copies {
		precommits[copy.CopyID] = copy
	}
	for _, d := range []ComputerStorageCopyDirective{a, b} {
		precommit, planned := precommits[d.OldCopyID]
		if !planned || !precommit.Superseded || precommit.StorageGeneration != d.OldGeneration {
			t.Fatalf("removal omitted restore %d's predecessor Backup copy: %#v", d.OperationRevision, removals[0].ComputerBackupCopies.Copies)
		}
		accepted := backupRemovalReceipt(precommit)
		if _, err := h.store.AcknowledgeComputerBackupPrune(ctx, "fabric-computer-node", computer.ComputerID,
			ComputerBackupPruneAcknowledgementRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID,
				IdempotencyKey: "precommit-removed-" + d.OldCopyID, Receipt: accepted}); err != nil {
			t.Fatalf("restore %d predecessor Backup absence: %v", d.OperationRevision, err)
		}
	}
	var outstanding int
	if err := h.store.db.QueryRow(`SELECT COUNT(*) FROM computer_storage_copy_operations
		WHERE destination_computer_id=? AND keep_old_as_backup=1 AND old_backup_absence_acknowledgement_key IS NULL`,
		computer.ComputerID).Scan(&outstanding); err != nil || outstanding != 0 {
		t.Fatalf("outstanding predecessor Backup copies = %d err=%v", outstanding, err)
	}
	after, err := h.store.ListNodeRemovalDirectives(ctx, "fabric-computer-node", node.NodeID, node.BootSessionID)
	if err != nil || len(after) != 1 {
		t.Fatalf("removal directive after predecessor absence = %#v err=%v", after, err)
	}
	for _, copy := range after[0].ComputerBackupCopies.Copies {
		if copy.CopyID == a.OldCopyID || copy.CopyID == b.OldCopyID {
			t.Fatalf("removal still plans a predecessor copy after its absence: %#v", copy)
		}
	}
}
