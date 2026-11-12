package l1

import (
	"context"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
)

func killStorageCopyNode(t *testing.T, h *integrationHarness, node Node) {
	t.Helper()
	if _, err := h.store.db.Exec(`UPDATE nodes SET state=? WHERE node_id=?`, contract.NodeDead, node.NodeID); err != nil {
		t.Fatal(err)
	}
}

func abortStorageCopy(t *testing.T, h *integrationHarness, computerID, key string) (Computer, bool, error) {
	t.Helper()
	current, err := testReads(h.store).GetComputer(context.Background(), computerID)
	if err != nil {
		t.Fatal(err)
	}
	return h.store.AbortComputerReconfiguration(context.Background(), computerID,
		ComputerReconfigurationAbortRequest{ComputerMutationPrecondition: computerPrecondition(current, "operator"),
			IdempotencyKey: key})
}

// A restore whose bound Node died can be aborted at every stage (#1608).
// Before publication the old generation stays current; after publication the
// restored one does. Either way the Computer is stable, held stopped, the
// operation is superseded with aborted_dead_node, late evidence from the Node
// changes nothing, and a new restore reserves a fresh generation.
func TestRestoreAbortsWhenItsBoundNodeDies(t *testing.T) {
	for _, stage := range []string{"reserved", "published"} {
		t.Run(stage, func(t *testing.T) {
			ctx := context.Background()
			h, node, computer, backup, _ := publishedBackupForStorageCopy(t, 3)
			directive := beginRevokedRestore(t, h, node, computer, backup, "abort-"+stage)
			receipt := successfulStorageCopyReceipt(directive)
			oldReceipt := successfulOldGenerationBackupReceipt(directive)
			wantGeneration := computer.StorageGeneration
			if stage == "published" {
				if _, err := h.store.AcknowledgeComputerStorageCopy(ctx, "fabric-computer-node", computer.ComputerID,
					ComputerStorageCopyAcknowledgementRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID,
						IdempotencyKey: receipt.ReceiptID, Receipt: receipt, OldBackupReceipt: &oldReceipt}); err != nil {
					t.Fatal(err)
				}
				wantGeneration = directive.DestinationGeneration
			}
			// A live Node refuses the abort.
			if _, _, err := abortStorageCopy(t, h, computer.ComputerID, "abort-live"); errorCode(err) != contract.ErrorConflict {
				t.Fatalf("abort with a live Node = %v", err)
			}
			killStorageCopyNode(t, h, node)
			aborted, replayed, err := abortStorageCopy(t, h, computer.ComputerID, "abort-dead")
			if err != nil || replayed || aborted.ReconfigurationPhase != ComputerReconfigurationStable ||
				aborted.StorageGeneration != wantGeneration || aborted.CurrentJob.State != contract.JobStopped {
				t.Fatalf("aborted restore = %#v replayed=%v err=%v", aborted, replayed, err)
			}
			if code := latchedFailureCode(t, h, aborted.CurrentJobID); code != contract.SpawnFailureReconfigurationAborted {
				t.Fatalf("latched failure = %q", code)
			}
			operation, err := h.store.ComputerRestoreOperation(ctx, computer.ComputerID, directive.OperationRevision)
			if err != nil || operation.Status != "superseded" || operation.FailureCode != ComputerRestoreFailureAbortedDeadNode {
				t.Fatalf("aborted restore operation = %#v err=%v", operation, err)
			}
			var stagingLeft int
			if err := h.store.db.QueryRow(`SELECT COUNT(*) FROM computer_storage_generations
				WHERE computer_id=? AND phase='staging'`, computer.ComputerID).Scan(&stagingLeft); err != nil || stagingLeft != 0 {
				t.Fatalf("staging generations after abort = %d err=%v", stagingLeft, err)
			}
			// Late evidence from the Node changes nothing.
			stage := ComputerStorageOperationPrepare
			if wantGeneration == directive.DestinationGeneration {
				stage = ComputerStorageOperationRetire
			}
			reporter := newFailureReporter(t, h, node, computer.ComputerID, ComputerStorageOperationRestore,
				directive.OperationRevision, stage, directive.CleanupFence)
			reporter.report(ComputerStorageOperationFailureEngine, ComputerStorageOperationFailureSuperseded)
			if stage == ComputerStorageOperationPrepare {
				if _, err := h.store.AcknowledgeComputerStorageCopy(ctx, "fabric-computer-node", computer.ComputerID,
					ComputerStorageCopyAcknowledgementRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID,
						IdempotencyKey: receipt.ReceiptID, Receipt: receipt, OldBackupReceipt: &oldReceipt}); err == nil {
					t.Fatal("a late copy receipt published an aborted restore")
				}
			}
			after, err := h.store.GetComputer(ctx, computer.ComputerID)
			if err != nil || after.StorageGeneration != wantGeneration || after.ReconfigurationPhase != ComputerReconfigurationStable {
				t.Fatalf("Computer after late evidence = %#v err=%v", after, err)
			}
			if storageCopyDirectiveCount(t, h, node) != 0 {
				t.Fatal("aborted restore is still listed")
			}
		})
	}
}

// The abort replays from its idempotency key with the identical request.
func TestRestoreAbortReplaysByIdempotencyKey(t *testing.T) {
	ctx := context.Background()
	h, node, computer, backup, _ := publishedBackupForStorageCopy(t, 3)
	beginRevokedRestore(t, h, node, computer, backup, "replay-restore")
	killStorageCopyNode(t, h, node)
	current, err := testReads(h.store).GetComputer(ctx, computer.ComputerID)
	if err != nil {
		t.Fatal(err)
	}
	request := ComputerReconfigurationAbortRequest{ComputerMutationPrecondition: computerPrecondition(current, "operator"),
		IdempotencyKey: "replay-abort"}
	first, replayed, err := h.store.AbortComputerReconfiguration(ctx, computer.ComputerID, request)
	if err != nil || replayed {
		t.Fatalf("abort = %v replayed=%v", err, replayed)
	}
	second, replayed, err := h.store.AbortComputerReconfiguration(ctx, computer.ComputerID, request)
	if err != nil || !replayed || second.IntentRevision != first.IntentRevision {
		t.Fatalf("replayed abort = %#v replayed=%v err=%v", second, replayed, err)
	}
}

// A new restore after an aborted one reserves a generation past every
// recorded one and runs normally.
func TestRestoreAfterAnAbortedRestoreReservesAFreshGeneration(t *testing.T) {
	h, node, computer, backup, _ := publishedBackupForStorageCopy(t, 3)
	first := beginRevokedRestore(t, h, node, computer, backup, "first-restore")
	killStorageCopyNode(t, h, node)
	aborted, _, err := abortStorageCopy(t, h, computer.ComputerID, "abort-first")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.db.Exec(`UPDATE nodes SET state=? WHERE node_id=?`, contract.NodeAlive, node.NodeID); err != nil {
		t.Fatal(err)
	}
	second := beginRevokedRestore(t, h, node, aborted, backup, "second-restore")
	if second.DestinationGeneration <= first.DestinationGeneration {
		t.Fatalf("second restore generation %d does not pass the aborted %d", second.DestinationGeneration, first.DestinationGeneration)
	}
}

// A clone whose bound Node died can be aborted (#1608). The destination is
// renamed so the name is free, cannot start, and a late receipt or report
// changes nothing.
func TestCloneAbortsWhenItsBoundNodeDies(t *testing.T) {
	ctx := context.Background()
	h, node, source, backup, _ := publishedBackupForStorageCopy(t, 2)
	clone, _, err := h.store.BeginComputerClone(ctx, ComputerCloneRequest{BackupID: backup.BackupID,
		ComputerMutationPrecondition: computerPrecondition(source, "operator"), Name: "abort-clone",
		DiskBytes: backup.AllocatedSize, IdempotencyKey: "abort-clone", Actor: "operator"})
	if err != nil {
		t.Fatal(err)
	}
	directives, err := h.store.ListNodeComputerStorageCopyDirectives(ctx, "fabric-computer-node", node.NodeID, node.BootSessionID)
	if err != nil || len(directives) != 1 {
		t.Fatalf("clone directives = %#v err=%v", directives, err)
	}
	directive := directives[0]
	if _, _, err := abortStorageCopy(t, h, clone.ComputerID, "abort-live"); errorCode(err) != contract.ErrorConflict {
		t.Fatalf("abort with a live Node = %v", err)
	}
	killStorageCopyNode(t, h, node)
	aborted, _, err := abortStorageCopy(t, h, clone.ComputerID, "abort-dead")
	if err != nil || aborted.ReconfigurationPhase != ComputerReconfigurationStable ||
		aborted.Name != "aborted-clone-"+clone.ComputerID {
		t.Fatalf("aborted clone = %#v err=%v", aborted, err)
	}
	ended, err := h.store.GetComputerWithCloneOperation(ctx, clone.ComputerID, directive.OperationRevision)
	if err != nil || ended.CloneOperation == nil || ended.CloneOperation.Status != "superseded" ||
		ended.CloneOperation.FailureCode != "aborted_dead_node" {
		t.Fatalf("aborted clone operation = %#v err=%v", ended.CloneOperation, err)
	}
	var phase string
	if err := h.store.db.QueryRow(`SELECT phase FROM computer_storage_generations WHERE computer_id=?`, clone.ComputerID).Scan(&phase); err != nil || phase != "retired" {
		t.Fatalf("aborted clone staging generation = %q err=%v", phase, err)
	}
	reporter := newFailureReporter(t, h, node, clone.ComputerID, ComputerStorageOperationClone,
		directive.OperationRevision, ComputerStorageOperationPrepare, directive.CleanupFence)
	reporter.report(ComputerStorageOperationFailureEngine, ComputerStorageOperationFailureSuperseded)
	receipt := successfulStorageCopyReceipt(directive)
	if _, err := h.store.AcknowledgeComputerStorageCopy(ctx, "fabric-computer-node", clone.ComputerID,
		ComputerStorageCopyAcknowledgementRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID,
			IdempotencyKey: receipt.ReceiptID, Receipt: receipt}); err == nil {
		t.Fatal("a late copy receipt completed an aborted clone")
	}
	// The name is free for a new clone.
	if _, err := h.store.db.Exec(`UPDATE nodes SET state=? WHERE node_id=?`, contract.NodeAlive, node.NodeID); err != nil {
		t.Fatal(err)
	}
	current, err := testReads(h.store).GetComputer(ctx, source.ComputerID)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := h.store.BeginComputerClone(ctx, ComputerCloneRequest{BackupID: backup.BackupID,
		ComputerMutationPrecondition: computerPrecondition(current, "operator"), Name: "abort-clone",
		DiskBytes: backup.AllocatedSize, IdempotencyKey: "abort-clone-again", Actor: "operator"}); err != nil {
		t.Fatalf("the aborted clone's name was not released: %v", err)
	}
}

// Removing a clone's source ends the clone failed with source_removed and
// latches its destination, instead of leaving it `cloning` for ever (#1608).
// The source's removal still claims the destination staging generation.
func TestRemovingCloneSourceLatchesTheDestinationFailed(t *testing.T) {
	ctx := context.Background()
	h, node, source, backup, _ := publishedBackupForStorageCopy(t, 2)
	clone, _, err := h.store.BeginComputerClone(ctx, ComputerCloneRequest{BackupID: backup.BackupID,
		ComputerMutationPrecondition: computerPrecondition(source, "operator"), Name: "orphan-clone",
		DiskBytes: backup.AllocatedSize, IdempotencyKey: "orphan-clone", Actor: "operator"})
	if err != nil {
		t.Fatal(err)
	}
	current, err := testReads(h.store).GetComputer(ctx, source.ComputerID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.RemoveComputer(ctx, source.ComputerID,
		ComputerRemoveRequest{ComputerMutationPrecondition: computerPrecondition(current, "operator-remove")}); err != nil {
		t.Fatal(err)
	}
	ended, err := h.store.GetComputerWithCloneOperation(ctx, clone.ComputerID, clone.IntentRevision)
	if err != nil || ended.ReconfigurationPhase != ComputerReconfigurationStable || ended.CurrentJob.State != contract.JobFailed ||
		ended.CloneOperation == nil || ended.CloneOperation.Status != "failed" ||
		ended.CloneOperation.FailureCode != ComputerCloneFailureSourceRemoved {
		t.Fatalf("orphaned clone = %#v operation=%#v err=%v", ended, ended.CloneOperation, err)
	}
	if code := latchedFailureCode(t, h, ended.CurrentJobID); code != contract.SpawnFailureCode(ComputerCloneFailureSourceRemoved) {
		t.Fatalf("latched failure = %q", code)
	}
	removals, err := h.store.ListNodeRemovalDirectives(ctx, "fabric-computer-node", node.NodeID, node.BootSessionID)
	if err != nil || len(removals) != 1 {
		t.Fatalf("source removal directives = %#v err=%v", removals, err)
	}
	claimed := false
	for _, generation := range removals[0].ComputerStorageGenerations.Generations {
		claimed = claimed || generation.ComputerID == clone.ComputerID
	}
	if !claimed {
		t.Fatalf("source removal no longer claims the destination generation: %#v", removals[0].ComputerStorageGenerations)
	}
	// The destination can now be removed by its owner.
	if _, err := h.store.RemoveComputer(ctx, clone.ComputerID,
		ComputerRemoveRequest{ComputerMutationPrecondition: computerPrecondition(ended, "operator-remove")}); err != nil {
		t.Fatalf("remove the latched destination: %v", err)
	}
}
