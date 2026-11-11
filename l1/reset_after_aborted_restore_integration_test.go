package l1

import (
	"context"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
)

// An aborted restore keeps its staging generation as a retired row, so a
// Storage reset after it reserves the generation past that row: current+2,
// with the current generation as its predecessor (#1598). The helper accepts
// any successor newer than the current generation.
func TestStorageResetAfterAbortedRestoreSkipsTheRetiredGeneration(t *testing.T) {
	ctx := context.Background()
	h, node, computer, source, _ := publishedBackupForStorageCopy(t, 3)
	reserved, _, err := h.store.BeginComputerRestore(ctx, computer.ComputerID,
		ComputerRestoreRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"),
			BackupID: source.BackupID, KeepOldBackup: true, IdempotencyKey: "aborted-before-reset"})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.store.RecordComputerRestoreAuthorityRevoked(ctx, computer.ComputerID, reserved.IntentRevision,
		testRestoreRevocationEvidence(computer.ComputerID, reserved.IntentRevision)); err != nil {
		t.Fatal(err)
	}
	directives, err := h.store.ListNodeComputerStorageCopyDirectives(ctx, "fabric-computer-node", node.NodeID, node.BootSessionID)
	if err != nil || len(directives) != 1 {
		t.Fatalf("restore directives = %#v err=%v", directives, err)
	}
	copyReceipt := successfulStorageCopyReceipt(directives[0])
	oldReceipt := successfulOldGenerationBackupReceipt(directives[0])
	oldReceipt.Kind, oldReceipt.ContentDigest, oldReceipt.FailureCode, oldReceipt.CopyAbsent =
		computerBackupFailureReceiptKind, "", "insufficient_disk", true
	if _, err := h.store.AcknowledgeComputerStorageCopy(ctx, "fabric-computer-node", computer.ComputerID,
		ComputerStorageCopyAcknowledgementRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID,
			IdempotencyKey: copyReceipt.ReceiptID, Receipt: copyReceipt, OldBackupReceipt: &oldReceipt}); errorCode(err) != contract.ErrorConflict {
		t.Fatalf("failed predecessor copy = %v, want the typed abort", err)
	}
	aborted, err := testReads(h.store).GetComputer(ctx, computer.ComputerID)
	if err != nil || aborted.ReconfigurationPhase != ComputerReconfigurationStable ||
		aborted.StorageGeneration != computer.StorageGeneration {
		t.Fatalf("aborted restore left Computer = %#v err=%v", aborted, err)
	}
	if _, _, err := h.store.BeginComputerStorageReset(ctx, computer.ComputerID,
		ComputerStorageResetRequest{ComputerMutationPrecondition: computerPrecondition(aborted, "operator"),
			IdempotencyKey: "reset-after-aborted-restore"}); err != nil {
		t.Fatalf("reset after an aborted restore: %v", err)
	}
	resets, err := h.store.ListNodeComputerStorageResetDirectives(ctx, "fabric-computer-node", node.NodeID, node.BootSessionID)
	if err != nil || len(resets) != 1 {
		t.Fatalf("reset directives = %#v err=%v", resets, err)
	}
	if resets[0].OldGeneration != computer.StorageGeneration ||
		resets[0].NewGeneration != directives[0].DestinationGeneration+1 ||
		resets[0].NewGeneration != computer.StorageGeneration+2 {
		t.Fatalf("reset after an aborted restore = %d -> %d, want %d -> %d (restore staged %d)",
			resets[0].OldGeneration, resets[0].NewGeneration, computer.StorageGeneration,
			computer.StorageGeneration+2, directives[0].DestinationGeneration)
	}
}
