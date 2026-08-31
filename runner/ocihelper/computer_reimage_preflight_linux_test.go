//go:build linux

package ocihelper

import (
	"errors"
	"strings"
	"testing"
)

func TestReimagePreflightStageErrorIsBoundedAndPreservesCause(t *testing.T) {
	cause := errors.New("private /host/path detail")
	err := reimagePreflightStageError("disk_owner", cause)
	if !errors.Is(err, cause) {
		t.Fatal("preflight stage error did not preserve its cause for local classification")
	}
	if got := err.Error(); got != "Computer reimage preflight failed at disk_owner" || strings.Contains(got, "/host/path") {
		t.Fatalf("preflight stage error crossed private mechanics: %q", got)
	}
}

func TestReimageAcceptsVerifiedNeverAttachedResetSuccessor(t *testing.T) {
	storage := testComputerStorage()
	storage.StorageGeneration = 2
	storage.IntentRevision = 3
	preparation := ComputerStorageResetAuthority{NodeID: "node-1", BootSessionID: "boot-1", HelperGeneration: 7,
		RootInstanceID: "root-1", JobID: "old-job", PriorJobID: "old-job", IntentRevision: 3, CleanupFence: "reset-fence"}
	receipt := ComputerStorageResetReceipt{Kind: "computer_storage_reset_verified", ReceiptID: "reset-receipt",
		ComputerID: storage.ComputerID, StorageID: storage.StorageID, OldGeneration: 1, NewGeneration: 2,
		NodeID: preparation.NodeID, RootInstanceID: preparation.RootInstanceID, JobID: preparation.JobID,
		IntentRevision: preparation.IntentRevision, CleanupFence: preparation.CleanupFence,
		HelperGeneration: preparation.HelperGeneration}
	manifest := computerDiskManifest{Version: computerDiskManifestVersion, Storage: storage, DiskImage: "disk.ext4",
		MountDirectory: "disk", Prepared: true, Preparation: &preparation, PreparationReceipt: &receipt}
	request := PreflightComputerReimageRequest{Storage: storage, Authority: ComputerReimagePreflightAuthority{
		NodeID: preparation.NodeID, RootInstanceID: preparation.RootInstanceID, OldJobID: preparation.JobID,
	}}
	evidence, ok := reimageDetachmentEvidence(manifest, request)
	if !ok || evidence.receiptID != receipt.ReceiptID || evidence.attemptID != "storage-reset-3" ||
		evidence.fencingToken != preparation.CleanupFence {
		t.Fatalf("reset-successor reimage evidence = %+v ok=%t", evidence, ok)
	}
	request.Authority.OldJobID = "different-job"
	if _, ok := reimageDetachmentEvidence(manifest, request); ok {
		t.Fatal("reset-successor preparation authorized a different prior Job")
	}
}
