package agent

import (
	"testing"

	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
)

func TestStorageCleanupQuarantineAcknowledgementPreservesTypedEvidence(t *testing.T) {
	err := &workloadrunner.ManagedVolumeCleanupQuarantinedError{Receipt: workloadrunner.ManagedVolumeQuarantineReceipt{
		Kind: "managed_volume_cleanup_quarantined", ReceiptID: "receipt", VolumeKind: workloadrunner.ManagedVolumeComputerDisk,
		ComputerStorage: workloadrunner.ComputerStorage{ComputerID: "computer", StorageID: "storage", StorageGeneration: 4},
		Removal: workloadrunner.ManagedVolumeRemovalAuthority{NodeID: "node", BootSessionID: "boot", JobID: "job", PriorJobID: "job",
			RemovalGeneration: 9, CleanupFence: "fence"}, FailureReason: "operation_failed", Attempts: 3,
	}}
	acknowledgement, ok := storageCleanupQuarantineAcknowledgement(err, "root", l1.ComputerStorageCleanupReset)
	if !ok || acknowledgement.CleanupQuarantine == nil || acknowledgement.IdempotencyKey != "receipt" ||
		acknowledgement.CleanupQuarantine.StorageGeneration != 4 || acknowledgement.CleanupQuarantine.Attempts != 3 ||
		acknowledgement.CleanupQuarantine.FailureReason != "operation_failed" ||
		acknowledgement.CleanupQuarantine.Operation != l1.ComputerStorageCleanupReset {
		t.Fatalf("cleanup quarantine acknowledgement = %#v ok=%t", acknowledgement, ok)
	}
}
