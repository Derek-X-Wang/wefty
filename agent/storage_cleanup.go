package agent

import (
	"errors"

	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
)

func storageCleanupQuarantineAcknowledgement(err error, rootInstanceID string) (l1.RemovalAcknowledgementRequest, bool) {
	var quarantined *workloadrunner.ManagedVolumeCleanupQuarantinedError
	if !errors.As(err, &quarantined) {
		return l1.RemovalAcknowledgementRequest{}, false
	}
	receipt := quarantined.Receipt
	return l1.RemovalAcknowledgementRequest{NodeID: receipt.Removal.NodeID, BootSessionID: receipt.Removal.BootSessionID,
		RemovalGeneration: receipt.Removal.RemovalGeneration, CleanupFence: receipt.Removal.CleanupFence,
		RootInstanceID: rootInstanceID, IdempotencyKey: receipt.ReceiptID,
		CleanupQuarantine: &l1.ComputerStorageCleanupQuarantine{Kind: receipt.Kind, ReceiptID: receipt.ReceiptID,
			VolumeKind: string(receipt.VolumeKind), ComputerID: receipt.ComputerStorage.ComputerID, StorageID: receipt.ComputerStorage.StorageID,
			StorageGeneration: receipt.ComputerStorage.StorageGeneration, NodeID: receipt.Removal.NodeID,
			BootSessionID: receipt.Removal.BootSessionID, JobID: receipt.Removal.JobID,
			RemovalGeneration: receipt.Removal.RemovalGeneration, CleanupFence: receipt.Removal.CleanupFence,
			FailureReason: receipt.FailureReason, Attempts: receipt.Attempts}}, true
}
