package ocihelper

import (
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
)

// The detached arm (#1611) is the third closed Storage-only attestation shape.
// No other shape may carry a detachment witness.
func TestAttestRemovalAcceptsOnlyAMatchingDetachedArm(t *testing.T) {
	storage := &ComputerStorageReference{ComputerID: "computer", StorageID: "storage", StorageGeneration: 2, DiskBytes: 8 << 30}
	witness := &contract.ComputerStorageDetachmentWitness{Kind: contract.ComputerStorageDetachedKind,
		EvidenceKind: contract.ComputerStorageDetachSameBootReap, ReceiptID: "receipt", NodeID: "node",
		ComputerID: storage.ComputerID, StorageID: storage.StorageID, StorageGeneration: storage.StorageGeneration,
		AttachedJobID: "job-that-ran", AttachedAttemptID: "attempt-a", AttachedBootSessionID: "boot", HelperGeneration: 1}
	attempt := func(attemptID string, mutate func(*RemovalAttemptManifest)) AttestRemovalRequest {
		row := RemovalAttemptManifest{
			Authority: AttemptAuthority{NodeID: "node", BootSessionID: "boot", JobID: "rotated-job", AttemptID: attemptID,
				FencingToken: "cleanup", Class: contract.JobClassService, RemovalGeneration: "2"},
			ComputerStorage: storage, StorageOnly: true, StorageDetachment: witness,
			Resources: expectedComputerStorageRemovalResources(storage),
		}
		if mutate != nil {
			mutate(&row)
		}
		return AttestRemovalRequest{JobID: "rotated-job", RemovalGeneration: "2", Attempts: []RemovalAttemptManifest{row}}
	}
	detachedID := contract.StorageDetachedRemovalAttemptID(storage.StorageGeneration)
	if err := validateAttestRemovalRequest(attempt(detachedID, nil), "node"); err != nil {
		t.Fatalf("matching detached arm refused: %v", err)
	}
	preparation := &contract.ComputerStoragePreparationWitness{Kind: contract.ComputerStorageResetVerifiedKind, ReceiptID: "r",
		NodeID: "node", RootInstanceID: "root", JobID: "j", ComputerID: storage.ComputerID, StorageID: storage.StorageID,
		StorageGeneration: storage.StorageGeneration, Revision: 1, Fence: "f", HelperGeneration: 1}
	for name, request := range map[string]AttestRemovalRequest{
		"no witness":          attempt(detachedID, func(r *RemovalAttemptManifest) { r.StorageDetachment = nil }),
		"with preparation":    attempt(detachedID, func(r *RemovalAttemptManifest) { r.StoragePreparation = preparation }),
		"invalid witness":     attempt(detachedID, func(r *RemovalAttemptManifest) { w := *witness; w.SweepEpoch = "x"; r.StorageDetachment = &w }),
		"foreign Node":        attempt(detachedID, func(r *RemovalAttemptManifest) { w := *witness; w.NodeID = "other"; r.StorageDetachment = &w }),
		"foreign generation":  attempt(detachedID, func(r *RemovalAttemptManifest) { w := *witness; w.StorageGeneration = 3; r.StorageDetachment = &w }),
		"foreign Storage":     attempt(detachedID, func(r *RemovalAttemptManifest) { w := *witness; w.StorageID = "other"; r.StorageDetachment = &w }),
		"absent with witness": attempt(contract.StorageAbsentRemovalAttemptID(2), func(r *RemovalAttemptManifest) { r.StorageAbsent = true }),
		"prepared with witness": attempt(contract.StorageOnlyRemovalAttemptID(2), func(r *RemovalAttemptManifest) {
			r.StoragePreparation = preparation
		}),
	} {
		if err := validateAttestRemovalRequest(request, "node"); err == nil {
			t.Errorf("%s: attestation accepted", name)
		}
	}
}
