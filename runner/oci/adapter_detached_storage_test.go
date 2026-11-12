package oci

import (
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

// A once-attached generation whose current Job never ran comes back from the
// helper as the detached Storage-only arm (#1611). The adapter accepts it only
// when the witness matches the row, the Node, the current helper session and,
// for a same-boot reap, the current boot.
func TestLegacyComputerRemovalReconstructsDetachedStorage(t *testing.T) {
	storage := &ocihelper.ComputerStorageReference{ComputerID: "computer", StorageID: "storage", StorageGeneration: 1, DiskBytes: 8 << 30}
	authority := ocihelper.AttemptAuthority{NodeID: "node", BootSessionID: "boot", JobID: "rotated-job",
		AttemptID: contract.StorageDetachedRemovalAttemptID(storage.StorageGeneration), FencingToken: "cleanup",
		Class: contract.JobClassService, RemovalGeneration: "1"}
	engine := &adapterTestEngine{}
	adapter, closeAdapter := startAdapterTestServer(t, engine)
	defer closeAdapter()
	session, err := adapter.sessions.Session()
	if err != nil {
		t.Fatal(err)
	}
	witness := contract.ComputerStorageDetachmentWitness{Kind: contract.ComputerStorageDetachedKind,
		EvidenceKind: contract.ComputerStorageDetachSameBootReap, ReceiptID: "receipt", NodeID: "node",
		ComputerID: storage.ComputerID, StorageID: storage.StorageID, StorageGeneration: storage.StorageGeneration,
		AttachedJobID: "job-that-ran", AttachedAttemptID: "attempt-a", AttachedBootSessionID: "boot",
		HelperGeneration: session.Handshake().SessionGeneration}
	resources := make([]ocihelper.RemovalResource, 0, 9)
	for _, class := range []ocihelper.RemovalResourceClass{ocihelper.RemovalResourceComputerDiskImage, ocihelper.RemovalResourceComputerDiskAllocation,
		ocihelper.RemovalResourceComputerDiskQuota, ocihelper.RemovalResourceComputerDiskManifest, ocihelper.RemovalResourceComputerDiskMount,
		ocihelper.RemovalResourceComputerDiskLoop, ocihelper.RemovalResourceComputerAttachment, ocihelper.RemovalResourceComputerResetManifest,
		ocihelper.RemovalResourceComputerQuarantine} {
		resources = append(resources, ocihelper.RemovalResource{Class: class, ID: mustComputerDiskName(t, *storage)})
	}
	row := func(mutate func(*ocihelper.RemovalAttemptManifest)) ocihelper.InventoryRemovalResponse {
		w := witness
		copied := *storage
		attempt := ocihelper.RemovalAttemptManifest{Authority: authority, ComputerStorage: &copied, StorageOnly: true,
			StorageDetachment: &w, Resources: resources}
		if mutate != nil {
			mutate(&attempt)
		}
		return ocihelper.InventoryRemovalResponse{Attempts: []ocihelper.RemovalAttemptManifest{attempt}}
	}
	request := workloadrunner.RuntimeRemovalProofRequest{NodeID: "node", BootSessionID: "boot", RootInstanceID: "root",
		JobID: authority.JobID, RemovalGeneration: 1, CleanupFence: "cleanup",
		ComputerStorage: &workloadrunner.ComputerStorage{ComputerID: storage.ComputerID, StorageID: storage.StorageID, StorageGeneration: storage.StorageGeneration}}

	engine.inventoryRemoval = row(nil)
	attempts, err := adapter.ReconstructRuntimeRemoval(t.Context(), request)
	if err != nil || len(attempts) != 1 || !attempts[0].StorageOnly || attempts[0].StorageAbsent ||
		attempts[0].StoragePreparation != nil || attempts[0].StorageDetachment == nil ||
		attempts[0].StorageDetachment.AttachedJobID != "job-that-ran" || len(attempts[0].RemovalResources()) != 9 {
		t.Fatalf("detached reconstruction = %+v err=%v", attempts, err)
	}

	preparation := &contract.ComputerStoragePreparationWitness{Kind: contract.ComputerStorageResetVerifiedKind, ReceiptID: "r",
		NodeID: "node", RootInstanceID: "root", JobID: "j", ComputerID: storage.ComputerID, StorageID: storage.StorageID,
		StorageGeneration: storage.StorageGeneration, Revision: 1, Fence: "f", HelperGeneration: 1}
	for name, mutate := range map[string]func(*ocihelper.RemovalAttemptManifest){
		"no witness":             func(a *ocihelper.RemovalAttemptManifest) { a.StorageDetachment = nil },
		"extra preparation":      func(a *ocihelper.RemovalAttemptManifest) { a.StoragePreparation = preparation },
		"absent flag":            func(a *ocihelper.RemovalAttemptManifest) { a.StorageAbsent = true },
		"foreign Node":           func(a *ocihelper.RemovalAttemptManifest) { a.StorageDetachment.NodeID = "other" },
		"foreign Computer":       func(a *ocihelper.RemovalAttemptManifest) { a.StorageDetachment.ComputerID = "other" },
		"foreign Storage":        func(a *ocihelper.RemovalAttemptManifest) { a.StorageDetachment.StorageID = "other" },
		"foreign generation":     func(a *ocihelper.RemovalAttemptManifest) { a.StorageDetachment.StorageGeneration = 2 },
		"reap from another boot": func(a *ocihelper.RemovalAttemptManifest) { a.StorageDetachment.AttachedBootSessionID = "old-boot" },
		"older helper session":   func(a *ocihelper.RemovalAttemptManifest) { a.StorageDetachment.HelperGeneration++ },
		"invalid witness":        func(a *ocihelper.RemovalAttemptManifest) { a.StorageDetachment.SweepEpoch = "epoch" },
		"stale cleanup fence":    func(a *ocihelper.RemovalAttemptManifest) { a.Authority.FencingToken = "stale" },
		"prepared attempt ID": func(a *ocihelper.RemovalAttemptManifest) {
			a.Authority.AttemptID = contract.StorageOnlyRemovalAttemptID(storage.StorageGeneration)
		},
		"prepared row with a witness": func(a *ocihelper.RemovalAttemptManifest) {
			a.Authority.AttemptID = contract.StorageOnlyRemovalAttemptID(storage.StorageGeneration)
			a.StoragePreparation = preparation
		},
		"absent row with a witness": func(a *ocihelper.RemovalAttemptManifest) {
			a.Authority.AttemptID = contract.StorageAbsentRemovalAttemptID(storage.StorageGeneration)
			a.StorageAbsent = true
		},
	} {
		engine.inventoryRemoval = row(mutate)
		if _, err := adapter.ReconstructRuntimeRemoval(t.Context(), request); err == nil {
			t.Errorf("%s: detached reconstruction accepted", name)
		}
	}

	// A sweep witness carries a detach across a restart: its last attachment
	// may name another boot.
	engine.inventoryRemoval = row(func(a *ocihelper.RemovalAttemptManifest) {
		a.StorageDetachment.EvidenceKind = contract.ComputerStorageDetachPriorBootSweep
		a.StorageDetachment.SweepEpoch = "epoch"
		a.StorageDetachment.AttachedBootSessionID = "old-boot"
	})
	if _, err := adapter.ReconstructRuntimeRemoval(t.Context(), request); err != nil {
		t.Fatalf("prior-boot sweep witness refused: %v", err)
	}
}
