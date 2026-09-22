//go:build linux

package oci

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

// Use real filesystem inventory and deletion behind the adapter's test RPC
// server. Runtime operations stay fake; a refused clone never ran a payload.
type tombstoneRemovalEngine struct {
	*adapterTestEngine
	disks *ocihelper.ContainerdEngine
}

func (engine *tombstoneRemovalEngine) InventoryRemoval(ctx context.Context, request ocihelper.InventoryRemovalRequest) (ocihelper.InventoryRemovalResponse, error) {
	return engine.disks.InventoryRemoval(ctx, request)
}

func (engine *tombstoneRemovalEngine) DeleteManagedVolume(ctx context.Context, request ocihelper.DeleteManagedVolumeRequest) (ocihelper.DeleteManagedVolumeResponse, error) {
	return engine.disks.DeleteManagedVolume(ctx, request)
}

func TestAdapterTombstoneRemovalCarriesFrozenAbsenceThroughFinalization(t *testing.T) {
	root := t.TempDir()
	disks, err := ocihelper.NewContainerdEngine(ocihelper.NativeEngineConfig{
		RuntimeRoot: root, Address: filepath.Join(root, "unused-containerd.sock"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer disks.Close()
	storage := ocihelper.ComputerStorageReference{ComputerID: "refused-clone", StorageID: "refused-storage",
		StorageGeneration: 1, IntentRevision: 1, DiskBytes: 4096}
	name, err := ocihelper.DeterministicComputerDiskName(storage)
	if err != nil {
		t.Fatal(err)
	}
	diskRoot := filepath.Join(root, "computer-disks", name)
	if err := os.MkdirAll(diskRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(diskRoot, "attachment.lock"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	// Durable state left by a completed refusal, before the agent returned
	// with the subsequent removal directive (whose fence is different).
	receipt := ocihelper.ComputerStorageCopyReceipt{
		Kind: "computer_storage_copy_failed_absent", ReceiptID: "refusal-receipt", Operation: "clone",
		BackupID: "backup", CopyID: "copy", SourceComputerID: "source", SourceStorageID: "source-storage",
		SourceGeneration: 1, DestinationComputerID: storage.ComputerID, DestinationStorageID: storage.StorageID,
		DestinationGeneration: 1, NodeID: "node", RootInstanceID: "root", JobID: "refused-job",
		OperationRevision: 1, CleanupFence: "copy-fence", HelperGeneration: 1, SourceSize: 4096,
		DestinationSize: 4096, SourceDigest: adapterTestDigest, FailureCode: "insufficient_disk", DestinationAbsent: true,
	}
	payload, err := json.Marshal(map[string]any{"version": 1, "disk_name": name, "storage": storage,
		"receipt": receipt, "refused_at": time.Now().UTC()})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(diskRoot, "copy-refused.json"), payload, 0o600); err != nil {
		t.Fatal(err)
	}
	adapter, closeAdapter := startAdapterTestServer(t, &tombstoneRemovalEngine{adapterTestEngine: &adapterTestEngine{}, disks: disks})
	defer closeAdapter()
	request := workloadrunner.RuntimeRemovalProofRequest{NodeID: "node", BootSessionID: "boot", RootInstanceID: "root",
		JobID: "refused-job", RemovalGeneration: 2, CleanupFence: "removal-fence",
		ComputerStorage: &workloadrunner.ComputerStorage{ComputerID: storage.ComputerID, StorageID: storage.StorageID,
			StorageGeneration: storage.StorageGeneration, DiskBytes: storage.DiskBytes}}
	attempts, err := adapter.ReconstructRuntimeRemoval(t.Context(), request)
	if err != nil || len(attempts) != 1 || !attempts[0].StorageOnly || !attempts[0].StorageAbsent || attempts[0].StoragePreparation != nil {
		t.Fatalf("tombstone reconstruction = %+v err=%v", attempts, err)
	}
	if attempts[0].JobID != request.JobID || attempts[0].FencingToken != request.CleanupFence {
		t.Fatalf("tombstone inventory lost removal authority: %+v", attempts)
	}
	if err := adapter.FinalizeManagedVolumes(t.Context(), workloadrunner.ManagedVolumeFinalizationRequest{
		Volumes: []workloadrunner.ManagedVolume{{Kind: workloadrunner.ManagedVolumeComputerDisk,
			ComputerStorage: attempts[0].ComputerStorage, StorageAbsent: attempts[0].StorageAbsent}},
		Removal: &workloadrunner.ManagedVolumeRemovalAuthority{NodeID: request.NodeID, BootSessionID: request.BootSessionID,
			JobID: request.JobID, PriorJobID: request.JobID, RemovalGeneration: request.RemovalGeneration, CleanupFence: request.CleanupFence},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(diskRoot); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("adapter finalization retained refused root: %v", err)
	}
}

// `current_job_id` rotates on reconfiguration, so the Job that refused a clone
// is the removal's prior Job rather than its current one. The adapter has to
// carry that prior Job into removal inventory, or the helper's acceptance of
// it can never be reached in production.
func TestAdapterTombstoneReconstructionCarriesThePriorJob(t *testing.T) {
	root := t.TempDir()
	disks, err := ocihelper.NewContainerdEngine(ocihelper.NativeEngineConfig{
		RuntimeRoot: root, Address: filepath.Join(root, "unused-containerd.sock"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer disks.Close()
	storage := ocihelper.ComputerStorageReference{ComputerID: "rotated-clone", StorageID: "rotated-storage",
		StorageGeneration: 1, IntentRevision: 1, DiskBytes: 4096}
	name, err := ocihelper.DeterministicComputerDiskName(storage)
	if err != nil {
		t.Fatal(err)
	}
	diskRoot := filepath.Join(root, "computer-disks", name)
	if err := os.MkdirAll(diskRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(diskRoot, "attachment.lock"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	receipt := ocihelper.ComputerStorageCopyReceipt{
		Kind: "computer_storage_copy_failed_absent", ReceiptID: "refusal-receipt", Operation: "clone",
		BackupID: "backup", CopyID: "copy", SourceComputerID: "source", SourceStorageID: "source-storage",
		SourceGeneration: 1, DestinationComputerID: storage.ComputerID, DestinationStorageID: storage.StorageID,
		DestinationGeneration: 1, NodeID: "node", RootInstanceID: "root", JobID: "refused-job",
		OperationRevision: 1, CleanupFence: "copy-fence", HelperGeneration: 1, SourceSize: 4096,
		DestinationSize: 4096, SourceDigest: adapterTestDigest, FailureCode: "insufficient_disk", DestinationAbsent: true,
	}
	payload, err := json.Marshal(map[string]any{"version": 1, "disk_name": name, "storage": storage,
		"receipt": receipt, "refused_at": time.Now().UTC()})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(diskRoot, "copy-refused.json"), payload, 0o600); err != nil {
		t.Fatal(err)
	}
	adapter, closeAdapter := startAdapterTestServer(t, &tombstoneRemovalEngine{adapterTestEngine: &adapterTestEngine{}, disks: disks})
	defer closeAdapter()
	// The Computer was reconfigured after the refusal: its current Job is new.
	request := workloadrunner.RuntimeRemovalProofRequest{NodeID: "node", BootSessionID: "boot", RootInstanceID: "root",
		JobID: "rotated-job", PriorJobID: "refused-job", RemovalGeneration: 2, CleanupFence: "removal-fence",
		ComputerStorage: &workloadrunner.ComputerStorage{ComputerID: storage.ComputerID, StorageID: storage.StorageID,
			StorageGeneration: storage.StorageGeneration, DiskBytes: storage.DiskBytes}}
	attempts, err := adapter.ReconstructRuntimeRemoval(t.Context(), request)
	if err != nil || len(attempts) != 1 || !attempts[0].StorageOnly || !attempts[0].StorageAbsent {
		t.Fatalf("rotated-Job tombstone reconstruction = %+v err=%v", attempts, err)
	}
	if attempts[0].JobID != request.JobID {
		t.Fatalf("reconstruction lost the current removal Job: %+v", attempts)
	}
	// Without the prior Job the refusing Job matches nothing the removal names.
	withoutPrior := request
	withoutPrior.PriorJobID = ""
	if _, err := adapter.ReconstructRuntimeRemoval(t.Context(), withoutPrior); err == nil {
		t.Error("reconstruction accepted a tombstone bound to neither named Job")
	}
}
