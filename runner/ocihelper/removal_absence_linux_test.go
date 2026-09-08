//go:build linux

package ocihelper

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// Freeze absence, publish a creator's complete prepared destination and release
// its flock, then replay deletion using that frozen removal inventory.
func TestRemovalAbsenceDoesNotDeletePublishedGeneration(t *testing.T) {
	for _, operation := range []string{"clone", "import", "reset"} {
		t.Run(operation, func(t *testing.T) {
			root, system, source := publishedStorageCopySource(t)
			engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system, storageCopyFinalize: fakeCloneFinalize(t)}
			copyRequest := storageCopyTestRequest(source, operation, source.Receipt.AllocatedSize)
			if operation == "import" {
				externalRoot := filepath.Join(t.TempDir(), "custody")
				exported, err := engine.ExportComputerCustody(t.Context(), custodyExportTestRequest(source, externalRoot))
				if err != nil {
					t.Fatal(err)
				}
				copyRequest.ExportID = exported.Receipt.ExportID
				copyRequest.ExternalPath = externalRoot
				copyRequest.ManifestDigest = exported.Receipt.ManifestDigest
			}
			storage := copyRequest.Destination
			removal := ManagedVolumeRemovalAuthority{NodeID: copyRequest.Authority.NodeID, BootSessionID: "removal-boot", JobID: copyRequest.Authority.JobID, PriorJobID: copyRequest.Authority.JobID, RemovalGeneration: 2, CleanupFence: "removal-fence"}
			inventory, err := engine.inventoryComputerStorageRemoval(t.Context(), InventoryRemovalRequest{Removal: removal, RootInstanceID: copyRequest.Authority.RootInstanceID, ComputerStorage: &storage}, ResourceInventory{})
			if err != nil || len(inventory.Attempts) != 1 || !inventory.Attempts[0].StorageAbsent {
				t.Fatalf("freeze absence: %+v %v", inventory, err)
			}
			if operation == "reset" {
				predecessor := storage
				predecessor.StorageGeneration--
				_, err = engine.prepareResetSuccessor(t.Context(), ResetComputerStorageRequest{Storage: predecessor, NewGeneration: storage.StorageGeneration, Authority: ComputerStorageResetAuthority{NodeID: removal.NodeID, BootSessionID: "reset-boot", HelperGeneration: 1, RootInstanceID: copyRequest.Authority.RootInstanceID, JobID: removal.JobID, PriorJobID: removal.JobID, IntentRevision: storage.IntentRevision, CleanupFence: "reset-fence"}})
			} else {
				_, err = engine.CopyComputerStorage(t.Context(), copyRequest)
			}
			if err != nil {
				t.Fatal(err)
			}
			name, _ := deterministicComputerDiskName(storage)
			diskPath := filepath.Join(root, "computer-disks", name, "disk.ext4")
			before, err := os.ReadFile(diskPath)
			if err != nil {
				t.Fatal(err)
			}
			response, err := engine.DeleteManagedVolume(t.Context(), DeleteManagedVolumeRequest{Kind: ManagedVolumeComputerDisk, ComputerStorage: &storage, Removal: &removal, StorageAbsent: inventory.Attempts[0].StorageAbsent, QuarantineOnFailure: true, FailureAttempts: 3})
			if err == nil || response.Deleted || response.Quarantine != nil {
				t.Errorf("stale absent inventory deleted published %s generation: response=%+v err=%v", operation, response, err)
			}
			after, readErr := os.ReadFile(diskPath)
			if readErr != nil || string(before) != string(after) {
				t.Fatalf("published %s bytes lost: %v", operation, readErr)
			}
		})
	}
}

// A nil containerd client is deliberate: any lease, snapshot, or container
// access panics, so all three generation outcomes prove filesystem-only routing.
func TestInventoryRemovalGenerationDoesNotAccessContainerd(t *testing.T) {
	root, system, source := publishedStorageCopySource(t)
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system, storageCopyFinalize: fakeCloneFinalize(t)}
	copyRequest := storageCopyTestRequest(source, "clone", source.Receipt.AllocatedSize)
	storage := copyRequest.Destination
	request := InventoryRemovalRequest{Removal: ManagedVolumeRemovalAuthority{NodeID: copyRequest.Authority.NodeID, BootSessionID: "removal-boot", JobID: copyRequest.Authority.JobID, PriorJobID: copyRequest.Authority.JobID, RemovalGeneration: 2, CleanupFence: "removal-fence"}, RootInstanceID: copyRequest.Authority.RootInstanceID, ComputerStorage: &storage}
	response, err := engine.InventoryRemoval(t.Context(), request)
	if err != nil || len(response.Attempts) != 1 || !response.Attempts[0].StorageAbsent || response.NoRuntimeAttempts {
		t.Fatalf("absent filesystem-only inventory: %+v %v", response, err)
	}
	if _, err := engine.CopyComputerStorage(t.Context(), copyRequest); err != nil {
		t.Fatal(err)
	}
	response, err = engine.InventoryRemoval(t.Context(), request)
	if err != nil || len(response.Attempts) != 1 || response.Attempts[0].StoragePreparation == nil || response.NoRuntimeAttempts {
		t.Fatalf("prepared filesystem-only inventory: %+v %v", response, err)
	}
	name, _ := deterministicComputerDiskName(storage)
	diskRoot := filepath.Join(root, "computer-disks", name)
	manifest, _, err := readComputerDiskManifest(filepath.Join(diskRoot, "attachment.json"))
	if err != nil {
		t.Fatal(err)
	}
	manifest.PreviousDetachment = &computerDiskEvidence{ReceiptID: "previous"}
	if err := writeComputerDiskManifest(diskRoot, manifest); err != nil {
		t.Fatal(err)
	}
	response, err = engine.InventoryRemoval(t.Context(), request)
	if err != nil || len(response.Attempts) != 0 || !response.NoStorageEvidence || response.NoRuntimeAttempts {
		t.Fatalf("detached filesystem-only inventory: %+v %v", response, err)
	}
	if err := os.Remove(filepath.Join(diskRoot, "disk.ext4")); err != nil {
		t.Fatal(err)
	}
	if response, err = engine.InventoryRemoval(t.Context(), request); err == nil {
		t.Fatalf("filesystem anomaly was discarded with runtime scans: %+v", response)
	}
}

// Pause deletion after its absent-root observation, at the filesystem system
// boundary. Root admission must still exclude destination creation; there is
// no generation flock inode to protect this interval.
func TestAbsentDeletionRetainsRootAdmission(t *testing.T) {
	system := &removalAdmissionDiskSystem{fakeComputerDiskSystem: newFakeComputerDiskSystem()}
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: t.TempDir()}, diskSystem: system}
	storage := testComputerStorage()
	name, _ := deterministicComputerDiskName(storage)
	root := filepath.Join(engine.config.RuntimeRoot, "computer-disks", name)
	checked := false
	system.inspect = func() {
		checked = true
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		lock, err := engine.openComputerStorageDestination(ctx, root)
		if lock != nil {
			closeComputerDiskLock(lock)
		}
		if !errors.Is(err, context.Canceled) {
			t.Errorf("creator entered absent deletion interval: lock=%v err=%v", lock, err)
		}
		if _, err := os.Lstat(root); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("absent deletion allowed a root to appear: %v", err)
		}
	}
	if err := engine.deleteComputerDiskWithAbsence(storage, ManagedVolumeRemovalAuthority{}, true); err != nil {
		t.Fatal(err)
	}
	if !checked {
		t.Fatal("deletion did not reach post-observation filesystem check")
	}
}

type removalAdmissionDiskSystem struct {
	*fakeComputerDiskSystem
	inspect func()
}

func (system *removalAdmissionDiskSystem) mountedSource(path string) (string, bool, error) {
	if system.inspect != nil {
		system.inspect()
	}
	return system.fakeComputerDiskSystem.mountedSource(path)
}

func TestStorageCreatorsReleaseRootAdmissionBeforeGenerationWork(t *testing.T) {
	for _, operation := range []string{"clone", "reset"} {
		t.Run(operation, func(t *testing.T) {
			root, system, source := publishedStorageCopySource(t)
			engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system, storageCopyFinalize: fakeCloneFinalize(t)}
			request := storageCopyTestRequest(source, "clone", source.Receipt.AllocatedSize)
			checked := false
			check := func() {
				checked = true
				if !engine.computerStorageRootMu.TryLock() {
					t.Error("generation work holds root admission")
					return
				}
				engine.computerStorageRootMu.Unlock()
				name, _ := deterministicComputerDiskName(request.Destination)
				lock, err := openComputerDiskLock(filepath.Join(root, "computer-disks", name))
				if lock != nil {
					closeComputerDiskLock(lock)
				}
				if !errors.Is(err, errComputerStorageAttachmentOwned) {
					t.Errorf("generation work lost its flock: %v", err)
				}
			}
			if operation == "clone" {
				engine.storageCopyHook = func(phase computerStorageCopyPhase) error {
					if phase == computerStorageCopyAllocated {
						check()
					}
					return nil
				}
				if _, err := engine.CopyComputerStorage(t.Context(), request); err != nil {
					t.Fatal(err)
				}
			} else {
				engine.storageResetHook = func(phase computerStorageResetPhase) error {
					if phase == computerStorageResetAllocated {
						check()
					}
					return nil
				}
				storage := request.Destination
				storage.StorageGeneration--
				if _, err := engine.prepareResetSuccessor(t.Context(), ResetComputerStorageRequest{Storage: storage, NewGeneration: request.Destination.StorageGeneration, Authority: ComputerStorageResetAuthority{NodeID: request.Authority.NodeID, BootSessionID: "boot", HelperGeneration: 1, RootInstanceID: request.Authority.RootInstanceID, JobID: request.Authority.JobID, IntentRevision: request.Destination.IntentRevision, CleanupFence: "reset-fence"}}); err != nil {
					t.Fatal(err)
				}
			}
			if !checked {
				t.Fatal("creator did not exercise generation work")
			}
		})
	}
}
