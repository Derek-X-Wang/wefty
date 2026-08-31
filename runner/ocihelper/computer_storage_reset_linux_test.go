//go:build linux

package ocihelper

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"syscall"
	"testing"
)

func resetTestRequest(storage ComputerStorageReference, authority AttemptAuthority) ResetComputerStorageRequest {
	return ResetComputerStorageRequest{Storage: storage, NewGeneration: storage.StorageGeneration + 1,
		Authority: ComputerStorageResetAuthority{NodeID: authority.NodeID, BootSessionID: authority.BootSessionID,
			HelperGeneration: 1, RootInstanceID: "managed-root-a", JobID: "reset-job", PriorJobID: authority.JobID,
			IntentRevision: storage.IntentRevision + 1, CleanupFence: "reset-fence"}}
}

func TestComputerStorageResetBindsSweepReceiptToNamedPriorJob(t *testing.T) {
	for _, test := range []struct {
		name          string
		priorJobID    string
		mutateReceipt func(*computerDiskEvidence)
		wantErr       bool
	}{
		{name: "same-boot helper sweep", priorJobID: "job-1"},
		{name: "wrong prior Job", priorJobID: "job-other", wantErr: true},
		{name: "unnamed prior Job", wantErr: true},
		{name: "missing receipt capability", priorJobID: "job-1", mutateReceipt: func(evidence *computerDiskEvidence) { evidence.ReceiptID = "" }, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			engine, storage, prior := prepareSameBootSweptComputerDisk(t)
			if test.mutateReceipt != nil {
				name, _ := deterministicComputerDiskName(storage)
				root := filepath.Join(engine.config.RuntimeRoot, "computer-disks", name)
				manifest, present, err := readComputerDiskManifest(filepath.Join(root, "attachment.json"))
				if err != nil || !present || manifest.PreviousDetachment == nil {
					t.Fatalf("swept manifest = %+v present=%t err=%v", manifest, present, err)
				}
				test.mutateReceipt(manifest.PreviousDetachment)
				if err := writeComputerDiskManifest(root, manifest); err != nil {
					t.Fatal(err)
				}
			}
			request := resetTestRequest(storage, prior)
			request.Authority.PriorJobID = test.priorJobID
			request.Storage.IntentRevision = request.Authority.IntentRevision
			_, err := engine.ResetComputerStorage(t.Context(), request)
			if (err != nil) != test.wantErr {
				t.Fatalf("reset Computer Storage error = %v, wantErr=%t", err, test.wantErr)
			}
		})
	}
}

func TestComputerStorageResetResumesEveryPreparationBoundaryThenUsesSharedRemoval(t *testing.T) {
	for _, checkpoint := range []computerStorageResetPhase{
		computerStorageResetRetirementFenced,
		computerStorageResetManifestWritten,
		computerStorageResetAllocated,
		computerStorageResetImagePublished,
		computerStorageResetVerified,
	} {
		t.Run(string(checkpoint), func(t *testing.T) {
			root := t.TempDir()
			system := newFakeComputerDiskSystem()
			engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system}
			storage := testComputerStorage()
			authority := testComputerAuthority("attempt-a", "fence-a", "boot-a")
			attachment, err := engine.attachComputerDisk(t.Context(), storage, authority)
			if err != nil {
				t.Fatal(err)
			}
			credentialPath := filepath.Join(filepath.Dir(attachment.imagePath), "planted-credential")
			if err := os.WriteFile(credentialPath, []byte("old-generation"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := engine.detachComputerDisk(attachment, computerDiskReapReceipt, ""); err != nil {
				t.Fatal(err)
			}
			request := resetTestRequest(storage, authority)
			request.Storage.IntentRevision = request.Authority.IntentRevision
			crash := errors.New("injected crash")
			fired := false
			engine.storageResetHook = func(observed computerStorageResetPhase) error {
				if observed == checkpoint && !fired {
					fired = true
					return crash
				}
				return nil
			}
			if _, err := engine.ResetComputerStorage(t.Context(), request); !errors.Is(err, crash) {
				t.Fatalf("checkpoint %q error = %v, want injected crash", checkpoint, err)
			}

			engine = &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system}
			request.Authority.BootSessionID = "boot-b"
			request.Authority.HelperGeneration = 2
			response, err := engine.ResetComputerStorage(t.Context(), request)
			if err != nil || !response.Verified || response.Receipt.ReceiptID == "" {
				t.Fatalf("resumed preparation = %+v err=%v", response, err)
			}
			expectedHelperGeneration := uint64(2)
			if checkpoint == computerStorageResetVerified {
				expectedHelperGeneration = 1
			}
			if response.Receipt.HelperGeneration != expectedHelperGeneration ||
				response.Receipt.RootInstanceID != request.Authority.RootInstanceID {
				t.Fatalf("prepared receipt = %+v", response.Receipt)
			}
			if _, err := os.Lstat(credentialPath); err != nil {
				t.Fatalf("pre-publication reset destroyed old bytes: %v", err)
			}
			if _, err := engine.attachComputerDisk(t.Context(), storage,
				testComputerAuthority("stale", "stale", "boot-b")); err == nil {
				t.Fatal("retirement-fenced Storage generation was reattached")
			}

			fresh := storage
			fresh.StorageGeneration++
			fresh.IntentRevision = request.Authority.IntentRevision
			freshName, _ := deterministicComputerDiskName(fresh)
			if err := verifyComputerDiskAllocation(filepath.Join(root, "computer-disks", freshName, "disk.ext4"), fresh.DiskBytes); err != nil {
				t.Fatalf("successor was not fully allocated: %v", err)
			}

			removal := ManagedVolumeRemovalAuthority{NodeID: request.Authority.NodeID,
				BootSessionID: request.Authority.BootSessionID, JobID: request.Authority.JobID, PriorJobID: authority.JobID,
				RemovalGeneration: uint64(request.Authority.IntentRevision), CleanupFence: request.Authority.CleanupFence}
			oldName, _ := deterministicComputerDiskName(storage)
			legacyManifestRoot := filepath.Join(root, "computer-storage-resets")
			legacyQuarantineRoot := filepath.Join(root, "computer-disk-quarantine", oldName+"-reset-2")
			if err := os.MkdirAll(legacyManifestRoot, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(legacyManifestRoot, oldName+".json"), []byte("{}\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.MkdirAll(legacyQuarantineRoot, 0o700); err != nil {
				t.Fatal(err)
			}
			before := ResourceInventory{}
			if err := engine.inventoryComputerDiskResources(&before); err != nil ||
				!slices.Contains(before.ComputerResetManifests, oldName) || !slices.Contains(before.ComputerQuarantines, oldName) {
				t.Fatalf("legacy reset residue was not inventoried: %+v err=%v", before, err)
			}
			if err := engine.deleteComputerDisk(storage, removal); err != nil {
				t.Fatalf("shared disk removal failed: %v", err)
			}
			after := ResourceInventory{}
			if err := engine.inventoryComputerDiskResources(&after); err != nil ||
				slices.Contains(after.ComputerResetManifests, oldName) || slices.Contains(after.ComputerQuarantines, oldName) {
				t.Fatalf("legacy reset residue survived shared removal: %+v err=%v", after, err)
			}
			resources := expectedComputerStorageRemovalResources(&storage)
			attestation, err := attestRemovalInventory(ResourceInventory{}, AttestRemovalRequest{
				JobID: request.Authority.JobID, RemovalGeneration: "2",
				Attempts: []RemovalAttemptManifest{{StorageOnly: true, ComputerStorage: &storage, Resources: resources}},
			})
			if err != nil || len(attestation.Assertions) != len(resources) {
				t.Fatalf("shared removal attestation = %+v err=%v", attestation, err)
			}
			if _, err := os.Lstat(filepath.Join(root, "computer-disks", oldName)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("old generation survived shared removal: %v", err)
			}
		})
	}
}

func TestComputerStorageResetRefusesAttachedGenerationAndAuthorityReuse(t *testing.T) {
	root := t.TempDir()
	system := newFakeComputerDiskSystem()
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system}
	storage := testComputerStorage()
	authority := testComputerAuthority("attempt-a", "fence-a", "boot-a")
	attachment, err := engine.attachComputerDisk(t.Context(), storage, authority)
	if err != nil {
		t.Fatal(err)
	}
	request := resetTestRequest(storage, authority)
	request.Storage.IntentRevision = request.Authority.IntentRevision
	if _, err := engine.ResetComputerStorage(t.Context(), request); err == nil {
		t.Fatal("attached Computer Storage generation was reset")
	}
	if err := engine.detachComputerDisk(attachment, computerDiskReapReceipt, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.ResetComputerStorage(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	request.Authority.IntentRevision++
	request.Storage.IntentRevision++
	request.Authority.CleanupFence = "different-fence"
	if _, err := engine.ResetComputerStorage(t.Context(), request); err == nil {
		t.Fatal("standing generation preparation accepted different authority")
	}
}

func TestComputerStorageResetENOSPCLeavesPredecessorCurrentAndSuccessorTracked(t *testing.T) {
	root := t.TempDir()
	system := newFakeComputerDiskSystem()
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system}
	storage := testComputerStorage()
	authority := testComputerAuthority("attempt-a", "fence-a", "boot-a")
	attachment, err := engine.attachComputerDisk(t.Context(), storage, authority)
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.detachComputerDisk(attachment, computerDiskReapReceipt, ""); err != nil {
		t.Fatal(err)
	}
	oldImage := attachment.imagePath
	system.allocationErr = syscall.ENOSPC
	request := resetTestRequest(storage, authority)
	request.Storage.IntentRevision = request.Authority.IntentRevision
	if _, err := engine.ResetComputerStorage(t.Context(), request); !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("reset allocation failure = %v, want ENOSPC", err)
	}
	if _, err := os.Lstat(oldImage); err != nil {
		t.Fatalf("ENOSPC destroyed predecessor image: %v", err)
	}
	successor := storage
	successor.StorageGeneration = request.NewGeneration
	successor.IntentRevision = request.Authority.IntentRevision
	successorName, _ := deterministicComputerDiskName(successor)
	successorRoot := filepath.Join(root, "computer-disks", successorName)
	manifest, present, err := readComputerDiskManifest(filepath.Join(successorRoot, "attachment.json"))
	if err != nil || !present || manifest.Preparation == nil || manifest.Prepared {
		t.Fatalf("ENOSPC successor staging manifest = %+v present=%t err=%v", manifest, present, err)
	}
	if _, err := os.Lstat(filepath.Join(successorRoot, "disk.ext4")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("ENOSPC published successor image: %v", err)
	}
	if staging, err := filepath.Glob(filepath.Join(successorRoot, ".disk.ext4.tmp-*")); err != nil || len(staging) != 0 {
		t.Fatalf("ENOSPC left allocation staging: %v err=%v", staging, err)
	}
}
