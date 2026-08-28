//go:build linux

package ocihelper

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestComputerStorageResetResumesEveryDeleteBoundaryAndRetiresOldGeneration(t *testing.T) {
	for _, checkpoint := range []computerStorageResetPhase{
		computerStorageResetPrepared,
		computerStorageResetQuarantined,
		computerStorageResetDeleted,
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
			if err := os.WriteFile(credentialPath, []byte("must-be-unreachable"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := engine.detachComputerDisk(attachment, computerDiskReapReceipt, ""); err != nil {
				t.Fatal(err)
			}
			// Reconstruct the engine to model a process crash after durable
			// detachment and before reset reservation reaches the helper.
			engine = &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system}
			crash := errors.New("injected crash")
			fired := false
			engine.storageResetHook = func(observed computerStorageResetPhase) error {
				if observed == checkpoint && !fired {
					fired = true
					return crash
				}
				return nil
			}
			request := ResetComputerStorageRequest{Storage: storage, NewGeneration: 2,
				Authority: ComputerStorageResetAuthority{NodeID: authority.NodeID, BootSessionID: authority.BootSessionID,
					HelperGeneration: 1, JobID: authority.JobID, IntentRevision: 2, CleanupFence: "reset-fence"}}
			request.Storage.IntentRevision = request.Authority.IntentRevision
			if _, err := engine.ResetComputerStorage(t.Context(), request); !errors.Is(err, crash) {
				t.Fatalf("checkpoint %q error = %v, want injected crash", checkpoint, err)
			}
			engine.storageResetHook = nil
			request.Authority.BootSessionID = "boot-b"
			request.Authority.HelperGeneration = 2
			response, err := engine.ResetComputerStorage(t.Context(), request)
			if err != nil || !response.Verified || response.Receipt.ReceiptID == "" {
				t.Fatalf("resumed reset = %+v err=%v", response, err)
			}
			expectedHelperGeneration := uint64(2)
			if checkpoint == computerStorageResetVerified {
				expectedHelperGeneration = 1
			}
			if response.Receipt.HelperGeneration != expectedHelperGeneration {
				t.Fatalf("receipt helper generation = %d, want %d", response.Receipt.HelperGeneration, expectedHelperGeneration)
			}
			request.Authority.HelperGeneration = 3
			replayed, err := engine.ResetComputerStorage(t.Context(), request)
			if err != nil || replayed.Receipt.ReceiptID != response.Receipt.ReceiptID ||
				replayed.Receipt.HelperGeneration != expectedHelperGeneration {
				t.Fatalf("reset replay = %+v err=%v", replayed, err)
			}
			if _, err := os.Lstat(credentialPath); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("old credential remained reachable: %v", err)
			}
			if _, err := engine.attachComputerDisk(t.Context(), storage, testComputerAuthority("stale", "stale", "boot-a")); err == nil {
				t.Fatal("retired Storage generation was recreated by a stale attach")
			}
			fresh := storage
			fresh.StorageGeneration = 2
			freshAttachment, err := engine.attachComputerDisk(t.Context(), fresh, testComputerAuthority("attempt-b", "fence-b", "boot-a"))
			if err != nil {
				t.Fatalf("new Storage generation did not attach: %v", err)
			}
			if freshAttachment.storage.StorageGeneration != 2 {
				t.Fatalf("fresh attachment = %+v", freshAttachment.storage)
			}
		})
	}
}

func TestComputerStorageResetRejectsStaleRevisionAndAttachedGeneration(t *testing.T) {
	root := t.TempDir()
	system := newFakeComputerDiskSystem()
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system}
	storage := testComputerStorage()
	authority := testComputerAuthority("attempt-a", "fence-a", "boot-a")
	attachment, err := engine.attachComputerDisk(t.Context(), storage, authority)
	if err != nil {
		t.Fatal(err)
	}
	request := ResetComputerStorageRequest{Storage: storage, NewGeneration: 2,
		Authority: ComputerStorageResetAuthority{NodeID: authority.NodeID, BootSessionID: authority.BootSessionID,
			HelperGeneration: 1, JobID: authority.JobID, IntentRevision: 2, CleanupFence: "reset-fence"}}
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
	request.Authority.CleanupFence = "newer-fence"
	if _, err := engine.ResetComputerStorage(t.Context(), request); err == nil {
		t.Fatal("stale reset manifest advanced a newer revision")
	}
}
