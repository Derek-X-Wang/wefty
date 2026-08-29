//go:build linux

package ocihelper

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func publishedStorageCopySource(t *testing.T) (string, *fakeComputerDiskSystem, CreateComputerBackupResponse) {
	t.Helper()
	root, system, storage, authority := prepareDetachedBackupSource(t)
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system}
	response, err := engine.CreateComputerBackup(t.Context(), backupTestRequest(storage, authority))
	if err != nil {
		t.Fatal(err)
	}
	return root, system, response
}

func storageCopyTestRequest(response CreateComputerBackupResponse, operation string, destinationSize int64) CopyComputerStorageRequest {
	receipt := response.Receipt
	request := CopyComputerStorageRequest{Operation: operation, BackupID: receipt.BackupID, CopyID: receipt.CopyID,
		SourceComputerID: receipt.ComputerID, SourceStorageID: receipt.StorageID,
		SourceGeneration: receipt.StorageGeneration, SourceSize: receipt.AllocatedSize, SourceDigest: receipt.ContentDigest,
		Destination: ComputerStorageReference{ComputerID: receipt.ComputerID, StorageID: receipt.StorageID,
			StorageGeneration: receipt.StorageGeneration + 1, IntentRevision: receipt.OperationRevision + 1, DiskBytes: destinationSize},
		Authority: ComputerStorageCopyAuthority{NodeID: receipt.NodeID, BootSessionID: "copy-boot",
			HelperGeneration: 3, RootInstanceID: receipt.RootInstanceID, JobID: receipt.JobID,
			OperationRevision: receipt.OperationRevision + 1, CleanupFence: "storage-copy-fence"}}
	if operation == "clone" {
		request.Destination.ComputerID = "clone-computer"
		request.Destination.StorageID = "clone-storage"
		request.Destination.StorageGeneration = 1
	}
	return request
}

func fakeCloneFinalize(_ *testing.T) func(context.Context, string, string, bool) error {
	return func(_ context.Context, imagePath, _ string, _ bool) error {
		file, err := os.OpenFile(imagePath, os.O_WRONLY, 0)
		if err != nil {
			return err
		}
		_, writeErr := file.WriteAt([]byte("machine-id=rekeyed\nssh-host-keys=removed\n"), 8192)
		return errors.Join(writeErr, file.Sync(), file.Close())
	}
}

func TestComputerStorageCopyResumesEveryCrashBoundaryAndPreservesBrowserBytes(t *testing.T) {
	for _, operation := range []string{"restore", "clone"} {
		for _, checkpoint := range []computerStorageCopyPhase{
			computerStorageCopyReserved, computerStorageCopyAllocated, computerStorageCopyCopied,
			computerStorageCopySourceVerified, computerStorageCopyManifestWritten, computerStorageCopyPublished,
			computerStorageCopyIdentityRekeyed, computerStorageCopyExpanded,
		} {
			if operation == "restore" && (checkpoint == computerStorageCopyIdentityRekeyed || checkpoint == computerStorageCopyExpanded) {
				continue
			}
			t.Run(operation+"/"+string(checkpoint), func(t *testing.T) {
				root, system, source := publishedStorageCopySource(t)
				destinationSize := source.Receipt.AllocatedSize
				if operation == "clone" {
					destinationSize += 1 << 20
				}
				request := storageCopyTestRequest(source, operation, destinationSize)
				engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system,
					storageCopyFinalize: fakeCloneFinalize(t)}
				crash := errors.New("injected Storage copy crash")
				fired := false
				engine.storageCopyHook = func(observed computerStorageCopyPhase) error {
					if observed == checkpoint && !fired {
						fired = true
						return crash
					}
					return nil
				}
				if _, err := engine.CopyComputerStorage(t.Context(), request); !errors.Is(err, crash) {
					t.Fatalf("checkpoint %q error = %v, want injected crash", checkpoint, err)
				}
				name, _ := deterministicComputerDiskName(request.Destination)
				rootPath := filepath.Join(root, "computer-disks", name)
				if _, err := os.Lstat(filepath.Join(rootPath, "storage-copy.json")); err != nil {
					t.Fatalf("checkpoint %q left untracked destination: %v", checkpoint, err)
				}
				engine = &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system,
					storageCopyFinalize: fakeCloneFinalize(t)}
				response, err := engine.CopyComputerStorage(t.Context(), request)
				if err != nil || response.Receipt.Kind != "computer_storage_copy_verified" ||
					response.Receipt.OSIdentityRekeyed != (operation == "clone") ||
					response.Receipt.FilesystemExpanded != (operation == "clone") ||
					response.Receipt.SourceDigest != source.Receipt.ContentDigest {
					t.Fatalf("resumed Storage copy = %+v err=%v", response, err)
				}
				payload, err := os.ReadFile(filepath.Join(rootPath, "disk.ext4"))
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Contains(payload, []byte("browser-secret=survives")) ||
					!bytes.Contains(payload, []byte("user-marker=alice")) ||
					!bytes.Contains(payload, []byte("old-credential=copied-but-not-authority")) {
					t.Fatal("Storage copy did not preserve user, browser, and deliberately copied old credential bytes")
				}
				if operation == "clone" && (!bytes.Contains(payload, []byte("machine-id=rekeyed")) ||
					!bytes.Contains(payload, []byte("ssh-host-keys=removed"))) {
					t.Fatal("clone did not narrowly rekey OS identity")
				}
			})
		}
	}
}

func TestComputerRestoreRejectsDigestMismatchAndTruncationBeforeDestinationPublication(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(string, CreateComputerBackupResponse) error
	}{
		{name: "digest mismatch", mutate: func(root string, response CreateComputerBackupResponse) error {
			name, _ := deterministicComputerBackupCopyName(response.Receipt.CopyID)
			file, err := os.OpenFile(filepath.Join(root, "computer-backups", name, "backup.ext4"), os.O_WRONLY, 0)
			if err != nil {
				return err
			}
			_, writeErr := file.WriteAt([]byte("tampered"), 4096)
			return errors.Join(writeErr, file.Close())
		}},
		{name: "truncation", mutate: func(root string, response CreateComputerBackupResponse) error {
			name, _ := deterministicComputerBackupCopyName(response.Receipt.CopyID)
			return os.Truncate(filepath.Join(root, "computer-backups", name, "backup.ext4"), response.Receipt.AllocatedSize-1)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			root, system, source := publishedStorageCopySource(t)
			if err := test.mutate(root, source); err != nil {
				t.Fatal(err)
			}
			request := storageCopyTestRequest(source, "restore", source.Receipt.AllocatedSize)
			engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system}
			if _, err := engine.CopyComputerStorage(t.Context(), request); err == nil {
				t.Fatal("corrupt Backup source was restored")
			}
			name, _ := deterministicComputerDiskName(request.Destination)
			if _, err := os.Lstat(filepath.Join(root, "computer-disks", name, "disk.ext4")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("corrupt source published destination: %v", err)
			}
		})
	}
}
