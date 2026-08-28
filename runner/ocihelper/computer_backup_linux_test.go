//go:build linux

package ocihelper

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func prepareDetachedBackupSource(t *testing.T) (string, *fakeComputerDiskSystem, ComputerStorageReference, AttemptAuthority) {
	t.Helper()
	root := t.TempDir()
	system := newFakeComputerDiskSystem()
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system}
	storage := testComputerStorage()
	authority := testComputerAuthority("backup-source", "source-fence", "boot-a")
	attachment, err := engine.attachComputerDisk(t.Context(), storage, authority)
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(attachment.imagePath, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	markers := []byte("browser-secret=survives\nuser-marker=alice\n")
	if _, err := file.WriteAt(markers, 4096); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := engine.detachComputerDisk(attachment, computerDiskReapReceipt, ""); err != nil {
		t.Fatal(err)
	}
	return root, system, storage, authority
}

func backupTestRequest(storage ComputerStorageReference, sourceAuthority AttemptAuthority) CreateComputerBackupRequest {
	storage.IntentRevision = 2
	return CreateComputerBackupRequest{BackupID: "backup-1", CopyID: "copy-1", Storage: storage,
		Authority: ComputerBackupAuthority{NodeID: sourceAuthority.NodeID, BootSessionID: sourceAuthority.BootSessionID,
			HelperGeneration: 1, RootInstanceID: "managed-root-1", JobID: sourceAuthority.JobID,
			OperationRevision: 2, CleanupFence: "backup-fence-1"}}
}

func TestComputerBackupResumesEveryTrackedCrashBoundary(t *testing.T) {
	for _, checkpoint := range []computerBackupCheckpoint{
		computerBackupReserved, computerBackupAllocated, computerBackupCopied,
		computerBackupDigested, computerBackupManifestWritten, computerBackupPublished,
	} {
		t.Run(string(checkpoint), func(t *testing.T) {
			root, system, storage, sourceAuthority := prepareDetachedBackupSource(t)
			request := backupTestRequest(storage, sourceAuthority)
			diskName, _ := deterministicComputerDiskName(storage)
			sourcePath := filepath.Join(root, "computer-disks", diskName, "disk.ext4")
			sourceBefore, err := digestFile(sourcePath)
			if err != nil {
				t.Fatal(err)
			}
			engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system}
			crash := errors.New("injected Backup crash")
			fired := false
			engine.computerBackupHook = func(observed computerBackupCheckpoint) error {
				if observed == checkpoint && !fired {
					fired = true
					return crash
				}
				return nil
			}
			if _, err := engine.CreateComputerBackup(t.Context(), request); !errors.Is(err, crash) {
				t.Fatalf("checkpoint %q error = %v, want injected crash", checkpoint, err)
			}
			copyName, _ := deterministicComputerBackupCopyName(request.CopyID)
			copyRoot := filepath.Join(root, "computer-backups", copyName)
			if _, err := os.Lstat(filepath.Join(copyRoot, "copy.json")); err != nil {
				t.Fatalf("checkpoint %q left untracked copy state: %v", checkpoint, err)
			}
			sourceAfterCrash, err := digestFile(sourcePath)
			if err != nil || sourceAfterCrash != sourceBefore {
				t.Fatalf("checkpoint %q mutated source: before=%s after=%s err=%v", checkpoint, sourceBefore, sourceAfterCrash, err)
			}

			engine = &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system}
			response, err := engine.CreateComputerBackup(t.Context(), request)
			if err != nil || response.Receipt.Kind != "computer_backup_copy_verified" ||
				response.Receipt.ContentDigest == "" || response.Receipt.Encryption != "none" || response.Receipt.CopyAbsent {
				t.Fatalf("resumed Backup = %+v err=%v", response, err)
			}
			payload, err := os.ReadFile(filepath.Join(copyRoot, "backup.ext4"))
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Contains(payload, []byte("browser-secret=survives")) || !bytes.Contains(payload, []byte("user-marker=alice")) {
				t.Fatal("Backup lost tenant browser-secret or user marker")
			}
			if bytes.Contains(payload, []byte("fencing_token")) || bytes.Contains(payload, []byte("cleanup_fence")) ||
				bytes.Contains(payload, []byte("root_instance_id")) {
				t.Fatal("Backup disk contains wefty authority artifacts")
			}
			for _, path := range []string{copyRoot, filepath.Join(copyRoot, "copy.json"), filepath.Join(copyRoot, "backup.ext4")} {
				info, err := os.Stat(path)
				if err != nil {
					t.Fatal(err)
				}
				want := os.FileMode(0o600)
				if info.IsDir() {
					want = 0o700
				}
				if info.Mode().Perm() != want {
					t.Fatalf("Backup permission %s = %o, want %o", path, info.Mode().Perm(), want)
				}
			}
			sourceAfterResume, err := digestFile(sourcePath)
			if err != nil || sourceAfterResume != sourceBefore {
				t.Fatalf("resumed checkpoint %q mutated source: before=%s after=%s err=%v", checkpoint, sourceBefore, sourceAfterResume, err)
			}
		})
	}
}

func TestComputerBackupENOSPCAndDigestMismatchLeaveNoBackupOrSourceMutation(t *testing.T) {
	for _, test := range []struct {
		name      string
		configure func(*ContainerdEngine, string, CreateComputerBackupRequest)
		wantCode  string
	}{
		{name: "ENOSPC", wantCode: "insufficient_disk", configure: func(engine *ContainerdEngine, _ string, _ CreateComputerBackupRequest) {
			engine.computerBackupAllocate = func(string, int64) error { return syscall.ENOSPC }
		}},
		{name: "digest mismatch", wantCode: "digest_mismatch", configure: func(engine *ContainerdEngine, root string, request CreateComputerBackupRequest) {
			engine.computerBackupHook = func(checkpoint computerBackupCheckpoint) error {
				if checkpoint != computerBackupDigested {
					return nil
				}
				name, _ := deterministicComputerBackupCopyName(request.CopyID)
				path := filepath.Join(root, "computer-backups", name, "backup.ext4.staging")
				file, err := os.OpenFile(path, os.O_WRONLY, 0)
				if err != nil {
					return err
				}
				_, err = file.WriteAt([]byte("tampered"), 4096)
				return errors.Join(err, file.Close())
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			root, system, storage, sourceAuthority := prepareDetachedBackupSource(t)
			request := backupTestRequest(storage, sourceAuthority)
			diskName, _ := deterministicComputerDiskName(storage)
			sourcePath := filepath.Join(root, "computer-disks", diskName, "disk.ext4")
			before, err := digestFile(sourcePath)
			if err != nil {
				t.Fatal(err)
			}
			engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system}
			test.configure(engine, root, request)
			response, err := engine.CreateComputerBackup(t.Context(), request)
			if err != nil || response.Receipt.Kind != "computer_backup_copy_failed_absent" ||
				response.Receipt.FailureCode != test.wantCode || !response.Receipt.CopyAbsent {
				t.Fatalf("typed Backup failure = %+v err=%v", response, err)
			}
			copyName, _ := deterministicComputerBackupCopyName(request.CopyID)
			if _, err := os.Lstat(filepath.Join(root, "computer-backups", copyName)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("failed Backup copy remains: %v", err)
			}
			after, err := digestFile(sourcePath)
			if err != nil || after != before {
				t.Fatalf("failed Backup mutated source: before=%s after=%s err=%v", before, after, err)
			}
		})
	}
}

func TestComputerBackupRequiresDetachmentAndPruneBindsAbsence(t *testing.T) {
	root := t.TempDir()
	system := newFakeComputerDiskSystem()
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system}
	storage := testComputerStorage()
	sourceAuthority := testComputerAuthority("live-source", "source-fence", "boot-a")
	attachment, err := engine.attachComputerDisk(t.Context(), storage, sourceAuthority)
	if err != nil {
		t.Fatal(err)
	}
	request := backupTestRequest(storage, sourceAuthority)
	if _, err := engine.CreateComputerBackup(t.Context(), request); err == nil {
		t.Fatal("attached Computer Storage was backed up")
	}
	if err := engine.detachComputerDisk(attachment, computerDiskReapReceipt, ""); err != nil {
		t.Fatal(err)
	}
	created, err := engine.CreateComputerBackup(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	deleteRequest := DeleteComputerBackupCopyRequest{BackupID: request.BackupID, CopyID: request.CopyID,
		Storage: request.Storage, Authority: request.Authority}
	deleteRequest.Authority.CleanupFence = "prune-fence"
	for name, mutate := range map[string]func(*DeleteComputerBackupCopyRequest){
		"backup":  func(r *DeleteComputerBackupCopyRequest) { r.BackupID = "other" },
		"storage": func(r *DeleteComputerBackupCopyRequest) { r.Storage.StorageGeneration++ },
		"node":    func(r *DeleteComputerBackupCopyRequest) { r.Authority.NodeID = "other" },
		"root":    func(r *DeleteComputerBackupCopyRequest) { r.Authority.RootInstanceID = "other" },
	} {
		t.Run("reject "+name, func(t *testing.T) {
			mutated := deleteRequest
			mutate(&mutated)
			if _, err := engine.DeleteComputerBackupCopy(t.Context(), mutated); err == nil {
				t.Fatal("mutated Backup copy removal authority was accepted")
			}
			copyName, _ := deterministicComputerBackupCopyName(request.CopyID)
			if _, err := os.Lstat(filepath.Join(root, "computer-backups", copyName, "copy.json")); err != nil {
				t.Fatalf("rejected removal deleted tracked copy: %v", err)
			}
		})
	}
	removed, err := engine.DeleteComputerBackupCopy(t.Context(), deleteRequest)
	if err != nil || !removed.Receipt.Absent || removed.Receipt.Kind != "computer_backup_copy_removed" ||
		removed.Receipt.CopyID != created.Receipt.CopyID || removed.Receipt.RootInstanceID != request.Authority.RootInstanceID ||
		removed.Receipt.CleanupFence != "prune-fence" {
		t.Fatalf("Backup prune receipt = %+v err=%v", removed, err)
	}
	copyName, _ := deterministicComputerBackupCopyName(request.CopyID)
	if _, err := os.Lstat(filepath.Join(root, "computer-backups", copyName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("pruned Backup remains: %v", err)
	}
}
