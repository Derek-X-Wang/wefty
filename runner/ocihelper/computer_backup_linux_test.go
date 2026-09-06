//go:build linux

package ocihelper

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestDigestFileHonorsRPCCancellation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "disk.ext4")
	if err := os.WriteFile(path, make([]byte, 1<<20), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := digestFile(ctx, path); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled digest = %v", err)
	}
}

type cancelAfterChecksContext struct {
	context.Context
	cancelAt int
	checks   int
}

func (ctx *cancelAfterChecksContext) Err() error {
	ctx.checks++
	if ctx.checks >= ctx.cancelAt {
		return context.Canceled
	}
	return nil
}

func TestComputerStorageDigestsHonorCancellationAfterMultipleChunks(t *testing.T) {
	path := filepath.Join(t.TempDir(), "disk.ext4")
	payload := make([]byte, 4*256*1024)
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	for name, digest := range map[string]func(context.Context) (string, error){
		"full file": func(ctx context.Context) (string, error) { return digestFile(ctx, path) },
		"prefix":    func(ctx context.Context) (string, error) { return digestFilePrefix(ctx, path, int64(len(payload))) },
	} {
		t.Run(name, func(t *testing.T) {
			ctx := &cancelAfterChecksContext{Context: t.Context(), cancelAt: 3}
			if _, err := digest(ctx); !errors.Is(err, context.Canceled) {
				t.Fatalf("multi-chunk digest error = %v, want context cancellation after %d checks", err, ctx.checks)
			}
			if ctx.checks != ctx.cancelAt {
				t.Fatalf("context checks = %d, want cancellation on check %d", ctx.checks, ctx.cancelAt)
			}
		})
	}
}

type zeroProgressReader struct{}

func (zeroProgressReader) Read([]byte) (int, error) { return 0, nil }

func TestDigestReaderRejectsZeroProgress(t *testing.T) {
	if _, err := digestReader(t.Context(), zeroProgressReader{}); !errors.Is(err, io.ErrNoProgress) {
		t.Fatalf("zero-progress digest = %v, want %v", err, io.ErrNoProgress)
	}
}

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
	markers := []byte("browser-secret=survives\nuser-marker=alice\nold-credential=copied-but-not-authority\n")
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
	authorityMarker := []byte("fencing_token=planted\ncleanup_fence=planted\nroot_instance_id=planted\n")
	if err := os.WriteFile(filepath.Join(filepath.Dir(attachment.imagePath), "wefty-authority.marker"), authorityMarker, 0o600); err != nil {
		t.Fatal(err)
	}
	return root, system, storage, authority
}

func backupTestRequest(storage ComputerStorageReference, sourceAuthority AttemptAuthority) CreateComputerBackupRequest {
	storage.IntentRevision = 2
	return CreateComputerBackupRequest{BackupID: "backup-1", CopyID: "copy-1", Storage: storage,
		Authority: ComputerBackupAuthority{NodeID: sourceAuthority.NodeID, BootSessionID: sourceAuthority.BootSessionID,
			HelperGeneration: 1, RootInstanceID: "managed-root-1", JobID: "backup-job", PriorJobID: sourceAuthority.JobID,
			OperationRevision: 2, CleanupFence: "backup-fence-1"}}
}

func TestComputerBackupBindsSweepReceiptToNamedPriorJob(t *testing.T) {
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
			request := backupTestRequest(storage, prior)
			request.Authority.PriorJobID = test.priorJobID
			_, err := engine.CreateComputerBackup(t.Context(), request)
			if (err != nil) != test.wantErr {
				t.Fatalf("create Computer Backup error = %v, wantErr=%t", err, test.wantErr)
			}
		})
	}
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
			sourceBefore, err := digestFile(t.Context(), sourcePath)
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
			sourceAfterCrash, err := digestFile(t.Context(), sourcePath)
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
			manifest, present, err := readComputerBackupManifest(filepath.Join(copyRoot, "copy.json"))
			if err != nil || !present || manifest.Encryption != "none" {
				t.Fatalf("durable Backup manifest encryption = %+v present=%t err=%v", manifest, present, err)
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
			sourceAfterResume, err := digestFile(t.Context(), sourcePath)
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
		{name: "mid-copy ENOSPC", wantCode: "insufficient_disk", configure: func(engine *ContainerdEngine, _ string, _ CreateComputerBackupRequest) {
			engine.computerBackupCopyN = func(destination io.Writer, source io.Reader, _ int64) (int64, error) {
				copied, err := io.CopyN(destination, source, 4096)
				if err != nil {
					return copied, err
				}
				return copied, syscall.ENOSPC
			}
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
			before, err := digestFile(t.Context(), sourcePath)
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
			after, err := digestFile(t.Context(), sourcePath)
			if err != nil || after != before {
				t.Fatalf("failed Backup mutated source: before=%s after=%s err=%v", before, after, err)
			}
		})
	}
}

func TestComputerBackupRemovalSupersessionWinsRacingStaleCreate(t *testing.T) {
	root, system, storage, sourceAuthority := prepareDetachedBackupSource(t)
	request := backupTestRequest(storage, sourceAuthority)
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system}
	removalEntered := make(chan struct{})
	releaseRemoval := make(chan struct{})
	engine.computerBackupRemovalHook = func() {
		close(removalEntered)
		<-releaseRemoval
	}
	deleteRequest := DeleteComputerBackupCopyRequest{BackupID: request.BackupID, CopyID: request.CopyID,
		Storage: request.Storage, Authority: request.Authority, Superseded: true}
	removed := make(chan error, 1)
	go func() {
		response, err := engine.DeleteComputerBackupCopy(t.Context(), deleteRequest)
		if err == nil && (!response.Receipt.Absent || response.Receipt.Kind != "computer_backup_copy_removed") {
			err = errors.New("removal returned no positive absence")
		}
		removed <- err
	}()
	<-removalEntered
	created := make(chan error, 1)
	go func() {
		_, err := engine.CreateComputerBackup(t.Context(), request)
		created <- err
	}()
	close(releaseRemoval)
	if err := <-removed; err != nil {
		t.Fatalf("superseding removal: %v", err)
	}
	if err := <-created; err == nil || !bytes.Contains([]byte(err.Error()), []byte("durably superseded")) {
		t.Fatalf("stale create after supersession = %v, want durable refusal", err)
	}
	copyName, _ := deterministicComputerBackupCopyName(request.CopyID)
	copyRoot := filepath.Join(root, "computer-backups", copyName)
	if _, err := os.Lstat(copyRoot); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("late copy exists after supersession: %v", err)
	}
	if _, err := os.Lstat(computerBackupSupersessionPath(filepath.Dir(copyRoot), copyName)); err != nil {
		t.Fatalf("durable supersession tombstone missing: %v", err)
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
