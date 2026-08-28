//go:build linux

package ocihelper

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

type computerBackupCheckpoint string

const (
	computerBackupReserved        computerBackupCheckpoint = "reserve"
	computerBackupAllocated       computerBackupCheckpoint = "allocate"
	computerBackupCopied          computerBackupCheckpoint = "copy"
	computerBackupDigested        computerBackupCheckpoint = "digest"
	computerBackupManifestWritten computerBackupCheckpoint = "manifest"
	computerBackupPublished       computerBackupCheckpoint = "publish"
)

type computerBackupManifest struct {
	Version       int                        `json:"version"`
	BackupID      string                     `json:"backup_id"`
	CopyID        string                     `json:"copy_id"`
	Storage       ComputerStorageReference   `json:"storage"`
	Authority     ComputerBackupAuthority    `json:"authority"`
	Phase         computerBackupCheckpoint   `json:"phase"`
	TemporaryFile string                     `json:"temporary_file,omitempty"`
	PublishedFile string                     `json:"published_file,omitempty"`
	ContentDigest string                     `json:"content_digest,omitempty"`
	Receipt       *ComputerBackupCopyReceipt `json:"receipt,omitempty"`
}

func (engine *ContainerdEngine) computerBackupCheckpoint(checkpoint computerBackupCheckpoint) error {
	if engine.computerBackupHook == nil {
		return nil
	}
	return engine.computerBackupHook(checkpoint)
}

func (engine *ContainerdEngine) allocateComputerBackup(path string, size int64) error {
	if engine.computerBackupAllocate != nil {
		return engine.computerBackupAllocate(path, size)
	}
	return fullyAllocateComputerDisk(path, size)
}

func deterministicComputerBackupCopyName(copyID string) (string, error) {
	copyID = strings.TrimSpace(copyID)
	if copyID == "" {
		return "", errors.New("Backup copy ID is required")
	}
	digest := sha256.Sum256([]byte(copyID))
	return "wefty-backup-copy-" + hex.EncodeToString(digest[:16]), nil
}

func sameComputerBackupAuthority(left, right ComputerBackupAuthority) bool {
	return left.NodeID == right.NodeID && left.RootInstanceID == right.RootInstanceID &&
		left.JobID == right.JobID && left.OperationRevision == right.OperationRevision &&
		left.CleanupFence == right.CleanupFence
}

func validBackupDetachmentEvidence(evidence *computerDiskEvidence, storage ComputerStorageReference, authority ComputerBackupAuthority) bool {
	if evidence == nil || evidence.ReceiptID == "" || evidence.NodeID != authority.NodeID ||
		evidence.JobID != authority.JobID || evidence.ComputerID != storage.ComputerID ||
		evidence.StorageID != storage.StorageID || evidence.StorageGeneration != storage.StorageGeneration {
		return false
	}
	switch evidence.Kind {
	case computerDiskReapReceipt:
		return evidence.BootSessionID == authority.BootSessionID && evidence.SweepEpoch == ""
	case computerDiskSweepReceipt:
		return evidence.BootSessionID != authority.BootSessionID && evidence.SweepEpoch != ""
	default:
		return false
	}
}

// lockDetachedBackupSource holds the same attachment flock used by attach and
// detach until release. Backup create retains it across copy, both digests,
// and durable publication so no delayed attach can reopen a write window.
func (engine *ContainerdEngine) lockDetachedBackupSource(storage ComputerStorageReference, authority ComputerBackupAuthority) (string, func(), error) {
	name, err := deterministicComputerDiskName(storage)
	if err != nil {
		return "", nil, err
	}
	diskRoot := filepath.Join(engine.config.RuntimeRoot, "computer-disks", name)
	lock, err := openComputerDiskLock(diskRoot)
	if err != nil {
		return "", nil, err
	}
	release := func() { closeComputerDiskLock(lock) }
	manifest, present, err := readComputerDiskManifest(filepath.Join(diskRoot, "attachment.json"))
	if err != nil {
		release()
		return "", nil, err
	}
	if !present || !sameComputerStorageIdentity(manifest.Storage, storage) || manifest.DiskImage != "disk.ext4" ||
		manifest.MountDirectory != name || manifest.Attached != nil || manifest.Pending != nil || manifest.Retirement != nil ||
		!validBackupDetachmentEvidence(manifest.PreviousDetachment, storage, authority) {
		release()
		return "", nil, errors.New("Computer Backup lacks exact detached source-generation evidence")
	}
	if _, mounted, err := engine.computerDiskSystem().mountedSource(filepath.Join(engine.config.RuntimeRoot, "computer-mounts", name)); err != nil {
		release()
		return "", nil, err
	} else if mounted {
		release()
		return "", nil, errors.New("Computer Backup source remains mounted")
	}
	loops, err := engine.computerDiskSystem().loopsForRoot(diskRoot)
	if err != nil {
		release()
		return "", nil, err
	}
	if len(loops) != 0 {
		release()
		return "", nil, errors.New("Computer Backup source remains loop-attached")
	}
	source := filepath.Join(diskRoot, "disk.ext4")
	if err := verifyComputerDiskAllocation(source, storage.DiskBytes); err != nil {
		release()
		return "", nil, fmt.Errorf("verify Computer Backup source allocation: %w", err)
	}
	return source, release, nil
}

func writeComputerBackupManifest(root string, manifest computerBackupManifest) error {
	if err := os.MkdirAll(root, 0o700); err != nil {
		return err
	}
	payload, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	file, err := os.CreateTemp(root, ".manifest.tmp-")
	if err != nil {
		return err
	}
	name := file.Name()
	defer os.Remove(name)
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return err
	}
	writeErr := error(nil)
	if _, writeErr = file.Write(payload); writeErr == nil {
		writeErr = file.Sync()
	}
	writeErr = errors.Join(writeErr, file.Close())
	if writeErr != nil {
		return writeErr
	}
	if err := os.Rename(name, filepath.Join(root, "copy.json")); err != nil {
		return err
	}
	return syncDirectory(root)
}

func readComputerBackupManifest(path string) (computerBackupManifest, bool, error) {
	payload, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return computerBackupManifest{}, false, nil
	}
	if err != nil {
		return computerBackupManifest{}, false, err
	}
	var manifest computerBackupManifest
	if err := json.Unmarshal(payload, &manifest); err != nil || manifest.Version != 1 {
		return computerBackupManifest{}, false, errors.New("Computer Backup manifest is invalid")
	}
	return manifest, true, nil
}

func digestFile(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	hash := sha256.New()
	_, copyErr := io.Copy(hash, file)
	if closeErr := file.Close(); copyErr == nil {
		copyErr = closeErr
	}
	if copyErr != nil {
		return "", copyErr
	}
	return "sha256:" + hex.EncodeToString(hash.Sum(nil)), nil
}

func removeComputerBackupRoot(root string) error {
	parent := filepath.Dir(root)
	if filepath.Base(parent) != "computer-backups" || !strings.HasPrefix(filepath.Base(root), "wefty-backup-copy-") {
		return errors.New("refuse unsafe Computer Backup cleanup target")
	}
	entries, err := os.ReadDir(root)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			return errors.New("Computer Backup root contains an unexpected directory")
		}
		if err := os.Remove(filepath.Join(root, entry.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	if err := os.Remove(root); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return syncDirectory(parent)
}

func backupFailureReceipt(request CreateComputerBackupRequest, helperGeneration uint64, code string) (CreateComputerBackupResponse, error) {
	receiptID, err := randomCapability()
	if err != nil {
		return CreateComputerBackupResponse{}, err
	}
	return CreateComputerBackupResponse{Receipt: ComputerBackupCopyReceipt{
		Kind: "computer_backup_copy_failed_absent", ReceiptID: receiptID,
		BackupID: request.BackupID, CopyID: request.CopyID, ComputerID: request.Storage.ComputerID,
		StorageID: request.Storage.StorageID, StorageGeneration: request.Storage.StorageGeneration,
		NodeID: request.Authority.NodeID, RootInstanceID: request.Authority.RootInstanceID,
		JobID: request.Authority.JobID, OperationRevision: request.Authority.OperationRevision,
		CleanupFence: request.Authority.CleanupFence, HelperGeneration: helperGeneration,
		AllocatedSize: request.Storage.DiskBytes, Encryption: "none", FailureCode: code, CopyAbsent: true,
	}}, nil
}

func (engine *ContainerdEngine) createComputerBackupLocked(ctx context.Context, request CreateComputerBackupRequest) (CreateComputerBackupResponse, error) {
	sourcePath, releaseSource, err := engine.lockDetachedBackupSource(request.Storage, request.Authority)
	if err != nil {
		return CreateComputerBackupResponse{}, err
	}
	defer releaseSource()
	copyName, err := deterministicComputerBackupCopyName(request.CopyID)
	if err != nil {
		return CreateComputerBackupResponse{}, err
	}
	backupParent := filepath.Join(engine.config.RuntimeRoot, "computer-backups")
	if err := os.MkdirAll(backupParent, 0o700); err != nil {
		return CreateComputerBackupResponse{}, err
	}
	root := filepath.Join(backupParent, copyName)
	manifestPath := filepath.Join(root, "copy.json")
	manifest, present, err := readComputerBackupManifest(manifestPath)
	if err != nil {
		return CreateComputerBackupResponse{}, err
	}
	if present {
		if manifest.BackupID != request.BackupID || manifest.CopyID != request.CopyID ||
			!sameComputerStorageIdentity(manifest.Storage, request.Storage) ||
			!sameComputerBackupAuthority(manifest.Authority, request.Authority) {
			return CreateComputerBackupResponse{}, errors.New("Computer Backup copy has different durable authority")
		}
		if manifest.Receipt != nil && manifest.Phase == computerBackupPublished {
			published := filepath.Join(root, manifest.PublishedFile)
			if err := verifyComputerDiskAllocation(published, request.Storage.DiskBytes); err != nil {
				return CreateComputerBackupResponse{}, err
			}
			digest, err := digestFile(published)
			if err != nil || digest != manifest.ContentDigest {
				return CreateComputerBackupResponse{}, errors.New("published Computer Backup digest no longer matches its manifest")
			}
			return CreateComputerBackupResponse{Receipt: *manifest.Receipt}, nil
		}
	} else {
		manifest = computerBackupManifest{Version: 1, BackupID: request.BackupID, CopyID: request.CopyID,
			Storage: request.Storage, Authority: request.Authority, Phase: computerBackupReserved,
			TemporaryFile: "backup.ext4.staging", PublishedFile: "backup.ext4"}
		if err := writeComputerBackupManifest(root, manifest); err != nil {
			return CreateComputerBackupResponse{}, err
		}
		if err := engine.computerBackupCheckpoint(computerBackupReserved); err != nil {
			return CreateComputerBackupResponse{}, err
		}
	}

	temporaryPath := filepath.Join(root, "backup.ext4.staging")
	publishedPath := filepath.Join(root, "backup.ext4")
	if _, err := os.Lstat(publishedPath); err == nil {
		if manifest.Phase != computerBackupManifestWritten && manifest.Phase != computerBackupPublished {
			return CreateComputerBackupResponse{}, errors.New("Computer Backup image exists before durable publication phase")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return CreateComputerBackupResponse{}, err
	} else {
		if err := os.Remove(temporaryPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return CreateComputerBackupResponse{}, err
		}
		file, err := os.OpenFile(temporaryPath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
		if err != nil {
			return CreateComputerBackupResponse{}, err
		}
		if err := engine.allocateComputerBackup(temporaryPath, request.Storage.DiskBytes); err != nil {
			_ = file.Close()
			if errors.Is(err, syscall.ENOSPC) {
				if cleanupErr := removeComputerBackupRoot(root); cleanupErr != nil {
					return CreateComputerBackupResponse{}, errors.Join(err, cleanupErr)
				}
				return backupFailureReceipt(request, request.Authority.HelperGeneration, "insufficient_disk")
			}
			return CreateComputerBackupResponse{}, err
		}
		if err := file.Close(); err != nil {
			return CreateComputerBackupResponse{}, err
		}
		manifest.Phase = computerBackupAllocated
		if err := writeComputerBackupManifest(root, manifest); err != nil {
			return CreateComputerBackupResponse{}, err
		}
		if err := engine.computerBackupCheckpoint(computerBackupAllocated); err != nil {
			return CreateComputerBackupResponse{}, err
		}
		copyErr := func() error {
			source, err := os.Open(sourcePath)
			if err != nil {
				return err
			}
			destination, err := os.OpenFile(temporaryPath, os.O_WRONLY, 0)
			if err != nil {
				_ = source.Close()
				return err
			}
			_, err = io.CopyN(destination, source, request.Storage.DiskBytes)
			if err == nil {
				err = destination.Sync()
			}
			return errors.Join(err, source.Close(), destination.Close())
		}()
		if copyErr != nil {
			if errors.Is(copyErr, syscall.ENOSPC) {
				if cleanupErr := removeComputerBackupRoot(root); cleanupErr != nil {
					return CreateComputerBackupResponse{}, errors.Join(copyErr, cleanupErr)
				}
				return backupFailureReceipt(request, request.Authority.HelperGeneration, "insufficient_disk")
			}
			return CreateComputerBackupResponse{}, copyErr
		}
		manifest.Phase = computerBackupCopied
		if err := writeComputerBackupManifest(root, manifest); err != nil {
			return CreateComputerBackupResponse{}, err
		}
		if err := engine.computerBackupCheckpoint(computerBackupCopied); err != nil {
			return CreateComputerBackupResponse{}, err
		}
	}

	sourceDigest, err := digestFile(sourcePath)
	if err != nil {
		return CreateComputerBackupResponse{}, err
	}
	if err := engine.computerBackupCheckpoint(computerBackupDigested); err != nil {
		return CreateComputerBackupResponse{}, err
	}
	targetPath := temporaryPath
	if _, err := os.Lstat(publishedPath); err == nil {
		targetPath = publishedPath
	}
	destinationDigest, err := digestFile(targetPath)
	if err != nil {
		return CreateComputerBackupResponse{}, err
	}
	if sourceDigest != destinationDigest {
		if cleanupErr := removeComputerBackupRoot(root); cleanupErr != nil {
			return CreateComputerBackupResponse{}, errors.Join(errors.New("Computer Backup digest mismatch"), cleanupErr)
		}
		return backupFailureReceipt(request, request.Authority.HelperGeneration, "digest_mismatch")
	}
	manifest.ContentDigest = sourceDigest
	manifest.Phase = computerBackupManifestWritten
	if err := writeComputerBackupManifest(root, manifest); err != nil {
		return CreateComputerBackupResponse{}, err
	}
	if err := engine.computerBackupCheckpoint(computerBackupManifestWritten); err != nil {
		return CreateComputerBackupResponse{}, err
	}
	if targetPath == temporaryPath {
		if err := os.Rename(temporaryPath, publishedPath); err != nil {
			return CreateComputerBackupResponse{}, err
		}
		if err := syncDirectory(root); err != nil {
			return CreateComputerBackupResponse{}, err
		}
	}
	if err := engine.computerBackupCheckpoint(computerBackupPublished); err != nil {
		return CreateComputerBackupResponse{}, err
	}
	receiptID, err := randomCapability()
	if err != nil {
		return CreateComputerBackupResponse{}, err
	}
	receipt := ComputerBackupCopyReceipt{Kind: "computer_backup_copy_verified", ReceiptID: receiptID,
		BackupID: request.BackupID, CopyID: request.CopyID, ComputerID: request.Storage.ComputerID,
		StorageID: request.Storage.StorageID, StorageGeneration: request.Storage.StorageGeneration,
		NodeID: request.Authority.NodeID, RootInstanceID: request.Authority.RootInstanceID,
		JobID: request.Authority.JobID, OperationRevision: request.Authority.OperationRevision,
		CleanupFence: request.Authority.CleanupFence, HelperGeneration: request.Authority.HelperGeneration,
		AllocatedSize: request.Storage.DiskBytes, ContentDigest: sourceDigest, Encryption: "none"}
	manifest.Phase = computerBackupPublished
	manifest.Receipt = &receipt
	if err := writeComputerBackupManifest(root, manifest); err != nil {
		return CreateComputerBackupResponse{}, err
	}
	return CreateComputerBackupResponse{Receipt: receipt}, nil
}

func (engine *ContainerdEngine) CreateComputerBackup(ctx context.Context, request CreateComputerBackupRequest) (CreateComputerBackupResponse, error) {
	engine.computerBackupMu.Lock()
	defer engine.computerBackupMu.Unlock()
	if request.Storage.DiskBytes <= 0 || request.Storage.IntentRevision != request.Authority.OperationRevision ||
		request.Authority.NodeID == "" || request.Authority.BootSessionID == "" ||
		request.Authority.RootInstanceID == "" || request.Authority.JobID == "" ||
		request.Authority.HelperGeneration == 0 || request.Authority.OperationRevision < 1 ||
		request.Authority.CleanupFence == "" || request.BackupID == "" || request.CopyID == "" {
		return CreateComputerBackupResponse{}, errors.New("Computer Backup request is incomplete")
	}
	return engine.createComputerBackupLocked(ctx, request)
}

func (engine *ContainerdEngine) DeleteComputerBackupCopy(_ context.Context, request DeleteComputerBackupCopyRequest) (DeleteComputerBackupCopyResponse, error) {
	engine.computerBackupMu.Lock()
	defer engine.computerBackupMu.Unlock()
	copyName, err := deterministicComputerBackupCopyName(request.CopyID)
	if err != nil {
		return DeleteComputerBackupCopyResponse{}, err
	}
	root := filepath.Join(engine.config.RuntimeRoot, "computer-backups", copyName)
	manifest, present, err := readComputerBackupManifest(filepath.Join(root, "copy.json"))
	if err != nil {
		return DeleteComputerBackupCopyResponse{}, err
	}
	if present && (manifest.BackupID != request.BackupID || manifest.CopyID != request.CopyID ||
		!sameComputerStorageIdentity(manifest.Storage, request.Storage) ||
		manifest.Authority.NodeID != request.Authority.NodeID ||
		manifest.Authority.RootInstanceID != request.Authority.RootInstanceID) {
		return DeleteComputerBackupCopyResponse{}, errors.New("Backup copy removal authority does not match its manifest")
	}
	if err := removeComputerBackupRoot(root); err != nil {
		return DeleteComputerBackupCopyResponse{}, err
	}
	if _, err := os.Lstat(root); !errors.Is(err, os.ErrNotExist) {
		return DeleteComputerBackupCopyResponse{}, errors.New("Backup copy remains after deletion")
	}
	receiptID, err := randomCapability()
	if err != nil {
		return DeleteComputerBackupCopyResponse{}, err
	}
	return DeleteComputerBackupCopyResponse{Receipt: ComputerBackupCopyRemovalReceipt{
		Kind: "computer_backup_copy_removed", ReceiptID: receiptID, BackupID: request.BackupID,
		CopyID: request.CopyID, ComputerID: request.Storage.ComputerID, StorageID: request.Storage.StorageID,
		StorageGeneration: request.Storage.StorageGeneration, NodeID: request.Authority.NodeID,
		RootInstanceID: request.Authority.RootInstanceID, OperationRevision: request.Authority.OperationRevision,
		CleanupFence: request.Authority.CleanupFence, HelperGeneration: request.Authority.HelperGeneration, Absent: true,
	}}, nil
}
