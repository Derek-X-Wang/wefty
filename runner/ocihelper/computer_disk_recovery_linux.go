//go:build linux

package ocihelper

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

const defaultComputerDiskQuarantineRetention = 24 * time.Hour

type computerDiskRecoveryRecordError struct {
	Operation string
	Storage   ComputerStorageReference
	Cause     error
}

type ComputerStorageResumeDeferredError struct{ Storage ComputerStorageReference }

func (err *ComputerStorageResumeDeferredError) Error() string {
	return "Computer Storage recovery is resume_deferred"
}

func computerStorageResumePending(root string) (bool, error) {
	if _, present, err := readComputerStorageGrowIntent(root); err != nil {
		return false, err
	} else if present {
		return true, nil
	}
	manifest, present, err := readComputerStorageCopyManifest(filepath.Join(root, "storage-copy.json"))
	if err != nil {
		return false, err
	}
	return present && (manifest.Phase != computerStorageCopyPublished || manifest.Receipt == nil), nil
}

func (err *computerDiskRecoveryRecordError) Error() string {
	return fmt.Sprintf("%s recovery authority is invalid: %v", err.Operation, err.Cause)
}

func (err *computerDiskRecoveryRecordError) Unwrap() error { return err.Cause }

type computerDiskQuarantinePhase string

const (
	computerDiskQuarantineRecordWritten computerDiskQuarantinePhase = "record_written"
	computerDiskQuarantineRenamed       computerDiskQuarantinePhase = "renamed"
)

type computerDiskQuarantineReceipt struct {
	Kind        string                   `json:"kind"`
	ReceiptID   string                   `json:"receipt_id"`
	DiskName    string                   `json:"disk_name"`
	Storage     ComputerStorageReference `json:"storage"`
	Reason      string                   `json:"reason"`
	CreatedAt   time.Time                `json:"created_at"`
	RetainUntil time.Time                `json:"retain_until"`
}

func validComputerStorageGrowIntentForInventory(root, name string, manifest computerDiskManifest) bool {
	intent, present, err := readComputerStorageGrowIntent(root)
	if err != nil || !present || !sameComputerStorageIdentity(manifest.Storage, intent.Request.Storage) ||
		manifest.DiskImage != "disk.ext4" || manifest.MountDirectory != name {
		return false
	}
	info, err := os.Lstat(filepath.Join(root, "disk.ext4"))
	return err == nil && info.Mode().IsRegular() &&
		(info.Size() == intent.Request.Storage.DiskBytes || info.Size() == intent.Request.NewDiskBytes)
}

func (engine *ContainerdEngine) resumeComputerStorageGrow(ctx context.Context, root, name string, manifest *computerDiskManifest) (bool, error) {
	intent, present, err := readComputerStorageGrowIntent(root)
	if err != nil {
		return false, &computerDiskRecoveryRecordError{Operation: "computer_storage_grow", Cause: err}
	}
	if !present {
		return false, err
	}
	request := intent.Request
	if !sameComputerStorageIdentity(manifest.Storage, request.Storage) || manifest.DiskImage != "disk.ext4" || manifest.MountDirectory != name {
		return false, &computerDiskRecoveryRecordError{Operation: "computer_storage_grow", Storage: manifest.Storage,
			Cause: errors.New("Computer Storage grow intent conflicts with its disk manifest")}
	}
	imagePath := filepath.Join(root, "disk.ext4")
	info, err := os.Lstat(imagePath)
	if err != nil {
		return false, err
	}
	if !info.Mode().IsRegular() {
		return false, errors.New("Computer Storage grow image is not regular")
	}
	if manifest.Storage.DiskBytes == request.NewDiskBytes {
		if err := verifyComputerDiskAllocation(imagePath, request.NewDiskBytes); err != nil {
			return false, err
		}
		return true, removeComputerStorageGrowIntent(root)
	}
	if manifest.Storage.DiskBytes != request.Storage.DiskBytes {
		return false, &computerDiskRecoveryRecordError{Operation: "computer_storage_grow", Storage: manifest.Storage,
			Cause: errors.New("Computer Storage grow intent has stale manifest authority")}
	}
	if info.Size() == request.Storage.DiskBytes {
		// The irreversible step never began. Removing the durable intent is a
		// complete rollback to the still-published old generation.
		return true, removeComputerStorageGrowIntent(root)
	}
	if info.Size() != request.NewDiskBytes {
		return false, &computerDiskRecoveryRecordError{Operation: "computer_storage_grow", Storage: manifest.Storage,
			Cause: errors.New("Computer Storage grow image conflicts with its durable target")}
	}
	preen := engine.computerGrowPreen
	if preen == nil {
		preen = preenComputerStorage
	}
	if err := preen(ctx, imagePath); err != nil {
		return false, err
	}
	if err := engine.resizeComputerStorage(ctx, imagePath, "", request.Storage.DiskBytes, request.NewDiskBytes); err != nil {
		return false, err
	}
	manifest.Storage.DiskBytes = request.NewDiskBytes
	if err := writeComputerDiskManifest(root, *manifest); err != nil {
		return false, err
	}
	if err := removeComputerStorageGrowIntent(root); err != nil {
		return false, err
	}
	engine.markCapacityDiskMaterialized(request.Authority.JobID)
	return true, nil
}

func (engine *ContainerdEngine) resumeComputerStorageCopy(ctx context.Context, root, name string) (string, error) {
	copyManifest, present, err := readComputerStorageCopyManifest(filepath.Join(root, "storage-copy.json"))
	if err != nil {
		return "", &computerDiskRecoveryRecordError{Operation: "computer_storage_copy", Cause: err}
	}
	if !present {
		return "", err
	}
	request := copyManifest.Request
	expected, err := deterministicComputerDiskName(request.Destination)
	validOperation := request.Operation == "restore" || request.Operation == "clone" || request.Operation == "import"
	if err != nil || expected != name || !validOperation || request.BackupID == "" || request.CopyID == "" || request.SourceSize < 1 ||
		request.SourceDigest == "" || request.Destination.DiskBytes < request.SourceSize || request.Destination.IntentRevision < 1 ||
		request.Authority.NodeID == "" || request.Authority.BootSessionID == "" || request.Authority.HelperGeneration == 0 ||
		request.Authority.RootInstanceID == "" || request.Authority.JobID == "" || request.Authority.OperationRevision < 1 ||
		request.Authority.CleanupFence == "" {
		return "", &computerDiskRecoveryRecordError{Operation: "computer_storage_copy", Storage: request.Destination,
			Cause: errors.New("Computer Storage copy recovery record is incomplete")}
	}
	if copyManifest.Phase == computerStorageCopyPublished && copyManifest.Receipt != nil {
		return "", nil
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	switch copyManifest.Phase {
	case computerStorageCopyReserved, computerStorageCopyAllocated, computerStorageCopyCopied,
		computerStorageCopySourceVerified, computerStorageCopyMountedRekey, computerStorageCopyIdentityRekeyed,
		computerStorageCopyExpanded:
		// No destination manifest has been published in these phases. Removing
		// the staged/premature image and operation record is an exact rollback;
		// the immutable source remains available for a fresh copy directive.
		for _, path := range []string{"disk.ext4.staging", "disk.ext4", "storage-copy.json"} {
			if err := os.Remove(filepath.Join(root, path)); err != nil && !errors.Is(err, os.ErrNotExist) {
				return "", err
			}
		}
		if err := syncDirectory(root); err != nil {
			return "", err
		}
		return "computer_storage_copy_rolled_back", nil
	case computerStorageCopyManifestWritten, computerStorageCopyPublished:
		if copyManifest.DestinationDigest == "" || !copyManifest.SourceUnchanged {
			return "", &computerDiskRecoveryRecordError{Operation: "computer_storage_copy", Storage: request.Destination,
				Cause: errors.New("Computer Storage copy publication recovery record is incomplete")}
		}
	default:
		return "", &computerDiskRecoveryRecordError{Operation: "computer_storage_copy", Storage: request.Destination,
			Cause: errors.New("Computer Storage copy recovery phase is unknown")}
	}
	publishedPath := filepath.Join(root, "disk.ext4")
	stagingPath := filepath.Join(root, "disk.ext4.staging")
	if _, err := os.Lstat(publishedPath); errors.Is(err, os.ErrNotExist) {
		if _, stagingErr := os.Lstat(stagingPath); stagingErr != nil {
			return "", stagingErr
		}
		if err := os.Rename(stagingPath, publishedPath); err != nil {
			return "", err
		}
		if err := syncDirectory(root); err != nil {
			return "", err
		}
	} else if err != nil {
		return "", err
	}
	if err := verifyComputerDiskAllocation(publishedPath, request.Destination.DiskBytes); err != nil {
		return "", err
	}
	digest, err := digestFile(publishedPath)
	if err != nil {
		return "", err
	}
	if digest != copyManifest.DestinationDigest {
		return "", &computerDiskRecoveryRecordError{Operation: "computer_storage_copy", Storage: request.Destination,
			Cause: errors.New("Computer Storage copy recovery digest mismatch")}
	}
	diskManifest := computerDiskManifest{Version: computerDiskManifestVersion, Storage: request.Destination,
		DiskImage: "disk.ext4", MountDirectory: name, Prepared: true}
	if err := writeComputerDiskManifest(root, diskManifest); err != nil {
		return "", err
	}
	receiptID, err := randomCapability()
	if err != nil {
		return "", err
	}
	receipt := ComputerStorageCopyReceipt{Kind: "computer_storage_copy_verified", ReceiptID: receiptID,
		Operation: request.Operation, BackupID: request.BackupID, CopyID: request.CopyID,
		ExportID: request.ExportID, ExternalPath: request.ExternalPath, ManifestDigest: request.ManifestDigest,
		SourceComputerID: request.SourceComputerID, SourceStorageID: request.SourceStorageID,
		SourceGeneration: request.SourceGeneration, DestinationComputerID: request.Destination.ComputerID,
		DestinationStorageID: request.Destination.StorageID, DestinationGeneration: request.Destination.StorageGeneration,
		NodeID: request.Authority.NodeID, RootInstanceID: request.Authority.RootInstanceID,
		JobID: request.Authority.JobID, OperationRevision: request.Authority.OperationRevision,
		CleanupFence: request.Authority.CleanupFence, HelperGeneration: request.Authority.HelperGeneration,
		SourceSize: request.SourceSize, DestinationSize: request.Destination.DiskBytes,
		SourceDigest: copyManifest.SourceDigest, DestinationDigest: copyManifest.DestinationDigest,
		OSIdentityRekeyed: copyManifest.OSIdentityRekeyed, MachineIDBeforeDigest: copyManifest.MachineIDBeforeDigest,
		MachineIDAfterDigest: copyManifest.MachineIDAfterDigest, MachineIDRepaired: copyManifest.MachineIDRepaired,
		SourceUnchanged: copyManifest.SourceUnchanged, DestinationPrepared: diskManifest.Prepared,
		PreparationReceipt: diskManifest.PreparationReceipt != nil, DestinationChown: request.Destination.Chown,
		FilesystemExpanded: copyManifest.FilesystemExpanded}
	copyManifest.Phase, copyManifest.Receipt = computerStorageCopyPublished, &receipt
	if err := writeComputerStorageCopyManifest(root, copyManifest); err != nil {
		return "", err
	}
	return "computer_storage_copy", nil
}

func preenComputerStorage(ctx context.Context, imagePath string) error {
	tool, err := findRootTool("e2fsck")
	if err != nil {
		return err
	}
	output, err := exec.CommandContext(ctx, tool, "-f", "-p", imagePath).CombinedOutput()
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 {
		return nil
	}
	if err != nil {
		return fmt.Errorf("preen interrupted Computer Storage filesystem: %w: %s", err, output)
	}
	return nil
}

func (engine *ContainerdEngine) quarantineComputerDiskAnomaly(root, name string, storage ComputerStorageReference, reason string) error {
	expected, err := deterministicComputerDiskName(storage)
	if err != nil || expected != name {
		return errors.Join(err, errors.New("Computer disk quarantine requires exact Storage identity"))
	}
	receiptID, err := randomCapability()
	if err != nil {
		return err
	}
	now := time.Now()
	if engine.config.Clock != nil {
		now = engine.config.Clock.Now()
	}
	receipt := computerDiskQuarantineReceipt{Kind: "computer_disk_anomaly_quarantined", ReceiptID: receiptID,
		DiskName: name, Storage: storage, Reason: reason, CreatedAt: now.UTC(), RetainUntil: now.Add(defaultComputerDiskQuarantineRetention).UTC()}
	payload, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	if err := writeDurableFile(root, ".quarantine.json.tmp-", "quarantine.json", payload, 0o600); err != nil {
		return err
	}
	if engine.computerQuarantineHook != nil {
		if err := engine.computerQuarantineHook(computerDiskQuarantineRecordWritten); err != nil {
			return err
		}
	}
	quarantineRoot := filepath.Join(engine.config.RuntimeRoot, "computer-disk-quarantine")
	if err := os.MkdirAll(quarantineRoot, 0o700); err != nil {
		return err
	}
	destination := filepath.Join(quarantineRoot, fmt.Sprintf("%s-anomaly-%s", name, receiptID))
	if err := os.Rename(root, destination); err != nil {
		return err
	}
	if engine.computerQuarantineHook != nil {
		if err := engine.computerQuarantineHook(computerDiskQuarantineRenamed); err != nil {
			return err
		}
	}
	if err := syncDirectory(filepath.Dir(root)); err != nil {
		return err
	}
	return syncDirectory(quarantineRoot)
}

func (engine *ContainerdEngine) resumeComputerDiskQuarantine(root, name string) (bool, error) {
	payload, err := os.ReadFile(filepath.Join(root, "quarantine.json"))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var receipt computerDiskQuarantineReceipt
	if json.Unmarshal(payload, &receipt) != nil || receipt.Kind != "computer_disk_anomaly_quarantined" ||
		receipt.ReceiptID == "" || receipt.Reason == "" || receipt.DiskName != name || receipt.CreatedAt.IsZero() || receipt.RetainUntil.IsZero() {
		return false, errors.New("Computer disk quarantine recovery record is invalid")
	}
	expected, err := deterministicComputerDiskName(receipt.Storage)
	if err != nil || expected != name {
		return false, errors.Join(err, errors.New("Computer disk quarantine recovery identity is invalid"))
	}
	quarantineRoot := filepath.Join(engine.config.RuntimeRoot, "computer-disk-quarantine")
	if err := os.MkdirAll(quarantineRoot, 0o700); err != nil {
		return false, err
	}
	if err := os.Rename(root, filepath.Join(quarantineRoot, name+"-anomaly-"+receipt.ReceiptID)); err != nil {
		return false, err
	}
	if err := syncDirectory(filepath.Dir(root)); err != nil {
		return false, err
	}
	return true, syncDirectory(quarantineRoot)
}

func (engine *ContainerdEngine) expireComputerDiskQuarantinePayloads() error {
	quarantineRoot := filepath.Join(engine.config.RuntimeRoot, "computer-disk-quarantine")
	entries, err := readDirectoryIfPresent(quarantineRoot)
	if err != nil {
		return err
	}
	now := time.Now()
	if engine.config.Clock != nil {
		now = engine.config.Clock.Now()
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		root := filepath.Join(quarantineRoot, entry.Name())
		var receipt computerDiskQuarantineReceipt
		payload, readErr := os.ReadFile(filepath.Join(root, "quarantine.json"))
		if readErr != nil || json.Unmarshal(payload, &receipt) != nil || receipt.RetainUntil.IsZero() || now.Before(receipt.RetainUntil) {
			continue
		}
		children, readErr := os.ReadDir(root)
		if readErr != nil {
			return readErr
		}
		for _, child := range children {
			if child.Name() == "quarantine.json" {
				continue
			}
			if err := os.RemoveAll(filepath.Join(root, child.Name())); err != nil {
				return err
			}
		}
		if err := syncDirectory(root); err != nil {
			return err
		}
	}
	return nil
}
