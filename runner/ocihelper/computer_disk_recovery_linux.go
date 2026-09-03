//go:build linux

package ocihelper

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

type computerDiskQuarantineReceipt struct {
	Kind      string    `json:"kind"`
	ReceiptID string    `json:"receipt_id"`
	DiskName  string    `json:"disk_name"`
	Reason    string    `json:"reason"`
	CreatedAt time.Time `json:"created_at"`
}

func (engine *ContainerdEngine) resumeComputerStorageGrow(ctx context.Context, root, name string, manifest *computerDiskManifest) (bool, error) {
	intent, present, err := readComputerStorageGrowIntent(root)
	if err != nil || !present {
		return false, err
	}
	request := intent.Request
	if !sameComputerStorageIdentity(manifest.Storage, request.Storage) || manifest.DiskImage != "disk.ext4" || manifest.MountDirectory != name {
		return false, errors.New("Computer Storage grow intent conflicts with its disk manifest")
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
		return false, errors.New("Computer Storage grow intent has stale manifest authority")
	}
	if info.Size() == request.Storage.DiskBytes {
		// The irreversible step never began. Removing the durable intent is a
		// complete rollback to the still-published old generation.
		return true, removeComputerStorageGrowIntent(root)
	}
	if info.Size() != request.NewDiskBytes {
		return false, errors.New("Computer Storage grow image conflicts with its durable target")
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

func (engine *ContainerdEngine) resumeComputerStorageCopy(root, name string) (string, error) {
	copyManifest, present, err := readComputerStorageCopyManifest(filepath.Join(root, "storage-copy.json"))
	if err != nil || !present {
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
		return "", errors.New("Computer Storage copy recovery record is incomplete")
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
			return "", errors.New("Computer Storage copy publication recovery record is incomplete")
		}
	default:
		return "", errors.New("Computer Storage copy recovery phase is unknown")
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
		return "", errors.New("Computer Storage copy recovery digest mismatch")
	}
	diskManifest := computerDiskManifest{Version: computerDiskManifestVersion, Storage: request.Destination,
		DiskImage: "disk.ext4", MountDirectory: name, Prepared: true}
	if err := writeComputerDiskManifest(root, diskManifest); err != nil {
		return "", err
	}
	return "computer_storage_copy", nil
}

func (engine *ContainerdEngine) quarantineComputerDiskAnomaly(root, name, reason string) error {
	receiptID, err := randomCapability()
	if err != nil {
		return err
	}
	now := time.Now()
	if engine.config.Clock != nil {
		now = engine.config.Clock.Now()
	}
	receipt := computerDiskQuarantineReceipt{Kind: "computer_disk_anomaly_quarantined", ReceiptID: receiptID,
		DiskName: name, Reason: reason, CreatedAt: now.UTC()}
	payload, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	if err := writeDurableFile(root, ".quarantine.json.tmp-", "quarantine.json", payload, 0o600); err != nil {
		return err
	}
	quarantineRoot := filepath.Join(engine.config.RuntimeRoot, "computer-disk-quarantine")
	if err := os.MkdirAll(quarantineRoot, 0o700); err != nil {
		return err
	}
	destination := filepath.Join(quarantineRoot, fmt.Sprintf("%s-anomaly-%s", name, receiptID))
	if err := os.Rename(root, destination); err != nil {
		return err
	}
	if err := syncDirectory(filepath.Dir(root)); err != nil {
		return err
	}
	return syncDirectory(quarantineRoot)
}
