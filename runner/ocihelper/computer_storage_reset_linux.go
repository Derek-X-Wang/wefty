//go:build linux

package ocihelper

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

// computerStorageResetPhase is a crash-injection seam. The durable phase
// record lives in the shared Computer disk manifest; reset does not introduce
// a second quarantine manifest or proof system.
type computerStorageResetPhase string

const (
	computerStorageResetRetirementFenced computerStorageResetPhase = "retirement_fenced"
	computerStorageResetManifestWritten  computerStorageResetPhase = "allocation_manifest_written"
	computerStorageResetAllocated        computerStorageResetPhase = "allocated_and_formatted"
	computerStorageResetImagePublished   computerStorageResetPhase = "image_published"
	computerStorageResetVerified         computerStorageResetPhase = "verified"
)

func sameComputerStorageResetAuthority(left, right ComputerStorageResetAuthority) bool {
	return left.NodeID == right.NodeID && left.RootInstanceID == right.RootInstanceID &&
		left.JobID == right.JobID && left.PriorJobID == right.PriorJobID && left.IntentRevision == right.IntentRevision &&
		left.CleanupFence == right.CleanupFence
}

func unverifiedComputerStorageResetPreparation(manifest computerDiskManifest) bool {
	authority := manifest.Preparation
	return authority != nil && manifest.PreparationReceipt == nil && !manifest.Prepared && manifest.Retirement == nil &&
		manifest.Attached == nil && manifest.Pending == nil && manifest.PreviousDetachment == nil &&
		authority.NodeID != "" && authority.BootSessionID != "" && authority.RootInstanceID != "" &&
		authority.JobID != "" && authority.PriorJobID != "" && authority.HelperGeneration != 0 &&
		authority.IntentRevision == manifest.Storage.IntentRevision && authority.IntentRevision > 0 &&
		strings.TrimSpace(authority.CleanupFence) != ""
}

func validResetDetachmentEvidence(evidence *computerDiskEvidence, storage ComputerStorageReference, authority ComputerStorageResetAuthority) bool {
	return validComputerDiskConsumerDetachmentEvidence(evidence, storage, computerDiskDetachmentAuthority{
		NodeID: authority.NodeID, BootSessionID: authority.BootSessionID, PriorJobID: authority.PriorJobID,
	})
}

func (engine *ContainerdEngine) storageResetCheckpoint(phase computerStorageResetPhase) error {
	if engine.storageResetHook == nil {
		return nil
	}
	return engine.storageResetHook(phase)
}

func openComputerDiskLock(root string) (*os.File, error) {
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, err
	}
	lock, err := os.OpenFile(filepath.Join(root, "attachment.lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = lock.Close()
		return nil, errors.New("Computer Storage generation already has an attachment owner")
	}
	return lock, nil
}

func closeComputerDiskLock(lock *os.File) {
	if lock == nil {
		return
	}
	_ = unix.Flock(int(lock.Fd()), unix.LOCK_UN)
	_ = lock.Close()
}

// fenceResetPredecessor takes the exact attachment flock before inspecting
// detachment and writes the retirement fence before releasing it. A delayed
// attach therefore either owns the lock first (and reset refuses) or observes
// the durable fence after acquiring it; it can never resurrect the generation
// after successor verification.
func (engine *ContainerdEngine) fenceResetPredecessor(storage ComputerStorageReference, authority ComputerStorageResetAuthority) error {
	name, err := deterministicComputerDiskName(storage)
	if err != nil {
		return err
	}
	diskRoot := filepath.Join(engine.config.RuntimeRoot, "computer-disks", name)
	lock, err := openComputerDiskLock(diskRoot)
	if err != nil {
		return err
	}
	defer closeComputerDiskLock(lock)

	manifest, present, err := readComputerDiskManifest(filepath.Join(diskRoot, "attachment.json"))
	if err != nil {
		return err
	}
	if present {
		if !sameComputerStorageIdentity(manifest.Storage, storage) || manifest.DiskImage != "disk.ext4" ||
			manifest.MountDirectory != name || manifest.Attached != nil || manifest.Pending != nil {
			return errors.New("Computer Storage reset lacks exact detached generation authority")
		}
		if manifest.Retirement != nil {
			if !sameComputerStorageResetAuthority(*manifest.Retirement, authority) {
				return errors.New("Computer Storage generation already has different retirement authority")
			}
			return nil
		}
		if _, imageErr := os.Lstat(filepath.Join(diskRoot, "disk.ext4")); imageErr == nil {
			if !validResetDetachmentEvidence(manifest.PreviousDetachment, storage, authority) {
				return errors.New("Computer Storage reset lacks exact detachment evidence")
			}
		} else if !errors.Is(imageErr, os.ErrNotExist) {
			return imageErr
		}
	} else {
		manifest = computerDiskManifest{Version: computerDiskManifestVersion, Storage: storage,
			DiskImage: "disk.ext4", MountDirectory: name}
	}
	if _, mounted, err := engine.computerDiskSystem().mountedSource(filepath.Join(engine.config.RuntimeRoot, "computer-mounts", name)); err != nil {
		return err
	} else if mounted {
		return errors.New("Computer Storage generation remains mounted during reset")
	}
	loops, err := engine.computerDiskSystem().loopsForRoot(diskRoot)
	if err != nil {
		return err
	}
	if len(loops) != 0 {
		return errors.New("Computer Storage generation remains loop-attached during reset")
	}
	manifest.Retirement = &authority
	if err := writeComputerDiskManifest(diskRoot, manifest); err != nil {
		return err
	}
	return engine.storageResetCheckpoint(computerStorageResetRetirementFenced)
}

func resetInsufficientDiskError(root string, requested int64, err error) error {
	if !errors.Is(err, syscall.ENOSPC) {
		return err
	}
	var stat unix.Statfs_t
	available := int64(0)
	if statErr := unix.Statfs(root, &stat); statErr == nil {
		const maxInt64 = int64(^uint64(0) >> 1)
		if stat.Bsize > 0 && stat.Bavail <= uint64(maxInt64/stat.Bsize) {
			available = int64(stat.Bavail) * stat.Bsize
		}
	}
	return &insufficientDiskError{RequestedBytes: requested, ObservedAvailableBytes: available, err: err}
}

func (engine *ContainerdEngine) prepareResetSuccessor(ctx context.Context, request ResetComputerStorageRequest) (ComputerStorageResetReceipt, error) {
	storage := request.Storage
	storage.StorageGeneration = request.NewGeneration
	storage.IntentRevision = request.Authority.IntentRevision
	name, err := deterministicComputerDiskName(storage)
	if err != nil {
		return ComputerStorageResetReceipt{}, err
	}
	diskRoot := filepath.Join(engine.config.RuntimeRoot, "computer-disks", name)
	lock, err := openComputerDiskLock(diskRoot)
	if err != nil {
		return ComputerStorageResetReceipt{}, err
	}
	defer closeComputerDiskLock(lock)

	manifest, present, err := readComputerDiskManifest(filepath.Join(diskRoot, "attachment.json"))
	if err != nil {
		return ComputerStorageResetReceipt{}, err
	}
	if present {
		if !sameComputerStorageIdentity(manifest.Storage, storage) || manifest.DiskImage != "disk.ext4" ||
			manifest.MountDirectory != name || manifest.Attached != nil || manifest.Pending != nil ||
			manifest.Retirement != nil || manifest.Preparation == nil ||
			!sameComputerStorageResetAuthority(*manifest.Preparation, request.Authority) {
			return ComputerStorageResetReceipt{}, errors.New("replacement Computer Storage has different preparation authority")
		}
		if manifest.PreparationReceipt != nil {
			if err := verifyComputerDiskAllocation(filepath.Join(diskRoot, "disk.ext4"), storage.DiskBytes); err != nil {
				return ComputerStorageResetReceipt{}, err
			}
			return *manifest.PreparationReceipt, nil
		}
	} else {
		manifest = computerDiskManifest{Version: computerDiskManifestVersion, Storage: storage,
			DiskImage: "disk.ext4", MountDirectory: name, Preparation: &request.Authority}
		if err := writeComputerDiskManifest(diskRoot, manifest); err != nil {
			return ComputerStorageResetReceipt{}, err
		}
		if err := engine.storageResetCheckpoint(computerStorageResetManifestWritten); err != nil {
			return ComputerStorageResetReceipt{}, err
		}
	}

	imagePath := filepath.Join(diskRoot, "disk.ext4")
	if _, err := os.Lstat(imagePath); errors.Is(err, os.ErrNotExist) {
		temporary, err := os.CreateTemp(diskRoot, ".disk.ext4.tmp-")
		if err != nil {
			return ComputerStorageResetReceipt{}, err
		}
		temporaryPath := temporary.Name()
		if closeErr := temporary.Close(); closeErr != nil {
			_ = os.Remove(temporaryPath)
			return ComputerStorageResetReceipt{}, closeErr
		}
		defer os.Remove(temporaryPath)
		if err := engine.computerDiskSystem().allocateAndFormat(ctx, temporaryPath, storage.DiskBytes); err != nil {
			return ComputerStorageResetReceipt{}, fmt.Errorf("fully allocate replacement Computer disk: %w",
				resetInsufficientDiskError(diskRoot, storage.DiskBytes, err))
		}
		if err := engine.storageResetCheckpoint(computerStorageResetAllocated); err != nil {
			return ComputerStorageResetReceipt{}, err
		}
		if err := os.Rename(temporaryPath, imagePath); err != nil {
			return ComputerStorageResetReceipt{}, fmt.Errorf("publish replacement Computer disk image: %w", err)
		}
		if err := syncDirectory(diskRoot); err != nil {
			return ComputerStorageResetReceipt{}, err
		}
		// The formerly unreachable rename -> phase-write crash boundary is now
		// mutation-testable. The manifest already names the staging generation,
		// so retry verifies the image and completes preparation.
		if err := engine.storageResetCheckpoint(computerStorageResetImagePublished); err != nil {
			return ComputerStorageResetReceipt{}, err
		}
	} else if err != nil {
		return ComputerStorageResetReceipt{}, err
	}
	if err := verifyComputerDiskAllocation(imagePath, storage.DiskBytes); err != nil {
		return ComputerStorageResetReceipt{}, err
	}
	receiptID, err := randomCapability()
	if err != nil {
		return ComputerStorageResetReceipt{}, err
	}
	receipt := ComputerStorageResetReceipt{Kind: "computer_storage_reset_verified", ReceiptID: receiptID,
		ComputerID: storage.ComputerID, StorageID: storage.StorageID,
		OldGeneration: request.Storage.StorageGeneration, NewGeneration: request.NewGeneration,
		NodeID: request.Authority.NodeID, RootInstanceID: request.Authority.RootInstanceID,
		JobID: request.Authority.JobID, IntentRevision: request.Authority.IntentRevision,
		CleanupFence: request.Authority.CleanupFence, HelperGeneration: request.Authority.HelperGeneration}
	manifest.Prepared = true
	manifest.PreparationReceipt = &receipt
	if err := writeComputerDiskManifest(diskRoot, manifest); err != nil {
		return ComputerStorageResetReceipt{}, err
	}
	if err := engine.storageResetCheckpoint(computerStorageResetVerified); err != nil {
		return ComputerStorageResetReceipt{}, err
	}
	return receipt, nil
}

// ResetComputerStorage prepares and verifies N+1 without deleting N. L1 first
// publishes the verified successor; retirement then runs through the shared
// managed-volume delete and removal-attestation path.
func (engine *ContainerdEngine) ResetComputerStorage(ctx context.Context, request ResetComputerStorageRequest) (ResetComputerStorageResponse, error) {
	engine.storageResetMu.Lock()
	defer engine.storageResetMu.Unlock()
	if request.Storage.DiskBytes <= 0 || request.Storage.IntentRevision != request.Authority.IntentRevision ||
		request.NewGeneration != request.Storage.StorageGeneration+1 || request.Authority.NodeID == "" ||
		request.Authority.BootSessionID == "" || request.Authority.RootInstanceID == "" ||
		request.Authority.JobID == "" || request.Authority.PriorJobID == "" || request.Authority.HelperGeneration == 0 ||
		request.Authority.IntentRevision < 1 || strings.TrimSpace(request.Authority.CleanupFence) == "" {
		return ResetComputerStorageResponse{}, errors.New("Computer Storage reset request is incomplete")
	}
	if err := engine.fenceResetPredecessor(request.Storage, request.Authority); err != nil {
		return ResetComputerStorageResponse{}, err
	}
	receipt, err := engine.prepareResetSuccessor(ctx, request)
	if err != nil {
		return ResetComputerStorageResponse{}, err
	}
	return ResetComputerStorageResponse{Verified: true, Receipt: receipt}, nil
}
