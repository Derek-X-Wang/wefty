//go:build linux

package ocihelper

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

func (engine *ContainerdEngine) growCheckpoint(name string) error {
	if engine.computerGrowHook != nil {
		return engine.computerGrowHook(name)
	}
	return nil
}

func growReceipt(request GrowComputerStorageRequest, kind string, applied bool, failure string, available int64) (ComputerStorageGrowReceipt, error) {
	receiptID, err := randomCapability()
	if err != nil {
		return ComputerStorageGrowReceipt{}, err
	}
	return ComputerStorageGrowReceipt{Kind: kind, ReceiptID: receiptID,
		ComputerID: request.Storage.ComputerID, StorageID: request.Storage.StorageID,
		StorageGeneration: request.Storage.StorageGeneration, NodeID: request.Authority.NodeID,
		RootInstanceID: request.Authority.RootInstanceID, JobID: request.Authority.JobID,
		OperationRevision: request.Authority.OperationRevision, OperationFence: request.Authority.OperationFence,
		HelperGeneration: request.Authority.HelperGeneration, OldDiskBytes: request.Storage.DiskBytes,
		NewDiskBytes: request.NewDiskBytes, Applied: applied, FailureCode: failure,
		ObservedAvailableBytes: available}, nil
}

func (engine *ContainerdEngine) reserveGrowCapacity(request GrowComputerStorageRequest, diskRoot string, imageSize int64) (int64, bool, error) {
	engine.capacityMu.Lock()
	defer engine.capacityMu.Unlock()
	available, err := filesystemAvailableBytes(diskRoot)
	if err != nil {
		return 0, false, err
	}
	if engine.capacityReservations == nil {
		engine.capacityReservations = make(map[string]*capacityReservation)
	}
	reservation := engine.capacityReservations[request.Authority.JobID]
	if reservation != nil && reservation.diskBytes != request.Storage.DiskBytes && reservation.diskBytes != request.NewDiskBytes {
		return 0, false, errors.New("Computer grow conflicts with its atomic capacity reservation")
	}
	gate := request.NewDiskBytes - request.Storage.DiskBytes
	if imageSize < 0 {
		gate = request.NewDiskBytes
	} else if imageSize == request.NewDiskBytes {
		// A retry after filesystem expansion must not charge the already
		// materialized delta a second time.
		gate = 0
	}
	pending := int64(0)
	for jobID, existing := range engine.capacityReservations {
		if jobID == request.Authority.JobID || existing.diskMaterialized {
			continue
		}
		if existing.diskBytes > int64(^uint64(0)>>1)-pending {
			return 0, false, errors.New("Computer disk capacity accounting overflowed")
		}
		pending += existing.diskBytes
	}
	available -= pending
	if available < 0 {
		available = 0
	}
	if gate > available {
		return available, false, nil
	}
	if reservation == nil {
		reservation = &capacityReservation{attempts: make(map[string]struct{})}
		engine.capacityReservations[request.Authority.JobID] = reservation
	}
	reservation.diskBytes = request.NewDiskBytes
	reservation.diskMaterialized = false
	return available, true, nil
}

func (engine *ContainerdEngine) rollbackGrowCapacity(jobID string, oldBytes int64, imagePresent bool) {
	engine.capacityMu.Lock()
	defer engine.capacityMu.Unlock()
	reservation := engine.capacityReservations[jobID]
	if reservation == nil {
		return
	}
	if !imagePresent && len(reservation.attempts) == 0 {
		delete(engine.capacityReservations, jobID)
		return
	}
	reservation.diskBytes = oldBytes
	reservation.diskMaterialized = imagePresent
}

// acquireComputerGrowLock serializes with detach for an attached disk and
// with attach/reset/removal for a detached disk. The operation must hold this
// guard while it reads and republishes attachment.json.
func (engine *ContainerdEngine) acquireComputerGrowLock(diskRoot string, storage ComputerStorageReference) (*computerDiskAttachment, func(), error) {
	engine.mu.Lock()
	var attached *computerDiskAttachment
	for _, attempt := range engine.attempts {
		if attempt.computerDisk != nil && sameComputerStorageIdentity(attempt.computerDisk.storage, storage) {
			attached = attempt.computerDisk
			break
		}
	}
	engine.mu.Unlock()
	if attached != nil {
		attached.mu.Lock()
		if attached.detached {
			attached.mu.Unlock()
			return nil, nil, errors.New("Computer disk detached while grow authority was acquired")
		}
		return attached, attached.mu.Unlock, nil
	}
	lock, err := os.OpenFile(filepath.Join(diskRoot, "attachment.lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, nil, fmt.Errorf("open Computer grow attachment lock: %w", err)
	}
	if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = lock.Close()
		return nil, nil, errors.New("Computer Storage attachment ownership changed during grow")
	}
	release := func() {
		_ = unix.Flock(int(lock.Fd()), unix.LOCK_UN)
		_ = lock.Close()
	}
	return nil, release, nil
}

func growExt4(ctx context.Context, imagePath, loopDevice string, oldBytes, newBytes int64) error {
	info, err := os.Lstat(imagePath)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || (info.Size() != oldBytes && info.Size() != newBytes) {
		return errors.New("Computer disk image size conflicts with grow authority")
	}
	if info.Size() == oldBytes {
		if err := fullyAllocateComputerDisk(imagePath, newBytes); err != nil {
			return err
		}
	}
	resize2fs, err := findRootTool("resize2fs")
	if err != nil {
		return err
	}
	target := imagePath
	if loopDevice != "" {
		loop, err := os.OpenFile(loopDevice, os.O_RDWR, 0)
		if err != nil {
			return err
		}
		_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, loop.Fd(), uintptr(unix.LOOP_SET_CAPACITY), 0)
		_ = loop.Close()
		if errno != 0 {
			return errno
		}
		target = loopDevice
	}
	output, err := exec.CommandContext(ctx, resize2fs, target).CombinedOutput()
	if err != nil {
		return fmt.Errorf("expand ext4 Computer filesystem: %w: %s", err, strings.TrimSpace(string(output)))
	}
	return verifyComputerDiskAllocation(imagePath, newBytes)
}

func (engine *ContainerdEngine) resizeComputerStorage(ctx context.Context, imagePath, loopDevice string, oldBytes, newBytes int64) error {
	if engine.computerGrowResize != nil {
		return engine.computerGrowResize(ctx, imagePath, loopDevice, oldBytes, newBytes)
	}
	return growExt4(ctx, imagePath, loopDevice, oldBytes, newBytes)
}

func (engine *ContainerdEngine) GrowComputerStorage(ctx context.Context, request GrowComputerStorageRequest) (GrowComputerStorageResponse, error) {
	engine.storageResetMu.Lock()
	defer engine.storageResetMu.Unlock()
	engine.computerBackupMu.Lock()
	defer engine.computerBackupMu.Unlock()
	name, err := deterministicComputerDiskName(request.Storage)
	if err != nil {
		return GrowComputerStorageResponse{}, err
	}
	diskRoot := filepath.Join(engine.config.RuntimeRoot, "computer-disks", name)
	imagePath := filepath.Join(diskRoot, "disk.ext4")
	if err := os.MkdirAll(diskRoot, 0o700); err != nil {
		return GrowComputerStorageResponse{}, err
	}
	attachment, release, err := engine.acquireComputerGrowLock(diskRoot, request.Storage)
	if err != nil {
		return GrowComputerStorageResponse{}, err
	}
	defer release()
	manifestPath := filepath.Join(diskRoot, "attachment.json")
	manifest, present, err := readComputerDiskManifest(manifestPath)
	if err != nil {
		return GrowComputerStorageResponse{}, err
	}
	if present && (!sameComputerStorageIdentity(manifest.Storage, request.Storage) ||
		(manifest.Storage.DiskBytes != request.Storage.DiskBytes && manifest.Storage.DiskBytes != request.NewDiskBytes)) {
		return GrowComputerStorageResponse{}, errors.New("Computer grow manifest conflicts with exact Storage authority")
	}
	imageInfo, statErr := os.Lstat(imagePath)
	imagePresent := statErr == nil
	if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
		return GrowComputerStorageResponse{}, statErr
	}
	imageSize := int64(-1)
	if imagePresent {
		if !imageInfo.Mode().IsRegular() || (imageInfo.Size() != request.Storage.DiskBytes && imageInfo.Size() != request.NewDiskBytes) {
			return GrowComputerStorageResponse{}, errors.New("Computer disk image size conflicts with grow authority")
		}
		imageSize = imageInfo.Size()
	}
	available, admitted, err := engine.reserveGrowCapacity(request, diskRoot, imageSize)
	if err != nil {
		return GrowComputerStorageResponse{}, err
	}
	if !admitted {
		receipt, err := growReceipt(request, "computer_storage_grow_failed_unchanged", false, "insufficient_disk", available)
		return GrowComputerStorageResponse{Receipt: receipt}, err
	}
	if err := engine.growCheckpoint("capacity_reserved"); err != nil {
		return GrowComputerStorageResponse{}, err
	}
	loopDevice := ""
	if attachment != nil {
		loopDevice = attachment.loopDevice
	} else if present && manifest.Attached != nil {
		return GrowComputerStorageResponse{}, errors.New("Computer grow found an attached manifest without live attachment ownership")
	}
	if !imagePresent {
		if present && (manifest.Attached != nil || manifest.Pending != nil || manifest.PreviousDetachment != nil) {
			return GrowComputerStorageResponse{}, errors.New("Computer grow found missing bytes for existing attachment history")
		}
		temporary, err := os.CreateTemp(diskRoot, ".grow-disk.tmp-")
		if err != nil {
			return GrowComputerStorageResponse{}, err
		}
		temporaryPath := temporary.Name()
		_ = temporary.Close()
		defer os.Remove(temporaryPath)
		if err := engine.computerDiskSystem().allocateAndFormat(ctx, temporaryPath, request.NewDiskBytes); err != nil {
			if errors.Is(err, unix.ENOSPC) {
				engine.rollbackGrowCapacity(request.Authority.JobID, request.Storage.DiskBytes, imagePresent)
				receipt, receiptErr := growReceipt(request, "computer_storage_grow_failed_unchanged", false, "insufficient_disk", available)
				return GrowComputerStorageResponse{Receipt: receipt}, receiptErr
			}
			return GrowComputerStorageResponse{}, err
		}
		if err := os.Rename(temporaryPath, imagePath); err != nil {
			return GrowComputerStorageResponse{}, err
		}
	} else if err := engine.resizeComputerStorage(ctx, imagePath, loopDevice, request.Storage.DiskBytes, request.NewDiskBytes); err != nil {
		return GrowComputerStorageResponse{}, err
	}
	if err := engine.growCheckpoint("filesystem_expanded"); err != nil {
		return GrowComputerStorageResponse{}, err
	}
	if !present {
		manifest = computerDiskManifest{Version: computerDiskManifestVersion, Storage: request.Storage,
			DiskImage: "disk.ext4", MountDirectory: name, Prepared: true}
	}
	manifest.Storage.DiskBytes = request.NewDiskBytes
	if err := writeComputerDiskManifest(diskRoot, manifest); err != nil {
		return GrowComputerStorageResponse{}, err
	}
	if err := engine.growCheckpoint("manifest_published"); err != nil {
		return GrowComputerStorageResponse{}, err
	}
	if attachment != nil {
		attachment.storage.DiskBytes = request.NewDiskBytes
	}
	engine.markCapacityDiskMaterialized(request.Authority.JobID)
	receipt, err := growReceipt(request, "computer_storage_grow_applied", true, "", available)
	if err != nil {
		return GrowComputerStorageResponse{}, err
	}
	return GrowComputerStorageResponse{Receipt: receipt}, nil
}
