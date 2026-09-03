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
	"strconv"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

const computerStorageGrowIntentName = "storage-grow.json"

type computerStorageGrowIntent struct {
	Version int                        `json:"version"`
	Request GrowComputerStorageRequest `json:"request"`
}

func writeComputerStorageGrowIntent(root string, request GrowComputerStorageRequest) error {
	if existing, present, err := readComputerStorageGrowIntent(root); err != nil {
		return err
	} else if present {
		if existing.Request != request {
			return errors.New("Computer Storage grow intent conflicts with its immutable authority")
		}
		return nil
	}
	payload, err := json.Marshal(computerStorageGrowIntent{Version: 1, Request: request})
	if err != nil {
		return err
	}
	return writeDurableFile(root, ".storage-grow.json.tmp-", computerStorageGrowIntentName, payload, 0o600)
}

func readComputerStorageGrowIntent(root string) (computerStorageGrowIntent, bool, error) {
	payload, err := os.ReadFile(filepath.Join(root, computerStorageGrowIntentName))
	if errors.Is(err, os.ErrNotExist) {
		return computerStorageGrowIntent{}, false, nil
	}
	if err != nil {
		return computerStorageGrowIntent{}, false, err
	}
	var intent computerStorageGrowIntent
	if json.Unmarshal(payload, &intent) != nil || intent.Version != 1 {
		return computerStorageGrowIntent{}, false, errors.New("Computer Storage grow intent is corrupt")
	}
	request := intent.Request
	if request.Storage.ComputerID == "" || request.Storage.StorageID == "" || request.Storage.StorageGeneration == 0 || request.Storage.IntentRevision < 1 ||
		request.Storage.DiskBytes <= 0 || request.NewDiskBytes <= request.Storage.DiskBytes || request.Authority.JobID == "" || request.Authority.BootSessionID == "" ||
		request.Authority.NodeID == "" || request.Authority.HelperGeneration == 0 || request.Authority.RootInstanceID == "" ||
		request.Authority.OperationRevision < 1 || request.Authority.OperationFence == "" {
		return computerStorageGrowIntent{}, false, errors.New("Computer Storage grow intent is incomplete")
	}
	return intent, true, nil
}

func removeComputerStorageGrowIntent(root string) error {
	if err := os.Remove(filepath.Join(root, computerStorageGrowIntentName)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return syncDirectory(root)
}

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

type computerGrowCapacityState uint8

const (
	computerGrowCapacityCurrent computerGrowCapacityState = iota
	computerGrowCapacityPending
	computerGrowCapacityMaterialized
)

func classifyComputerGrowCapacity(reservation *capacityReservation, oldBytes, newBytes int64) (computerGrowCapacityState, error) {
	if reservation == nil || (reservation.diskBytes == oldBytes && reservation.pendingDiskBytes == 0) {
		return computerGrowCapacityCurrent, nil
	}
	delta := newBytes - oldBytes
	if reservation.diskBytes == oldBytes && reservation.pendingDiskBytes == delta {
		return computerGrowCapacityPending, nil
	}
	if reservation.diskBytes == newBytes && reservation.pendingDiskBytes == 0 {
		return computerGrowCapacityMaterialized, nil
	}
	return 0, errors.New("Computer grow conflicts with its atomic capacity reservation")
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
	delta := request.NewDiskBytes - request.Storage.DiskBytes
	if delta <= 0 {
		return 0, false, errors.New("Computer grow delta must be positive")
	}
	state, err := classifyComputerGrowCapacity(reservation, request.Storage.DiskBytes, request.NewDiskBytes)
	if err != nil {
		return 0, false, err
	}
	if state == computerGrowCapacityMaterialized && imageSize != request.NewDiskBytes {
		return 0, false, errors.New("Computer grow materialized capacity conflicts with the disk image")
	}
	for jobID, existing := range engine.capacityReservations {
		if jobID != request.Authority.JobID && existing.pendingDiskBytes != 0 &&
			existing.pendingDiskAdmissionCeiling > 0 && available > existing.pendingDiskAdmissionCeiling {
			available = existing.pendingDiskAdmissionCeiling
		}
	}
	admissionCeiling := available
	gate := delta
	if imageSize == request.NewDiskBytes || state == computerGrowCapacityPending {
		// A retry after filesystem expansion must not charge the already
		// admitted delta a second time.
		gate = 0
	}
	pending := int64(0)
	for jobID, existing := range engine.capacityReservations {
		if jobID == request.Authority.JobID {
			continue
		}
		charge := existing.pendingDiskBytes
		if !existing.diskMaterialized {
			charge += existing.diskBytes
		}
		if charge > int64(^uint64(0)>>1)-pending {
			return 0, false, errors.New("Computer disk capacity accounting overflowed")
		}
		pending += charge
	}
	available -= pending
	if available < 0 {
		available = 0
	}
	if gate > available {
		return available, false, nil
	}
	if reservation == nil {
		reservation = &capacityReservation{diskBytes: request.Storage.DiskBytes,
			diskMaterialized: true, attempts: make(map[string]struct{})}
		engine.capacityReservations[request.Authority.JobID] = reservation
	}
	if imageSize == request.NewDiskBytes && state != computerGrowCapacityPending {
		reservation.diskBytes = request.NewDiskBytes
		reservation.pendingDiskBytes = 0
		reservation.pendingDiskAdmissionCeiling = 0
	} else {
		// An expanded image is not materialized capacity proof: reassertion may
		// still fail on sparse ranges. Keep the delta charged to other newcomers
		// until the successful grow path verifies allocation and publishes it.
		reservation.pendingDiskBytes = delta
		if reservation.pendingDiskAdmissionCeiling == 0 {
			reservation.pendingDiskAdmissionCeiling = admissionCeiling
		}
	}
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
	reservation.pendingDiskBytes = 0
	reservation.pendingDiskAdmissionCeiling = 0
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

func readExt4FilesystemBytes(ctx context.Context, target string) (int64, error) {
	dumpe2fs, err := findRootTool("dumpe2fs")
	if err != nil {
		return 0, err
	}
	output, err := exec.CommandContext(ctx, dumpe2fs, "-h", target).CombinedOutput()
	if err != nil {
		return 0, fmt.Errorf("read ext4 Computer filesystem size: %w: %s", err, strings.TrimSpace(string(output)))
	}
	var blockCount, blockSize int64
	for _, line := range strings.Split(string(output), "\n") {
		key, value, found := strings.Cut(line, ":")
		if !found {
			continue
		}
		number, parseErr := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
		if parseErr != nil {
			continue
		}
		switch strings.TrimSpace(key) {
		case "Block count":
			blockCount = number
		case "Block size":
			blockSize = number
		}
	}
	if blockCount <= 0 || blockSize <= 0 || blockCount > int64(^uint64(0)>>1)/blockSize {
		return 0, errors.New("ext4 Computer filesystem size readback was incomplete")
	}
	return blockCount * blockSize, nil
}

func growExt4(ctx context.Context, imagePath, loopDevice string, oldBytes, newBytes int64,
	readFilesystemBytes func(context.Context, string) (int64, error),
	allocate func(string, int64) error,
) error {
	info, err := os.Lstat(imagePath)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || (info.Size() != oldBytes && info.Size() != newBytes) {
		return errors.New("Computer disk image size conflicts with grow authority")
	}
	if info.Size() == oldBytes {
		file, openErr := os.OpenFile(imagePath, os.O_RDWR, 0)
		if openErr != nil {
			return openErr
		}
		allocationErr := unix.Fallocate(int(file.Fd()), unix.FALLOC_FL_KEEP_SIZE, oldBytes, newBytes-oldBytes)
		if allocationErr == nil {
			allocationErr = file.Truncate(newBytes)
		}
		if allocationErr == nil {
			allocationErr = file.Sync()
		}
		allocationErr = errors.Join(allocationErr, file.Close())
		if allocationErr != nil {
			_ = os.Truncate(imagePath, oldBytes)
			return allocationErr
		}
		if err := verifyComputerDiskAllocation(imagePath, newBytes); err != nil {
			_ = os.Truncate(imagePath, oldBytes)
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
		return &ComputerStorageGrowUncertainError{Cause: fmt.Errorf("expand ext4 Computer filesystem: %w: %s", err, strings.TrimSpace(string(output)))}
	}
	observed, err := readFilesystemBytes(ctx, target)
	if err != nil || observed < newBytes {
		if err != nil {
			return &ComputerStorageGrowUncertainError{Cause: err}
		}
		return &ComputerStorageGrowUncertainError{Cause: fmt.Errorf("ext4 Computer filesystem size readback = %d, want at least %d", observed, newBytes)}
	}
	if err := reassertComputerGrowAllocation(imagePath, newBytes, allocate); err != nil {
		return &ComputerStorageGrowUncertainError{Cause: err}
	}
	return nil
}

func reassertComputerGrowAllocation(imagePath string, newBytes int64, allocate func(string, int64) error) error {
	info, err := os.Lstat(imagePath)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() != newBytes {
		return fmt.Errorf("Computer disk allocation does not match its %d-byte grow target", newBytes)
	}
	// Like mkfs, resize2fs may discard or sparse-write ranges in the backing
	// file. Reassert allocation after the filesystem operation so a successful
	// grow cannot strand a manifest whose image has fewer blocks than its budget.
	if allocate == nil {
		allocate = fullyAllocateComputerDisk
	}
	if err := allocate(imagePath, newBytes); err != nil {
		return err
	}
	return verifyComputerDiskAllocation(imagePath, newBytes)
}

func (engine *ContainerdEngine) resizeComputerStorage(ctx context.Context, imagePath, loopDevice string, oldBytes, newBytes int64) error {
	if engine.computerGrowResize != nil {
		if err := engine.computerGrowResize(ctx, imagePath, loopDevice, oldBytes, newBytes); err != nil {
			return err
		}
		reader := engine.computerGrowFilesystemBytes
		if reader == nil {
			reader = readExt4FilesystemBytes
		}
		target := imagePath
		if loopDevice != "" {
			target = loopDevice
		}
		observed, err := reader(ctx, target)
		if err != nil {
			return &ComputerStorageGrowUncertainError{Cause: err}
		}
		if observed < newBytes {
			return &ComputerStorageGrowUncertainError{Cause: fmt.Errorf("ext4 Computer filesystem size readback = %d, want at least %d", observed, newBytes)}
		}
		if err := reassertComputerGrowAllocation(imagePath, newBytes, engine.computerGrowAllocate); err != nil {
			return &ComputerStorageGrowUncertainError{Cause: err}
		}
		return nil
	}
	reader := engine.computerGrowFilesystemBytes
	if reader == nil {
		reader = readExt4FilesystemBytes
	}
	return growExt4(ctx, imagePath, loopDevice, oldBytes, newBytes, reader, engine.computerGrowAllocate)
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
	if quarantined, quarantineErr := computerDiskQuarantined(engine.config.RuntimeRoot, request.Storage); quarantineErr != nil {
		return GrowComputerStorageResponse{}, quarantineErr
	} else if quarantined {
		return GrowComputerStorageResponse{}, &ComputerStorageQuarantinedError{Storage: request.Storage}
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
	if !present {
		prepared, err := durableComputerStoragePreparationExists(diskRoot, request.Storage)
		if err != nil {
			return GrowComputerStorageResponse{}, err
		}
		if prepared {
			return GrowComputerStorageResponse{}, &ComputerStorageGrowUncertainError{Cause: errors.New("Computer grow refuses to reconstruct a missing attachment manifest over durable preparation evidence")}
		}
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
	if !imagePresent {
		return GrowComputerStorageResponse{}, errors.New("Computer grow refused because the current disk image is missing")
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
	if err := writeComputerStorageGrowIntent(diskRoot, request); err != nil {
		return GrowComputerStorageResponse{}, err
	}
	loopDevice := ""
	if attachment != nil {
		loopDevice = attachment.loopDevice
	} else if present && manifest.Attached != nil {
		return GrowComputerStorageResponse{}, errors.New("Computer grow found an attached manifest without live attachment ownership")
	}
	if err := engine.resizeComputerStorage(ctx, imagePath, loopDevice, request.Storage.DiskBytes, request.NewDiskBytes); err != nil {
		var uncertain *ComputerStorageGrowUncertainError
		if errors.As(err, &uncertain) {
			return GrowComputerStorageResponse{}, err
		}
		engine.rollbackGrowCapacity(request.Authority.JobID, request.Storage.DiskBytes, true)
		_ = os.Truncate(imagePath, request.Storage.DiskBytes)
		if removeErr := removeComputerStorageGrowIntent(diskRoot); removeErr != nil {
			return GrowComputerStorageResponse{}, errors.Join(err, removeErr)
		}
		if errors.Is(err, unix.ENOSPC) || strings.Contains(strings.ToLower(err.Error()), "no space left on device") {
			receipt, receiptErr := growReceipt(request, "computer_storage_grow_failed_unchanged", false, "insufficient_disk", available)
			return GrowComputerStorageResponse{Receipt: receipt}, receiptErr
		}
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
	if err := removeComputerStorageGrowIntent(diskRoot); err != nil {
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

func durableComputerStoragePreparationExists(root string, storage ComputerStorageReference) (bool, error) {
	if witness, present, err := readComputerStoragePreparationWitness(root); err != nil {
		return false, err
	} else if present {
		if witness.ComputerID != storage.ComputerID || witness.StorageID != storage.StorageID || witness.StorageGeneration != storage.StorageGeneration {
			return false, errors.New("Computer grow found a conflicting durable Storage preparation witness")
		}
		return true, nil
	}
	copyManifest, present, err := readComputerStorageCopyManifest(filepath.Join(root, "storage-copy.json"))
	if err != nil {
		return false, err
	}
	if !present || copyManifest.Phase != computerStorageCopyPublished || copyManifest.Receipt == nil {
		return false, nil
	}
	receipt := copyManifest.Receipt
	if receipt.DestinationComputerID != storage.ComputerID || receipt.DestinationStorageID != storage.StorageID || receipt.DestinationGeneration != storage.StorageGeneration {
		return false, errors.New("Computer grow found a conflicting durable Storage copy receipt")
	}
	return true, nil
}
