//go:build linux

package ocihelper

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"golang.org/x/sys/unix"
)

const (
	computerDiskManifestVersion          = 1
	computerStoragePreparationRecordName = "storage-preparation.json"
)

type computerDiskCheckpoint string

const (
	computerDiskManifestBeforeImage computerDiskCheckpoint = "manifest_before_image"
	computerDiskImageBeforePhase    computerDiskCheckpoint = "image_before_phase"
	computerDiskPendingBeforeAttach computerDiskCheckpoint = "pending_before_attach"
	computerDiskAttachedBeforePhase computerDiskCheckpoint = "attached_before_phase"
	computerDiskDetached            computerDiskCheckpoint = "detached"
)

func (engine *ContainerdEngine) computerDiskCheckpoint(checkpoint computerDiskCheckpoint) error {
	if engine.computerDiskHook == nil {
		return nil
	}
	return engine.computerDiskHook(checkpoint)
}

type computerDiskManifest struct {
	Version            int                            `json:"version"`
	Storage            ComputerStorageReference       `json:"storage"`
	DiskImage          string                         `json:"disk_image"`
	MountDirectory     string                         `json:"mount_directory"`
	Prepared           bool                           `json:"prepared,omitempty"`
	Preparation        *ComputerStorageResetAuthority `json:"preparation,omitempty"`
	PreparationReceipt *ComputerStorageResetReceipt   `json:"preparation_receipt,omitempty"`
	Retirement         *ComputerStorageResetAuthority `json:"retirement,omitempty"`
	LoopDevice         string                         `json:"loop_device,omitempty"`
	Attached           *AttemptAuthority              `json:"attached,omitempty"`
	Pending            *AttemptAuthority              `json:"pending,omitempty"`
	PreviousDetachment *computerDiskEvidence          `json:"previous_detachment,omitempty"`
}

func readComputerStoragePreparationWitness(root string) (*contract.ComputerStoragePreparationWitness, bool, error) {
	payload, err := os.ReadFile(filepath.Join(root, computerStoragePreparationRecordName))
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	var witness contract.ComputerStoragePreparationWitness
	if json.Unmarshal(payload, &witness) != nil || !witness.Valid() {
		return nil, false, errors.New("Computer Storage preparation record is corrupt")
	}
	return &witness, true, nil
}

func persistComputerStoragePreparationWitness(root string, witness contract.ComputerStoragePreparationWitness) error {
	if !witness.Valid() {
		return errors.New("Computer Storage preparation witness is incomplete")
	}
	if existing, present, err := readComputerStoragePreparationWitness(root); err != nil {
		return err
	} else if present {
		if *existing != witness {
			return errors.New("Computer Storage preparation witness conflicts with its immutable record")
		}
		return nil
	}
	payload, err := json.Marshal(witness)
	if err != nil {
		return err
	}
	return writeDurableFile(root, ".storage-preparation.json.tmp-", computerStoragePreparationRecordName, payload, 0o600)
}

type computerDiskAttachment struct {
	name       string
	storage    ComputerStorageReference
	imagePath  string
	mountPath  string
	loopDevice string
	authority  AttemptAuthority
	lock       *os.File
	mu         sync.Mutex
	detached   bool
	fresh      bool
}

type computerDiskSystem interface {
	allocateAndFormat(context.Context, string, int64) error
	attachAndMount(context.Context, string, string) (string, error)
	detach(string, string, string) error
	mountedSource(string) (string, bool, error)
	loopBackingFile(string) (string, bool, error)
	loopsForRoot(string) (map[string]string, error)
}

type linuxComputerDiskSystem struct{}

func deterministicComputerDiskName(storage ComputerStorageReference) (string, error) {
	return DeterministicComputerDiskName(storage)
}

func (engine *ContainerdEngine) computerDiskSystem() computerDiskSystem {
	if engine.diskSystem != nil {
		return engine.diskSystem
	}
	return linuxComputerDiskSystem{}
}

func (engine *ContainerdEngine) attachComputerDisk(ctx context.Context, storage ComputerStorageReference, authority AttemptAuthority) (_ *computerDiskAttachment, err error) {
	engine.computerReimageMu.Lock()
	reimageLocked := true
	defer func() {
		if reimageLocked {
			engine.computerReimageMu.Unlock()
		}
	}()
	if storage.DiskBytes <= 0 || storage.IntentRevision < 1 {
		return nil, errors.New("Computer disk requires a positive allocation")
	}
	name, err := deterministicComputerDiskName(storage)
	if err != nil {
		return nil, err
	}
	if quarantined, err := computerDiskQuarantined(engine.config.RuntimeRoot, storage); err != nil {
		return nil, err
	} else if quarantined {
		return nil, &ComputerStorageQuarantinedError{Storage: storage}
	}
	diskRoot := filepath.Join(engine.config.RuntimeRoot, "computer-disks", name)
	if pending, pendingErr := engine.computerStorageResumePending(diskRoot, name); pendingErr != nil {
		return nil, pendingErr
	} else if pending {
		return nil, &ComputerStorageResumeDeferredError{Storage: storage}
	}
	mountPath := filepath.Join(engine.config.RuntimeRoot, "computer-mounts", name)
	if err := os.MkdirAll(diskRoot, 0o700); err != nil {
		return nil, fmt.Errorf("create Computer disk root: %w", err)
	}
	lockPath := filepath.Join(diskRoot, "attachment.lock")
	lock, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open Computer attachment lock: %w", err)
	}
	defer func() {
		if err != nil {
			_ = unix.Flock(int(lock.Fd()), unix.LOCK_UN)
			_ = lock.Close()
		}
	}()
	if err = unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return nil, errComputerStorageAttachmentOwned
	}
	// The node-wide mutex only orders manifest/flock admission against
	// preflight. The generation flock now owns this disk, so formatting and
	// mounting it must not freeze unrelated Computers on the node.
	engine.computerReimageMu.Unlock()
	reimageLocked = false
	manifestPath := filepath.Join(diskRoot, "attachment.json")
	manifest, present, err := readComputerDiskManifest(manifestPath)
	if err != nil {
		return nil, err
	}
	if present {
		if !sameComputerStorageIdentity(manifest.Storage, storage) || manifest.DiskImage != "disk.ext4" || manifest.MountDirectory != name {
			return nil, errors.New("Computer disk manifest does not match its durable Storage identity")
		}
		if manifest.Retirement != nil {
			return nil, &computerStorageRetiredError{}
		}
		if manifest.Attached != nil {
			return nil, errors.New("Computer Storage generation remains attached; lock disappearance is not detachment proof")
		}
		if manifest.Pending != nil {
			return nil, errors.New("Computer Storage generation has an unresolved pending attachment")
		}
		if manifest.Preparation != nil && manifest.PreparationReceipt == nil {
			return nil, errors.New("Computer Storage reset preparation is not verified for attachment")
		}
		// An authority manifest with no attachment history is the durable first-
		// allocation checkpoint. It may be resumed whether the image rename has
		// happened or not. Once any detachment evidence exists, it must be exact;
		// missing evidence never authorizes reuse of a previously attached disk.
		if !manifest.Prepared && manifest.PreviousDetachment != nil &&
			!validComputerDiskEvidence(manifest.PreviousDetachment, storage, authority) {
			return nil, errors.New("Computer Storage generation lacks positive prior attachment detachment evidence")
		}
		if storage.IntentRevision < manifest.Storage.IntentRevision {
			return nil, errors.New("Computer Storage attachment intent revision is stale")
		}
	}
	imagePath := filepath.Join(diskRoot, "disk.ext4")
	createdImage := false
	if _, statErr := os.Lstat(imagePath); errors.Is(statErr, os.ErrNotExist) {
		if present && (manifest.Attached != nil || manifest.Pending != nil || manifest.PreviousDetachment != nil || manifest.Prepared || manifest.Retirement != nil) {
			return nil, errors.New("Computer disk image is missing for an existing manifest")
		}
		if !present {
			manifest = computerDiskManifest{Version: computerDiskManifestVersion, Storage: storage,
				DiskImage: "disk.ext4", MountDirectory: name}
			if err = writeComputerDiskManifest(diskRoot, manifest); err != nil {
				return nil, err
			}
			present = true
			if err = engine.computerDiskCheckpoint(computerDiskManifestBeforeImage); err != nil {
				return nil, err
			}
		}
		temporary, createErr := os.CreateTemp(diskRoot, ".disk.ext4.tmp-")
		if createErr != nil {
			return nil, fmt.Errorf("create Computer disk staging file: %w", createErr)
		}
		temporaryPath := temporary.Name()
		if closeErr := temporary.Close(); closeErr != nil {
			_ = os.Remove(temporaryPath)
			return nil, closeErr
		}
		defer func() { _ = os.Remove(temporaryPath) }()
		if err = engine.computerDiskSystem().allocateAndFormat(ctx, temporaryPath, storage.DiskBytes); err != nil {
			if errors.Is(err, syscall.ENOSPC) {
				var stat unix.Statfs_t
				available := int64(0)
				if statErr := unix.Statfs(diskRoot, &stat); statErr == nil {
					const maxInt64 = int64(^uint64(0) >> 1)
					if stat.Bsize > 0 && stat.Bavail <= uint64(maxInt64/stat.Bsize) {
						available = int64(stat.Bavail) * stat.Bsize
					}
				}
				err = &insufficientDiskError{RequestedBytes: storage.DiskBytes, ObservedAvailableBytes: available, err: err}
			}
			_ = os.Remove(manifestPath)
			return nil, fmt.Errorf("fully allocate Computer disk: %w", err)
		}
		if err = os.Rename(temporaryPath, imagePath); err != nil {
			return nil, fmt.Errorf("publish Computer disk image: %w", err)
		}
		createdImage = true
		if err = syncDirectory(diskRoot); err != nil {
			_ = os.Remove(imagePath)
			return nil, err
		}
		if err = engine.computerDiskCheckpoint(computerDiskImageBeforePhase); err != nil {
			return nil, err
		}
	} else if statErr != nil {
		return nil, fmt.Errorf("inspect Computer disk image: %w", statErr)
	}
	allocationBytes := storage.DiskBytes
	if present {
		allocationBytes = manifest.Storage.DiskBytes
	}
	if err = verifyComputerDiskAllocation(imagePath, allocationBytes); err != nil {
		if createdImage {
			_ = os.Remove(imagePath)
		}
		return nil, err
	}
	if !createdImage && !present {
		return nil, errors.New("Computer disk image exists without an authority manifest")
	}
	if createdImage {
		manifest.Prepared = true
		if err = writeComputerDiskManifest(diskRoot, manifest); err != nil {
			_ = os.Remove(imagePath)
			return nil, err
		}
	}
	// Only Reset's verified preparation proves an existing image still contains
	// a freshly formatted empty filesystem. Copy and grow also publish Prepared
	// manifests, but their bytes are already tenant-owned.
	freshRoot := createdImage || (manifest.Prepared && manifest.PreparationReceipt != nil)
	{
		previousDetachment := manifest.PreviousDetachment
		storage.DiskBytes = manifest.Storage.DiskBytes
		manifest.Storage = storage
		manifest.Pending = &authority
		manifest.PreviousDetachment = nil
		if err = writeComputerDiskManifest(diskRoot, manifest); err != nil {
			return nil, err
		}
		// Arm in-process rollback before exposing the pending checkpoint. A
		// returned checkpoint failure must not strand the generation as pending;
		// an actual process crash is instead reconciled by the next boot sweep.
		defer func() {
			if err != nil {
				manifest.Pending = nil
				manifest.PreviousDetachment = previousDetachment
				_ = writeComputerDiskManifest(diskRoot, manifest)
			}
		}()
		if err = engine.computerDiskCheckpoint(computerDiskPendingBeforeAttach); err != nil {
			return nil, err
		}
	}
	if err = os.MkdirAll(mountPath, 0o700); err != nil {
		if createdImage {
			_ = os.Remove(manifestPath)
			_ = os.Remove(imagePath)
		}
		return nil, err
	}
	loopDevice, err := engine.computerDiskSystem().attachAndMount(ctx, imagePath, mountPath)
	if err != nil {
		if createdImage {
			_ = os.Remove(manifestPath)
			_ = os.Remove(imagePath)
		}
		return nil, fmt.Errorf("attach Computer disk: %w", err)
	}
	if err = engine.computerDiskCheckpoint(computerDiskAttachedBeforePhase); err != nil {
		_ = engine.computerDiskSystem().detach(mountPath, loopDevice, imagePath)
		return nil, err
	}
	attachment := &computerDiskAttachment{name: name, storage: manifest.Storage, imagePath: imagePath, mountPath: mountPath, loopDevice: loopDevice, authority: authority, lock: lock, fresh: freshRoot}
	manifest = computerDiskManifest{
		Version: computerDiskManifestVersion, Storage: attachment.storage, DiskImage: "disk.ext4", MountDirectory: name,
		LoopDevice: loopDevice, Attached: &authority,
	}
	if err = writeComputerDiskManifest(diskRoot, manifest); err != nil {
		_ = engine.computerDiskSystem().detach(mountPath, loopDevice, imagePath)
		if createdImage {
			_ = os.Remove(manifestPath)
			_ = os.Remove(imagePath)
		}
		return nil, err
	}
	return attachment, nil
}

func computerDiskQuarantined(runtimeRoot string, storage ComputerStorageReference) (bool, error) {
	name, err := deterministicComputerDiskName(storage)
	if err != nil {
		return false, err
	}
	entries, err := readDirectoryIfPresent(filepath.Join(runtimeRoot, "computer-disk-quarantine"))
	if err != nil {
		return false, err
	}
	for _, entry := range entries {
		path := filepath.Join(runtimeRoot, "computer-disk-quarantine", entry.Name())
		if entry.IsDir() && strings.HasPrefix(entry.Name(), name+"-anomaly-") {
			receipt, readErr := readAndValidateComputerDiskQuarantineReceipt(filepath.Join(path, "quarantine.json"))
			if readErr == nil && receipt.DiskName == name && entry.Name() == name+"-anomaly-"+receipt.ReceiptID {
				if receipt.Kind == computerDiskQuarantineKindAuthority ||
					receipt.Kind == computerDiskQuarantineKindGeneration && sameComputerStorageIdentity(receipt.Storage, storage) {
					return true, nil
				}
			}
		}
		if !entry.IsDir() && strings.HasPrefix(entry.Name(), name+"-cleanup-") && strings.HasSuffix(entry.Name(), ".json") {
			var receipt ManagedVolumeQuarantineReceipt
			payload, readErr := os.ReadFile(path)
			if readErr == nil && json.Unmarshal(payload, &receipt) == nil && receipt.Kind == "managed_volume_cleanup_quarantined" &&
				receipt.ReceiptID != "" && receipt.VolumeKind == ManagedVolumeComputerDisk && receipt.Removal.RemovalGeneration > 0 &&
				sameComputerStorageIdentity(receipt.ComputerStorage, storage) &&
				entry.Name() == fmt.Sprintf("%s-cleanup-%d.json", name, receipt.Removal.RemovalGeneration) {
				return true, nil
			}
		}
	}
	return false, nil
}

func (engine *ContainerdEngine) quarantineComputerDiskCleanup(request DeleteManagedVolumeRequest) (ManagedVolumeQuarantineReceipt, error) {
	name, err := deterministicComputerDiskName(*request.ComputerStorage)
	if err != nil {
		return ManagedVolumeQuarantineReceipt{}, err
	}
	receiptID, err := randomCapability()
	if err != nil {
		return ManagedVolumeQuarantineReceipt{}, err
	}
	receipt := ManagedVolumeQuarantineReceipt{Kind: "managed_volume_cleanup_quarantined", ReceiptID: receiptID,
		VolumeKind: request.Kind, ComputerStorage: *request.ComputerStorage, Removal: *request.Removal,
		FailureReason: EngineFailureOperationFailed, Attempts: request.FailureAttempts}
	root := filepath.Join(engine.config.RuntimeRoot, "computer-disk-quarantine")
	if err := os.MkdirAll(root, 0o700); err != nil {
		return ManagedVolumeQuarantineReceipt{}, err
	}
	payload, err := json.Marshal(receipt)
	if err != nil {
		return ManagedVolumeQuarantineReceipt{}, err
	}
	payload = append(payload, '\n')
	temporary, err := os.CreateTemp(root, ".cleanup-quarantine.tmp-")
	if err != nil {
		return ManagedVolumeQuarantineReceipt{}, err
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	writeErr := temporary.Chmod(0o600)
	if writeErr == nil {
		_, writeErr = temporary.Write(payload)
	}
	if writeErr == nil {
		writeErr = temporary.Sync()
	}
	writeErr = errors.Join(writeErr, temporary.Close())
	if writeErr != nil {
		return ManagedVolumeQuarantineReceipt{}, writeErr
	}
	path := filepath.Join(root, fmt.Sprintf("%s-cleanup-%d.json", name, request.Removal.RemovalGeneration))
	if err := os.Rename(temporaryName, path); err != nil {
		return ManagedVolumeQuarantineReceipt{}, err
	}
	if err := syncDirectory(root); err != nil {
		return ManagedVolumeQuarantineReceipt{}, err
	}
	return receipt, nil
}

func (engine *ContainerdEngine) deleteComputerDisk(storage ComputerStorageReference, removal ManagedVolumeRemovalAuthority) error {
	engine.computerReimageMu.Lock()
	reimageLocked := true
	defer func() {
		if reimageLocked {
			engine.computerReimageMu.Unlock()
		}
	}()
	name, err := deterministicComputerDiskName(storage)
	if err != nil {
		return err
	}
	quarantineEntryMatches := func(entry string) bool {
		return entry == name || strings.HasPrefix(entry, name+"-") ||
			strings.HasPrefix(entry, "."+name+"-") && strings.HasSuffix(entry, computerDiskQuarantineGCFailureSuffix)
	}
	diskRoot := filepath.Join(engine.config.RuntimeRoot, "computer-disks", name)
	var lock *os.File
	defer func() {
		if lock != nil {
			_ = unix.Flock(int(lock.Fd()), unix.LOCK_UN)
			_ = lock.Close()
		}
	}()
	if _, statErr := os.Lstat(diskRoot); statErr == nil {
		lock, err = os.OpenFile(filepath.Join(diskRoot, "attachment.lock"), os.O_CREATE|os.O_RDWR, 0o600)
		if err != nil {
			return err
		}
		if err = unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
			return errors.New("Computer disk deletion could not acquire detached generation lock")
		}
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return statErr
	}
	// The per-generation flock, when present, now excludes preflight and
	// attachment for this disk. Release the node-wide admission mutex before
	// filesystem inspection and deletion so other Computers remain live.
	engine.computerReimageMu.Unlock()
	reimageLocked = false
	manifest, present, err := readComputerDiskManifest(filepath.Join(diskRoot, "attachment.json"))
	if err != nil {
		return err
	}
	if present {
		if !sameComputerStorageIdentity(manifest.Storage, storage) || manifest.Attached != nil || manifest.Pending != nil {
			return errors.New("Computer disk deletion lacks exact detached manifest authority")
		}
		if manifest.Retirement == nil && !manifest.Prepared && manifest.PreviousDetachment != nil {
			if !validComputerDiskConsumerDetachmentEvidence(manifest.PreviousDetachment, storage, computerDiskDetachmentAuthority{
				NodeID: removal.NodeID, BootSessionID: removal.BootSessionID, PriorJobID: removal.PriorJobID,
			}) {
				return errors.New("Computer disk deletion receipt does not match removal authority")
			}
		} else if manifest.Retirement == nil && !manifest.Prepared {
			return errors.New("Computer disk deletion lacks detachment or staging authority")
		}
	} else if _, statErr := os.Lstat(diskRoot); statErr == nil {
		return errors.New("Computer disk deletion found bytes without an authority manifest")
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return statErr
	}
	imagePath := filepath.Join(diskRoot, "disk.ext4")
	mountPath := filepath.Join(engine.config.RuntimeRoot, "computer-mounts", name)
	if _, mounted, err := engine.computerDiskSystem().mountedSource(mountPath); err != nil {
		return err
	} else if mounted {
		return errors.New("Computer disk remains mounted during deletion")
	}
	loops, err := engine.computerDiskSystem().loopsForRoot(diskRoot)
	if err != nil {
		return err
	}
	for _, backing := range loops {
		if backing == imagePath {
			return errors.New("Computer disk loop remains during deletion")
		}
	}
	if err := os.RemoveAll(diskRoot); err != nil {
		return err
	}
	if err := os.Remove(mountPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	resetManifestPath := filepath.Join(engine.config.RuntimeRoot, "computer-storage-resets", name+".json")
	if err := os.Remove(resetManifestPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	quarantineRoot := filepath.Join(engine.config.RuntimeRoot, "computer-disk-quarantine")
	quarantines, err := readDirectoryIfPresent(quarantineRoot)
	if err != nil {
		return err
	}
	for _, entry := range quarantines {
		if quarantineEntryMatches(entry.Name()) {
			if err := os.RemoveAll(filepath.Join(quarantineRoot, entry.Name())); err != nil {
				return err
			}
		}
	}
	for _, path := range []string{diskRoot, imagePath, filepath.Join(diskRoot, "attachment.json"), mountPath} {
		if _, err := os.Lstat(path); err == nil {
			return fmt.Errorf("Computer disk removal left %s", filepath.Base(path))
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	if _, err := os.Lstat(resetManifestPath); err == nil {
		return errors.New("Computer disk removal left reset manifest")
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	quarantines, err = readDirectoryIfPresent(quarantineRoot)
	if err != nil {
		return err
	}
	for _, entry := range quarantines {
		if quarantineEntryMatches(entry.Name()) {
			return errors.New("Computer disk removal left quarantine residue")
		}
	}
	return nil
}

func (engine *ContainerdEngine) detachComputerDisk(attachment *computerDiskAttachment, kind, sweepEpoch string) error {
	if attachment == nil {
		return nil
	}
	attachment.mu.Lock()
	defer attachment.mu.Unlock()
	if attachment.detached {
		return nil
	}
	diskRoot := filepath.Dir(attachment.imagePath)
	manifest, present, err := readComputerDiskManifest(filepath.Join(diskRoot, "attachment.json"))
	if err != nil {
		return err
	}
	if !present || manifest.Attached == nil || !sameComputerStorageIdentity(manifest.Storage, attachment.storage) || *manifest.Attached != attachment.authority {
		return errors.New("Computer attachment manifest no longer matches exact attempt authority")
	}
	if err := engine.computerDiskSystem().detach(attachment.mountPath, attachment.loopDevice, attachment.imagePath); err != nil {
		return err
	}
	if _, mounted, err := engine.computerDiskSystem().mountedSource(attachment.mountPath); err != nil {
		return err
	} else if mounted {
		return errors.New("Computer disk mount remained after detach")
	}
	if backing, present, err := engine.computerDiskSystem().loopBackingFile(attachment.loopDevice); err != nil {
		return err
	} else if present && backing == attachment.imagePath {
		return errors.New("Computer disk loop remained after detach")
	}
	evidence, err := newComputerDiskEvidence(kind, sweepEpoch, attachment.storage, attachment.authority)
	if err != nil {
		return errors.New("generate Computer disk detachment receipt")
	}
	manifest.Attached = nil
	manifest.LoopDevice = ""
	manifest.PreviousDetachment = &evidence
	if err := writeComputerDiskManifest(diskRoot, manifest); err != nil {
		return err
	}
	if err := engine.computerDiskCheckpoint(computerDiskDetached); err != nil {
		return err
	}
	attachment.detached = true
	_ = unix.Flock(int(attachment.lock.Fd()), unix.LOCK_UN)
	_ = attachment.lock.Close()
	return nil
}

func readComputerDiskManifest(path string) (computerDiskManifest, bool, error) {
	payload, present, err := readComputerRecoveryRecord(path)
	if !present && err == nil {
		return computerDiskManifest{}, false, nil
	}
	if err != nil {
		return computerDiskManifest{}, false, fmt.Errorf("read Computer disk manifest: %w", err)
	}
	var manifest computerDiskManifest
	if err := json.Unmarshal(payload, &manifest); err != nil || manifest.Version != computerDiskManifestVersion {
		return computerDiskManifest{}, false, errors.New("Computer disk manifest is invalid")
	}
	return manifest, true, nil
}

func writeComputerDiskManifest(root string, manifest computerDiskManifest) error {
	payload, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	payload = append(payload, '\n')
	if err := writeDurableFile(root, ".attachment.json.tmp-", "attachment.json", payload, 0o600); err != nil {
		return fmt.Errorf("write Computer disk manifest: %w", err)
	}
	return nil
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func verifyComputerDiskAllocation(path string, bytes int64) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() != bytes {
		return fmt.Errorf("Computer disk allocation does not match its %d-byte budget", bytes)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Blocks*512 < bytes {
		return errors.New("Computer disk image is not fully allocated")
	}
	return nil
}

func migrateComputerDiskOwnership(root string, uid, gid uint32, lchown func(string, int, int) error) error {
	return filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		return lchown(path, int(uid), int(gid))
	})
}

func syncComputerDiskFilesystem(root string) error {
	directory, err := os.Open(root)
	if err != nil {
		return err
	}
	err = unix.Syncfs(int(directory.Fd()))
	return errors.Join(err, directory.Close())
}

func initializeComputerDiskRoot(attachment *computerDiskAttachment, uid, gid uint32, migrate bool) error {
	if attachment == nil {
		return nil
	}
	if attachment.fresh {
		if err := os.Chown(attachment.mountPath, int(uid), int(gid)); err != nil {
			return fmt.Errorf("initialize Computer disk root ownership: %w", err)
		}
	} else if migrate {
		// WalkDir uses lstat semantics. Lchown changes a symlink's own metadata
		// and never follows tenant-controlled links outside the mounted disk.
		if err := migrateComputerDiskOwnership(attachment.mountPath, uid, gid, os.Lchown); err != nil {
			return fmt.Errorf("migrate Computer disk ownership: %w", err)
		}
		if err := syncComputerDiskFilesystem(attachment.mountPath); err != nil {
			return fmt.Errorf("sync Computer disk ownership migration: %w", err)
		}
	}
	info, err := os.Stat(attachment.mountPath)
	if err != nil {
		return fmt.Errorf("verify Computer disk root ownership: %w", err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uid || stat.Gid != gid {
		return errors.New("Computer disk root ownership does not match image process owner")
	}
	return nil
}

func (linuxComputerDiskSystem) allocateAndFormat(ctx context.Context, path string, bytes int64) error {
	if err := fullyAllocateComputerDisk(path, bytes); err != nil {
		return err
	}
	mkfs, err := findRootTool("mkfs.ext4")
	if err != nil {
		return err
	}
	output, err := exec.CommandContext(ctx, mkfs, "-q", "-F", "-m", "0", path).CombinedOutput()
	if err != nil {
		return fmt.Errorf("format ext4 Computer disk: %w: %s", err, strings.TrimSpace(string(output)))
	}
	// Reassert the final formatted file's allocation so formatter discard or
	// sparse-write behavior cannot turn a successful admission into later host
	// ENOSPC for already-budgeted tenant bytes.
	return fullyAllocateComputerDisk(path, bytes)
}

func fullyAllocateComputerDisk(path string, bytes int64) error {
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	if err = unix.Fallocate(int(file.Fd()), 0, 0, bytes); err == nil {
		err = file.Sync()
	}
	return errors.Join(err, file.Close())
}

func findRootTool(name string) (string, error) {
	for _, directory := range []string{"/usr/sbin", "/sbin", "/usr/bin", "/bin"} {
		path := filepath.Join(directory, name)
		info, err := os.Stat(path)
		if err == nil && info.Mode().IsRegular() && info.Mode().Perm()&0o022 == 0 && rootOwnedPath(path) {
			return path, nil
		}
	}
	return "", fmt.Errorf("required root helper tool %s is unavailable", name)
}

func rootOwnedPath(path string) bool {
	for current := path; ; current = filepath.Dir(current) {
		info, err := os.Stat(current)
		if err != nil || info.Mode().Perm()&0o022 != 0 {
			return false
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || stat.Uid != 0 {
			return false
		}
		if current == "/" {
			return true
		}
	}
}

func (linuxComputerDiskSystem) attachAndMount(_ context.Context, imagePath, mountPath string) (loopPath string, returnedErr error) {
	control, err := os.OpenFile("/dev/loop-control", os.O_RDWR, 0)
	if err != nil {
		return "", err
	}
	number, _, errno := syscall.Syscall(syscall.SYS_IOCTL, control.Fd(), uintptr(unix.LOOP_CTL_GET_FREE), 0)
	_ = control.Close()
	if errno != 0 {
		return "", errno
	}
	loopPath = "/dev/loop" + strconv.FormatUint(uint64(number), 10)
	loop, err := os.OpenFile(loopPath, os.O_RDWR, 0)
	if err != nil {
		return "", err
	}
	defer loop.Close()
	image, err := os.OpenFile(imagePath, os.O_RDWR, 0)
	if err != nil {
		return "", err
	}
	defer image.Close()
	configuration := &unix.LoopConfig{Fd: uint32(image.Fd()), Info: unix.LoopInfo64{Flags: unix.LO_FLAGS_AUTOCLEAR}}
	copy(configuration.Info.File_name[:], imagePath)
	if err := unix.IoctlLoopConfigure(int(loop.Fd()), configuration); err != nil {
		return "", err
	}
	defer func() {
		if returnedErr != nil {
			_, _, _ = syscall.Syscall(syscall.SYS_IOCTL, loop.Fd(), uintptr(unix.LOOP_CLR_FD), 0)
		}
	}()
	if err := unix.Mount(loopPath, mountPath, "ext4", uintptr(unix.MS_NODEV|unix.MS_NOSUID), ""); err != nil {
		return "", err
	}
	return loopPath, nil
}

func (system linuxComputerDiskSystem) detach(mountPath, loopPath, expectedImagePath string) error {
	if loopPath != "" {
		backing, present, err := system.loopBackingFile(loopPath)
		if err != nil {
			return err
		}
		if !present || backing != filepath.Clean(expectedImagePath) {
			return nil
		}
	}
	if _, mounted, err := system.mountedSource(mountPath); err != nil {
		return err
	} else if mounted {
		var unmountErr error
		for attempt := 0; attempt < 3; attempt++ {
			unmountErr = unix.Unmount(mountPath, 0)
			if unmountErr == nil || errors.Is(unmountErr, syscall.EINVAL) || errors.Is(unmountErr, syscall.ENOENT) {
				break
			}
			if !errors.Is(unmountErr, syscall.EBUSY) {
				return fmt.Errorf("unmount Computer disk: %w", unmountErr)
			}
			time.Sleep(10 * time.Millisecond)
		}
		if errors.Is(unmountErr, syscall.EBUSY) {
			unmountErr = unix.Unmount(mountPath, unix.MNT_DETACH)
		}
		if unmountErr != nil && !errors.Is(unmountErr, syscall.EINVAL) && !errors.Is(unmountErr, syscall.ENOENT) {
			return fmt.Errorf("unmount Computer disk: %w", unmountErr)
		}
	}
	if loopPath == "" {
		return nil
	}
	// LOOP_FLAGS_AUTOCLEAR owns teardown. Never clear a loop by its recycled
	// number: after the last mount close another tenant may already own it.
	for attempt := 0; attempt < 20; attempt++ {
		backing, present, err := system.loopBackingFile(loopPath)
		if err != nil {
			return err
		}
		if !present || backing != filepath.Clean(expectedImagePath) {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return errors.New("Computer disk loop remained attached after autoclear")
}

func (linuxComputerDiskSystem) mountedSource(target string) (string, bool, error) {
	file, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return "", false, err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		separator := -1
		for index, value := range fields {
			if value == "-" {
				separator = index
				break
			}
		}
		if len(fields) > 5 && separator >= 0 && separator+2 < len(fields) && unescapeMountInfo(fields[4]) == target {
			return unescapeMountInfo(fields[separator+2]), true, nil
		}
	}
	return "", false, scanner.Err()
}

func unescapeMountInfo(value string) string {
	replacer := strings.NewReplacer("\\040", " ", "\\011", "\t", "\\012", "\n", "\\134", "\\")
	return replacer.Replace(value)
}

func (linuxComputerDiskSystem) loopBackingFile(loopPath string) (string, bool, error) {
	value, err := os.ReadFile(filepath.Join("/sys/block", filepath.Base(loopPath), "loop/backing_file"))
	if errors.Is(err, os.ErrNotExist) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	path := strings.TrimSpace(string(value))
	if path == "" {
		return "", false, nil
	}
	if !filepath.IsAbs(path) {
		path = string(filepath.Separator) + path
	}
	return filepath.Clean(path), true, nil
}

func (system linuxComputerDiskSystem) loopsForRoot(root string) (map[string]string, error) {
	entries, err := filepath.Glob("/sys/block/loop*/loop/backing_file")
	if err != nil {
		return nil, err
	}
	result := make(map[string]string)
	for _, entry := range entries {
		loopPath := filepath.Join("/dev", filepath.Base(filepath.Dir(filepath.Dir(entry))))
		backing, present, err := system.loopBackingFile(loopPath)
		if err != nil {
			return nil, err
		}
		if present && (backing == root || strings.HasPrefix(backing, root+string(filepath.Separator))) {
			result[loopPath] = backing
		}
	}
	return result, nil
}

func (engine *ContainerdEngine) inventoryComputerDiskResources(result *ResourceInventory) error {
	diskRoot := filepath.Join(engine.config.RuntimeRoot, "computer-disks")
	entries, err := readDirectoryIfPresent(diskRoot)
	if err != nil {
		return err
	}
	seenDeferred := make(map[string]struct{})
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), "wefty-computer-disk-") || !entry.IsDir() {
			continue
		}
		root := filepath.Join(diskRoot, entry.Name())
		if deferred, present, readErr := engine.inspectOperationalComputerRecoveryDeferral(root, entry.Name()); readErr == nil && present {
			result.ComputerStorageDeferred = append(result.ComputerStorageDeferred,
				recoveryInventoryEntry(entry.Name(), deferred.Operation, deferred.Storage, deferred.Recovery))
			seenDeferred[entry.Name()] = struct{}{}
			continue
		} else if readErr != nil {
			if classifyComputerRecoveryFileFailure(readErr) == computerRecoveryFileOperational {
				result.ComputerStorageDeferred = append(result.ComputerStorageDeferred, ComputerStorageRecoveryInventoryEntry{
					DiskName: entry.Name(), Operation: "deferral_record_unreadable", Reason: recoveryDeferralReason(readErr),
				})
			} else {
				result.ComputerDiskAnomalies = append(result.ComputerDiskAnomalies, entry.Name()+":recovery_deferral_invalid")
			}
			continue
		}
		if _, err := engine.lstatComputerDisk(filepath.Join(root, "quarantine.json")); err == nil {
			receipt, receiptErr := readAndValidateComputerDiskQuarantineReceipt(filepath.Join(root, "quarantine.json"))
			if receiptErr != nil || receipt.DiskName != entry.Name() {
				result.ComputerDiskAnomalies = append(result.ComputerDiskAnomalies, entry.Name()+":quarantine_authority_invalid")
			} else {
				result.ComputerQuarantines = append(result.ComputerQuarantines, entry.Name())
				recovery := recoveryInventoryEntry(entry.Name(), "quarantine", receipt.Storage, computerStorageRecoveryDeferral{
					Attempts: receipt.RecoveryAttempts, Reason: receipt.Reason, FirstDeferredAt: receipt.FirstDeferredAt,
				})
				recovery.DeferredReason = receipt.DeferredReason
				if receipt.PayloadDroppedAt != nil {
					droppedAt := receipt.PayloadDroppedAt.UTC()
					recovery.PayloadDroppedAt = &droppedAt
				}
				result.ComputerStorageQuarantined = append(result.ComputerStorageQuarantined, recovery)
			}
			continue
		} else if !errors.Is(err, os.ErrNotExist) {
			result.ComputerStorageDeferred = append(result.ComputerStorageDeferred, ComputerStorageRecoveryInventoryEntry{
				DiskName: entry.Name(), Operation: "computer_disk_quarantine", Reason: recoveryDeferralReason(err),
			})
			continue
		}
		if recovery, deferred := engine.operationalComputerRecoveryDeferral(entry.Name()); deferred {
			result.ComputerStorageDeferred = append(result.ComputerStorageDeferred, recovery)
			continue
		}
		var imageInfo os.FileInfo
		if info, err := engine.lstatComputerDisk(filepath.Join(root, "disk.ext4")); err == nil {
			if !info.Mode().IsRegular() {
				result.ComputerDiskAnomalies = append(result.ComputerDiskAnomalies, entry.Name()+":image_not_regular")
				continue
			}
			imageInfo = info
			result.ComputerDiskImages = append(result.ComputerDiskImages, entry.Name())
		} else if !errors.Is(err, os.ErrNotExist) {
			result.ComputerStorageDeferred = append(result.ComputerStorageDeferred, ComputerStorageRecoveryInventoryEntry{
				DiskName: entry.Name(), Operation: "computer_disk_image", Reason: recoveryDeferralReason(err),
			})
			continue
		}
		copyRecord, copyPresent, copyErr := readComputerStorageCopyManifest(filepath.Join(root, "storage-copy.json"))
		if copyErr == nil && copyPresent && validComputerStorageCopyRecoveryForInventory(entry.Name(), copyRecord) {
			result.ComputerStorageDeferred = append(result.ComputerStorageDeferred,
				recoveryInventoryEntry(entry.Name(), "computer_storage_copy", copyRecord.Request.Destination, copyRecord.Recovery))
		}
		manifest, present, err := readComputerDiskManifest(filepath.Join(root, "attachment.json"))
		if err != nil {
			result.ComputerDiskAnomalies = append(result.ComputerDiskAnomalies, entry.Name()+":manifest_invalid")
			continue
		}
		if present {
			result.ComputerDiskManifests = append(result.ComputerDiskManifests, entry.Name())
			if manifest.Attached != nil {
				result.ComputerAttachments = append(result.ComputerAttachments, entry.Name())
			}
			if imageInfo == nil {
				result.ComputerDiskAnomalies = append(result.ComputerDiskAnomalies, entry.Name()+":image_missing")
				continue
			}
			if err := verifyComputerDiskAllocation(filepath.Join(root, "disk.ext4"), manifest.Storage.DiskBytes); err != nil {
				if !validComputerStorageGrowIntentForInventory(root, entry.Name(), manifest) {
					result.ComputerDiskAnomalies = append(result.ComputerDiskAnomalies, entry.Name()+":allocation_mismatch")
					continue
				}
			}
			result.ComputerDiskAllocations = append(result.ComputerDiskAllocations, entry.Name())
			// A Computer disk's current fully allocated image size is its
			// writable filesystem budget; no host free space can expand it.
			result.ComputerDiskQuotas = append(result.ComputerDiskQuotas, entry.Name())
			if growRecord, growPresent, growErr := readComputerStorageGrowIntent(root); growErr == nil && growPresent {
				result.ComputerStorageDeferred = append(result.ComputerStorageDeferred,
					recoveryInventoryEntry(entry.Name(), "computer_storage_grow", growRecord.Request.Storage, growRecord.Recovery))
			}
		}
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), ".") || !strings.HasSuffix(entry.Name(), computerOperationalDeferralFaultSuffix) {
			continue
		}
		name := strings.TrimSuffix(strings.TrimPrefix(entry.Name(), "."), computerOperationalDeferralFaultSuffix)
		if !validComputerDiskDirectoryName(name) {
			continue
		}
		if _, present := seenDeferred[name]; present {
			continue
		}
		record, present, readErr := readOperationalComputerRecoveryDeferralAt(filepath.Join(diskRoot, entry.Name()), name)
		if readErr != nil || !present {
			result.ComputerDiskAnomalies = append(result.ComputerDiskAnomalies, name+":recovery_deferral_invalid")
			continue
		}
		result.ComputerStorageDeferred = append(result.ComputerStorageDeferred,
			recoveryInventoryEntry(name, record.Operation, record.Storage, record.Recovery))
	}
	mountRoot := filepath.Join(engine.config.RuntimeRoot, "computer-mounts")
	mountEntries, err := readDirectoryIfPresent(mountRoot)
	if err != nil {
		return err
	}
	for _, entry := range mountEntries {
		if !strings.HasPrefix(entry.Name(), "wefty-computer-disk-") || !entry.IsDir() {
			continue
		}
		if _, mounted, err := engine.computerDiskSystem().mountedSource(filepath.Join(mountRoot, entry.Name())); err != nil {
			return err
		} else if mounted {
			result.ComputerDiskMounts = append(result.ComputerDiskMounts, entry.Name())
		}
	}
	loops, err := engine.computerDiskSystem().loopsForRoot(diskRoot)
	if err != nil {
		return err
	}
	seenLoopOwners := make(map[string]struct{})
	for _, backing := range loops {
		relative, relativeErr := filepath.Rel(diskRoot, backing)
		if relativeErr != nil || relative == "." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			continue
		}
		name := strings.Split(relative, string(filepath.Separator))[0]
		if strings.HasPrefix(name, "wefty-computer-disk-") {
			seenLoopOwners[name] = struct{}{}
		}
	}
	for name := range seenLoopOwners {
		result.ComputerDiskLoops = append(result.ComputerDiskLoops, name)
	}
	resetEntries, err := readDirectoryIfPresent(filepath.Join(engine.config.RuntimeRoot, "computer-storage-resets"))
	if err != nil {
		return err
	}
	for _, entry := range resetEntries {
		if strings.HasPrefix(entry.Name(), "wefty-computer-disk-") && strings.HasSuffix(entry.Name(), ".json") {
			result.ComputerResetManifests = append(result.ComputerResetManifests, strings.TrimSuffix(entry.Name(), ".json"))
		}
	}
	quarantineRoot := filepath.Join(engine.config.RuntimeRoot, "computer-disk-quarantine")
	quarantineEntries, err := engine.readComputerDiskDirectory(quarantineRoot)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		result.ComputerStorageDeferred = append(result.ComputerStorageDeferred, ComputerStorageRecoveryInventoryEntry{
			DiskName: filepath.Base(quarantineRoot), Operation: "quarantine_root_inventory", Reason: recoveryDeferralReason(err),
		})
		return nil
	}
	for _, entry := range quarantineEntries {
		entryName := entry.Name()
		if !strings.HasPrefix(entryName, "wefty-computer-disk-") {
			continue
		}
		path := filepath.Join(engine.config.RuntimeRoot, "computer-disk-quarantine", entryName)
		valid := false
		name := ""
		if entry.IsDir() {
			if resetAt := strings.LastIndex(entryName, "-reset-"); resetAt > len("wefty-computer-disk-") {
				if _, parseErr := strconv.ParseUint(entryName[resetAt+len("-reset-"):], 10, 64); parseErr == nil {
					name = entryName[:resetAt]
					valid = validComputerDiskDirectoryName(name)
					if valid {
						result.ComputerStorageQuarantined = append(result.ComputerStorageQuarantined,
							ComputerStorageRecoveryInventoryEntry{DiskName: name, Operation: "legacy_reset", Reason: "legacy_reset_quarantine"})
					}
				}
			}
		}
		if entry.IsDir() && !valid {
			receipt, readErr := readAndValidateComputerDiskQuarantineReceipt(filepath.Join(path, "quarantine.json"))
			if readErr == nil {
				valid = entryName == receipt.DiskName+"-anomaly-"+receipt.ReceiptID
				name = receipt.DiskName
				if valid {
					recovery := recoveryInventoryEntry(name, "quarantine", receipt.Storage, computerStorageRecoveryDeferral{Attempts: receipt.RecoveryAttempts, Reason: receipt.Reason, FirstDeferredAt: receipt.FirstDeferredAt})
					recovery.DeferredReason = receipt.DeferredReason
					if receipt.PayloadDroppedAt != nil {
						droppedAt := receipt.PayloadDroppedAt.UTC()
						recovery.PayloadDroppedAt = &droppedAt
					}
					result.ComputerStorageQuarantined = append(result.ComputerStorageQuarantined, recovery)
				}
			}
		} else if strings.HasSuffix(entryName, ".json") {
			var receipt ManagedVolumeQuarantineReceipt
			payload, readErr := os.ReadFile(path)
			if readErr == nil && json.Unmarshal(payload, &receipt) == nil && receipt.Kind == "managed_volume_cleanup_quarantined" &&
				receipt.ReceiptID != "" && receipt.VolumeKind == ManagedVolumeComputerDisk && receipt.Removal.RemovalGeneration > 0 {
				expected, identityErr := deterministicComputerDiskName(receipt.ComputerStorage)
				valid = identityErr == nil && entryName == fmt.Sprintf("%s-cleanup-%d.json", expected, receipt.Removal.RemovalGeneration)
				name = expected
				if valid {
					result.ComputerStorageQuarantined = append(result.ComputerStorageQuarantined,
						recoveryInventoryEntry(name, "authorized_removal", receipt.ComputerStorage, computerStorageRecoveryDeferral{Attempts: receipt.Attempts, Reason: string(receipt.FailureReason)}))
				}
			}
		}
		if !valid {
			result.ComputerDiskAnomalies = append(result.ComputerDiskAnomalies, entryName+":quarantine_authority_invalid")
			continue
		}
		result.ComputerQuarantines = append(result.ComputerQuarantines, name)
	}
	return nil
}

func (engine *ContainerdEngine) sweepComputerDisks(ctx context.Context, sweepEpoch string) error {
	return engine.sweepComputerDisksWithRecoveryAttempt(ctx, sweepEpoch, true)
}

func (engine *ContainerdEngine) computerDiskDeferredDuringSweep(name string) bool {
	for _, evidence := range engine.computerDiskSweepEvidence {
		if evidence.ID == name && evidence.Action == SweepActionResumeDeferred {
			return true
		}
	}
	return false
}

func (engine *ContainerdEngine) sweepComputerDisksWithRecoveryAttempt(ctx context.Context, sweepEpoch string, countRecoveryAttempt bool) error {
	engine.computerDiskSweepEvidence = nil
	engine.resetOperationalComputerRecoveryDeferrals()
	if err := engine.expireComputerDiskQuarantinePayloads(ctx); err != nil {
		return err
	}
	if sweepEpoch == "" {
		var err error
		sweepEpoch, err = randomCapability()
		if err != nil {
			return errors.New("generate Computer disk sweep epoch")
		}
	}
	engine.mu.Lock()
	attachments := make([]*computerDiskAttachment, 0, len(engine.attempts))
	for _, attempt := range engine.attempts {
		if attempt.computerDisk != nil {
			attachments = append(attachments, attempt.computerDisk)
		}
	}
	engine.mu.Unlock()
	for _, attachment := range attachments {
		if err := engine.detachComputerDisk(attachment, computerDiskSweepReceipt, sweepEpoch); err != nil {
			return err
		}
	}

	diskRoot := filepath.Join(engine.config.RuntimeRoot, "computer-disks")
	mountRoot := filepath.Join(engine.config.RuntimeRoot, "computer-mounts")
	mountEntries, err := readDirectoryIfPresent(mountRoot)
	if err != nil {
		return err
	}
	for _, entry := range mountEntries {
		if !strings.HasPrefix(entry.Name(), "wefty-computer-disk-") || !entry.IsDir() {
			continue
		}
		mountPath := filepath.Join(mountRoot, entry.Name())
		source, mounted, err := engine.computerDiskSystem().mountedSource(mountPath)
		if err != nil {
			root := filepath.Join(diskRoot, entry.Name())
			engine.computerDiskSweepEvidence = append(engine.computerDiskSweepEvidence,
				engine.resolveOperationalComputerRecoveryFailure(root, entry.Name(), "computer_disk_mount_inspection", ComputerStorageReference{}, err, countRecoveryAttempt))
			continue
		}
		if mounted {
			expectedImage := filepath.Join(diskRoot, entry.Name(), "disk.ext4")
			backing, ours, backingErr := engine.computerDiskSystem().loopBackingFile(source)
			if backingErr != nil {
				root := filepath.Join(diskRoot, entry.Name())
				engine.computerDiskSweepEvidence = append(engine.computerDiskSweepEvidence,
					engine.resolveOperationalComputerRecoveryFailure(root, entry.Name(), "computer_disk_loop_inspection", ComputerStorageReference{}, backingErr, countRecoveryAttempt))
				continue
			}
			if ours && backing == expectedImage {
				if err := engine.computerDiskSystem().detach(mountPath, source, expectedImage); err != nil {
					root := filepath.Join(diskRoot, entry.Name())
					engine.computerDiskSweepEvidence = append(engine.computerDiskSweepEvidence,
						engine.resolveOperationalComputerRecoveryFailure(root, entry.Name(), "computer_disk_mount_detach", ComputerStorageReference{}, err, countRecoveryAttempt))
					continue
				}
			}
		}
	}
	copyMountRoot := filepath.Join(engine.config.RuntimeRoot, "computer-copy-mounts")
	copyMountEntries, err := readDirectoryIfPresent(copyMountRoot)
	if err != nil {
		return err
	}
	for _, entry := range copyMountEntries {
		if !strings.HasPrefix(entry.Name(), "wefty-computer-disk-") || !entry.IsDir() {
			continue
		}
		mountPath := filepath.Join(copyMountRoot, entry.Name())
		source, mounted, err := engine.computerDiskSystem().mountedSource(mountPath)
		if err != nil {
			root := filepath.Join(diskRoot, entry.Name())
			engine.computerDiskSweepEvidence = append(engine.computerDiskSweepEvidence,
				engine.resolveOperationalComputerRecoveryFailure(root, entry.Name(), "computer_storage_copy_mount_inspection", ComputerStorageReference{}, err, countRecoveryAttempt))
			continue
		}
		if !mounted {
			continue
		}
		backing, ours, err := engine.computerDiskSystem().loopBackingFile(source)
		if err != nil {
			root := filepath.Join(diskRoot, entry.Name())
			engine.computerDiskSweepEvidence = append(engine.computerDiskSweepEvidence,
				engine.resolveOperationalComputerRecoveryFailure(root, entry.Name(), "computer_storage_copy_loop_inspection", ComputerStorageReference{}, err, countRecoveryAttempt))
			continue
		}
		expectedRoot := filepath.Join(diskRoot, entry.Name())
		relative, relErr := filepath.Rel(expectedRoot, backing)
		if !ours || relErr != nil || relative == "." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			engine.computerDiskSweepEvidence = append(engine.computerDiskSweepEvidence,
				engine.resolveOperationalComputerRecoveryFailure(expectedRoot, entry.Name(), "computer_storage_copy_mount_identity", ComputerStorageReference{}, errors.Join(relErr, errors.New("Computer Storage copy mount has an unexpected backing image")), countRecoveryAttempt))
			continue
		}
		if err := engine.computerDiskSystem().detach(mountPath, source, backing); err != nil {
			engine.computerDiskSweepEvidence = append(engine.computerDiskSweepEvidence,
				engine.resolveOperationalComputerRecoveryFailure(expectedRoot, entry.Name(), "computer_storage_copy_mount_detach", ComputerStorageReference{}, err, countRecoveryAttempt))
			continue
		}
	}
	loops, err := engine.computerDiskSystem().loopsForRoot(diskRoot)
	if err != nil {
		return err
	}
	for loop, backing := range loops {
		if err := engine.computerDiskSystem().detach("", loop, backing); err != nil {
			return err
		}
	}

	diskEntries, err := readDirectoryIfPresent(diskRoot)
	if err != nil {
		return err
	}
diskLoop:
	for _, entry := range diskEntries {
		if !strings.HasPrefix(entry.Name(), "wefty-computer-disk-") || !entry.IsDir() {
			continue
		}
		if engine.computerDiskDeferredDuringSweep(entry.Name()) {
			continue
		}
		root := filepath.Join(diskRoot, entry.Name())
		if _, err := engine.lstatComputerDisk(root); err != nil {
			engine.computerDiskSweepEvidence = append(engine.computerDiskSweepEvidence,
				engine.resolveOperationalComputerRecoveryFault(root, entry.Name(), "computer_disk_directory", ComputerStorageReference{}, err, countRecoveryAttempt, recoveryDeferralReason(err)))
			continue
		}
		for _, pattern := range []string{".disk.ext4.tmp-*", ".storage-grow.json.tmp-*", ".quarantine.json.tmp-*"} {
			staging, err := filepath.Glob(filepath.Join(root, pattern))
			if err != nil {
				engine.computerDiskSweepEvidence = append(engine.computerDiskSweepEvidence,
					engine.resolveOperationalComputerRecoveryFailure(root, entry.Name(), "computer_disk_staging_cleanup", ComputerStorageReference{}, err, countRecoveryAttempt))
				continue diskLoop
			}
			for _, path := range staging {
				if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
					engine.computerDiskSweepEvidence = append(engine.computerDiskSweepEvidence,
						engine.resolveOperationalComputerRecoveryFailure(root, entry.Name(), "computer_disk_staging_cleanup", ComputerStorageReference{}, err, countRecoveryAttempt))
					continue diskLoop
				}
			}
		}
		recoveryLock, err := openComputerDiskLock(root)
		if err != nil {
			if errors.Is(err, errComputerStorageAttachmentOwned) {
				return errors.New("Computer attachment lock remained owned after sweep")
			}
			engine.computerDiskSweepEvidence = append(engine.computerDiskSweepEvidence,
				engine.resolveOperationalComputerRecoveryFailure(root, entry.Name(), "computer_disk_lock_open", ComputerStorageReference{}, err, countRecoveryAttempt))
			continue
		}
		quarantineResumed, quarantineResumeErr := engine.resumeComputerDiskQuarantine(root, entry.Name())
		if quarantineResumeErr != nil {
			if quarantineErr := engine.quarantineComputerDiskAuthorityFailure(root, entry.Name(), "quarantine_authority_invalid"); quarantineErr != nil {
				closeComputerDiskLock(recoveryLock)
				engine.computerDiskSweepEvidence = append(engine.computerDiskSweepEvidence,
					engine.resolveOperationalComputerRecoveryFailure(root, entry.Name(), "quarantine_move_failed", ComputerStorageReference{}, errors.Join(quarantineResumeErr, quarantineErr), countRecoveryAttempt))
				continue
			}
			engine.computerDiskSweepEvidence = append(engine.computerDiskSweepEvidence, SweepEvidence{
				Class: RemovalResourceComputerQuarantine, ID: entry.Name(), Action: SweepActionQuarantined, Method: "quarantine_authority_invalid",
			})
			closeComputerDiskLock(recoveryLock)
			continue
		}
		if quarantineResumed {
			engine.computerDiskSweepEvidence = append(engine.computerDiskSweepEvidence, SweepEvidence{
				Class: RemovalResourceComputerQuarantine, ID: entry.Name(), Action: SweepActionQuarantined, Method: "quarantine_recovered",
			})
			closeComputerDiskLock(recoveryLock)
			continue
		}
		manifest, present, err := readComputerDiskManifest(filepath.Join(root, "attachment.json"))
		if err != nil {
			if classifyComputerRecoveryFileFailure(err) == computerRecoveryFileOperational {
				engine.computerDiskSweepEvidence = append(engine.computerDiskSweepEvidence,
					engine.resolveOperationalComputerRecoveryFailure(root, entry.Name(), "computer_disk_manifest", ComputerStorageReference{}, err, countRecoveryAttempt))
				closeComputerDiskLock(recoveryLock)
				continue
			}
			reason := computerRecoveryStructuralReason(err, "manifest_invalid")
			if quarantineErr := engine.quarantineComputerDiskAuthorityFailure(root, entry.Name(), reason); quarantineErr != nil {
				closeComputerDiskLock(recoveryLock)
				engine.computerDiskSweepEvidence = append(engine.computerDiskSweepEvidence,
					engine.resolveOperationalComputerRecoveryFailure(root, entry.Name(), "quarantine_move_failed", ComputerStorageReference{}, errors.Join(err, quarantineErr), countRecoveryAttempt))
				continue
			}
			engine.computerDiskSweepEvidence = append(engine.computerDiskSweepEvidence, SweepEvidence{
				Class: RemovalResourceComputerQuarantine, ID: entry.Name(), Action: SweepActionQuarantined, Method: reason,
			})
			closeComputerDiskLock(recoveryLock)
			continue
		}
		if present {
			expectedName, identityErr := deterministicComputerDiskName(manifest.Storage)
			if identityErr != nil || expectedName != entry.Name() || manifest.DiskImage != "disk.ext4" || manifest.MountDirectory != entry.Name() {
				if quarantineErr := engine.quarantineComputerDiskIdentityMismatch(root, entry.Name(), manifest.Storage); quarantineErr != nil {
					closeComputerDiskLock(recoveryLock)
					engine.computerDiskSweepEvidence = append(engine.computerDiskSweepEvidence,
						engine.resolveOperationalComputerRecoveryFailure(root, entry.Name(), "quarantine_move_failed", manifest.Storage, errors.Join(identityErr, quarantineErr), countRecoveryAttempt))
					continue
				}
				engine.computerDiskSweepEvidence = append(engine.computerDiskSweepEvidence, SweepEvidence{
					Class: RemovalResourceComputerQuarantine, ID: entry.Name(), Action: SweepActionQuarantined, Method: "identity_mismatch",
				})
				closeComputerDiskLock(recoveryLock)
				continue
			}
			if err := engine.clearOperationalComputerRecoveryDeferral(root, entry.Name(), "computer_disk_manifest"); err != nil {
				engine.computerDiskSweepEvidence = append(engine.computerDiskSweepEvidence,
					engine.resolveOperationalDeferralRecordFailure(root, entry.Name(), "computer_disk_manifest", manifest.Storage, err, countRecoveryAttempt))
				closeComputerDiskLock(recoveryLock)
				continue
			}
		}
		copyRecovery, resumeErr := engine.resumeComputerStorageCopy(ctx, root, entry.Name())
		if resumeErr != nil {
			fallback := ComputerStorageReference{}
			if present {
				fallback = manifest.Storage
			}
			evidence, resolveErr := engine.resolveComputerStorageRecoveryFailure(root, entry.Name(), "computer_storage_copy", fallback, resumeErr, countRecoveryAttempt)
			if resolveErr != nil {
				closeComputerDiskLock(recoveryLock)
				engine.computerDiskSweepEvidence = append(engine.computerDiskSweepEvidence,
					engine.resolveOperationalComputerRecoveryFailure(root, entry.Name(), "quarantine_move_failed", fallback, resolveErr, countRecoveryAttempt))
				continue
			}
			engine.computerDiskSweepEvidence = append(engine.computerDiskSweepEvidence, evidence)
			closeComputerDiskLock(recoveryLock)
			continue
		}
		if err := engine.clearOperationalComputerRecoveryDeferral(root, entry.Name(), "computer_storage_copy"); err != nil {
			engine.computerDiskSweepEvidence = append(engine.computerDiskSweepEvidence,
				engine.resolveOperationalDeferralRecordFailure(root, entry.Name(), "computer_storage_copy", manifest.Storage, err, countRecoveryAttempt))
			closeComputerDiskLock(recoveryLock)
			continue
		}
		if copyRecovery != "" {
			action := SweepActionResumed
			if copyRecovery == "computer_storage_copy_rolled_back" {
				action = SweepActionRolledBack
			}
			engine.computerDiskSweepEvidence = append(engine.computerDiskSweepEvidence, SweepEvidence{
				Class: RemovalResourceComputerDiskManifest, ID: entry.Name(), Action: action, Method: copyRecovery,
			})
			manifest, present, err = readComputerDiskManifest(filepath.Join(root, "attachment.json"))
			if err != nil {
				engine.computerDiskSweepEvidence = append(engine.computerDiskSweepEvidence,
					engine.resolveOperationalComputerRecoveryFailure(root, entry.Name(), "computer_disk_manifest", ComputerStorageReference{}, err, countRecoveryAttempt))
				closeComputerDiskLock(recoveryLock)
				continue diskLoop
			}
		}
		if !present {
			if copyRecovery == "computer_storage_copy_rolled_back" {
				if err := engine.clearOperationalComputerRecoveryDeferral(root, entry.Name(), "computer_disk_manifest"); err != nil {
					engine.computerDiskSweepEvidence = append(engine.computerDiskSweepEvidence,
						engine.resolveOperationalDeferralRecordFailure(root, entry.Name(), "computer_disk_manifest", ComputerStorageReference{}, err, countRecoveryAttempt))
				}
				closeComputerDiskLock(recoveryLock)
				continue
			}
			deferred, deferredPresent, deferredReadErr := engine.inspectOperationalComputerRecoveryDeferral(root, entry.Name())
			method := "manifest_missing"
			if deferredReadErr != nil {
				deferred.Recovery.Reason = "deferral_record_unreadable"
				deferredPresent = true
				method += ":deferral_record_unreadable"
			}
			quarantineErr := error(nil)
			if deferredPresent {
				quarantineErr = engine.quarantineComputerDiskAuthorityFailureWithDeferral(root, entry.Name(), "manifest_missing", deferred.Recovery)
			} else {
				quarantineErr = engine.quarantineComputerDiskAuthorityFailure(root, entry.Name(), "manifest_missing")
			}
			if quarantineErr != nil {
				closeComputerDiskLock(recoveryLock)
				engine.computerDiskSweepEvidence = append(engine.computerDiskSweepEvidence,
					engine.resolveOperationalComputerRecoveryFailure(root, entry.Name(), "quarantine_move_failed", ComputerStorageReference{}, errors.Join(deferredReadErr, quarantineErr), countRecoveryAttempt))
				continue
			}
			engine.computerDiskSweepEvidence = append(engine.computerDiskSweepEvidence, SweepEvidence{
				Class: RemovalResourceComputerQuarantine, ID: entry.Name(), Action: SweepActionQuarantined, Method: method,
			})
			closeComputerDiskLock(recoveryLock)
			continue
		}
		resumedGrow, growErr := engine.resumeComputerStorageGrow(ctx, root, entry.Name(), &manifest)
		if growErr != nil {
			evidence, resolveErr := engine.resolveComputerStorageRecoveryFailure(root, entry.Name(), "computer_storage_grow", manifest.Storage, growErr, countRecoveryAttempt)
			if resolveErr != nil {
				closeComputerDiskLock(recoveryLock)
				engine.computerDiskSweepEvidence = append(engine.computerDiskSweepEvidence,
					engine.resolveOperationalComputerRecoveryFailure(root, entry.Name(), "quarantine_move_failed", manifest.Storage, resolveErr, countRecoveryAttempt))
				continue
			}
			engine.computerDiskSweepEvidence = append(engine.computerDiskSweepEvidence, evidence)
			closeComputerDiskLock(recoveryLock)
			continue
		}
		if resumedGrow {
			engine.computerDiskSweepEvidence = append(engine.computerDiskSweepEvidence, SweepEvidence{
				Class: RemovalResourceComputerDiskManifest, ID: entry.Name(), Action: SweepActionResumed, Method: "computer_storage_grow",
			})
		}
		if err := engine.clearOperationalComputerRecoveryDeferral(root, entry.Name(), "computer_storage_grow"); err != nil {
			engine.computerDiskSweepEvidence = append(engine.computerDiskSweepEvidence,
				engine.resolveOperationalDeferralRecordFailure(root, entry.Name(), "computer_storage_grow", manifest.Storage, err, countRecoveryAttempt))
			closeComputerDiskLock(recoveryLock)
			continue
		}
		// A reset successor has no tenant bytes before its preparation receipt is
		// durably published. If the helper died anywhere in that preparation, drop
		// the exact unverified generation so the standing L1 reset authority can
		// recreate it; retaining a half-published image would instead make startup
		// verification fail closed forever on allocation_mismatch/image_missing.
		if unverifiedComputerStorageResetPreparation(manifest) {
			if manifest.Storage.StorageGeneration < 2 {
				if quarantineErr := engine.quarantineComputerDiskIdentityMismatch(root, entry.Name(), manifest.Storage); quarantineErr != nil {
					closeComputerDiskLock(recoveryLock)
					engine.computerDiskSweepEvidence = append(engine.computerDiskSweepEvidence,
						engine.resolveOperationalComputerRecoveryFailure(root, entry.Name(), "quarantine_move_failed", manifest.Storage, quarantineErr, countRecoveryAttempt))
					continue
				}
				engine.computerDiskSweepEvidence = append(engine.computerDiskSweepEvidence, SweepEvidence{
					Class: RemovalResourceComputerQuarantine, ID: entry.Name(), Action: SweepActionQuarantined, Method: "identity_mismatch",
				})
				closeComputerDiskLock(recoveryLock)
				continue
			}
			closeComputerDiskLock(recoveryLock)
			lock, lockErr := os.OpenFile(filepath.Join(root, "attachment.lock"), os.O_CREATE|os.O_RDWR, 0o600)
			if lockErr != nil {
				engine.computerDiskSweepEvidence = append(engine.computerDiskSweepEvidence,
					engine.resolveOperationalComputerRecoveryFailure(root, entry.Name(), "computer_storage_reset_lock_open", manifest.Storage, lockErr, countRecoveryAttempt))
				continue
			}
			if lockErr = unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); lockErr != nil {
				_ = lock.Close()
				return errors.New("unverified Computer Storage reset preparation lock remained owned after sweep")
			}
			removeErr := os.RemoveAll(root)
			_ = unix.Flock(int(lock.Fd()), unix.LOCK_UN)
			_ = lock.Close()
			if removeErr != nil {
				engine.computerDiskSweepEvidence = append(engine.computerDiskSweepEvidence,
					engine.resolveOperationalComputerRecoveryFailure(root, entry.Name(), "computer_storage_reset_cleanup", manifest.Storage, removeErr, countRecoveryAttempt))
				continue
			}
			if err := removeOperationalComputerRecoveryDeferralFault(root, entry.Name()); err != nil {
				engine.computerDiskSweepEvidence = append(engine.computerDiskSweepEvidence,
					engine.resolveOperationalComputerRecoveryFault(root, entry.Name(), "computer_storage_reset_sync", manifest.Storage, err, countRecoveryAttempt, recoveryDeferralReason(err)))
				continue
			}
			if err := syncDirectory(diskRoot); err != nil {
				engine.computerDiskSweepEvidence = append(engine.computerDiskSweepEvidence,
					engine.resolveOperationalComputerRecoveryFault(root, entry.Name(), "computer_storage_reset_sync", manifest.Storage, err, countRecoveryAttempt, recoveryDeferralReason(err)))
				continue
			}
			continue
		}
		if err := verifyComputerDiskAllocation(filepath.Join(root, "disk.ext4"), manifest.Storage.DiskBytes); err != nil {
			if classifyComputerRecoveryFileFailure(err) == computerRecoveryFileOperational {
				engine.computerDiskSweepEvidence = append(engine.computerDiskSweepEvidence,
					engine.resolveOperationalComputerRecoveryFailure(root, entry.Name(), "computer_disk_allocation", manifest.Storage, err, countRecoveryAttempt))
				closeComputerDiskLock(recoveryLock)
				continue
			}
			expectedName, identityErr := deterministicComputerDiskName(manifest.Storage)
			reason := "allocation_mismatch"
			var quarantineErr error
			if identityErr != nil || expectedName != entry.Name() {
				reason = "identity_mismatch"
				quarantineErr = engine.quarantineComputerDiskAuthorityFailure(root, entry.Name(), reason)
			} else {
				quarantineErr = engine.quarantineComputerDiskAnomaly(root, entry.Name(), manifest.Storage, reason)
			}
			if quarantineErr != nil {
				closeComputerDiskLock(recoveryLock)
				engine.computerDiskSweepEvidence = append(engine.computerDiskSweepEvidence,
					engine.resolveOperationalComputerRecoveryFailure(root, entry.Name(), "quarantine_move_failed", manifest.Storage, errors.Join(err, quarantineErr), countRecoveryAttempt))
				continue
			}
			engine.computerDiskSweepEvidence = append(engine.computerDiskSweepEvidence, SweepEvidence{
				Class: RemovalResourceComputerQuarantine, ID: entry.Name(), Action: SweepActionQuarantined, Method: reason,
			})
			closeComputerDiskLock(recoveryLock)
			continue
		}
		if err := engine.clearOperationalComputerRecoveryDeferral(root, entry.Name(), "computer_disk_allocation"); err != nil {
			engine.computerDiskSweepEvidence = append(engine.computerDiskSweepEvidence,
				engine.resolveOperationalDeferralRecordFailure(root, entry.Name(), "computer_disk_allocation", manifest.Storage, err, countRecoveryAttempt))
			closeComputerDiskLock(recoveryLock)
			continue
		}
		closeComputerDiskLock(recoveryLock)
		refreshDetachedReap := manifest.Attached == nil && manifest.Pending == nil &&
			manifest.PreviousDetachment != nil && manifest.PreviousDetachment.Kind == computerDiskReapReceipt
		if manifest.Attached == nil && manifest.Pending == nil && !refreshDetachedReap {
			if err := clearAllOperationalComputerRecoveryDeferrals(root, entry.Name()); err != nil {
				engine.computerDiskSweepEvidence = append(engine.computerDiskSweepEvidence,
					engine.resolveOperationalDeferralRecordFailure(root, entry.Name(), "computer_disk_sweep_complete", manifest.Storage, err, countRecoveryAttempt))
			}
			continue
		}
		lock, err := os.OpenFile(filepath.Join(root, "attachment.lock"), os.O_CREATE|os.O_RDWR, 0o600)
		if err != nil {
			engine.computerDiskSweepEvidence = append(engine.computerDiskSweepEvidence,
				engine.resolveOperationalComputerRecoveryFailure(root, entry.Name(), "computer_disk_lock_open", manifest.Storage, err, countRecoveryAttempt))
			continue
		}
		if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
			_ = lock.Close()
			return errors.New("Computer attachment lock remained owned after sweep")
		}
		mountPath := filepath.Join(mountRoot, manifest.MountDirectory)
		if _, mounted, err := engine.computerDiskSystem().mountedSource(mountPath); err != nil {
			_ = unix.Flock(int(lock.Fd()), unix.LOCK_UN)
			_ = lock.Close()
			engine.computerDiskSweepEvidence = append(engine.computerDiskSweepEvidence,
				engine.resolveOperationalComputerRecoveryFailure(root, entry.Name(), "computer_disk_mount_inspection", manifest.Storage, err, countRecoveryAttempt))
			continue
		} else if mounted {
			_ = lock.Close()
			return errors.New("Computer disk mount remained after sweep")
		}
		if manifest.LoopDevice != "" {
			backing, present, err := engine.computerDiskSystem().loopBackingFile(manifest.LoopDevice)
			if err != nil {
				_ = unix.Flock(int(lock.Fd()), unix.LOCK_UN)
				_ = lock.Close()
				engine.computerDiskSweepEvidence = append(engine.computerDiskSweepEvidence,
					engine.resolveOperationalComputerRecoveryFailure(root, entry.Name(), "computer_disk_loop_inspection", manifest.Storage, err, countRecoveryAttempt))
				continue
			} else if present && backing == filepath.Join(root, "disk.ext4") {
				_ = lock.Close()
				return errors.New("Computer disk loop remained after sweep")
			}
		}
		rootLoops, err := engine.computerDiskSystem().loopsForRoot(root)
		if err != nil {
			_ = unix.Flock(int(lock.Fd()), unix.LOCK_UN)
			_ = lock.Close()
			engine.computerDiskSweepEvidence = append(engine.computerDiskSweepEvidence,
				engine.resolveOperationalComputerRecoveryFailure(root, entry.Name(), "computer_disk_loop_inventory", manifest.Storage, err, countRecoveryAttempt))
			continue
		}
		if len(rootLoops) != 0 {
			_ = lock.Close()
			return errors.New("Computer disk loop remained after sweep")
		}
		priorAuthority := manifest.Attached
		if priorAuthority == nil {
			priorAuthority = manifest.Pending
		}
		if priorAuthority == nil {
			priorEvidence := manifest.PreviousDetachment
			validEvidence := priorEvidence != nil && validComputerDiskDetachmentEvidence(priorEvidence, manifest.Storage, computerDiskDetachmentAuthority{
				NodeID: priorEvidence.NodeID, BootSessionID: priorEvidence.BootSessionID,
			})
			if !validEvidence {
				_ = unix.Flock(int(lock.Fd()), unix.LOCK_UN)
				_ = lock.Close()
				if quarantineErr := engine.quarantineComputerDiskAnomaly(root, entry.Name(), manifest.Storage, "detachment_evidence_invalid"); quarantineErr != nil {
					engine.computerDiskSweepEvidence = append(engine.computerDiskSweepEvidence,
						engine.resolveOperationalComputerRecoveryFailure(root, entry.Name(), "quarantine_move_failed", manifest.Storage, quarantineErr, countRecoveryAttempt))
					continue
				}
				engine.computerDiskSweepEvidence = append(engine.computerDiskSweepEvidence, SweepEvidence{
					Class: RemovalResourceComputerQuarantine, ID: entry.Name(), Action: SweepActionQuarantined, Method: "detachment_evidence_invalid",
				})
				continue
			}
			priorAuthority = &AttemptAuthority{
				NodeID: priorEvidence.NodeID, JobID: priorEvidence.JobID, AttemptID: priorEvidence.AttemptID,
				FencingToken: priorEvidence.FencingToken, BootSessionID: priorEvidence.BootSessionID,
			}
		}
		evidence, err := newComputerDiskEvidence(computerDiskSweepReceipt, sweepEpoch, manifest.Storage, *priorAuthority)
		if err == nil {
			manifest.Attached = nil
			manifest.Pending = nil
			manifest.LoopDevice = ""
			manifest.PreviousDetachment = &evidence
			err = writeComputerDiskManifest(root, manifest)
		}
		_ = unix.Flock(int(lock.Fd()), unix.LOCK_UN)
		_ = lock.Close()
		if err != nil {
			engine.computerDiskSweepEvidence = append(engine.computerDiskSweepEvidence,
				engine.resolveOperationalComputerRecoveryFailure(root, entry.Name(), "computer_disk_receipt_write", manifest.Storage, err, countRecoveryAttempt))
			continue
		}
		if err := clearAllOperationalComputerRecoveryDeferrals(root, entry.Name()); err != nil {
			engine.computerDiskSweepEvidence = append(engine.computerDiskSweepEvidence,
				engine.resolveOperationalDeferralRecordFailure(root, entry.Name(), "computer_disk_sweep_complete", manifest.Storage, err, countRecoveryAttempt))
		}
	}
	return nil
}

func recordErrStorage(err *computerDiskRecoveryRecordError) ComputerStorageReference {
	if err == nil {
		return ComputerStorageReference{}
	}
	return err.Storage
}
