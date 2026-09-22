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
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

type computerStorageCopyPhase string

const (
	computerStorageCopyReserved        computerStorageCopyPhase = "reserved"
	computerStorageCopyAllocated       computerStorageCopyPhase = "allocated"
	computerStorageCopyCopied          computerStorageCopyPhase = "copied"
	computerStorageCopySourceVerified  computerStorageCopyPhase = "source_verified"
	computerStorageCopyMountedRekey    computerStorageCopyPhase = "mounted_rekey"
	computerStorageCopyIdentityRekeyed computerStorageCopyPhase = "identity_rekeyed"
	computerStorageCopyExpanded        computerStorageCopyPhase = "expanded"
	computerStorageCopyManifestWritten computerStorageCopyPhase = "manifest_written"
	computerStorageCopyPublished       computerStorageCopyPhase = "published"
	// computerStorageCopyRefusalCleanup is not a durable phase: it marks the
	// instant a refusal has removed the payload and has not yet written its
	// tombstone, so a test can observe that the generation lock is still held
	// and no creator can take the destination.
	computerStorageCopyRefusalCleanup computerStorageCopyPhase = "refusal_cleanup"
)

type computerStorageCopyFacts struct {
	OSIdentityRekeyed     bool
	MachineIDBeforeDigest string
	MachineIDAfterDigest  string
	MachineIDRepaired     bool
	FilesystemExpanded    bool
}

type computerStorageCopyManifest struct {
	Version               int                             `json:"version"`
	Request               CopyComputerStorageRequest      `json:"request"`
	Phase                 computerStorageCopyPhase        `json:"phase"`
	SourceDigest          string                          `json:"source_digest,omitempty"`
	DestinationDigest     string                          `json:"destination_digest,omitempty"`
	OSIdentityRekeyed     bool                            `json:"os_identity_rekeyed,omitempty"`
	MachineIDBeforeDigest string                          `json:"machine_id_before_digest,omitempty"`
	MachineIDAfterDigest  string                          `json:"machine_id_after_digest,omitempty"`
	MachineIDRepaired     bool                            `json:"machine_id_repaired,omitempty"`
	SourceUnchanged       bool                            `json:"source_unchanged,omitempty"`
	FilesystemExpanded    bool                            `json:"filesystem_expanded,omitempty"`
	Receipt               *ComputerStorageCopyReceipt     `json:"receipt,omitempty"`
	Recovery              computerStorageRecoveryDeferral `json:"recovery,omitempty"`
}

func (engine *ContainerdEngine) storageCopyCheckpoint(phase computerStorageCopyPhase) error {
	if engine.storageCopyHook == nil {
		return nil
	}
	return engine.storageCopyHook(phase)
}

func writeComputerStorageCopyManifest(root string, manifest computerStorageCopyManifest) error {
	payload, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	return writeDurableFile(root, ".storage-copy.json.tmp-", "storage-copy.json", payload, 0o600)
}

func readComputerStorageCopyManifest(path string) (computerStorageCopyManifest, bool, error) {
	payload, present, err := readComputerRecoveryRecord(path)
	if !present && err == nil {
		return computerStorageCopyManifest{}, false, nil
	}
	if err != nil {
		return computerStorageCopyManifest{}, false, err
	}
	var manifest computerStorageCopyManifest
	if err := json.Unmarshal(payload, &manifest); err != nil {
		return computerStorageCopyManifest{}, false, err
	}
	if manifest.Version != 1 {
		return computerStorageCopyManifest{}, false, errors.New("Computer Storage copy manifest version is unsupported")
	}
	return manifest, true, nil
}

func sameComputerStorageCopyRequest(left, right CopyComputerStorageRequest) bool {
	return left.Operation == right.Operation && left.BackupID == right.BackupID && left.CopyID == right.CopyID &&
		left.SourceComputerID == right.SourceComputerID && left.SourceStorageID == right.SourceStorageID &&
		left.SourceGeneration == right.SourceGeneration && left.SourceSize == right.SourceSize &&
		left.SourceDigest == right.SourceDigest && left.ExportID == right.ExportID &&
		left.ExternalPath == right.ExternalPath && left.ManifestDigest == right.ManifestDigest &&
		sameComputerStorageIdentity(left.Destination, right.Destination) &&
		left.Destination.DiskBytes == right.Destination.DiskBytes && left.Destination.IntentRevision == right.Destination.IntentRevision &&
		left.Authority == right.Authority
}

func validateStorageCopySource(ctx context.Context, root string, request CopyComputerStorageRequest) (string, error) {
	manifest, present, err := readComputerBackupManifest(filepath.Join(root, "copy.json"))
	if err != nil {
		return "", err
	}
	if !present || manifest.Phase != computerBackupPublished || manifest.Receipt == nil ||
		manifest.BackupID != request.BackupID || manifest.CopyID != request.CopyID ||
		manifest.Storage.ComputerID != request.SourceComputerID || manifest.Storage.StorageID != request.SourceStorageID ||
		manifest.Storage.StorageGeneration != request.SourceGeneration || manifest.Storage.DiskBytes != request.SourceSize ||
		manifest.Authority.NodeID != request.Authority.NodeID || manifest.Authority.RootInstanceID != request.Authority.RootInstanceID ||
		manifest.ContentDigest != request.SourceDigest || manifest.Receipt.ContentDigest != request.SourceDigest {
		return "", errors.New("Computer Storage copy source conflicts with published Backup authority")
	}
	path := filepath.Join(root, "backup.ext4")
	info, err := os.Lstat(path)
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() || info.Size() != request.SourceSize {
		return "", errors.New("Computer Storage copy source is truncated or not a regular file")
	}
	digest, err := digestFile(ctx, path)
	if err != nil {
		return "", err
	}
	if digest != request.SourceDigest {
		return "", errors.New("Computer Storage copy source digest mismatch")
	}
	return path, nil
}

// openStorageCopySource reads through the retained descriptor when one
// exists — an import must never re-resolve the operator's pathname — and
// otherwise opens the helper-managed source by path.
func openStorageCopySource(path string, handle *os.File) (io.Reader, func() error, error) {
	if handle != nil {
		if _, err := handle.Seek(0, io.SeekStart); err != nil {
			return nil, nil, err
		}
		return handle, func() error { return nil }, nil
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	return file, file.Close, nil
}

func digestStorageCopySource(ctx context.Context, path string, handle *os.File) (string, error) {
	if handle == nil {
		return digestFile(ctx, path)
	}
	if _, err := handle.Seek(0, io.SeekStart); err != nil {
		return "", err
	}
	return digestReader(ctx, handle)
}

func digestFilePrefix(ctx context.Context, path string, size int64) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	digest := sha256.New()
	buffer := make([]byte, 256*1024)
	var copied int64
	var copyErr error
	for copied < size && copyErr == nil {
		if err := ctx.Err(); err != nil {
			copyErr = err
			break
		}
		want := min(int64(len(buffer)), size-copied)
		read, readErr := io.ReadFull(file, buffer[:want])
		if read > 0 {
			_, copyErr = digest.Write(buffer[:read])
			copied += int64(read)
		}
		if copyErr == nil {
			copyErr = readErr
		}
	}
	err = errors.Join(copyErr, file.Close())
	if err != nil {
		return "", err
	}
	if copied != size {
		return "", io.ErrUnexpectedEOF
	}
	return "sha256:" + hex.EncodeToString(digest.Sum(nil)), nil
}

func runFilesystemTool(ctx context.Context, name string, arguments ...string) ([]byte, error) {
	command := exec.CommandContext(ctx, name, arguments...)
	output, err := command.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("%s failed: %w: %s", filepath.Base(name), err, strings.TrimSpace(string(output)))
	}
	return output, nil
}

func ensureRealDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("clone identity path %q is not a real directory", path)
	}
	return nil
}

func readRegularFile(path string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("identity evidence is not a regular file")
	}
	return os.ReadFile(path)
}

func rekeyCloneIdentity(mountPath string) (computerStorageCopyFacts, error) {
	facts := computerStorageCopyFacts{}
	current, err := ensureComputerStorageIdentity(mountPath)
	if err != nil {
		return facts, err
	}
	paths := computerStorageIdentityAt(mountPath)
	for {
		identity, err := newComputerMachineID()
		if err != nil {
			return facts, err
		}
		if computerMachineIDDigest(identity) == current.MachineIDDigest {
			continue
		}
		if err := writeDurableFile(paths.Directory, ".machine-id.tmp-", computerStorageMachineIDName, identity, 0o444); err != nil {
			return facts, err
		}
		break
	}
	newMachineID, err := readRegularFile(paths.MachineID)
	if err != nil || !validComputerMachineID(newMachineID) {
		return facts, errors.New("clone machine-id was not well-formed after rekey")
	}
	newDigest := computerMachineIDDigest(newMachineID)
	if newDigest == current.MachineIDDigest {
		return facts, errors.New("clone machine-id was not observably rekeyed")
	}
	root, err := os.Open(mountPath)
	if err != nil {
		return facts, err
	}
	defer root.Close()
	facts.OSIdentityRekeyed = true
	facts.MachineIDBeforeDigest = current.MachineIDDigest
	facts.MachineIDAfterDigest = newDigest
	facts.MachineIDRepaired = current.Repaired
	return facts, unix.Syncfs(int(root.Fd()))
}

func ext4Geometry(ctx context.Context, imagePath string) (blocks, blockSize int64, returnedErr error) {
	dumpe2fs, err := findRootTool("dumpe2fs")
	if err != nil {
		return 0, 0, err
	}
	output, err := runFilesystemTool(ctx, dumpe2fs, "-h", imagePath)
	if err != nil {
		return 0, 0, err
	}
	for _, line := range strings.Split(string(output), "\n") {
		if key, value, ok := strings.Cut(line, ":"); ok {
			parsed, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
			if strings.TrimSpace(key) == "Block count" {
				blocks, returnedErr = parsed, err
			}
			if strings.TrimSpace(key) == "Block size" {
				blockSize, returnedErr = parsed, err
			}
		}
	}
	if returnedErr != nil {
		return 0, 0, returnedErr
	}
	if blocks <= 0 || blockSize <= 0 {
		return 0, 0, errors.New("ext4 block geometry was not reported")
	}
	return blocks, blockSize, nil
}

func (engine *ContainerdEngine) finalizeComputerStorageCopy(ctx context.Context, operation, imagePath, mountPath string, sourceSize int64, expanded bool) (facts computerStorageCopyFacts, returnedErr error) {
	if engine.storageCopyFinalize != nil {
		if operation == "clone" || operation == "import" {
			if err := engine.storageCopyCheckpoint(computerStorageCopyMountedRekey); err != nil {
				return facts, err
			}
		}
		return engine.storageCopyFinalize(ctx, operation, imagePath, mountPath, sourceSize, expanded)
	}
	e2fsck, err := findRootTool("e2fsck")
	if err != nil {
		return facts, err
	}
	if _, err := runFilesystemTool(ctx, e2fsck, "-f", "-n", imagePath); err != nil {
		return facts, err
	}
	beforeBlocks, blockSize, err := ext4Geometry(ctx, imagePath)
	if err != nil {
		return facts, err
	}
	if expanded {
		resize2fs, err := findRootTool("resize2fs")
		if err != nil {
			return facts, err
		}
		if _, err := runFilesystemTool(ctx, resize2fs, imagePath); err != nil {
			return facts, err
		}
		afterBlocks, afterBlockSize, err := ext4Geometry(ctx, imagePath)
		alreadyExpanded := beforeBlocks > sourceSize/blockSize
		if err != nil || afterBlockSize != blockSize || (afterBlocks <= beforeBlocks && !alreadyExpanded) {
			return facts, errors.New("filesystem expansion was not observed in ext4 block count")
		}
		facts.FilesystemExpanded = true
	}
	if operation != "clone" && operation != "import" {
		return facts, nil
	}
	if err := os.MkdirAll(mountPath, 0o700); err != nil {
		return facts, err
	}
	loopPath, err := engine.computerDiskSystem().attachAndMount(ctx, imagePath, mountPath)
	if err != nil {
		return facts, err
	}
	defer func() {
		returnedErr = errors.Join(returnedErr, engine.computerDiskSystem().detach(mountPath, loopPath, imagePath))
	}()
	if err := engine.storageCopyCheckpoint(computerStorageCopyMountedRekey); err != nil {
		return facts, err
	}
	rekeyFacts, err := rekeyCloneIdentity(mountPath)
	rekeyFacts.FilesystemExpanded = facts.FilesystemExpanded
	return rekeyFacts, err
}

// computerStorageDestinationFullError is raised only where this call writes to
// the Node's own Computer-disk filesystem. ENOSPC anywhere else -- inside the
// copied image while its filesystem is mounted, for example -- is not host
// destination capacity and never becomes a capacity refusal.
type computerStorageDestinationFullError struct {
	ObservedAvailableBytes int64
	Err                    error
}

func (e *computerStorageDestinationFullError) Error() string {
	return fmt.Sprintf("Computer Storage destination filesystem is full with %d available bytes: %v",
		e.ObservedAvailableBytes, e.Err)
}

func (e *computerStorageDestinationFullError) Unwrap() error { return e.Err }

// destinationCapacityError converts ENOSPC from one exact destination
// allocation or write into the typed capacity refusal, recording the Node
// filesystem's available bytes while this call still owns the generation. Any
// other error, and any failure to read that capacity fact, is returned
// unchanged.
func (engine *ContainerdEngine) destinationCapacityError(err error) error {
	if err == nil || soleCause(err) != unix.ENOSPC {
		return err
	}
	available, availableErr := filesystemAvailableBytes(filepath.Join(engine.config.RuntimeRoot, "computer-disks"))
	if availableErr != nil {
		return errors.Join(err, availableErr)
	}
	return &computerStorageDestinationFullError{ObservedAvailableBytes: available, Err: err}
}

// computerStorageCopySourceError names a recognized source-validation failure
// at the site that detects it, so the verb's typed outcome never depends on
// matching an error's text.
type computerStorageCopySourceError struct {
	Code string
	Err  error
}

func (e *computerStorageCopySourceError) Error() string { return e.Err.Error() }

func (e *computerStorageCopySourceError) Unwrap() error { return e.Err }

// soleCause unwraps a single-cause error chain to the error underneath. An
// error that joins several failures has no sole cause: it returns nil, so a
// second failure travelling beside ENOSPC can never be read as a clean
// capacity fact.
func soleCause(err error) error {
	for err != nil {
		if _, joined := err.(interface{ Unwrap() []error }); joined {
			return nil
		}
		next := errors.Unwrap(err)
		if next == nil {
			return err
		}
		err = next
	}
	return nil
}

// storageCopyDestinationPublished reports whether the destination generation
// already owns published bytes. A failure after publication is never rewritten
// into an absence receipt, because that receipt authorizes deletion.
func storageCopyDestinationPublished(runtimeRoot string, request CopyComputerStorageRequest) (bool, error) {
	destinationName, err := deterministicComputerDiskName(request.Destination)
	if err != nil {
		return false, err
	}
	if _, err := os.Lstat(filepath.Join(runtimeRoot, "computer-disks", destinationName, "disk.ext4")); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// storageCopyFailureReceipt proves the destination generation holds no bytes
// and names the typed reason. `observedAvailableBytes` is the capacity fact
// behind an `insufficient_disk` refusal and is zero for every other code.
// A refused copy never deletes the destination root itself. `attachment.lock`
// lives inside that root, so unlinking it would drop the generation ownership
// this refusal is holding and let a concurrent creator take a replacement
// inode mid-cleanup. The payload is removed through the retained root instead,
// and a durable refusal tombstone is left beside the lock. A root holding
// nothing but the lock and that tombstone is an absent generation: it is not
// quarantined, it is not prepared, and it is deleted whole by ordinary
// authorized removal.
const (
	computerDiskAttachmentLockFile   = "attachment.lock"
	computerStorageCopyRefusalRecord = "copy-refused.json"
)

type computerStorageCopyRefusal struct {
	Version   int                        `json:"version"`
	DiskName  string                     `json:"disk_name"`
	Storage   ComputerStorageReference   `json:"storage"`
	Receipt   ComputerStorageCopyReceipt `json:"receipt"`
	RefusedAt time.Time                  `json:"refused_at"`
}

func writeComputerStorageCopyRefusal(root, name string, storage ComputerStorageReference,
	receipt ComputerStorageCopyReceipt) error {
	payload, err := json.Marshal(computerStorageCopyRefusal{Version: 1, DiskName: name,
		Storage: storage, Receipt: receipt, RefusedAt: time.Now().UTC()})
	if err != nil {
		return err
	}
	return writeDurableFile(root, ".copy-refused.json.tmp-", computerStorageCopyRefusalRecord, payload, 0o600)
}

// readComputerStorageCopyRefusal returns the tombstone only when it is exactly
// the refusal this generation's own root may carry. Anything else is not
// absence evidence and leaves the ordinary recovery paths in charge.
func readComputerStorageCopyRefusal(root, name string) (computerStorageCopyRefusal, bool, error) {
	payload, present, err := readComputerRecoveryRecord(filepath.Join(root, computerStorageCopyRefusalRecord))
	if err != nil || !present {
		return computerStorageCopyRefusal{}, false, err
	}
	var refusal computerStorageCopyRefusal
	if err := json.Unmarshal(payload, &refusal); err != nil {
		return computerStorageCopyRefusal{}, false, err
	}
	expected, err := deterministicComputerDiskName(refusal.Storage)
	if err != nil {
		return computerStorageCopyRefusal{}, false, err
	}
	if refusal.Version != 1 || refusal.DiskName != name || expected != name ||
		refusal.Receipt.Kind != "computer_storage_copy_failed_absent" || !refusal.Receipt.DestinationAbsent ||
		refusal.Receipt.FailureCode == "" || refusal.RefusedAt.IsZero() {
		return computerStorageCopyRefusal{}, false, errors.New("Computer Storage copy refusal record lacks exact generation authority")
	}
	return refusal, true, nil
}

// refusedComputerStorageAbsent reports the one shape a refused generation may
// have: its own lock, its own tombstone, and no payload at all.
func refusedComputerStorageAbsent(root, name string) (computerStorageCopyRefusal, bool, error) {
	refusal, present, err := readComputerStorageCopyRefusal(root, name)
	if err != nil || !present {
		return computerStorageCopyRefusal{}, false, err
	}
	if err := requireRefusedComputerStorageAbsence(root); err != nil {
		return computerStorageCopyRefusal{}, false, err
	}
	return refusal, true, nil
}

// reconcileDeferredComputerStorageCopy runs one recovery attempt for a
// destination a previous sweep deferred, and translates its outcome into the
// same typed answers the sweep records. It is the same code path the boot
// sweep uses, called with the copy's own generation lock already held, so the
// attempt cannot interleave with another copy of the same generation.
func (engine *ContainerdEngine) reconcileDeferredComputerStorageCopy(ctx context.Context, root, name string,
	destination ComputerStorageReference) error {
	_, resumeErr := engine.resumeComputerStorageCopy(ctx, root, name)
	if resumeErr != nil {
		evidence, resolveErr := engine.resolveComputerStorageRecoveryFailure(root, name, "computer_storage_copy",
			destination, resumeErr, true)
		if resolveErr != nil {
			return errors.Join(resumeErr, resolveErr)
		}
		switch evidence.Action {
		case SweepActionQuarantined:
			return &ComputerStorageQuarantinedError{Storage: destination}
		default:
			return &ComputerStorageResumeDeferredError{Storage: destination}
		}
	}
	// The fault cleared. Drop the deferral record so the next ordinary call
	// is an ordinary copy again rather than another counted attempt.
	return engine.clearOperationalComputerRecoveryDeferral(root, name, "computer_storage_copy")
}

// computerDiskRemovalResidueAbsent reports the generation-root shapes that
// hold no bytes at all, which an authorized removal therefore deletes whole:
// the exact refusal shape -- its own lock and its own durable tombstone -- and
// a root holding nothing but the lock. The second is what a creator leaves
// when it takes the flock and fails before writing anything, and what a
// removal leaves if it is interrupted between dropping the payload and
// unlinking the root. Reading it as bytes without an authority manifest would
// wedge the removal of a refused clone for good.
//
// Only removal may read a root this way -- its per-generation inventory as
// well as its deletion, because an inventory that refused the shape would
// never let deletion be reached. A lock-only root is also the ordinary
// first-allocation shape, so attachment, preparation, and the startup sweep
// keep judging it by the narrower refusal rule.
func computerDiskRemovalResidueAbsent(root, name string) (bool, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return false, err
	}
	lockOnly := true
	for _, entry := range entries {
		if entry.Name() != computerDiskAttachmentLockFile {
			lockOnly = false
			break
		}
	}
	if lockOnly {
		return true, nil
	}
	_, absent, err := refusedComputerStorageAbsent(root, name)
	return absent, err
}

func removeComputerStorageCopyPayload(root string) error {
	entries, err := os.ReadDir(root)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.Name() == computerDiskAttachmentLockFile || entry.Name() == computerStorageCopyRefusalRecord {
			continue
		}
		if err := os.RemoveAll(filepath.Join(root, entry.Name())); err != nil {
			return err
		}
	}
	return syncDirectory(root)
}

func requireRefusedComputerStorageAbsence(root string) error {
	entries, err := os.ReadDir(root)
	if err != nil {
		return err
	}
	tombstone := false
	for _, entry := range entries {
		switch entry.Name() {
		case computerDiskAttachmentLockFile:
		case computerStorageCopyRefusalRecord:
			tombstone = true
		default:
			return errors.New("Computer Storage copy payload remains after failure cleanup")
		}
	}
	if !tombstone {
		return errors.New("Computer Storage copy refusal left no durable tombstone")
	}
	return nil
}

func (engine *ContainerdEngine) storageCopyFailureReceipt(request CopyComputerStorageRequest, code string,
	observedAvailableBytes int64) (CopyComputerStorageResponse, error) {
	destinationName, err := deterministicComputerDiskName(request.Destination)
	if err != nil {
		return CopyComputerStorageResponse{}, err
	}
	runtimeRoot := engine.config.RuntimeRoot
	destinationRoot := filepath.Join(runtimeRoot, "computer-disks", destinationName)
	// Unlinking a pathname is not absence: a still-mounted filesystem or a
	// live loop device keeps the copied bytes reachable. Prove both are gone
	// before any receipt claims the destination holds nothing.
	mountPath := filepath.Join(runtimeRoot, "computer-copy-mounts", destinationName)
	if _, mounted, err := engine.computerDiskSystem().mountedSource(mountPath); err != nil {
		return CopyComputerStorageResponse{}, err
	} else if mounted {
		return CopyComputerStorageResponse{}, errors.New("Computer Storage copy destination remains mounted after failure")
	}
	loops, err := engine.computerDiskSystem().loopsForRoot(destinationRoot)
	if err != nil {
		return CopyComputerStorageResponse{}, err
	}
	if len(loops) != 0 {
		return CopyComputerStorageResponse{}, errors.New("Computer Storage copy destination remains loop-attached after failure")
	}
	if err := removeComputerStorageCopyPayload(destinationRoot); err != nil {
		return CopyComputerStorageResponse{}, err
	}
	// The payload is gone and the tombstone is not written yet: this is the
	// one instant a creator could race the refusal, and the generation lock
	// inode it must take is still the one this call holds.
	if err := engine.storageCopyCheckpoint(computerStorageCopyRefusalCleanup); err != nil {
		return CopyComputerStorageResponse{}, err
	}
	if err := os.Remove(mountPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return CopyComputerStorageResponse{}, err
	}
	receiptID, err := randomCapability()
	if err != nil {
		return CopyComputerStorageResponse{}, err
	}
	receipt := ComputerStorageCopyReceipt{
		Kind: "computer_storage_copy_failed_absent", ReceiptID: receiptID, Operation: request.Operation,
		BackupID: request.BackupID, CopyID: request.CopyID, ExportID: request.ExportID,
		ExternalPath: request.ExternalPath, ManifestDigest: request.ManifestDigest,
		SourceComputerID: request.SourceComputerID, SourceStorageID: request.SourceStorageID,
		SourceGeneration: request.SourceGeneration, DestinationComputerID: request.Destination.ComputerID,
		DestinationStorageID: request.Destination.StorageID, DestinationGeneration: request.Destination.StorageGeneration,
		NodeID: request.Authority.NodeID, RootInstanceID: request.Authority.RootInstanceID,
		JobID: request.Authority.JobID, OperationRevision: request.Authority.OperationRevision,
		CleanupFence: request.Authority.CleanupFence, HelperGeneration: request.Authority.HelperGeneration,
		SourceSize: request.SourceSize, DestinationSize: request.Destination.DiskBytes,
		SourceDigest: request.SourceDigest, FailureCode: code, DestinationAbsent: true,
		ObservedAvailableBytes: observedAvailableBytes,
	}
	if err := writeComputerStorageCopyRefusal(destinationRoot, destinationName, request.Destination, receipt); err != nil {
		return CopyComputerStorageResponse{}, err
	}
	if err := requireRefusedComputerStorageAbsence(destinationRoot); err != nil {
		return CopyComputerStorageResponse{}, err
	}
	return CopyComputerStorageResponse{Receipt: receipt}, nil
}

// storageCopyFailureCode is the closed typed vocabulary each verb may report
// instead of an opaque engine failure. The capacity refusal is recognized only
// as the exact typed error raised at a destination write site, never from an
// error chain or from tool output text, and never from a joined error that
// also carries a second failure: a copy whose cleanup or detach also failed is
// still an engine failure. Clone has that one code and nothing else; every
// other clone failure leaves the destination in doubt and stays on the
// integrity path.
func storageCopyFailureCode(operation string, err error) (string, int64) {
	if full, exact := err.(*computerStorageDestinationFullError); exact {
		if operation == "clone" || operation == "import" {
			return "insufficient_disk", full.ObservedAvailableBytes
		}
		return "", 0
	}
	if operation != "import" {
		return "", 0
	}
	if source, exact := err.(*computerStorageCopySourceError); exact {
		return source.Code, 0
	}
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return "cancelled", 0
	case strings.Contains(err.Error(), "digest"):
		return "digest_mismatch", 0
	case strings.Contains(err.Error(), "Custody import manifest"),
		strings.Contains(err.Error(), "Custody import disk size"):
		return "manifest_invalid", 0
	}
	return "", 0
}

func (engine *ContainerdEngine) CopyComputerStorage(ctx context.Context, request CopyComputerStorageRequest) (response CopyComputerStorageResponse, returnedErr error) {
	engine.computerBackupMu.Lock()
	defer engine.computerBackupMu.Unlock()
	engine.storageCopyMu.Lock()
	defer engine.storageCopyMu.Unlock()
	managedSource := request.Operation == "restore" || request.Operation == "clone"
	importSource := request.Operation == "import"
	if (!managedSource && !importSource) || request.BackupID == "" || request.CopyID == "" ||
		(importSource && (request.ExportID == "" || request.ExternalPath == "" || request.ManifestDigest == "")) ||
		request.SourceComputerID == "" || request.SourceStorageID == "" || request.SourceGeneration < 1 || request.SourceSize < 1 ||
		request.SourceDigest == "" || request.Destination.ComputerID == "" || request.Destination.StorageID == "" ||
		request.Destination.StorageGeneration < 1 || request.Destination.DiskBytes < request.SourceSize ||
		request.Destination.IntentRevision != request.Authority.OperationRevision || request.Authority.NodeID == "" ||
		request.Authority.BootSessionID == "" || request.Authority.HelperGeneration == 0 || request.Authority.RootInstanceID == "" ||
		request.Authority.JobID == "" || request.Authority.OperationRevision < 1 || request.Authority.CleanupFence == "" {
		return CopyComputerStorageResponse{}, errors.New("Computer Storage copy request is incomplete")
	}
	if quarantined, err := computerDiskQuarantined(engine.config.RuntimeRoot, request.Destination); err != nil {
		return CopyComputerStorageResponse{}, err
	} else if quarantined {
		return CopyComputerStorageResponse{}, &ComputerStorageQuarantinedError{Storage: request.Destination}
	}
	destinationName, err := deterministicComputerDiskName(request.Destination)
	if err != nil {
		return CopyComputerStorageResponse{}, err
	}
	destinationRoot := filepath.Join(engine.config.RuntimeRoot, "computer-disks", destinationName)
	lock, err := engine.openComputerStorageDestination(ctx, destinationRoot)
	if err != nil {
		return CopyComputerStorageResponse{}, err
	}
	defer closeComputerDiskLock(lock)
	// Registered after the generation lock and inside both copy mutexes, so
	// it runs before any of them is released: the publication check, the
	// mount/loop absence proof and the deletion are one owned interval that no
	// concurrent copy can interleave with.
	defer func() {
		if returnedErr == nil {
			return
		}
		code, observedAvailableBytes := storageCopyFailureCode(request.Operation, returnedErr)
		if code == "" {
			return
		}
		published, err := storageCopyDestinationPublished(engine.config.RuntimeRoot, request)
		if err != nil || published {
			return
		}
		converted, receiptErr := engine.storageCopyFailureReceipt(request, code, observedAvailableBytes)
		if receiptErr != nil {
			returnedErr = errors.Join(returnedErr, receiptErr)
			return
		}
		response, returnedErr = converted, nil
	}()
	// A generation already refused stays refused: its tombstone replays the
	// exact receipt instead of starting the copy again under an identity that
	// L1 has already closed.
	if refusal, absent, refusalErr := refusedComputerStorageAbsent(destinationRoot, destinationName); refusalErr != nil {
		return CopyComputerStorageResponse{}, refusalErr
	} else if absent {
		if !sameComputerStorageIdentity(refusal.Storage, request.Destination) ||
			refusal.Receipt.OperationRevision != request.Authority.OperationRevision {
			return CopyComputerStorageResponse{}, errors.New("Computer Storage copy destination was refused under different durable authority")
		}
		return CopyComputerStorageResponse{Receipt: refusal.Receipt}, nil
	}
	// Source validation runs inside the owned destination interval: its typed
	// failures must reach the same proven-absence receipt, or a rejected
	// portable manifest would leave the reserved import with nothing to
	// acknowledge.
	var sourcePath string
	// An import holds the descriptor admission verified; the copy and both
	// digests read that inode instead of re-resolving the operator's path.
	var sourceHandle *os.File
	defer func() {
		if sourceHandle != nil {
			_ = sourceHandle.Close()
		}
	}()
	if importSource {
		sourceHandle, err = engine.validateImportCustodySource(request)
	} else {
		var copyName string
		copyName, err = deterministicComputerBackupCopyName(request.CopyID)
		if err == nil {
			sourceRoot := filepath.Join(engine.config.RuntimeRoot, "computer-backups", copyName)
			sourcePath, err = validateStorageCopySource(ctx, sourceRoot, request)
		}
	}
	if err != nil {
		return CopyComputerStorageResponse{}, err
	}
	manifestPath := filepath.Join(destinationRoot, "storage-copy.json")
	manifest, present, err := readComputerStorageCopyManifest(manifestPath)
	if err != nil {
		return CopyComputerStorageResponse{}, err
	}
	if present && !sameComputerStorageCopyRequest(manifest.Request, request) {
		return CopyComputerStorageResponse{}, errors.New("Computer Storage copy destination has different durable authority")
	}
	// A destination a boot sweep deferred is not simply refused here. Nothing
	// else ever ran another recovery: healthy heartbeats do not sweep, and the
	// abandonment bound needs counted attempts as well as elapsed hours, so a
	// copy that answered `resume_deferred` and stopped would stay deferred for
	// as long as the node stayed healthy -- including after the underlying
	// fault cleared. Ordinary reconciliation therefore runs the same recovery
	// step the sweep runs, under the locks this call already holds, and counts
	// the attempt. Success falls through into the ordinary copy below (a
	// rolled-back destination starts again; a completed one replays its
	// receipt); a still-faulted destination answers deferred with the
	// incremented count; the bound answers quarantined, which is terminal.
	if present && manifest.Phase != computerStorageCopyPublished && manifest.Recovery.Attempts > 0 {
		if err := engine.reconcileDeferredComputerStorageCopy(ctx, destinationRoot, destinationName, request.Destination); err != nil {
			return CopyComputerStorageResponse{}, err
		}
		manifest, present, err = readComputerStorageCopyManifest(manifestPath)
		if err != nil {
			return CopyComputerStorageResponse{}, err
		}
		if present && !sameComputerStorageCopyRequest(manifest.Request, request) {
			return CopyComputerStorageResponse{}, errors.New("Computer Storage copy destination has different durable authority")
		}
	}
	publishedPath := filepath.Join(destinationRoot, "disk.ext4")
	stagingPath := filepath.Join(destinationRoot, "disk.ext4.staging")
	if present && manifest.Phase == computerStorageCopyPublished && manifest.Receipt != nil {
		digest, err := digestFile(ctx, publishedPath)
		if err != nil {
			return CopyComputerStorageResponse{}, err
		}
		if digest != manifest.Receipt.DestinationDigest {
			return CopyComputerStorageResponse{}, errors.New("published Computer Storage copy digest changed")
		}
		return CopyComputerStorageResponse{Receipt: *manifest.Receipt}, nil
	}
	if !present {
		manifest = computerStorageCopyManifest{Version: 1, Request: request, Phase: computerStorageCopyReserved}
		if err := writeComputerStorageCopyManifest(destinationRoot, manifest); err != nil {
			return CopyComputerStorageResponse{}, err
		}
		if err := engine.storageCopyCheckpoint(computerStorageCopyReserved); err != nil {
			return CopyComputerStorageResponse{}, err
		}
	}
	if manifest.Phase == computerStorageCopyReserved || manifest.Phase == computerStorageCopyAllocated {
		if err := os.Remove(stagingPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return CopyComputerStorageResponse{}, err
		}
		file, err := os.OpenFile(stagingPath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
		if err != nil {
			return CopyComputerStorageResponse{}, err
		}
		// Only the allocation itself can be a capacity fact. Either close
		// failure joins the outcome and keeps it an engine failure, and
		// neither is ever discarded.
		allocationErr, allocationCloseErr := engine.allocateComputerDestination(stagingPath, request.SourceSize)
		outerCloseErr := file.Close()
		if allocationCloseErr != nil || outerCloseErr != nil {
			return CopyComputerStorageResponse{}, errors.Join(allocationErr, allocationCloseErr, outerCloseErr)
		}
		if allocationErr != nil {
			return CopyComputerStorageResponse{}, engine.destinationCapacityError(allocationErr)
		}
		manifest.Phase = computerStorageCopyAllocated
		if err := writeComputerStorageCopyManifest(destinationRoot, manifest); err != nil {
			return CopyComputerStorageResponse{}, err
		}
		if err := engine.storageCopyCheckpoint(computerStorageCopyAllocated); err != nil {
			return CopyComputerStorageResponse{}, err
		}
		source, closeSource, err := openStorageCopySource(sourcePath, sourceHandle)
		if err != nil {
			return CopyComputerStorageResponse{}, err
		}
		destination, err := os.OpenFile(stagingPath, os.O_WRONLY, 0)
		if err != nil {
			_ = closeSource()
			return CopyComputerStorageResponse{}, err
		}
		copied, copyErr := engine.copyComputerBackup(destination, custodyContextReader{ctx: ctx, r: source}, request.SourceSize)
		if copyErr == nil && copied != request.SourceSize {
			copyErr = io.ErrUnexpectedEOF
		}
		if copyErr == nil {
			copyErr = destination.Sync()
		}
		// The destination write is classified alone: a close that also failed
		// joins the error and keeps the whole outcome an engine failure.
		closeErr := errors.Join(closeSource(), destination.Close())
		if closeErr != nil {
			return CopyComputerStorageResponse{}, errors.Join(copyErr, closeErr)
		}
		if copyErr != nil {
			return CopyComputerStorageResponse{}, engine.destinationCapacityError(copyErr)
		}
		manifest.Phase = computerStorageCopyCopied
		if err := writeComputerStorageCopyManifest(destinationRoot, manifest); err != nil {
			return CopyComputerStorageResponse{}, err
		}
		if err := engine.storageCopyCheckpoint(computerStorageCopyCopied); err != nil {
			return CopyComputerStorageResponse{}, err
		}
	}
	sourceDigest, err := digestStorageCopySource(ctx, sourcePath, sourceHandle)
	if err != nil {
		return CopyComputerStorageResponse{}, err
	}
	if sourceDigest != request.SourceDigest {
		return CopyComputerStorageResponse{}, errors.New("Computer Storage copy source digest mismatch before publication")
	}
	manifest.SourceDigest = sourceDigest
	if manifest.Phase == computerStorageCopyCopied {
		stagingDigest, err := digestFilePrefix(ctx, stagingPath, request.SourceSize)
		if err != nil {
			return CopyComputerStorageResponse{}, err
		}
		if stagingDigest != request.SourceDigest {
			return CopyComputerStorageResponse{}, errors.New("Computer Storage staging digest mismatch before filesystem mutation")
		}
		manifest.Phase = computerStorageCopySourceVerified
		if err := writeComputerStorageCopyManifest(destinationRoot, manifest); err != nil {
			return CopyComputerStorageResponse{}, err
		}
		if err := engine.storageCopyCheckpoint(computerStorageCopySourceVerified); err != nil {
			return CopyComputerStorageResponse{}, err
		}
	}
	facts := computerStorageCopyFacts{}
	if manifest.Phase == computerStorageCopySourceVerified {
		if request.Destination.DiskBytes > request.SourceSize {
			expansionErr, expansionCloseErr := engine.allocateComputerDestination(stagingPath, request.Destination.DiskBytes)
			if expansionCloseErr != nil {
				return CopyComputerStorageResponse{}, errors.Join(expansionErr, expansionCloseErr)
			}
			if expansionErr != nil {
				return CopyComputerStorageResponse{}, engine.destinationCapacityError(expansionErr)
			}
		}
		expanded := request.Destination.DiskBytes > request.SourceSize
		mountPath := filepath.Join(engine.config.RuntimeRoot, "computer-copy-mounts", destinationName)
		facts, err = engine.finalizeComputerStorageCopy(ctx, request.Operation, stagingPath, mountPath, request.SourceSize, expanded)
		if err != nil {
			return CopyComputerStorageResponse{}, err
		}
		manifest.OSIdentityRekeyed = facts.OSIdentityRekeyed
		manifest.MachineIDBeforeDigest = facts.MachineIDBeforeDigest
		manifest.MachineIDAfterDigest = facts.MachineIDAfterDigest
		manifest.MachineIDRepaired = facts.MachineIDRepaired
		manifest.FilesystemExpanded = facts.FilesystemExpanded
		manifest.Phase = computerStorageCopyIdentityRekeyed
		if err := writeComputerStorageCopyManifest(destinationRoot, manifest); err != nil {
			return CopyComputerStorageResponse{}, err
		}
		if err := engine.storageCopyCheckpoint(computerStorageCopyIdentityRekeyed); err != nil {
			return CopyComputerStorageResponse{}, err
		}
		if facts.FilesystemExpanded {
			manifest.Phase = computerStorageCopyExpanded
			if err := writeComputerStorageCopyManifest(destinationRoot, manifest); err != nil {
				return CopyComputerStorageResponse{}, err
			}
			if err := engine.storageCopyCheckpoint(computerStorageCopyExpanded); err != nil {
				return CopyComputerStorageResponse{}, err
			}
		}
	}
	if manifest.Phase == computerStorageCopyIdentityRekeyed &&
		request.Destination.DiskBytes > request.SourceSize {
		manifest.Phase = computerStorageCopyExpanded
		if err := writeComputerStorageCopyManifest(destinationRoot, manifest); err != nil {
			return CopyComputerStorageResponse{}, err
		}
		if err := engine.storageCopyCheckpoint(computerStorageCopyExpanded); err != nil {
			return CopyComputerStorageResponse{}, err
		}
	}
	workingPath := stagingPath
	if _, err := os.Lstat(workingPath); errors.Is(err, os.ErrNotExist) {
		if _, publishedErr := os.Lstat(publishedPath); publishedErr != nil {
			return CopyComputerStorageResponse{}, err
		}
		workingPath = publishedPath
	} else if err != nil {
		return CopyComputerStorageResponse{}, err
	}
	if err := verifyComputerDiskAllocation(workingPath, request.Destination.DiskBytes); err != nil {
		return CopyComputerStorageResponse{}, err
	}
	destinationDigest, err := digestFile(ctx, workingPath)
	if err != nil {
		return CopyComputerStorageResponse{}, err
	}
	if request.Operation == "restore" && request.Destination.DiskBytes == request.SourceSize && destinationDigest != sourceDigest {
		return CopyComputerStorageResponse{}, errors.New("Computer restore destination digest mismatch")
	}
	postMutationSourceDigest, err := digestStorageCopySource(ctx, sourcePath, sourceHandle)
	if err != nil {
		return CopyComputerStorageResponse{}, err
	}
	if postMutationSourceDigest != sourceDigest {
		return CopyComputerStorageResponse{}, errors.New("Computer Storage copy source changed during destination mutation")
	}
	manifest.SourceUnchanged = true
	manifest.DestinationDigest = destinationDigest
	manifest.Phase = computerStorageCopyManifestWritten
	if err := writeComputerStorageCopyManifest(destinationRoot, manifest); err != nil {
		return CopyComputerStorageResponse{}, err
	}
	if err := engine.storageCopyCheckpoint(computerStorageCopyManifestWritten); err != nil {
		return CopyComputerStorageResponse{}, err
	}
	if workingPath == stagingPath {
		if err := os.Rename(stagingPath, publishedPath); err != nil {
			return CopyComputerStorageResponse{}, err
		}
	}
	if err := syncDirectory(destinationRoot); err != nil {
		return CopyComputerStorageResponse{}, err
	}
	diskManifest := computerDiskManifest{Version: computerDiskManifestVersion, Storage: request.Destination,
		DiskImage: "disk.ext4", MountDirectory: destinationName, Prepared: true}
	if err := writeComputerDiskManifest(destinationRoot, diskManifest); err != nil {
		return CopyComputerStorageResponse{}, err
	}
	if err := engine.storageCopyCheckpoint(computerStorageCopyPublished); err != nil {
		return CopyComputerStorageResponse{}, err
	}
	receiptID, err := randomCapability()
	if err != nil {
		return CopyComputerStorageResponse{}, err
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
		SourceDigest: sourceDigest, DestinationDigest: destinationDigest,
		OSIdentityRekeyed:     manifest.OSIdentityRekeyed,
		MachineIDBeforeDigest: manifest.MachineIDBeforeDigest,
		MachineIDAfterDigest:  manifest.MachineIDAfterDigest,
		MachineIDRepaired:     manifest.MachineIDRepaired,
		SourceUnchanged:       manifest.SourceUnchanged,
		DestinationPrepared:   diskManifest.Prepared,
		PreparationReceipt:    diskManifest.PreparationReceipt != nil,
		DestinationChown:      request.Destination.Chown,
		FilesystemExpanded:    manifest.FilesystemExpanded}
	manifest.Phase, manifest.Receipt = computerStorageCopyPublished, &receipt
	if err := writeComputerStorageCopyManifest(destinationRoot, manifest); err != nil {
		return CopyComputerStorageResponse{}, err
	}
	return CopyComputerStorageResponse{Receipt: receipt}, nil
}
