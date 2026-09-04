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
	"strings"
	"syscall"
	"time"
)

const (
	defaultComputerDiskQuarantineRetention = 24 * time.Hour
	// Recovery is counted once per agent boot-barrier sweep. Twenty-four failed
	// barrier observations is the attempt bound; the independent 24-hour elapsed
	// floor prevents rapid barrier retries from abandoning live tenant bytes.
	defaultComputerStorageRecoveryAttempts = 24
)

var errComputerStoragePreenCorrected = errors.New("Computer Storage preen corrected filesystem errors")

const (
	computerDiskQuarantineKindGeneration = "computer_disk_anomaly_quarantined"
	computerDiskQuarantineKindAuthority  = "computer_disk_authority_quarantined"
)

type computerDiskRecoveryRecordError struct {
	Operation string
	Storage   ComputerStorageReference
	Cause     error
}

type computerStorageRecoveryDeferral struct {
	Attempts        int       `json:"attempts,omitempty"`
	FirstDeferredAt time.Time `json:"first_deferred_at,omitempty"`
	Reason          string    `json:"reason,omitempty"`
}

type computerDiskRecoveryStructuralError struct {
	Reason string
	Cause  error
}

func (err *computerDiskRecoveryStructuralError) Error() string {
	return fmt.Sprintf("%s: %v", err.Reason, err.Cause)
}
func (err *computerDiskRecoveryStructuralError) Unwrap() error { return err.Cause }

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

func recoveryDeferralReason(err error) string {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return "context_expired"
	}
	if errors.Is(err, os.ErrNotExist) {
		return "prerequisite_missing"
	}
	return "operational_failure"
}

func advanceRecoveryDeferral(now time.Time, state computerStorageRecoveryDeferral, reason string) (computerStorageRecoveryDeferral, bool) {
	if state.FirstDeferredAt.IsZero() {
		state.FirstDeferredAt = now.UTC()
	}
	state.Attempts++
	state.Reason = reason
	abandoned := state.Attempts >= defaultComputerStorageRecoveryAttempts && !now.Before(state.FirstDeferredAt.Add(defaultComputerDiskQuarantineRetention))
	return state, abandoned
}

func (engine *ContainerdEngine) deferComputerStorageRecovery(root, operation string, cause error, countAttempt bool) (ComputerStorageReference, computerStorageRecoveryDeferral, bool, error) {
	now := time.Now()
	if engine.config.Clock != nil {
		now = engine.config.Clock.Now()
	}
	reason := recoveryDeferralReason(cause)
	switch operation {
	case "computer_storage_grow":
		intent, present, err := readComputerStorageGrowIntent(root)
		if err != nil || !present {
			return ComputerStorageReference{}, computerStorageRecoveryDeferral{}, false, errors.Join(err, errors.New("deferred grow authority is unavailable"))
		}
		if !countAttempt {
			return intent.Request.Storage, intent.Recovery, false, nil
		}
		intent.Recovery, present = advanceRecoveryDeferral(now, intent.Recovery, reason)
		return intent.Request.Storage, intent.Recovery, present, writeComputerStorageGrowIntentRecord(root, intent)
	case "computer_storage_copy":
		manifest, present, err := readComputerStorageCopyManifest(filepath.Join(root, "storage-copy.json"))
		if err != nil || !present {
			return ComputerStorageReference{}, computerStorageRecoveryDeferral{}, false, errors.Join(err, errors.New("deferred copy authority is unavailable"))
		}
		if !countAttempt {
			return manifest.Request.Destination, manifest.Recovery, false, nil
		}
		manifest.Recovery, present = advanceRecoveryDeferral(now, manifest.Recovery, reason)
		return manifest.Request.Destination, manifest.Recovery, present, writeComputerStorageCopyManifest(root, manifest)
	default:
		return ComputerStorageReference{}, computerStorageRecoveryDeferral{}, false, errors.New("unknown Computer Storage recovery operation")
	}
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
	Kind             string                   `json:"kind"`
	ReceiptID        string                   `json:"receipt_id"`
	DiskName         string                   `json:"disk_name"`
	Storage          ComputerStorageReference `json:"storage"`
	Reason           string                   `json:"reason"`
	CreatedAt        time.Time                `json:"created_at"`
	RetainUntil      time.Time                `json:"retain_until"`
	DeferredReason   string                   `json:"deferred_reason,omitempty"`
	RecoveryAttempts int                      `json:"recovery_attempts,omitempty"`
	FirstDeferredAt  time.Time                `json:"first_deferred_at,omitempty"`
	PayloadDroppedAt *time.Time               `json:"payload_dropped_at,omitempty"`
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

func validComputerStorageCopyRecoveryForInventory(name string, manifest computerStorageCopyManifest) bool {
	request := manifest.Request
	expected, err := deterministicComputerDiskName(request.Destination)
	validOperation := request.Operation == "restore" || request.Operation == "clone" || request.Operation == "import"
	return err == nil && expected == name && validOperation && manifest.Version == 1 && manifest.Phase != computerStorageCopyPublished &&
		request.BackupID != "" && request.CopyID != "" && request.SourceSize > 0 && request.SourceDigest != "" &&
		request.Destination.DiskBytes >= request.SourceSize && request.Destination.IntentRevision > 0 &&
		request.Authority.NodeID != "" && request.Authority.BootSessionID != "" && request.Authority.HelperGeneration > 0 &&
		request.Authority.RootInstanceID != "" && request.Authority.JobID != "" && request.Authority.OperationRevision > 0 && request.Authority.CleanupFence != ""
}

func recoveryInventoryEntry(name, operation string, storage ComputerStorageReference, recovery computerStorageRecoveryDeferral) ComputerStorageRecoveryInventoryEntry {
	return ComputerStorageRecoveryInventoryEntry{Storage: storage, DiskName: name, Operation: operation,
		Reason: recovery.Reason, Attempts: recovery.Attempts, FirstDeferredAt: recovery.FirstDeferredAt}
}

func (engine *ContainerdEngine) resetOperationalComputerRecoveryDeferrals() {
	engine.computerRecoveryMu.Lock()
	engine.computerOperationalDeferred = make(map[string]ComputerStorageRecoveryInventoryEntry)
	engine.computerRecoveryMu.Unlock()
}

func (engine *ContainerdEngine) rememberOperationalComputerRecoveryDeferral(name, operation string, storage ComputerStorageReference, cause error) {
	engine.computerRecoveryMu.Lock()
	defer engine.computerRecoveryMu.Unlock()
	if engine.computerOperationalDeferred == nil {
		engine.computerOperationalDeferred = make(map[string]ComputerStorageRecoveryInventoryEntry)
	}
	engine.computerOperationalDeferred[name] = ComputerStorageRecoveryInventoryEntry{
		Storage: storage, DiskName: name, Operation: operation, Reason: recoveryDeferralReason(cause),
	}
}

func (engine *ContainerdEngine) operationalComputerRecoveryDeferral(name string) (ComputerStorageRecoveryInventoryEntry, bool) {
	engine.computerRecoveryMu.Lock()
	defer engine.computerRecoveryMu.Unlock()
	entry, present := engine.computerOperationalDeferred[name]
	return entry, present
}

type computerRecoveryFileFailure uint8

const (
	computerRecoveryFileInvalid computerRecoveryFileFailure = iota
	computerRecoveryFileMissing
	computerRecoveryFileOperational
)

type computerRecoveryRecordNotRegularError struct{ path string }

func (err *computerRecoveryRecordNotRegularError) Error() string {
	return fmt.Sprintf("Computer recovery record %q is not a regular file", err.path)
}

func readComputerRecoveryRecord(path string) ([]byte, bool, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if !info.Mode().IsRegular() {
		return nil, false, &computerRecoveryRecordNotRegularError{path: path}
	}
	payload, err := os.ReadFile(path)
	if err != nil {
		return nil, false, err
	}
	return payload, true, nil
}

// classifyComputerRecoveryFileFailure keeps successful reads with invalid
// content structural, while read/stat failures remain retryable. ENOENT and
// ENOTDIR are positive structural absence rather than transient I/O.
func classifyComputerRecoveryFileFailure(err error) computerRecoveryFileFailure {
	var notRegular *computerRecoveryRecordNotRegularError
	if errors.As(err, &notRegular) {
		return computerRecoveryFileInvalid
	}
	if errors.Is(err, syscall.ENOENT) || errors.Is(err, syscall.ENOTDIR) {
		return computerRecoveryFileMissing
	}
	var pathErr *os.PathError
	if errors.As(err, &pathErr) {
		return computerRecoveryFileOperational
	}
	return computerRecoveryFileInvalid
}

func computerRecoveryStructuralReason(err error, fallback string) string {
	var notRegular *computerRecoveryRecordNotRegularError
	if errors.As(err, &notRegular) {
		return "record_not_regular"
	}
	return fallback
}

func (engine *ContainerdEngine) resumeComputerStorageGrow(ctx context.Context, root, name string, manifest *computerDiskManifest) (bool, error) {
	intent, present, err := readComputerStorageGrowIntent(root)
	if err != nil {
		if classifyComputerRecoveryFileFailure(err) == computerRecoveryFileOperational {
			return false, err
		}
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
		if errors.Is(err, os.ErrNotExist) {
			return false, &computerDiskRecoveryStructuralError{Reason: "image_missing", Cause: err}
		}
		return false, err
	}
	if !info.Mode().IsRegular() {
		return false, &computerDiskRecoveryStructuralError{Reason: "image_not_regular", Cause: errors.New("Computer Storage grow image is not regular")}
	}
	if manifest.Storage.DiskBytes == request.NewDiskBytes {
		if err := verifyComputerDiskAllocation(imagePath, request.NewDiskBytes); err != nil {
			if classifyComputerRecoveryFileFailure(err) == computerRecoveryFileOperational {
				return false, err
			}
			return false, &computerDiskRecoveryStructuralError{Reason: "allocation_mismatch", Cause: err}
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
		return false, &computerDiskRecoveryStructuralError{Reason: "allocation_mismatch", Cause: errors.New("Computer Storage grow image conflicts with its durable target")}
	}
	preen := engine.computerGrowPreen
	if preen == nil {
		preen = preenComputerStorage
	}
	if err := preen(ctx, imagePath); err != nil {
		if errors.Is(err, errComputerStoragePreenCorrected) {
			engine.computerDiskSweepEvidence = append(engine.computerDiskSweepEvidence, SweepEvidence{Class: RemovalResourceComputerDiskManifest,
				ID: name, Action: SweepActionPreenCorrected, Method: "e2fsck_exit_1"})
		} else {
			return false, err
		}
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
		if classifyComputerRecoveryFileFailure(err) == computerRecoveryFileOperational {
			return "", err
		}
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
	switch copyManifest.Phase {
	case computerStorageCopyReserved, computerStorageCopyAllocated, computerStorageCopyCopied,
		computerStorageCopySourceVerified, computerStorageCopyMountedRekey, computerStorageCopyIdentityRekeyed,
		computerStorageCopyExpanded:
		if err := ctx.Err(); err != nil {
			return "", err
		}
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
			if errors.Is(stagingErr, os.ErrNotExist) {
				return "", &computerDiskRecoveryStructuralError{Reason: "image_missing", Cause: stagingErr}
			}
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
		if classifyComputerRecoveryFileFailure(err) == computerRecoveryFileOperational {
			return "", err
		}
		return "", &computerDiskRecoveryStructuralError{Reason: "allocation_mismatch", Cause: err}
	}
	digestFileRecovery := engine.computerRecoveryDigest
	if digestFileRecovery == nil {
		digestFileRecovery = digestFileContext
	}
	digest, err := digestFileRecovery(ctx, publishedPath)
	if err != nil {
		return "", err
	}
	if digest != copyManifest.DestinationDigest {
		return "", &computerDiskRecoveryStructuralError{Reason: "digest_mismatch", Cause: errors.New("Computer Storage copy recovery digest mismatch")}
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

func digestFileContext(ctx context.Context, path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	digest := sha256.New()
	buffer := make([]byte, 256*1024)
	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		read, readErr := file.Read(buffer)
		if read > 0 {
			if _, err := digest.Write(buffer[:read]); err != nil {
				return "", err
			}
		}
		if errors.Is(readErr, io.EOF) {
			return "sha256:" + hex.EncodeToString(digest.Sum(nil)), nil
		}
		if readErr != nil {
			return "", readErr
		}
	}
}

func preenComputerStorage(ctx context.Context, imagePath string) error {
	tool, err := findRootTool("e2fsck")
	if err != nil {
		return err
	}
	output, err := exec.CommandContext(ctx, tool, "-f", "-p", imagePath).CombinedOutput()
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 {
		return errComputerStoragePreenCorrected
	}
	if err != nil {
		return fmt.Errorf("preen interrupted Computer Storage filesystem: %w: %s", err, output)
	}
	return nil
}

func (engine *ContainerdEngine) quarantineComputerDiskAnomaly(root, name string, storage ComputerStorageReference, reason string) error {
	return engine.quarantineComputerDiskAnomalyWithDeferral(root, name, storage, reason, computerStorageRecoveryDeferral{})
}

func (engine *ContainerdEngine) quarantineComputerDiskAnomalyWithDeferral(root, name string, storage ComputerStorageReference, reason string, deferral computerStorageRecoveryDeferral) error {
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
	receipt := computerDiskQuarantineReceipt{Kind: computerDiskQuarantineKindGeneration, ReceiptID: receiptID,
		DiskName: name, Storage: storage, Reason: reason, CreatedAt: now.UTC(), RetainUntil: now.Add(defaultComputerDiskQuarantineRetention).UTC(),
		DeferredReason: deferral.Reason, RecoveryAttempts: deferral.Attempts, FirstDeferredAt: deferral.FirstDeferredAt}
	return engine.quarantineComputerDisk(root, name, receipt)
}

func (engine *ContainerdEngine) resolveComputerStorageRecoveryFailure(root, name, operation string, fallbackStorage ComputerStorageReference, recoveryErr error, countAttempt bool) (SweepEvidence, error) {
	reason := map[string]string{"computer_storage_copy": "copy_recovery_authority_invalid", "computer_storage_grow": "grow_recovery_authority_invalid"}[operation]
	if reason == "" {
		reason = "recovery_authority_invalid"
	}
	reason = computerRecoveryStructuralReason(recoveryErr, reason)
	storage := fallbackStorage
	var recordErr *computerDiskRecoveryRecordError
	var structuralErr *computerDiskRecoveryStructuralError
	if errors.As(recoveryErr, &recordErr) {
		if candidate := recordErrStorage(recordErr); candidate.ComputerID != "" {
			storage = candidate
		}
	} else if errors.As(recoveryErr, &structuralErr) {
		reason = structuralErr.Reason
	} else {
		var deferral computerStorageRecoveryDeferral
		var abandoned bool
		var err error
		// A read/stat failure can be retried but cannot safely rewrite the record
		// that could not be read. Retain it and emit typed deferral evidence.
		if classifyComputerRecoveryFileFailure(recoveryErr) == computerRecoveryFileOperational {
			engine.rememberOperationalComputerRecoveryDeferral(name, operation, storage, recoveryErr)
			return SweepEvidence{Class: RemovalResourceComputerDiskManifest, ID: name, Action: SweepActionResumeDeferred,
				Method: operation}, nil
		}
		storage, deferral, abandoned, err = engine.deferComputerStorageRecovery(root, operation, recoveryErr, countAttempt)
		if err != nil {
			return SweepEvidence{}, errors.Join(recoveryErr, err)
		}
		if !abandoned {
			return SweepEvidence{Class: RemovalResourceComputerDiskManifest, ID: name, Action: SweepActionResumeDeferred,
				Method: operation}, nil
		}
		reason = "resume_abandoned"
		if err := engine.quarantineComputerDiskAnomalyWithDeferral(root, name, storage, reason, deferral); err != nil {
			return SweepEvidence{}, errors.Join(recoveryErr, err)
		}
		return SweepEvidence{Class: RemovalResourceComputerQuarantine, ID: name, Action: SweepActionQuarantined,
			Method: reason + ":" + deferral.Reason}, nil
	}
	if storage.ComputerID == "" && operation == "computer_storage_copy" {
		if manifest, present, err := readComputerStorageCopyManifest(filepath.Join(root, "storage-copy.json")); err == nil && present {
			storage = manifest.Request.Destination
		}
	}
	if storage.ComputerID == "" {
		if err := engine.quarantineComputerDiskAuthorityFailure(root, name, reason); err != nil {
			return SweepEvidence{}, errors.Join(recoveryErr, err)
		}
		return SweepEvidence{Class: RemovalResourceComputerQuarantine, ID: name, Action: SweepActionQuarantined, Method: reason}, nil
	}
	expected, identityErr := deterministicComputerDiskName(storage)
	if identityErr != nil || expected != name {
		if err := engine.quarantineComputerDiskAuthorityFailure(root, name, "identity_mismatch"); err != nil {
			return SweepEvidence{}, errors.Join(recoveryErr, identityErr, err)
		}
		return SweepEvidence{Class: RemovalResourceComputerQuarantine, ID: name, Action: SweepActionQuarantined, Method: "identity_mismatch"}, nil
	}
	if err := engine.quarantineComputerDiskAnomaly(root, name, storage, reason); err != nil {
		return SweepEvidence{}, errors.Join(recoveryErr, err)
	}
	return SweepEvidence{Class: RemovalResourceComputerQuarantine, ID: name, Action: SweepActionQuarantined, Method: reason}, nil
}

// quarantineComputerDiskAuthorityFailure moves bytes whose durable record can
// no longer prove a Storage identity into helper-authored custody. The receipt
// deliberately carries no generation authority; it proves only the exact
// directory the helper removed from the active namespace and why.
func (engine *ContainerdEngine) quarantineComputerDiskAuthorityFailure(root, name, reason string) error {
	if !validComputerDiskAuthorityFailureReason(reason) || !validComputerDiskDirectoryName(name) {
		return errors.New("Computer disk authority-failure quarantine is invalid")
	}
	receiptID, err := randomCapability()
	if err != nil {
		return err
	}
	now := time.Now()
	if engine.config.Clock != nil {
		now = engine.config.Clock.Now()
	}
	receipt := computerDiskQuarantineReceipt{Kind: computerDiskQuarantineKindAuthority, ReceiptID: receiptID,
		DiskName: name, Reason: reason, CreatedAt: now.UTC(), RetainUntil: now.Add(defaultComputerDiskQuarantineRetention).UTC()}
	return engine.quarantineComputerDisk(root, name, receipt)
}

func (engine *ContainerdEngine) quarantineComputerDisk(root, name string, receipt computerDiskQuarantineReceipt) error {
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
	destination := filepath.Join(quarantineRoot, fmt.Sprintf("%s-anomaly-%s", name, receipt.ReceiptID))
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

func validComputerDiskDirectoryName(name string) bool {
	return strings.HasPrefix(name, "wefty-computer-disk-") && filepath.Base(name) == name && name != "wefty-computer-disk-"
}

func validComputerDiskAuthorityFailureReason(reason string) bool {
	return reason == "manifest_invalid" || reason == "quarantine_authority_invalid" ||
		reason == "copy_recovery_authority_invalid" || reason == "grow_recovery_authority_invalid" ||
		reason == "identity_mismatch" || reason == "record_not_regular"
}

func validateComputerDiskQuarantineReceipt(receipt computerDiskQuarantineReceipt) error {
	if receipt.ReceiptID == "" || receipt.Reason == "" || !validComputerDiskDirectoryName(receipt.DiskName) ||
		receipt.CreatedAt.IsZero() || receipt.RetainUntil.IsZero() || !receipt.RetainUntil.After(receipt.CreatedAt) {
		return errors.New("Computer disk quarantine recovery record is invalid")
	}
	switch receipt.Kind {
	case computerDiskQuarantineKindGeneration:
		expected, err := deterministicComputerDiskName(receipt.Storage)
		if err != nil || expected != receipt.DiskName {
			return errors.Join(err, errors.New("Computer disk quarantine recovery identity is invalid"))
		}
		if receipt.Reason == "resume_abandoned" && (receipt.DeferredReason == "" || receipt.RecoveryAttempts < 1 || receipt.FirstDeferredAt.IsZero() || receipt.FirstDeferredAt.After(receipt.CreatedAt)) {
			return errors.New("abandoned Computer Storage recovery evidence is incomplete")
		}
	case computerDiskQuarantineKindAuthority:
		if !validComputerDiskAuthorityFailureReason(receipt.Reason) || receipt.Storage != (ComputerStorageReference{}) {
			return errors.New("Computer disk authority-failure quarantine receipt is invalid")
		}
	default:
		return errors.New("Computer disk quarantine recovery kind is invalid")
	}
	return nil
}

func readAndValidateComputerDiskQuarantineReceipt(path string) (computerDiskQuarantineReceipt, error) {
	payload, err := os.ReadFile(path)
	if err != nil {
		return computerDiskQuarantineReceipt{}, err
	}
	var receipt computerDiskQuarantineReceipt
	if json.Unmarshal(payload, &receipt) != nil {
		return computerDiskQuarantineReceipt{}, errors.New("Computer disk quarantine recovery record is invalid")
	}
	return receipt, validateComputerDiskQuarantineReceipt(receipt)
}

func (engine *ContainerdEngine) resumeComputerDiskQuarantine(root, name string) (bool, error) {
	receipt, err := readAndValidateComputerDiskQuarantineReceipt(filepath.Join(root, "quarantine.json"))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if receipt.DiskName != name {
		return false, errors.New("Computer disk quarantine recovery directory is invalid")
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

func (engine *ContainerdEngine) expireComputerDiskQuarantinePayloads(ctx context.Context) error {
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
		lock, lockErr := openComputerDiskLock(root)
		if lockErr != nil {
			engine.computerDiskSweepEvidence = append(engine.computerDiskSweepEvidence, SweepEvidence{Class: RemovalResourceComputerQuarantine, ID: entry.Name(), Action: SweepActionQuarantineGCFailed, Method: "generation_lock"})
			continue
		}
		receipt, readErr := readAndValidateComputerDiskQuarantineReceipt(filepath.Join(root, "quarantine.json"))
		if readErr != nil || entry.Name() != receipt.DiskName+"-anomaly-"+receipt.ReceiptID {
			engine.computerDiskSweepEvidence = append(engine.computerDiskSweepEvidence, SweepEvidence{
				Class: RemovalResourceComputerQuarantine, ID: entry.Name(), Action: SweepActionRetained, Method: "quarantine_authority_invalid",
			})
			closeComputerDiskLock(lock)
			continue
		}
		if now.Before(receipt.RetainUntil) || receipt.PayloadDroppedAt != nil {
			closeComputerDiskLock(lock)
			continue
		}
		if err := ctx.Err(); err != nil {
			closeComputerDiskLock(lock)
			return err
		}
		children, readErr := os.ReadDir(root)
		if readErr != nil {
			engine.computerDiskSweepEvidence = append(engine.computerDiskSweepEvidence, SweepEvidence{Class: RemovalResourceComputerQuarantine, ID: receipt.DiskName, Action: SweepActionQuarantineGCFailed, Method: "read_payload"})
			closeComputerDiskLock(lock)
			continue
		}
		removeAll := engine.computerQuarantineRemoveAll
		if removeAll == nil {
			removeAll = os.RemoveAll
		}
		failed := false
		for _, child := range children {
			if child.Name() == "quarantine.json" || child.Name() == "attachment.lock" {
				continue
			}
			if err := removeAll(filepath.Join(root, child.Name())); err != nil {
				engine.computerDiskSweepEvidence = append(engine.computerDiskSweepEvidence, SweepEvidence{Class: RemovalResourceComputerQuarantine, ID: receipt.DiskName, Action: SweepActionQuarantineGCFailed, Method: "remove_payload"})
				failed = true
				break
			}
		}
		if failed {
			closeComputerDiskLock(lock)
			continue
		}
		droppedAt := now.UTC()
		receipt.PayloadDroppedAt = &droppedAt
		payload, marshalErr := json.Marshal(receipt)
		writeErr := marshalErr
		if writeErr == nil {
			writeErr = writeDurableFile(root, ".quarantine.json.tmp-", "quarantine.json", payload, 0o600)
		}
		if writeErr != nil {
			engine.computerDiskSweepEvidence = append(engine.computerDiskSweepEvidence, SweepEvidence{Class: RemovalResourceComputerQuarantine, ID: receipt.DiskName, Action: SweepActionQuarantineGCFailed, Method: "record_payload_drop"})
			closeComputerDiskLock(lock)
			continue
		}
		engine.computerDiskSweepEvidence = append(engine.computerDiskSweepEvidence, SweepEvidence{Class: RemovalResourceComputerQuarantine, ID: receipt.DiskName, Action: SweepActionQuarantinePayloadDropped, Method: "remove_payload"})
		closeComputerDiskLock(lock)
	}
	return nil
}
