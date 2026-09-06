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
	"time"
)

const (
	defaultComputerDiskQuarantineRetention = 24 * time.Hour
	// Recovery is counted once per agent boot-barrier sweep. Twenty-four failed
	// barrier observations is the attempt bound; the independent 24-hour elapsed
	// floor prevents rapid barrier retries from abandoning live tenant bytes.
	defaultComputerStorageRecoveryAttempts  = 24
	defaultComputerDiskQuarantineGCFailures = 3
	// When neither GC evidence location is writable, an observed in-memory
	// failure is bounded by an absolute window derived from the durable
	// retention deadline.
	defaultComputerDiskQuarantineGCUnrecordedRetryWindow = 24 * time.Hour
	computerOperationalDeferralRecordName                = "recovery-deferral.json"
	computerOperationalDeferralFaultSuffix               = "-recovery-deferral-fault.json"
	computerDiskQuarantineGCFailureSuffix                = "-quarantine-gc-failure.json"
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

type computerOperationalRecoveryDeferral struct {
	Version   int                             `json:"version"`
	DiskName  string                          `json:"disk_name"`
	Operation string                          `json:"operation"`
	Storage   ComputerStorageReference        `json:"storage"`
	Recovery  computerStorageRecoveryDeferral `json:"recovery"`
}

func (engine *ContainerdEngine) lstatComputerDisk(path string) (os.FileInfo, error) {
	if engine.computerLstat != nil {
		return engine.computerLstat(path)
	}
	return os.Lstat(path)
}

func (engine *ContainerdEngine) readComputerDiskDirectory(path string) ([]os.DirEntry, error) {
	if engine.computerReadDir != nil {
		return engine.computerReadDir(path)
	}
	return os.ReadDir(path)
}

type computerDiskRecoveryStructuralError struct {
	Reason string
	Cause  error
}

func (err *computerDiskRecoveryStructuralError) Error() string {
	return fmt.Sprintf("%s: %v", err.Reason, err.Cause)
}
func (err *computerDiskRecoveryStructuralError) Unwrap() error { return err.Cause }

func (engine *ContainerdEngine) computerStorageResumePending(root, name string) (bool, error) {
	if _, present, err := readOperationalComputerRecoveryDeferral(root); err != nil {
		return false, err
	} else if present {
		return true, nil
	}
	if _, present, err := readOperationalComputerRecoveryDeferralFault(root, name); err != nil {
		return false, err
	} else if present {
		return true, nil
	}
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

func readOperationalComputerRecoveryDeferral(root string) (computerOperationalRecoveryDeferral, bool, error) {
	payload, present, err := readComputerRecoveryRecord(filepath.Join(root, computerOperationalDeferralRecordName))
	if err != nil || !present {
		return computerOperationalRecoveryDeferral{}, present, err
	}
	var record computerOperationalRecoveryDeferral
	if json.Unmarshal(payload, &record) != nil || record.Version != 1 || !validComputerDiskDirectoryName(record.DiskName) ||
		record.Operation == "" || record.Recovery.Attempts < 1 || record.Recovery.FirstDeferredAt.IsZero() || record.Recovery.Reason == "" {
		return computerOperationalRecoveryDeferral{}, false, errors.New("Computer operational recovery deferral record is invalid")
	}
	if record.Storage.ComputerID != "" {
		expected, identityErr := deterministicComputerDiskName(record.Storage)
		if identityErr != nil || expected != record.DiskName {
			return computerOperationalRecoveryDeferral{}, false, errors.New("Computer operational recovery deferral identity is invalid")
		}
	}
	return record, true, nil
}

func operationalComputerRecoveryDeferralFaultPath(root, name string) string {
	return filepath.Join(filepath.Dir(root), "."+name+computerOperationalDeferralFaultSuffix)
}

func readOperationalComputerRecoveryDeferralFault(root, name string) (computerOperationalRecoveryDeferral, bool, error) {
	return readOperationalComputerRecoveryDeferralAt(operationalComputerRecoveryDeferralFaultPath(root, name), name)
}

func readOperationalComputerRecoveryDeferralAt(path, name string) (computerOperationalRecoveryDeferral, bool, error) {
	payload, present, err := readComputerRecoveryRecord(path)
	if err != nil || !present {
		return computerOperationalRecoveryDeferral{}, present, err
	}
	var record computerOperationalRecoveryDeferral
	if json.Unmarshal(payload, &record) != nil || record.Version != 1 || record.DiskName != name || record.Operation == "" ||
		record.Recovery.Attempts < 1 || record.Recovery.FirstDeferredAt.IsZero() || record.Recovery.Reason == "" {
		return computerOperationalRecoveryDeferral{}, false, errors.New("Computer operational recovery fault deferral record is invalid")
	}
	if record.Storage.ComputerID != "" {
		expected, identityErr := deterministicComputerDiskName(record.Storage)
		if identityErr != nil || expected != record.DiskName {
			return computerOperationalRecoveryDeferral{}, false, errors.New("Computer operational recovery fault deferral identity is invalid")
		}
	}
	return record, true, nil
}

func mergeOperationalComputerRecoveryDeferrals(primary computerOperationalRecoveryDeferral, primaryPresent bool, fallback computerOperationalRecoveryDeferral, fallbackPresent bool) (computerOperationalRecoveryDeferral, bool) {
	if !primaryPresent {
		return fallback, fallbackPresent
	}
	if fallbackPresent && fallback.Recovery.Attempts > primary.Recovery.Attempts {
		return fallback, true
	}
	return primary, true
}

func (engine *ContainerdEngine) inspectOperationalComputerRecoveryDeferral(root, name string) (computerOperationalRecoveryDeferral, bool, error) {
	_, rootErr := engine.lstatComputerDisk(root)
	primary, primaryPresent, primaryErr := readOperationalComputerRecoveryDeferral(root)
	if primaryErr == nil && primaryPresent && primary.DiskName != name {
		primary = computerOperationalRecoveryDeferral{}
		primaryPresent = false
		primaryErr = &computerDiskRecoveryStructuralError{
			Reason: "deferral_record_identity_mismatch",
			Cause:  errors.New("Computer operational recovery deferral belongs to another disk directory"),
		}
	}
	fallback, fallbackPresent, fallbackErr := readOperationalComputerRecoveryDeferralFault(root, name)
	if primaryErr == nil && primaryPresent && (fallbackErr != nil || !fallbackPresent) {
		return primary, true, nil
	}
	if fallbackErr == nil && fallbackPresent && (primaryErr != nil || !primaryPresent) {
		return fallback, true, nil
	}
	if primaryErr == nil && fallbackErr == nil {
		record, present := mergeOperationalComputerRecoveryDeferrals(primary, primaryPresent, fallback, fallbackPresent)
		if present || rootErr == nil {
			return record, present, nil
		}
	}
	return computerOperationalRecoveryDeferral{}, false, errors.Join(rootErr, primaryErr, fallbackErr)
}

func (engine *ContainerdEngine) deferOperationalComputerRecovery(root, name, operation string, storage ComputerStorageReference, cause error, countAttempt bool) (computerStorageRecoveryDeferral, bool, error) {
	record, present, err := engine.inspectOperationalComputerRecoveryDeferral(root, name)
	if err != nil {
		return engine.deferOperationalComputerRecoveryFault(root, name, operation, storage, cause, countAttempt, "deferral_record_unreadable")
	}
	if present && (record.DiskName != name ||
		record.Storage.ComputerID != "" && storage.ComputerID != "" && !sameComputerStorageIdentity(record.Storage, storage)) {
		return computerStorageRecoveryDeferral{}, false, errors.New("Computer operational recovery deferral conflicts with current recovery")
	}
	if !present {
		record = computerOperationalRecoveryDeferral{Version: 1, DiskName: name, Operation: operation, Storage: storage}
	} else {
		record.Operation = operation
		if record.Storage.ComputerID == "" && storage.ComputerID != "" {
			record.Storage = storage
		}
	}
	if !countAttempt {
		return record.Recovery, false, nil
	}
	now := time.Now()
	if engine.config.Clock != nil {
		now = engine.config.Clock.Now()
	}
	record.Recovery, present = advanceRecoveryDeferral(now, record.Recovery, recoveryDeferralReason(cause))
	payload, err := json.Marshal(record)
	if err != nil {
		return record.Recovery, present, err
	}
	err = writeDurableFile(root, ".recovery-deferral.json.tmp-", computerOperationalDeferralRecordName, payload, 0o600)
	parent := filepath.Dir(root)
	if err != nil {
		fallbackErr := writeDurableFile(parent, ".recovery-deferral-fault.tmp-", filepath.Base(operationalComputerRecoveryDeferralFaultPath(root, name)), payload, 0o600)
		return record.Recovery, present, fallbackErr
	}
	_ = writeDurableFile(parent, ".recovery-deferral-fault.tmp-", filepath.Base(operationalComputerRecoveryDeferralFaultPath(root, name)), payload, 0o600)
	return record.Recovery, present, nil
}

func (engine *ContainerdEngine) deferOperationalComputerRecoveryFault(root, name, operation string, storage ComputerStorageReference, cause error, countAttempt bool, reason string) (computerStorageRecoveryDeferral, bool, error) {
	record, present, err := readOperationalComputerRecoveryDeferralFault(root, name)
	if err != nil {
		return computerStorageRecoveryDeferral{}, false, err
	}
	if present && record.Storage.ComputerID != "" && storage.ComputerID != "" && !sameComputerStorageIdentity(record.Storage, storage) {
		return computerStorageRecoveryDeferral{}, false, errors.New("Computer operational recovery fault deferral conflicts with current recovery")
	}
	if !present {
		record = computerOperationalRecoveryDeferral{Version: 1, DiskName: name, Operation: operation, Storage: storage}
	} else {
		record.Operation = operation
		if record.Storage.ComputerID == "" && storage.ComputerID != "" {
			record.Storage = storage
		}
	}
	if !countAttempt {
		return record.Recovery, false, nil
	}
	now := time.Now()
	if engine.config.Clock != nil {
		now = engine.config.Clock.Now()
	}
	if reason == "" {
		reason = recoveryDeferralReason(cause)
	}
	record.Recovery, present = advanceRecoveryDeferral(now, record.Recovery, reason)
	payload, err := json.Marshal(record)
	if err == nil {
		parent := filepath.Dir(root)
		err = writeDurableFile(parent, ".recovery-deferral-fault.tmp-", filepath.Base(operationalComputerRecoveryDeferralFaultPath(root, name)), payload, 0o600)
	}
	return record.Recovery, present, err
}

func removeOperationalComputerRecoveryDeferralFault(root, name string) error {
	path := operationalComputerRecoveryDeferralFaultPath(root, name)
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return syncDirectory(filepath.Dir(root))
}

func clearAllOperationalComputerRecoveryDeferrals(root, name string) error {
	removedPrimary := false
	if err := os.Remove(filepath.Join(root, computerOperationalDeferralRecordName)); err == nil {
		removedPrimary = true
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if removedPrimary {
		if err := syncDirectory(root); err != nil {
			return err
		}
	}
	return removeOperationalComputerRecoveryDeferralFault(root, name)
}

func (engine *ContainerdEngine) clearOperationalComputerRecoveryDeferral(root, name, operation string) error {
	if _, err := engine.lstatComputerDisk(root); err != nil {
		return err
	}
	record, present, err := readOperationalComputerRecoveryDeferral(root)
	if err != nil {
		return err
	}
	removedPrimary := false
	if present && record.Operation == operation {
		if err := os.Remove(filepath.Join(root, computerOperationalDeferralRecordName)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		removedPrimary = true
	}
	fallback, fallbackPresent, err := readOperationalComputerRecoveryDeferralFault(root, name)
	if err != nil {
		return err
	}
	removedFallback := false
	if fallbackPresent && fallback.Operation == operation {
		if err := os.Remove(operationalComputerRecoveryDeferralFaultPath(root, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		removedFallback = true
	}
	if removedPrimary {
		if err := syncDirectory(root); err != nil {
			return err
		}
	}
	if removedFallback {
		return syncDirectory(filepath.Dir(root))
	}
	return nil
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
	GCFailures       int                      `json:"gc_failures,omitempty"`
	GCFirstFailedAt  *time.Time               `json:"gc_first_failed_at,omitempty"`
	GCEscalatedAt    *time.Time               `json:"gc_escalated_at,omitempty"`
	GCLastFailure    string                   `json:"gc_last_failure,omitempty"`
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

func (engine *ContainerdEngine) quarantineRecoveryInventoryEntry(root, name string, receipt computerDiskQuarantineReceipt) ComputerStorageRecoveryInventoryEntry {
	receipt, evidenceStorage := engine.mergeComputerDiskQuarantineGC(root, receipt)
	recovery := recoveryInventoryEntry(name, "quarantine", receipt.Storage, computerStorageRecoveryDeferral{
		Attempts: receipt.RecoveryAttempts, Reason: receipt.Reason, FirstDeferredAt: receipt.FirstDeferredAt,
	})
	recovery.DeferredReason = receipt.DeferredReason
	recovery.GCFailures = receipt.GCFailures
	recovery.GCLastFailure = receipt.GCLastFailure
	recovery.GCEvidenceStorage = evidenceStorage
	if receipt.PayloadDroppedAt != nil {
		droppedAt := receipt.PayloadDroppedAt.UTC()
		recovery.PayloadDroppedAt = &droppedAt
	}
	if receipt.GCFirstFailedAt != nil {
		recovery.GCFirstFailedAt = receipt.GCFirstFailedAt.UTC()
	}
	if receipt.GCEscalatedAt != nil {
		recovery.GCEscalatedAt = receipt.GCEscalatedAt.UTC()
	}
	now := time.Now()
	if engine.config.Clock != nil {
		now = engine.config.Clock.Now()
	}
	if reason, stoppedAt, stopped := computerDiskQuarantineGCStop(receipt, evidenceStorage, now); stopped {
		recovery.GCRetryStopReason = reason
		recovery.GCRetryStoppedAt = stoppedAt
	}
	return recovery
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

func (engine *ContainerdEngine) resolveOperationalComputerRecoveryFailure(root, name, operation string, storage ComputerStorageReference, cause error, countAttempt bool) SweepEvidence {
	deferral, abandoned, err := engine.deferOperationalComputerRecovery(root, name, operation, storage, cause, countAttempt)
	return engine.resolveOperationalComputerRecoveryDeferral(root, name, operation, storage, cause, deferral, abandoned, err)
}

func (engine *ContainerdEngine) resolveOperationalComputerRecoveryFault(root, name, operation string, storage ComputerStorageReference, cause error, countAttempt bool, reason string) SweepEvidence {
	deferral, abandoned, err := engine.deferOperationalComputerRecoveryFault(root, name, operation, storage, cause, countAttempt, reason)
	return engine.resolveOperationalComputerRecoveryDeferral(root, name, operation, storage, cause, deferral, abandoned, err)
}

func (engine *ContainerdEngine) resolveOperationalComputerRecoveryDeferral(root, name, operation string, storage ComputerStorageReference, cause error, deferral computerStorageRecoveryDeferral, abandoned bool, err error) SweepEvidence {
	if err != nil {
		engine.rememberOperationalComputerRecoveryDeferral(name, operation, storage, err)
		return SweepEvidence{Class: RemovalResourceComputerDiskManifest, ID: name, Action: SweepActionResumeDeferred,
			Method: operation + ":deferral_persistence_failed"}
	}
	if !abandoned {
		engine.computerRecoveryMu.Lock()
		if engine.computerOperationalDeferred == nil {
			engine.computerOperationalDeferred = make(map[string]ComputerStorageRecoveryInventoryEntry)
		}
		engine.computerOperationalDeferred[name] = recoveryInventoryEntry(name, operation, storage, deferral)
		engine.computerRecoveryMu.Unlock()
		return SweepEvidence{Class: RemovalResourceComputerDiskManifest, ID: name, Action: SweepActionResumeDeferred, Method: operation}
	}
	quarantineErr := error(nil)
	if expected, identityErr := deterministicComputerDiskName(storage); identityErr == nil && expected == name {
		quarantineErr = engine.quarantineComputerDiskAnomalyWithDeferral(root, name, storage, "resume_abandoned", deferral)
	} else {
		quarantineErr = engine.quarantineComputerDiskAuthorityFailureWithDeferral(root, name, "resume_abandoned", deferral)
	}
	if quarantineErr != nil {
		engine.rememberOperationalComputerRecoveryDeferral(name, operation, storage, quarantineErr)
		return SweepEvidence{Class: RemovalResourceComputerDiskManifest, ID: name, Action: SweepActionResumeDeferred,
			Method: operation + ":quarantine_failed"}
	}
	_ = removeOperationalComputerRecoveryDeferralFault(root, name)
	return SweepEvidence{Class: RemovalResourceComputerQuarantine, ID: name, Action: SweepActionQuarantined,
		Method: "resume_abandoned:" + deferral.Reason}
}

func (engine *ContainerdEngine) resolveOperationalDeferralRecordFailure(root, name, operation string, storage ComputerStorageReference, cause error, countAttempt bool) SweepEvidence {
	if classifyComputerRecoveryFileFailure(cause) == computerRecoveryFileOperational {
		return engine.resolveOperationalComputerRecoveryFault(root, name, operation, storage, cause, countAttempt, "deferral_record_unreadable")
	}
	if err := engine.quarantineComputerDiskAuthorityFailure(root, name, "recovery_deferral_invalid"); err != nil {
		engine.rememberOperationalComputerRecoveryDeferral(name, operation, storage, errors.Join(cause, err))
		return SweepEvidence{Class: RemovalResourceComputerDiskManifest, ID: name, Action: SweepActionResumeDeferred,
			Method: operation + ":quarantine_failed"}
	}
	return SweepEvidence{Class: RemovalResourceComputerQuarantine, ID: name, Action: SweepActionQuarantined,
		Method: "recovery_deferral_invalid"}
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
		if _, err := os.Lstat(filepath.Join(root, "attachment.json")); err == nil {
			return "", &computerDiskRecoveryRecordError{Operation: "computer_storage_copy", Storage: request.Destination,
				Cause: errors.New("pre-publication Computer Storage copy conflicts with a published attachment")}
		} else if !errors.Is(err, os.ErrNotExist) {
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
		digestFileRecovery = digestFile
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

func (engine *ContainerdEngine) quarantineComputerDiskIdentityMismatch(root, name string, storage ComputerStorageReference) error {
	expected, err := deterministicComputerDiskName(storage)
	if err != nil || expected != name {
		return engine.quarantineComputerDiskAuthorityFailure(root, name, "identity_mismatch")
	}
	return engine.quarantineComputerDiskAnomaly(root, name, storage, "identity_mismatch")
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
		// that could not be read. Retain it under a separate durable deferral
		// record so helper replacement cannot reset the abandonment bound.
		if classifyComputerRecoveryFileFailure(recoveryErr) == computerRecoveryFileOperational {
			return engine.resolveOperationalComputerRecoveryFailure(root, name, operation, storage, recoveryErr, countAttempt), nil
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
	return engine.quarantineComputerDiskAuthorityFailureWithDeferral(root, name, reason, computerStorageRecoveryDeferral{})
}

func (engine *ContainerdEngine) quarantineComputerDiskAuthorityFailureWithDeferral(root, name, reason string, deferral computerStorageRecoveryDeferral) error {
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
		DiskName: name, Reason: reason, CreatedAt: now.UTC(), RetainUntil: now.Add(defaultComputerDiskQuarantineRetention).UTC(),
		DeferredReason: deferral.Reason, RecoveryAttempts: deferral.Attempts, FirstDeferredAt: deferral.FirstDeferredAt}
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
	if err := syncDirectory(quarantineRoot); err != nil {
		return err
	}
	return removeOperationalComputerRecoveryDeferralFault(root, name)
}

func validComputerDiskDirectoryName(name string) bool {
	return strings.HasPrefix(name, "wefty-computer-disk-") && filepath.Base(name) == name && name != "wefty-computer-disk-"
}

func validComputerDiskAuthorityFailureReason(reason string) bool {
	return reason == "manifest_invalid" || reason == "quarantine_authority_invalid" ||
		reason == "copy_recovery_authority_invalid" || reason == "grow_recovery_authority_invalid" ||
		reason == "identity_mismatch" || reason == "record_not_regular" || reason == "resume_abandoned" ||
		reason == "legacy_reset_quarantine" || reason == "recovery_deferral_invalid" || reason == "manifest_missing" ||
		reason == "deferral_record_identity_mismatch"
}

func validateComputerDiskQuarantineReceipt(receipt computerDiskQuarantineReceipt) error {
	if receipt.ReceiptID == "" || receipt.Reason == "" || !validComputerDiskDirectoryName(receipt.DiskName) ||
		receipt.CreatedAt.IsZero() || receipt.RetainUntil.IsZero() || !receipt.RetainUntil.After(receipt.CreatedAt) {
		return errors.New("Computer disk quarantine recovery record is invalid")
	}
	if receipt.GCFailures < 0 || receipt.GCFailures > defaultComputerDiskQuarantineGCFailures ||
		receipt.GCFailures == 0 && (receipt.GCFirstFailedAt != nil || receipt.GCEscalatedAt != nil || receipt.GCLastFailure != "") ||
		receipt.GCFailures > 0 && (receipt.GCFirstFailedAt == nil || receipt.GCLastFailure == "") ||
		receipt.GCEscalatedAt != nil && receipt.GCFailures != defaultComputerDiskQuarantineGCFailures {
		return errors.New("Computer disk quarantine GC evidence is invalid")
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
		if receipt.Reason == "resume_abandoned" && (receipt.DeferredReason == "" || receipt.RecoveryAttempts < 1 || receipt.FirstDeferredAt.IsZero() || receipt.FirstDeferredAt.After(receipt.CreatedAt)) {
			return errors.New("abandoned Computer Storage authority recovery evidence is incomplete")
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

func computerDiskQuarantineGCFailurePath(root string) string {
	return filepath.Join(filepath.Dir(root), "."+filepath.Base(root)+computerDiskQuarantineGCFailureSuffix)
}

func readComputerDiskQuarantineGCFailure(root string) (computerDiskQuarantineReceipt, error) {
	return readAndValidateComputerDiskQuarantineReceipt(computerDiskQuarantineGCFailurePath(root))
}

func (engine *ContainerdEngine) writeComputerDiskQuarantineReceipt(root string, payload []byte) error {
	write := engine.computerQuarantineWrite
	if write == nil {
		write = writeDurableFile
	}
	return write(root, ".quarantine.json.tmp-", "quarantine.json", payload, 0o600)
}

func removeComputerDiskQuarantineGCFailure(root string) error {
	path := computerDiskQuarantineGCFailurePath(root)
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return syncDirectory(filepath.Dir(root))
}

func sameComputerDiskQuarantineAuthority(left, right computerDiskQuarantineReceipt) bool {
	return left.Kind == right.Kind && left.ReceiptID == right.ReceiptID && left.DiskName == right.DiskName && left.Storage == right.Storage
}

func (engine *ContainerdEngine) rememberComputerDiskQuarantineGC(root string, receipt computerDiskQuarantineReceipt) {
	engine.computerQuarantineGCMu.Lock()
	defer engine.computerQuarantineGCMu.Unlock()
	if engine.computerQuarantineGC == nil {
		engine.computerQuarantineGC = make(map[string]computerDiskQuarantineReceipt)
	}
	engine.computerQuarantineGC[root] = receipt
}

func (engine *ContainerdEngine) forgetComputerDiskQuarantineGC(root string) {
	engine.computerQuarantineGCMu.Lock()
	delete(engine.computerQuarantineGC, root)
	engine.computerQuarantineGCMu.Unlock()
}

func (engine *ContainerdEngine) mergeComputerDiskQuarantineGC(root string, receipt computerDiskQuarantineReceipt) (computerDiskQuarantineReceipt, ComputerDiskQuarantineGCEvidenceStorage) {
	storage := ComputerDiskQuarantineGCEvidenceStorage("")
	if receipt.GCFailures > 0 {
		storage = ComputerDiskQuarantineGCEvidencePrimary
	}
	if mirrored, err := readComputerDiskQuarantineGCFailure(root); err == nil &&
		sameComputerDiskQuarantineAuthority(receipt, mirrored) && mirrored.GCFailures > receipt.GCFailures {
		receipt = mirrored
		storage = ComputerDiskQuarantineGCEvidenceMirror
	}
	engine.computerQuarantineGCMu.Lock()
	memory, present := engine.computerQuarantineGC[root]
	engine.computerQuarantineGCMu.Unlock()
	if present && sameComputerDiskQuarantineAuthority(receipt, memory) && memory.GCFailures > receipt.GCFailures {
		receipt = memory
		storage = ComputerDiskQuarantineGCEvidenceMemory
	}
	return receipt, storage
}

func computerDiskQuarantineGCStop(receipt computerDiskQuarantineReceipt, evidenceStorage ComputerDiskQuarantineGCEvidenceStorage, now time.Time) (ComputerDiskQuarantineGCStopReason, time.Time, bool) {
	if receipt.GCEscalatedAt != nil {
		return ComputerDiskQuarantineGCStopFailureLimit, receipt.GCEscalatedAt.UTC(), true
	}
	if receipt.PayloadDroppedAt == nil && receipt.GCFailures > 0 && evidenceStorage == ComputerDiskQuarantineGCEvidenceMemory {
		stopAt := receipt.RetainUntil.Add(defaultComputerDiskQuarantineGCUnrecordedRetryWindow).UTC()
		if !now.Before(stopAt) {
			return ComputerDiskQuarantineGCStopUnrecordedWindow, stopAt, true
		}
	}
	return "", time.Time{}, false
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
	if err := syncDirectory(quarantineRoot); err != nil {
		return false, err
	}
	return true, removeOperationalComputerRecoveryDeferralFault(root, name)
}

func legacyComputerDiskQuarantineName(entryName string) (string, bool) {
	resetAt := strings.LastIndex(entryName, "-reset-")
	if resetAt <= len("wefty-computer-disk-") {
		return "", false
	}
	if _, err := strconv.ParseUint(entryName[resetAt+len("-reset-"):], 10, 64); err != nil {
		return "", false
	}
	name := entryName[:resetAt]
	return name, validComputerDiskDirectoryName(name)
}

const legacyComputerStorageResetManifestVersion = 1

type legacyComputerStorageResetManifest struct {
	Version        int                           `json:"version"`
	Storage        ComputerStorageReference      `json:"storage"`
	NewGeneration  int64                         `json:"new_generation"`
	Authority      ComputerStorageResetAuthority `json:"authority"`
	QuarantineName string                        `json:"quarantine_name"`
	Phase          string                        `json:"phase"`
}

func validatedLegacyComputerStorageResetManifest(runtimeRoot, diskName, quarantineName string) bool {
	payload, err := os.ReadFile(filepath.Join(runtimeRoot, "computer-storage-resets", diskName+".json"))
	if err != nil {
		return false
	}
	var manifest legacyComputerStorageResetManifest
	if json.Unmarshal(payload, &manifest) != nil || manifest.Version != legacyComputerStorageResetManifestVersion ||
		manifest.Storage.DiskBytes <= 0 || manifest.Storage.StorageGeneration <= 0 ||
		manifest.NewGeneration != manifest.Storage.StorageGeneration+1 || manifest.QuarantineName != quarantineName ||
		manifest.Storage.IntentRevision != manifest.Authority.IntentRevision || manifest.Authority.NodeID == "" ||
		manifest.Authority.BootSessionID == "" || manifest.Authority.JobID == "" || manifest.Authority.HelperGeneration == 0 ||
		manifest.Authority.IntentRevision <= 0 || strings.TrimSpace(manifest.Authority.CleanupFence) == "" ||
		(manifest.Phase != "quarantined" && manifest.Phase != "deleted" && manifest.Phase != "verified") {
		return false
	}
	wantDiskName, err := deterministicComputerDiskName(manifest.Storage)
	return err == nil && wantDiskName == diskName &&
		quarantineName == fmt.Sprintf("%s-reset-%d", diskName, manifest.Authority.IntentRevision)
}

func (engine *ContainerdEngine) normalizeLegacyComputerDiskQuarantine(root, name string, createdAt time.Time) (computerDiskQuarantineReceipt, string, error) {
	receiptID, err := randomCapability()
	if err != nil {
		return computerDiskQuarantineReceipt{}, root, err
	}
	createdAt = createdAt.UTC()
	receipt := computerDiskQuarantineReceipt{Kind: computerDiskQuarantineKindAuthority, ReceiptID: receiptID,
		DiskName: name, Reason: "legacy_reset_quarantine", CreatedAt: createdAt,
		RetainUntil: createdAt.Add(defaultComputerDiskQuarantineRetention)}
	payload, err := json.Marshal(receipt)
	if err != nil {
		return computerDiskQuarantineReceipt{}, root, err
	}
	if err := writeDurableFile(root, ".quarantine.json.tmp-", "quarantine.json", payload, 0o600); err != nil {
		return computerDiskQuarantineReceipt{}, root, err
	}
	destination := filepath.Join(filepath.Dir(root), name+"-anomaly-"+receiptID)
	if err := os.Rename(root, destination); err != nil {
		return computerDiskQuarantineReceipt{}, root, err
	}
	if err := syncDirectory(filepath.Dir(root)); err != nil {
		return computerDiskQuarantineReceipt{}, destination, err
	}
	return receipt, destination, nil
}

func (engine *ContainerdEngine) recordComputerDiskQuarantineGCFailure(root string, receipt *computerDiskQuarantineReceipt, method string, now time.Time) (SweepAction, ComputerDiskQuarantineGCEvidenceStorage) {
	failedAt := now.UTC()
	if receipt.GCFirstFailedAt == nil {
		receipt.GCFirstFailedAt = &failedAt
	}
	if receipt.GCFailures < defaultComputerDiskQuarantineGCFailures {
		receipt.GCFailures++
	}
	receipt.GCLastFailure = method
	action := SweepActionQuarantineGCFailed
	if receipt.GCFailures == defaultComputerDiskQuarantineGCFailures {
		receipt.GCEscalatedAt = &failedAt
		action = SweepActionQuarantineGCEscalated
	}
	payload, err := json.Marshal(receipt)
	if err != nil {
		return SweepActionQuarantineGCFailed, ""
	}
	parent := filepath.Dir(root)
	mirrorErr := writeDurableFile(parent, ".quarantine-gc-failure.tmp-", filepath.Base(computerDiskQuarantineGCFailurePath(root)), payload, 0o600)
	primaryErr := engine.writeComputerDiskQuarantineReceipt(root, payload)
	if primaryErr != nil && mirrorErr != nil {
		engine.rememberComputerDiskQuarantineGC(root, *receipt)
		return action, ComputerDiskQuarantineGCEvidenceMemory
	}
	engine.forgetComputerDiskQuarantineGC(root)
	if primaryErr != nil {
		return action, ComputerDiskQuarantineGCEvidenceMirror
	}
	if mirrorErr == nil {
		_ = removeComputerDiskQuarantineGCFailure(root)
	}
	return action, ComputerDiskQuarantineGCEvidencePrimary
}

func (engine *ContainerdEngine) expireComputerDiskQuarantinePayloads(ctx context.Context) error {
	quarantineRoot := filepath.Join(engine.config.RuntimeRoot, "computer-disk-quarantine")
	entries, err := engine.readComputerDiskDirectory(quarantineRoot)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		engine.computerDiskSweepEvidence = append(engine.computerDiskSweepEvidence, SweepEvidence{
			Class: RemovalResourceComputerQuarantine, ID: filepath.Base(quarantineRoot), Action: SweepActionResumeDeferred, Method: "quarantine_root_inventory",
		})
		return nil
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
		entryName := entry.Name()
		legacyName, legacy := legacyComputerDiskQuarantineName(entryName)
		if legacy && !validatedLegacyComputerStorageResetManifest(engine.config.RuntimeRoot, legacyName, entryName) {
			engine.computerDiskSweepEvidence = append(engine.computerDiskSweepEvidence, SweepEvidence{
				Class: RemovalResourceComputerQuarantine, ID: legacyName, Action: SweepActionRetained, Method: "legacy_reset_quarantine",
			})
			closeComputerDiskLock(lock)
			continue
		}
		if legacy {
			receipt, root, readErr = engine.normalizeLegacyComputerDiskQuarantine(root, legacyName, now)
			entryName = receipt.DiskName + "-anomaly-" + receipt.ReceiptID
		}
		if readErr != nil || entryName != receipt.DiskName+"-anomaly-"+receipt.ReceiptID {
			engine.computerDiskSweepEvidence = append(engine.computerDiskSweepEvidence, SweepEvidence{
				Class: RemovalResourceComputerQuarantine, ID: entry.Name(), Action: SweepActionRetained, Method: "quarantine_authority_invalid",
			})
			closeComputerDiskLock(lock)
			continue
		}
		receipt, gcEvidenceStorage := engine.mergeComputerDiskQuarantineGC(root, receipt)
		if now.Before(receipt.RetainUntil) || receipt.PayloadDroppedAt != nil {
			closeComputerDiskLock(lock)
			continue
		}
		if stopReason, _, stopped := computerDiskQuarantineGCStop(receipt, gcEvidenceStorage, now); stopped {
			method := "quarantine_gc_escalated"
			if stopReason == ComputerDiskQuarantineGCStopUnrecordedWindow {
				method = "quarantine_gc_unrecorded_retry_window_elapsed"
			}
			engine.computerDiskSweepEvidence = append(engine.computerDiskSweepEvidence, SweepEvidence{
				Class: RemovalResourceComputerQuarantine, ID: receipt.DiskName, Action: SweepActionRetained, Method: method,
				GCEvidenceStorage: gcEvidenceStorage, GCStopReason: stopReason,
			})
			closeComputerDiskLock(lock)
			continue
		}
		if err := ctx.Err(); err != nil {
			closeComputerDiskLock(lock)
			return err
		}
		children, readErr := engine.readComputerDiskDirectory(root)
		if readErr != nil {
			action, storage := engine.recordComputerDiskQuarantineGCFailure(root, &receipt, "read_payload", now)
			engine.computerDiskSweepEvidence = append(engine.computerDiskSweepEvidence, SweepEvidence{Class: RemovalResourceComputerQuarantine, ID: receipt.DiskName, Action: action, Method: "read_payload", GCEvidenceStorage: storage})
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
				action, storage := engine.recordComputerDiskQuarantineGCFailure(root, &receipt, "remove_payload", now)
				engine.computerDiskSweepEvidence = append(engine.computerDiskSweepEvidence, SweepEvidence{Class: RemovalResourceComputerQuarantine, ID: receipt.DiskName, Action: action, Method: "remove_payload", GCEvidenceStorage: storage})
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
			writeErr = engine.writeComputerDiskQuarantineReceipt(root, payload)
		}
		if writeErr != nil {
			receipt.PayloadDroppedAt = nil
			action, storage := engine.recordComputerDiskQuarantineGCFailure(root, &receipt, "record_payload_drop", now)
			engine.computerDiskSweepEvidence = append(engine.computerDiskSweepEvidence, SweepEvidence{Class: RemovalResourceComputerQuarantine, ID: receipt.DiskName, Action: action, Method: "record_payload_drop", GCEvidenceStorage: storage})
			closeComputerDiskLock(lock)
			continue
		}
		engine.forgetComputerDiskQuarantineGC(root)
		_ = removeComputerDiskQuarantineGCFailure(root)
		method := "remove_payload"
		if receipt.Reason == "legacy_reset_quarantine" {
			method = receipt.Reason
		}
		engine.computerDiskSweepEvidence = append(engine.computerDiskSweepEvidence, SweepEvidence{Class: RemovalResourceComputerQuarantine, ID: receipt.DiskName, Action: SweepActionQuarantinePayloadDropped, Method: method})
		closeComputerDiskLock(lock)
	}
	return nil
}
