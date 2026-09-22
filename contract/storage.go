package contract

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

const (
	StorageOnlyRemovalAttemptPrefix          = "storage-removal-"
	StorageAbsentRemovalAttemptPrefix        = "storage-absent-"
	ComputerStorageResetRetirementPrefix     = "storage-reset-"
	ComputerStorageRestoreRetirementPrefix   = "storage-restore-"
	ComputerStorageFailedImportCleanupPrefix = "storage-import-failed-"
	ComputerStorageCopyVerifiedKind          = "computer_storage_copy_verified"
	ComputerStorageResetVerifiedKind         = "computer_storage_reset_verified"
)

func StorageOnlyRemovalAttemptID(generation int64) string {
	return fmt.Sprintf("%s%d", StorageOnlyRemovalAttemptPrefix, generation)
}

func ValidStorageOnlyRemovalAttemptID(attemptID string, generation int64) bool {
	return generation > 0 && attemptID == StorageOnlyRemovalAttemptID(generation)
}

func StorageAbsentRemovalAttemptID(generation int64) string {
	return fmt.Sprintf("%s%d", StorageAbsentRemovalAttemptPrefix, generation)
}

func ValidStorageAbsentRemovalAttemptID(attemptID string, generation int64) bool {
	return generation > 0 && attemptID == StorageAbsentRemovalAttemptID(generation)
}

func ComputerStorageResetRetirementAttemptID(revision int64) string {
	return fmt.Sprintf("%s%d", ComputerStorageResetRetirementPrefix, revision)
}

func ComputerStorageRestoreRetirementAttemptID(revision int64) string {
	return fmt.Sprintf("%s%d", ComputerStorageRestoreRetirementPrefix, revision)
}

func ComputerStorageFailedImportCleanupAttemptID(revision int64) string {
	return fmt.Sprintf("%s%d", ComputerStorageFailedImportCleanupPrefix, revision)
}

func ValidComputerStorageCleanupAttemptID(attemptID string, revision int64) bool {
	return revision > 0 && (attemptID == ComputerStorageResetRetirementAttemptID(revision) ||
		attemptID == ComputerStorageRestoreRetirementAttemptID(revision) ||
		attemptID == ComputerStorageFailedImportCleanupAttemptID(revision))
}

// ComputerStoragePreparationWitness is helper-originated durable evidence
// that a Storage generation was published without ever being attached.
type ComputerStoragePreparationWitness struct {
	Kind              string `json:"kind"`
	ReceiptID         string `json:"receipt_id"`
	NodeID            string `json:"node_id"`
	RootInstanceID    string `json:"root_instance_id"`
	JobID             string `json:"job_id"`
	ComputerID        string `json:"computer_id"`
	StorageID         string `json:"storage_id"`
	StorageGeneration int64  `json:"storage_generation"`
	Revision          int64  `json:"revision"`
	Fence             string `json:"fence"`
	HelperGeneration  uint64 `json:"helper_generation"`
}

func (w ComputerStoragePreparationWitness) Valid() bool {
	validKind := w.Kind == ComputerStorageCopyVerifiedKind || w.Kind == ComputerStorageResetVerifiedKind
	return validKind && strings.TrimSpace(w.ReceiptID) != "" &&
		strings.TrimSpace(w.NodeID) != "" && strings.TrimSpace(w.RootInstanceID) != "" &&
		strings.TrimSpace(w.JobID) != "" && strings.TrimSpace(w.ComputerID) != "" &&
		strings.TrimSpace(w.StorageID) != "" && w.StorageGeneration > 0 && w.Revision > 0 &&
		strings.TrimSpace(w.Fence) != "" && w.HelperGeneration > 0
}

// ComputerStorageResetReceipt is the one cross-boundary witness for a
// prepared replacement generation. Keeping the wire shape in contract avoids
// hand-copied receipt declarations drifting between L1, agent, adapter, and
// helper.
type ComputerStorageResetReceipt struct {
	Kind             string `json:"kind"`
	ReceiptID        string `json:"receipt_id"`
	ComputerID       string `json:"computer_id"`
	StorageID        string `json:"storage_id"`
	OldGeneration    int64  `json:"old_generation"`
	NewGeneration    int64  `json:"new_generation"`
	NodeID           string `json:"node_id"`
	RootInstanceID   string `json:"root_instance_id"`
	JobID            string `json:"job_id"`
	IntentRevision   int64  `json:"intent_revision"`
	CleanupFence     string `json:"cleanup_fence"`
	HelperGeneration uint64 `json:"helper_generation"`
}

// ComputerStorageGrowReceipt is assertion-derived evidence that the exact
// current generation either reached its fully allocated target size or was
// left at the old size after an atomic capacity refusal.
type ComputerStorageGrowReceipt struct {
	Kind                   string `json:"kind"`
	ReceiptID              string `json:"receipt_id"`
	ComputerID             string `json:"computer_id"`
	StorageID              string `json:"storage_id"`
	StorageGeneration      int64  `json:"storage_generation"`
	NodeID                 string `json:"node_id"`
	RootInstanceID         string `json:"root_instance_id"`
	JobID                  string `json:"job_id"`
	OperationRevision      int64  `json:"operation_revision"`
	OperationFence         string `json:"operation_fence"`
	HelperGeneration       uint64 `json:"helper_generation"`
	OldDiskBytes           int64  `json:"old_disk_bytes"`
	NewDiskBytes           int64  `json:"new_disk_bytes"`
	Applied                bool   `json:"applied"`
	FailureCode            string `json:"failure_code,omitempty"`
	ObservedAvailableBytes int64  `json:"observed_available_bytes,omitempty"`
}

// ComputerBackupCopyReceipt is assertion-derived helper evidence for one
// planned source-node copy. Failure receipts are accepted only when CopyAbsent
// proves the helper removed every staging and published byte for that copy.
type ComputerBackupCopyReceipt struct {
	Kind              string `json:"kind"`
	ReceiptID         string `json:"receipt_id"`
	BackupID          string `json:"backup_id"`
	CopyID            string `json:"copy_id"`
	ComputerID        string `json:"computer_id"`
	StorageID         string `json:"storage_id"`
	StorageGeneration int64  `json:"storage_generation"`
	NodeID            string `json:"node_id"`
	RootInstanceID    string `json:"root_instance_id"`
	JobID             string `json:"job_id"`
	OperationRevision int64  `json:"operation_revision"`
	CleanupFence      string `json:"cleanup_fence"`
	HelperGeneration  uint64 `json:"helper_generation"`
	AllocatedSize     int64  `json:"allocated_size"`
	ContentDigest     string `json:"content_digest,omitempty"`
	Encryption        string `json:"encryption"`
	FailureCode       string `json:"failure_code,omitempty"`
	CopyAbsent        bool   `json:"copy_absent"`
}

// ComputerBackupCopyRemovalReceipt positively binds physical-copy absence to
// its Node, managed-root instance, source generation, and cleanup operation.
type ComputerBackupCopyRemovalReceipt struct {
	Kind              string `json:"kind"`
	ReceiptID         string `json:"receipt_id"`
	BackupID          string `json:"backup_id"`
	CopyID            string `json:"copy_id"`
	ComputerID        string `json:"computer_id"`
	StorageID         string `json:"storage_id"`
	StorageGeneration int64  `json:"storage_generation"`
	NodeID            string `json:"node_id"`
	RootInstanceID    string `json:"root_instance_id"`
	OperationRevision int64  `json:"operation_revision"`
	CleanupFence      string `json:"cleanup_fence"`
	HelperGeneration  uint64 `json:"helper_generation"`
	Absent            bool   `json:"absent"`
}

// ComputerStorageCopyReceipt is assertion-derived helper evidence that one
// exact Backup copy produced a detached destination Storage generation. The
// source digest is checked before any destination publication; the destination
// digest is recorded after clone identity rekeying and optional expansion.
type ComputerStorageCopyReceipt struct {
	Kind                  string `json:"kind"`
	ReceiptID             string `json:"receipt_id"`
	Operation             string `json:"operation"`
	BackupID              string `json:"backup_id"`
	CopyID                string `json:"copy_id"`
	ExportID              string `json:"export_id,omitempty"`
	ExternalPath          string `json:"external_path,omitempty"`
	ManifestDigest        string `json:"manifest_digest,omitempty"`
	SourceComputerID      string `json:"source_computer_id"`
	SourceStorageID       string `json:"source_storage_id"`
	SourceGeneration      int64  `json:"source_generation"`
	DestinationComputerID string `json:"destination_computer_id"`
	DestinationStorageID  string `json:"destination_storage_id"`
	DestinationGeneration int64  `json:"destination_generation"`
	NodeID                string `json:"node_id"`
	RootInstanceID        string `json:"root_instance_id"`
	JobID                 string `json:"job_id"`
	OperationRevision     int64  `json:"operation_revision"`
	CleanupFence          string `json:"cleanup_fence"`
	HelperGeneration      uint64 `json:"helper_generation"`
	SourceSize            int64  `json:"source_size"`
	DestinationSize       int64  `json:"destination_size"`
	SourceDigest          string `json:"source_digest"`
	DestinationDigest     string `json:"destination_digest"`
	OSIdentityRekeyed     bool   `json:"os_identity_rekeyed"`
	MachineIDBeforeDigest string `json:"machine_id_before_digest,omitempty"`
	MachineIDAfterDigest  string `json:"machine_id_after_digest,omitempty"`
	MachineIDRepaired     bool   `json:"machine_id_repaired,omitempty"`
	SourceUnchanged       bool   `json:"source_unchanged"`
	DestinationPrepared   bool   `json:"destination_prepared"`
	PreparationReceipt    bool   `json:"preparation_receipt"`
	DestinationChown      bool   `json:"destination_chown"`
	FilesystemExpanded    bool   `json:"filesystem_expanded"`
	FailureCode           string `json:"failure_code,omitempty"`
	DestinationAbsent     bool   `json:"destination_absent,omitempty"`
}

// ComputerCustodyManifest is the portable, self-contained authority record
// written beside exported bytes. Import does not depend on the L1 database
// that authorized the export: the operator submits this exact manifest and
// the destination helper independently re-reads and verifies it.
type ComputerCustodyManifest struct {
	Version           int     `json:"version"`
	ExportID          string  `json:"export_id"`
	BackupID          string  `json:"backup_id"`
	CopyID            string  `json:"copy_id"`
	ComputerID        string  `json:"computer_id"`
	StorageID         string  `json:"storage_id"`
	StorageGeneration int64   `json:"storage_generation"`
	AllocatedSize     int64   `json:"allocated_size"`
	ContentDigest     string  `json:"content_digest"`
	Encryption        string  `json:"encryption"`
	NodeID            string  `json:"node_id"`
	RootInstanceID    string  `json:"root_instance_id"`
	OperationRevision int64   `json:"operation_revision"`
	CustodyFence      string  `json:"custody_fence"`
	JobSpec           JobSpec `json:"job_spec"`
	JobSpecHash       string  `json:"job_spec_hash"`
	DiskFile          string  `json:"disk_file"`
	Phase             string  `json:"phase"`
}

func DigestComputerCustodyManifest(manifest ComputerCustodyManifest) (string, error) {
	payload, err := json.Marshal(manifest)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(digest[:]), nil
}

// ComputerCustodyExportReceipt proves that the helper observed the exact
// immutable Backup bytes and manifest at an operator-owned destination. The
// L1 custody event predates this receipt: even a missing receipt cannot undo
// the fact that an external write was authorized and may contain secrets.
type ComputerCustodyExportReceipt struct {
	Kind               string `json:"kind"`
	ReceiptID          string `json:"receipt_id"`
	ExportID           string `json:"export_id"`
	BackupID           string `json:"backup_id"`
	CopyID             string `json:"copy_id"`
	ComputerID         string `json:"computer_id"`
	StorageID          string `json:"storage_id"`
	StorageGeneration  int64  `json:"storage_generation"`
	NodeID             string `json:"node_id"`
	RootInstanceID     string `json:"root_instance_id"`
	OperationRevision  int64  `json:"operation_revision"`
	CustodyFence       string `json:"custody_fence"`
	HelperGeneration   uint64 `json:"helper_generation"`
	ExternalPath       string `json:"external_path"`
	AllocatedSize      int64  `json:"allocated_size"`
	ContentDigest      string `json:"content_digest"`
	ManifestDigest     string `json:"manifest_digest"`
	ExternalOwnerUID   uint32 `json:"external_owner_uid"`
	ExternalOwnerGID   uint32 `json:"external_owner_gid"`
	OwnershipApplied   bool   `json:"ownership_applied"`
	PrivateModeApplied bool   `json:"private_mode_applied"`
	FailureCode        string `json:"failure_code,omitempty"`
	// ExternalRoots names the node's operator mount roots on a confinement or
	// mount refusal. An operator who asked for the wrong path learns the
	// answer from the refusal itself instead of from `node oci doctor`.
	ExternalRoots []string `json:"external_roots,omitempty"`
}

// Typed Custody export refusals the helper raises strictly before it creates
// or writes anything at the destination.
const (
	// CustodyExportManagedRootPath is an operator path that resolves inside
	// the helper-managed root.
	CustodyExportManagedRootPath = "managed_root_path"
	// CustodyExportPathUnconfined is an operator path that is not a strict
	// descendant of one of the node's configured operator mount roots.
	CustodyExportPathUnconfined = "external_path_unconfined"
	// CustodyExportRootUnmounted is an operator mount root the helper sees
	// only as a directory in its own filesystem: on a Node whose helper reads
	// a translated view of the node's paths, an absent mount would silently
	// take the export into the helper's own rootfs.
	CustodyExportRootUnmounted = "external_root_unmounted"
	// CustodyExportPathCrossesMount is an operator path that leaves the
	// filesystem the node shares with its helper. A device boundary below the
	// shared root is another filesystem inside the helper, not the
	// operator's storage.
	CustodyExportPathCrossesMount = "external_path_crosses_mount"
)

// CustodyExportWriteStarted is the typed refusal for an export whose helper
// has already begun placing bytes on operator storage in some earlier
// invocation. It is never an untouched refusal: the durable evidence says
// external bytes may exist, so the source Storage stays tainted no matter
// what a later attempt decides about the path.
const CustodyExportWriteStarted = "external_write_started"

// CustodyExportWriteCompleted is the typed refusal for an export whose
// helper already wrote and verified the external bytes in an earlier
// invocation whose acknowledgement never reached L1. Like
// CustodyExportWriteStarted it is never an untouched refusal; it says more,
// namely that the operator's storage holds a complete verified copy.
const CustodyExportWriteCompleted = "external_write_completed"

// CustodyExportLeftDestinationUntouched reports whether a durable Custody
// export ended without placing any byte of that Storage on operator storage.
// Such a refusal may have created empty operator-owned directories under a
// configured operator mount root — that is where the export was allowed to
// go — but it wrote no manifest and no disk, so the source Storage stays
// free of custody taint. Every other outcome, including a missing or late
// receipt, means bytes may exist outside managed custody.
func CustodyExportLeftDestinationUntouched(status, failureCode string) bool {
	if status != "failed" {
		return false
	}
	switch failureCode {
	case CustodyExportManagedRootPath, CustodyExportPathUnconfined, CustodyExportRootUnmounted,
		CustodyExportPathCrossesMount:
		return true
	default:
		return false
	}
}

// CustodyExportUntouchedRefusalCodes lists the failure codes
// CustodyExportLeftDestinationUntouched accepts, for callers that must express
// the same predicate in a query.
func CustodyExportUntouchedRefusalCodes() []string {
	return []string{CustodyExportManagedRootPath, CustodyExportPathUnconfined, CustodyExportRootUnmounted,
		CustodyExportPathCrossesMount}
}

// CustodyExportRefusalNamesRoots reports whether a failure code is one whose
// receipt must name the node's operator mount roots, so a refused operator
// learns where an export may go without reading node facts.
func CustodyExportRefusalNamesRoots(failureCode string) bool {
	return CustodyExportLeftDestinationUntouched("failed", failureCode)
}
