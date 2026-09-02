package contract

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

const (
	StorageOnlyRemovalAttemptPrefix  = "storage-removal-"
	ComputerStorageCopyVerifiedKind  = "computer_storage_copy_verified"
	ComputerStorageResetVerifiedKind = "computer_storage_reset_verified"
)

func StorageOnlyRemovalAttemptID(generation int64) string {
	return fmt.Sprintf("%s%d", StorageOnlyRemovalAttemptPrefix, generation)
}

func ValidStorageOnlyRemovalAttemptID(attemptID string, generation int64) bool {
	return generation > 0 && attemptID == StorageOnlyRemovalAttemptID(generation)
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
}
