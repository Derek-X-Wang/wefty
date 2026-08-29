package contract

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
	FilesystemExpanded    bool   `json:"filesystem_expanded"`
}
