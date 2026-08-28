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
