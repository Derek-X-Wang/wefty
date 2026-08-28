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
