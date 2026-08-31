package ocihelper

type computerDiskEvidence struct {
	Kind              string `json:"kind"`
	ReceiptID         string `json:"receipt_id"`
	ComputerID        string `json:"computer_id"`
	StorageID         string `json:"storage_id"`
	StorageGeneration int64  `json:"storage_generation"`
	NodeID            string `json:"node_id"`
	JobID             string `json:"job_id"`
	AttemptID         string `json:"attempt_id"`
	FencingToken      string `json:"fencing_token"`
	BootSessionID     string `json:"boot_session_id"`
	SweepEpoch        string `json:"sweep_epoch,omitempty"`
}

const (
	computerDiskReapReceipt  = "same_boot_reap"
	computerDiskSweepReceipt = "prior_boot_sweep"
)

func sameComputerStorageIdentity(left, right ComputerStorageReference) bool {
	return left.ComputerID == right.ComputerID && left.StorageID == right.StorageID && left.StorageGeneration == right.StorageGeneration
}

// validComputerDiskDetachmentEvidence validates a prior attachment's positive
// detach fact against durable Storage and Node identity. The evidence names
// the prior Job/attempt/fence; those fields must be present but must not equal
// successor authority because a Computer can replace its helper within one
// agent boot or replace its Job while retaining the same Storage generation.
func validComputerDiskDetachmentEvidence(evidence *computerDiskEvidence, storage ComputerStorageReference, nodeID, bootSessionID string) bool {
	if evidence == nil || evidence.ReceiptID == "" || evidence.ComputerID != storage.ComputerID || evidence.StorageID != storage.StorageID || evidence.StorageGeneration != storage.StorageGeneration ||
		evidence.NodeID != nodeID || evidence.JobID == "" || evidence.AttemptID == "" || evidence.FencingToken == "" || evidence.BootSessionID == "" {
		return false
	}
	switch evidence.Kind {
	case computerDiskReapReceipt:
		return evidence.SweepEpoch == "" && evidence.BootSessionID == bootSessionID
	case computerDiskSweepReceipt:
		return evidence.SweepEpoch != ""
	default:
		return false
	}
}

func validComputerDiskEvidence(evidence *computerDiskEvidence, storage ComputerStorageReference, authority AttemptAuthority) bool {
	return validComputerDiskDetachmentEvidence(evidence, storage, authority.NodeID, authority.BootSessionID)
}

func newComputerDiskEvidence(kind, sweepEpoch string, storage ComputerStorageReference, authority AttemptAuthority) (computerDiskEvidence, error) {
	receiptID, err := randomCapability()
	if err != nil {
		return computerDiskEvidence{}, err
	}
	return computerDiskEvidence{Kind: kind, ReceiptID: receiptID, ComputerID: storage.ComputerID, StorageID: storage.StorageID,
		StorageGeneration: storage.StorageGeneration, NodeID: authority.NodeID, JobID: authority.JobID, AttemptID: authority.AttemptID,
		FencingToken: authority.FencingToken, BootSessionID: authority.BootSessionID, SweepEpoch: sweepEpoch}, nil
}
