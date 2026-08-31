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

type computerDiskDetachmentAuthority struct {
	NodeID        string
	BootSessionID string
	PriorJobID    string
}

// validComputerDiskDetachmentEvidence validates a prior attachment's positive
// detach fact against durable Storage and Node identity. The prior
// Job/attempt/fence are retained as historical evidence and are not compared
// to successor authority; consumers that can mutate or copy detached bytes
// additionally bind the named prior Job through
// validComputerDiskConsumerDetachmentEvidence.
func validComputerDiskDetachmentEvidence(evidence *computerDiskEvidence, storage ComputerStorageReference, authority computerDiskDetachmentAuthority) bool {
	if evidence == nil || authority.NodeID == "" || authority.BootSessionID == "" || evidence.ReceiptID == "" || evidence.ComputerID != storage.ComputerID || evidence.StorageID != storage.StorageID || evidence.StorageGeneration != storage.StorageGeneration ||
		evidence.NodeID != authority.NodeID || evidence.JobID == "" || evidence.AttemptID == "" || evidence.FencingToken == "" || evidence.BootSessionID == "" {
		return false
	}
	switch evidence.Kind {
	case computerDiskReapReceipt:
		return evidence.SweepEpoch == "" && evidence.BootSessionID == authority.BootSessionID
	case computerDiskSweepReceipt:
		return evidence.SweepEpoch != ""
	default:
		return false
	}
}

func validComputerDiskConsumerDetachmentEvidence(evidence *computerDiskEvidence, storage ComputerStorageReference, authority computerDiskDetachmentAuthority) bool {
	return authority.PriorJobID != "" && evidence != nil && evidence.JobID == authority.PriorJobID &&
		validComputerDiskDetachmentEvidence(evidence, storage, authority)
}

func validComputerDiskEvidence(evidence *computerDiskEvidence, storage ComputerStorageReference, authority AttemptAuthority) bool {
	return validComputerDiskDetachmentEvidence(evidence, storage, computerDiskDetachmentAuthority{
		NodeID: authority.NodeID, BootSessionID: authority.BootSessionID,
	})
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
