package ocihelper

import "testing"

func TestComputerDiskSweepEvidenceAuthorizesDurableStorageSuccessors(t *testing.T) {
	storage := ComputerStorageReference{
		ComputerID: "computer-1", StorageID: "storage-1", StorageGeneration: 1,
		IntentRevision: 1, DiskBytes: 16 << 20,
	}
	prior := AttemptAuthority{
		NodeID: "node-1", JobID: "job-1", AttemptID: "attempt-a",
		FencingToken: "fence-a", BootSessionID: "boot-a", Class: "service",
		RemovalGeneration: "attempt",
	}
	evidence, err := newComputerDiskEvidence(computerDiskSweepReceipt, "helper-replacement-sweep", storage, prior)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name          string
		successorJob  string
		successorBoot string
	}{
		{name: "helper replacement within agent boot", successorJob: "job-1", successorBoot: "boot-a"},
		{name: "replacement Job within agent boot", successorJob: "job-2", successorBoot: "boot-a"},
		{name: "same Job after agent boot", successorJob: "job-1", successorBoot: "boot-b"},
		{name: "replacement Job after agent boot", successorJob: "job-2", successorBoot: "boot-b"},
	} {
		t.Run(test.name, func(t *testing.T) {
			successor := prior
			successor.JobID = test.successorJob
			successor.AttemptID = "attempt-b"
			successor.FencingToken = "fence-b"
			successor.BootSessionID = test.successorBoot
			if !validComputerDiskEvidence(&evidence, storage, successor) {
				t.Fatal("exact helper sweep detachment did not authorize the durable Storage successor")
			}
		})
	}
}

func TestComputerDiskSameBootReapEvidenceRequiresCurrentBoot(t *testing.T) {
	storage := ComputerStorageReference{
		ComputerID: "computer-1", StorageID: "storage-1", StorageGeneration: 1,
		IntentRevision: 1, DiskBytes: 16 << 20,
	}
	prior := AttemptAuthority{
		NodeID: "node-1", JobID: "job-1", AttemptID: "attempt-a",
		FencingToken: "fence-a", BootSessionID: "boot-a", Class: "service",
		RemovalGeneration: "attempt",
	}
	evidence, err := newComputerDiskEvidence(computerDiskReapReceipt, "", storage, prior)
	if err != nil {
		t.Fatal(err)
	}
	successor := prior
	successor.JobID = "job-2"
	successor.AttemptID = "attempt-b"
	successor.FencingToken = "fence-b"
	if !validComputerDiskEvidence(&evidence, storage, successor) {
		t.Fatal("same-boot reap did not authorize a fresh successor Job")
	}

	wrongBoot := successor
	wrongBoot.BootSessionID = "boot-b"
	if validComputerDiskEvidence(&evidence, storage, wrongBoot) {
		t.Fatal("same-boot reap authorized a successor from another boot")
	}
	withSweepEpoch := evidence
	withSweepEpoch.SweepEpoch = "unexpected"
	if validComputerDiskEvidence(&withSweepEpoch, storage, successor) {
		t.Fatal("same-boot reap with a sweep epoch authorized a successor")
	}
	if validComputerDiskEvidence(nil, storage, successor) {
		t.Fatal("missing detachment evidence authorized a successor")
	}
}

func TestComputerDiskSweepEvidenceRejectsIncompleteOrForeignDetachment(t *testing.T) {
	storage := ComputerStorageReference{
		ComputerID: "computer-1", StorageID: "storage-1", StorageGeneration: 1,
		IntentRevision: 1, DiskBytes: 16 << 20,
	}
	successor := AttemptAuthority{
		NodeID: "node-1", JobID: "job-2", AttemptID: "attempt-b",
		FencingToken: "fence-b", BootSessionID: "boot-b", Class: "service",
		RemovalGeneration: "attempt",
	}
	valid := computerDiskEvidence{
		Kind: computerDiskSweepReceipt, ReceiptID: "receipt-1", ComputerID: storage.ComputerID,
		StorageID: storage.StorageID, StorageGeneration: storage.StorageGeneration,
		NodeID: "node-1", JobID: "job-1", AttemptID: "attempt-a",
		FencingToken: "fence-a", BootSessionID: "boot-a", SweepEpoch: "helper-sweep",
	}
	for _, test := range []struct {
		name   string
		mutate func(*computerDiskEvidence)
	}{
		{name: "missing receipt", mutate: func(evidence *computerDiskEvidence) { evidence.ReceiptID = "" }},
		{name: "foreign Computer", mutate: func(evidence *computerDiskEvidence) { evidence.ComputerID = "computer-2" }},
		{name: "foreign Storage", mutate: func(evidence *computerDiskEvidence) { evidence.StorageID = "storage-2" }},
		{name: "foreign generation", mutate: func(evidence *computerDiskEvidence) { evidence.StorageGeneration++ }},
		{name: "foreign Node", mutate: func(evidence *computerDiskEvidence) { evidence.NodeID = "node-2" }},
		{name: "missing prior Job", mutate: func(evidence *computerDiskEvidence) { evidence.JobID = "" }},
		{name: "missing prior attempt", mutate: func(evidence *computerDiskEvidence) { evidence.AttemptID = "" }},
		{name: "missing prior fence", mutate: func(evidence *computerDiskEvidence) { evidence.FencingToken = "" }},
		{name: "missing prior boot", mutate: func(evidence *computerDiskEvidence) { evidence.BootSessionID = "" }},
		{name: "missing sweep epoch", mutate: func(evidence *computerDiskEvidence) { evidence.SweepEpoch = "" }},
		{name: "unknown evidence kind", mutate: func(evidence *computerDiskEvidence) { evidence.Kind = "unknown" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			evidence := valid
			test.mutate(&evidence)
			if validComputerDiskEvidence(&evidence, storage, successor) {
				t.Fatal("incomplete or foreign detachment authorized successor attach")
			}
		})
	}
}

func TestComputerDiskConsumerEvidenceBindsNamedPriorJobNotConsumerJob(t *testing.T) {
	storage := ComputerStorageReference{ComputerID: "computer-1", StorageID: "storage-1", StorageGeneration: 1}
	evidence := computerDiskEvidence{
		Kind: computerDiskSweepReceipt, ReceiptID: "receipt-1", ComputerID: storage.ComputerID,
		StorageID: storage.StorageID, StorageGeneration: storage.StorageGeneration,
		NodeID: "node-1", JobID: "prior-job", AttemptID: "attempt-a",
		FencingToken: "fence-a", BootSessionID: "boot-a", SweepEpoch: "same-boot-helper-sweep",
	}
	consumer := computerDiskDetachmentAuthority{
		NodeID: "node-1", BootSessionID: "boot-a", PriorJobID: "prior-job",
	}
	if !validComputerDiskConsumerDetachmentEvidence(&evidence, storage, consumer) {
		t.Fatal("same-boot helper sweep did not authorize the consumer naming the prior Job")
	}
	consumer.PriorJobID = "consumer-job"
	if validComputerDiskConsumerDetachmentEvidence(&evidence, storage, consumer) {
		t.Fatal("consumer Job substituted for the named prior Job")
	}
	consumer.PriorJobID = ""
	if validComputerDiskConsumerDetachmentEvidence(&evidence, storage, consumer) {
		t.Fatal("unnamed prior Job authorized detached Storage consumption")
	}
}
