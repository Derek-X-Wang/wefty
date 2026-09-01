package agent

import (
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestComputerReimagePreflightDirectivePropagatesDiskBudget(t *testing.T) {
	digest := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	directive := l1.ComputerReimagePreflightDirective{ComputerID: "computer", StorageID: "storage",
		StorageGeneration: 3, DiskBytes: 160 << 20, OldJobID: "old", StagingJobID: "staging",
		RootInstanceID: "root", OperationRevision: 7, OperationFence: "fence",
		TargetImage: contract.OCIImageSpec{Reference: "example.test/computer", Digest: &digest}}
	request := computerReimagePreflightRequest(directive, "node", "boot")
	if request.Storage.DiskBytes != directive.DiskBytes || request.Storage.StorageGeneration != directive.StorageGeneration ||
		request.Storage.IntentRevision != directive.OperationRevision {
		t.Fatalf("Computer reimage Storage propagation = %+v, want %d-byte generation %d revision %d",
			request.Storage, directive.DiskBytes, directive.StorageGeneration, directive.OperationRevision)
	}
}

func TestComputerReimageTransientFailuresUseBoundedNextPollBudget(t *testing.T) {
	controller := &reimagePreflightController{retryCounts: make(map[string]int)}
	directive := l1.ComputerReimagePreflightDirective{ComputerID: "computer", OperationRevision: 7}
	for _, receipt := range []l1.ComputerReimagePreflightReceipt{
		{Kind: "computer_reimage_preflight_failed_unchanged", FailureStage: "manifest_read", FailureReason: "detachment_required"},
		{Kind: "computer_reimage_preflight_failed_unchanged", FailureStage: "generation_lock", FailureReason: "deadline_exceeded"},
	} {
		for attempt := 1; attempt <= computerReimagePreflightRetryLimit; attempt++ {
			deferred := controller.deferTransientFailure(directive, receipt)
			if deferred != (attempt < computerReimagePreflightRetryLimit) {
				t.Fatalf("transient attempt %d deferred=%t", attempt, deferred)
			}
		}
	}
	definitive := l1.ComputerReimagePreflightReceipt{Kind: "computer_reimage_preflight_failed_unchanged",
		FailureStage: "image_identity", FailureReason: "deadline_exceeded"}
	if controller.deferTransientFailure(directive, definitive) {
		t.Fatal("non-generation image deadline was treated as retryable")
	}
}
