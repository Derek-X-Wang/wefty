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
