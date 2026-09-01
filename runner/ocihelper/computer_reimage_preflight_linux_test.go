//go:build linux

package ocihelper

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestReimagePreflightStageErrorIsBoundedAndPreservesCause(t *testing.T) {
	cause := errors.New("private /host/path detail")
	err := reimagePreflightStageError("disk_owner", cause)
	if !errors.Is(err, cause) {
		t.Fatal("preflight stage error did not preserve its cause for local classification")
	}
	if got := err.Error(); got != "Computer reimage preflight failed at disk_owner" || strings.Contains(got, "/host/path") {
		t.Fatalf("preflight stage error crossed private mechanics: %q", got)
	}
}

func TestReimageAcceptsVerifiedNeverAttachedResetSuccessor(t *testing.T) {
	storage := testComputerStorage()
	storage.StorageGeneration = 2
	storage.IntentRevision = 3
	preparation := ComputerStorageResetAuthority{NodeID: "node-1", BootSessionID: "boot-1", HelperGeneration: 7,
		RootInstanceID: "root-1", JobID: "old-job", PriorJobID: "old-job", IntentRevision: 3, CleanupFence: "reset-fence"}
	receipt := ComputerStorageResetReceipt{Kind: "computer_storage_reset_verified", ReceiptID: "reset-receipt",
		ComputerID: storage.ComputerID, StorageID: storage.StorageID, OldGeneration: 1, NewGeneration: 2,
		NodeID: preparation.NodeID, RootInstanceID: preparation.RootInstanceID, JobID: preparation.JobID,
		IntentRevision: preparation.IntentRevision, CleanupFence: preparation.CleanupFence,
		HelperGeneration: preparation.HelperGeneration}
	manifest := computerDiskManifest{Version: computerDiskManifestVersion, Storage: storage, DiskImage: "disk.ext4",
		MountDirectory: "disk", Prepared: true, Preparation: &preparation, PreparationReceipt: &receipt}
	request := PreflightComputerReimageRequest{Storage: storage, Authority: ComputerReimagePreflightAuthority{
		NodeID: preparation.NodeID, RootInstanceID: preparation.RootInstanceID, OldJobID: preparation.JobID,
	}}
	evidence, ok := reimageDetachmentEvidence(manifest, request)
	if !ok || evidence.kind != "computer_reimage_reset_preparation" ||
		evidence.resetPreparationReceiptID != receipt.ReceiptID || evidence.attemptID != "" || evidence.fencingToken != "" {
		t.Fatalf("reset-successor reimage evidence = %+v ok=%t", evidence, ok)
	}
	request.Authority.OldJobID = "different-job"
	if _, ok := reimageDetachmentEvidence(manifest, request); ok {
		t.Fatal("reset-successor preparation authorized a different prior Job")
	}
}

func testComputerReimageRequest(storage ComputerStorageReference, prior AttemptAuthority) PreflightComputerReimageRequest {
	return PreflightComputerReimageRequest{Storage: storage, TargetImage: EnsureImageRequest{
		Reference: "example.test/computer", Digest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Platform: OCIPlatform{OS: "linux", Architecture: "amd64"}},
		Authority: ComputerReimagePreflightAuthority{NodeID: prior.NodeID, BootSessionID: prior.BootSessionID,
			HelperGeneration: 7, RootInstanceID: "root", OldJobID: prior.JobID, StagingJobID: "staging",
			OperationRevision: storage.IntentRevision, OperationFence: "reimage-fence"}}
}

func TestPreflightComputerReimageUsesManifestBudgetAndReturnsTypedEvidence(t *testing.T) {
	engine, storage, prior := prepareSameBootSweptComputerDisk(t)
	engine.computerReimageImageInspect = func(context.Context, PreflightComputerReimageRequest) (computerReimageImageFacts, error) {
		return computerReimageImageFacts{platform: OCIPlatform{OS: "linux", Architecture: "amd64"}, uid: 1000, gid: 1000}, nil
	}
	engine.computerReimageDiskOwner = func(context.Context, string) (uint32, uint32, error) { return 1000, 1000, nil }
	request := testComputerReimageRequest(storage, prior)
	request.Storage.DiskBytes = 1 // The durable manifest, not the transport copy, owns the byte budget.
	started := time.Now()
	response, err := engine.PreflightComputerReimage(t.Context(), request)
	t.Logf("measured engine-level Computer reimage preflight: %s", time.Since(started))
	if err != nil || response.Receipt.Kind != "computer_reimage_preflight_verified" ||
		response.Receipt.StorageEvidenceKind != "computer_reimage_detachment" ||
		response.Receipt.DetachmentAttemptID != prior.AttemptID || response.Receipt.FailureStage != "" {
		t.Fatalf("engine-level Computer reimage preflight = %+v err=%v", response.Receipt, err)
	}
}

func TestPreflightComputerReimageDeadlineReturnsTypedReceipt(t *testing.T) {
	engine, storage, prior := prepareSameBootSweptComputerDisk(t)
	engine.config.ComputerReimagePreflightTimeout = 20 * time.Millisecond
	engine.computerReimageImageInspect = func(ctx context.Context, _ PreflightComputerReimageRequest) (computerReimageImageFacts, error) {
		<-ctx.Done()
		return computerReimageImageFacts{}, ctx.Err()
	}
	engine.computerReimageDiskOwner = func(context.Context, string) (uint32, uint32, error) { return 1000, 1000, nil }
	response, err := engine.PreflightComputerReimage(t.Context(), testComputerReimageRequest(storage, prior))
	if err != nil || response.Receipt.Kind != "computer_reimage_preflight_failed_unchanged" ||
		response.Receipt.FailureStage != "image_identity" || response.Receipt.FailureReason != "deadline_exceeded" ||
		response.Receipt.FailureCode != "computer_reimage_preflight_failed" {
		t.Fatalf("deadline Computer reimage receipt = %+v err=%v", response.Receipt, err)
	}
}

func TestPreflightComputerReimageDoesNotCreateMissingDiskRoot(t *testing.T) {
	root := t.TempDir()
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}}
	storage := testComputerStorage()
	prior := testComputerAuthority("attempt", "fence", "boot")
	engine.computerReimageImageInspect = func(context.Context, PreflightComputerReimageRequest) (computerReimageImageFacts, error) {
		return computerReimageImageFacts{platform: OCIPlatform{OS: "linux", Architecture: "amd64"}}, nil
	}
	response, err := engine.PreflightComputerReimage(t.Context(), testComputerReimageRequest(storage, prior))
	name, _ := deterministicComputerDiskName(storage)
	if err != nil || response.Receipt.FailureStage != "generation_lock" {
		t.Fatalf("missing-root preflight = %+v err=%v", response.Receipt, err)
	}
	if _, statErr := os.Lstat(filepath.Join(root, "computer-disks", name)); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("read-only preflight created empty disk root: %v", statErr)
	}
}

func TestPreflightComputerReimageMissingTransportBudgetReturnsTypedRefusal(t *testing.T) {
	engine, storage, prior := prepareSameBootSweptComputerDisk(t)
	request := testComputerReimageRequest(storage, prior)
	request.Storage.DiskBytes = 0
	response, err := engine.PreflightComputerReimage(t.Context(), request)
	if err != nil || response.Receipt.Kind != "computer_reimage_preflight_failed_unchanged" ||
		response.Receipt.FailureStage != "allocation_verify" || response.Receipt.FailureReason != "operation_failed" ||
		response.Receipt.FailureCode != "computer_reimage_preflight_failed" {
		t.Fatalf("missing transport budget refusal = %+v err=%v", response.Receipt, err)
	}
}

func TestPreflightComputerReimageSerializesTransientAttachAndDeleteContention(t *testing.T) {
	for _, operation := range []string{"attach", "delete"} {
		t.Run(operation, func(t *testing.T) {
			engine, storage, prior := prepareSameBootSweptComputerDisk(t)
			entered := make(chan struct{})
			release := make(chan struct{})
			engine.computerReimageImageInspect = func(context.Context, PreflightComputerReimageRequest) (computerReimageImageFacts, error) {
				close(entered)
				<-release
				return computerReimageImageFacts{platform: OCIPlatform{OS: "linux", Architecture: "amd64"}, uid: 1000, gid: 1000}, nil
			}
			engine.computerReimageDiskOwner = func(context.Context, string) (uint32, uint32, error) { return 1000, 1000, nil }
			preflightDone := make(chan PreflightComputerReimageResponse, 1)
			go func() {
				response, _ := engine.PreflightComputerReimage(t.Context(), testComputerReimageRequest(storage, prior))
				preflightDone <- response
			}()
			<-entered
			operationDone := make(chan error, 1)
			go func() {
				if operation == "attach" {
					attachment, err := engine.attachComputerDisk(t.Context(), storage,
						testComputerAuthority("successor", "successor-fence", prior.BootSessionID))
					if err == nil {
						err = engine.detachComputerDisk(attachment, computerDiskReapReceipt, "")
					}
					operationDone <- err
					return
				}
				operationDone <- engine.deleteComputerDisk(storage, ManagedVolumeRemovalAuthority{NodeID: prior.NodeID,
					BootSessionID: prior.BootSessionID, JobID: "removal-consumer", PriorJobID: prior.JobID,
					RemovalGeneration: 1, CleanupFence: "cleanup-fence"})
			}()
			select {
			case err := <-operationDone:
				t.Fatalf("%s observed transient preflight contention instead of serializing: %v", operation, err)
			case <-time.After(25 * time.Millisecond):
			}
			close(release)
			if response := <-preflightDone; response.Receipt.Kind != "computer_reimage_preflight_verified" {
				t.Fatalf("preflight receipt = %+v", response.Receipt)
			}
			if err := <-operationDone; err != nil {
				t.Fatalf("serialized %s = %v", operation, err)
			}
		})
	}
}

func TestSlowAttachOnAnotherComputerDoesNotBlockReimagePreflight(t *testing.T) {
	engine, storage, prior := prepareSameBootSweptComputerDisk(t)
	system := engine.diskSystem.(*fakeComputerDiskSystem)
	other := storage
	other.ComputerID = "computer-other"
	other.StorageID = "storage-other"
	other.IntentRevision = 2
	system.allocationHit = make(chan struct{})
	system.allocationGate = make(chan struct{})
	attachDone := make(chan error, 1)
	go func() {
		attachment, err := engine.attachComputerDisk(t.Context(), other,
			testComputerAuthority("other-attempt", "other-fence", prior.BootSessionID))
		if err == nil {
			err = engine.detachComputerDisk(attachment, computerDiskReapReceipt, "")
		}
		attachDone <- err
	}()
	<-system.allocationHit
	engine.config.ComputerReimagePreflightTimeout = 75 * time.Millisecond
	engine.computerReimageImageInspect = func(context.Context, PreflightComputerReimageRequest) (computerReimageImageFacts, error) {
		return computerReimageImageFacts{platform: OCIPlatform{OS: "linux", Architecture: "amd64"}, uid: 1000, gid: 1000}, nil
	}
	engine.computerReimageDiskOwner = func(context.Context, string) (uint32, uint32, error) { return 1000, 1000, nil }
	response, err := engine.PreflightComputerReimage(t.Context(), testComputerReimageRequest(storage, prior))
	close(system.allocationGate)
	if attachErr := <-attachDone; attachErr != nil {
		t.Fatal(attachErr)
	}
	if err != nil || response.Receipt.Kind != "computer_reimage_preflight_verified" {
		t.Fatalf("unrelated slow attach blocked reimage preflight: receipt=%+v err=%v", response.Receipt, err)
	}
}

func TestReimageContextReadJoinsWorkerBeforeReturning(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan struct{})
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	returned := make(chan error, 1)
	go func() {
		_, _, err := readComputerReimageDiskOwnerContext(ctx, func(context.Context, string) (uint32, uint32, error) {
			close(started)
			<-release
			close(finished)
			return 0, 0, nil
		}, "disk")
		returned <- err
	}()
	<-started
	<-ctx.Done()
	select {
	case err := <-returned:
		t.Fatalf("context wrapper returned before joining its worker: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-returned; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("joined context read error = %v", err)
	}
	select {
	case <-finished:
	default:
		t.Fatal("context wrapper returned before the worker finished")
	}
}
