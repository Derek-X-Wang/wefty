//go:build linux

package ocihelper

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/containerd/platforms"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

func numericImageOwner(user string) (uint32, uint32, error) {
	if strings.TrimSpace(user) == "" {
		return 0, 0, nil
	}
	uidText, gidText, hasGID := strings.Cut(user, ":")
	uid, err := strconv.ParseUint(uidText, 10, 32)
	if err != nil {
		return 0, 0, errors.New("Computer reimage preflight requires a numeric image user")
	}
	gid := uint64(0)
	if hasGID {
		gid, err = strconv.ParseUint(gidText, 10, 32)
		if err != nil {
			return 0, 0, errors.New("Computer reimage preflight requires a numeric image group")
		}
	}
	return uint32(uid), uint32(gid), nil
}

func readExt4RootOwner(ctx context.Context, imagePath string) (uint32, uint32, error) {
	debugfs, err := findRootTool("debugfs")
	if err != nil {
		return 0, 0, err
	}
	output, err := exec.CommandContext(ctx, debugfs, "-R", "stat <2>", imagePath).CombinedOutput()
	if err != nil {
		return 0, 0, fmt.Errorf("read Computer disk root ownership: %w: %s", err, strings.TrimSpace(string(output)))
	}
	fields := strings.Fields(string(output))
	var uid, gid uint64
	foundUID, foundGID := false, false
	for index := 0; index+1 < len(fields); index++ {
		label := strings.TrimSuffix(fields[index], ":")
		value, parseErr := strconv.ParseUint(fields[index+1], 10, 32)
		if parseErr != nil {
			continue
		}
		switch label {
		case "User":
			uid, foundUID = value, true
		case "Group":
			gid, foundGID = value, true
		}
	}
	if !foundUID || !foundGID {
		return 0, 0, errors.New("Computer disk root ownership readback was incomplete")
	}
	return uint32(uid), uint32(gid), nil
}

type computerReimageDetachmentEvidence struct {
	receiptID, attemptID, fencingToken string
}

func reimageDetachmentEvidence(manifest computerDiskManifest, request PreflightComputerReimageRequest) (computerReimageDetachmentEvidence, bool) {
	if evidence := manifest.PreviousDetachment; evidence != nil && evidence.ReceiptID != "" &&
		evidence.ComputerID == request.Storage.ComputerID && evidence.StorageID == request.Storage.StorageID &&
		evidence.StorageGeneration == request.Storage.StorageGeneration && evidence.NodeID == request.Authority.NodeID &&
		evidence.JobID == request.Authority.OldJobID && evidence.AttemptID != "" && evidence.FencingToken != "" &&
		evidence.BootSessionID != "" && (evidence.Kind == computerDiskReapReceipt || evidence.Kind == computerDiskSweepReceipt) {
		return computerReimageDetachmentEvidence{evidence.ReceiptID, evidence.AttemptID, evidence.FencingToken}, true
	}
	// A newly published reset generation can be reimaged before its first
	// attachment. Its verified preparation receipt is stronger than a detach
	// receipt: it proves that this exact generation contains fresh helper-owned
	// bytes and has never carried an attempt.
	preparation, receipt := manifest.Preparation, manifest.PreparationReceipt
	if !manifest.Prepared || preparation == nil || receipt == nil || manifest.PreviousDetachment != nil ||
		receipt.Kind != "computer_storage_reset_verified" || receipt.ReceiptID == "" || receipt.HelperGeneration == 0 ||
		receipt.ComputerID != request.Storage.ComputerID || receipt.StorageID != request.Storage.StorageID ||
		receipt.NewGeneration != request.Storage.StorageGeneration || receipt.OldGeneration+1 != receipt.NewGeneration ||
		receipt.NodeID != request.Authority.NodeID || receipt.RootInstanceID != request.Authority.RootInstanceID ||
		receipt.JobID != request.Authority.OldJobID || receipt.IntentRevision != manifest.Storage.IntentRevision ||
		receipt.CleanupFence == "" || preparation.NodeID != receipt.NodeID ||
		preparation.RootInstanceID != receipt.RootInstanceID || preparation.JobID != receipt.JobID ||
		preparation.PriorJobID != receipt.JobID || preparation.IntentRevision != receipt.IntentRevision ||
		preparation.CleanupFence != receipt.CleanupFence || preparation.HelperGeneration != receipt.HelperGeneration {
		return computerReimageDetachmentEvidence{}, false
	}
	return computerReimageDetachmentEvidence{receipt.ReceiptID,
		fmt.Sprintf("storage-reset-%d", receipt.IntentRevision), receipt.CleanupFence}, true
}

func (engine *ContainerdEngine) PreflightComputerReimage(ctx context.Context, request PreflightComputerReimageRequest) (PreflightComputerReimageResponse, error) {
	engine.storageResetMu.Lock()
	defer engine.storageResetMu.Unlock()
	engine.computerBackupMu.Lock()
	defer engine.computerBackupMu.Unlock()
	name, err := deterministicComputerDiskName(request.Storage)
	if err != nil {
		return PreflightComputerReimageResponse{}, err
	}
	diskRoot := filepath.Join(engine.config.RuntimeRoot, "computer-disks", name)
	imagePath := filepath.Join(diskRoot, "disk.ext4")
	// Reimage preflight requires detached Storage. Use the generation flock
	// directly: a completed detach can leave its immutable attempt record in the
	// engine map, and the grow helper intentionally treats that record as a race
	// because grow is also allowed while attached.
	lock, err := openComputerDiskLock(diskRoot)
	if err != nil {
		return PreflightComputerReimageResponse{}, reimagePreflightStageError("generation_lock", err)
	}
	defer closeComputerDiskLock(lock)
	manifest, present, err := readComputerDiskManifest(filepath.Join(diskRoot, "attachment.json"))
	if err != nil {
		return PreflightComputerReimageResponse{}, reimagePreflightStageError("manifest_read", err)
	}
	detachment, detached := reimageDetachmentEvidence(manifest, request)
	if !present || !sameComputerStorageIdentity(manifest.Storage, request.Storage) ||
		manifest.Attached != nil || manifest.Pending != nil || !detached {
		return PreflightComputerReimageResponse{}, errComputerReimageDetachmentRequired
	}
	if err := verifyComputerDiskAllocation(imagePath, request.Storage.DiskBytes); err != nil {
		return PreflightComputerReimageResponse{}, reimagePreflightStageError("allocation_verify", err)
	}
	receiptID, err := randomCapability()
	if err != nil {
		return PreflightComputerReimageResponse{}, reimagePreflightStageError("receipt_create", err)
	}
	receipt := ComputerReimagePreflightReceipt{ReceiptID: receiptID,
		ComputerID: request.Storage.ComputerID, StorageID: request.Storage.StorageID,
		StorageGeneration: request.Storage.StorageGeneration, OldJobID: request.Authority.OldJobID,
		StagingJobID: request.Authority.StagingJobID, NodeID: request.Authority.NodeID,
		RootInstanceID: request.Authority.RootInstanceID, OperationRevision: request.Authority.OperationRevision,
		OperationFence: request.Authority.OperationFence, TargetDigest: request.TargetImage.Digest,
		PlatformOS: request.TargetImage.Platform.OS, PlatformArchitecture: request.TargetImage.Platform.Architecture,
		DetachmentReceiptID:    detachment.receiptID,
		DetachmentAttemptID:    detachment.attemptID,
		DetachmentFencingToken: detachment.fencingToken,
		HelperGeneration:       request.Authority.HelperGeneration}
	platform := platforms.OnlyStrict(platforms.Normalize(ocispec.Platform{OS: request.TargetImage.Platform.OS,
		Architecture: request.TargetImage.Platform.Architecture, Variant: request.TargetImage.Platform.Variant}))
	image, evidence, err := engine.localImageForPlatform(engineContext(ctx), request.TargetImage.Reference,
		request.TargetImage.Digest, platform)
	if err != nil {
		receipt.Kind = "computer_reimage_preflight_failed_unchanged"
		receipt.FailureCode = "image_unavailable"
		var mechanics *ImageMechanicsError
		if errors.As(err, &mechanics) && mechanics.Fact.Kind == ImageFailurePlatformMismatch {
			receipt.FailureCode = "image_platform_unsupported"
		}
		return PreflightComputerReimageResponse{Receipt: receipt}, nil
	}
	if evidence.TopLevelDigest != request.TargetImage.Digest ||
		evidence.Platform != request.TargetImage.Platform {
		return PreflightComputerReimageResponse{}, reimagePreflightStageError("image_identity",
			errors.New("Computer reimage image preflight did not verify exact platform identity"))
	}
	imageConfig, err := readImageRuntimeConfig(engineContext(ctx), engine.client.ContentStore(), image)
	if err != nil {
		return PreflightComputerReimageResponse{}, reimagePreflightStageError("image_config", err)
	}
	imageUID, imageGID, err := numericImageOwner(imageConfig.User)
	if err != nil {
		return PreflightComputerReimageResponse{}, reimagePreflightStageError("image_owner", err)
	}
	diskUID, diskGID, err := readExt4RootOwner(ctx, imagePath)
	if err != nil {
		return PreflightComputerReimageResponse{}, reimagePreflightStageError("disk_owner", err)
	}
	if !request.Chown && (imageUID != diskUID || imageGID != diskGID) {
		return PreflightComputerReimageResponse{}, reimagePreflightStageError("ownership_match",
			errors.New("Computer reimage image user does not own the detached disk root"))
	}
	receipt.Kind = "computer_reimage_preflight_verified"
	receipt.PlatformOS = evidence.Platform.OS
	receipt.PlatformArchitecture = evidence.Platform.Architecture
	receipt.ImageUID = imageUID
	receipt.ImageGID = imageGID
	receipt.DiskRootUID = diskUID
	receipt.DiskRootGID = diskGID
	receipt.FailureCode = ""
	return PreflightComputerReimageResponse{Receipt: receipt}, nil
}
