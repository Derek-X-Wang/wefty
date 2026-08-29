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

func validReimageDetachment(evidence *computerDiskEvidence, request PreflightComputerReimageRequest) bool {
	return evidence != nil && evidence.ReceiptID != "" && evidence.ComputerID == request.Storage.ComputerID &&
		evidence.StorageID == request.Storage.StorageID && evidence.StorageGeneration == request.Storage.StorageGeneration &&
		evidence.NodeID == request.Authority.NodeID && evidence.JobID == request.Authority.OldJobID &&
		evidence.AttemptID != "" && evidence.FencingToken != "" && evidence.BootSessionID != "" &&
		(evidence.Kind == computerDiskReapReceipt || evidence.Kind == computerDiskSweepReceipt)
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
	_, release, err := engine.acquireComputerGrowLock(diskRoot, request.Storage)
	if err != nil {
		return PreflightComputerReimageResponse{}, err
	}
	defer release()
	manifest, present, err := readComputerDiskManifest(filepath.Join(diskRoot, "attachment.json"))
	if err != nil {
		return PreflightComputerReimageResponse{}, err
	}
	if !present || !sameComputerStorageIdentity(manifest.Storage, request.Storage) ||
		manifest.Attached != nil || manifest.Pending != nil || !validReimageDetachment(manifest.PreviousDetachment, request) {
		return PreflightComputerReimageResponse{}, errors.New("Computer reimage requires exact positive detachment evidence")
	}
	if err := verifyComputerDiskAllocation(imagePath, request.Storage.DiskBytes); err != nil {
		return PreflightComputerReimageResponse{}, err
	}
	receiptID, err := randomCapability()
	if err != nil {
		return PreflightComputerReimageResponse{}, err
	}
	receipt := ComputerReimagePreflightReceipt{ReceiptID: receiptID,
		ComputerID: request.Storage.ComputerID, StorageID: request.Storage.StorageID,
		StorageGeneration: request.Storage.StorageGeneration, OldJobID: request.Authority.OldJobID,
		StagingJobID: request.Authority.StagingJobID, NodeID: request.Authority.NodeID,
		RootInstanceID: request.Authority.RootInstanceID, OperationRevision: request.Authority.OperationRevision,
		OperationFence: request.Authority.OperationFence, TargetDigest: request.TargetImage.Digest,
		PlatformOS: request.TargetImage.Platform.OS, PlatformArchitecture: request.TargetImage.Platform.Architecture,
		DetachmentReceiptID:    manifest.PreviousDetachment.ReceiptID,
		DetachmentAttemptID:    manifest.PreviousDetachment.AttemptID,
		DetachmentFencingToken: manifest.PreviousDetachment.FencingToken,
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
		return PreflightComputerReimageResponse{}, errors.New("Computer reimage image preflight did not verify exact platform identity")
	}
	imageConfig, err := readImageRuntimeConfig(engineContext(ctx), engine.client.ContentStore(), image)
	if err != nil {
		return PreflightComputerReimageResponse{}, err
	}
	imageUID, imageGID, err := numericImageOwner(imageConfig.User)
	if err != nil {
		return PreflightComputerReimageResponse{}, err
	}
	diskUID, diskGID, err := readExt4RootOwner(ctx, imagePath)
	if err != nil {
		return PreflightComputerReimageResponse{}, err
	}
	if !request.Chown && (imageUID != diskUID || imageGID != diskGID) {
		return PreflightComputerReimageResponse{}, errors.New("Computer reimage image user does not own the detached disk root")
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
