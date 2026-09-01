//go:build linux

package ocihelper

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/containerd/platforms"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"golang.org/x/sys/unix"
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
	kind, receiptID, attemptID, fencingToken, resetPreparationReceiptID string
}

func reimageDetachmentEvidence(manifest computerDiskManifest, request PreflightComputerReimageRequest) (computerReimageDetachmentEvidence, bool) {
	if evidence := manifest.PreviousDetachment; evidence != nil && evidence.ReceiptID != "" &&
		evidence.ComputerID == request.Storage.ComputerID && evidence.StorageID == request.Storage.StorageID &&
		evidence.StorageGeneration == request.Storage.StorageGeneration && evidence.NodeID == request.Authority.NodeID &&
		evidence.JobID == request.Authority.OldJobID && evidence.AttemptID != "" && evidence.FencingToken != "" &&
		evidence.BootSessionID != "" && (evidence.Kind == computerDiskReapReceipt || evidence.Kind == computerDiskSweepReceipt) {
		return computerReimageDetachmentEvidence{kind: "computer_reimage_detachment", receiptID: evidence.ReceiptID,
			attemptID: evidence.AttemptID, fencingToken: evidence.FencingToken}, true
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
	return computerReimageDetachmentEvidence{kind: "computer_reimage_reset_preparation",
		resetPreparationReceiptID: receipt.ReceiptID}, true
}

type computerReimageImageFacts struct {
	platform OCIPlatform
	uid      uint32
	gid      uint32
}

func computerReimageReceipt(request PreflightComputerReimageRequest) ComputerReimagePreflightReceipt {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%d\x00%d", request.Authority.OperationFence,
		request.Authority.HelperGeneration, request.Authority.OperationRevision)))
	receiptID := fmt.Sprintf("reimage-preflight-%x", sum)
	return ComputerReimagePreflightReceipt{ReceiptID: receiptID,
		ComputerID: request.Storage.ComputerID, StorageID: request.Storage.StorageID,
		StorageGeneration: request.Storage.StorageGeneration, OldJobID: request.Authority.OldJobID,
		StagingJobID: request.Authority.StagingJobID, NodeID: request.Authority.NodeID,
		RootInstanceID: request.Authority.RootInstanceID, OperationRevision: request.Authority.OperationRevision,
		OperationFence: request.Authority.OperationFence, TargetDigest: request.TargetImage.Digest,
		PlatformOS: request.TargetImage.Platform.OS, PlatformArchitecture: request.TargetImage.Platform.Architecture,
		HelperGeneration: request.Authority.HelperGeneration}
}

func failedComputerReimagePreflight(receipt ComputerReimagePreflightReceipt, stage, reason, code string) PreflightComputerReimageResponse {
	receipt.Kind = "computer_reimage_preflight_failed_unchanged"
	receipt.FailureStage = stage
	receipt.FailureReason = reason
	receipt.FailureCode = code
	return PreflightComputerReimageResponse{Receipt: receipt}
}

func lockComputerReimageMutex(ctx context.Context, mutex *sync.Mutex) bool {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		if mutex.TryLock() {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
		}
	}
}

func openExistingComputerDiskLock(ctx context.Context, root string) (*os.File, error) {
	type openResult struct {
		lock *os.File
		err  error
	}
	opened := make(chan openResult, 1)
	go func() {
		lock, err := os.OpenFile(filepath.Join(root, "attachment.lock"), os.O_RDWR, 0)
		opened <- openResult{lock: lock, err: err}
	}()
	var lock *os.File
	select {
	case result := <-opened:
		if result.err != nil {
			return nil, result.err
		}
		lock = result.lock
	case <-ctx.Done():
		// Join the open before returning so a descriptor cannot arrive after
		// cancellation and become stranded in the buffered result channel.
		result := <-opened
		if result.lock != nil {
			_ = result.lock.Close()
		}
		return nil, ctx.Err()
	}
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err == nil {
			return lock, nil
		} else if !errors.Is(err, unix.EWOULDBLOCK) && !errors.Is(err, unix.EAGAIN) {
			_ = lock.Close()
			return nil, err
		}
		select {
		case <-ctx.Done():
			_ = lock.Close()
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}

func readComputerDiskManifestContext(ctx context.Context, path string) (computerDiskManifest, bool, error) {
	type readResult struct {
		manifest computerDiskManifest
		present  bool
		err      error
	}
	result := make(chan readResult, 1)
	go func() {
		manifest, present, err := readComputerDiskManifest(path)
		result <- readResult{manifest: manifest, present: present, err: err}
	}()
	select {
	case read := <-result:
		return read.manifest, read.present, read.err
	case <-ctx.Done():
		// The caller still owns the generation flock. Join every filesystem
		// reader before cancellation can release that authority.
		<-result
		return computerDiskManifest{}, false, ctx.Err()
	}
}

func verifyComputerDiskAllocationContext(ctx context.Context, path string, bytes int64) error {
	result := make(chan error, 1)
	go func() { result <- verifyComputerDiskAllocation(path, bytes) }()
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		<-result
		return ctx.Err()
	}
}

func readComputerReimageDiskOwnerContext(ctx context.Context, readOwner func(context.Context, string) (uint32, uint32, error), path string) (uint32, uint32, error) {
	type ownerResult struct {
		uid uint32
		gid uint32
		err error
	}
	result := make(chan ownerResult, 1)
	go func() {
		uid, gid, err := readOwner(ctx, path)
		result <- ownerResult{uid: uid, gid: gid, err: err}
	}()
	select {
	case owner := <-result:
		return owner.uid, owner.gid, owner.err
	case <-ctx.Done():
		<-result
		return 0, 0, ctx.Err()
	}
}

func (engine *ContainerdEngine) inspectComputerReimageImage(ctx context.Context, request PreflightComputerReimageRequest) (computerReimageImageFacts, error) {
	if engine.computerReimageImageInspect != nil {
		facts, err := engine.computerReimageImageInspect(ctx, request)
		return facts, reimagePreflightStageError("image_identity", err)
	}
	platform := platforms.OnlyStrict(platforms.Normalize(ocispec.Platform{OS: request.TargetImage.Platform.OS,
		Architecture: request.TargetImage.Platform.Architecture, Variant: request.TargetImage.Platform.Variant}))
	image, evidence, err := engine.localImageForPlatform(engineContext(ctx), request.TargetImage.Reference,
		request.TargetImage.Digest, platform)
	if err != nil {
		return computerReimageImageFacts{}, reimagePreflightStageError("image_identity", err)
	}
	if evidence.TopLevelDigest != request.TargetImage.Digest ||
		evidence.Platform != request.TargetImage.Platform {
		return computerReimageImageFacts{}, reimagePreflightStageError("image_identity",
			errors.New("Computer reimage image preflight did not verify exact platform identity"))
	}
	imageConfig, err := readImageRuntimeConfig(engineContext(ctx), engine.client.ContentStore(), image)
	if err != nil {
		return computerReimageImageFacts{}, reimagePreflightStageError("image_config", err)
	}
	imageUID, imageGID, err := numericImageOwner(imageConfig.User)
	if err != nil {
		return computerReimageImageFacts{}, reimagePreflightStageError("image_owner", err)
	}
	return computerReimageImageFacts{platform: evidence.Platform, uid: imageUID, gid: imageGID}, nil
}

func (engine *ContainerdEngine) inspectComputerReimageImageContext(ctx context.Context, request PreflightComputerReimageRequest) (computerReimageImageFacts, error) {
	type inspectResult struct {
		facts computerReimageImageFacts
		err   error
	}
	result := make(chan inspectResult, 1)
	go func() {
		facts, err := engine.inspectComputerReimageImage(ctx, request)
		result <- inspectResult{facts: facts, err: err}
	}()
	select {
	case inspected := <-result:
		return inspected.facts, inspected.err
	case <-ctx.Done():
		<-result
		return computerReimageImageFacts{}, reimagePreflightStageError("image_identity", ctx.Err())
	}
}

func preflightFailure(err error) (stage, reason, code string) {
	stage, reason, code = "manifest_read", "operation_failed", "computer_reimage_preflight_failed"
	var staged *computerReimagePreflightStageError
	if errors.As(err, &staged) {
		stage = staged.stage
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		reason = "deadline_exceeded"
	}
	if errors.Is(err, errComputerReimageDetachmentRequired) {
		stage, reason = "manifest_read", "detachment_required"
	}
	var mechanics *ImageMechanicsError
	if errors.As(err, &mechanics) {
		code, reason = "image_unavailable", "image_unavailable"
		if mechanics.Fact.Kind == ImageFailurePlatformMismatch {
			code, reason = "image_platform_unsupported", "image_platform_unsupported"
		}
	}
	return stage, reason, code
}

func (engine *ContainerdEngine) PreflightComputerReimage(ctx context.Context, request PreflightComputerReimageRequest) (PreflightComputerReimageResponse, error) {
	timeout := engine.config.ComputerReimagePreflightTimeout
	if timeout <= 0 {
		timeout = defaultComputerReimagePreflightTimeout
	}
	preflightCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	receipt := computerReimageReceipt(request)
	if request.Storage.DiskBytes <= 0 {
		return failedComputerReimagePreflight(receipt, "allocation_verify", "operation_failed", "computer_reimage_preflight_failed"), nil
	}
	if !lockComputerReimageMutex(preflightCtx, &engine.computerReimageMu) {
		return failedComputerReimagePreflight(receipt, "generation_lock", "deadline_exceeded", "computer_reimage_preflight_failed"), nil
	}
	defer engine.computerReimageMu.Unlock()
	if !lockComputerReimageMutex(preflightCtx, &engine.storageResetMu) {
		return failedComputerReimagePreflight(receipt, "generation_lock", "deadline_exceeded", "computer_reimage_preflight_failed"), nil
	}
	defer engine.storageResetMu.Unlock()
	if !lockComputerReimageMutex(preflightCtx, &engine.computerBackupMu) {
		return failedComputerReimagePreflight(receipt, "generation_lock", "deadline_exceeded", "computer_reimage_preflight_failed"), nil
	}
	defer engine.computerBackupMu.Unlock()

	imageFacts, imageErr := engine.inspectComputerReimageImageContext(preflightCtx, request)
	if imageErr != nil && preflightCtx.Err() != nil {
		stage, reason, code := preflightFailure(imageErr)
		return failedComputerReimagePreflight(receipt, stage, reason, code), nil
	}
	name, err := deterministicComputerDiskName(request.Storage)
	if err != nil {
		stage, reason, code := preflightFailure(reimagePreflightStageError("manifest_read", err))
		return failedComputerReimagePreflight(receipt, stage, reason, code), nil
	}
	diskRoot := filepath.Join(engine.config.RuntimeRoot, "computer-disks", name)
	imagePath := filepath.Join(diskRoot, "disk.ext4")
	lock, err := openExistingComputerDiskLock(preflightCtx, diskRoot)
	if err != nil {
		stage, reason, code := preflightFailure(reimagePreflightStageError("generation_lock", err))
		return failedComputerReimagePreflight(receipt, stage, reason, code), nil
	}
	manifest, present, err := readComputerDiskManifestContext(preflightCtx, filepath.Join(diskRoot, "attachment.json"))
	if err != nil {
		closeComputerDiskLock(lock)
		stage, reason, code := preflightFailure(reimagePreflightStageError("manifest_read", err))
		return failedComputerReimagePreflight(receipt, stage, reason, code), nil
	}
	detachment, detached := reimageDetachmentEvidence(manifest, request)
	if !present || !sameComputerStorageIdentity(manifest.Storage, request.Storage) || manifest.DiskImage != "disk.ext4" ||
		manifest.MountDirectory != name || manifest.Retirement != nil || manifest.Attached != nil || manifest.Pending != nil || !detached {
		closeComputerDiskLock(lock)
		stage, reason, code := preflightFailure(errComputerReimageDetachmentRequired)
		return failedComputerReimagePreflight(receipt, stage, reason, code), nil
	}
	receipt.StorageEvidenceKind = detachment.kind
	receipt.DetachmentReceiptID = detachment.receiptID
	receipt.DetachmentAttemptID = detachment.attemptID
	receipt.DetachmentFencingToken = detachment.fencingToken
	receipt.ResetPreparationReceiptID = detachment.resetPreparationReceiptID
	if err := verifyComputerDiskAllocationContext(preflightCtx, imagePath, manifest.Storage.DiskBytes); err != nil {
		closeComputerDiskLock(lock)
		stage, reason, code := preflightFailure(reimagePreflightStageError("allocation_verify", err))
		return failedComputerReimagePreflight(receipt, stage, reason, code), nil
	}
	diskOwner := engine.computerReimageDiskOwner
	if diskOwner == nil {
		diskOwner = readExt4RootOwner
	}
	diskUID, diskGID, err := readComputerReimageDiskOwnerContext(preflightCtx, diskOwner, imagePath)
	closeComputerDiskLock(lock)
	if err != nil {
		stage, reason, code := preflightFailure(reimagePreflightStageError("disk_owner", err))
		return failedComputerReimagePreflight(receipt, stage, reason, code), nil
	}
	if imageErr != nil {
		stage, reason, code := preflightFailure(imageErr)
		return failedComputerReimagePreflight(receipt, stage, reason, code), nil
	}
	if !request.Chown && (imageFacts.uid != diskUID || imageFacts.gid != diskGID) {
		return failedComputerReimagePreflight(receipt, "ownership_match", "operation_failed", "computer_reimage_preflight_failed"), nil
	}
	receipt.Kind = "computer_reimage_preflight_verified"
	receipt.PlatformOS = imageFacts.platform.OS
	receipt.PlatformArchitecture = imageFacts.platform.Architecture
	receipt.ImageUID = imageFacts.uid
	receipt.ImageGID = imageFacts.gid
	receipt.DiskRootUID = diskUID
	receipt.DiskRootGID = diskGID
	return PreflightComputerReimageResponse{Receipt: receipt}, nil
}
