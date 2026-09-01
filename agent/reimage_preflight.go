package agent

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
)

const computerReimagePreflightRetryLimit = 3

type reimagePreflightController struct {
	client         *Client
	preflighter    workloadrunner.ComputerReimagePreflighter
	nodeID         string
	bootSessionID  string
	rootInstanceID string
	logf           func(string, ...any)
	mu             sync.Mutex
	inflight       map[string]struct{}
	retryCounts    map[string]int
	wg             sync.WaitGroup
}

func newReimagePreflightController(client *Client, preflighter workloadrunner.ComputerReimagePreflighter,
	nodeID, bootSessionID, rootInstanceID string, logf func(string, ...any),
) *reimagePreflightController {
	if preflighter == nil {
		return nil
	}
	return &reimagePreflightController{client: client, preflighter: preflighter, nodeID: nodeID,
		bootSessionID: bootSessionID, rootInstanceID: rootInstanceID, logf: logf,
		inflight: make(map[string]struct{}), retryCounts: make(map[string]int)}
}

func (controller *reimagePreflightController) deferTransientFailure(
	directive l1.ComputerReimagePreflightDirective,
	receipt l1.ComputerReimagePreflightReceipt,
) bool {
	key := fmt.Sprintf("%s\x00%d", directive.ComputerID, directive.OperationRevision)
	retryable := receipt.Kind == "computer_reimage_preflight_failed_unchanged" &&
		(receipt.FailureReason == "detachment_required" ||
			(receipt.FailureStage == "generation_lock" && receipt.FailureReason == "deadline_exceeded"))
	controller.mu.Lock()
	defer controller.mu.Unlock()
	if controller.retryCounts == nil {
		controller.retryCounts = make(map[string]int)
	}
	if !retryable {
		delete(controller.retryCounts, key)
		return false
	}
	controller.retryCounts[key]++
	if controller.retryCounts[key] < computerReimagePreflightRetryLimit {
		return true
	}
	delete(controller.retryCounts, key)
	return false
}

func (controller *reimagePreflightController) process(ctx context.Context, directive l1.ComputerReimagePreflightDirective) error {
	if directive.BoundNodeID != controller.nodeID || directive.RootInstanceID != controller.rootInstanceID || directive.TargetImage.Digest == nil {
		return fmt.Errorf("Computer reimage preflight authority belongs to another Node or managed-root instance")
	}
	started := time.Now()
	receipt, err := controller.preflighter.PreflightComputerReimage(ctx, computerReimagePreflightRequest(directive,
		controller.nodeID, controller.bootSessionID))
	if err != nil {
		return err
	}
	if controller.logf != nil {
		controller.logf("agent: Computer reimage preflight %q completed stage=%q kind=%q in %s",
			directive.ComputerID, receipt.FailureStage, receipt.Kind, time.Since(started))
	}
	if controller.deferTransientFailure(directive, l1.ComputerReimagePreflightReceipt{
		Kind: receipt.Kind, FailureStage: receipt.FailureStage, FailureReason: receipt.FailureReason,
	}) {
		if controller.logf != nil {
			controller.logf("agent: Computer reimage preflight %q deferred retryable stage=%q reason=%q until next poll",
				directive.ComputerID, receipt.FailureStage, receipt.FailureReason)
		}
		return nil
	}
	l1Receipt := l1.ComputerReimagePreflightReceipt{Kind: receipt.Kind, ReceiptID: receipt.ReceiptID,
		ComputerID: receipt.ComputerID, StorageID: receipt.StorageID, StorageGeneration: receipt.StorageGeneration,
		OldJobID: receipt.OldJobID, StagingJobID: receipt.StagingJobID, NodeID: receipt.NodeID,
		RootInstanceID: receipt.RootInstanceID, OperationRevision: receipt.OperationRevision,
		OperationFence: receipt.OperationFence, TargetDigest: receipt.TargetDigest, PlatformOS: receipt.PlatformOS,
		PlatformArchitecture: receipt.PlatformArchitecture, ImageUID: receipt.ImageUID, ImageGID: receipt.ImageGID,
		DiskRootUID: receipt.DiskRootUID, DiskRootGID: receipt.DiskRootGID,
		StorageEvidenceKind: receipt.StorageEvidenceKind, DetachmentReceiptID: receipt.DetachmentReceiptID,
		DetachmentAttemptID: receipt.DetachmentAttemptID, DetachmentFencingToken: receipt.DetachmentFencingToken,
		ResetPreparationReceiptID: receipt.ResetPreparationReceiptID, HelperGeneration: receipt.HelperGeneration,
		FailureCode: receipt.FailureCode, FailureStage: receipt.FailureStage, FailureReason: receipt.FailureReason}
	_, err = controller.client.AcknowledgeComputerReimagePreflight(ctx, directive.ComputerID,
		l1.ComputerReimagePreflightAcknowledgementRequest{NodeID: controller.nodeID, BootSessionID: controller.bootSessionID,
			IdempotencyKey: fmt.Sprintf("computer-reimage-preflight:%s:%d", directive.ComputerID, directive.OperationRevision),
			Receipt:        l1Receipt})
	return err
}

func computerReimagePreflightRequest(directive l1.ComputerReimagePreflightDirective, nodeID, bootSessionID string) workloadrunner.ComputerReimagePreflightRequest {
	return workloadrunner.ComputerReimagePreflightRequest{
		Storage: workloadrunner.ComputerStorage{ComputerID: directive.ComputerID, StorageID: directive.StorageID,
			StorageGeneration: directive.StorageGeneration, IntentRevision: directive.OperationRevision, DiskBytes: directive.DiskBytes},
		OldJobID: directive.OldJobID, StagingJobID: directive.StagingJobID, NodeID: nodeID,
		BootSessionID: bootSessionID, RootInstanceID: directive.RootInstanceID,
		OperationRevision: directive.OperationRevision, OperationFence: directive.OperationFence,
		TargetReference: directive.TargetImage.Reference, TargetDigest: *directive.TargetImage.Digest, Chown: directive.Chown,
	}
}

func (controller *reimagePreflightController) enqueue(ctx context.Context, directive l1.ComputerReimagePreflightDirective, failures chan<- destinationError) {
	key := fmt.Sprintf("%s\x00%d", directive.ComputerID, directive.OperationRevision)
	controller.mu.Lock()
	if _, exists := controller.inflight[key]; exists {
		controller.mu.Unlock()
		return
	}
	controller.inflight[key] = struct{}{}
	controller.wg.Add(1)
	controller.mu.Unlock()
	go func() {
		defer controller.wg.Done()
		defer func() { controller.mu.Lock(); delete(controller.inflight, key); controller.mu.Unlock() }()
		if err := controller.process(ctx, directive); err != nil && ctx.Err() == nil && controller.logf != nil {
			controller.logf("agent: preflight Computer reimage %q: %v", directive.ComputerID, err)
		}
	}()
}

func (controller *reimagePreflightController) wait() { controller.wg.Wait() }
