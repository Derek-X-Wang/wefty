package agent

import (
	"context"
	"fmt"
	"sync"

	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
)

type reimagePreflightController struct {
	client         *Client
	preflighter    workloadrunner.ComputerReimagePreflighter
	nodeID         string
	bootSessionID  string
	rootInstanceID string
	logf           func(string, ...any)
	mu             sync.Mutex
	inflight       map[string]struct{}
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
		inflight: make(map[string]struct{})}
}

func (controller *reimagePreflightController) process(ctx context.Context, directive l1.ComputerReimagePreflightDirective) error {
	if directive.BoundNodeID != controller.nodeID || directive.RootInstanceID != controller.rootInstanceID || directive.TargetImage.Digest == nil {
		return fmt.Errorf("Computer reimage preflight authority belongs to another Node or managed-root instance")
	}
	receipt, err := controller.preflighter.PreflightComputerReimage(ctx, workloadrunner.ComputerReimagePreflightRequest{
		Storage: workloadrunner.ComputerStorage{ComputerID: directive.ComputerID, StorageID: directive.StorageID,
			StorageGeneration: directive.StorageGeneration, IntentRevision: directive.OperationRevision},
		OldJobID: directive.OldJobID, StagingJobID: directive.StagingJobID, NodeID: controller.nodeID,
		BootSessionID: controller.bootSessionID, RootInstanceID: directive.RootInstanceID,
		OperationRevision: directive.OperationRevision, OperationFence: directive.OperationFence,
		TargetReference: directive.TargetImage.Reference, TargetDigest: *directive.TargetImage.Digest, Chown: directive.Chown,
	})
	if err != nil {
		return err
	}
	l1Receipt := l1.ComputerReimagePreflightReceipt{Kind: receipt.Kind, ReceiptID: receipt.ReceiptID,
		ComputerID: receipt.ComputerID, StorageID: receipt.StorageID, StorageGeneration: receipt.StorageGeneration,
		OldJobID: receipt.OldJobID, StagingJobID: receipt.StagingJobID, NodeID: receipt.NodeID,
		RootInstanceID: receipt.RootInstanceID, OperationRevision: receipt.OperationRevision,
		OperationFence: receipt.OperationFence, TargetDigest: receipt.TargetDigest, PlatformOS: receipt.PlatformOS,
		PlatformArchitecture: receipt.PlatformArchitecture, ImageUID: receipt.ImageUID, ImageGID: receipt.ImageGID,
		DiskRootUID: receipt.DiskRootUID, DiskRootGID: receipt.DiskRootGID,
		DetachmentReceiptID: receipt.DetachmentReceiptID, DetachmentAttemptID: receipt.DetachmentAttemptID,
		DetachmentFencingToken: receipt.DetachmentFencingToken, HelperGeneration: receipt.HelperGeneration,
		FailureCode: receipt.FailureCode}
	_, err = controller.client.AcknowledgeComputerReimagePreflight(ctx, directive.ComputerID,
		l1.ComputerReimagePreflightAcknowledgementRequest{NodeID: controller.nodeID, BootSessionID: controller.bootSessionID,
			IdempotencyKey: fmt.Sprintf("computer-reimage-preflight:%s:%d", directive.ComputerID, directive.OperationRevision),
			Receipt:        l1Receipt})
	return err
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
