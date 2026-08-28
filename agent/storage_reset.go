package agent

import (
	"context"
	"fmt"
	"sync"

	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
)

type storageResetController struct {
	client        *Client
	session       *agentSession
	resetter      workloadrunner.ComputerStorageResetter
	nodeID        string
	bootSessionID string
	logf          func(string, ...any)

	mu       sync.Mutex
	inflight map[string]struct{}
	wg       sync.WaitGroup
}

func newStorageResetController(client *Client, session *agentSession, resetter workloadrunner.ComputerStorageResetter, nodeID, bootSessionID string, logf func(string, ...any)) *storageResetController {
	if resetter == nil {
		return nil
	}
	return &storageResetController{client: client, session: session, resetter: resetter,
		nodeID: nodeID, bootSessionID: bootSessionID, logf: logf, inflight: make(map[string]struct{})}
}

func storageResetKey(directive l1.ComputerStorageResetDirective) string {
	return fmt.Sprintf("%s\x00%d", directive.ComputerID, directive.IntentRevision)
}

func (controller *storageResetController) process(ctx context.Context, directive l1.ComputerStorageResetDirective) error {
	if controller == nil {
		return nil
	}
	if directive.BoundNodeID != controller.nodeID {
		return fmt.Errorf("Computer Storage reset belongs to node %q, not %q", directive.BoundNodeID, controller.nodeID)
	}
	// A local runtime reap is the fast same-boot path. The helper remains the
	// authority: after an agent/helper restart its prior-boot sweep receipt can
	// independently prove detachment.
	if controller.session != nil {
		_, _ = controller.session.reapServiceForRemoval(ctx, directive.JobID)
	}
	receipt, err := controller.resetter.ResetComputerStorage(ctx, workloadrunner.ComputerStorageResetRequest{
		Storage: workloadrunner.ComputerStorage{ComputerID: directive.ComputerID, StorageID: directive.StorageID,
			StorageGeneration: directive.OldGeneration, IntentRevision: directive.IntentRevision, DiskBytes: directive.DiskBytes},
		NewGeneration: directive.NewGeneration, NodeID: controller.nodeID, BootSessionID: controller.bootSessionID,
		JobID: directive.JobID, IntentRevision: directive.IntentRevision, CleanupFence: directive.CleanupFence,
	})
	if err != nil {
		return err
	}
	_, err = controller.client.AcknowledgeComputerStorageReset(ctx, directive.ComputerID, l1.ComputerStorageResetAcknowledgementRequest{
		NodeID: controller.nodeID, BootSessionID: controller.bootSessionID, IdempotencyKey: receipt.ReceiptID,
		Receipt: l1.ComputerStorageResetReceipt{Kind: receipt.Kind, ReceiptID: receipt.ReceiptID,
			ComputerID: receipt.ComputerID, StorageID: receipt.StorageID, OldGeneration: receipt.OldGeneration,
			NewGeneration: receipt.NewGeneration, NodeID: receipt.NodeID, JobID: receipt.JobID,
			IntentRevision: receipt.IntentRevision, CleanupFence: receipt.CleanupFence,
			HelperGeneration: receipt.HelperGeneration},
	})
	if err != nil {
		return err
	}
	if controller.session != nil {
		controller.session.clearRuntimeReap(directive.JobID)
	}
	return nil
}

func (controller *storageResetController) enqueue(ctx context.Context, directive l1.ComputerStorageResetDirective, failures chan<- destinationError) {
	if controller == nil {
		return
	}
	key := storageResetKey(directive)
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
		defer func() {
			controller.mu.Lock()
			delete(controller.inflight, key)
			controller.mu.Unlock()
		}()
		if err := controller.process(ctx, directive); err != nil && ctx.Err() == nil {
			classification := classifyAgentProtocolError(err)
			if classification.destination == errorDestinationNodeSession {
				select {
				case failures <- destinationError{destination: classification.destination, err: fmt.Errorf("agent: reset Computer Storage %q: %w", directive.ComputerID, err)}:
				default:
				}
				return
			}
			if controller.logf != nil {
				controller.logf("agent: reset Computer Storage %q: %v", directive.ComputerID, err)
			}
		}
	}()
}

func (controller *storageResetController) wait() {
	if controller != nil {
		controller.wg.Wait()
	}
}
