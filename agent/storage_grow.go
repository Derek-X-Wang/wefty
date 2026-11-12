package agent

import (
	"context"
	"fmt"
	"sync"

	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
)

type storageGrowController struct {
	client         *Client
	grower         workloadrunner.ComputerStorageGrower
	nodeID         string
	bootSessionID  string
	rootInstanceID string
	logf           func(string, ...any)
	// failures reports every failed attempt to L1's failure budget (#1608).
	failures *storageFailureReporter
	mu       sync.Mutex
	inflight map[string]struct{}
	wg       sync.WaitGroup
}

func newStorageGrowController(client *Client, grower workloadrunner.ComputerStorageGrower,
	nodeID, bootSessionID, rootInstanceID string, logf func(string, ...any)) *storageGrowController {
	if grower == nil {
		return nil
	}
	return &storageGrowController{client: client, grower: grower, nodeID: nodeID,
		bootSessionID: bootSessionID, rootInstanceID: rootInstanceID, logf: logf,
		inflight: make(map[string]struct{}), failures: newStorageFailureReporter(client, nodeID, bootSessionID, logf)}
}

// storageGrowOperation names the budget a grow directive spends.
func storageGrowOperation(directive l1.ComputerStorageGrowDirective) storageOperationRef {
	return storageOperationRef{computerID: directive.ComputerID, verb: l1.ComputerStorageOperationGrow,
		revision: directive.OperationRevision, stage: l1.ComputerStorageOperationPrepare, fence: directive.OperationFence}
}

func (controller *storageGrowController) process(ctx context.Context, directive l1.ComputerStorageGrowDirective) error {
	if directive.BoundNodeID != controller.nodeID {
		return fmt.Errorf("Computer grow authority belongs to node %q, not %q", directive.BoundNodeID, controller.nodeID)
	}
	// Recovery passes call process directly, so the pace L1 answered is
	// honoured here as well as at the in-flight reservation.
	operation := storageGrowOperation(directive)
	if !controller.failures.due(operation) {
		return nil
	}
	return controller.failures.settle(ctx, operation, controller.attempt(ctx, directive))
}

func (controller *storageGrowController) attempt(ctx context.Context, directive l1.ComputerStorageGrowDirective) error {
	if directive.RootInstanceID != controller.rootInstanceID {
		return refuseStorageOperation("managed_root_instance_mismatch",
			fmt.Errorf("Computer grow belongs to managed-root instance %q, not %q", directive.RootInstanceID, controller.rootInstanceID))
	}
	receipt, err := controller.grower.GrowComputerStorage(ctx, workloadrunner.ComputerStorageGrowRequest{
		Storage: workloadrunner.ComputerStorage{ComputerID: directive.ComputerID, StorageID: directive.StorageID,
			StorageGeneration: directive.StorageGeneration, IntentRevision: directive.OperationRevision,
			DiskBytes: directive.OldDiskBytes},
		NewDiskBytes: directive.NewDiskBytes, NodeID: controller.nodeID, BootSessionID: controller.bootSessionID,
		RootInstanceID: directive.RootInstanceID, JobID: directive.JobID,
		OperationRevision: directive.OperationRevision, OperationFence: directive.OperationFence,
	})
	if err != nil {
		return err
	}
	_, err = controller.client.AcknowledgeComputerStorageGrow(ctx, directive.ComputerID,
		l1.ComputerStorageGrowAcknowledgementRequest{NodeID: controller.nodeID, BootSessionID: controller.bootSessionID,
			IdempotencyKey: fmt.Sprintf("computer-grow:%s:%d", directive.ComputerID, directive.OperationRevision), Receipt: receipt})
	return err
}

func (controller *storageGrowController) enqueue(ctx context.Context, directive l1.ComputerStorageGrowDirective, failures chan<- destinationError) {
	key := fmt.Sprintf("%s\x00%d", directive.ComputerID, directive.OperationRevision)
	controller.mu.Lock()
	if _, exists := controller.inflight[key]; exists || !controller.failures.due(storageGrowOperation(directive)) {
		controller.mu.Unlock()
		return
	}
	controller.inflight[key] = struct{}{}
	controller.wg.Add(1)
	controller.mu.Unlock()
	go func() {
		defer controller.wg.Done()
		defer func() { controller.mu.Lock(); delete(controller.inflight, key); controller.mu.Unlock() }()
		if err := controller.process(ctx, directive); err != nil && ctx.Err() == nil {
			classification := classifyAgentProtocolError(err)
			if classification.destination == errorDestinationNodeSession {
				select {
				case failures <- destinationError{destination: classification.destination, err: err}:
				default:
				}
			} else if controller.logf != nil {
				controller.logf("agent: grow Computer Storage %q: %v", directive.ComputerID, err)
			}
		}
	}()
}

func (controller *storageGrowController) wait() { controller.wg.Wait() }
