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
	mu             sync.Mutex
	inflight       map[string]struct{}
	wg             sync.WaitGroup
}

func newStorageGrowController(client *Client, grower workloadrunner.ComputerStorageGrower,
	nodeID, bootSessionID, rootInstanceID string, logf func(string, ...any)) *storageGrowController {
	if grower == nil {
		return nil
	}
	return &storageGrowController{client: client, grower: grower, nodeID: nodeID,
		bootSessionID: bootSessionID, rootInstanceID: rootInstanceID, logf: logf,
		inflight: make(map[string]struct{})}
}

func (controller *storageGrowController) process(ctx context.Context, directive l1.ComputerStorageGrowDirective) error {
	if directive.BoundNodeID != controller.nodeID || directive.RootInstanceID != controller.rootInstanceID {
		return fmt.Errorf("Computer grow authority belongs to another Node or managed-root instance")
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
