package agent

import (
	"context"
	"fmt"
	"sync"

	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
)

type backupController struct {
	client         *Client
	backupper      workloadrunner.ComputerBackupper
	nodeID         string
	bootSessionID  string
	rootInstanceID string
	logf           func(string, ...any)

	mu       sync.Mutex
	inflight map[string]struct{}
	wg       sync.WaitGroup
}

func newBackupController(client *Client, backupper workloadrunner.ComputerBackupper, nodeID, bootSessionID, rootInstanceID string, logf func(string, ...any)) *backupController {
	if backupper == nil {
		return nil
	}
	return &backupController{client: client, backupper: backupper, nodeID: nodeID,
		bootSessionID: bootSessionID, rootInstanceID: rootInstanceID, logf: logf, inflight: make(map[string]struct{})}
}

func (controller *backupController) processCreate(ctx context.Context, directive l1.ComputerBackupDirective) error {
	if directive.BoundNodeID != controller.nodeID {
		return fmt.Errorf("Computer Backup belongs to node %q, not %q", directive.BoundNodeID, controller.nodeID)
	}
	if directive.RootInstanceID != controller.rootInstanceID || controller.rootInstanceID == "" {
		return fmt.Errorf("Computer Backup belongs to managed-root instance %q, not %q", directive.RootInstanceID, controller.rootInstanceID)
	}
	receipt, err := controller.backupper.CreateComputerBackup(ctx, workloadrunner.ComputerBackupRequest{
		BackupID: directive.BackupID, CopyID: directive.CopyID,
		Storage: workloadrunner.ComputerStorage{ComputerID: directive.ComputerID, StorageID: directive.StorageID,
			StorageGeneration: directive.StorageGeneration, IntentRevision: directive.OperationRevision,
			DiskBytes: directive.AllocatedSize},
		NodeID: controller.nodeID, BootSessionID: controller.bootSessionID,
		RootInstanceID: directive.RootInstanceID, JobID: directive.JobID, PriorJobID: directive.JobID,
		OperationRevision: directive.OperationRevision, CleanupFence: directive.CleanupFence,
	})
	if err != nil {
		return err
	}
	_, err = controller.client.AcknowledgeComputerBackup(ctx, directive.ComputerID,
		l1.ComputerBackupAcknowledgementRequest{NodeID: controller.nodeID, BootSessionID: controller.bootSessionID,
			IdempotencyKey: receipt.ReceiptID, Receipt: receipt})
	return err
}

func (controller *backupController) processPrune(ctx context.Context, directive l1.ComputerBackupPruneDirective) error {
	if directive.BoundNodeID != controller.nodeID {
		return fmt.Errorf("Backup prune belongs to node %q, not %q", directive.BoundNodeID, controller.nodeID)
	}
	if directive.RootInstanceID != controller.rootInstanceID || controller.rootInstanceID == "" {
		return fmt.Errorf("Backup prune belongs to managed-root instance %q, not %q", directive.RootInstanceID, controller.rootInstanceID)
	}
	receipt, err := controller.backupper.DeleteComputerBackupCopy(ctx, workloadrunner.ComputerBackupCopyRemovalRequest{
		BackupID: directive.BackupID, CopyID: directive.CopyID,
		Storage: workloadrunner.ComputerStorage{ComputerID: directive.ComputerID, StorageID: directive.StorageID,
			StorageGeneration: directive.StorageGeneration, IntentRevision: directive.OperationRevision,
			DiskBytes: directive.AllocatedSize},
		NodeID: controller.nodeID, BootSessionID: controller.bootSessionID,
		RootInstanceID: directive.RootInstanceID, OperationRevision: directive.OperationRevision,
		CleanupFence: directive.CleanupFence, Superseded: directive.Superseded,
	})
	if err != nil {
		return err
	}
	_, err = controller.client.AcknowledgeComputerBackupPrune(ctx, directive.ComputerID,
		l1.ComputerBackupPruneAcknowledgementRequest{NodeID: controller.nodeID,
			BootSessionID: controller.bootSessionID, IdempotencyKey: receipt.ReceiptID, Receipt: receipt})
	return err
}

func (controller *backupController) enqueue(ctx context.Context, key string, run func(context.Context) error, failures chan<- destinationError) {
	if controller == nil {
		return
	}
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
		if err := run(ctx); err != nil && ctx.Err() == nil {
			classification := classifyAgentProtocolError(err)
			if classification.destination == errorDestinationNodeSession && failures != nil {
				select {
				case failures <- destinationError{destination: classification.destination, err: fmt.Errorf("agent: reconcile Backup copy %q: %w", key, err)}:
				default:
				}
				return
			}
			if controller.logf != nil {
				controller.logf("agent: reconcile Backup copy %q: %v", key, err)
			}
		}
	}()
}

func (controller *backupController) wait() {
	if controller != nil {
		controller.wg.Wait()
	}
}
