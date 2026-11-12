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
	// copyRemovalAcknowledged and recordCopyRemovalAcknowledged are the node's
	// durable memory of which Backup copies L1 has already accepted as absent.
	copyRemovalAcknowledged       func(context.Context, l1.ComputerBackupPruneDirective) (bool, error)
	recordCopyRemovalAcknowledged func(context.Context, l1.ComputerBackupPruneDirective) error

	// failures reports every failed creation to L1's failure budget and
	// paces the next one from L1's answer (#1608). The directive comes back
	// on every heartbeat and on every OCI recovery pass until L1 settles it,
	// so without that pace a failure that cannot clear is retried at whatever
	// rate those passes arrive (#558).
	failures *storageFailureReporter
	// beforeCreateReservation is a test seam: it runs after a pass has decided
	// to dispatch a creation and before that creation is reserved.
	beforeCreateReservation func()

	mu       sync.Mutex
	inflight map[string]struct{}
	wg       sync.WaitGroup
}

func newBackupController(client *Client, outbox *evidenceOutbox, backupper workloadrunner.ComputerBackupper, nodeID, bootSessionID, rootInstanceID string, logf func(string, ...any)) *backupController {
	if backupper == nil {
		return nil
	}
	controller := &backupController{client: client, backupper: backupper, nodeID: nodeID,
		bootSessionID: bootSessionID, rootInstanceID: rootInstanceID, logf: logf, inflight: make(map[string]struct{}),
		failures: newStorageFailureReporter(client, nodeID, bootSessionID, logf)}
	if outbox != nil {
		controller.copyRemovalAcknowledged = outbox.backupCopyRemovalAcknowledged
		controller.recordCopyRemovalAcknowledged = outbox.recordBackupCopyRemovalAcknowledged
	}
	return controller
}

func (controller *backupController) processCreate(ctx context.Context, directive l1.ComputerBackupDirective) error {
	if directive.BoundNodeID != controller.nodeID {
		return fmt.Errorf("Computer Backup belongs to node %q, not %q", directive.BoundNodeID, controller.nodeID)
	}
	if directive.RootInstanceID != controller.rootInstanceID || controller.rootInstanceID == "" {
		return refuseStorageOperation("managed_root_instance_mismatch",
			fmt.Errorf("Computer Backup belongs to managed-root instance %q, not %q", directive.RootInstanceID, controller.rootInstanceID))
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
	// A directive list built before this copy's acknowledgement still names it.
	// Deleting it again would ask the helper to prove an absence it already
	// proved and mint a second receipt for it, which L1 refuses -- and that
	// refusal now counts into the removal's stall streak (#501). The
	// acknowledgement is the answer the directive is asking for, so replay it
	// from the node's own durable record instead.
	if controller.copyRemovalAcknowledged != nil {
		acknowledged, err := controller.copyRemovalAcknowledged(ctx, directive)
		if err != nil {
			return err
		}
		if acknowledged {
			return nil
		}
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
	if _, err := controller.client.AcknowledgeComputerBackupPrune(ctx, directive.ComputerID,
		l1.ComputerBackupPruneAcknowledgementRequest{NodeID: controller.nodeID,
			BootSessionID: controller.bootSessionID, IdempotencyKey: receipt.ReceiptID, Receipt: receipt}); err != nil {
		return err
	}
	if controller.recordCopyRemovalAcknowledged != nil {
		return controller.recordCopyRemovalAcknowledged(ctx, directive)
	}
	return nil
}

// backupOperation names the budget a Backup creation spends.
func backupOperation(directive l1.ComputerBackupDirective) storageOperationRef {
	return storageOperationRef{computerID: directive.ComputerID, verb: l1.ComputerStorageOperationBackup,
		revision: directive.OperationRevision, stage: l1.ComputerStorageOperationPrepare, fence: directive.CleanupFence}
}

// enqueueCreate dispatches one Backup creation unless the same copy failed
// recently and its retry is not yet due. A creation that ends in a receipt --
// published or a positive-absence failure such as source_never_detached -- is
// L1's to settle, and the directive stops arriving once it has. A failed one
// is reported to L1, which ends the Backup once its budget is spent.
func (controller *backupController) enqueueCreate(ctx context.Context, directive l1.ComputerBackupDirective, failures chan<- destinationError) {
	if controller == nil {
		return
	}
	key := "create\x00" + directive.CopyID
	if controller.beforeCreateReservation != nil {
		controller.beforeCreateReservation()
	}
	operation := backupOperation(directive)
	// The retry deadline is read in the same critical section that reserves
	// the copy. Checked beforehand, a creation still in flight could fail and
	// release its reservation between the check and the reservation, and this
	// pass would redispatch it with its new deadline unread.
	controller.enqueueWhen(ctx, key, func() bool { return controller.failures.due(operation) },
		func(runContext context.Context) error {
			err := controller.processCreate(runContext, directive)
			if directive.BoundNodeID != controller.nodeID {
				return err
			}
			return controller.failures.settle(runContext, operation, err)
		}, failures)
}

func (controller *backupController) enqueue(ctx context.Context, key string, run func(context.Context) error, failures chan<- destinationError) {
	controller.enqueueWhen(ctx, key, nil, run, failures)
}

// enqueueWhen reserves key and runs it unless it is already in flight or due,
// evaluated under controller.mu together with the reservation, says it is not
// yet time.
func (controller *backupController) enqueueWhen(ctx context.Context, key string, due func() bool, run func(context.Context) error, failures chan<- destinationError) {
	if controller == nil {
		return
	}
	controller.mu.Lock()
	if _, exists := controller.inflight[key]; exists {
		controller.mu.Unlock()
		return
	}
	if due != nil && !due() {
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
