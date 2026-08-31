package agent

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
)

type storageCopyController struct {
	client               *Client
	copier               workloadrunner.ComputerStorageCopier
	backupper            workloadrunner.ComputerBackupper
	finalizeVolumes      func(context.Context, workloadrunner.ManagedVolumeFinalizationRequest) error
	attestRuntimeRemoval func(context.Context, workloadrunner.RuntimeRemovalProofRequest) (workloadrunner.RuntimeRemovalAttestation, error)
	nodeID               string
	bootSessionID        string
	rootInstanceID       string
	logf                 func(string, ...any)

	mu       sync.Mutex
	inflight map[string]struct{}
	wg       sync.WaitGroup
}

func newStorageCopyController(client *Client, copier workloadrunner.ComputerStorageCopier,
	backupper workloadrunner.ComputerBackupper, nodeID, bootSessionID, rootInstanceID string, logf func(string, ...any)) *storageCopyController {
	if copier == nil {
		return nil
	}
	return &storageCopyController{client: client, copier: copier, backupper: backupper,
		nodeID: nodeID, bootSessionID: bootSessionID, rootInstanceID: rootInstanceID,
		logf: logf, inflight: make(map[string]struct{})}
}

func (controller *storageCopyController) process(ctx context.Context, directive l1.ComputerStorageCopyDirective) error {
	if directive.BoundNodeID != controller.nodeID {
		return fmt.Errorf("Computer Storage copy belongs to node %q, not %q", directive.BoundNodeID, controller.nodeID)
	}
	if directive.RootInstanceID != controller.rootInstanceID {
		return fmt.Errorf("Computer Storage copy belongs to managed-root instance %q, not %q", directive.RootInstanceID, controller.rootInstanceID)
	}
	if directive.Phase == "published" {
		if directive.Operation != "restore" {
			return errors.New("only restore has a published predecessor retirement phase")
		}
		return controller.retirePredecessor(ctx, directive)
	}
	var oldBackupReceipt *l1.ComputerBackupCopyReceipt
	if directive.KeepOldBackup {
		if controller.backupper == nil {
			return errors.New("old-generation Backup requires Computer Backup mechanics")
		}
		receipt, err := controller.backupper.CreateComputerBackup(ctx, workloadrunner.ComputerBackupRequest{
			BackupID: directive.OldBackupID, CopyID: directive.OldCopyID,
			Storage: workloadrunner.ComputerStorage{ComputerID: directive.DestinationComputerID,
				StorageID: directive.DestinationStorageID, StorageGeneration: directive.OldGeneration,
				IntentRevision: directive.OperationRevision, DiskBytes: directive.DestinationSize},
			NodeID: controller.nodeID, BootSessionID: controller.bootSessionID,
			RootInstanceID: directive.RootInstanceID, JobID: directive.JobID, PriorJobID: directive.JobID,
			OperationRevision: directive.OperationRevision, CleanupFence: directive.CleanupFence,
		})
		if err != nil {
			return err
		}
		oldBackupReceipt = &receipt
		if receipt.FailureCode != "" || receipt.CopyAbsent {
			_, acknowledgeErr := controller.client.AcknowledgeComputerStorageCopy(ctx, directive.DestinationComputerID,
				l1.ComputerStorageCopyAcknowledgementRequest{NodeID: controller.nodeID,
					BootSessionID: controller.bootSessionID, IdempotencyKey: receipt.ReceiptID,
					OldBackupReceipt: oldBackupReceipt})
			return acknowledgeErr
		}
	}
	receipt, err := controller.copier.CopyComputerStorage(ctx, workloadrunner.ComputerStorageCopyRequest{
		Operation: directive.Operation, BackupID: directive.BackupID, CopyID: directive.CopyID,
		SourceComputerID: directive.SourceComputerID, SourceStorageID: directive.SourceStorageID,
		SourceGeneration: directive.SourceGeneration, SourceSize: directive.SourceSize, SourceDigest: directive.SourceDigest,
		ExportID: directive.ExportID, ExternalPath: directive.ExternalPath, ManifestDigest: directive.ManifestDigest,
		Destination: workloadrunner.ComputerStorage{ComputerID: directive.DestinationComputerID,
			StorageID: directive.DestinationStorageID, StorageGeneration: directive.DestinationGeneration,
			IntentRevision: directive.OperationRevision, DiskBytes: directive.DestinationSize},
		NodeID: controller.nodeID, BootSessionID: controller.bootSessionID,
		RootInstanceID: directive.RootInstanceID, JobID: directive.JobID,
		OperationRevision: directive.OperationRevision, CleanupFence: directive.CleanupFence,
	})
	if err != nil {
		return err
	}
	if directive.Operation == "import" && receipt.Kind == "computer_storage_copy_failed_absent" {
		if err := controller.attestFailedImportCleanup(ctx, directive); err != nil {
			return err
		}
	}
	_, err = controller.client.AcknowledgeComputerStorageCopy(ctx, directive.DestinationComputerID,
		l1.ComputerStorageCopyAcknowledgementRequest{NodeID: controller.nodeID,
			BootSessionID: controller.bootSessionID, IdempotencyKey: receipt.ReceiptID,
			Receipt: receipt, OldBackupReceipt: oldBackupReceipt})
	return err
}

func (controller *storageCopyController) attestFailedImportCleanup(ctx context.Context, directive l1.ComputerStorageCopyDirective) error {
	if controller.finalizeVolumes == nil || controller.attestRuntimeRemoval == nil {
		return errors.New("failed Custody import cleanup requires shared removal mechanics")
	}
	generation := uint64(directive.OperationRevision)
	storage := &workloadrunner.ComputerStorage{ComputerID: directive.DestinationComputerID,
		StorageID: directive.DestinationStorageID, StorageGeneration: directive.DestinationGeneration,
		IntentRevision: directive.OperationRevision, DiskBytes: directive.DestinationSize}
	if err := controller.finalizeVolumes(ctx, workloadrunner.ManagedVolumeFinalizationRequest{
		Volumes: []workloadrunner.ManagedVolume{{Kind: workloadrunner.ManagedVolumeComputerDisk, ComputerStorage: storage}},
		Removal: &workloadrunner.ManagedVolumeRemovalAuthority{NodeID: controller.nodeID,
			BootSessionID: controller.bootSessionID, JobID: directive.JobID, PriorJobID: directive.JobID,
			RemovalGeneration: generation, CleanupFence: directive.CleanupFence},
	}); err != nil {
		return fmt.Errorf("delete failed Custody import staging through shared removal machinery: %w", err)
	}
	manifest := workloadrunner.RuntimeResourceManifest{Version: 1, RuntimeKind: contract.JobKindOCI,
		NodeID: controller.nodeID, BootSessionID: controller.bootSessionID, JobID: directive.JobID,
		AttemptID:    "storage-import-failed-" + fmt.Sprint(directive.OperationRevision),
		FencingToken: directive.CleanupFence, WorkloadClass: contract.JobClassService,
		RemovalGeneration: fmt.Sprint(generation), ComputerStorage: storage, StorageOnly: true}
	attestation, err := controller.attestRuntimeRemoval(ctx, workloadrunner.RuntimeRemovalProofRequest{
		JobID: directive.JobID, RemovalGeneration: generation,
		Attempts: []workloadrunner.RuntimeResourceManifest{manifest},
	})
	if err != nil {
		return fmt.Errorf("attest failed Custody import staging absence: %w", err)
	}
	if err := validateRuntimeRemovalAttestation(runtimeRemovalManifest{Version: 1, JobID: directive.JobID,
		RemovalGeneration: generation, Attempts: []workloadrunner.RuntimeResourceManifest{manifest}}, attestation); err != nil {
		return fmt.Errorf("validate failed Custody import staging absence: %w", err)
	}
	return nil
}

func (controller *storageCopyController) retirePredecessor(ctx context.Context, directive l1.ComputerStorageCopyDirective) error {
	if controller.finalizeVolumes == nil || controller.attestRuntimeRemoval == nil {
		return errors.New("Computer restore retirement requires shared OCI deletion and attestation")
	}
	generation := uint64(directive.OperationRevision)
	storage := &workloadrunner.ComputerStorage{ComputerID: directive.DestinationComputerID,
		StorageID: directive.DestinationStorageID, StorageGeneration: directive.OldGeneration,
		IntentRevision: directive.OperationRevision, DiskBytes: directive.DestinationSize}
	if err := controller.finalizeVolumes(ctx, workloadrunner.ManagedVolumeFinalizationRequest{
		Volumes: []workloadrunner.ManagedVolume{{Kind: workloadrunner.ManagedVolumeComputerDisk, ComputerStorage: storage}},
		Removal: &workloadrunner.ManagedVolumeRemovalAuthority{NodeID: controller.nodeID,
			BootSessionID: controller.bootSessionID, JobID: directive.JobID, PriorJobID: directive.JobID,
			RemovalGeneration: generation, CleanupFence: directive.CleanupFence},
	}); err != nil {
		if acknowledgement, quarantined := storageCleanupQuarantineAcknowledgement(err, directive.RootInstanceID); quarantined {
			_, acknowledgementErr := controller.client.AcknowledgeComputerRestoreRetirement(ctx, directive.DestinationComputerID, acknowledgement)
			return errors.Join(err, acknowledgementErr)
		}
		return fmt.Errorf("delete restore predecessor through shared removal machinery: %w", err)
	}
	manifest := workloadrunner.RuntimeResourceManifest{Version: 1, RuntimeKind: contract.JobKindOCI,
		NodeID: controller.nodeID, BootSessionID: controller.bootSessionID, JobID: directive.JobID,
		AttemptID:    "storage-restore-" + fmt.Sprint(directive.OperationRevision),
		FencingToken: directive.CleanupFence, WorkloadClass: contract.JobClassService,
		RemovalGeneration: fmt.Sprint(generation), ComputerStorage: storage, StorageOnly: true}
	attestation, err := controller.attestRuntimeRemoval(ctx, workloadrunner.RuntimeRemovalProofRequest{
		JobID: directive.JobID, RemovalGeneration: generation,
		Attempts: []workloadrunner.RuntimeResourceManifest{manifest},
	})
	if err != nil {
		return fmt.Errorf("attest restore predecessor through shared removal machinery: %w", err)
	}
	if err := validateRuntimeRemovalAttestation(runtimeRemovalManifest{Version: 1, JobID: directive.JobID,
		RemovalGeneration: generation, Attempts: []workloadrunner.RuntimeResourceManifest{manifest}}, attestation); err != nil {
		return err
	}
	_, err = controller.client.AcknowledgeComputerRestoreRetirement(ctx, directive.DestinationComputerID,
		l1.RemovalAcknowledgementRequest{NodeID: controller.nodeID, BootSessionID: controller.bootSessionID,
			RemovalGeneration: generation, CleanupFence: directive.CleanupFence,
			RootInstanceID: directive.RootInstanceID,
			IdempotencyKey: removalAcknowledgementKey(localRemoval{jobID: directive.JobID,
				generation: generation, cleanupFence: directive.CleanupFence,
				rootInstanceID: directive.RootInstanceID}, controller.bootSessionID)})
	return err
}

func (controller *storageCopyController) enqueue(ctx context.Context, directive l1.ComputerStorageCopyDirective, failures chan<- destinationError) {
	if controller == nil {
		return
	}
	key := directive.String()
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
			if classification.destination == errorDestinationNodeSession && failures != nil {
				select {
				case failures <- destinationError{destination: classification.destination, err: fmt.Errorf("agent: reconcile Storage copy %q: %w", key, err)}:
				default:
				}
				return
			}
			if controller.logf != nil {
				controller.logf("agent: reconcile Storage copy %q: %v", key, err)
			}
		}
	}()
}

func (controller *storageCopyController) wait() {
	if controller != nil {
		controller.wg.Wait()
	}
}
