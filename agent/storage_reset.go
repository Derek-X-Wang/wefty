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

type storageResetController struct {
	client               *Client
	session              *agentSession
	resetter             workloadrunner.ComputerStorageResetter
	finalizeVolumes      func(context.Context, workloadrunner.ManagedVolumeFinalizationRequest) error
	attestRuntimeRemoval func(context.Context, workloadrunner.RuntimeRemovalProofRequest) (workloadrunner.RuntimeRemovalAttestation, error)
	nodeID               string
	bootSessionID        string
	logf                 func(string, ...any)
	// failures reports every failed attempt to L1's failure budget (#1608).
	failures *storageFailureReporter

	mu       sync.Mutex
	inflight map[string]struct{}
	wg       sync.WaitGroup
}

func newStorageResetController(client *Client, session *agentSession, resetter workloadrunner.ComputerStorageResetter, nodeID, bootSessionID string, logf func(string, ...any)) *storageResetController {
	if resetter == nil {
		return nil
	}
	return &storageResetController{client: client, session: session, resetter: resetter,
		nodeID: nodeID, bootSessionID: bootSessionID, logf: logf, inflight: make(map[string]struct{}),
		failures: newStorageFailureReporter(client, nodeID, bootSessionID, logf)}
}

// storageResetOperation names the budget a reset directive spends: prepare
// until the new generation is published, then retire.
func storageResetOperation(directive l1.ComputerStorageResetDirective) storageOperationRef {
	stage := l1.ComputerStorageOperationPrepare
	if directive.Phase == "published" {
		stage = l1.ComputerStorageOperationRetire
	}
	return storageOperationRef{computerID: directive.ComputerID, verb: l1.ComputerStorageOperationReset,
		revision: directive.IntentRevision, stage: stage, fence: directive.CleanupFence}
}

func storageResetKey(directive l1.ComputerStorageResetDirective) string {
	return fmt.Sprintf("%s\x00%d", directive.ComputerID, directive.IntentRevision)
}

// process runs one directive from a recovery pass. It takes the same
// in-flight reservation and pace as a heartbeat's enqueue, so the two paths
// never run one operation twice at once or ahead of L1's retry_after.
func (controller *storageResetController) process(ctx context.Context, directive l1.ComputerStorageResetDirective) error {
	if controller == nil {
		return nil
	}
	key, reserved := controller.reserve(directive)
	if !reserved {
		return nil
	}
	defer controller.release(key)
	return controller.run(ctx, directive)
}

func (controller *storageResetController) run(ctx context.Context, directive l1.ComputerStorageResetDirective) error {
	if directive.BoundNodeID != controller.nodeID {
		return fmt.Errorf("Computer Storage reset belongs to node %q, not %q", directive.BoundNodeID, controller.nodeID)
	}
	return controller.failures.settle(ctx, storageResetOperation(directive), controller.attempt(ctx, directive))
}

// reserve claims the directive unless it is already in flight or L1's pace
// says it is not yet due; both are read under the one lock, so a failing
// attempt records its pace before it releases the reservation.
func (controller *storageResetController) reserve(directive l1.ComputerStorageResetDirective) (string, bool) {
	key := storageResetKey(directive)
	controller.mu.Lock()
	defer controller.mu.Unlock()
	if _, exists := controller.inflight[key]; exists || !controller.failures.due(storageResetOperation(directive)) {
		return "", false
	}
	controller.inflight[key] = struct{}{}
	return key, true
}

func (controller *storageResetController) release(key string) {
	controller.mu.Lock()
	delete(controller.inflight, key)
	controller.mu.Unlock()
}

func (controller *storageResetController) attempt(ctx context.Context, directive l1.ComputerStorageResetDirective) error {
	if directive.Phase == "published" {
		return controller.retirePredecessor(ctx, directive)
	}
	receipt, err := controller.resetter.ResetComputerStorage(ctx, workloadrunner.ComputerStorageResetRequest{
		Storage: workloadrunner.ComputerStorage{ComputerID: directive.ComputerID, StorageID: directive.StorageID,
			StorageGeneration: directive.OldGeneration, IntentRevision: directive.IntentRevision, DiskBytes: directive.DiskBytes},
		NewGeneration: directive.NewGeneration, NodeID: controller.nodeID, BootSessionID: controller.bootSessionID,
		RootInstanceID: directive.RootInstanceID, JobID: directive.JobID, PriorJobID: directive.JobID,
		IntentRevision: directive.IntentRevision, CleanupFence: directive.CleanupFence,
	})
	if err != nil {
		return err
	}
	_, err = controller.client.AcknowledgeComputerStorageReset(ctx, directive.ComputerID, l1.ComputerStorageResetAcknowledgementRequest{
		NodeID: controller.nodeID, BootSessionID: controller.bootSessionID, IdempotencyKey: receipt.ReceiptID,
		Receipt: l1.ComputerStorageResetReceipt{Kind: receipt.Kind, ReceiptID: receipt.ReceiptID,
			ComputerID: receipt.ComputerID, StorageID: receipt.StorageID, OldGeneration: receipt.OldGeneration,
			NewGeneration: receipt.NewGeneration, NodeID: receipt.NodeID, RootInstanceID: receipt.RootInstanceID, JobID: receipt.JobID,
			IntentRevision: receipt.IntentRevision, CleanupFence: receipt.CleanupFence,
			HelperGeneration: receipt.HelperGeneration},
	})
	return err
}

func (controller *storageResetController) retirePredecessor(ctx context.Context, directive l1.ComputerStorageResetDirective) error {
	if controller.finalizeVolumes == nil || controller.attestRuntimeRemoval == nil {
		return refuseStorageOperation("removal_mechanics_missing", errors.New("Computer Storage retirement requires shared OCI deletion and attestation"))
	}
	generation := uint64(directive.IntentRevision)
	storage := &workloadrunner.ComputerStorage{ComputerID: directive.ComputerID, StorageID: directive.StorageID,
		StorageGeneration: directive.OldGeneration, IntentRevision: directive.IntentRevision, DiskBytes: directive.DiskBytes}
	if err := controller.finalizeVolumes(ctx, workloadrunner.ManagedVolumeFinalizationRequest{
		Volumes: []workloadrunner.ManagedVolume{{Kind: workloadrunner.ManagedVolumeComputerDisk, ComputerStorage: storage}},
		Removal: &workloadrunner.ManagedVolumeRemovalAuthority{NodeID: controller.nodeID,
			BootSessionID: controller.bootSessionID, JobID: directive.JobID, PriorJobID: directive.JobID,
			RemovalGeneration: generation, CleanupFence: directive.CleanupFence},
	}); err != nil {
		if acknowledgement, quarantined := storageCleanupQuarantineAcknowledgement(err, directive.RootInstanceID, l1.ComputerStorageCleanupReset); quarantined {
			_, acknowledgementErr := controller.client.AcknowledgeComputerStorageRetirement(ctx, directive.ComputerID, acknowledgement)
			return errors.Join(err, acknowledgementErr)
		}
		return fmt.Errorf("delete reset predecessor through shared removal machinery: %w", err)
	}
	manifest := workloadrunner.RuntimeResourceManifest{
		Version: 1, RuntimeKind: contract.JobKindOCI, NodeID: controller.nodeID,
		BootSessionID: controller.bootSessionID, JobID: directive.JobID,
		AttemptID:    contract.ComputerStorageResetRetirementAttemptID(directive.IntentRevision),
		FencingToken: directive.CleanupFence, WorkloadClass: contract.JobClassService,
		RemovalGeneration: fmt.Sprint(generation), ComputerStorage: storage, StorageOnly: true,
	}
	attestation, err := controller.attestRuntimeRemoval(ctx, workloadrunner.RuntimeRemovalProofRequest{
		JobID: directive.JobID, RemovalGeneration: generation,
		Attempts: []workloadrunner.RuntimeResourceManifest{manifest},
	})
	if err != nil {
		return fmt.Errorf("attest reset predecessor through shared removal machinery: %w", err)
	}
	if err := validateRuntimeRemovalAttestation(runtimeRemovalManifest{Version: 1, JobID: directive.JobID,
		RemovalGeneration: generation, Attempts: []workloadrunner.RuntimeResourceManifest{manifest}}, attestation); err != nil {
		return refuseStorageOperation("attestation_mismatch", err)
	}
	_, err = controller.client.AcknowledgeComputerStorageRetirement(ctx, directive.ComputerID, l1.RemovalAcknowledgementRequest{
		NodeID: controller.nodeID, BootSessionID: controller.bootSessionID,
		RemovalGeneration: generation, CleanupFence: directive.CleanupFence,
		RootInstanceID: directive.RootInstanceID,
		IdempotencyKey: removalAcknowledgementKey(localRemoval{jobID: directive.JobID, generation: generation,
			cleanupFence: directive.CleanupFence, rootInstanceID: directive.RootInstanceID}, controller.bootSessionID),
	})
	return err
}

func (controller *storageResetController) enqueue(ctx context.Context, directive l1.ComputerStorageResetDirective, failures chan<- destinationError) {
	if controller == nil {
		return
	}
	key, reserved := controller.reserve(directive)
	if !reserved {
		return
	}
	controller.wg.Add(1)
	go func() {
		defer controller.wg.Done()
		defer controller.release(key)
		if err := controller.run(ctx, directive); err != nil && ctx.Err() == nil {
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
