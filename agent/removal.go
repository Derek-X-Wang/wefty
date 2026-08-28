package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
)

var errServiceRemovalRequested = errors.New("service removal requested")

// removalController executes the node-scoped removal directive. Filesystem
// deletion remains entirely inside managedResourceManager; this type owns only
// the ordering between durable local intent, process reaping, spool cleanup,
// the guardrail call, and L1 attestation.
type removalController struct {
	client                *Client
	outbox                *evidenceOutbox
	managed               managedResourceManager
	session               *agentSession
	nodeID                string
	bootSessionID         string
	logf                  func(string, ...any)
	beginRemoval          func(context.Context, localRemoval) error
	loadRuntimeRemoval    func(context.Context, string) (runtimeRemovalRecord, bool, error)
	listRuntimeRemovals   func(context.Context) ([]runtimeRemovalRecord, error)
	recordRuntimeQuiesced func(context.Context, localRemoval, workloadrunner.ReapReceipt) error
	recordRuntimeAttested func(context.Context, localRemoval, workloadrunner.RuntimeRemovalAttestation) error
	persistRuntimeRemoval func(context.Context, localRemoval, []workloadrunner.RuntimeResourceManifest) error
	loadRemovalIntent     func(context.Context, string) (localRemoval, bool, error)
	reapService           func(context.Context, string) (workloadrunner.ReapReceipt, error)
	clearReap             func(string)
	purgeJob              func(context.Context, string) error
	removeResource        func(context.Context, localRemoval) error
	releaseImagePin       func(context.Context, string) error
	finalizeVolumes       func(context.Context, workloadrunner.ManagedVolumeFinalizationRequest) error
	reconstructRuntime    func(context.Context, workloadrunner.RuntimeRemovalProofRequest) ([]workloadrunner.RuntimeResourceManifest, error)
	deleteRuntimeData     func(context.Context, workloadrunner.RuntimeRemovalProofRequest) error
	attestRuntimeRemoval  func(context.Context, workloadrunner.RuntimeRemovalProofRequest) (workloadrunner.RuntimeRemovalAttestation, error)
	ackRemoval            func(context.Context, localRemoval) error
	finishRemoval         func(context.Context, localRemoval) error

	mu       sync.Mutex
	inflight map[string]struct{}
	wg       sync.WaitGroup
}

func newRemovalController(
	client *Client,
	outbox *evidenceOutbox,
	managed managedResourceManager,
	session *agentSession,
	nodeID, bootSessionID string,
	logf func(string, ...any),
) *removalController {
	controller := &removalController{
		client: client, outbox: outbox, managed: managed, session: session,
		nodeID: nodeID, bootSessionID: bootSessionID, logf: logf,
		inflight: make(map[string]struct{}),
	}
	if outbox != nil {
		controller.beginRemoval = outbox.beginRemoval
		controller.loadRuntimeRemoval = outbox.runtimeRemoval
		controller.listRuntimeRemovals = outbox.pendingRuntimeRemovals
		controller.recordRuntimeQuiesced = outbox.recordRuntimeQuiesced
		controller.recordRuntimeAttested = outbox.recordRuntimeAttested
		controller.persistRuntimeRemoval = outbox.storeReconstructedRuntimeRemoval
		controller.loadRemovalIntent = outbox.removalIntent
		controller.purgeJob = outbox.purgeJob
		controller.finishRemoval = outbox.completeRemoval
	}
	if session != nil {
		controller.reapService = session.reapServiceForRemoval
		controller.clearReap = session.clearRuntimeReap
	}
	if managed != nil {
		controller.removeResource = managed.remove
	}
	controller.ackRemoval = controller.acknowledge
	return controller
}

func (controller *removalController) enqueue(
	ctx context.Context,
	directive l1.RemovalDirective,
	failures chan<- destinationError,
) {
	if controller == nil || controller.managed == nil || controller.outbox == nil {
		return
	}
	key := removalKey(directive.JobID, directive.RemovalGeneration)
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
				case failures <- destinationError{destination: classification.destination, err: fmt.Errorf("agent: remove service %q: %w", directive.JobID, err)}:
				default:
				}
				return
			}
			controller.log("agent: remove service %q: %v", directive.JobID, err)
		}
	}()
}

func (controller *removalController) process(ctx context.Context, directive l1.RemovalDirective) error {
	if directive.BoundNodeID != controller.nodeID {
		return fmt.Errorf("removal directive belongs to node %q, not %q", directive.BoundNodeID, controller.nodeID)
	}
	removal := localRemoval{
		jobID: directive.JobID, kind: directive.Kind, generation: directive.RemovalGeneration,
		rootInstanceID: directive.RootInstanceID, cleanupFence: directive.CleanupFence,
	}
	if removal.kind != contract.JobKindProcess && removal.kind != contract.JobKindOCI {
		return fmt.Errorf("removal directive for service %q has invalid workload kind %q", directive.JobID, removal.kind)
	}
	if directive.ComputerStorage != nil && removal.kind != contract.JobKindOCI {
		return fmt.Errorf("agent: Computer removal for service %q requires OCI workload kind", directive.JobID)
	}
	// This FULL-synchronous SQLite write must precede any signal sent to the
	// guardian. A crash after it leaves an unambiguous local removing record.
	if err := controller.beginRemoval(ctx, removal); err != nil {
		return err
	}
	if controller.loadRuntimeRemoval != nil {
		runtimeRemoval, found, err := controller.loadRuntimeRemoval(ctx, removal.jobID)
		if err != nil {
			return err
		}
		if !found && removal.kind == contract.JobKindOCI {
			runtimeRemoval, err = controller.reconstructAndPersistRuntimeRemoval(ctx, removal, computerStorageFromClaim(directive.ComputerStorage))
			if err != nil {
				return err
			}
			found = true
		}
		if found {
			return controller.continueRuntimeRemoval(ctx, removal, &runtimeRemoval, directive.ComputerStorage)
		}
	}
	if removal.kind == contract.JobKindOCI {
		return fmt.Errorf("agent: legacy OCI removal %q has no persisted helper-owned inventory", removal.jobID)
	}
	receipt, err := controller.reapService(ctx, directive.JobID)
	if err != nil {
		return err
	}
	if !receipt.RuntimeQuiesced || receipt.Evidence == "" {
		return fmt.Errorf("service %q removal has no positive runtime reap receipt", directive.JobID)
	}
	// Services created before runtime manifests were introduced retain the
	// legacy boolean as their crash-resume compatibility marker.
	removal.processTreeReaped = true
	return controller.completeLocalRemoval(ctx, removal, nil, computerStorageFromClaim(directive.ComputerStorage))
}

func (controller *removalController) completeLocalRemoval(ctx context.Context, removal localRemoval, runtimeRemoval *runtimeRemovalRecord, computerStorage *workloadrunner.ComputerStorage) error {
	if err := controller.purgeJob(ctx, removal.jobID); err != nil {
		return err
	}
	if err := controller.removeResource(ctx, removal); err != nil {
		return fmt.Errorf("delete managed service resource: %w", err)
	}
	needsRuntimeProof := removal.kind == contract.JobKindOCI && (runtimeRemoval == nil || runtimeRemoval.phase != runtimeRemovalComplete)
	if computerStorage != nil && needsRuntimeProof {
		if controller.finalizeVolumes == nil {
			return errors.New("Computer removal requires OCI disk finalization")
		}
		if err := controller.finalizeVolumes(ctx, workloadrunner.ManagedVolumeFinalizationRequest{
			Volumes: []workloadrunner.ManagedVolume{{Kind: workloadrunner.ManagedVolumeComputerDisk, ComputerStorage: computerStorage}},
			Removal: &workloadrunner.ManagedVolumeRemovalAuthority{NodeID: controller.nodeID, BootSessionID: controller.bootSessionID, JobID: removal.jobID, RemovalGeneration: removal.generation, CleanupFence: removal.cleanupFence},
		}); err != nil {
			return fmt.Errorf("delete Computer disk resource: %w", err)
		}
	}
	if removal.kind == contract.JobKindOCI {
		if needsRuntimeProof {
			if controller.deleteRuntimeData == nil || controller.attestRuntimeRemoval == nil {
				return errors.New("OCI service removal proof runtime is unavailable")
			}
			attempts := []workloadrunner.RuntimeResourceManifest(nil)
			if runtimeRemoval != nil {
				attempts = runtimeRemoval.manifest.Attempts
			}
			proofRequest := workloadrunner.RuntimeRemovalProofRequest{
				NodeID: controller.nodeID, BootSessionID: controller.bootSessionID, JobID: removal.jobID,
				RemovalGeneration: removal.generation, CleanupFence: removal.cleanupFence,
				ComputerStorage: computerStorage, Attempts: attempts,
			}
			if err := controller.deleteRuntimeData(ctx, proofRequest); err != nil {
				return fmt.Errorf("delete OCI service data: %w", err)
			}
			attestation, err := controller.attestRuntimeRemoval(ctx, proofRequest)
			if err != nil {
				return fmt.Errorf("attest deleted OCI service resources: %w", err)
			}
			manifest := runtimeRemovalManifest{Version: 1, JobID: removal.jobID, RemovalGeneration: removal.generation, Attempts: attestation.Attempts}
			if runtimeRemoval != nil {
				manifest = runtimeRemoval.manifest
			}
			if err := validateRuntimeRemovalAttestation(manifest, attestation); err != nil {
				return err
			}
			if runtimeRemoval != nil {
				if err := controller.recordRuntimeAttested(ctx, removal, attestation); err != nil {
					return err
				}
				runtimeRemoval.phase = runtimeRemovalComplete
				runtimeRemoval.attestation = attestation
			}
		}
	}
	if controller.releaseImagePin != nil {
		if err := controller.releaseImagePin(ctx, removal.jobID); err != nil {
			return fmt.Errorf("release service binding image pin: %w", err)
		}
	}
	if err := controller.ackRemoval(ctx, removal); err != nil {
		return err
	}
	if err := controller.finishRemoval(ctx, removal); err != nil {
		return err
	}
	if controller.clearReap != nil {
		controller.clearReap(removal.jobID)
	}
	return nil
}

func (controller *removalController) reconstructAndPersistRuntimeRemoval(ctx context.Context, removal localRemoval, computerStorage *workloadrunner.ComputerStorage) (runtimeRemovalRecord, error) {
	if controller.reconstructRuntime == nil || controller.persistRuntimeRemoval == nil || controller.loadRuntimeRemoval == nil {
		return runtimeRemovalRecord{}, errors.New("agent: legacy OCI removal inventory reconstruction is unavailable")
	}
	request := workloadrunner.RuntimeRemovalProofRequest{
		NodeID: controller.nodeID, BootSessionID: controller.bootSessionID, JobID: removal.jobID,
		RemovalGeneration: removal.generation, CleanupFence: removal.cleanupFence, ComputerStorage: computerStorage,
	}
	attempts, err := controller.reconstructRuntime(ctx, request)
	if err != nil {
		return runtimeRemovalRecord{}, fmt.Errorf("agent: legacy OCI removal %q remains pending because helper inventory reconstruction failed: %w", removal.jobID, err)
	}
	if err := controller.persistRuntimeRemoval(ctx, removal, attempts); err != nil {
		return runtimeRemovalRecord{}, err
	}
	record, found, err := controller.loadRuntimeRemoval(ctx, removal.jobID)
	if err != nil {
		return runtimeRemovalRecord{}, err
	}
	if !found {
		return runtimeRemovalRecord{}, errors.New("agent: reconstructed OCI removal manifest disappeared after persistence")
	}
	return record, nil
}

func (controller *removalController) continueRuntimeRemoval(ctx context.Context, removal localRemoval, runtimeRemoval *runtimeRemovalRecord, claim *l1.ComputerStorageClaim) error {
	computerStorage, err := removalComputerStorage(runtimeRemoval.manifest, claim)
	if err != nil {
		return err
	}
	if computerStorage != nil && removal.kind != contract.JobKindOCI {
		return errors.New("agent: frozen Computer removal inventory requires OCI workload kind")
	}
	switch runtimeRemoval.phase {
	case runtimeRemovalComplete:
		// The post-delete receipt is already durable; continue to pin release
		// and L1 acknowledgement without repeating helper mutation or proof.
	case runtimeRemovalQuarantined:
		// Runtime quiescence is durable; local and helper deletion may resume.
	case runtimeRemovalPrepared:
		receipt, err := controller.reapService(ctx, removal.jobID)
		if err != nil {
			return err
		}
		if !receipt.RuntimeQuiesced || receipt.Evidence == "" {
			return fmt.Errorf("service %q removal has no positive runtime reap receipt", removal.jobID)
		}
		if err := controller.recordRuntimeQuiesced(ctx, removal, receipt); err != nil {
			return err
		}
		runtimeRemoval.receipt = receipt
		runtimeRemoval.phase = runtimeRemovalQuarantined
	default:
		return fmt.Errorf("service %q runtime removal has invalid phase %q", removal.jobID, runtimeRemoval.phase)
	}
	removal.processTreeReaped = true
	return controller.completeLocalRemoval(ctx, removal, runtimeRemoval, computerStorage)
}

// prepareAuthorityLoss closes the renewal-vs-heartbeat race for a running
// service. If L1 fenced the attempt because removal was requested, the
// renewal path fetches and persists that standing directive before it tells
// the lifecycle to signal the guardian. Other authority losses have no
// removal directive and retain their immediate reap behavior.
func (controller *removalController) prepareAuthorityLoss(ctx context.Context, jobID string) error {
	if controller == nil || controller.outbox == nil {
		return nil
	}
	response, err := controller.session.heartbeat(ctx)
	if err != nil {
		return err
	}
	for _, directive := range response.RemovalDirectives {
		if directive.JobID != jobID {
			continue
		}
		if directive.BoundNodeID != controller.nodeID {
			return fmt.Errorf("removal directive belongs to node %q, not %q", directive.BoundNodeID, controller.nodeID)
		}
		return controller.outbox.beginRemoval(ctx, localRemoval{
			jobID: directive.JobID, kind: directive.Kind, generation: directive.RemovalGeneration,
			rootInstanceID: directive.RootInstanceID, cleanupFence: directive.CleanupFence,
		})
	}
	return nil
}

func (controller *removalController) resume(ctx context.Context) error {
	if controller == nil {
		return nil
	}
	if controller.listRuntimeRemovals != nil {
		removals, err := controller.listRuntimeRemovals(ctx)
		if err != nil {
			return fmt.Errorf("resume runtime service removals: %w", err)
		}
		for _, record := range removals {
			if err := controller.continueRuntimeRemoval(ctx, record.removal, &record, nil); err != nil {
				return err
			}
		}
	}
	if controller.managed == nil {
		return nil
	}
	completed, err := controller.managed.resumeRemovals(ctx)
	if err != nil {
		return fmt.Errorf("resume managed service removals: %w", err)
	}
	for _, removal := range completed {
		if controller.loadRemovalIntent == nil {
			return errors.New("resume managed service removal requires durable runtime-kind intent")
		}
		intent, found, err := controller.loadRemovalIntent(ctx, removal.jobID)
		if err != nil {
			return err
		}
		if !found {
			// managedroot tombstones are permanent. Once the matching durable
			// intent has been released, this historical completion is done.
			continue
		}
		intent.processTreeReaped = removal.processTreeReaped
		removal = intent
		if removal.kind == contract.JobKindOCI {
			record, found, err := controller.loadRuntimeRemoval(ctx, removal.jobID)
			if err != nil {
				return err
			}
			if !found {
				record, err = controller.reconstructAndPersistRuntimeRemoval(ctx, removal, nil)
				if err != nil {
					return err
				}
			}
			if err := controller.continueRuntimeRemoval(ctx, removal, &record, nil); err != nil {
				return err
			}
			continue
		}
		if err := controller.purgeJob(ctx, removal.jobID); err != nil {
			return err
		}
		if controller.releaseImagePin != nil {
			if err := controller.releaseImagePin(ctx, removal.jobID); err != nil {
				return fmt.Errorf("release resumed service binding image pin: %w", err)
			}
		}
		if err := controller.ackRemoval(ctx, removal); err != nil {
			return err
		}
		if err := controller.finishRemoval(ctx, removal); err != nil {
			return err
		}
	}
	return nil
}

func removalComputerStorage(manifest runtimeRemovalManifest, claim *l1.ComputerStorageClaim) (*workloadrunner.ComputerStorage, error) {
	var frozen *workloadrunner.ComputerStorage
	sawNonComputer := false
	for _, attempt := range manifest.Attempts {
		if attempt.ComputerStorage == nil {
			if frozen != nil {
				return nil, errors.New("agent: frozen runtime removal manifest mixes Computer and service-data attempts")
			}
			sawNonComputer = true
			continue
		}
		if sawNonComputer {
			return nil, errors.New("agent: frozen runtime removal manifest mixes Computer and service-data attempts")
		}
		if frozen == nil {
			storage := *attempt.ComputerStorage
			frozen = &storage
			continue
		}
		if *frozen != *attempt.ComputerStorage {
			return nil, errors.New("agent: frozen runtime removal manifest has conflicting Computer Storage identities")
		}
	}
	if claim == nil {
		return frozen, nil
	}
	if frozen == nil || frozen.ComputerID != claim.ComputerID || frozen.StorageID != claim.StorageID || frozen.StorageGeneration != claim.StorageGeneration {
		return nil, errors.New("agent: Computer removal directive does not match the frozen runtime manifest")
	}
	return frozen, nil
}

func computerStorageFromClaim(claim *l1.ComputerStorageClaim) *workloadrunner.ComputerStorage {
	if claim == nil {
		return nil
	}
	return &workloadrunner.ComputerStorage{
		ComputerID: claim.ComputerID, StorageID: claim.StorageID, StorageGeneration: claim.StorageGeneration,
		IntentRevision: claim.IntentRevision,
	}
}

func (controller *removalController) acknowledge(ctx context.Context, removal localRemoval) error {
	request := l1.RemovalAcknowledgementRequest{
		NodeID: controller.nodeID, BootSessionID: controller.bootSessionID,
		RemovalGeneration: removal.generation, CleanupFence: removal.cleanupFence,
		RootInstanceID: removal.rootInstanceID,
		IdempotencyKey: removalAcknowledgementKey(removal, controller.bootSessionID),
	}
	if _, err := controller.client.AcknowledgeRemoval(ctx, removal.jobID, request); err != nil {
		return fmt.Errorf("acknowledge completed service removal: %w", err)
	}
	return nil
}

func (controller *removalController) wait() { controller.wg.Wait() }

func (controller *removalController) log(format string, args ...any) {
	if controller.logf != nil {
		controller.logf(format, args...)
	}
}

func removalKey(jobID string, generation uint64) string {
	return fmt.Sprintf("%s:%d", jobID, generation)
}

func removalAcknowledgementKey(removal localRemoval, bootSessionID string) string {
	digest := sha256.Sum256([]byte(fmt.Sprintf(
		"%s\x00%d\x00%s\x00%s\x00%s",
		removal.jobID, removal.generation, removal.cleanupFence, removal.rootInstanceID, bootSessionID,
	)))
	return "removal:" + hex.EncodeToString(digest[:])
}
