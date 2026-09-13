package agent

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
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
	reapService           func(context.Context, string, string, []workloadrunner.RuntimeResourceManifest) (workloadrunner.ReapReceipt, error)
	clearReap             func(string)
	purgeJob              func(context.Context, string) error
	removeResource        func(context.Context, localRemoval) error
	releaseImagePin       func(context.Context, string) error
	finalizeVolumes       func(context.Context, workloadrunner.ManagedVolumeFinalizationRequest) error
	reconstructRuntime    func(context.Context, workloadrunner.RuntimeRemovalProofRequest) ([]workloadrunner.RuntimeResourceManifest, error)
	deleteRuntimeData     func(context.Context, workloadrunner.RuntimeRemovalProofRequest) error
	attestRuntimeRemoval  func(context.Context, workloadrunner.RuntimeRemovalProofRequest) (workloadrunner.RuntimeRemovalAttestation, error)
	ackRemoval            func(context.Context, localRemoval) error
	ackRemovalQuarantine  func(context.Context, localRemoval, error) error
	ackRemovalStall       func(context.Context, localRemoval, runtimeRemovalRecord) error
	recordRemovalFailure  func(context.Context, localRemoval, string, string) error
	recordUntypedFailure  func(context.Context, localRemoval) error
	freezeStall           func(context.Context, localRemoval, []byte, string) ([]byte, string, error)
	removalStartedAt      func(context.Context, string) (time.Time, error)
	recordStallDeclared   func(context.Context, localRemoval) error
	stallBound            time.Duration
	now                   func() time.Time
	finishRemoval         func(context.Context, localRemoval) error
	removeBackupCopies    func(context.Context, []l1.ComputerBackupPruneDirective) error

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
		controller.recordRemovalFailure = outbox.recordRuntimeRemovalFailure
		controller.recordUntypedFailure = outbox.recordRuntimeRemovalUntypedFailure
		controller.freezeStall = outbox.freezeRuntimeRemovalStallDeclaration
		controller.removalStartedAt = outbox.removalStartedAt
		controller.recordStallDeclared = outbox.recordRuntimeRemovalStallDeclared
		controller.now = outbox.clock.Now
	}
	if controller.now == nil {
		controller.now = time.Now
	}
	controller.stallBound = l1.DefaultRemovalStallBound
	if session != nil {
		controller.reapService = session.reapServiceForRemoval
		controller.clearReap = session.clearRuntimeReap
	}
	if managed != nil {
		controller.removeResource = managed.remove
	}
	controller.ackRemoval = controller.acknowledge
	controller.ackRemovalQuarantine = controller.acknowledgeQuarantine
	controller.ackRemovalStall = controller.acknowledgeStall
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
		if err := controller.reconcile(ctx, directive); err != nil && ctx.Err() == nil {
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

// reconcile is the one path a removal directive takes, from the boot sweep and
// from the heartbeat alike. A refused removal used to end at a log line and the
// next heartbeat started the identical attempt again (#450); counting the
// refusal is what lets the same loop notice that it is a loop.
//
// A removal already declared stalled is expected to keep failing -- that is
// what the declaration said -- so its refusal is no longer an error the caller
// must act on. Returning it would fail the boot sweep on every restart, and the
// boot sweep gates image-pin restoration, which is the one thing a stalled
// removal must keep until cleanup finally succeeds.
func (controller *removalController) reconcile(ctx context.Context, directive l1.RemovalDirective) error {
	return controller.process(ctx, directive)
}

// declaredRefusalCode is the exact helper refusal an accepted declaration
// stands for. Only that refusal is expected afterwards; anything else --
// a replaced node session, a transport failure, a local persistence fault --
// is new information the caller must still act on.
func declaredRefusalCode(record runtimeRemovalRecord) string {
	declaration, ok := frozenStallDeclaration(record)
	if !ok {
		return ""
	}
	return declaration.LastRefusalCode
}

func frozenStallDeclaration(record runtimeRemovalRecord) (l1.ServiceRemovalStallEvidence, bool) {
	if len(record.stallDeclaration) == 0 {
		return l1.ServiceRemovalStallEvidence{}, false
	}
	var declaration l1.ServiceRemovalStallEvidence
	if err := json.Unmarshal(record.stallDeclaration, &declaration); err != nil {
		return l1.ServiceRemovalStallEvidence{}, false
	}
	return declaration, true
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
	if directive.ComputerBackupCopies != nil && len(directive.ComputerBackupCopies.Copies) != 0 {
		if controller.removeBackupCopies == nil {
			return errors.New("Computer removal requires Backup copy removal support")
		}
		if err := controller.removeBackupCopies(ctx, directive.ComputerBackupCopies.Copies); err != nil {
			return fmt.Errorf("delete Computer Backup copies: %w", err)
		}
	}
	if controller.loadRuntimeRemoval != nil {
		directiveStorages := computerStoragesFromDirective(directive.ComputerStorage, directive.ComputerStorageGenerations)
		runtimeRemoval, found, err := controller.loadRuntimeRemoval(ctx, removal.jobID)
		if err != nil {
			return err
		}
		if !found && removal.kind == contract.JobKindOCI {
			runtimeRemoval, err = controller.reconstructAndPersistRuntimeRemoval(ctx, removal, directiveStorages)
			if err != nil {
				return err
			}
			found = true
		}
		if found {
			if runtimeRemoval.phase == runtimeRemovalPrepared && runtimeRemoval.stallDeclaredAt == nil &&
				storageOnlyManifestNeedsRefresh(runtimeRemoval.manifest, controller.bootSessionID) {
				runtimeRemoval, err = controller.reconstructAndPersistRuntimeRemoval(ctx, removal, directiveStorages)
				if err != nil {
					return err
				}
			}
			computerStorages, err := removalComputerStorages(runtimeRemoval.manifest, storageGenerationClaims(directive.ComputerStorageGenerations))
			if err != nil {
				return err
			}
			return controller.continueRuntimeRemoval(ctx, removal, &runtimeRemoval, computerStorages)
		}
	}
	if removal.kind == contract.JobKindOCI {
		return fmt.Errorf("agent: legacy OCI removal %q has no persisted helper-owned inventory", removal.jobID)
	}
	receipt, err := controller.reapService(ctx, directive.JobID, directive.Kind, nil)
	if err != nil {
		return err
	}
	if !receipt.RuntimeQuiesced || receipt.Evidence == "" {
		return fmt.Errorf("service %q removal has no positive runtime reap receipt", directive.JobID)
	}
	// Services created before runtime manifests were introduced retain the
	// legacy boolean as their crash-resume compatibility marker.
	removal.processTreeReaped = true
	return controller.completeLocalRemoval(ctx, removal, nil, computerStoragesFromClaims(storageGenerationClaims(directive.ComputerStorageGenerations)))
}

func (controller *removalController) completeLocalRemoval(ctx context.Context, removal localRemoval, runtimeRemoval *runtimeRemovalRecord, computerStorages []*workloadrunner.ComputerStorage) error {
	noRuntime := runtimeRemoval != nil && runtimeRemoval.receipt.Evidence == workloadrunner.ReapEvidenceNoRuntime
	needsRuntimeProof := removal.kind == contract.JobKindOCI && (runtimeRemoval == nil || runtimeRemoval.phase != runtimeRemovalComplete)
	attempts := []workloadrunner.RuntimeResourceManifest(nil)
	if runtimeRemoval != nil {
		attempts = runtimeRemoval.manifest.Attempts
	}
	// Only the Storage-only no-runtime proof may skip deleting the local
	// managed service resource. The adapter returns the same evidence kind
	// when image delivery failed before the helper `Run` RPC was entered, and
	// that attempt still prepared a managed service directory on this node.
	skipManagedResource := noRuntime && storageOnlyNoRuntimeReceipt(runtimeRemoval.receipt, attempts)
	// A no-runtime receipt skips guardian reaping, so it is authoritative
	// only for the exact Storage generations in the frozen helper inventory.
	// Refuse incomplete coverage before any durable cleanup begins. A real
	// runtime reap covers the job-wide guardian while finalizeVolumes
	// independently deletes every L1-claimed generation.
	if needsRuntimeProof && noRuntime && len(computerStorages) != 0 && !computerStorageInventoryComplete(attempts, computerStorages) {
		return errors.New("agent: Computer removal lacks helper inventory for every claimed Storage generation")
	}
	volumes := []workloadrunner.ManagedVolume(nil)
	if needsRuntimeProof {
		volumes = make([]workloadrunner.ManagedVolume, 0, len(computerStorages))
		for _, storage := range computerStorages {
			absent := false
			for _, attempt := range attempts {
				candidate := attempt.ComputerStorage
				if !attempt.StorageAbsent || candidate == nil || candidate.ComputerID != storage.ComputerID ||
					candidate.StorageID != storage.StorageID || candidate.StorageGeneration != storage.StorageGeneration {
					continue
				}
				if validateRuntimeResourceManifest(attempt) != nil || attempt.JobID != removal.jobID || attempt.NodeID != controller.nodeID ||
					attempt.FencingToken != removal.cleanupFence || attempt.RemovalGeneration != fmt.Sprint(removal.generation) {
					return errors.New("Computer removal has invalid frozen Storage absence authority")
				}
				absent = true
			}
			// Any exact frozen absence remains a restrictive precondition even if
			// another runtime manifest names this generation.
			volumes = append(volumes, workloadrunner.ManagedVolume{Kind: workloadrunner.ManagedVolumeComputerDisk, ComputerStorage: storage, StorageAbsent: absent})
		}
	}
	if err := controller.purgeJob(ctx, removal.jobID); err != nil {
		return err
	}
	if !skipManagedResource {
		if err := controller.removeResource(ctx, removal); err != nil {
			return fmt.Errorf("delete managed service resource: %w", err)
		}
	}
	if len(computerStorages) != 0 && needsRuntimeProof {
		if controller.finalizeVolumes == nil {
			return errors.New("Computer removal requires OCI disk finalization")
		}
		if err := controller.finalizeVolumes(ctx, workloadrunner.ManagedVolumeFinalizationRequest{
			Volumes: volumes,
			Removal: &workloadrunner.ManagedVolumeRemovalAuthority{NodeID: controller.nodeID, BootSessionID: controller.bootSessionID, JobID: removal.jobID, PriorJobID: removal.jobID, RemovalGeneration: removal.generation, CleanupFence: removal.cleanupFence},
		}); err != nil {
			var quarantined *workloadrunner.ManagedVolumeCleanupQuarantinedError
			if errors.As(err, &quarantined) && controller.ackRemovalQuarantine != nil {
				if ackErr := controller.ackRemovalQuarantine(ctx, removal, err); ackErr != nil {
					return errors.Join(fmt.Errorf("delete Computer disk resource: %w", err), ackErr)
				}
			}
			return fmt.Errorf("delete Computer disk resource: %w", err)
		}
	}
	if removal.kind == contract.JobKindOCI {
		if needsRuntimeProof {
			if controller.deleteRuntimeData == nil || controller.attestRuntimeRemoval == nil {
				return errors.New("OCI service removal proof runtime is unavailable")
			}
			proofRequest := workloadrunner.RuntimeRemovalProofRequest{
				NodeID: controller.nodeID, BootSessionID: controller.bootSessionID, JobID: removal.jobID,
				RemovalGeneration: removal.generation, CleanupFence: removal.cleanupFence, RootInstanceID: removal.rootInstanceID,
				Attempts: attempts,
			}
			if err := controller.deleteRuntimeData(ctx, proofRequest); err != nil {
				return fmt.Errorf("delete OCI service data: %w", err)
			}
			attestation, err := controller.attestRuntimeRemoval(ctx, proofRequest)
			if err != nil {
				return fmt.Errorf("attest deleted OCI service resources: %w", err)
			}
			manifest := runtimeRemovalManifest{Version: 1, JobID: removal.jobID, RemovalGeneration: removal.generation, Attempts: attempts}
			if runtimeRemoval != nil {
				manifest = runtimeRemoval.manifest
				manifest.Attempts = attempts
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

func (controller *removalController) reconstructAndPersistRuntimeRemoval(ctx context.Context, removal localRemoval, computerStorages []*workloadrunner.ComputerStorage) (runtimeRemovalRecord, error) {
	if controller.reconstructRuntime == nil || controller.persistRuntimeRemoval == nil || controller.loadRuntimeRemoval == nil {
		return runtimeRemovalRecord{}, errors.New("agent: legacy OCI removal inventory reconstruction is unavailable")
	}
	var attempts []workloadrunner.RuntimeResourceManifest
	requests := []*workloadrunner.ComputerStorage{nil}
	if len(computerStorages) != 0 {
		// Inventory runtime authorities once, then ask per generation only for
		// prepared/never-attached or already-deleted Storage evidence.
		requests = append(requests, computerStorages...)
	}
	seenAttempts := make(map[string]workloadrunner.RuntimeResourceManifest)
	jobScopedRuntimeFound := false
	missingStorageEvidence := false
	for index, computerStorage := range requests {
		request := workloadrunner.RuntimeRemovalProofRequest{
			NodeID: controller.nodeID, BootSessionID: controller.bootSessionID, JobID: removal.jobID,
			RemovalGeneration: removal.generation, CleanupFence: removal.cleanupFence, RootInstanceID: removal.rootInstanceID,
			ComputerStorage: computerStorage,
		}
		reconstructed, err := controller.reconstructRuntime(ctx, request)
		if err != nil {
			return runtimeRemovalRecord{}, fmt.Errorf("agent: legacy OCI removal %q remains pending because helper inventory reconstruction failed: %w", removal.jobID, err)
		}
		if index == 0 {
			jobScopedRuntimeFound = len(reconstructed) != 0
		} else if len(reconstructed) == 0 {
			missingStorageEvidence = true
		}
		for _, attempt := range reconstructed {
			if prior, exists := seenAttempts[attempt.AttemptID]; exists {
				if !reflect.DeepEqual(prior, attempt) {
					return runtimeRemovalRecord{}, fmt.Errorf("agent: legacy OCI removal %q returned conflicting duplicate attempt %q", removal.jobID, attempt.AttemptID)
				}
				continue
			}
			seenAttempts[attempt.AttemptID] = attempt
			attempts = append(attempts, attempt)
		}
	}
	if !jobScopedRuntimeFound && missingStorageEvidence {
		return runtimeRemovalRecord{}, fmt.Errorf("agent: legacy OCI removal %q found neither runtime authority nor Storage evidence", removal.jobID)
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

func (controller *removalController) continueRuntimeRemoval(ctx context.Context, removal localRemoval, runtimeRemoval *runtimeRemovalRecord, computerStorages []*workloadrunner.ComputerStorage) error {
	if len(runtimeRemoval.manifest.Attempts) != 0 && removal.kind != contract.JobKindOCI {
		return errors.New("agent: frozen runtime removal manifest requires OCI workload kind")
	}
	if len(computerStorages) != 0 && removal.kind != contract.JobKindOCI {
		return errors.New("agent: frozen Computer removal inventory requires OCI workload kind")
	}
	if controller.declaredRemovalRetryDeferred(*runtimeRemoval) {
		return nil
	}
	err := controller.continueRuntimeRemovalAttempt(ctx, removal, runtimeRemoval, computerStorages)
	if err == nil {
		if runtimeRemoval.stallDeclaredAt != nil {
			controller.log("agent: service %q cleanup succeeded after its declared stall", removal.jobID)
		}
		return nil
	}
	if ctx.Err() != nil {
		return err
	}
	suppressed, acknowledgementErr := controller.noteRemovalFailure(ctx, l1.RemovalDirective{
		JobID: removal.jobID, BoundNodeID: controller.nodeID, Kind: removal.kind,
		RemovalGeneration: removal.generation, CleanupFence: removal.cleanupFence,
		RootInstanceID: removal.rootInstanceID,
	}, err)
	if acknowledgementErr != nil {
		return acknowledgementErr
	}
	if suppressed {
		return nil
	}
	return err
}

func (controller *removalController) continueRuntimeRemovalAttempt(ctx context.Context, removal localRemoval, runtimeRemoval *runtimeRemovalRecord, computerStorages []*workloadrunner.ComputerStorage) error {
	switch runtimeRemoval.phase {
	case runtimeRemovalComplete:
		// The post-delete receipt is already durable; continue to pin release
		// and L1 acknowledgement without repeating helper mutation or proof.
	case runtimeRemovalQuarantined:
		// Runtime quiescence is durable; local and helper deletion may resume.
	case runtimeRemovalPrepared:
		receipt, err := controller.reapService(ctx, removal.jobID, removal.kind, runtimeRemoval.manifest.Attempts)
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
	return controller.completeLocalRemoval(ctx, removal, runtimeRemoval, computerStorages)
}

const (
	declaredRemovalRetryBase = 15 * time.Second
	declaredRemovalRetryMax  = 3 * time.Minute
)

// declaredRemovalRetryDeferred is the durable retry gate shared by directive
// processing and both boot-resume paths. The last attempted time and refusal
// retry count live in the spool, so a restart or refusal-code change cannot
// reset the cadence.
func (controller *removalController) declaredRemovalRetryDeferred(record runtimeRemovalRecord) bool {
	if record.phase == runtimeRemovalComplete || record.stallDeclaredAt == nil || record.lastAttemptedAt == nil {
		return false
	}
	delay := declaredRemovalRetryBase
	for retry := 0; retry < record.stallRetryAttempts && delay < declaredRemovalRetryMax; retry++ {
		delay *= 2
		if delay > declaredRemovalRetryMax {
			delay = declaredRemovalRetryMax
		}
	}
	now := controller.now
	if now == nil {
		now = time.Now
	}
	return now().UTC().Before(record.lastAttemptedAt.Add(delay))
}

func storageOnlyManifestNeedsRefresh(manifest runtimeRemovalManifest, bootSessionID string) bool {
	if len(manifest.Attempts) == 0 {
		return false
	}
	for _, attempt := range manifest.Attempts {
		if !attempt.StorageOnly {
			return false
		}
		if attempt.BootSessionID != bootSessionID {
			return true
		}
	}
	return false
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
			if record.invalidReason != "" {
				// The listing carries rows this agent cannot validate so the
				// operator can read them. Resume acts on none of them.
				controller.log("agent: runtime removal %q is not resumable: %s", record.removal.jobID, record.invalidReason)
				continue
			}
			computerStorages, err := removalComputerStorages(record.manifest, nil)
			if err != nil {
				return err
			}
			if len(computerStorages) != 0 && record.phase != runtimeRemovalComplete {
				// Computer removal needs the authoritative generation inventory
				// carried by the standing L1 directive. Heartbeat processing will
				// resume it without guessing from historical attempts.
				continue
			}
			if err := controller.continueRuntimeRemoval(ctx, record.removal, &record, computerStorages); err != nil {
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
			computerStorages, err := removalComputerStorages(record.manifest, nil)
			if err != nil {
				return err
			}
			if len(computerStorages) != 0 && record.phase != runtimeRemovalComplete {
				// The standing L1 directive carries every reserved generation.
				// Do not guess a destructive subset during local-only resumption.
				continue
			}
			if err := controller.continueRuntimeRemoval(ctx, removal, &record, computerStorages); err != nil {
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

func removalComputerStorages(manifest runtimeRemovalManifest, claims []l1.ComputerStorageGenerationClaim) ([]*workloadrunner.ComputerStorage, error) {
	byIdentity := make(map[string]*workloadrunner.ComputerStorage)
	sawNonComputer := false
	for _, attempt := range manifest.Attempts {
		if attempt.ComputerStorage == nil {
			if len(byIdentity) != 0 {
				return nil, errors.New("agent: frozen runtime removal manifest mixes Computer and service-data attempts")
			}
			sawNonComputer = true
			continue
		}
		if sawNonComputer {
			return nil, errors.New("agent: frozen runtime removal manifest mixes Computer and service-data attempts")
		}
		key := fmt.Sprintf("%s\x00%s\x00%d", attempt.ComputerStorage.ComputerID,
			attempt.ComputerStorage.StorageID, attempt.ComputerStorage.StorageGeneration)
		if existing := byIdentity[key]; existing != nil {
			if existing.DiskBytes != 0 && attempt.ComputerStorage.DiskBytes != 0 && existing.DiskBytes != attempt.ComputerStorage.DiskBytes {
				return nil, errors.New("agent: frozen runtime removal manifest has conflicting Computer Storage allocation truth")
			}
			continue
		}
		storage := *attempt.ComputerStorage
		byIdentity[key] = &storage
	}
	for _, claim := range claims {
		key := fmt.Sprintf("%s\x00%s\x00%d", claim.ComputerID, claim.StorageID, claim.StorageGeneration)
		if existing := byIdentity[key]; existing != nil {
			if existing.DiskBytes != 0 && claim.DiskBytes != 0 && existing.DiskBytes != claim.DiskBytes {
				return nil, errors.New("agent: Computer removal directive conflicts with frozen Storage allocation truth")
			}
			continue
		}
		byIdentity[key] = &workloadrunner.ComputerStorage{ComputerID: claim.ComputerID, StorageID: claim.StorageID,
			StorageGeneration: claim.StorageGeneration, DiskBytes: claim.DiskBytes}
	}
	storages := make([]*workloadrunner.ComputerStorage, 0, len(byIdentity))
	for _, storage := range byIdentity {
		storages = append(storages, storage)
	}
	slices.SortFunc(storages, func(left, right *workloadrunner.ComputerStorage) int {
		return cmp.Compare(left.StorageGeneration, right.StorageGeneration)
	})
	return storages, nil
}

func computerStoragesFromClaims(claims []l1.ComputerStorageGenerationClaim) []*workloadrunner.ComputerStorage {
	storages := make([]*workloadrunner.ComputerStorage, 0, len(claims))
	for _, claim := range claims {
		storages = append(storages, &workloadrunner.ComputerStorage{ComputerID: claim.ComputerID,
			StorageID: claim.StorageID, StorageGeneration: claim.StorageGeneration, DiskBytes: claim.DiskBytes})
	}
	return storages
}

func computerStorageFromClaim(claim *l1.ComputerStorageClaim) *workloadrunner.ComputerStorage {
	if claim == nil {
		return nil
	}
	return &workloadrunner.ComputerStorage{ComputerID: claim.ComputerID, StorageID: claim.StorageID,
		StorageGeneration: claim.StorageGeneration}
}

func computerStoragesFromDirective(current *l1.ComputerStorageClaim, claims *l1.ComputerStorageGenerationClaims) []*workloadrunner.ComputerStorage {
	result := computerStoragesFromClaims(storageGenerationClaims(claims))
	if current != nil {
		candidate := computerStorageFromClaim(current)
		found := false
		for _, storage := range result {
			if storage.ComputerID == candidate.ComputerID && storage.StorageID == candidate.StorageID && storage.StorageGeneration == candidate.StorageGeneration {
				found = true
				break
			}
		}
		if !found {
			result = append(result, candidate)
		}
	}
	slices.SortFunc(result, func(left, right *workloadrunner.ComputerStorage) int {
		return cmp.Compare(left.StorageGeneration, right.StorageGeneration)
	})
	return result
}

func computerStorageInventoryComplete(attempts []workloadrunner.RuntimeResourceManifest, storages []*workloadrunner.ComputerStorage) bool {
	inventoried := make(map[string]struct{}, len(attempts))
	for _, attempt := range attempts {
		if attempt.ComputerStorage != nil {
			inventoried[fmt.Sprintf("%s\x00%s\x00%d", attempt.ComputerStorage.ComputerID, attempt.ComputerStorage.StorageID,
				attempt.ComputerStorage.StorageGeneration)] = struct{}{}
		}
	}
	for _, storage := range storages {
		if _, ok := inventoried[fmt.Sprintf("%s\x00%s\x00%d", storage.ComputerID, storage.StorageID, storage.StorageGeneration)]; !ok {
			return false
		}
	}
	return true
}

func storageGenerationClaims(claims *l1.ComputerStorageGenerationClaims) []l1.ComputerStorageGenerationClaim {
	if claims == nil {
		return nil
	}
	return claims.Generations
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

// noteRemovalFailure records one refused cleanup attempt and, once the record
// shows a removal that has been retrying past the bound against the same
// refusal, declares to L1 that it cannot complete. The directive is not
// cancelled and the agent keeps trying; what the declaration buys is the
// service slot, which otherwise stays pinned for as long as the refusal lasts.
//
// It reports whether this removal is already declared stalled, so the caller
// can stop treating an expected refusal as a boot failure.
func (controller *removalController) noteRemovalFailure(ctx context.Context, directive l1.RemovalDirective, cause error) (bool, error) {
	if controller.recordRemovalFailure == nil || controller.loadRuntimeRemoval == nil {
		return false, nil
	}
	removal := localRemoval{
		jobID: directive.JobID, kind: directive.Kind, generation: directive.RemovalGeneration,
		rootInstanceID: directive.RootInstanceID, cleanupFence: directive.CleanupFence,
	}
	refusalCode, refusalDetail := removalRefusalCode(cause)
	if refusalCode == "" {
		// An untyped failure is not evidence that retrying cannot help, and a
		// permanent unverified outcome is too expensive to spend on one. It
		// still ends the streak: the bound asks for three consecutive attempts
		// that produced the same typed refusal, and this attempt did not.
		if controller.recordUntypedFailure != nil {
			if err := controller.recordUntypedFailure(ctx, removal); err != nil {
				controller.log("agent: reset removal refusal streak for service %q: %v", directive.JobID, err)
			}
		}
		// An untyped failure is never the refusal a declaration stood for, so
		// it stays the caller's problem however long ago the stall was
		// declared.
		return false, nil
	}
	before, beforeFound, beforeErr := controller.loadRuntimeRemoval(ctx, removal.jobID)
	if beforeErr != nil {
		return false, nil
	}
	if err := controller.recordRemovalFailure(ctx, removal, refusalCode, refusalDetail); err != nil {
		controller.log("agent: record removal failure for service %q: %v", directive.JobID, err)
		return false, nil
	}
	record, found, err := controller.loadRuntimeRemoval(ctx, removal.jobID)
	if err != nil || !found || record.invalidReason != "" {
		return false, nil
	}
	if record.stallDeclaredAt != nil {
		if beforeFound && before.stallDeclaredAt != nil && before.lastRefusalCode != refusalCode {
			controller.log("agent: service %q cleanup remains refused after its declared stall: %s", directive.JobID, refusalCode)
		}
		return refusalCode == declaredRefusalCode(record), nil
	}
	// A frozen declaration means a previous send may already have been
	// committed by L1 and only its response was lost, so the retry replays it
	// rather than weighing the bound again.
	if len(record.stallDeclaration) == 0 && !controller.removalIsStalled(ctx, record) {
		return false, nil
	}
	if controller.ackRemovalStall == nil {
		return false, nil
	}
	if err := controller.ackRemovalStall(ctx, removal, record); err != nil {
		return false, err
	}
	if controller.recordStallDeclared != nil {
		if err := controller.recordStallDeclared(ctx, removal); err != nil {
			return false, err
		}
	}
	accepted := record
	if latest, latestFound, latestErr := controller.loadRuntimeRemoval(ctx, removal.jobID); latestErr == nil && latestFound {
		accepted = latest
	}
	declaration, frozen := frozenStallDeclaration(accepted)
	if !frozen {
		declaration.Attempts = record.failedAttempts
		declaration.LastRefusalCode = record.lastRefusalCode
	}
	controller.log("agent: service %q removal declared stalled after %d consecutive %q refusals; Slot released without cleanup proof",
		directive.JobID, declaration.Attempts, declaration.LastRefusalCode)
	return refusalCode == declaration.LastRefusalCode, nil
}

// removalIsStalled is the agent half of the two-sided bound. L1 can see how
// long a directive has stood; only the node knows whether anything was actually
// retried during that time, so a node offline for the whole window declares
// nothing. The elapsed time is measured from the immutable moment this node
// accepted the directive, never from the frozen manifest's prepared time: a
// Storage-only inventory is reconstructed on every boot, so measuring against
// that would let repeated restarts postpone the declaration forever.
func (controller *removalController) removalIsStalled(ctx context.Context, record runtimeRemovalRecord) bool {
	if record.lastAttemptedAt == nil || record.completedAt != nil {
		return false
	}
	if record.failedAttempts < l1.MinimumServiceRemovalStallAttempts || strings.TrimSpace(record.lastRefusalCode) == "" {
		return false
	}
	startedAt := record.preparedAt
	if controller.removalStartedAt != nil {
		durable, err := controller.removalStartedAt(ctx, record.removal.jobID)
		if err != nil {
			controller.log("agent: read removal start for service %q: %v", record.removal.jobID, err)
			return false
		}
		startedAt = durable
	}
	if startedAt.IsZero() {
		return false
	}
	now := controller.now
	if now == nil {
		now = time.Now
	}
	return now().UTC().Sub(startedAt) >= controller.stallBound
}

// removalRefusalCode reduces a failed cleanup to the stable typed code the
// streak is counted against. The raw message cannot be used: it carries job
// and attempt identifiers, so every retry would look like a different refusal
// and no streak would ever form.
func removalRefusalCode(cause error) (string, string) {
	var rpcErr *ocihelper.RPCError
	if errors.As(cause, &rpcErr) && strings.TrimSpace(string(rpcErr.Code)) != "" {
		return string(rpcErr.Code), rpcErr.Message
	}
	var reasoned interface{ ControlFailureReason() string }
	if errors.As(cause, &reasoned) && strings.TrimSpace(reasoned.ControlFailureReason()) != "" {
		return reasoned.ControlFailureReason(), ""
	}
	var protocolErr *ProtocolError
	if errors.As(cause, &protocolErr) && strings.TrimSpace(string(protocolErr.APIError.Code)) != "" {
		return "l1_" + string(protocolErr.APIError.Code), protocolErr.APIError.Message
	}
	return "", ""
}

// acknowledgeStall freezes the declaration before it is sent and replays the
// frozen bytes until L1 accepts them. A lost response is indistinguishable
// from a refusal, and rebuilding the declaration on the retry would advance
// the attempt count under the same key -- turning an already accepted
// declaration into a permanent idempotency conflict. The key is derived
// without the boot session for the same reason: the retry that finally lands
// may come from a later boot.
func (controller *removalController) acknowledgeStall(ctx context.Context, removal localRemoval, record runtimeRemovalRecord) error {
	if controller.client == nil {
		return errors.New("declaring a stalled removal requires an L1 client")
	}
	if controller.freezeStall == nil {
		return errors.New("declaring a stalled removal requires durable declaration storage")
	}
	lastAttemptedAt := record.preparedAt
	if record.lastAttemptedAt != nil {
		lastAttemptedAt = *record.lastAttemptedAt
	}
	proposed, err := json.Marshal(l1.ServiceRemovalStallEvidence{
		Kind: l1.ServiceRemovalStallEvidenceKind, JobID: removal.jobID, NodeID: controller.nodeID,
		RemovalGeneration: removal.generation, CleanupFence: removal.cleanupFence, Phase: string(record.phase),
		LastRefusalCode: record.lastRefusalCode, LastRefusalDetail: record.lastRefusalDetail,
		Attempts: record.failedAttempts, PreparedAt: record.preparedAt, LastAttemptedAt: lastAttemptedAt,
	})
	if err != nil {
		return fmt.Errorf("encode stalled service removal declaration: %w", err)
	}
	frozen, key, err := controller.freezeStall(ctx, removal, proposed,
		removalStallAcknowledgementKey(removal))
	if err != nil {
		return err
	}
	var declaration l1.ServiceRemovalStallEvidence
	if err := json.Unmarshal(frozen, &declaration); err != nil {
		return fmt.Errorf("decode frozen stalled service removal declaration: %w", err)
	}
	request := l1.RemovalAcknowledgementRequest{
		NodeID: controller.nodeID, BootSessionID: controller.bootSessionID,
		RemovalGeneration: removal.generation, CleanupFence: removal.cleanupFence,
		RootInstanceID: removal.rootInstanceID, IdempotencyKey: key,
		CleanupStall: &declaration,
	}
	if _, err := controller.client.AcknowledgeRemoval(ctx, removal.jobID, request); err != nil {
		return fmt.Errorf("declare stalled service removal: %w", err)
	}
	return nil
}

func (controller *removalController) acknowledgeQuarantine(ctx context.Context, removal localRemoval, cleanupErr error) error {
	request, quarantined := storageCleanupQuarantineAcknowledgement(cleanupErr, removal.rootInstanceID, l1.ComputerStorageCleanupRemoval)
	if !quarantined {
		return errors.New("acknowledge Computer removal quarantine requires typed cleanup evidence")
	}
	if controller.client == nil {
		return errors.New("acknowledge Computer removal quarantine requires an L1 client")
	}
	if _, err := controller.client.AcknowledgeRemoval(ctx, removal.jobID, request); err != nil {
		return fmt.Errorf("acknowledge quarantined Computer removal: %w", err)
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

// removalStallAcknowledgementKey derives the declaration's key under its own
// namespace, so declaring a stall never forecloses a later genuine completion
// from the same boot. Unlike the completion key it deliberately omits the boot
// session: a declaration L1 may already have committed must be replayable
// after the restart a lost response can straddle, and a key that changed with
// the boot would turn that retry into a conflict.
func removalStallAcknowledgementKey(removal localRemoval) string {
	digest := sha256.Sum256([]byte(fmt.Sprintf(
		"%s\x00%d\x00%s\x00%s",
		removal.jobID, removal.generation, removal.cleanupFence, removal.rootInstanceID,
	)))
	return l1.ServiceRemovalStallKeyPrefix + hex.EncodeToString(digest[:])
}
