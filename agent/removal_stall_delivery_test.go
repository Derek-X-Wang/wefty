//go:build darwin || linux

package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
)

// TestRefusedOCIRemovalDeclaresAStallAndSurvivesALostResponse is the wedge end
// to end, over the real acknowledgement path: a standing directive the node
// keeps being refused, a declaration that L1 commits, and a response the node
// never sees. The retry must replay the same declaration rather than build a
// newer one -- a newer one carries a higher attempt count under the same key
// and would conflict forever, trading the old wedge for a new one.
// stalledRemovalFixture is one OCI service driven to a declared stall over the
// real acknowledgement path: real L1 store and server, real client, real spool,
// real HTTP. Only the helper is stubbed, and only to produce the refusal that
// wedged the first Mac Computer run.
type stalledRemovalFixture struct {
	ctx        context.Context
	store      *l1.Store
	client     *Client
	controller *removalController
	directive  l1.RemovalDirective
	jobID      string
	root       string
	accepted   *atomic.Int64
	close      func()
}

func newStalledRemovalFixture(t *testing.T) stalledRemovalFixture {
	return newStalledRemovalFixtureForKind(t, false)
}

func newStalledComputerRemovalFixture(t *testing.T) stalledRemovalFixture {
	return newStalledRemovalFixtureForKind(t, true)
}

func newStalledRemovalFixtureForKind(t *testing.T, computerKind bool) stalledRemovalFixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	network := plain.NewNetwork()
	serverFabric := network.NewFabric(fabric.Identity{NodeID: "control-plane"})
	// L1 measures the bound on its own clock, so the test moves that clock
	// rather than asserting the agent's word for how long it waited.
	var l1Offset atomic.Int64
	store, err := l1.OpenStore(filepath.Join(t.TempDir(), "l1.sqlite"), l1.StoreOptions{
		Clock: l1.ClockFunc(func() time.Time { return time.Now().Add(time.Duration(l1Offset.Load())) }),
	})
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	policy := l1.DefaultNodePolicy("linux")
	policy.Tags = append(policy.Tags, contract.StableNodeTagPrefix+"stall-node")
	server, err := l1.NewServer(serverFabric, store, l1.ServerConfig{
		NodePolicies: map[string]l1.NodePolicy{"stall-node": policy},
	})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := serverFabric.Listen("tcp", "wefty://control-plane")
	if err != nil {
		t.Fatal(err)
	}

	// Drop exactly one accepted acknowledgement response. L1 has committed;
	// the agent cannot tell that from a refusal.
	var accepted, dropped atomic.Int64
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || !bytes.HasSuffix([]byte(r.URL.Path), []byte("/removal-acknowledgement")) {
			server.Handler().ServeHTTP(w, r)
			return
		}
		buffered := httptest.NewRecorder()
		server.Handler().ServeHTTP(buffered, r)
		if buffered.Code == http.StatusOK {
			accepted.Add(1)
			if dropped.CompareAndSwap(0, 1) {
				// The committed response never reaches the agent.
				w.WriteHeader(http.StatusBadGateway)
				return
			}
		}
		for key, values := range buffered.Header() {
			w.Header()[key] = values
		}
		w.WriteHeader(buffered.Code)
		_, _ = w.Write(buffered.Body.Bytes())
	})
	httpServer := &http.Server{Handler: handler}
	serveDone := make(chan error, 1)
	go func() { serveDone <- httpServer.Serve(listener) }()

	agentClient, err := NewClient(network.NewFabric(fabric.Identity{
		NodeID: "fabric-agent", Tags: []string{l1.DefaultAgentPrincipalTag}}), "wefty://control-plane")
	if err != nil {
		t.Fatal(err)
	}
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	managed, err := initializeManagedResource(root, "stall-node", "stall-boot")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := agentClient.Register(ctx, contract.NodeRegistration{
		NodeID: "stall-node", BootSessionID: "stall-boot", RootInstanceID: managed.rootInstanceID(),
		OS: "linux", Architecture: "arm64", AgentVersion: "test", CapabilityRevision: 1,
		CapabilityObservedAt: time.Now(), MissingCapabilities: []string{},
		Capabilities: map[string]bool{"kind:process": true, "kind:oci": true,
			"runtime_handler:io.containerd.runc.v2": true, "cgroup_v2": true, "computer": true},
	}); err != nil {
		t.Fatal(err)
	}

	digest := "sha256:0000000000000000000000000000000000000000000000000000000000000001"
	spec := contract.JobSpec{
		SchemaVersion: contract.SchemaVersionV1, DispatchKey: "stall-delivery", Kind: contract.JobKindOCI,
		Class: contract.JobClassService, Restart: contract.RestartAlways, RoutingTags: []string{"linux"},
		RuntimeHandler: "io.containerd.runc.v2",
		Execution: contract.ExecutionSpec{OCI: &contract.OCIExecutionSpec{
			Image: contract.OCIImageSpec{Reference: "ghcr.io/example/tool:latest", Digest: &digest}}},
	}
	var computer l1.Computer
	if computerKind {
		memoryBytes := int64(64 << 20)
		spec.DispatchKey = "computer:stall-delivery"
		spec.RoutingTags = []string{contract.StableNodeTagPrefix + "stall-node"}
		spec.RuntimeHandler = ""
		spec.Execution.OCI.Limits = &contract.OCILimits{MemoryBytes: &memoryBytes}
		spec.Execution.OCI.Computer = &contract.OCIComputerSpec{
			Display:   contract.OCIComputerDisplaySpec{Protocol: contract.ComputerDisplayProtocolRFBWebSocketV1},
			DiskBytes: 1 << 30,
		}
	}
	if err := contract.ValidateJobSpec(&spec); err != nil {
		t.Fatalf("OCI service fixture does not satisfy the public contract: %v", err)
	}
	jobID := ""
	if computerKind {
		computer, _, err = store.CreateComputer(ctx, l1.CreateComputerRequest{Name: "stall-delivery", Spec: spec, Actor: "operator"})
		if err != nil {
			t.Fatal(err)
		}
		jobID = computer.CurrentJobID
	} else {
		job, _, createErr := store.CreateJob(ctx, spec)
		if createErr != nil {
			t.Fatal(createErr)
		}
		jobID = job.JobID
	}
	claim, err := agentClient.Claim(ctx, "stall-node", "stall-boot", contract.JobClassService)
	if err != nil || claim == nil || claim.Job.JobID != jobID {
		t.Fatalf("claim=%+v err=%v", claim, err)
	}
	outbox, err := newEvidenceOutbox(t.TempDir(), "stall-node", 1<<20, systemClock{}, 1, time.Second, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := outbox.spool.ensureAttempt(ctx, *claim); err != nil {
		t.Fatal(err)
	}
	manifest := testRuntimeResourceManifest(jobID, claim.Lease.AttemptID)
	if computerKind {
		manifest.ServiceDataVolume = ""
		manifest.ServiceDataOwnerRecord = ""
		manifest.ComputerStorage = &workloadrunner.ComputerStorage{ComputerID: computer.ComputerID,
			StorageID: computer.StorageID, StorageGeneration: computer.StorageGeneration, DiskBytes: computer.DesiredDiskBytes}
	}
	if err := outbox.spool.storeRuntimeResourceManifest(ctx, manifest, time.Now()); err != nil {
		t.Fatal(err)
	}
	if computerKind {
		if _, err := store.RemoveComputer(ctx, computer.ComputerID, l1.ComputerRemoveRequest{
			ComputerMutationPrecondition: l1.ComputerMutationPrecondition{IntentRevision: computer.IntentRevision,
				StorageID: computer.StorageID, StorageGeneration: computer.StorageGeneration, Actor: "operator"},
		}); err != nil {
			t.Fatal(err)
		}
	} else if _, err := store.RemoveService(ctx, jobID); err != nil {
		t.Fatal(err)
	}
	directives, err := store.ListNodeRemovalDirectives(ctx, "fabric-agent", "stall-node", "stall-boot")
	if err != nil || len(directives) != 1 {
		t.Fatalf("directives=%+v err=%v", directives, err)
	}
	// The directive now stands past both sides of the bound.
	l1Offset.Store(int64(l1.DefaultRemovalStallBound + time.Minute))

	controller := newRemovalController(agentClient, outbox, managed, nil, "stall-node", "stall-boot", t.Logf)
	// The helper is the one seam: it answers the refusal that produced #450.
	// Everything past it -- accounting, the bound, freezing, the HTTP
	// acknowledgement and L1's transaction -- is real.
	controller.reapService = func(context.Context, string, string, []workloadrunner.RuntimeResourceManifest) (workloadrunner.ReapReceipt, error) {
		return workloadrunner.ReapReceipt{}, wedgeRefusal()
	}
	past := time.Now().Add(l1.DefaultRemovalStallBound + time.Minute)
	controller.now = func() time.Time { return past }

	return stalledRemovalFixture{
		ctx: ctx, store: store, client: agentClient, controller: controller, directive: directives[0],
		jobID: jobID, root: root, accepted: &accepted,
		close: func() {
			cancel()
			_ = httpServer.Close()
			if err := <-serveDone; err != nil && err != http.ErrServerClosed {
				t.Errorf("serve: %v", err)
			}
			agentClient.Close()
			outbox.Close()
			store.Close()
		},
	}
}

// declareStall drives the heartbeats that build the streak, declare, lose the
// response, and replay.
func (fixture stalledRemovalFixture) declareStall(t *testing.T) {
	t.Helper()
	for beat := 0; beat < 5; beat++ {
		if err := fixture.controller.reconcile(fixture.ctx, fixture.directive); err != nil {
			t.Logf("beat %d still refused: %v", beat, err)
		}
	}
}

// TestRefusedOCIRemovalDeclaresAStallAndSurvivesALostResponse is the wedge end
// to end, over the real acknowledgement path: a standing directive the node
// keeps being refused, a declaration that L1 commits, and a response the node
// never sees. The retry must replay the same declaration rather than build a
// newer one -- a newer one carries a higher attempt count under the same key
// and would conflict forever, trading the old wedge for a new one.
func TestRefusedOCIRemovalDeclaresAStallAndSurvivesALostResponse(t *testing.T) {
	fixture := newStalledRemovalFixture(t)
	defer fixture.close()
	fixture.declareStall(t)
	if fixture.accepted.Load() < 2 {
		t.Fatalf("L1 accepted %d declarations; the lost response was never retried", fixture.accepted.Load())
	}

	observed, err := fixture.store.GetJob(fixture.ctx, fixture.jobID)
	if err != nil {
		t.Fatal(err)
	}
	if observed.State != contract.JobStalledCleanupUnverified || observed.Removal == nil ||
		observed.Removal.RemovalOutcome != l1.ServiceRemovalOutcomeCleanupStalled {
		t.Fatalf("declared removal projection = %#v", observed)
	}
	if observed.Removal.Stall == nil || observed.Removal.Stall.LastRefusalCode != "unauthorized_attempt" ||
		observed.Removal.Stall.Attempts < l1.MinimumServiceRemovalStallAttempts {
		t.Fatalf("declared evidence = %#v", observed.Removal.Stall)
	}
	// A further identical refusal stays a no-op rather than resending a newer
	// body, but an unrelated failure is still the caller's problem.
	if err := fixture.controller.reconcile(fixture.ctx, fixture.directive); err != nil {
		t.Fatalf("refusal after a declared stall was reported as a boot failure: %v", err)
	}
	nodes, err := fixture.store.ListNodes(fixture.ctx)
	if err != nil || len(nodes) != 1 {
		t.Fatalf("nodes = %#v, %v", nodes, err)
	}
	if nodes[0].ServiceOccupancy != 0 {
		t.Fatalf("service occupancy after a declared stall = %d, want 0", nodes[0].ServiceOccupancy)
	}
}

// stallPinRuntime is a minimal image-pin runtime: it holds one pin and deletes
// it exactly when L1's binding proof says the node no longer owes it. That is
// the behaviour the real adapter has, and the behaviour a stalled removal must
// survive.
type stallPinRuntime struct {
	pinned     map[string]struct{}
	reconciles int
}

func (runtime *stallPinRuntime) SetOCIImageBindingPinLedger(workloadrunner.OCIImageBindingPinLedger) {
}

func (runtime *stallPinRuntime) ReleaseOCIImageBindingPin(_ context.Context, jobID string) error {
	delete(runtime.pinned, jobID)
	return nil
}

func (runtime *stallPinRuntime) ReconcileOCIImagePins(ctx context.Context,
	prove workloadrunner.OCIImageBindingProof) ([]workloadrunner.OCIImagePinReconciliationFailure, error) {
	runtime.reconciles++
	for jobID := range runtime.pinned {
		bound, err := prove(ctx, jobID)
		if err != nil {
			return nil, err
		}
		if !bound {
			delete(runtime.pinned, jobID)
		}
	}
	return nil, nil
}

// TestRestartAfterADeclaredStallStillRestoresTheRetainedImagePin drives the
// boot path itself. Before this change the expected refusal from an
// already-declared stall landed in the registration error, registration
// skipped image-pin reconciliation entirely, and the pin the standing
// directive still needs was never restored (#450).
func TestRestartAfterADeclaredStallStillRestoresTheRetainedImagePin(t *testing.T) {
	fixture := newStalledRemovalFixture(t)
	defer fixture.close()
	fixture.declareStall(t)
	fixture.completeManagedRootRemoval(t)

	// Boot through agentSession.register: the runtime record is quarantined,
	// managed-root resume returns its completed tombstone, and the helper still
	// refuses. Every path must share the declared-removal deferral.
	pins := &stallPinRuntime{pinned: map[string]struct{}{fixture.jobID: {}}}
	session := fixture.rebootSession(t, pins)
	node, err := session.register(fixture.ctx)
	if err != nil || !node.Capabilities["kind:oci"] || !session.capabilities.snapshot().Capabilities["kind:oci"] {
		t.Fatalf("registration suppressed OCI after declared cleanup refusal: node=%+v err=%v", node, err)
	}
	if pins.reconciles != 1 {
		t.Fatalf("image-pin reconciliation ran %d times, want 1", pins.reconciles)
	}
	if _, retained := pins.pinned[fixture.jobID]; !retained {
		t.Fatal("the stalled removal's image pin was deleted; its standing directive still needs it")
	}

	// The Slot it gave back is really free: a fresh service places on it.
	fresh := contract.JobSpec{
		SchemaVersion: contract.SchemaVersionV1, DispatchKey: "stall-successor", Kind: contract.JobKindProcess,
		Class: contract.JobClassService, Restart: contract.RestartAlways, RoutingTags: []string{"linux"},
		Execution: contract.ExecutionSpec{Executable: contract.ExecutableSpec{Path: "/bin/true"},
			Argv: []string{"true"}, WorkingDirectory: fixture.root},
	}
	successor, _, err := fixture.store.CreateJob(fixture.ctx, fresh)
	if err != nil {
		t.Fatal(err)
	}
	// The fixture's L1 clock jumped past the stall bound, so the node must
	// heartbeat before it can claim again; liveness is not what this asserts.
	if _, err := fixture.client.Heartbeat(fixture.ctx, "stall-node", l1.HeartbeatRequest{
		BootSessionID: session.registration.BootSessionID, CapabilityRevision: node.CapabilityRevision,
		CapabilityObservedAt: node.CapabilityObservedAt,
		MissingCapabilities:  []string{},
		Capabilities: map[string]bool{"kind:process": true, "kind:oci": true,
			"runtime_handler:io.containerd.runc.v2": true},
	}); err != nil {
		t.Fatal(err)
	}
	claim, err := fixture.client.Claim(fixture.ctx, "stall-node", session.registration.BootSessionID, contract.JobClassService)
	if err != nil || claim == nil || claim.Job.JobID != successor.JobID {
		t.Fatalf("fresh service placement after a declared stall = %+v, %v", claim, err)
	}

	// The obligation ends only when positive cleanup does.
	bound, err := fixture.store.ProveServiceBinding(fixture.ctx, "fabric-agent", fixture.jobID,
		l1.ServiceBindingProofRequest{NodeID: "stall-node", BootSessionID: session.registration.BootSessionID})
	if err != nil || !bound {
		t.Fatalf("binding proof before cleanup = %t, %v", bound, err)
	}
}

func TestDeclaredStorageOnlyRefreshRefusalAtRetryDeadlineDoesNotPoisonRegistration(t *testing.T) {
	fixture := newStalledRemovalFixture(t)
	defer fixture.close()
	fixture.declareStall(t)
	record, found, err := fixture.controller.loadRuntimeRemoval(fixture.ctx, fixture.jobID)
	if err != nil || !found || record.stallDeclaredAt == nil {
		t.Fatalf("declared runtime removal = %+v found=%t err=%v", record, found, err)
	}
	storage := &workloadrunner.ComputerStorage{
		ComputerID: "computer-storage-only", StorageID: "storage-only", StorageGeneration: 1, DiskBytes: 8 << 30,
	}
	attempt := workloadrunner.RuntimeResourceManifest{
		Version: 1, RuntimeKind: contract.JobKindOCI, NodeID: fixture.controller.nodeID,
		BootSessionID: fixture.controller.bootSessionID, JobID: fixture.jobID,
		AttemptID: contract.StorageOnlyRemovalAttemptID(storage.StorageGeneration), FencingToken: record.removal.cleanupFence,
		WorkloadClass: contract.JobClassService, RemovalGeneration: fmt.Sprint(record.removal.generation),
		StorageOnly: true, ComputerStorage: storage,
		StoragePreparation: &contract.ComputerStoragePreparationWitness{
			Kind: contract.ComputerStorageCopyVerifiedKind, ReceiptID: "storage-only-receipt",
			NodeID: fixture.controller.nodeID, RootInstanceID: record.removal.rootInstanceID, JobID: fixture.jobID,
			ComputerID: storage.ComputerID, StorageID: storage.StorageID, StorageGeneration: storage.StorageGeneration,
			Revision: 1, Fence: "storage-only-fence", HelperGeneration: 1,
		},
	}
	payload, err := json.Marshal(runtimeRemovalManifest{Version: 1, JobID: fixture.jobID,
		RemovalGeneration: record.removal.generation, Attempts: []workloadrunner.RuntimeResourceManifest{attempt}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.controller.outbox.spool.db.ExecContext(fixture.ctx,
		`UPDATE runtime_removal_manifests SET manifest_json=? WHERE job_id=?`, payload, fixture.jobID); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.controller.outbox.spool.db.ExecContext(fixture.ctx,
		`DELETE FROM runtime_service_manifests WHERE job_id=?`, fixture.jobID); err != nil {
		t.Fatal(err)
	}

	pins := &stallPinRuntime{pinned: map[string]struct{}{fixture.jobID: {}}}
	session := fixture.rebootSession(t, pins)
	reconstructCalls := 0
	session.removals.reconstructRuntime = func(context.Context, workloadrunner.RuntimeRemovalProofRequest) ([]workloadrunner.RuntimeResourceManifest, error) {
		reconstructCalls++
		return nil, wedgeRefusal()
	}
	session.removals.now = func() time.Time { return record.lastAttemptedAt.Add(declaredRemovalRetryBase - time.Second) }
	if err := session.removals.reconcile(fixture.ctx, fixture.directive); err != nil {
		t.Fatal(err)
	}
	if reconstructCalls != 0 {
		t.Fatalf("declared Storage-only removal reconstructed %d times before its retry deadline, want 0", reconstructCalls)
	}
	session.removals.now = func() time.Time { return record.lastAttemptedAt.Add(declaredRemovalRetryMax + time.Second) }
	node, err := session.register(fixture.ctx)
	if err != nil || !node.Capabilities["kind:oci"] || !session.capabilities.snapshot().Capabilities["kind:oci"] {
		t.Fatalf("registration suppressed OCI around declared Storage-only refresh: node=%+v err=%v", node, err)
	}
	if reconstructCalls != 1 {
		t.Fatalf("declared Storage-only removal reconstructed %d times at its retry deadline, want 1", reconstructCalls)
	}
	after, found, err := session.removals.loadRuntimeRemoval(fixture.ctx, fixture.jobID)
	if err != nil || !found || after.stallRetryAttempts != record.stallRetryAttempts+1 {
		t.Fatalf("suppressed refresh refusal retry count = %d, want %d (found=%t err=%v)",
			after.stallRetryAttempts, record.stallRetryAttempts+1, found, err)
	}
	if pins.reconciles != 1 {
		t.Fatalf("image-pin reconciliation ran %d times, want 1", pins.reconciles)
	}
	if _, retained := pins.pinned[fixture.jobID]; !retained {
		t.Fatal("declared Storage-only removal lost its retained image pin")
	}
}

func (fixture stalledRemovalFixture) completeManagedRootRemoval(t *testing.T) {
	t.Helper()
	record, found, err := fixture.controller.loadRuntimeRemoval(fixture.ctx, fixture.jobID)
	if err != nil || !found {
		t.Fatalf("load stalled runtime record: found=%t err=%v", found, err)
	}
	_, cleanup, err := fixture.controller.managed.prepareAttempt(fixture.jobID, "managed-attempt")
	if err != nil {
		t.Fatal(err)
	}
	cleanup()
	if record.phase == runtimeRemovalPrepared {
		if err := fixture.controller.recordRuntimeQuiesced(fixture.ctx, record.removal, workloadrunner.ReapReceipt{
			RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidenceAttempt,
			BootSessionID: fixture.controller.bootSessionID,
		}); err != nil {
			t.Fatal(err)
		}
	}
	removal := record.removal
	removal.processTreeReaped = true
	if err := fixture.controller.managed.remove(fixture.ctx, removal); err != nil {
		t.Fatal(err)
	}
	completed, err := fixture.controller.managed.resumeRemovals(fixture.ctx)
	if err != nil || len(completed) != 1 {
		t.Fatalf("completed managed-root tombstone = %+v, %v", completed, err)
	}
	fixture.controller.deleteRuntimeData = func(context.Context, workloadrunner.RuntimeRemovalProofRequest) error {
		return wedgeRefusal()
	}
	fixture.controller.attestRuntimeRemoval = func(_ context.Context, request workloadrunner.RuntimeRemovalProofRequest) (workloadrunner.RuntimeRemovalAttestation, error) {
		return testRuntimeRemovalAttestation(runtimeRemovalManifest{Version: 1, JobID: request.JobID,
			RemovalGeneration: request.RemovalGeneration, Attempts: request.Attempts}), nil
	}
}

func (fixture stalledRemovalFixture) rebootSession(t *testing.T, pins *stallPinRuntime) *agentSession {
	t.Helper()
	bootSessionID := "stall-reboot"
	managed, err := initializeManagedResource(fixture.root, "stall-node", bootSessionID)
	if err != nil {
		t.Fatal(err)
	}
	controller := newRemovalController(fixture.client, fixture.controller.outbox, managed, nil,
		"stall-node", bootSessionID, t.Logf)
	controller.reapService = fixture.controller.reapService
	controller.deleteRuntimeData = fixture.controller.deleteRuntimeData
	controller.attestRuntimeRemoval = fixture.controller.attestRuntimeRemoval
	controller.releaseImagePin = pins.ReleaseOCIImageBindingPin
	probe := capabilityProbeFunc(func(context.Context) (CapabilityProbeResult, error) {
		return CapabilityProbeResult{Capabilities: map[string]bool{
			"kind:oci": true, "runtime_handler:io.containerd.runc.v2": true,
		}}, nil
	})
	capabilities := newCapabilityState(map[string]bool{"kind:process": true}, probe, systemClock{}, time.Second, nil)
	session := newAgentSession(fixture.client, contract.NodeRegistration{
		NodeID: "stall-node", BootSessionID: bootSessionID, RootInstanceID: managed.rootInstanceID(),
		OS: "linux", Architecture: "arm64", AgentVersion: "test",
	}, capabilities, 15*time.Second, time.Second, systemClock{}, newLifecycleObserver(systemClock{}), t.Logf, 1, 2)
	session.ociBootBarrier = readyOCIBootBarrier{}
	session.ociImagePins = pins
	session.removals = controller
	t.Cleanup(session.computerPolicy.Close)
	return session
}

func TestRestartAfterFinalizedCleanupClearsSurvivingLocalRecord(t *testing.T) {
	fixture := newStalledRemovalFixture(t)
	defer fixture.close()
	// Accept the directive once so its helper-owned runtime inventory is frozen.
	_ = fixture.controller.reconcile(fixture.ctx, fixture.directive)
	fixture.completeManagedRootRemoval(t)
	controller := fixture.controller
	controller.reapService = func(context.Context, string, string, []workloadrunner.RuntimeResourceManifest) (workloadrunner.ReapReceipt, error) {
		return workloadrunner.ReapReceipt{RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidenceAttempt,
			BootSessionID: controller.bootSessionID}, nil
	}
	controller.deleteRuntimeData = func(context.Context, workloadrunner.RuntimeRemovalProofRequest) error { return nil }
	controller.attestRuntimeRemoval = func(_ context.Context, request workloadrunner.RuntimeRemovalProofRequest) (workloadrunner.RuntimeRemovalAttestation, error) {
		return testRuntimeRemovalAttestation(runtimeRemovalManifest{Version: 1, JobID: request.JobID,
			RemovalGeneration: request.RemovalGeneration, Attempts: request.Attempts}), nil
	}
	controller.finishRemoval = func(context.Context, localRemoval) error { return errInjectedRuntimeRemovalCrash }
	for attempt := 0; attempt < 2; attempt++ {
		_ = controller.reconcile(fixture.ctx, fixture.directive)
	}
	job, err := fixture.store.GetJob(fixture.ctx, fixture.jobID)
	if err != nil || job.State != contract.JobRemovedVerified {
		t.Fatalf("finalized Job = %+v, %v", job, err)
	}
	record, found, err := controller.loadRuntimeRemoval(fixture.ctx, fixture.jobID)
	if err != nil || !found || record.phase != runtimeRemovalComplete {
		t.Fatalf("surviving local record = %+v found=%t err=%v", record, found, err)
	}
	pins := &stallPinRuntime{pinned: map[string]struct{}{fixture.jobID: {}}}
	session := fixture.rebootSession(t, pins)
	node, err := session.register(fixture.ctx)
	if err != nil || !node.Capabilities["kind:oci"] {
		t.Fatalf("returning registration suppressed OCI: node=%+v err=%v", node, err)
	}
	if _, found, err := controller.loadRuntimeRemoval(fixture.ctx, fixture.jobID); err != nil || found {
		t.Fatalf("local record survived reboot acknowledgement: found=%t err=%v", found, err)
	}
	if pins.reconciles != 1 || len(pins.pinned) != 0 {
		t.Fatalf("pin reconciliation after finalized replay = %+v", pins)
	}
}

func TestRestartAfterFinalizedStalledComputerCleanupClearsSurvivingLocalRecord(t *testing.T) {
	fixture := newStalledComputerRemovalFixture(t)
	defer fixture.close()
	fixture.declareStall(t)
	fixture.completeManagedRootRemoval(t)
	controller := fixture.controller
	controller.reapService = func(context.Context, string, string, []workloadrunner.RuntimeResourceManifest) (workloadrunner.ReapReceipt, error) {
		return workloadrunner.ReapReceipt{RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidenceAttempt,
			BootSessionID: controller.bootSessionID}, nil
	}
	controller.finalizeVolumes = func(context.Context, workloadrunner.ManagedVolumeFinalizationRequest) error { return nil }
	controller.deleteRuntimeData = func(context.Context, workloadrunner.RuntimeRemovalProofRequest) error { return nil }
	controller.attestRuntimeRemoval = func(_ context.Context, request workloadrunner.RuntimeRemovalProofRequest) (workloadrunner.RuntimeRemovalAttestation, error) {
		return testRuntimeRemovalAttestation(runtimeRemovalManifest{Version: 1, JobID: request.JobID,
			RemovalGeneration: request.RemovalGeneration, Attempts: request.Attempts}), nil
	}
	controller.finishRemoval = func(context.Context, localRemoval) error { return errInjectedRuntimeRemovalCrash }
	for attempt := 0; attempt < 2; attempt++ {
		_ = controller.reconcile(fixture.ctx, fixture.directive)
	}
	job, err := fixture.store.GetJob(fixture.ctx, fixture.jobID)
	if err != nil || job.State != contract.JobStalledCleanupUnverified {
		t.Fatalf("finalized stalled Computer Job = %+v, %v", job, err)
	}
	record, found, err := controller.loadRuntimeRemoval(fixture.ctx, fixture.jobID)
	if err != nil || !found || record.phase != runtimeRemovalComplete {
		t.Fatalf("surviving stalled Computer local record = %+v found=%t err=%v", record, found, err)
	}
	pins := &stallPinRuntime{pinned: map[string]struct{}{fixture.jobID: {}}}
	session := fixture.rebootSession(t, pins)
	node, err := session.register(fixture.ctx)
	if err != nil || !node.Capabilities["kind:oci"] {
		t.Fatalf("returning stalled Computer registration suppressed OCI: node=%+v err=%v", node, err)
	}
	if _, found, err := controller.loadRuntimeRemoval(fixture.ctx, fixture.jobID); err != nil || found {
		t.Fatalf("stalled Computer local record survived reboot acknowledgement: found=%t err=%v", found, err)
	}
	if pins.reconciles != 1 || len(pins.pinned) != 0 {
		t.Fatalf("pin reconciliation after finalized stalled Computer replay = %+v", pins)
	}
}
