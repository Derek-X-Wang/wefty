package agent

import (
	"context"
	"errors"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

// immutableBackupRuntime is the fault the Mac Computer lane staged with
// `chattr +i`: every Backup-copy deletion comes back as the helper's typed
// per-copy engine refusal, forever. Nothing else about the runtime matters
// here -- the defect was never about the copy, it was about what the node did
// with the refusal.
type immutableBackupRuntime struct {
	deleted chan string
	// untyped turns the same refusal into the shape #513's exclusion could not
	// see: a transport loss that carries no code at all. It is the same retry
	// of the same removal, one lost connection away from the typed refusal.
	untyped atomic.Bool
}

func newImmutableBackupRuntime() *immutableBackupRuntime {
	return &immutableBackupRuntime{deleted: make(chan string, 16)}
}

func (*immutableBackupRuntime) Preflight(context.Context, workloadrunner.Request) (workloadrunner.Admission, workloadrunner.Result, error) {
	return workloadrunner.Admission{}, workloadrunner.Result{}, errors.New("immutableBackupRuntime runs no workload")
}

func (*immutableBackupRuntime) Run(context.Context, workloadrunner.Request, workloadrunner.OutputSink) (workloadrunner.Result, error) {
	return workloadrunner.Result{}, errors.New("immutableBackupRuntime runs no workload")
}

func (*immutableBackupRuntime) ReapAndVerify(context.Context, workloadrunner.ReapRequest) (workloadrunner.ReapReceipt, error) {
	return workloadrunner.ReapReceipt{RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidenceNoRuntime}, nil
}

func (*immutableBackupRuntime) CreateComputerBackup(context.Context, workloadrunner.ComputerBackupRequest) (workloadrunner.ComputerBackupCopyReceipt, error) {
	return workloadrunner.ComputerBackupCopyReceipt{}, errors.New("immutableBackupRuntime creates no Backup")
}

func (runtime *immutableBackupRuntime) DeleteComputerBackupCopy(_ context.Context, request workloadrunner.ComputerBackupCopyRemovalRequest) (workloadrunner.ComputerBackupCopyRemovalReceipt, error) {
	select {
	case runtime.deleted <- request.CopyID:
	default:
	}
	if runtime.untyped.Load() {
		return workloadrunner.ComputerBackupCopyRemovalReceipt{}, errors.New("OCI helper session is unavailable")
	}
	return workloadrunner.ComputerBackupCopyRemovalReceipt{}, &ocihelper.RPCError{
		Code: ocihelper.CodeEngineFailure, Message: "OCI engine operation failed",
		EngineFailure: &ocihelper.EngineFailureFact{
			Operation: ocihelper.MethodDeleteBackup, Reason: ocihelper.EngineFailurePermissionDenied,
		},
	}
}

func (runtime *immutableBackupRuntime) attempted() []string {
	var copies []string
	for {
		select {
		case copyID := <-runtime.deleted:
			copies = append(copies, copyID)
		default:
			return copies
		}
	}
}

// stalledRemovalNode is a registered node whose durable state carries one
// Computer removal, optionally already declared stalled, plus the standing
// directives L1 keeps redispatching for it.
type stalledRemovalNode struct {
	agent    *Agent
	runtime  *immutableBackupRuntime
	removal  localRemoval
	response l1.HeartbeatResponse
	computer string
	copyIDs  []string
}

func newStalledRemovalNode(t *testing.T, name string, declareStall bool) *stalledRemovalNode {
	t.Helper()
	network := plain.NewNetwork()
	_, stopServer := startFailureServer(t, network, nil, map[string][]string{name: nil})
	t.Cleanup(stopServer)
	runtime := newImmutableBackupRuntime()
	agentFabric := network.NewFabric(fabric.Identity{
		NodeID: "fabric-" + name, Tags: []string{l1.DefaultAgentPrincipalTag},
	})
	managedRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	nodeAgent, err := New(Config{
		Fabric: agentFabric, ControlPlaneAddress: "wefty://control-plane", NodeID: name,
		BootSessionID: "boot-" + name, Version: "test", OS: "linux", Architecture: "amd64",
		Capabilities: map[string]bool{"kind:process": true},
		CapabilityProbe: capabilityProbeFunc(func(context.Context) (CapabilityProbeResult, error) {
			return CapabilityProbeResult{Capabilities: map[string]bool{"kind:oci": true}}, nil
		}),
		OCIIntent: enabledTestOCIIntent, OCIBootBarrier: readyOCIBootBarrier{},
		WorkloadRuntimes:       map[string]WorkloadRuntime{contract.JobKindOCI: runtime},
		HeartbeatInterval:      time.Hour,
		ClaimInterval:          time.Hour,
		ManagedRootDirectory:   managedRoot,
		LogSpoolDirectory:      t.TempDir(),
		CapabilityRevisionPath: filepath.Join(t.TempDir(), "wefty-capability-revision.json"),
		Logf:                   t.Logf,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { nodeAgent.Close() })
	if _, err := nodeAgent.Register(t.Context()); err != nil {
		t.Fatal(err)
	}
	if nodeAgent.session.backups == nil {
		t.Fatal("the OCI runtime did not supply the Backup seam this defect lives on")
	}

	spool := nodeAgent.session.removals.outbox.spool
	removal := localRemoval{
		jobID: name + "-job", kind: contract.JobKindOCI,
		generation: l1.InitialServiceRemovalGeneration, cleanupFence: "cleanup-fence",
		rootInstanceID: nodeAgent.session.registration.RootInstanceID,
	}
	prepared := time.Now().UTC()
	computerID, storageID := name+"-computer", name+"-storage"
	manifest := testRuntimeResourceManifest(removal.jobID, "attempt")
	manifest.NodeID = name
	manifest.BootSessionID = nodeAgent.session.registration.BootSessionID
	// A Computer removal's frozen manifest carries the Storage it owns. Boot
	// resumption reads it to decide that this removal needs the authoritative
	// generation inventory only the standing directive carries, which is what
	// keeps resume from guessing a destructive subset.
	manifest.ComputerStorage = &workloadrunner.ComputerStorage{
		ComputerID: computerID, StorageID: storageID, StorageGeneration: 1, IntentRevision: 1, DiskBytes: 8 << 30,
	}
	manifest.ServiceDataVolume, manifest.ServiceDataOwnerRecord = "", ""
	if err := spool.storeRuntimeResourceManifest(t.Context(), manifest, prepared); err != nil {
		t.Fatal(err)
	}
	if err := spool.beginRemoval(t.Context(), removal, prepared); err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < l1.MinimumServiceRemovalStallAttempts; attempt++ {
		if err := spool.recordRuntimeRemovalFailure(t.Context(), removal, string(ocihelper.CodeEngineFailure),
			"permission_denied", nodeAgent.session.registration.BootSessionID, prepared); err != nil {
			t.Fatal(err)
		}
	}
	if declareStall {
		record, found, err := spool.runtimeRemoval(t.Context(), removal.jobID)
		if err != nil || !found {
			t.Fatalf("seeded removal record found=%t err=%v", found, err)
		}
		if _, _, err := spool.freezeRuntimeRemovalStallDeclaration(t.Context(), removal,
			[]byte(`{"kind":"service_removal_cleanup_stall"}`), removalStallAcknowledgementKey(removal)); err != nil {
			t.Fatal(err)
		}
		if err := spool.recordRuntimeRemovalStallDeclared(t.Context(), removal, prepared); err != nil {
			t.Fatal(err)
		}
		if record.stallDeclaredAt != nil {
			t.Fatal("the seeded record was already declared stalled")
		}
	}

	node := &stalledRemovalNode{
		agent: nodeAgent, runtime: runtime, removal: removal,
		computer: computerID,
		copyIDs:  []string{name + "-copy-a", name + "-copy-b"},
	}
	copies := make([]l1.ComputerBackupPruneDirective, 0, len(node.copyIDs))
	for index, copyID := range node.copyIDs {
		copies = append(copies, l1.ComputerBackupPruneDirective{
			BackupID: name + "-backup", CopyID: copyID, ComputerID: node.computer,
			StorageID: storageID, StorageGeneration: 1, AllocatedSize: 1 << 20,
			BoundNodeID: name, RootInstanceID: removal.rootInstanceID,
			OperationRevision: int64(index + 1), CleanupFence: removal.cleanupFence,
		})
	}
	node.response = l1.HeartbeatResponse{
		RemovalDirectives: []l1.RemovalDirective{{
			JobID: removal.jobID, BoundNodeID: name, Kind: contract.JobKindOCI,
			RemovalGeneration: removal.generation, CleanupFence: removal.cleanupFence,
			RootInstanceID:       removal.rootInstanceID,
			ComputerStorage:      &l1.ComputerStorageClaim{ComputerID: node.computer, StorageID: storageID, StorageGeneration: 1},
			ComputerBackupCopies: &l1.ComputerBackupCopyClaims{Copies: copies},
		}},
		BackupPruneDirectives: copies,
	}
	return node
}

// TestStalledRemovalLeftoversDoNotBlockTheOCIBootBarrier is the #513
// regression, measured on owner hardware by Computer lane run 2. A Backup copy
// the helper refused to delete correctly took the removal to
// `stalled_cleanup_unverified` and released its Slot, and then the same
// refusal -- redelivered as an ordinary prune directive on every heartbeat --
// kept failing the node's OCI boot sequence. The node advertised
// `missing_capabilities: ["kind:oci"]`, a fresh Computer sat `queued` with no
// diagnosis for half an hour, and the operator had been told the Slot was free.
//
// Resources a stalled removal retains are that declaration's own subject, so
// they are outside the boot sequence's absence proof; everything else in the
// same response is still reconciled and still gates.
func TestStalledRemovalLeftoversDoNotBlockTheOCIBootBarrier(t *testing.T) {
	node := newStalledRemovalNode(t, "stalled-barrier-node", true)
	session := node.agent.session

	// The restrictive observation the boot sequence publishes before it runs
	// the standing directives; kind:oci is withdrawn at this point.
	session.capabilities.suppressOCI(contract.CapabilityReasonBootSweepFailed,
		errors.New("OCI helper session requires a new boot sweep"))
	if withdrawn := node.agent.CapabilitySnapshot(); withdrawn.Capabilities["kind:oci"] {
		t.Fatalf("restrictive observation = %+v, want kind:oci withdrawn", withdrawn)
	}

	if err := session.processStandingDirectives(t.Context(), node.response); err != nil {
		t.Fatalf("standing directives while the removal stands stalled = %v, want the boot sequence's gate open", err)
	}
	if attempted := node.runtime.attempted(); len(attempted) != 0 {
		t.Fatalf("Backup-copy deletions attempted for a stalled removal = %v, want none (its own backoff owns them)", attempted)
	}

	// The gate is open, so the boot sequence reaches its probe and its pinned
	// positive publication -- the two steps `register` and OCI recovery run
	// immediately after the standing directives.
	if err := node.agent.RecoverOCIRuntimeCapabilities(t.Context()); err != nil {
		t.Fatalf("OCI recovery with only stalled leftovers = %v, want the node to re-earn kind:oci", err)
	}
	earned := node.agent.CapabilitySnapshot()
	if !earned.Capabilities["kind:oci"] || earned.ReasonCode != "" {
		t.Fatalf("published observation = %+v, want kind:oci earned with no reason", earned)
	}

	// Nothing is special-cased durably: once the stalled removal's record is
	// released -- which is what a cleanup that finally succeeds does -- the
	// very same directives are reconciled again like any other.
	if err := node.agent.session.removals.outbox.spool.completeRemoval(t.Context(), node.removal); err != nil {
		t.Fatal(err)
	}
	if err := session.processStandingDirectives(t.Context(), node.response); err == nil {
		t.Fatal("a released removal kept its leftovers excluded from the boot sequence")
	}
	attempted := node.runtime.attempted()
	if len(attempted) != len(node.copyIDs) {
		t.Fatalf("Backup-copy deletions after the record was released = %v, want %v", attempted, node.copyIDs)
	}
}

// TestUndeclaredLeftoversStillBlockTheOCIBootBarrier keeps the guarantee the
// exclusion is carved out of. Only an accepted declaration -- L1's own record
// that this removal's cleanup is not proven and its Slot is released -- takes
// a resource out of the boot sequence's proof. The identical refusal without
// one is an unfinished removal, and it must still hold kind:oci back.
func TestUndeclaredLeftoversStillBlockTheOCIBootBarrier(t *testing.T) {
	node := newStalledRemovalNode(t, "undeclared-barrier-node", false)
	err := node.agent.session.processStandingDirectives(t.Context(), node.response)
	if err == nil {
		t.Fatal("an undeclared refused Backup-copy deletion left the boot sequence's gate open")
	}
	for _, copyID := range node.copyIDs {
		if !strings.Contains(err.Error(), copyID) {
			t.Fatalf("standing-directive failure = %v, want it to name refused copy %q", err, copyID)
		}
	}
	attempted := node.runtime.attempted()
	for _, copyID := range node.copyIDs {
		if !slices.Contains(attempted, copyID) {
			t.Fatalf("Backup-copy deletions without a declaration = %v, want %q attempted", attempted, copyID)
		}
	}
}

// TestStalledRetentionOnlyCoversItsOwnRemoval proves the exclusion is derived
// from one removal's own standing directive, not from "a Backup copy was
// refused somewhere". A second Computer's prune is untouched.
func TestStalledRetentionOnlyCoversItsOwnRemoval(t *testing.T) {
	node := newStalledRemovalNode(t, "scoped-retention-node", true)
	neighbour := l1.ComputerBackupPruneDirective{
		BackupID: "neighbour-backup", CopyID: "neighbour-copy", ComputerID: "neighbour-computer",
		StorageID: "neighbour-storage", StorageGeneration: 1, AllocatedSize: 1 << 20,
		BoundNodeID: "scoped-retention-node", RootInstanceID: node.removal.rootInstanceID,
		OperationRevision: 1, CleanupFence: "neighbour-fence",
	}
	response := node.response
	response.BackupPruneDirectives = append(append([]l1.ComputerBackupPruneDirective{}, node.response.BackupPruneDirectives...), neighbour)

	err := node.agent.session.processStandingDirectives(t.Context(), response)
	if err == nil || !strings.Contains(err.Error(), neighbour.CopyID) {
		t.Fatalf("standing directives = %v, want the unrelated Computer's prune still reconciled and still gating", err)
	}
	for _, copyID := range node.copyIDs {
		if strings.Contains(err.Error(), copyID) {
			t.Fatalf("standing-directive failure named stalled copy %q", copyID)
		}
	}
	if attempted := node.runtime.attempted(); len(attempted) != 1 || attempted[0] != neighbour.CopyID {
		t.Fatalf("Backup-copy deletions = %v, want only %q", attempted, neighbour.CopyID)
	}
}

// TestStalledRetentionOnlyCoversTheCopiesItsDirectiveNames keeps the
// suppression inside the boundary the contract states. A Computer-wide match
// looked equivalent and is not: the stalled removal's own retries receive only
// the copies its standing directive names, so a copy of the same Computer that
// the directive does not name would be reconciled by nothing at all, and the
// boot sequence would return success without it.
func TestStalledRetentionOnlyCoversTheCopiesItsDirectiveNames(t *testing.T) {
	node := newStalledRemovalNode(t, "unlisted-copy-node", true)
	named := node.response.BackupPruneDirectives[0]
	unlisted := named
	unlisted.CopyID = "unlisted-copy-node-copy-c"
	unlisted.OperationRevision = 9
	response := node.response
	response.BackupPruneDirectives = []l1.ComputerBackupPruneDirective{named, unlisted}

	err := node.agent.session.processStandingDirectives(t.Context(), response)
	if err == nil || !strings.Contains(err.Error(), unlisted.CopyID) {
		t.Fatalf("standing directives = %v, want the unlisted copy of the same Computer still reconciled and still gating", err)
	}
	if strings.Contains(err.Error(), named.CopyID) {
		t.Fatalf("standing-directive failure named the stalled directive's own copy %q", named.CopyID)
	}
	if attempted := node.runtime.attempted(); len(attempted) != 1 || attempted[0] != unlisted.CopyID {
		t.Fatalf("Backup-copy deletions = %v, want only %q", attempted, unlisted.CopyID)
	}
}

// TestStalledRetentionRequiresMatchingRemovalAuthority refuses to suppress on
// a shared job ID. The durable record proves only that some removal of that
// job was declared stalled; record validation proves a row internally
// consistent, never that it is the removal this standing directive carries.
// A directive that disagrees on removal authority is reconciled like any other.
func TestStalledRetentionRequiresMatchingRemovalAuthority(t *testing.T) {
	for _, testCase := range []struct {
		name    string
		mutate  func(*l1.RemovalDirective)
		mutated string
	}{
		{name: "removal generation", mutated: "generation", mutate: func(directive *l1.RemovalDirective) {
			directive.RemovalGeneration++
		}},
		{name: "cleanup fence", mutated: "fence", mutate: func(directive *l1.RemovalDirective) {
			directive.CleanupFence = "a-different-cleanup-fence"
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			node := newStalledRemovalNode(t, "authority-"+testCase.mutated+"-node", true)
			response := node.response
			directive := response.RemovalDirectives[0]
			testCase.mutate(&directive)
			response.RemovalDirectives = []l1.RemovalDirective{directive}

			retention := node.agent.session.removals.declaredStalledRetention(t.Context(), response.RemovalDirectives)
			if !retention.empty() {
				t.Fatalf("retention derived from a directive whose %s disagrees with the durable record: %+v", testCase.name, retention)
			}
			err := node.agent.session.processStandingDirectives(t.Context(), response)
			if err == nil {
				t.Fatalf("a directive whose %s disagrees with the durable record left the boot sequence's gate open", testCase.name)
			}
			attempted := node.runtime.attempted()
			for _, copyID := range node.copyIDs {
				if !slices.Contains(attempted, copyID) {
					t.Fatalf("Backup-copy deletions = %v, want %q reconciled", attempted, copyID)
				}
			}
		})
	}
}

// releaseDeclaredRetryBackoff moves the removal controller's injected clock
// past the declared removal's durable backoff, so its own retry runs in this
// pass instead of being deferred. The removal fixtures assert the outcome of
// that retry, never who won a race with a ticker.
func (node *stalledRemovalNode) releaseDeclaredRetryBackoff(t *testing.T) runtimeRemovalRecord {
	t.Helper()
	record, found, err := node.agent.session.removals.outbox.spool.runtimeRemoval(t.Context(), node.removal.jobID)
	if err != nil || !found {
		t.Fatalf("seeded removal record found=%t err=%v", found, err)
	}
	if record.stallDeclaredAt == nil || record.lastAttemptedAt == nil {
		t.Fatal("the seeded removal was not declared stalled with a recorded attempt")
	}
	released := record.lastAttemptedAt.Add(declaredRemovalRetryMax + time.Second)
	node.agent.session.removals.now = func() time.Time { return released }
	return record
}

// seedNeighbourRemoval adds a second Computer removal to the same node's
// durable state -- never declared stalled -- and returns the standing directive
// L1 keeps redispatching for it.
func (node *stalledRemovalNode) seedNeighbourRemoval(t *testing.T, jobID string) l1.RemovalDirective {
	t.Helper()
	session := node.agent.session
	spool := session.removals.outbox.spool
	computerID, storageID := jobID+"-computer", jobID+"-storage"
	removal := localRemoval{
		jobID: jobID, kind: contract.JobKindOCI,
		generation: l1.InitialServiceRemovalGeneration, cleanupFence: jobID + "-fence",
		rootInstanceID: session.registration.RootInstanceID,
	}
	prepared := time.Now().UTC()
	manifest := testRuntimeResourceManifest(jobID, jobID+"-attempt")
	manifest.NodeID = session.registration.NodeID
	manifest.BootSessionID = session.registration.BootSessionID
	manifest.ComputerStorage = &workloadrunner.ComputerStorage{
		ComputerID: computerID, StorageID: storageID, StorageGeneration: 1, IntentRevision: 1, DiskBytes: 8 << 30,
	}
	manifest.ServiceDataVolume, manifest.ServiceDataOwnerRecord = "", ""
	if err := spool.storeRuntimeResourceManifest(t.Context(), manifest, prepared); err != nil {
		t.Fatal(err)
	}
	if err := spool.beginRemoval(t.Context(), removal, prepared); err != nil {
		t.Fatal(err)
	}
	return l1.RemovalDirective{
		JobID: jobID, BoundNodeID: session.registration.NodeID, Kind: contract.JobKindOCI,
		RemovalGeneration: removal.generation, CleanupFence: removal.cleanupFence,
		RootInstanceID:  removal.rootInstanceID,
		ComputerStorage: &l1.ComputerStorageClaim{ComputerID: computerID, StorageID: storageID, StorageGeneration: 1},
		ComputerBackupCopies: &l1.ComputerBackupCopyClaims{Copies: []l1.ComputerBackupPruneDirective{{
			BackupID: jobID + "-backup", CopyID: jobID + "-copy", ComputerID: computerID,
			StorageID: storageID, StorageGeneration: 1, AllocatedSize: 1 << 20,
			BoundNodeID: session.registration.NodeID, RootInstanceID: removal.rootInstanceID,
			OperationRevision: 1, CleanupFence: removal.cleanupFence,
		}}},
	}
}

// TestUntypedRetryFailureOfADeclaredStalledRemovalDoesNotGateTheBarrier is the
// half of the Mac Computer wedge #513 left behind. Excluding the duplicate
// prune took the typed per-copy refusal off the boot sequence's gate, but the
// same retry of the same removal reaching the helper over a connection that
// drops fails untyped -- and an untyped failure carries no refusal code, so
// nothing about a declaration could recognize it. The node withdrew `kind:oci`
// again, for work it had already told L1 it cannot finish, while that
// removal's Slot stood free.
//
// A declared stall is L1's own record that this cleanup is not proven and the
// Slot is released. Its retries are that removal's cadence, whatever shape
// their failures take, so the node keeps admitting OCI work and the failure is
// accounted and kept visible on the removal instead.
func TestUntypedRetryFailureOfADeclaredStalledRemovalDoesNotGateTheBarrier(t *testing.T) {
	node := newStalledRemovalNode(t, "untyped-stalled-node", true)
	session := node.agent.session
	node.runtime.untyped.Store(true)
	before := node.releaseDeclaredRetryBackoff(t)

	// The restrictive observation the boot sequence publishes before it runs
	// the standing directives; kind:oci is withdrawn at this point.
	session.capabilities.suppressOCI(contract.CapabilityReasonBootSweepFailed,
		errors.New("OCI helper session requires a new boot sweep"))

	if err := session.processStandingDirectives(t.Context(), node.response); err != nil {
		t.Fatalf("standing directives while a declared-stalled removal's retry lost its transport = %v, "+
			"want the boot sequence's gate open", err)
	}
	attempted := node.runtime.attempted()
	if len(attempted) != 1 || !slices.Contains(node.copyIDs, attempted[0]) {
		t.Fatalf("Backup-copy deletions = %v, want exactly one of the removal's own copies %v "+
			"(the retry must really have run)", attempted, node.copyIDs)
	}

	// The gate is open, so the boot sequence reaches its probe and its pinned
	// positive publication, and the heartbeat that carries it goes out.
	if err := node.agent.RecoverOCIRuntimeCapabilities(t.Context()); err != nil {
		t.Fatalf("OCI recovery while the stalled removal keeps failing untyped = %v, want the node to re-earn kind:oci", err)
	}
	earned := node.agent.CapabilitySnapshot()
	if !earned.Capabilities["kind:oci"] || earned.ReasonCode != "" {
		t.Fatalf("published observation = %+v, want kind:oci earned with no reason", earned)
	}

	// The failure is not swallowed: it is accounted on the removal, and the
	// record `wefty node doctor` reads shows the attempt and its shape.
	after, found, err := session.removals.outbox.spool.runtimeRemoval(t.Context(), node.removal.jobID)
	if err != nil || !found {
		t.Fatalf("removal record after the failed retry found=%t err=%v", found, err)
	}
	if after.stallDeclaredAt == nil {
		t.Fatal("the removal stopped being declared stalled")
	}
	if after.stallRetryAttempts <= before.stallRetryAttempts {
		t.Fatalf("stall retry attempts = %d, want more than the %d recorded before the retry failed",
			after.stallRetryAttempts, before.stallRetryAttempts)
	}
	if after.lastAttemptedAt == nil || !after.lastAttemptedAt.After(*before.lastAttemptedAt) {
		t.Fatalf("last attempted = %v, want the failed retry recorded after %v", after.lastAttemptedAt, before.lastAttemptedAt)
	}
	if after.lastRefusalCode != "" || after.failedAttempts != 0 {
		t.Fatalf("refusal streak = %q/%d, want an untyped failure to leave no refusal code and end the streak",
			after.lastRefusalCode, after.failedAttempts)
	}
}

// TestUntypedRetryFailureWithoutADeclarationStillGatesTheBarrier is the
// preservation test: it passes before this change as well as after. Only L1's
// accepted declaration -- its own record that cleanup is not proven and the
// Slot is released -- takes a removal's failures off the gate. The identical
// untyped failure of a removal that has declared nothing is an unfinished boot
// step, and it must still hold `kind:oci` back.
func TestUntypedRetryFailureWithoutADeclarationStillGatesTheBarrier(t *testing.T) {
	node := newStalledRemovalNode(t, "untyped-undeclared-node", false)
	node.runtime.untyped.Store(true)

	err := node.agent.session.processStandingDirectives(t.Context(), node.response)
	if err == nil {
		t.Fatal("an undeclared removal whose retry failed untyped left the boot sequence's gate open")
	}
	if !strings.Contains(err.Error(), node.removal.jobID) {
		t.Fatalf("standing-directive failure = %v, want it to name the unfinished removal %q", err, node.removal.jobID)
	}
	if attempted := node.runtime.attempted(); len(attempted) == 0 {
		t.Fatal("the undeclared removal's retry never reached the runtime")
	}
}

// TestAnotherRemovalsFailureStillGatesWhileOneStandsStalled is the boundary.
// The exemption is attributed to one removal's own retries, not to "a node has
// a stalled removal on it". A second removal on the same node, failing in the
// very same untyped way, is work L1 is still waiting on with nothing declared
// about it, so it still withdraws the node's OCI capability.
func TestAnotherRemovalsFailureStillGatesWhileOneStandsStalled(t *testing.T) {
	node := newStalledRemovalNode(t, "two-removal-node", true)
	session := node.agent.session
	node.runtime.untyped.Store(true)
	node.releaseDeclaredRetryBackoff(t)
	neighbour := node.seedNeighbourRemoval(t, "two-removal-node-neighbour")

	response := node.response
	response.RemovalDirectives = []l1.RemovalDirective{node.response.RemovalDirectives[0], neighbour}

	err := session.processStandingDirectives(t.Context(), response)
	if err == nil || !strings.Contains(err.Error(), neighbour.JobID) {
		t.Fatalf("standing directives = %v, want the undeclared second removal still reconciled and still gating", err)
	}
	if strings.Contains(err.Error(), node.removal.jobID) {
		t.Fatalf("standing-directive failure named the declared-stalled removal %q", node.removal.jobID)
	}
	attempted := node.runtime.attempted()
	if !slices.Contains(attempted, "two-removal-node-neighbour-copy") {
		t.Fatalf("Backup-copy deletions = %v, want the second removal's own copy attempted", attempted)
	}
}
