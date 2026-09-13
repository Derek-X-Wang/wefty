package oci

import (
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

// A helper that no longer holds an attempt as live refuses Delete with
// unauthorized_attempt, and no later Delete can make that refusal positive.
// Run 1 of #196 retried exactly that every fifteen seconds forever and pinned a
// node service slot until the node was rebuilt (#450). Absence has to come from
// the one proof a session holding no live attempt still offers -- and the proof
// has to be real, so this drives the fake helper's namespace inventory with the
// attempt's own deterministic names: first present, so a projection that looked
// nothing up would fail the row, then absent.
func TestRemovalProvesAbsenceWhenDeleteRefusesANonLiveAttempt(t *testing.T) {
	request := adapterTestRequest()
	identity, err := ocihelper.DeterministicResourceIdentity(HelperAuthority(request.Authority))
	if err != nil {
		t.Fatal(err)
	}
	present := emptyAdapterInventory()
	present.Containers = []string{identity.ContainerID}
	present.Tasks = []string{identity.ContainerID}
	present.Cgroups = []string{"/sys/fs/cgroup/wefty/" + identity.CgroupID + ".scope"}
	// A second attempt's resources share every list and must never be read as
	// this attempt's: absence that ignores the names is not absence.
	absent := emptyAdapterInventory()
	absent.Containers = []string{"wefty-container-" + strings.Repeat("b", 32)}
	absent.Tasks = []string{"wefty-container-" + strings.Repeat("b", 32)}
	absent.Cgroups = []string{"/sys/fs/cgroup/wefty/wefty-cgroup-" + strings.Repeat("b", 32) + ".scope"}

	engine := &adapterTestEngine{
		watch:            ocihelper.WatchResponse{ExitCode: intPointer(0)},
		readOnlyVerifies: []ocihelper.VerifyResponse{namespaceVerification(present), namespaceVerification(absent)},
	}
	adapter, closeAdapter := startAdapterTestServer(t, engine)
	defer closeAdapter()
	if result, err := adapter.Run(t.Context(), request, nil); err != nil || result.Outcome.ExitCode == nil {
		t.Fatalf("run before the refusal = (%+v, %v)", result.Outcome, err)
	}
	first, err := adapter.ReapAndVerify(t.Context(), workloadrunner.ReapRequest{Authority: request.Authority})
	if err != nil || !first.RuntimeQuiesced || first.Evidence != workloadrunner.ReapEvidenceAttempt {
		t.Fatalf("ordinary attempt reap = (%+v, %v)", first, err)
	}

	// The helper has now ended this attempt on its own terms. The agent still
	// carries the run entry whenever it was the helper's guardian or deadman
	// rather than the agent that ended it, which is the run-1 shape.
	trackNonLiveAttempt(t, adapter, request)
	_, err = adapter.ReapAndVerify(t.Context(), workloadrunner.ReapRequest{Authority: request.Authority})
	if err == nil || !strings.Contains(err.Error(), identity.ContainerID) {
		t.Fatalf("a refusal with this attempt's container still present was read as absence: %v", err)
	}
	if !strings.Contains(err.Error(), "unauthorized_attempt") {
		t.Fatalf("a completed Verify showing residue lost the refusal that prompted it: %v", err)
	}

	trackNonLiveAttempt(t, adapter, request)
	receipt, err := adapter.ReapAndVerify(t.Context(), workloadrunner.ReapRequest{Authority: request.Authority})
	if err != nil {
		t.Fatalf("removal wedged on a non-live attempt refusal: %v", err)
	}
	if !receipt.RuntimeQuiesced || receipt.Evidence != workloadrunner.ReapEvidenceAttempt || receipt.BootSessionID != request.Authority.BootSessionID {
		t.Fatalf("non-live attempt absence receipt = %+v", receipt)
	}
	adapter.mu.Lock()
	_, stillTracked := adapter.runEntered[request.Authority]
	adapter.mu.Unlock()
	if stillTracked {
		t.Fatal("a proven-absent attempt stayed tracked and would be reaped again")
	}
}

// The reap outcome for a job is latched once and replayed for the rest of the
// boot (agent/session.go, serviceReaps), so a Verify that never completed must
// not become a permanent wedge with a new cause. It is a typed loss the caller
// recovers from and retries, the run entry survives it, and the next tick
// finishes the removal.
func TestNonLiveAttemptVerifyTransportFailureStaysRetryable(t *testing.T) {
	request := adapterTestRequest()
	engine := &adapterTestEngine{
		watch:              ocihelper.WatchResponse{ExitCode: intPointer(0)},
		readOnlyVerifyErrs: []error{errors.New("helper namespace read failed")},
	}
	adapter, barrier, _, closeAdapter := startAdapterTestServerWithSnapshots(t, engine, ImagePolicy{})
	defer closeAdapter()
	if result, err := adapter.Run(t.Context(), request, nil); err != nil || result.Outcome.ExitCode == nil {
		t.Fatalf("run before the refusal = (%+v, %v)", result.Outcome, err)
	}
	if _, err := adapter.ReapAndVerify(t.Context(), workloadrunner.ReapRequest{Authority: request.Authority}); err != nil {
		t.Fatalf("ordinary attempt reap: %v", err)
	}

	trackNonLiveAttempt(t, adapter, request)
	_, err := adapter.ReapAndVerify(t.Context(), workloadrunner.ReapRequest{Authority: request.Authority})
	if err == nil {
		t.Fatal("a failed absence Verify was reported as proof")
	}
	var loss *workloadrunner.RuntimeLossError
	if !errors.As(err, &loss) {
		t.Fatalf("a failed absence Verify was not typed as recoverable runtime loss: %v", err)
	}
	if strings.Contains(err.Error(), "unauthorized_attempt") {
		t.Fatalf("a transport failure was latched together with the refusal: %v", err)
	}
	adapter.mu.Lock()
	_, stillTracked := adapter.runEntered[request.Authority]
	adapter.mu.Unlock()
	if !stillTracked {
		t.Fatal("a retryable absence failure dropped the run entry the retry needs")
	}

	// The caller's bounded recovery re-establishes the barrier, exactly as it
	// does for any other typed runtime loss, and the next tick finishes the
	// removal instead of replaying a latched refusal forever.
	if err := barrier.Ensure(t.Context()); err != nil {
		t.Fatal(err)
	}
	receipt, err := adapter.ReapAndVerify(t.Context(), workloadrunner.ReapRequest{Authority: request.Authority})
	if err != nil || !receipt.RuntimeQuiesced || receipt.Evidence == "" {
		t.Fatalf("removal after a recovered absence Verify = (%+v, %v)", receipt, err)
	}
}

// A bounded retention is the whole reason a survivor is tolerated, so one that
// names no attempt, or whose bound has already passed, explains nothing.
func TestNonLiveAttemptAbsenceAcceptsOnlyAnUnexpiredHelperRetention(t *testing.T) {
	authority := ocihelper.AttemptAuthority{
		NodeID: "node", BootSessionID: "boot", JobID: "job", AttemptID: "attempt",
		FencingToken: "fence", Class: "one-shot", RemovalGeneration: "attempt",
	}
	identity, err := ocihelper.DeterministicResourceIdentity(authority)
	if err != nil {
		t.Fatal(err)
	}
	inventory := emptyAdapterInventory()
	inventory.LogSegments = []string{identity.LogSegmentDirectory}
	now := time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC)
	verification := func(recordedAt time.Time) ocihelper.VerifyResponse {
		return ocihelper.VerifyResponse{
			Inventory: inventory, RuntimeResidue: emptyAdapterInventory(),
			DurableRetentions: []ocihelper.DurableRetention{{
				Class: ocihelper.RemovalResourceLogSegments, ID: identity.LogSegmentDirectory,
				Owner: ocihelper.DurableRetentionOwnerOCIHelper, Reason: ocihelper.DurableRetentionReasonLogSpoolSealing,
				AttemptID: authority.AttemptID, State: ocihelper.DurableRetentionStateUnsealed,
				Bound: time.Minute, RecordedAt: recordedAt, Deadline: recordedAt.Add(time.Minute),
			}},
		}
	}
	if err := attemptAbsentFromNamespace(ocihelper.VerifyResponse{
		Inventory: inventory, RuntimeResidue: emptyAdapterInventory(),
	}, authority, nil, now); err == nil {
		t.Fatal("an unexplained surviving log segment was accepted as absence")
	}
	if err := attemptAbsentFromNamespace(verification(now.Add(-30*time.Second)), authority, nil, now); err != nil {
		t.Fatalf("a live sealing retention was refused: %v", err)
	}
	if err := attemptAbsentFromNamespace(verification(now.Add(-2*time.Minute)), authority, nil, now); err == nil {
		t.Fatal("a retention whose bound had already passed still excused a survivor")
	}
}

// The agent reads the read-only namespace inventory the helper serves, so it
// must narrow it exactly as the helper's own attempt scope does. They share one
// projection; this pins that they do, over an inventory carrying both this
// attempt's resources and a foreign attempt's in every list.
func TestAgentAbsenceProofUsesTheHelpersOwnAttemptProjection(t *testing.T) {
	authority := ocihelper.AttemptAuthority{
		NodeID: "node", BootSessionID: "boot", JobID: "job", AttemptID: "attempt",
		FencingToken: "fence", Class: "service", RemovalGeneration: "1",
	}
	identity, err := ocihelper.DeterministicResourceIdentity(authority)
	if err != nil {
		t.Fatal(err)
	}
	storage := ocihelper.ComputerStorageReference{
		ComputerID: "computer", StorageID: "storage", StorageGeneration: 3, IntentRevision: 1, DiskBytes: 1 << 30,
	}
	diskName, err := ocihelper.DeterministicComputerDiskName(storage)
	if err != nil {
		t.Fatal(err)
	}
	foreign := "wefty-container-" + strings.Repeat("b", 32)
	inventory := ocihelper.ResourceInventory{
		Leases:     []string{identity.LeaseID, "wefty-lease-" + strings.Repeat("b", 32)},
		Snapshots:  []string{identity.SnapshotID, "wefty-snapshot-" + strings.Repeat("b", 32)},
		Containers: []string{identity.ContainerID, foreign},
		Tasks:      []string{identity.ContainerID, foreign},
		Shims:      []string{identity.ContainerID, foreign},
		Cgroups: []string{"/sys/fs/cgroup/wefty/" + identity.CgroupID + ".scope",
			"/sys/fs/cgroup/wefty/wefty-cgroup-" + strings.Repeat("b", 32) + ".scope"},
		LogSegments:          []string{identity.LogSegmentDirectory, "wefty-log-segments-" + strings.Repeat("b", 32)},
		ManagedVolumes:       []string{identity.ServiceVolumeDirectory, "wefty-service-volume-foreign"},
		ManagedVolumeRecords: []string{identity.ServiceVolumeOwnerRecord, "wefty-service-volume-foreign.owner"},
		ComputerDiskImages:   []string{diskName, "wefty-computer-disk-foreign"},
		ComputerDiskMounts:   []string{diskName, "wefty-computer-disk-foreign"},
	}
	projected := ocihelper.ProjectAttemptInventory(inventory, identity, diskName)

	// Everything the shared projection keeps is this attempt's, and every one
	// of this attempt's names in the raw inventory is kept.
	kept := []string{}
	for _, list := range [][]string{projected.Leases, projected.Snapshots, projected.Containers, projected.Tasks,
		projected.Shims, projected.Cgroups, projected.LogSegments, projected.ManagedVolumes,
		projected.ManagedVolumeRecords, projected.ComputerDiskImages, projected.ComputerDiskMounts} {
		kept = append(kept, list...)
	}
	for _, name := range kept {
		if strings.Contains(name, "foreign") || strings.Contains(name, strings.Repeat("b", 32)) {
			t.Fatalf("the shared projection kept another attempt's resource %q", name)
		}
	}
	want := []string{identity.LeaseID, identity.SnapshotID, identity.ContainerID, identity.ContainerID,
		identity.ContainerID, "/sys/fs/cgroup/wefty/" + identity.CgroupID + ".scope", identity.LogSegmentDirectory,
		identity.ServiceVolumeDirectory, identity.ServiceVolumeOwnerRecord, diskName, diskName}
	slices.Sort(kept)
	slices.Sort(want)
	if !slices.Equal(kept, want) {
		t.Fatalf("shared projection kept %v, want %v", kept, want)
	}

	// And the absence proof reads that projection, not a private copy: with
	// only the foreign attempt left the proof passes, and it fails the moment
	// this attempt's own container is back.
	volumes := []ocihelper.ManagedVolumeDescriptor{
		{Kind: ocihelper.ManagedVolumeServiceData},
		{Kind: ocihelper.ManagedVolumeComputerDisk, ComputerStorage: &storage},
	}
	foreignOnly := emptyAdapterInventory()
	foreignOnly.Containers = []string{foreign}
	foreignOnly.Tasks = []string{foreign}
	if err := attemptAbsentFromNamespace(ocihelper.VerifyResponse{
		Inventory: foreignOnly, RuntimeResidue: emptyAdapterInventory(),
	}, authority, volumes, time.Now()); err != nil {
		t.Fatalf("another attempt's resources failed this attempt's absence proof: %v", err)
	}
	withOwn := foreignOnly
	withOwn.Containers = []string{foreign, identity.ContainerID}
	if err := attemptAbsentFromNamespace(ocihelper.VerifyResponse{
		Inventory: withOwn, RuntimeResidue: emptyAdapterInventory(),
	}, authority, volumes, time.Now()); err == nil {
		t.Fatal("this attempt's own surviving container passed the absence proof")
	}

	// A Computer disk is durable Storage the removal deletes in its own
	// attested step, so it may remain in the inventory -- but never as runtime
	// residue.
	withDisk := emptyAdapterInventory()
	withDisk.ComputerDiskImages = []string{diskName}
	if err := attemptAbsentFromNamespace(ocihelper.VerifyResponse{
		Inventory: withDisk, RuntimeResidue: emptyAdapterInventory(),
	}, authority, volumes, time.Now()); err != nil {
		t.Fatalf("a durable Computer disk was read as attempt residue: %v", err)
	}
	if err := attemptAbsentFromNamespace(ocihelper.VerifyResponse{
		Inventory: emptyAdapterInventory(), RuntimeResidue: withDisk,
	}, authority, volumes, time.Now()); err == nil {
		t.Fatal("a Computer disk reported as runtime residue passed the absence proof")
	}
}

// namespaceVerification shapes one read-only observation the way the helper's
// own contract requires: the observed inventory is exactly the runtime residue
// plus durable retentions, and Absent agrees with an empty residue. Nothing
// here is durably retained, so the whole observation is live runtime residue.
func namespaceVerification(inventory ocihelper.ResourceInventory) ocihelper.VerifyResponse {
	return ocihelper.VerifyResponse{
		Absent: ocihelper.InventoryEmpty(inventory), Inventory: inventory, RuntimeResidue: inventory,
		DurableRetained: emptyAdapterInventory(),
	}
}

// trackNonLiveAttempt restores the run entry the adapter keeps while the helper
// itself, not the agent, ended the attempt: the managed volumes the attempt's
// own Run carried, and the sweep baseline it entered Run against.
func trackNonLiveAttempt(t *testing.T, adapter *Adapter, request workloadrunner.Request) {
	t.Helper()
	_, receipt, err := adapter.sessions.ExecutionSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	adapter.trackRun(request.Authority, runEntry{
		entered: true, volumes: helperManagedVolumes(request),
		sweep: sweepBaseline{epoch: receipt.SweepEpoch, helper: receipt.HelperSession},
	})
}
