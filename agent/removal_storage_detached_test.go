package agent

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
)

func detachedStorageRow(jobID, boot string, storage *workloadrunner.ComputerStorage) workloadrunner.RuntimeResourceManifest {
	return workloadrunner.RuntimeResourceManifest{
		Version: 1, RuntimeKind: contract.JobKindOCI, NodeID: "node", BootSessionID: boot, JobID: jobID,
		AttemptID: contract.StorageDetachedRemovalAttemptID(storage.StorageGeneration), FencingToken: "cleanup",
		WorkloadClass: contract.JobClassService, RemovalGeneration: "3", ComputerStorage: storage, StorageOnly: true,
		StorageDetachment: &contract.ComputerStorageDetachmentWitness{Kind: contract.ComputerStorageDetachedKind,
			EvidenceKind: contract.ComputerStorageDetachSameBootReap, ReceiptID: "detach-receipt", NodeID: "node",
			ComputerID: storage.ComputerID, StorageID: storage.StorageID, StorageGeneration: storage.StorageGeneration,
			AttachedJobID: "job-that-ran", AttachedAttemptID: "attempt-a", AttachedBootSessionID: boot, HelperGeneration: 4},
	}
}

// A stopped Computer whose Job L1 replaced, and which never ran, is removed
// on the helper's detached-Storage evidence (#1611): the controller gets a
// no-runtime receipt for the complete Storage inventory, deletes the detached
// generation with the full helper check (not as already absent), attests,
// and acknowledges.
func TestRotatedNeverRunComputerRemovalCompletesOnDetachedEvidence(t *testing.T) {
	removal := localRemoval{jobID: "rotated-job", kind: contract.JobKindOCI, generation: 3, rootInstanceID: "root", cleanupFence: "cleanup"}
	storages := []*workloadrunner.ComputerStorage{
		{ComputerID: "computer", StorageID: "storage", StorageGeneration: 1, DiskBytes: 8 << 30},
		{ComputerID: "computer", StorageID: "storage", StorageGeneration: 2, DiskBytes: 8 << 30},
	}
	absent := workloadrunner.RuntimeResourceManifest{
		Version: 1, RuntimeKind: contract.JobKindOCI, NodeID: "node", BootSessionID: "boot", JobID: removal.jobID,
		AttemptID: contract.StorageAbsentRemovalAttemptID(1), FencingToken: removal.cleanupFence,
		WorkloadClass: contract.JobClassService, RemovalGeneration: "3", ComputerStorage: storages[0],
		StorageOnly: true, StorageAbsent: true,
	}
	detached := detachedStorageRow(removal.jobID, "boot", storages[1])
	directive := l1.RemovalDirective{
		JobID: removal.jobID, BoundNodeID: "node", Kind: removal.kind, RemovalGeneration: removal.generation,
		CleanupFence: removal.cleanupFence, RootInstanceID: removal.rootInstanceID,
		ComputerStorageGenerations: &l1.ComputerStorageGenerationClaims{Generations: []l1.ComputerStorageGenerationClaim{
			{ComputerID: "computer", StorageID: "storage", StorageGeneration: 1, DiskBytes: 8 << 30},
			{ComputerID: "computer", StorageID: "storage", StorageGeneration: 2, DiskBytes: 8 << 30},
		}},
	}
	var record runtimeRemovalRecord
	found, acknowledged, finalized := false, false, false
	controller := &removalController{nodeID: "node", bootSessionID: "boot"}
	controller.beginRemoval = func(context.Context, localRemoval) error { return nil }
	controller.loadRuntimeRemoval = func(context.Context, string) (runtimeRemovalRecord, bool, error) { return record, found, nil }
	controller.reconstructRuntime = func(_ context.Context, request workloadrunner.RuntimeRemovalProofRequest) ([]workloadrunner.RuntimeResourceManifest, error) {
		if request.ComputerStorage == nil {
			return nil, nil
		}
		switch request.ComputerStorage.StorageGeneration {
		case 1:
			return []workloadrunner.RuntimeResourceManifest{absent}, nil
		case 2:
			return []workloadrunner.RuntimeResourceManifest{detached}, nil
		default:
			return nil, errors.New("unexpected Storage generation")
		}
	}
	controller.persistRuntimeRemoval = func(_ context.Context, got localRemoval, attempts []workloadrunner.RuntimeResourceManifest) error {
		for _, attempt := range attempts {
			if err := validateRuntimeResourceManifest(attempt); err != nil {
				t.Fatalf("persisted row %s is invalid: %v", attempt.AttemptID, err)
			}
		}
		manifest := runtimeRemovalManifest{Version: 1, JobID: got.jobID, RemovalGeneration: got.generation, Attempts: slices.Clone(attempts)}
		slices.SortFunc(manifest.Attempts, func(left, right workloadrunner.RuntimeResourceManifest) int {
			return strings.Compare(left.AttemptID, right.AttemptID)
		})
		record = runtimeRemovalRecord{removal: got, manifest: manifest, phase: runtimeRemovalPrepared}
		found = true
		if err := validateRuntimeRemovalRecord(record); err != nil {
			t.Fatalf("persisted record is invalid: %v", err)
		}
		return nil
	}
	controller.reapService = func(_ context.Context, jobID, _ string, attempts []workloadrunner.RuntimeResourceManifest) (workloadrunner.ReapReceipt, error) {
		receipt, ok := storageOnlyAttemptsReceipt(jobID, "boot", attempts)
		if !ok {
			return workloadrunner.ReapReceipt{}, errors.New("typed Storage inventory did not prove no runtime")
		}
		return receipt, nil
	}
	controller.recordRuntimeQuiesced = func(_ context.Context, _ localRemoval, receipt workloadrunner.ReapReceipt) error {
		record.phase = runtimeRemovalQuarantined
		record.receipt = receipt
		return nil
	}
	controller.purgeJob = func(context.Context, string) error { return nil }
	controller.removeResource = func(context.Context, localRemoval) error {
		t.Fatal("never-run Computer attempted managed service-resource deletion")
		return nil
	}
	controller.finalizeVolumes = func(_ context.Context, request workloadrunner.ManagedVolumeFinalizationRequest) error {
		finalized = true
		if len(request.Volumes) != 2 || !request.Volumes[0].StorageAbsent || request.Volumes[1].StorageAbsent {
			t.Fatalf("the detached generation must be deleted with the full helper check, not as absent: %+v", request.Volumes)
		}
		return nil
	}
	controller.deleteRuntimeData = func(context.Context, workloadrunner.RuntimeRemovalProofRequest) error { return nil }
	controller.attestRuntimeRemoval = func(context.Context, workloadrunner.RuntimeRemovalProofRequest) (workloadrunner.RuntimeRemovalAttestation, error) {
		return testRuntimeRemovalAttestation(record.manifest), nil
	}
	controller.recordRuntimeAttested = func(context.Context, localRemoval, workloadrunner.RuntimeRemovalAttestation) error { return nil }
	controller.ackRemoval = func(context.Context, localRemoval) error { acknowledged = true; return nil }
	controller.finishRemoval = func(context.Context, localRemoval) error { return nil }
	if err := controller.process(t.Context(), directive); err != nil {
		t.Fatal(err)
	}
	if !acknowledged || !finalized {
		t.Fatalf("rotated never-run Computer removal acknowledged=%t finalized=%t", acknowledged, finalized)
	}
}

func TestDetachedStorageManifestArm(t *testing.T) {
	storage := &workloadrunner.ComputerStorage{ComputerID: "computer", StorageID: "storage", StorageGeneration: 2, DiskBytes: 8 << 30}
	if err := validateRuntimeResourceManifest(detachedStorageRow("rotated-job", "boot", storage)); err != nil {
		t.Fatalf("detached row refused: %v", err)
	}
	sweep := detachedStorageRow("rotated-job", "boot", storage)
	sweep.StorageDetachment.EvidenceKind, sweep.StorageDetachment.SweepEpoch, sweep.StorageDetachment.AttachedBootSessionID =
		contract.ComputerStorageDetachPriorBootSweep, "epoch", "old-boot"
	if err := validateRuntimeResourceManifest(sweep); err != nil {
		t.Fatalf("prior-boot sweep row refused: %v", err)
	}
	preparation := &contract.ComputerStoragePreparationWitness{Kind: contract.ComputerStorageResetVerifiedKind, ReceiptID: "r",
		NodeID: "node", RootInstanceID: "root", JobID: "j", ComputerID: "computer", StorageID: "storage",
		StorageGeneration: 2, Revision: 1, Fence: "f", HelperGeneration: 1}
	for name, mutate := range map[string]func(*workloadrunner.RuntimeResourceManifest){
		"no witness":             func(m *workloadrunner.RuntimeResourceManifest) { m.StorageDetachment = nil },
		"extra preparation":      func(m *workloadrunner.RuntimeResourceManifest) { m.StoragePreparation = preparation },
		"foreign Node":           func(m *workloadrunner.RuntimeResourceManifest) { m.StorageDetachment.NodeID = "other" },
		"foreign Computer":       func(m *workloadrunner.RuntimeResourceManifest) { m.StorageDetachment.ComputerID = "other" },
		"foreign Storage":        func(m *workloadrunner.RuntimeResourceManifest) { m.StorageDetachment.StorageID = "other" },
		"foreign generation":     func(m *workloadrunner.RuntimeResourceManifest) { m.StorageDetachment.StorageGeneration = 3 },
		"reap from another boot": func(m *workloadrunner.RuntimeResourceManifest) { m.StorageDetachment.AttachedBootSessionID = "old" },
		"invalid witness":        func(m *workloadrunner.RuntimeResourceManifest) { m.StorageDetachment.HelperGeneration = 0 },
		"prepared ID with witness": func(m *workloadrunner.RuntimeResourceManifest) {
			m.AttemptID = contract.StorageOnlyRemovalAttemptID(2)
			m.StoragePreparation = preparation
		},
		"absent with witness": func(m *workloadrunner.RuntimeResourceManifest) {
			m.AttemptID = contract.StorageAbsentRemovalAttemptID(2)
			m.StorageAbsent = true
		},
		"runtime row with witness": func(m *workloadrunner.RuntimeResourceManifest) {
			m.StorageOnly = false
			m.LeaseID, m.TaskID, m.ContainerID, m.SnapshotID, m.ShimID, m.CgroupID, m.LogSegmentDirectory = "l", "t", "c", "s", "sh", "cg", "logs"
		},
	} {
		row := detachedStorageRow("rotated-job", "boot", storage)
		witness := *row.StorageDetachment
		row.StorageDetachment = &witness
		mutate(&row)
		if err := validateRuntimeResourceManifest(row); err == nil {
			t.Errorf("%s: detached row accepted", name)
		}
	}
	if _, ok := storageOnlyAttemptsReceipt("rotated-job", "boot", []workloadrunner.RuntimeResourceManifest{detachedStorageRow("rotated-job", "boot", storage)}); !ok {
		t.Fatal("a complete detached inventory did not prove no runtime")
	}
	withPreparation := detachedStorageRow("rotated-job", "boot", storage)
	withPreparation.StoragePreparation = preparation
	if _, ok := storageOnlyAttemptsReceipt("rotated-job", "boot", []workloadrunner.RuntimeResourceManifest{withPreparation}); ok {
		t.Fatal("a detached row that also claims preparation proved no runtime")
	}
}

// A detached row may only be restamped: the same last attachment re-proven by
// a newer boot or sweep. A changed last attachment means an attach happened
// after removal began, and a detached row never turns into a prepared one.
func TestStorageOnlyRefreshKeepsTheLastAttachment(t *testing.T) {
	storage := &workloadrunner.ComputerStorage{ComputerID: "computer", StorageID: "storage", StorageGeneration: 2, DiskBytes: 8 << 30}
	manifest := func(rows ...workloadrunner.RuntimeResourceManifest) runtimeRemovalManifest {
		return runtimeRemovalManifest{Version: 1, JobID: "rotated-job", RemovalGeneration: 3, Attempts: rows}
	}
	old := detachedStorageRow("rotated-job", "boot-1", storage)
	restamp := detachedStorageRow("rotated-job", "boot-2", storage)
	restamp.StorageDetachment.AttachedBootSessionID = "boot-1"
	restamp.StorageDetachment.EvidenceKind, restamp.StorageDetachment.SweepEpoch, restamp.StorageDetachment.ReceiptID =
		contract.ComputerStorageDetachPriorBootSweep, "epoch", "new-receipt"
	if !sameStorageOnlyInventory(manifest(old), manifest(restamp)) {
		t.Fatal("a sweep restamp of the same detach was refused")
	}
	newer := detachedStorageRow("rotated-job", "boot-2", storage)
	newer.StorageDetachment.AttachedAttemptID = "attempt-after-removal-began"
	if sameStorageOnlyInventory(manifest(old), manifest(newer)) {
		t.Fatal("a detach of a newer attachment refreshed the frozen inventory")
	}
	absent := old
	absent.AttemptID, absent.StorageAbsent, absent.StorageDetachment = contract.StorageAbsentRemovalAttemptID(2), true, nil
	if !sameStorageOnlyInventory(manifest(old), manifest(absent)) {
		t.Fatal("a detached generation that is now absent was refused")
	}
	prepared := old
	prepared.AttemptID, prepared.StorageDetachment = contract.StorageOnlyRemovalAttemptID(2), nil
	prepared.StoragePreparation = &contract.ComputerStoragePreparationWitness{Kind: contract.ComputerStorageResetVerifiedKind, ReceiptID: "r",
		NodeID: "node", RootInstanceID: "root", JobID: "j", ComputerID: "computer", StorageID: "storage",
		StorageGeneration: 2, Revision: 1, Fence: "f", HelperGeneration: 1}
	if sameStorageOnlyInventory(manifest(old), manifest(prepared)) {
		t.Fatal("a detached generation refreshed as never attached")
	}
	if sameStorageOnlyInventory(manifest(prepared), manifest(old)) {
		t.Fatal("a never-attached generation refreshed as detached")
	}
}

func TestStorageOnlyRecordValidationDetachedArm(t *testing.T) {
	storage := &workloadrunner.ComputerStorage{ComputerID: "computer", StorageID: "storage", StorageGeneration: 2, DiskBytes: 8 << 30}
	removal := localRemoval{jobID: "rotated-job", kind: contract.JobKindOCI, generation: 3, rootInstanceID: "root", cleanupFence: "cleanup"}
	record := func(row workloadrunner.RuntimeResourceManifest) runtimeRemovalRecord {
		return runtimeRemovalRecord{removal: removal, phase: runtimeRemovalPrepared,
			manifest: runtimeRemovalManifest{Version: 1, JobID: removal.jobID, RemovalGeneration: removal.generation,
				Attempts: []workloadrunner.RuntimeResourceManifest{row}}}
	}
	// Detach evidence records no root; the removal record binds it.
	if err := validateRuntimeRemovalRecord(record(detachedStorageRow(removal.jobID, "boot", storage))); err != nil {
		t.Fatalf("detached record refused: %v", err)
	}
	withPreparation := detachedStorageRow(removal.jobID, "boot", storage)
	withPreparation.StoragePreparation = &contract.ComputerStoragePreparationWitness{RootInstanceID: "root"}
	if err := validateRuntimeRemovalRecord(record(withPreparation)); err == nil {
		t.Fatal("a detached record that also claims preparation was accepted")
	}
	absentWithWitness := detachedStorageRow(removal.jobID, "boot", storage)
	absentWithWitness.StorageAbsent = true
	if err := validateRuntimeRemovalRecord(record(absentWithWitness)); err == nil {
		t.Fatal("an absent record with a detachment witness was accepted")
	}
}

// Records the previous agent wrote carry no detachment field; their bytes
// stay unchanged, so the byte-for-byte attestation comparison still holds.
func TestStorageOnlyRowsWithoutDetachmentKeepTheirFormat(t *testing.T) {
	storage := &workloadrunner.ComputerStorage{ComputerID: "computer", StorageID: "storage", StorageGeneration: 2, DiskBytes: 8 << 30}
	absent := workloadrunner.RuntimeResourceManifest{
		Version: 1, RuntimeKind: contract.JobKindOCI, NodeID: "node", BootSessionID: "boot", JobID: "job",
		AttemptID: contract.StorageAbsentRemovalAttemptID(2), FencingToken: "cleanup", WorkloadClass: contract.JobClassService,
		RemovalGeneration: "3", ComputerStorage: storage, StorageOnly: true, StorageAbsent: true,
	}
	payload, err := json.Marshal(absent)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(payload), "storage_detachment") {
		t.Fatalf("a row without detachment evidence gained the field: %s", payload)
	}
	const previousFormat = `{"version":1,"runtime_kind":"oci","node_id":"node","boot_session_id":"boot","job_id":"job","attempt_id":"storage-absent-2","fencing_token":"cleanup","workload_class":"service","removal_generation":"3","lease_id":"","task_id":"","container_id":"","snapshot_id":"","shim_id":"","cgroup_id":"","log_segment_directory":"","computer_storage":{"ComputerID":"computer","StorageID":"storage","StorageGeneration":2,"IntentRevision":0,"DiskBytes":8589934592,"Chown":false},"storage_only":true,"storage_absent":true}`
	var decoded workloadrunner.RuntimeResourceManifest
	if err := json.Unmarshal([]byte(previousFormat), &decoded); err != nil {
		t.Fatal(err)
	}
	if err := validateRuntimeResourceManifest(decoded); err != nil {
		t.Fatalf("a previous-format absent row no longer validates: %v", err)
	}
	again, err := json.Marshal(decoded)
	if err != nil || string(again) != previousFormat {
		t.Fatalf("a previous-format row re-marshals differently:\n%s\n%s", again, previousFormat)
	}
}
