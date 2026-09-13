package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
)

var errInjectedRuntimeRemovalCrash = errors.New("injected runtime removal crash")

func TestRuntimeRemovalManifestFreezesCurrentAttemptAndServiceDataIdentity(t *testing.T) {
	spool := openTestLogSpool(t, t.TempDir(), "runtime-manifest-node", 1024)
	defer spool.Close()
	createdAt := time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)
	for index, attemptID := range []string{"attempt-a", "attempt-b"} {
		if err := spool.storeRuntimeResourceManifest(t.Context(), testRuntimeResourceManifest("runtime-job", attemptID), createdAt.Add(time.Duration(index)*time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	removal := testRuntimeRemoval("runtime-job")
	if err := spool.beginRemoval(t.Context(), removal, createdAt.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	record, found, err := spool.runtimeRemoval(t.Context(), removal.jobID)
	if err != nil || !found {
		t.Fatalf("frozen removal = %+v found=%t err=%v", record, found, err)
	}
	if record.phase != runtimeRemovalPrepared || record.receipt.RuntimeQuiesced || len(record.manifest.Attempts) != 1 {
		t.Fatalf("prepared runtime removal = %+v", record)
	}
	if got := record.manifest.Attempts[0].AttemptID; got != "attempt-b" {
		t.Fatalf("frozen current attempt = %q, want attempt-b", got)
	}
	for _, attempt := range record.manifest.Attempts {
		if attempt.ServiceDataVolume == "" || attempt.ServiceDataOwnerRecord == "" {
			t.Fatalf("manifest omitted service data identity: %+v", attempt)
		}
	}

	if err := spool.storeRuntimeResourceManifest(t.Context(), testRuntimeResourceManifest("runtime-job", "attempt-c"), createdAt.Add(2*time.Minute)); err == nil {
		t.Fatal("removal intent accepted a new runtime attempt manifest")
	}
	if err := spool.beginRemoval(t.Context(), removal, createdAt.Add(3*time.Minute)); err != nil {
		t.Fatalf("idempotent removal intent = %v", err)
	}
}

func TestRuntimeRemovalManifestAcceptsComputerStorageInsteadOfPhantomServiceData(t *testing.T) {
	manifest := testRuntimeResourceManifest("computer-job", "attempt-1")
	manifest.ServiceDataVolume = ""
	manifest.ServiceDataOwnerRecord = ""
	manifest.ComputerStorage = &workloadrunner.ComputerStorage{
		ComputerID: "computer-1", StorageID: "storage-1", StorageGeneration: 2, DiskBytes: 8 << 30,
	}
	if err := validateRuntimeResourceManifest(manifest); err != nil {
		t.Fatalf("Computer runtime manifest rejected: %v", err)
	}
	resources := manifest.RemovalResources()
	computerClasses := map[workloadrunner.RuntimeRemovalResourceClass]bool{
		workloadrunner.RuntimeRemovalComputerDiskImage: false, workloadrunner.RuntimeRemovalComputerDiskAllocation: false,
		workloadrunner.RuntimeRemovalComputerDiskQuota: false, workloadrunner.RuntimeRemovalComputerDiskManifest: false,
		workloadrunner.RuntimeRemovalComputerDiskMount: false, workloadrunner.RuntimeRemovalComputerDiskLoop: false,
		workloadrunner.RuntimeRemovalComputerAttachment: false,
	}
	for _, resource := range resources {
		if resource.Class == workloadrunner.RuntimeRemovalServiceData || resource.Class == workloadrunner.RuntimeRemovalServiceDataRecord {
			t.Fatalf("Computer manifest projected phantom service data: %+v", resources)
		}
		if _, ok := computerClasses[resource.Class]; ok {
			if resource.ID == "" {
				t.Fatalf("Computer manifest projected an empty resource identity: %+v", resource)
			}
			computerClasses[resource.Class] = true
		}
	}
	for class, present := range computerClasses {
		if !present {
			t.Fatalf("Computer manifest omitted removal class %q: %+v", class, resources)
		}
	}
	manifest.ServiceDataVolume = "phantom"
	manifest.ServiceDataOwnerRecord = "phantom.owner"
	if err := validateRuntimeResourceManifest(manifest); err == nil {
		t.Fatal("Computer runtime manifest accepted phantom service-data classes")
	}
}

func TestReconstructedStorageOnlyManifestRefreshesAfterHelperBoot(t *testing.T) {
	spool := openTestLogSpool(t, t.TempDir(), "refresh-node", 1024)
	defer spool.Close()
	removal := testRuntimeRemoval("prepared-computer")
	startedAt := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
	if err := spool.beginRemoval(t.Context(), removal, startedAt); err != nil {
		t.Fatal(err)
	}
	storage := &workloadrunner.ComputerStorage{
		ComputerID: "computer", StorageID: "storage", StorageGeneration: 1, DiskBytes: 8 << 30,
	}
	witness := &contract.ComputerStoragePreparationWitness{
		Kind: contract.ComputerStorageCopyVerifiedKind, ReceiptID: "copy-receipt",
		NodeID: "refresh-node", RootInstanceID: removal.rootInstanceID, JobID: removal.jobID,
		ComputerID: storage.ComputerID, StorageID: storage.StorageID,
		StorageGeneration: storage.StorageGeneration, Revision: 1,
		Fence: "copy-fence", HelperGeneration: 1,
	}
	attempt := workloadrunner.RuntimeResourceManifest{
		Version: 1, RuntimeKind: contract.JobKindOCI, NodeID: "refresh-node",
		BootSessionID: "boot-old", JobID: removal.jobID,
		AttemptID:    contract.StorageOnlyRemovalAttemptID(storage.StorageGeneration),
		FencingToken: removal.cleanupFence, WorkloadClass: contract.JobClassService,
		RemovalGeneration: fmt.Sprint(removal.generation), StorageOnly: true,
		ComputerStorage: storage, StoragePreparation: witness,
	}
	if err := spool.storeReconstructedRuntimeRemoval(t.Context(), removal, []workloadrunner.RuntimeResourceManifest{attempt}, startedAt); err != nil {
		t.Fatal(err)
	}
	attempt.BootSessionID = "boot-current"
	if err := spool.storeReconstructedRuntimeRemoval(t.Context(), removal, []workloadrunner.RuntimeResourceManifest{attempt}, startedAt.Add(time.Minute)); err != nil {
		t.Fatalf("refresh Storage-only inventory after helper boot: %v", err)
	}
	record, found, err := spool.runtimeRemoval(t.Context(), removal.jobID)
	if err != nil || !found || record.phase != runtimeRemovalPrepared || len(record.manifest.Attempts) != 1 ||
		record.manifest.Attempts[0].BootSessionID != "boot-current" {
		t.Fatalf("refreshed Storage-only removal = %+v found=%t err=%v", record, found, err)
	}
}

func TestRuntimeRemovalManifestResumesCrashBoundaries(t *testing.T) {
	directory := t.TempDir()
	createdAt := time.Date(2026, 8, 27, 13, 0, 0, 0, time.UTC)
	removal := testRuntimeRemoval("crash-job")
	receipt := workloadrunner.ReapReceipt{
		RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidencePriorBootOCISweep,
		BootSessionID: "prior-boot", SweepEpoch: "sweep-2", HelperGeneration: 2,
	}

	spool := openTestLogSpool(t, directory, "crash-node", 1024)
	if err := spool.storeRuntimeResourceManifest(t.Context(), testRuntimeResourceManifest(removal.jobID, "attempt-1"), createdAt); err != nil {
		t.Fatal(err)
	}
	spool.runtimeRemovalCheckpoint = func(checkpoint runtimeRemovalCheckpoint) error {
		if checkpoint == runtimeRemovalCheckpointAfterManifest {
			return errInjectedRuntimeRemovalCrash
		}
		return nil
	}
	if err := spool.beginRemoval(t.Context(), removal, createdAt.Add(time.Minute)); !errors.Is(err, errInjectedRuntimeRemovalCrash) {
		t.Fatalf("manifest crash = %v", err)
	}
	if err := spool.Close(); err != nil {
		t.Fatal(err)
	}

	spool = openTestLogSpool(t, directory, "crash-node", 1024)
	record, found, err := spool.runtimeRemoval(t.Context(), removal.jobID)
	if err != nil || !found || record.phase != runtimeRemovalPrepared {
		t.Fatalf("post-manifest restart = %+v found=%t err=%v", record, found, err)
	}
	spool.runtimeRemovalCheckpoint = func(checkpoint runtimeRemovalCheckpoint) error {
		if checkpoint == runtimeRemovalCheckpointAfterQuiescence {
			return errInjectedRuntimeRemovalCrash
		}
		return nil
	}
	if err := spool.recordRuntimeQuiesced(t.Context(), removal, receipt, createdAt.Add(2*time.Minute)); !errors.Is(err, errInjectedRuntimeRemovalCrash) {
		t.Fatalf("quiescence crash = %v", err)
	}
	if err := spool.Close(); err != nil {
		t.Fatal(err)
	}

	spool = openTestLogSpool(t, directory, "crash-node", 1024)
	record, found, err = spool.runtimeRemoval(t.Context(), removal.jobID)
	if err != nil || !found || record.phase != runtimeRemovalQuarantined || record.receipt != receipt {
		t.Fatalf("post-quiescence restart = %+v found=%t err=%v", record, found, err)
	}
	spool.runtimeRemovalCheckpoint = func(checkpoint runtimeRemovalCheckpoint) error {
		if checkpoint == runtimeRemovalCheckpointAfterComplete {
			return errInjectedRuntimeRemovalCrash
		}
		return nil
	}
	if err := spool.recordRuntimeAttested(t.Context(), removal, testRuntimeRemovalAttestation(record.manifest), createdAt.Add(3*time.Minute)); !errors.Is(err, errInjectedRuntimeRemovalCrash) {
		t.Fatalf("completion crash = %v", err)
	}
	if err := spool.Close(); err != nil {
		t.Fatal(err)
	}

	spool = openTestLogSpool(t, directory, "crash-node", 1024)
	defer spool.Close()
	record, found, err = spool.runtimeRemoval(t.Context(), removal.jobID)
	if err != nil || !found || record.phase != runtimeRemovalComplete || record.completedAt == nil {
		t.Fatalf("post-completion restart = %+v found=%t err=%v", record, found, err)
	}
}

func TestRuntimeRemovalManifestRejectsConflictingIdentityAndReceipt(t *testing.T) {
	spool := openTestLogSpool(t, t.TempDir(), "conflict-node", 1024)
	defer spool.Close()
	manifest := testRuntimeResourceManifest("conflict-job", "attempt-1")
	if err := spool.storeRuntimeResourceManifest(t.Context(), manifest, time.Now()); err != nil {
		t.Fatal(err)
	}
	changed := manifest
	changed.FencingToken = "other-fence"
	if err := spool.storeRuntimeResourceManifest(t.Context(), changed, time.Now()); err == nil {
		t.Fatal("attempt manifest accepted conflicting fenced identity")
	}
	removal := testRuntimeRemoval(manifest.JobID)
	if err := spool.beginRemoval(t.Context(), removal, time.Now()); err != nil {
		t.Fatal(err)
	}
	receipt := workloadrunner.ReapReceipt{RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidenceAttempt, BootSessionID: manifest.BootSessionID}
	spool.runtimeRemovalCheckpoint = func(checkpoint runtimeRemovalCheckpoint) error {
		if checkpoint == runtimeRemovalCheckpointAfterQuiescence {
			return errInjectedRuntimeRemovalCrash
		}
		return nil
	}
	if err := spool.recordRuntimeQuiesced(t.Context(), removal, receipt, time.Now()); !errors.Is(err, errInjectedRuntimeRemovalCrash) {
		t.Fatal(err)
	}
	spool.runtimeRemovalCheckpoint = nil
	conflict := receipt
	conflict.Evidence = workloadrunner.ReapEvidenceOCISweep
	if err := spool.recordRuntimeQuiesced(t.Context(), removal, conflict, time.Now()); err == nil {
		t.Fatal("quarantined removal accepted conflicting runtime evidence")
	}
}

func TestRuntimeRemovalReceiptValidationIsClosedOnWriteAndDecode(t *testing.T) {
	validSweep := workloadrunner.ReapReceipt{
		RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidenceOCISweep,
		BootSessionID: "boot", SweepEpoch: "sweep", HelperGeneration: 1,
	}
	tests := []struct {
		name    string
		receipt workloadrunner.ReapReceipt
	}{
		{name: "unknown kind", receipt: workloadrunner.ReapReceipt{RuntimeQuiesced: true, Evidence: "invented", BootSessionID: "boot"}},
		{name: "missing boot", receipt: workloadrunner.ReapReceipt{RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidenceAttempt}},
		{name: "sweep missing epoch", receipt: workloadrunner.ReapReceipt{RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidenceOCISweep, BootSessionID: "boot", HelperGeneration: 1}},
		{name: "sweep missing helper generation", receipt: workloadrunner.ReapReceipt{RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidenceOCISweep, BootSessionID: "boot", SweepEpoch: "sweep"}},
		{name: "attempt with sweep authority", receipt: workloadrunner.ReapReceipt{RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidenceAttempt, BootSessionID: "boot", SweepEpoch: "sweep", HelperGeneration: 1}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			spool := openTestLogSpool(t, t.TempDir(), "receipt-node", 1024)
			defer spool.Close()
			removal := testRuntimeRemoval("receipt-job")
			if err := spool.storeRuntimeResourceManifest(t.Context(), testRuntimeResourceManifest(removal.jobID, "attempt"), time.Now()); err != nil {
				t.Fatal(err)
			}
			if err := spool.beginRemoval(t.Context(), removal, time.Now()); err != nil {
				t.Fatal(err)
			}
			if err := spool.recordRuntimeQuiesced(t.Context(), removal, test.receipt, time.Now()); err == nil {
				t.Fatal("invalid runtime receipt was accepted on write")
			}

			payload, err := json.Marshal(test.receipt)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := spool.db.Exec(`UPDATE runtime_removal_manifests
SET runtime_quiescence_json=?, phase=?, quiesced_ns=? WHERE job_id=?`,
				payload, runtimeRemovalQuarantined, time.Now().UnixNano(), removal.jobID); err != nil {
				t.Fatal(err)
			}
			if _, found, err := spool.runtimeRemoval(t.Context(), removal.jobID); err == nil || found {
				t.Fatalf("invalid persisted runtime receipt decoded: found=%t err=%v", found, err)
			}
		})
	}
	if err := validateRuntimeReapReceipt(validSweep); err != nil {
		t.Fatalf("valid sweep receipt rejected: %v", err)
	}
}

func TestRuntimeRemovalFreezeRejectsCorruptAndGenerationMismatchedAttempts(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, *logSpool, workloadrunner.RuntimeResourceManifest)
	}{
		{name: "corrupt row", mutate: func(t *testing.T, spool *logSpool, manifest workloadrunner.RuntimeResourceManifest) {
			if _, err := spool.db.Exec(`UPDATE runtime_service_manifests SET manifest_json=? WHERE attempt_id=?`, []byte("{"), manifest.AttemptID); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "generation mismatch", mutate: func(t *testing.T, spool *logSpool, manifest workloadrunner.RuntimeResourceManifest) {
			manifest.RemovalGeneration = "2"
			payload, err := json.Marshal(manifest)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := spool.db.Exec(`UPDATE runtime_service_manifests SET manifest_json=? WHERE attempt_id=?`, payload, manifest.AttemptID); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			spool := openTestLogSpool(t, t.TempDir(), "freeze-node", 1024)
			defer spool.Close()
			removal := testRuntimeRemoval("freeze-job")
			manifest := testRuntimeResourceManifest(removal.jobID, "attempt")
			if err := spool.storeRuntimeResourceManifest(t.Context(), manifest, time.Now()); err != nil {
				t.Fatal(err)
			}
			test.mutate(t, spool, manifest)
			if err := spool.beginRemoval(t.Context(), removal, time.Now()); err == nil {
				t.Fatal("invalid runtime attempt row froze removal")
			}
			var count int
			if err := spool.db.QueryRow(`SELECT COUNT(*) FROM spool_removals WHERE job_id=?`, removal.jobID).Scan(&count); err != nil {
				t.Fatal(err)
			}
			if count != 0 {
				t.Fatalf("failed freeze retained %d removal intent rows", count)
			}
		})
	}
}

func TestRuntimeRemovalAttestationRejectsUnexecutedAndFailedRows(t *testing.T) {
	manifest := runtimeRemovalManifest{
		Version: 1, JobID: "attestation-job", RemovalGeneration: 1,
		Attempts: []workloadrunner.RuntimeResourceManifest{testRuntimeResourceManifest("attestation-job", "attempt")},
	}
	valid := testRuntimeRemovalAttestation(manifest)
	if err := validateRuntimeRemovalAttestation(manifest, valid); err != nil {
		t.Fatalf("valid attestation rejected: %v", err)
	}
	omitted := valid
	omitted.Assertions = omitted.Assertions[:len(omitted.Assertions)-1]
	if err := validateRuntimeRemovalAttestation(manifest, omitted); err == nil {
		t.Fatal("attestation omitted an unexecuted manifest row")
	}
	failed := valid
	failed.Assertions = append([]workloadrunner.RuntimeRemovalAssertion(nil), valid.Assertions...)
	failed.Assertions[0].Absent = false
	if err := validateRuntimeRemovalAttestation(manifest, failed); err == nil {
		t.Fatal("attestation recorded a failed row as proof")
	}
}

func testRuntimeRemoval(jobID string) localRemoval {
	return localRemoval{jobID: jobID, kind: contract.JobKindOCI, generation: l1.InitialServiceRemovalGeneration, cleanupFence: "cleanup-fence", rootInstanceID: "root-instance"}
}

func testRuntimeRemovalAttestation(manifest runtimeRemovalManifest) workloadrunner.RuntimeRemovalAttestation {
	seen := make(map[workloadrunner.RuntimeRemovalResource]struct{})
	var assertions []workloadrunner.RuntimeRemovalAssertion
	for _, attempt := range manifest.Attempts {
		for _, resource := range attempt.RemovalResources() {
			if _, exists := seen[resource]; exists {
				continue
			}
			seen[resource] = struct{}{}
			assertions = append(assertions, workloadrunner.RuntimeRemovalAssertion{Class: resource.Class, ID: resource.ID, Absent: true})
		}
	}
	return workloadrunner.RuntimeRemovalAttestation{
		Version: 1, JobID: manifest.JobID, RemovalGeneration: manifest.RemovalGeneration,
		RuntimeInstanceID: "helper", RuntimeGeneration: 1, Attempts: manifest.Attempts, Assertions: assertions,
	}
}

func testRuntimeResourceManifest(jobID, attemptID string) workloadrunner.RuntimeResourceManifest {
	return workloadrunner.RuntimeResourceManifest{
		Version: 1, RuntimeKind: contract.JobKindOCI,
		NodeID: "node", BootSessionID: "boot", JobID: jobID, AttemptID: attemptID,
		FencingToken: "fence-" + attemptID, WorkloadClass: contract.JobClassService,
		RemovalGeneration: fmt.Sprint(l1.InitialServiceRemovalGeneration), LeaseID: "lease-" + attemptID, TaskID: "task-" + attemptID,
		ContainerID: "container-" + attemptID, SnapshotID: "snapshot-" + attemptID,
		ShimID: "shim-" + attemptID, CgroupID: "cgroup-" + attemptID,
		LogSegmentDirectory: "logs-" + attemptID, ServiceDataVolume: "service-volume-" + jobID,
		ServiceDataOwnerRecord: "service-volume-" + jobID + ".owner",
	}
}

func TestAgentRuntimeRemovalsProjectsDurableProofWithoutAdvancingIt(t *testing.T) {
	outbox, err := newEvidenceOutbox(t.TempDir(), "removal-read-node", 1024, systemClock{}, 1, time.Second, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer outbox.Close()
	nodeAgent := &Agent{outbox: outbox}
	empty, err := nodeAgent.RuntimeRemovals(t.Context())
	if err != nil || len(empty) != 0 {
		t.Fatalf("empty removal read = %+v err=%v", empty, err)
	}

	createdAt := time.Date(2026, 9, 12, 9, 0, 0, 0, time.UTC)
	if err := outbox.spool.storeRuntimeResourceManifest(t.Context(), testRuntimeResourceManifest("read-job", "attempt-a"), createdAt); err != nil {
		t.Fatal(err)
	}
	removal := testRuntimeRemoval("read-job")
	if err := outbox.spool.beginRemoval(t.Context(), removal, createdAt.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	prepared, err := nodeAgent.RuntimeRemovals(t.Context())
	if err != nil || len(prepared) != 1 {
		t.Fatalf("prepared removal read = %+v err=%v", prepared, err)
	}
	if prepared[0].Phase != string(runtimeRemovalPrepared) || prepared[0].JobID != "read-job" ||
		prepared[0].RemovalGeneration != removal.generation || prepared[0].CleanupFence != removal.cleanupFence ||
		prepared[0].QuiescedAt != nil || prepared[0].Attestation != nil || len(prepared[0].ResourceManifests) != 1 ||
		prepared[0].ResourceManifests[0].ServiceDataOwnerRecord != "service-volume-read-job.owner" {
		t.Fatalf("prepared removal projection = %+v", prepared[0])
	}

	receipt := workloadrunner.ReapReceipt{RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidencePriorBootOCISweep, BootSessionID: "prior-boot", SweepEpoch: "epoch-1", HelperGeneration: 7}
	if err := outbox.spool.recordRuntimeQuiesced(t.Context(), removal, receipt, createdAt.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	record, found, err := outbox.spool.runtimeRemoval(t.Context(), removal.jobID)
	if err != nil || !found {
		t.Fatalf("stored removal = %+v found=%t err=%v", record, found, err)
	}
	if err := outbox.spool.recordRuntimeAttested(t.Context(), removal, testRuntimeRemovalAttestation(record.manifest), createdAt.Add(3*time.Minute)); err != nil {
		t.Fatal(err)
	}
	complete, err := nodeAgent.RuntimeRemovals(t.Context())
	if err != nil || len(complete) != 1 {
		t.Fatalf("complete removal read = %+v err=%v", complete, err)
	}
	if complete[0].Phase != string(runtimeRemovalComplete) || complete[0].Quiescence != receipt ||
		complete[0].QuiescedAt == nil || complete[0].AttestedAt == nil || complete[0].CompletedAt == nil ||
		complete[0].Attestation == nil || len(complete[0].Attestation.Assertions) == 0 {
		t.Fatalf("complete removal projection = %+v", complete[0])
	}
	for _, assertion := range complete[0].Attestation.Assertions {
		if !assertion.Absent {
			t.Fatalf("projected attestation dropped a negative assertion: %+v", assertion)
		}
	}

	// The read is a projection: the phase it reported is still the phase the
	// durable record holds, and the record is still there to be acted on.
	after, found, err := outbox.spool.runtimeRemoval(t.Context(), removal.jobID)
	if err != nil || !found || after.phase != runtimeRemovalComplete {
		t.Fatalf("removal record after read = %+v found=%t err=%v", after, found, err)
	}

	// Cleanup acknowledgement releases the record, and the read surface stops
	// claiming a removal the agent no longer carries.
	if err := outbox.spool.completeRemoval(t.Context(), removal); err != nil {
		t.Fatal(err)
	}
	released, err := nodeAgent.RuntimeRemovals(t.Context())
	if err != nil || len(released) != 0 {
		t.Fatalf("released removal read = %+v err=%v", released, err)
	}
}

// testComputerAttemptManifest is the shape #196 run 1 froze for a Computer:
// an ordinary runtime attempt manifest whose durable service-data class is a
// Computer disk, not a Storage-only removal inventory.
func testComputerAttemptManifest(jobID, attemptID string) workloadrunner.RuntimeResourceManifest {
	manifest := testRuntimeResourceManifest(jobID, attemptID)
	manifest.ServiceDataVolume = ""
	manifest.ServiceDataOwnerRecord = ""
	manifest.ComputerStorage = &workloadrunner.ComputerStorage{
		ComputerID: "computer-" + jobID, StorageID: "storage-" + jobID, StorageGeneration: 1, DiskBytes: 160 << 20,
	}
	return manifest
}

// A Computer whose helper `Run` never entered is reaped with positive
// `no_runtime_resources` evidence against that ordinary attempt manifest. The
// record used to demand a Storage-only manifest for that evidence kind, so the
// receipt persisted and then made its own row unreadable: the removal loop
// re-read "runtime removal record is invalid" every heartbeat forever and the
// Computer pinned its node service slot (#443).
func TestComputerRemovalRecordCompletesOnNeverEnteredRunNoRuntimeReceipt(t *testing.T) {
	spool := openTestLogSpool(t, t.TempDir(), "never-entered-node", 1024)
	defer spool.Close()
	createdAt := time.Date(2026, 9, 13, 2, 0, 0, 0, time.UTC)
	if err := spool.storeRuntimeResourceManifest(t.Context(), testComputerAttemptManifest("computer-job", "attempt-a"), createdAt); err != nil {
		t.Fatal(err)
	}
	removal := testRuntimeRemoval("computer-job")
	if err := spool.beginRemoval(t.Context(), removal, createdAt.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	receipt := workloadrunner.ReapReceipt{RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidenceNoRuntime, BootSessionID: "boot"}
	if err := spool.recordRuntimeQuiesced(t.Context(), removal, receipt, createdAt.Add(2*time.Minute)); err != nil {
		t.Fatalf("never-entered-Run quiescence receipt rejected: %v", err)
	}
	record, found, err := spool.runtimeRemoval(t.Context(), removal.jobID)
	if err != nil || !found || record.phase != runtimeRemovalQuarantined {
		t.Fatalf("quiesced Computer removal = %+v found=%t err=%v", record, found, err)
	}
	if err := spool.recordRuntimeAttested(t.Context(), removal, testRuntimeRemovalAttestation(record.manifest), createdAt.Add(3*time.Minute)); err != nil {
		t.Fatalf("never-entered-Run removal could not attest: %v", err)
	}
	completed, found, err := spool.runtimeRemoval(t.Context(), removal.jobID)
	if err != nil || !found || completed.phase != runtimeRemovalComplete {
		t.Fatalf("completed Computer removal = %+v found=%t err=%v", completed, found, err)
	}
}

// The boot session is what a no-runtime receipt may still be held to: it can
// only speak for attempts frozen under the same boot. A record that breaks
// that says which field disagreed.
func TestRuntimeRemovalRecordRejectsForeignBootNoRuntimeReceiptByField(t *testing.T) {
	removal := testRuntimeRemoval("computer-job")
	manifest := runtimeRemovalManifest{Version: 1, JobID: removal.jobID, RemovalGeneration: removal.generation,
		Attempts: []workloadrunner.RuntimeResourceManifest{testComputerAttemptManifest(removal.jobID, "attempt-a")}}
	quiescedAt := time.Date(2026, 9, 13, 2, 2, 0, 0, time.UTC)
	record := runtimeRemovalRecord{removal: removal, manifest: manifest, phase: runtimeRemovalQuarantined,
		receipt:    workloadrunner.ReapReceipt{RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidenceNoRuntime, BootSessionID: "another-boot"},
		quiescedAt: &quiescedAt}
	err := validateRuntimeRemovalRecord(record)
	if err == nil {
		t.Fatal("no-runtime receipt from another boot was accepted")
	}
	for _, want := range []string{"boot_session_id", "attempt-a"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("rejection %q does not name %q", err, want)
		}
	}
	record.receipt.BootSessionID = "boot"
	if err := validateRuntimeRemovalRecord(record); err != nil {
		t.Fatalf("boot-bound no-runtime receipt rejected: %v", err)
	}
}

// A durable row this agent cannot validate is the row an operator most needs
// to read. It used to fail the whole listing, so `node oci removals` went red
// exactly when a removal was stuck, taking every well-formed record with it.
func TestRuntimeRemovalReadListsUnvalidatableRecordWithoutFailingTheVerb(t *testing.T) {
	outbox, err := newEvidenceOutbox(t.TempDir(), "removal-read-node", 1024, systemClock{}, 1, time.Second, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer outbox.Close()
	nodeAgent := &Agent{outbox: outbox}
	createdAt := time.Date(2026, 9, 13, 2, 0, 0, 0, time.UTC)
	// Service-data manifests on purpose: a Computer inventory is skipped by
	// resume for its own reason, which would hide whether the unvalidatable
	// row was skipped because it is unvalidatable.
	for index, jobID := range []string{"broken-job", "healthy-job"} {
		if err := outbox.spool.storeRuntimeResourceManifest(t.Context(), testRuntimeResourceManifest(jobID, "attempt-"+jobID), createdAt.Add(time.Duration(index)*time.Second)); err != nil {
			t.Fatal(err)
		}
		if err := outbox.spool.beginRemoval(t.Context(), testRuntimeRemoval(jobID), createdAt.Add(time.Minute+time.Duration(index)*time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	if err := outbox.spool.recordRuntimeQuiesced(t.Context(), testRuntimeRemoval("healthy-job"),
		workloadrunner.ReapReceipt{RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidenceAttempt, BootSessionID: "boot"},
		createdAt.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	// Durable rows outlive the code that wrote them, so the read path has to
	// survive a receipt this build would never persist.
	foreign, err := json.Marshal(workloadrunner.ReapReceipt{RuntimeQuiesced: true,
		Evidence: workloadrunner.ReapEvidenceNoRuntime, BootSessionID: "another-boot"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := outbox.spool.db.ExecContext(t.Context(), `UPDATE runtime_removal_manifests
SET runtime_quiescence_json=?, phase=?, quiesced_ns=? WHERE job_id=?`, foreign, runtimeRemovalQuarantined,
		createdAt.Add(2*time.Minute).UnixNano(), "broken-job"); err != nil {
		t.Fatal(err)
	}

	views, err := nodeAgent.RuntimeRemovals(t.Context())
	if err != nil || len(views) != 2 {
		t.Fatalf("removal read with one unvalidatable row = %+v err=%v", views, err)
	}
	byJob := make(map[string]RuntimeRemovalView, len(views))
	for _, view := range views {
		byJob[view.JobID] = view
	}
	if healthy := byJob["healthy-job"]; healthy.InvalidReason != "" || healthy.Phase != string(runtimeRemovalQuarantined) {
		t.Fatalf("well-formed record was reported red: %+v", healthy)
	}
	broken := byJob["broken-job"]
	if broken.InvalidReason == "" || !strings.Contains(broken.InvalidReason, "boot_session_id") {
		t.Fatalf("unvalidatable record did not name its field: %+v", broken)
	}
	if broken.Phase != string(runtimeRemovalQuarantined) || broken.RemovalGeneration == 0 {
		t.Fatalf("unvalidatable record was not rendered as persisted: %+v", broken)
	}

	// Resume walks the same listing, finishes the healthy removal, and acts on
	// the unvalidatable one not at all: it can never be driven round a retry
	// loop that pins the slot behind it.
	resumed := make([]string, 0, 2)
	controller := &removalController{nodeID: "removal-read-node", bootSessionID: "boot",
		listRuntimeRemovals: outbox.spool.pendingRuntimeRemovals}
	controller.purgeJob = func(_ context.Context, jobID string) error { resumed = append(resumed, jobID); return nil }
	controller.removeResource = func(context.Context, localRemoval) error { return nil }
	controller.deleteRuntimeData = func(context.Context, workloadrunner.RuntimeRemovalProofRequest) error { return nil }
	controller.attestRuntimeRemoval = func(_ context.Context, request workloadrunner.RuntimeRemovalProofRequest) (workloadrunner.RuntimeRemovalAttestation, error) {
		return testRuntimeRemovalAttestation(runtimeRemovalManifest{Version: 1, JobID: request.JobID,
			RemovalGeneration: request.RemovalGeneration, Attempts: request.Attempts}), nil
	}
	controller.recordRuntimeAttested = func(context.Context, localRemoval, workloadrunner.RuntimeRemovalAttestation) error { return nil }
	controller.ackRemoval = func(context.Context, localRemoval) error { return nil }
	controller.finishRemoval = func(context.Context, localRemoval) error { return nil }
	if err := controller.resume(t.Context()); err != nil {
		t.Fatalf("resume over an unvalidatable row = %v", err)
	}
	if len(resumed) != 1 || resumed[0] != "healthy-job" {
		t.Fatalf("resume acted on %v, want the healthy removal alone", resumed)
	}
}

// A durable row whose stored JSON no longer parses has no manifest to name
// itself with, so job_id is read from its own column. Otherwise the one row an
// operator has to act on is the one row the verb cannot show them (#443).
func TestRuntimeRemovalReadNamesARowWhoseStoredJSONNoLongerParses(t *testing.T) {
	outbox, err := newEvidenceOutbox(t.TempDir(), "removal-json-node", 1024, systemClock{}, 1, time.Second, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer outbox.Close()
	nodeAgent := &Agent{outbox: outbox}
	createdAt := time.Date(2026, 9, 13, 4, 0, 0, 0, time.UTC)
	if err := outbox.spool.storeRuntimeResourceManifest(t.Context(), testComputerAttemptManifest("torn-job", "attempt-a"), createdAt); err != nil {
		t.Fatal(err)
	}
	if err := outbox.spool.beginRemoval(t.Context(), testRuntimeRemoval("torn-job"), createdAt.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := outbox.spool.db.ExecContext(t.Context(), `UPDATE runtime_removal_manifests
SET manifest_json=? WHERE job_id=?`, []byte(`{"version":1,"attempts":[`), "torn-job"); err != nil {
		t.Fatal(err)
	}
	views, err := nodeAgent.RuntimeRemovals(t.Context())
	if err != nil || len(views) != 1 {
		t.Fatalf("removal read over torn JSON = %+v err=%v", views, err)
	}
	if views[0].JobID != "torn-job" || !strings.Contains(views[0].InvalidReason, "unreadable_json") {
		t.Fatalf("torn row did not name itself and its reason: %+v", views[0])
	}
	// The single-record read stays a refusal: nothing acts on this row.
	if _, found, err := outbox.spool.runtimeRemoval(t.Context(), "torn-job"); err == nil || found {
		t.Fatalf("torn row was returned as actionable: found=%t err=%v", found, err)
	}
}
