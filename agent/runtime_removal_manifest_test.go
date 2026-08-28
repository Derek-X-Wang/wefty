package agent

import (
	"encoding/json"
	"errors"
	"fmt"
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
	manifest.ServiceDataVolume = "phantom"
	manifest.ServiceDataOwnerRecord = "phantom.owner"
	if err := validateRuntimeResourceManifest(manifest); err == nil {
		t.Fatal("Computer runtime manifest accepted phantom service-data classes")
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
	if err := spool.recordRuntimeQuiesced(t.Context(), removal, receipt, createdAt.Add(3*time.Minute)); !errors.Is(err, errInjectedRuntimeRemovalCrash) {
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

func testRuntimeRemoval(jobID string) localRemoval {
	return localRemoval{jobID: jobID, generation: l1.InitialServiceRemovalGeneration, cleanupFence: "cleanup-fence", rootInstanceID: "root-instance"}
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
