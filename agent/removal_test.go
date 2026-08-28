package agent

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
)

func TestRemovalControllerPersistsReapsDeletesThenAcknowledges(t *testing.T) {
	directive := l1.RemovalDirective{
		JobID: "service-remove-order", BoundNodeID: "node-remove-order",
		Kind:              contract.JobKindProcess,
		RemovalGeneration: 3, CleanupFence: "cleanup-fence", RootInstanceID: "root-instance",
	}
	var stages []string
	controller := &removalController{nodeID: directive.BoundNodeID}
	controller.beginRemoval = func(_ context.Context, removal localRemoval) error {
		if removal.processTreeReaped {
			t.Fatal("removal was locally persisted only after the process tree was reaped")
		}
		stages = append(stages, "persist")
		return nil
	}
	controller.reapService = func(context.Context, string) (workloadrunner.ReapReceipt, error) {
		stages = append(stages, "withdraw-and-reap")
		return workloadrunner.ReapReceipt{RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidenceAttempt}, nil
	}
	controller.purgeJob = func(context.Context, string) error {
		stages = append(stages, "purge-spool")
		return nil
	}
	controller.removeResource = func(_ context.Context, removal localRemoval) error {
		if !removal.processTreeReaped {
			t.Fatal("managed deletion was invoked without positive reap evidence")
		}
		stages = append(stages, "managedroot-remove")
		return nil
	}
	controller.releaseImagePin = func(context.Context, string) error {
		stages = append(stages, "release-image-pin")
		return nil
	}
	controller.ackRemoval = func(context.Context, localRemoval) error {
		stages = append(stages, "acknowledge")
		return nil
	}
	controller.finishRemoval = func(context.Context, localRemoval) error {
		stages = append(stages, "release-local-record")
		return nil
	}

	if err := controller.process(context.Background(), directive); err != nil {
		t.Fatal(err)
	}
	want := []string{
		"persist", "withdraw-and-reap", "purge-spool", "managedroot-remove", "release-image-pin", "acknowledge", "release-local-record",
	}
	if !reflect.DeepEqual(stages, want) {
		t.Fatalf("removal stages = %v, want %v", stages, want)
	}
}

func TestComputerRemovalDeletesDiskBeforeAcknowledgement(t *testing.T) {
	directive := l1.RemovalDirective{JobID: "computer-job", BoundNodeID: "node", Kind: contract.JobKindOCI, RemovalGeneration: 4, CleanupFence: "cleanup", RootInstanceID: "root",
		ComputerStorage: &l1.ComputerStorageClaim{ComputerID: "computer", StorageID: "storage", StorageGeneration: 2}}
	var stages []string
	storage := &workloadrunner.ComputerStorage{ComputerID: "computer", StorageID: "storage", StorageGeneration: 2, DiskBytes: 8 << 30}
	attempt := testRuntimeResourceManifest(directive.JobID, "legacy-attempt")
	attempt.ServiceDataVolume = ""
	attempt.ServiceDataOwnerRecord = ""
	attempt.ComputerStorage = storage
	manifest := runtimeRemovalManifest{Version: 1, JobID: directive.JobID, RemovalGeneration: directive.RemovalGeneration,
		Attempts: []workloadrunner.RuntimeResourceManifest{attempt}}
	var frozen bool
	controller := &removalController{nodeID: "node", bootSessionID: "boot"}
	controller.beginRemoval = func(context.Context, localRemoval) error { return nil }
	controller.loadRuntimeRemoval = func(context.Context, string) (runtimeRemovalRecord, bool, error) {
		if !frozen {
			return runtimeRemovalRecord{}, false, nil
		}
		return runtimeRemovalRecord{removal: localRemoval{jobID: directive.JobID, kind: contract.JobKindOCI, generation: directive.RemovalGeneration, cleanupFence: directive.CleanupFence, rootInstanceID: directive.RootInstanceID}, manifest: manifest, phase: runtimeRemovalPrepared}, true, nil
	}
	controller.reconstructRuntime = func(context.Context, workloadrunner.RuntimeRemovalProofRequest) ([]workloadrunner.RuntimeResourceManifest, error) {
		stages = append(stages, "scan")
		return manifest.Attempts, nil
	}
	controller.persistRuntimeRemoval = func(_ context.Context, _ localRemoval, attempts []workloadrunner.RuntimeResourceManifest) error {
		stages = append(stages, "freeze")
		if !reflect.DeepEqual(attempts, manifest.Attempts) {
			t.Fatalf("persisted Computer inventory = %+v", attempts)
		}
		frozen = true
		return nil
	}
	controller.reapService = func(context.Context, string) (workloadrunner.ReapReceipt, error) {
		stages = append(stages, "reap")
		return workloadrunner.ReapReceipt{RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidenceAttempt, BootSessionID: "boot"}, nil
	}
	controller.recordRuntimeQuiesced = func(context.Context, localRemoval, workloadrunner.ReapReceipt) error { return nil }
	controller.recordRuntimeAttested = func(context.Context, localRemoval, workloadrunner.RuntimeRemovalAttestation) error { return nil }
	controller.purgeJob = func(context.Context, string) error { return nil }
	controller.removeResource = func(context.Context, localRemoval) error { stages = append(stages, "managed"); return nil }
	controller.finalizeVolumes = func(_ context.Context, request workloadrunner.ManagedVolumeFinalizationRequest) error {
		stages = append(stages, "disk")
		if request.Removal == nil || request.Removal.CleanupFence != directive.CleanupFence || len(request.Volumes) != 1 || request.Volumes[0].ComputerStorage.StorageGeneration != 2 {
			t.Fatalf("Computer finalization request = %+v", request)
		}
		return nil
	}
	controller.deleteRuntimeData = func(context.Context, workloadrunner.RuntimeRemovalProofRequest) error {
		stages = append(stages, "delete-data")
		return nil
	}
	controller.attestRuntimeRemoval = func(_ context.Context, request workloadrunner.RuntimeRemovalProofRequest) (workloadrunner.RuntimeRemovalAttestation, error) {
		stages = append(stages, "attest")
		classes := make(map[workloadrunner.RuntimeRemovalResourceClass]bool)
		for _, resource := range request.Attempts[0].RemovalResources() {
			classes[resource.Class] = true
		}
		for _, class := range []workloadrunner.RuntimeRemovalResourceClass{
			workloadrunner.RuntimeRemovalComputerDiskImage, workloadrunner.RuntimeRemovalComputerDiskAllocation,
			workloadrunner.RuntimeRemovalComputerDiskQuota, workloadrunner.RuntimeRemovalComputerDiskManifest,
			workloadrunner.RuntimeRemovalComputerDiskMount, workloadrunner.RuntimeRemovalComputerDiskLoop,
			workloadrunner.RuntimeRemovalComputerAttachment,
		} {
			if !classes[class] {
				t.Fatalf("Computer attestation omitted class %q: %+v", class, request.Attempts[0].RemovalResources())
			}
		}
		if classes[workloadrunner.RuntimeRemovalServiceData] || classes[workloadrunner.RuntimeRemovalServiceDataRecord] {
			t.Fatalf("Computer attestation included service-data classes: %+v", request.Attempts[0].RemovalResources())
		}
		return testRuntimeRemovalAttestation(manifest), nil
	}
	controller.ackRemoval = func(context.Context, localRemoval) error { stages = append(stages, "ack"); return nil }
	controller.finishRemoval = func(context.Context, localRemoval) error { return nil }
	if err := controller.process(t.Context(), directive); err != nil {
		t.Fatal(err)
	}
	if want := []string{"scan", "freeze", "reap", "managed", "disk", "delete-data", "attest", "ack"}; !reflect.DeepEqual(stages, want) {
		t.Fatalf("Computer removal stages = %v, want %v", stages, want)
	}
}

func TestComputerRemovalRejectsNonOCIKind(t *testing.T) {
	controller := &removalController{nodeID: "node"}
	began := false
	controller.beginRemoval = func(context.Context, localRemoval) error { began = true; return nil }
	err := controller.process(t.Context(), l1.RemovalDirective{
		JobID: "computer-job", BoundNodeID: "node", Kind: contract.JobKindProcess,
		RemovalGeneration: 1, CleanupFence: "cleanup", RootInstanceID: "root",
		ComputerStorage: &l1.ComputerStorageClaim{ComputerID: "computer", StorageID: "storage", StorageGeneration: 1},
	})
	if err == nil || began {
		t.Fatalf("Computer/process pairing = err %v began=%t", err, began)
	}
}

func TestComputerRemovalResumesFromDurableAttestationWithoutRepeatingHelperDeletion(t *testing.T) {
	removal := localRemoval{jobID: "computer-job", kind: contract.JobKindOCI, generation: 4, cleanupFence: "cleanup", rootInstanceID: "root", processTreeReaped: true}
	storage := &workloadrunner.ComputerStorage{ComputerID: "computer", StorageID: "storage", StorageGeneration: 2, DiskBytes: 8 << 30}
	record := runtimeRemovalRecord{
		removal: removal, phase: runtimeRemovalComplete,
		manifest: runtimeRemovalManifest{Version: 1, JobID: removal.jobID, RemovalGeneration: removal.generation,
			Attempts: []workloadrunner.RuntimeResourceManifest{{ComputerStorage: storage}}},
	}
	var stages []string
	controller := &removalController{managed: &resumedManagedResource{}, nodeID: "node", bootSessionID: "new-boot"}
	controller.listRuntimeRemovals = func(context.Context) ([]runtimeRemovalRecord, error) { return []runtimeRemovalRecord{record}, nil }
	controller.purgeJob = func(context.Context, string) error { return nil }
	controller.removeResource = func(context.Context, localRemoval) error { return nil }
	controller.finalizeVolumes = func(_ context.Context, request workloadrunner.ManagedVolumeFinalizationRequest) error {
		t.Fatalf("durably attested removal repeated Computer deletion: %+v", request)
		return errors.New("unreachable")
	}
	controller.ackRemoval = func(context.Context, localRemoval) error { stages = append(stages, "ack"); return nil }
	controller.finishRemoval = func(context.Context, localRemoval) error { return nil }
	if err := controller.resume(t.Context()); err != nil {
		t.Fatal(err)
	}
	if want := []string{"ack"}; !reflect.DeepEqual(stages, want) {
		t.Fatalf("resumed Computer removal stages = %v, want %v", stages, want)
	}
}

func TestRemovalControllerNeverAcknowledgesFailedDeletion(t *testing.T) {
	errDelete := errors.New("injected deletion failure")
	acknowledged := false
	controller := &removalController{nodeID: "node"}
	controller.beginRemoval = func(context.Context, localRemoval) error { return nil }
	controller.reapService = func(context.Context, string) (workloadrunner.ReapReceipt, error) {
		return workloadrunner.ReapReceipt{RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidenceAttempt}, nil
	}
	controller.purgeJob = func(context.Context, string) error { return nil }
	controller.removeResource = func(context.Context, localRemoval) error { return errDelete }
	controller.ackRemoval = func(context.Context, localRemoval) error {
		acknowledged = true
		return nil
	}
	controller.finishRemoval = func(context.Context, localRemoval) error { return nil }

	err := controller.process(context.Background(), l1.RemovalDirective{
		JobID: "service", BoundNodeID: "node", Kind: contract.JobKindProcess, RemovalGeneration: 1,
		CleanupFence: "fence", RootInstanceID: "root",
	})
	if !errors.Is(err, errDelete) || acknowledged {
		t.Fatalf("failed deletion = err %v acknowledged %t", err, acknowledged)
	}
}

func TestRemovalControllerBlocksWithoutPositiveRuntimeReceipt(t *testing.T) {
	tests := []struct {
		name    string
		receipt workloadrunner.ReapReceipt
		err     error
	}{
		{name: "missing receipt"},
		{name: "missing evidence kind", receipt: workloadrunner.ReapReceipt{RuntimeQuiesced: true}},
		{name: "refused receipt", err: errors.New("runtime refused quiescence")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			downstream := false
			controller := &removalController{nodeID: "node"}
			controller.beginRemoval = func(context.Context, localRemoval) error { return nil }
			controller.reapService = func(context.Context, string) (workloadrunner.ReapReceipt, error) {
				return test.receipt, test.err
			}
			controller.purgeJob = func(context.Context, string) error { downstream = true; return nil }
			controller.removeResource = func(context.Context, localRemoval) error { downstream = true; return nil }
			controller.ackRemoval = func(context.Context, localRemoval) error { downstream = true; return nil }
			controller.finishRemoval = func(context.Context, localRemoval) error { downstream = true; return nil }
			err := controller.process(context.Background(), l1.RemovalDirective{
				JobID: "service", BoundNodeID: "node", Kind: contract.JobKindProcess, RemovalGeneration: 1,
				CleanupFence: "fence", RootInstanceID: "root",
			})
			if err == nil || downstream {
				t.Fatalf("removal without receipt = err %v downstream=%t", err, downstream)
			}
		})
	}
}

func TestRemovalControllerCompletesRuntimeManifestThenDeletesAndAcknowledges(t *testing.T) {
	directive := l1.RemovalDirective{
		JobID: "oci-service", BoundNodeID: "node", Kind: contract.JobKindOCI, RemovalGeneration: 1,
		CleanupFence: "fence", RootInstanceID: "root",
	}
	var stages []string
	controller := &removalController{nodeID: directive.BoundNodeID}
	controller.beginRemoval = func(context.Context, localRemoval) error {
		stages = append(stages, "manifest")
		return nil
	}
	manifest := runtimeRemovalManifest{Version: 1, JobID: directive.JobID, RemovalGeneration: 1, Attempts: []workloadrunner.RuntimeResourceManifest{testRuntimeResourceManifest(directive.JobID, "attempt")}}
	controller.loadRuntimeRemoval = func(context.Context, string) (runtimeRemovalRecord, bool, error) {
		return runtimeRemovalRecord{removal: testRuntimeRemoval(directive.JobID), manifest: manifest, phase: runtimeRemovalPrepared}, true, nil
	}
	controller.reapService = func(context.Context, string) (workloadrunner.ReapReceipt, error) {
		stages = append(stages, "quiesce")
		return workloadrunner.ReapReceipt{RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidenceAttempt}, nil
	}
	controller.recordRuntimeQuiesced = func(context.Context, localRemoval, workloadrunner.ReapReceipt) error {
		stages = append(stages, "record-quiescence")
		return nil
	}
	controller.deleteRuntimeData = func(context.Context, workloadrunner.RuntimeRemovalProofRequest) error {
		stages = append(stages, "delete-runtime-data")
		return nil
	}
	controller.attestRuntimeRemoval = func(context.Context, workloadrunner.RuntimeRemovalProofRequest) (workloadrunner.RuntimeRemovalAttestation, error) {
		stages = append(stages, "delete-and-attest-runtime")
		return testRuntimeRemovalAttestation(manifest), nil
	}
	controller.recordRuntimeAttested = func(context.Context, localRemoval, workloadrunner.RuntimeRemovalAttestation) error {
		stages = append(stages, "record-attestation")
		return nil
	}
	controller.purgeJob = func(context.Context, string) error {
		stages = append(stages, "purge-spool")
		return nil
	}
	controller.removeResource = func(context.Context, localRemoval) error {
		stages = append(stages, "remove-resource")
		return nil
	}
	controller.releaseImagePin = func(context.Context, string) error {
		stages = append(stages, "release-image-pin")
		return nil
	}
	controller.ackRemoval = func(context.Context, localRemoval) error {
		stages = append(stages, "acknowledge")
		return nil
	}
	controller.finishRemoval = func(context.Context, localRemoval) error {
		stages = append(stages, "finish-removal")
		return nil
	}

	if err := controller.process(t.Context(), directive); err != nil {
		t.Fatal(err)
	}
	if want := []string{
		"manifest", "quiesce", "record-quiescence", "purge-spool", "remove-resource", "delete-runtime-data", "delete-and-attest-runtime", "record-attestation",
		"release-image-pin", "acknowledge", "finish-removal",
	}; !reflect.DeepEqual(stages, want) {
		t.Fatalf("OCI removal preparation stages = %v, want %v", stages, want)
	}
}

func TestRemovalControllerResumesEveryRuntimeManifestPhase(t *testing.T) {
	for _, test := range []struct {
		name       string
		phase      runtimeRemovalPhase
		wantReap   bool
		wantRecord bool
	}{
		{name: "prepared", phase: runtimeRemovalPrepared, wantReap: true, wantRecord: true},
		{name: "quarantined", phase: runtimeRemovalQuarantined},
		{name: "complete", phase: runtimeRemovalComplete},
	} {
		t.Run(test.name, func(t *testing.T) {
			removal := testRuntimeRemoval("resume-" + test.name)
			receipt := workloadrunner.ReapReceipt{
				RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidencePriorBootOCISweep,
				BootSessionID: "prior-boot", SweepEpoch: "sweep-1", HelperGeneration: 1,
			}
			manifest := runtimeRemovalManifest{Version: 1, JobID: removal.jobID, RemovalGeneration: removal.generation, Attempts: []workloadrunner.RuntimeResourceManifest{testRuntimeResourceManifest(removal.jobID, "attempt")}}
			record := runtimeRemovalRecord{removal: removal, manifest: manifest, phase: test.phase}
			if test.phase != runtimeRemovalPrepared {
				record.receipt = receipt
			}
			reaped := false
			recorded := false
			controller := &removalController{managed: &resumedManagedResource{}}
			controller.listRuntimeRemovals = func(context.Context) ([]runtimeRemovalRecord, error) { return []runtimeRemovalRecord{record}, nil }
			controller.reapService = func(context.Context, string) (workloadrunner.ReapReceipt, error) {
				reaped = true
				return receipt, nil
			}
			controller.recordRuntimeQuiesced = func(context.Context, localRemoval, workloadrunner.ReapReceipt) error {
				recorded = true
				return nil
			}
			controller.deleteRuntimeData = func(context.Context, workloadrunner.RuntimeRemovalProofRequest) error { return nil }
			controller.attestRuntimeRemoval = func(context.Context, workloadrunner.RuntimeRemovalProofRequest) (workloadrunner.RuntimeRemovalAttestation, error) {
				return testRuntimeRemovalAttestation(manifest), nil
			}
			controller.recordRuntimeAttested = func(context.Context, localRemoval, workloadrunner.RuntimeRemovalAttestation) error { return nil }
			controller.purgeJob = func(context.Context, string) error { return nil }
			controller.removeResource = func(context.Context, localRemoval) error { return nil }
			controller.releaseImagePin = func(context.Context, string) error { return nil }
			controller.ackRemoval = func(context.Context, localRemoval) error { return nil }
			controller.finishRemoval = func(context.Context, localRemoval) error { return nil }
			if err := controller.resume(t.Context()); err != nil {
				t.Fatal(err)
			}
			if reaped != test.wantReap || recorded != test.wantRecord {
				t.Fatalf("resume phase %s = reaped %t recorded %t, want %t/%t", test.phase, reaped, recorded, test.wantReap, test.wantRecord)
			}
		})
	}
}

func TestRemovalControllerCrashBetweenHelperDeleteAndAttestationNeverAcknowledgesEarly(t *testing.T) {
	directive := l1.RemovalDirective{
		JobID: "oci-delete-crash", BoundNodeID: "node", Kind: contract.JobKindOCI,
		RemovalGeneration: 1, CleanupFence: "fence", RootInstanceID: "root",
	}
	removal := testRuntimeRemoval(directive.JobID)
	removal.cleanupFence = directive.CleanupFence
	removal.rootInstanceID = directive.RootInstanceID
	manifest := runtimeRemovalManifest{Version: 1, JobID: directive.JobID, RemovalGeneration: 1, Attempts: []workloadrunner.RuntimeResourceManifest{testRuntimeResourceManifest(directive.JobID, "attempt")}}
	record := runtimeRemovalRecord{removal: removal, manifest: manifest, phase: runtimeRemovalQuarantined, receipt: workloadrunner.ReapReceipt{RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidenceAttempt, BootSessionID: "boot"}}
	acknowledged := false
	proofCalls := 0
	deleteCalls := 0
	controller := &removalController{nodeID: directive.BoundNodeID}
	controller.beginRemoval = func(context.Context, localRemoval) error { return nil }
	controller.loadRuntimeRemoval = func(context.Context, string) (runtimeRemovalRecord, bool, error) { return record, true, nil }
	controller.purgeJob = func(context.Context, string) error { return nil }
	controller.removeResource = func(context.Context, localRemoval) error { return nil }
	controller.deleteRuntimeData = func(context.Context, workloadrunner.RuntimeRemovalProofRequest) error {
		deleteCalls++
		return nil
	}
	controller.attestRuntimeRemoval = func(context.Context, workloadrunner.RuntimeRemovalProofRequest) (workloadrunner.RuntimeRemovalAttestation, error) {
		proofCalls++
		if proofCalls == 1 {
			return workloadrunner.RuntimeRemovalAttestation{}, errInjectedRuntimeRemovalCrash
		}
		return testRuntimeRemovalAttestation(manifest), nil
	}
	controller.recordRuntimeAttested = func(context.Context, localRemoval, workloadrunner.RuntimeRemovalAttestation) error { return nil }
	controller.releaseImagePin = func(context.Context, string) error { return nil }
	controller.ackRemoval = func(context.Context, localRemoval) error { acknowledged = true; return nil }
	controller.finishRemoval = func(context.Context, localRemoval) error { return nil }
	if err := controller.process(t.Context(), directive); !errors.Is(err, errInjectedRuntimeRemovalCrash) || acknowledged {
		t.Fatalf("delete/attest crash = err %v acknowledged=%t", err, acknowledged)
	}
	if err := controller.process(t.Context(), directive); err != nil || !acknowledged || proofCalls != 2 || deleteCalls != 2 {
		t.Fatalf("delete/attest resume = err %v acknowledged=%t delete_calls=%d proof_calls=%d", err, acknowledged, deleteCalls, proofCalls)
	}
}

func TestSessionRoutesRuntimeReceiptIntoLiveRemoval(t *testing.T) {
	resident := &residentAttempt{
		class: contract.JobClassService, cancel: func(error) {},
		done: make(chan struct{}), runtimeReaped: make(chan runtimeReapOutcome, 1),
	}
	close(resident.done)
	session := &agentSession{
		resident:      map[string]*residentAttempt{"service": resident},
		residentJobID: map[string]struct{}{"service": {}},
		serviceReaps:  make(map[string]runtimeReapOutcome),
	}
	want := workloadrunner.ReapReceipt{RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidenceAttempt}
	session.recordRuntimeReap("service", want, nil)
	got, err := session.reapServiceForRemoval(context.Background(), "service")
	if err != nil || got != want {
		t.Fatalf("routed runtime receipt = %#v err=%v, want %#v", got, err, want)
	}
}

func TestReturningNodeRemovalUsesPriorBootGuardianReceipt(t *testing.T) {
	called := false
	session := &agentSession{
		registration: contract.NodeRegistration{BootSessionID: "boot-current"},
		resident:     make(map[string]*residentAttempt), residentJobID: make(map[string]struct{}),
		serviceReaps: make(map[string]runtimeReapOutcome), serviceBoots: make(map[string]string),
		reapPriorBoot: func(_ context.Context, jobID string) (workloadrunner.ReapReceipt, error) {
			called = true
			if jobID != "offline-service" {
				t.Fatalf("prior-boot reap job = %q", jobID)
			}
			return workloadrunner.ReapReceipt{
				RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidencePriorBootGuardian,
				BootSessionID: "boot-prior",
			}, nil
		},
	}
	receipt, err := session.reapServiceForRemoval(context.Background(), "offline-service")
	if err != nil || !called || receipt.Evidence != workloadrunner.ReapEvidencePriorBootGuardian {
		t.Fatalf("returning-node receipt = %#v err=%v called=%t", receipt, err, called)
	}
}

func TestRemovalControllerResumesQuarantinedDeletionBeforeAcknowledging(t *testing.T) {
	removal := localRemoval{
		jobID: "resumed-service", kind: contract.JobKindProcess, generation: 2, rootInstanceID: "root", cleanupFence: "fence",
		processTreeReaped: true,
	}
	managed := &resumedManagedResource{completed: []localRemoval{removal}}
	var stages []string
	controller := &removalController{managed: managed}
	controller.loadRemovalIntent = func(context.Context, string) (localRemoval, bool, error) { return removal, true, nil }
	controller.purgeJob = func(context.Context, string) error {
		stages = append(stages, "purge-spool")
		return nil
	}
	controller.releaseImagePin = func(context.Context, string) error {
		stages = append(stages, "release-image-pin")
		return nil
	}
	controller.ackRemoval = func(context.Context, localRemoval) error {
		stages = append(stages, "acknowledge")
		return nil
	}
	controller.finishRemoval = func(context.Context, localRemoval) error {
		stages = append(stages, "release-local-record")
		return nil
	}
	if err := controller.resume(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !managed.resumed {
		t.Fatal("managedroot.Resume was not invoked")
	}
	if want := []string{"purge-spool", "release-image-pin", "acknowledge", "release-local-record"}; !reflect.DeepEqual(stages, want) {
		t.Fatalf("resumed removal stages = %v, want %v", stages, want)
	}
}

func TestRemovalControllerSkipsCompletedManagedrootTombstoneWithoutIntent(t *testing.T) {
	historical := localRemoval{jobID: "historical", processTreeReaped: true}
	pending := localRemoval{jobID: "pending", kind: contract.JobKindProcess, generation: 2, rootInstanceID: "root", cleanupFence: "fence", processTreeReaped: true}
	managed := &resumedManagedResource{completed: []localRemoval{historical, pending}}
	var acknowledged []string
	controller := &removalController{managed: managed}
	controller.loadRemovalIntent = func(_ context.Context, jobID string) (localRemoval, bool, error) {
		if jobID == historical.jobID {
			return localRemoval{}, false, nil
		}
		return pending, true, nil
	}
	controller.purgeJob = func(context.Context, string) error { return nil }
	controller.ackRemoval = func(_ context.Context, removal localRemoval) error {
		acknowledged = append(acknowledged, removal.jobID)
		return nil
	}
	controller.finishRemoval = func(context.Context, localRemoval) error { return nil }
	if err := controller.resume(t.Context()); err != nil {
		t.Fatal(err)
	}
	if want := []string{pending.jobID}; !reflect.DeepEqual(acknowledged, want) {
		t.Fatalf("acknowledged removals = %v, want %v", acknowledged, want)
	}
}

type resumedManagedResource struct {
	completed []localRemoval
	resumed   bool
}

func (*resumedManagedResource) rootInstanceID() string { return "root" }

func (*resumedManagedResource) prepareAttempt(string, string) (managedResourceAttempt, func(), error) {
	return managedResourceAttempt{}, func() {}, nil
}

func (*resumedManagedResource) remove(context.Context, localRemoval) error { return nil }

func (resource *resumedManagedResource) resumeRemovals(context.Context) ([]localRemoval, error) {
	resource.resumed = true
	return resource.completed, nil
}
