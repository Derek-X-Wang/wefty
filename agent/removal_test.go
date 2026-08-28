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
		JobID: "service", BoundNodeID: "node", RemovalGeneration: 1,
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
				JobID: "service", BoundNodeID: "node", RemovalGeneration: 1,
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
		JobID: "oci-service", BoundNodeID: "node", RemovalGeneration: 1,
		CleanupFence: "fence", RootInstanceID: "root",
	}
	var stages []string
	controller := &removalController{nodeID: directive.BoundNodeID}
	controller.beginRemoval = func(context.Context, localRemoval) error {
		stages = append(stages, "manifest")
		return nil
	}
	controller.loadRuntimeRemoval = func(context.Context, string) (runtimeRemovalRecord, bool, error) {
		return runtimeRemovalRecord{removal: testRuntimeRemoval(directive.JobID), phase: runtimeRemovalPrepared}, true, nil
	}
	controller.reapService = func(context.Context, string) (workloadrunner.ReapReceipt, error) {
		stages = append(stages, "quiesce")
		return workloadrunner.ReapReceipt{RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidenceAttempt}, nil
	}
	controller.recordRuntimeQuiesced = func(context.Context, localRemoval, workloadrunner.ReapReceipt) error {
		stages = append(stages, "complete-manifest")
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
		"manifest", "quiesce", "complete-manifest", "purge-spool", "remove-resource",
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
		{name: "quarantined", phase: runtimeRemovalQuarantined, wantRecord: true},
		{name: "complete", phase: runtimeRemovalComplete},
	} {
		t.Run(test.name, func(t *testing.T) {
			removal := testRuntimeRemoval("resume-" + test.name)
			receipt := workloadrunner.ReapReceipt{
				RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidencePriorBootOCISweep,
				BootSessionID: "prior-boot", SweepEpoch: "sweep-1", HelperGeneration: 1,
			}
			record := runtimeRemovalRecord{removal: removal, phase: test.phase}
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
		jobID: "resumed-service", generation: 2, rootInstanceID: "root", cleanupFence: "fence",
		processTreeReaped: true,
	}
	managed := &resumedManagedResource{completed: []localRemoval{removal}}
	var stages []string
	controller := &removalController{managed: managed}
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
