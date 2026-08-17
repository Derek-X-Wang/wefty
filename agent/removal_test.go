package agent

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/Derek-X-Wang/wefty/l1"
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
	controller.reapService = func(context.Context, string) error {
		stages = append(stages, "withdraw-and-reap")
		return nil
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
		"persist", "withdraw-and-reap", "purge-spool", "managedroot-remove", "acknowledge", "release-local-record",
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
	controller.reapService = func(context.Context, string) error { return nil }
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
	if want := []string{"purge-spool", "acknowledge", "release-local-record"}; !reflect.DeepEqual(stages, want) {
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
