package l1

import (
	"path/filepath"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
)

func TestComputerDefaultImageChangesOnlyNewOnDemandComputers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "l1.sqlite")
	store, err := OpenStore(path, StoreOptions{ComputerDefaultImage: "ghcr.io/example/desktop:v1"})
	if err != nil {
		t.Fatal(err)
	}
	request := CreateComputerRequest{Name: "shared-one", Spec: computerCapabilityJobSpec("default-image-one"), Actor: "seat-request",
		SeatRequestSettings: &ComputerSettings{SeatLimit: 2, IdleAction: ComputerIdleDelete, Network: ComputerNetworkInternet}}
	request.Spec.Execution.OCI.Image.Reference, err = store.ComputerDefaultImage()
	if err != nil {
		store.Close()
		t.Fatal(err)
	}
	first, _, err := store.CreateComputer(t.Context(), request)
	if err != nil {
		store.Close()
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	// Like the Backup-cap default, the setting changes on an L1 restart.
	store, err = OpenStore(path, StoreOptions{ComputerDefaultImage: "ghcr.io/example/desktop:v2"})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	existing, err := testReads(store).GetComputer(t.Context(), first.ComputerID)
	if err != nil || existing.CurrentJob.Spec.Execution.OCI.Image.Reference != "ghcr.io/example/desktop:v1" {
		t.Fatalf("existing image changed = %+v, %v", existing, err)
	}
	if replay, replayed, err := store.CreateComputer(t.Context(), request); err != nil || !replayed || replay.ComputerID != first.ComputerID {
		t.Fatalf("old request replay = %+v, %v, %v", replay, replayed, err)
	}
	request.Name = "shared-two"
	request.Spec.DispatchKey = "default-image-two"
	if _, _, err := store.CreateComputer(t.Context(), request); errorCode(err) != contract.ErrorConflict {
		t.Fatalf("stale default was accepted for a new Computer: %v", err)
	}
	request.Spec.Execution.OCI.Image.Reference, err = store.ComputerDefaultImage()
	if err != nil {
		t.Fatal(err)
	}
	digest := "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	request.Spec.Execution.OCI.Image.Digest = &digest
	second, _, err := store.CreateComputer(t.Context(), request)
	if err != nil || second.CurrentJob.Spec.Execution.OCI.Image.Reference != "ghcr.io/example/desktop:v2" ||
		*second.CurrentJob.Spec.Execution.OCI.Image.Digest != digest {
		t.Fatalf("new Computer image = %+v, %v", second, err)
	}
	// An owner's explicit image remains their choice.
	_, _, err = store.CreateComputer(t.Context(), CreateComputerRequest{Name: "owner-custom",
		Spec: computerCapabilityJobSpec("owner-custom-image"), Actor: "owner"})
	if err != nil {
		t.Fatalf("default overrode the owner's explicit image: %v", err)
	}
}
