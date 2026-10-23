package l1

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
)

func TestComputerDefaultImageChangesOnlyNewOnDemandComputers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "l1.sqlite")
	store, err := OpenStore(path, StoreOptions{ComputerDefaultImage: "ghcr.io/example/desktop:v1@sha256:" + strings.Repeat("a", 64)})
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
	request.Spec.Execution.OCI.Image.Reference, _, _ = strings.Cut(request.Spec.Execution.OCI.Image.Reference, "@")
	first, _, err := store.CreateComputer(t.Context(), request)
	if err != nil {
		store.Close()
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	// Like the Backup-cap default, the setting changes on an L1 restart.
	store, err = OpenStore(path, StoreOptions{ComputerDefaultImage: "ghcr.io/example/desktop:v2@sha256:" + strings.Repeat("b", 64)})
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
	request.Spec.Execution.OCI.Image.Reference, _, _ = strings.Cut(request.Spec.Execution.OCI.Image.Reference, "@")
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

func TestComputerDefaultImageComparesNormalizedPins(t *testing.T) {
	pin := "sha256:" + strings.Repeat("a", 64)
	store, err := OpenStore(filepath.Join(t.TempDir(), "l1.sqlite"), StoreOptions{ComputerDefaultImage: "docker.io/library/alpine@" + pin})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	request := CreateComputerRequest{Name: "normalized", Spec: computerCapabilityJobSpec("normalized-default"), Actor: "seat-request", SeatRequestSettings: &ComputerSettings{SeatLimit: 2, IdleAction: ComputerIdleDelete, Network: ComputerNetworkInternet}}
	request.Spec.Execution.OCI.Image.Reference = "alpine:latest"
	request.Spec.Execution.OCI.Image.Digest = &pin
	if _, _, err := store.CreateComputer(t.Context(), request); err != nil {
		t.Fatalf("equivalent normalized image refused: %v", err)
	}
	request.Name = "wrong-digest"
	request.Spec.DispatchKey = "wrong-digest"
	wrong := "sha256:" + strings.Repeat("b", 64)
	request.Spec.Execution.OCI.Image.Digest = &wrong
	if _, _, err := store.CreateComputer(t.Context(), request); errorCode(err) != contract.ErrorConflict {
		t.Fatalf("wrong pinned digest accepted: %v", err)
	}
}

func TestComputerDefaultImageRequiresDigestPin(t *testing.T) {
	if err := ValidateComputerDefaultImage("ghcr.io/example/desktop:current"); err == nil {
		t.Fatal("mutable default image accepted")
	}
}
