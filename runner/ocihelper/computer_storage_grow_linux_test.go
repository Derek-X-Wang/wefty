//go:build linux

package ocihelper

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

func growTestRequest(newBytes int64) GrowComputerStorageRequest {
	return GrowComputerStorageRequest{
		Storage: ComputerStorageReference{ComputerID: "computer", StorageID: "storage",
			StorageGeneration: 3, IntentRevision: 7, DiskBytes: 8 << 20},
		NewDiskBytes: newBytes,
		Authority: ComputerStorageGrowAuthority{NodeID: "node", BootSessionID: "boot",
			HelperGeneration: 4, RootInstanceID: "root", JobID: "job",
			OperationRevision: 7, OperationFence: "fence"},
	}
}

func TestComputerGrowCapacityRetryNeverDoubleReservesMaterializedDelta(t *testing.T) {
	root := t.TempDir()
	available, err := filesystemAvailableBytes(root)
	if err != nil {
		t.Fatal(err)
	}
	request := growTestRequest(available + (16 << 20))
	engine := &ContainerdEngine{capacityReservations: map[string]*capacityReservation{
		"job": {diskBytes: request.Storage.DiskBytes, diskMaterialized: true, attempts: map[string]struct{}{"attempt": {}}},
	}}
	if _, admitted, err := engine.reserveGrowCapacity(request, root, request.Storage.DiskBytes); err != nil || admitted {
		t.Fatalf("unmaterialized delta admission = %t err=%v", admitted, err)
	}
	if got := engine.capacityReservations["job"].diskBytes; got != request.Storage.DiskBytes {
		t.Fatalf("refused grow changed reservation to %d", got)
	}
	if _, admitted, err := engine.reserveGrowCapacity(request, root, request.NewDiskBytes); err != nil || !admitted {
		t.Fatalf("materialized retry admission = %t err=%v", admitted, err)
	}
	if got := engine.capacityReservations["job"].diskBytes; got != request.NewDiskBytes {
		t.Fatalf("materialized grow reservation = %d", got)
	}
}

func TestComputerGrowCapacityRefusalReturnsBoundUnchangedReceipt(t *testing.T) {
	root := t.TempDir()
	available, err := filesystemAvailableBytes(root)
	if err != nil {
		t.Fatal(err)
	}
	request := growTestRequest(available + (16 << 20))
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root},
		capacityReservations: make(map[string]*capacityReservation), attempts: make(map[string]*containerdAttempt)}
	response, err := engine.GrowComputerStorage(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	receipt := response.Receipt
	if receipt.Kind != "computer_storage_grow_failed_unchanged" || receipt.Applied ||
		receipt.FailureCode != "insufficient_disk" || receipt.ComputerID != request.Storage.ComputerID ||
		receipt.StorageGeneration != request.Storage.StorageGeneration || receipt.NodeID != request.Authority.NodeID ||
		receipt.RootInstanceID != request.Authority.RootInstanceID || receipt.OperationFence != request.Authority.OperationFence ||
		receipt.OldDiskBytes != request.Storage.DiskBytes || receipt.NewDiskBytes != request.NewDiskBytes ||
		receipt.HelperGeneration != request.Authority.HelperGeneration {
		t.Fatalf("capacity refusal receipt = %#v", receipt)
	}
}

func TestComputerGrowResumesEveryCrashBoundaryWithoutDoubleAllocation(t *testing.T) {
	for _, checkpoint := range []string{"capacity_reserved", "filesystem_expanded", "manifest_published"} {
		t.Run(checkpoint, func(t *testing.T) {
			root := t.TempDir()
			request := growTestRequest(16 << 20)
			request.Storage.DiskBytes = 8 << 20
			diskSystem := newFakeComputerDiskSystem()
			engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: diskSystem,
				capacityReservations: make(map[string]*capacityReservation), attempts: make(map[string]*containerdAttempt),
				computerGrowResize: func(context.Context, string, string, int64, int64) error { return nil },
			}
			injected := errors.New("injected grow crash")
			engine.computerGrowHook = func(observed string) error {
				if observed == checkpoint {
					return injected
				}
				return nil
			}
			if _, err := engine.GrowComputerStorage(context.Background(), request); !errors.Is(err, injected) {
				t.Fatalf("injected %s error = %v", checkpoint, err)
			}
			engine.computerGrowHook = nil
			response, err := engine.GrowComputerStorage(context.Background(), request)
			if err != nil || !response.Receipt.Applied || response.Receipt.Kind != "computer_storage_grow_applied" {
				t.Fatalf("resumed %s grow = %#v err=%v", checkpoint, response, err)
			}
			name, err := deterministicComputerDiskName(request.Storage)
			if err != nil {
				t.Fatal(err)
			}
			manifestPath := filepath.Join(root, "computer-disks", name, "attachment.json")
			manifest, present, err := readComputerDiskManifest(manifestPath)
			if err != nil || !present || manifest.Storage.DiskBytes != request.NewDiskBytes {
				t.Fatalf("resumed %s manifest = %#v present=%t err=%v", checkpoint, manifest, present, err)
			}
			if diskSystem.allocationRuns != 1 {
				t.Fatalf("resumed %s allocation runs = %d", checkpoint, diskSystem.allocationRuns)
			}
			reservation := engine.capacityReservations[request.Authority.JobID]
			if reservation == nil || reservation.diskBytes != request.NewDiskBytes || !reservation.diskMaterialized {
				t.Fatalf("resumed %s reservation = %#v", checkpoint, reservation)
			}
		})
	}
}
