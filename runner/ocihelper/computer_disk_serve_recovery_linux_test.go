//go:build linux

package ocihelper

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type serveRecoveryEngine struct {
	*fakeEngine
	disk *ContainerdEngine
}

func (engine *serveRecoveryEngine) Sweep(ctx context.Context, request SweepRequest) (SweepResponse, error) {
	if err := engine.disk.sweepComputerDisks(ctx, request.SweepEpoch); err != nil {
		return SweepResponse{}, err
	}
	inventory := ResourceInventory{}
	if err := engine.disk.inventoryComputerDiskResources(&inventory); err != nil {
		return SweepResponse{}, err
	}
	return SweepResponse{Inventory: inventory}, nil
}

func (engine *serveRecoveryEngine) Verify(context.Context, VerifyRequest) (VerifyResponse, error) {
	inventory := ResourceInventory{}
	if err := engine.disk.inventoryComputerDiskResources(&inventory); err != nil {
		return VerifyResponse{}, err
	}
	runtimeResidue := inventory
	runtimeResidue.ComputerQuarantines = nil
	durableRetained := ResourceInventory{ComputerQuarantines: append([]string(nil), inventory.ComputerQuarantines...)}
	return VerifyResponse{Absent: InventoryEmpty(runtimeResidue), Inventory: inventory, RuntimeResidue: runtimeResidue, DurableRetained: durableRetained}, nil
}

// TestServeQuarantinesUnrecordedMismatchAndAdmitsBarrier is the product-path
// regression for the startup incident: the listener is not published until
// startup Sweep has converted the unrecorded mismatch into typed retained
// state, after which a real BootBarrier can verify and admit the namespace.
func TestServeQuarantinesUnrecordedMismatchAndAdmitsBarrier(t *testing.T) {
	root := t.TempDir()
	storage := testComputerStorage()
	name, err := deterministicComputerDiskName(storage)
	if err != nil {
		t.Fatal(err)
	}
	diskRoot := filepath.Join(root, "computer-disks", name)
	if err := os.MkdirAll(diskRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	imagePath := filepath.Join(diskRoot, "disk.ext4")
	if err := os.WriteFile(imagePath, make([]byte, 4096), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := writeComputerDiskManifest(diskRoot, computerDiskManifest{Version: computerDiskManifestVersion,
		Storage: storage, DiskImage: "disk.ext4", MountDirectory: name, Prepared: true}); err != nil {
		t.Fatal(err)
	}
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: newFakeComputerDiskSystem(),
		attempts: make(map[string]*containerdAttempt), capacityReservations: make(map[string]*capacityReservation)}
	serverEngine := &serveRecoveryEngine{fakeEngine: newFakeEngine(), disk: engine}
	server, err := NewServer(serverEngine, ServerConfig{HelperVersion: "test", HelperChecksum: "checksum", AllowedUIDs: []uint32{uint32(os.Getuid())}})
	if err != nil {
		t.Fatal(err)
	}
	socket := filepath.Join(t.TempDir(), "helper.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx, listener) }()
	t.Cleanup(func() {
		cancel()
		_ = listener.Close()
		select {
		case <-done:
		case <-time.After(time.Second):
		}
	})
	client := NewUnixClient(socket, "checksum")
	barrier, err := NewBootBarrierWithConfig(client, AcquireSessionRequest{NodeID: "node", BootSessionID: "boot"}, BootBarrierConfig{
		TakeoverTimeout: time.Second, TakeoverReapTimeout: 10 * time.Second, TakeoverRetry: 5 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer barrier.Close()
	if err := barrier.Ensure(t.Context()); err != nil {
		select {
		case serveErr := <-done:
			t.Fatalf("Serve failed before barrier admission: %v (barrier: %v)", serveErr, err)
		default:
			t.Fatalf("barrier not admitted: %v", err)
		}
	}
	entries, err := os.ReadDir(filepath.Join(root, "computer-disk-quarantine"))
	if err != nil || len(entries) != 1 || !strings.HasPrefix(entries[0].Name(), name+"-anomaly-") {
		t.Fatalf("quarantine directory = %v err=%v", entries, err)
	}
	if _, err := os.Stat(filepath.Join(root, "computer-disk-quarantine", entries[0].Name(), "quarantine.json")); err != nil {
		t.Fatalf("typed quarantine receipt missing: %v", err)
	}
}
