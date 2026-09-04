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
	if err := engine.disk.sweepComputerDisksWithRecoveryAttempt(ctx, request.SweepEpoch, request.countComputerStorageRecoveryAttempt); err != nil {
		return SweepResponse{}, err
	}
	inventory := ResourceInventory{}
	if err := engine.disk.inventoryComputerDiskResources(&inventory); err != nil {
		return SweepResponse{}, err
	}
	return SweepResponse{Inventory: inventory}, nil
}

func (engine *serveRecoveryEngine) Verify(ctx context.Context, request VerifyRequest) (VerifyResponse, error) {
	inventory := ResourceInventory{}
	if err := engine.disk.inventoryComputerDiskResources(&inventory); err != nil {
		return VerifyResponse{}, err
	}
	_ = ctx
	_ = request
	runtimeResidue, retentions, err := engine.disk.runtimeAbsenceInventory(inventory, time.Now())
	if err != nil {
		return VerifyResponse{}, err
	}
	durableRetained := subtractResourceInventory(inventory, runtimeResidue)
	return VerifyResponse{Absent: InventoryEmpty(runtimeResidue), Inventory: inventory, RuntimeResidue: runtimeResidue, DurableRetained: durableRetained, DurableRetentions: retentions}, nil
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
	assertServeQuarantinesAndAdmits(t, root, storage, name, "allocation_mismatch")
}

func TestServeQuarantinesCorruptAttachmentAuthorityAndAdmitsBarrier(t *testing.T) {
	root := t.TempDir()
	storage := testComputerStorage()
	name, _ := deterministicComputerDiskName(storage)
	diskRoot := filepath.Join(root, "computer-disks", name)
	if err := os.MkdirAll(diskRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(diskRoot, "disk.ext4"), make([]byte, 4096), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(diskRoot, "attachment.json"), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	assertServeQuarantinesAndAdmits(t, root, storage, name, "manifest_invalid")
}

func TestServeQuarantinesInvalidPendingQuarantineAuthorityAndAdmitsBarrier(t *testing.T) {
	root := t.TempDir()
	storage := testComputerStorage()
	name, _ := deterministicComputerDiskName(storage)
	diskRoot := filepath.Join(root, "computer-disks", name)
	if err := os.MkdirAll(diskRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(diskRoot, "disk.ext4"), make([]byte, 4096), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(diskRoot, "quarantine.json"), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	assertServeQuarantinesAndAdmits(t, root, storage, name, "quarantine_authority_invalid")
}

func TestServeQuarantinesMalformedIdentitylessCopyRecordAndAdmitsBarrier(t *testing.T) {
	root := t.TempDir()
	storage := testComputerStorage()
	name, _ := deterministicComputerDiskName(storage)
	diskRoot := filepath.Join(root, "computer-disks", name)
	if err := os.MkdirAll(diskRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(diskRoot, "storage-copy.json"), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	assertServeQuarantinesAndAdmits(t, root, storage, name, "copy_recovery_authority_invalid")
}

func TestServeQuarantinesIncompleteIdentitylessCopyRecordAndAdmitsBarrier(t *testing.T) {
	root := t.TempDir()
	storage := testComputerStorage()
	name, _ := deterministicComputerDiskName(storage)
	diskRoot := filepath.Join(root, "computer-disks", name)
	if err := os.MkdirAll(diskRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(diskRoot, "storage-copy.json"), []byte(`{"version":1}`), 0o600); err != nil {
		t.Fatal(err)
	}
	assertServeQuarantinesAndAdmits(t, root, storage, name, "copy_recovery_authority_invalid")
}

func TestServeQuarantinesIdentityMismatchedManifestAndAdmitsBarrier(t *testing.T) {
	root := t.TempDir()
	storage := testComputerStorage()
	name, _ := deterministicComputerDiskName(storage)
	diskRoot := filepath.Join(root, "computer-disks", name)
	if err := os.MkdirAll(diskRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(diskRoot, "disk.ext4"), make([]byte, 4096), 0o600); err != nil {
		t.Fatal(err)
	}
	mismatched := storage
	mismatched.ComputerID = "different-computer"
	mismatched.DiskBytes = 8192
	if err := writeComputerDiskManifest(diskRoot, computerDiskManifest{Version: computerDiskManifestVersion,
		Storage: mismatched, DiskImage: "disk.ext4", MountDirectory: name, Prepared: true}); err != nil {
		t.Fatal(err)
	}
	assertServeQuarantinesAndAdmits(t, root, storage, name, "identity_mismatch")
}

func TestServeQuarantinesNonRegularRecoveryRecordsAndAdmitsBarrier(t *testing.T) {
	for _, test := range []struct {
		name  string
		write func(*testing.T, string)
	}{
		{name: "directory", write: func(t *testing.T, path string) {
			if err := os.Mkdir(path, 0o700); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "symlink", write: func(t *testing.T, path string) {
			target := filepath.Join(t.TempDir(), "attachment.json")
			if err := os.WriteFile(target, []byte(`{"version":1}`), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(target, path); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			storage := testComputerStorage()
			name, _ := deterministicComputerDiskName(storage)
			diskRoot := filepath.Join(root, "computer-disks", name)
			if err := os.MkdirAll(diskRoot, 0o700); err != nil {
				t.Fatal(err)
			}
			test.write(t, filepath.Join(diskRoot, "attachment.json"))
			assertServeQuarantinesAndAdmits(t, root, storage, name, "record_not_regular")
		})
	}
}

func assertServeQuarantinesAndAdmits(t *testing.T, root string, storage ComputerStorageReference, name, wantReason string) {
	t.Helper()
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
	barrierReceipt, ok := barrier.SweepReceipt()
	if !ok || barrierReceipt.ComputerStorageDeferredCount != 0 || barrierReceipt.ComputerStorageQuarantinedCount != 1 ||
		len(barrierReceipt.VerifiedRetained.ComputerStorageQuarantined) != 1 {
		t.Fatalf("node recovery receipt = %+v, ok=%t", barrierReceipt, ok)
	}
	entries, err := os.ReadDir(filepath.Join(root, "computer-disk-quarantine"))
	if err != nil || len(entries) != 1 || !strings.HasPrefix(entries[0].Name(), name+"-anomaly-") {
		t.Fatalf("quarantine directory = %v err=%v", entries, err)
	}
	receipt, err := readAndValidateComputerDiskQuarantineReceipt(filepath.Join(root, "computer-disk-quarantine", entries[0].Name(), "quarantine.json"))
	if err != nil || receipt.Reason != wantReason {
		t.Fatalf("typed quarantine receipt = %+v err=%v, want reason %q", receipt, err, wantReason)
	}
	quarantined, err := computerDiskQuarantined(root, storage)
	if err != nil || !quarantined {
		t.Fatalf("quarantined generation remained admissible: quarantined=%t err=%v receipt=%+v", quarantined, err, receipt)
	}
}
