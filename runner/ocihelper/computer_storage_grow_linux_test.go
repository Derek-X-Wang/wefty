//go:build linux

package ocihelper

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"golang.org/x/sys/unix"
)

func TestStartupAbandonsBoundedGrowDeferralIntoTypedQuarantine(t *testing.T) {
	root := t.TempDir()
	request := growTestRequest(16 << 20)
	imagePath := prepareGrowTestImage(t, root, request)
	if err := fullyAllocateComputerDisk(imagePath, request.NewDiskBytes); err != nil {
		t.Fatal(err)
	}
	if err := writeComputerStorageGrowIntent(filepath.Dir(imagePath), request); err != nil {
		t.Fatal(err)
	}
	clock := newManualClock(time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC))
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root, Clock: clock}, diskSystem: newFakeComputerDiskSystem(),
		computerGrowPreen: func(context.Context, string) error { return errors.New("transient") }}
	for sweep := 0; sweep < 10; sweep++ {
		if err := engine.sweepComputerDisksWithRecoveryAttempt(t.Context(), "session-reap", false); err != nil {
			t.Fatal(err)
		}
	}
	intent, present, err := readComputerStorageGrowIntent(filepath.Dir(imagePath))
	if err != nil || !present || intent.Recovery.Attempts != 0 {
		t.Fatalf("in-session reap consumed recovery attempts: intent=%+v present=%t err=%v", intent, present, err)
	}
	for sweep := 0; sweep < 100; sweep++ {
		if err := engine.sweepComputerDisks(t.Context(), "rapid-start"); err != nil {
			t.Fatal(err)
		}
	}
	intent, present, err = readComputerStorageGrowIntent(filepath.Dir(imagePath))
	if err != nil || !present || intent.Recovery.Attempts != 100 || !intent.Recovery.FirstDeferredAt.Equal(clock.Now()) {
		t.Fatalf("rapid recovery deferral = %+v present=%t err=%v", intent, present, err)
	}
	if _, err := os.Stat(filepath.Dir(imagePath)); err != nil {
		t.Fatalf("100 rapid sweeps abandoned recovery before wall-clock floor: %v", err)
	}
	clock.Advance(defaultComputerDiskQuarantineRetention)
	if err := engine.sweepComputerDisks(t.Context(), "elapsed-start"); err != nil {
		t.Fatal(err)
	}
	name, _ := deterministicComputerDiskName(request.Storage)
	if !slices.ContainsFunc(engine.computerDiskSweepEvidence, func(item SweepEvidence) bool {
		return item.ID == name && item.Action == SweepActionQuarantined && strings.HasPrefix(item.Method, "resume_abandoned:")
	}) {
		t.Fatalf("abandon evidence = %+v", engine.computerDiskSweepEvidence)
	}
	inventory := ResourceInventory{}
	if err := engine.inventoryComputerDiskResources(&inventory); err != nil || !slices.ContainsFunc(inventory.ComputerStorageQuarantined, func(entry ComputerStorageRecoveryInventoryEntry) bool {
		return entry.Storage == request.Storage && entry.Reason == "resume_abandoned" && entry.DeferredReason == "operational_failure" &&
			entry.Attempts == 101 && entry.FirstDeferredAt.Equal(time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC))
	}) {
		t.Fatalf("abandoned inventory = %+v err=%v", inventory, err)
	}
}

func TestStartupResumesDurableComputerGrowBeforeInventoryVerification(t *testing.T) {
	root := t.TempDir()
	request := growTestRequest(16 << 20)
	imagePath := prepareGrowTestImage(t, root, request)
	diskRoot := filepath.Dir(imagePath)
	if err := writeComputerStorageGrowIntent(diskRoot, request); err != nil {
		t.Fatal(err)
	}
	if err := fullyAllocateComputerDisk(imagePath, request.NewDiskBytes); err != nil {
		t.Fatal(err)
	}

	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: newFakeComputerDiskSystem(),
		capacityReservations: make(map[string]*capacityReservation), attempts: make(map[string]*containerdAttempt),
		computerGrowResize:          func(context.Context, string, string, int64, int64) error { return nil },
		computerGrowFilesystemBytes: func(context.Context, string) (int64, error) { return request.NewDiskBytes, nil },
		computerGrowPreen:           func(context.Context, string) error { return nil },
	}
	if err := engine.sweepComputerDisks(t.Context(), "startup-sweep"); err != nil {
		t.Fatal(err)
	}
	evidence := engine.computerDiskSweepEvidence
	name, _ := deterministicComputerDiskName(request.Storage)
	manifest, present, err := readComputerDiskManifest(filepath.Join(root, "computer-disks", name, "attachment.json"))
	if err != nil || !present || manifest.Storage.DiskBytes != request.NewDiskBytes {
		t.Fatalf("recovered grow manifest = %+v present=%t err=%v", manifest, present, err)
	}
	inventory := ResourceInventory{}
	if err := engine.inventoryComputerDiskResources(&inventory); err != nil {
		t.Fatal(err)
	}
	if len(inventory.ComputerDiskAnomalies) != 0 || !slices.Contains(inventory.ComputerDiskAllocations, name) {
		t.Fatalf("recovered grow inventory = %+v", inventory)
	}
	if !slices.ContainsFunc(evidence, func(item SweepEvidence) bool {
		return item.ID == name && item.Action == SweepActionResumed && item.Method == "computer_storage_grow"
	}) {
		t.Fatalf("recovered grow evidence = %+v", evidence)
	}
}

func TestStartupDefersTransientGrowRecoveryAndNextSweepCompletes(t *testing.T) {
	root := t.TempDir()
	request := growTestRequest(16 << 20)
	imagePath := prepareGrowTestImage(t, root, request)
	diskRoot := filepath.Dir(imagePath)
	if err := writeComputerStorageGrowIntent(diskRoot, request); err != nil {
		t.Fatal(err)
	}
	if err := fullyAllocateComputerDisk(imagePath, request.NewDiskBytes); err != nil {
		t.Fatal(err)
	}
	transient := errors.New("transient e2fsck failure")
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: newFakeComputerDiskSystem(),
		capacityReservations: make(map[string]*capacityReservation), attempts: make(map[string]*containerdAttempt),
		computerGrowPreen:           func(context.Context, string) error { return transient },
		computerGrowResize:          func(context.Context, string, string, int64, int64) error { return nil },
		computerGrowFilesystemBytes: func(context.Context, string) (int64, error) { return request.NewDiskBytes, nil },
	}
	if err := engine.sweepComputerDisks(t.Context(), "deferred"); err != nil {
		t.Fatal(err)
	}
	name, _ := deterministicComputerDiskName(request.Storage)
	if _, err := os.Stat(diskRoot); err != nil {
		t.Fatalf("deferred generation moved: %v", err)
	}
	if _, present, err := readComputerStorageGrowIntent(diskRoot); err != nil || !present {
		t.Fatalf("deferred grow record present=%t err=%v", present, err)
	}
	if !slices.ContainsFunc(engine.computerDiskSweepEvidence, func(item SweepEvidence) bool {
		return item.ID == name && item.Action == SweepActionResumeDeferred
	}) {
		t.Fatalf("deferred evidence = %+v", engine.computerDiskSweepEvidence)
	}
	if _, err := engine.attachComputerDisk(t.Context(), request.Storage, testComputerAuthority("deferred", "fence", "boot")); !errors.As(err, new(*ComputerStorageResumeDeferredError)) {
		t.Fatalf("deferred generation was attachable: %v", err)
	}
	inventory := ResourceInventory{}
	if err := engine.inventoryComputerDiskResources(&inventory); err != nil || len(inventory.ComputerDiskAnomalies) != 0 {
		t.Fatalf("deferred namespace inventory = %+v err=%v", inventory, err)
	}
	engine.computerGrowPreen = func(context.Context, string) error { return nil }
	if err := engine.sweepComputerDisks(t.Context(), "retry"); err != nil {
		t.Fatal(err)
	}
	manifest, present, err := readComputerDiskManifest(filepath.Join(diskRoot, "attachment.json"))
	if err != nil || !present || manifest.Storage.DiskBytes != request.NewDiskBytes {
		t.Fatalf("retried grow = %+v present=%t err=%v", manifest, present, err)
	}
}

func TestStartupContextExpiryDefersGrowWithoutQuarantine(t *testing.T) {
	root := t.TempDir()
	request := growTestRequest(16 << 20)
	imagePath := prepareGrowTestImage(t, root, request)
	diskRoot := filepath.Dir(imagePath)
	if err := writeComputerStorageGrowIntent(diskRoot, request); err != nil {
		t.Fatal(err)
	}
	if err := fullyAllocateComputerDisk(imagePath, request.NewDiskBytes); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: newFakeComputerDiskSystem(),
		capacityReservations: make(map[string]*capacityReservation), attempts: make(map[string]*containerdAttempt),
		computerGrowPreen: func(ctx context.Context, _ string) error { return ctx.Err() },
	}
	if err := engine.sweepComputerDisks(ctx, "expired"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(diskRoot); err != nil {
		t.Fatalf("expired recovery moved generation: %v", err)
	}
	entries, err := readDirectoryIfPresent(filepath.Join(root, "computer-disk-quarantine"))
	if err != nil || len(entries) != 0 {
		t.Fatalf("expired recovery quarantine=%v err=%v", entries, err)
	}
}

func TestStartupStructuralGrowImageLossQuarantinesImmediately(t *testing.T) {
	root := t.TempDir()
	request := growTestRequest(16 << 20)
	imagePath := prepareGrowTestImage(t, root, request)
	if err := writeComputerStorageGrowIntent(filepath.Dir(imagePath), request); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(imagePath); err != nil {
		t.Fatal(err)
	}
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: newFakeComputerDiskSystem()}
	if err := engine.sweepComputerDisks(t.Context(), "structural"); err != nil {
		t.Fatal(err)
	}
	name, _ := deterministicComputerDiskName(request.Storage)
	if !slices.ContainsFunc(engine.computerDiskSweepEvidence, func(item SweepEvidence) bool {
		return item.ID == name && item.Action == SweepActionQuarantined && item.Method == "image_missing"
	}) {
		t.Fatalf("structural recovery evidence = %+v", engine.computerDiskSweepEvidence)
	}
}

func TestStartupPreenCorrectionEmitsTypedSweepEvidence(t *testing.T) {
	root := t.TempDir()
	request := growTestRequest(16 << 20)
	imagePath := prepareGrowTestImage(t, root, request)
	if err := writeComputerStorageGrowIntent(filepath.Dir(imagePath), request); err != nil {
		t.Fatal(err)
	}
	if err := fullyAllocateComputerDisk(imagePath, request.NewDiskBytes); err != nil {
		t.Fatal(err)
	}
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: newFakeComputerDiskSystem(),
		computerGrowPreen:           func(context.Context, string) error { return errComputerStoragePreenCorrected },
		computerGrowResize:          func(context.Context, string, string, int64, int64) error { return nil },
		computerGrowFilesystemBytes: func(context.Context, string) (int64, error) { return request.NewDiskBytes, nil }}
	if err := engine.sweepComputerDisks(t.Context(), "preen"); err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(engine.computerDiskSweepEvidence, func(item SweepEvidence) bool {
		return item.Action == SweepActionPreenCorrected && item.Method == "e2fsck_exit_1"
	}) {
		t.Fatalf("preen correction evidence = %+v", engine.computerDiskSweepEvidence)
	}
}

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

func prepareGrowTestImage(t *testing.T, root string, request GrowComputerStorageRequest) string {
	t.Helper()
	name, err := deterministicComputerDiskName(request.Storage)
	if err != nil {
		t.Fatal(err)
	}
	diskRoot := filepath.Join(root, "computer-disks", name)
	if err := os.MkdirAll(diskRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	imagePath := filepath.Join(diskRoot, "disk.ext4")
	file, err := os.OpenFile(imagePath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := fullyAllocateComputerDisk(imagePath, request.Storage.DiskBytes); err != nil {
		t.Fatal(err)
	}
	manifest := computerDiskManifest{Version: computerDiskManifestVersion, Storage: request.Storage,
		DiskImage: "disk.ext4", MountDirectory: name, Prepared: true}
	if err := writeComputerDiskManifest(diskRoot, manifest); err != nil {
		t.Fatal(err)
	}
	return imagePath
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
	prepareGrowTestImage(t, root, request)
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
			prepareGrowTestImage(t, root, request)
			resizeRuns := 0
			engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root},
				capacityReservations: make(map[string]*capacityReservation), attempts: make(map[string]*containerdAttempt),
				computerGrowResize: func(_ context.Context, path, _ string, _, newBytes int64) error {
					resizeRuns++
					return fullyAllocateComputerDisk(path, newBytes)
				},
				computerGrowFilesystemBytes: func(context.Context, string) (int64, error) { return request.NewDiskBytes, nil },
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
			if resizeRuns < 1 || resizeRuns > 2 {
				t.Fatalf("resumed %s resize runs = %d", checkpoint, resizeRuns)
			}
			reservation := engine.capacityReservations[request.Authority.JobID]
			if reservation == nil || reservation.diskBytes != request.NewDiskBytes || !reservation.diskMaterialized {
				t.Fatalf("resumed %s reservation = %#v", checkpoint, reservation)
			}
		})
	}
}

func TestComputerGrowRefusesMissingCurrentImage(t *testing.T) {
	root := t.TempDir()
	request := growTestRequest(16 << 20)
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root},
		capacityReservations: make(map[string]*capacityReservation), attempts: make(map[string]*containerdAttempt)}
	if response, err := engine.GrowComputerStorage(t.Context(), request); err == nil || response.Receipt.Applied {
		t.Fatalf("missing-image grow = %#v err=%v", response, err)
	}
	if len(engine.capacityReservations) != 0 {
		t.Fatalf("missing-image grow reserved capacity: %#v", engine.capacityReservations)
	}
}

func TestComputerGrowRefusesMissingManifestWithDurablePreparationEvidence(t *testing.T) {
	for _, test := range []struct {
		name    string
		prepare func(*testing.T, string, GrowComputerStorageRequest)
	}{
		{name: "copy receipt", prepare: func(t *testing.T, root string, request GrowComputerStorageRequest) {
			receipt := ComputerStorageCopyReceipt{
				Kind: contract.ComputerStorageCopyVerifiedKind, ReceiptID: "copy-receipt",
				DestinationComputerID: request.Storage.ComputerID, DestinationStorageID: request.Storage.StorageID,
				DestinationGeneration: request.Storage.StorageGeneration, NodeID: "node", RootInstanceID: "root",
				JobID: "job", OperationRevision: 7, CleanupFence: "copy", HelperGeneration: 1,
			}
			if err := writeComputerStorageCopyManifest(root, computerStorageCopyManifest{Version: 1, Phase: computerStorageCopyPublished, Receipt: &receipt}); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "reset receipt", prepare: func(t *testing.T, root string, request GrowComputerStorageRequest) {
			witness := contract.ComputerStoragePreparationWitness{
				Kind: contract.ComputerStorageResetVerifiedKind, ReceiptID: "reset-receipt", NodeID: "node", RootInstanceID: "root",
				JobID: "job", ComputerID: request.Storage.ComputerID, StorageID: request.Storage.StorageID,
				StorageGeneration: request.Storage.StorageGeneration, Revision: 7, Fence: "reset", HelperGeneration: 1,
			}
			if err := persistComputerStoragePreparationWitness(root, witness); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			request := growTestRequest(16 << 20)
			imagePath := prepareGrowTestImage(t, root, request)
			diskRoot := filepath.Dir(imagePath)
			test.prepare(t, diskRoot, request)
			if err := os.Remove(filepath.Join(diskRoot, "attachment.json")); err != nil {
				t.Fatal(err)
			}
			engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root},
				capacityReservations: make(map[string]*capacityReservation), attempts: make(map[string]*containerdAttempt)}
			response, err := engine.GrowComputerStorage(t.Context(), request)
			var uncertain *ComputerStorageGrowUncertainError
			if !errors.As(err, &uncertain) || response.Receipt.Applied {
				t.Fatalf("missing manifest with %s = %#v err=%v", test.name, response, err)
			}
			if len(engine.capacityReservations) != 0 {
				t.Fatalf("missing manifest with %s reserved capacity: %#v", test.name, engine.capacityReservations)
			}
			if info, statErr := os.Stat(imagePath); statErr != nil || info.Size() != request.Storage.DiskBytes {
				t.Fatalf("missing manifest with %s mutated image: info=%v err=%v", test.name, info, statErr)
			}
		})
	}
}

func TestComputerGrowNoopResizeFailsReadback(t *testing.T) {
	root := t.TempDir()
	request := growTestRequest(16 << 20)
	imagePath := prepareGrowTestImage(t, root, request)
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root},
		capacityReservations: make(map[string]*capacityReservation), attempts: make(map[string]*containerdAttempt),
		computerGrowResize:          func(context.Context, string, string, int64, int64) error { return nil },
		computerGrowFilesystemBytes: func(context.Context, string) (int64, error) { return request.Storage.DiskBytes, nil },
	}
	if response, err := engine.GrowComputerStorage(t.Context(), request); err == nil || response.Receipt.Applied {
		t.Fatalf("no-op resize = %#v err=%v", response, err)
	}
	if info, err := os.Stat(imagePath); err != nil || info.Size() != request.Storage.DiskBytes {
		t.Fatalf("rolled-back image size = %v err=%v", info, err)
	}
}

func TestComputerGrowFailureRemovesIntentSoSmallerGrowCanSucceed(t *testing.T) {
	root := t.TempDir()
	request := growTestRequest(16 << 20)
	imagePath := prepareGrowTestImage(t, root, request)
	resizeRuns := 0
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, capacityReservations: make(map[string]*capacityReservation), attempts: make(map[string]*containerdAttempt),
		computerGrowResize: func(_ context.Context, path, _ string, _, newBytes int64) error {
			resizeRuns++
			if resizeRuns == 1 {
				return unix.ENOSPC
			}
			return fullyAllocateComputerDisk(path, newBytes)
		},
		computerGrowFilesystemBytes: func(context.Context, string) (int64, error) { return 12 << 20, nil },
	}
	if response, err := engine.GrowComputerStorage(t.Context(), request); err != nil || response.Receipt.FailureCode != "insufficient_disk" {
		t.Fatalf("first grow = %+v err=%v", response, err)
	}
	if _, present, err := readComputerStorageGrowIntent(filepath.Dir(imagePath)); err != nil || present {
		t.Fatalf("failed grow intent present=%t err=%v", present, err)
	}
	request.NewDiskBytes = 12 << 20
	request.Storage.IntentRevision = 8
	request.Authority.OperationRevision = 8
	request.Authority.OperationFence = "fence-2"
	response, err := engine.GrowComputerStorage(t.Context(), request)
	if err != nil || !response.Receipt.Applied {
		t.Fatalf("smaller retry grow = %+v err=%v", response, err)
	}
}

func TestComputerGrowReassertsAllocationAfterFilesystemResize(t *testing.T) {
	root := t.TempDir()
	request := growTestRequest(16 << 20)
	imagePath := prepareGrowTestImage(t, root, request)
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root},
		capacityReservations: make(map[string]*capacityReservation), attempts: make(map[string]*containerdAttempt),
		computerGrowResize: func(_ context.Context, path, _ string, _, newBytes int64) error {
			if err := fullyAllocateComputerDisk(path, newBytes); err != nil {
				return err
			}
			file, err := os.OpenFile(path, os.O_RDWR, 0)
			if err != nil {
				return err
			}
			err = unix.Fallocate(int(file.Fd()), unix.FALLOC_FL_PUNCH_HOLE|unix.FALLOC_FL_KEEP_SIZE, 0, 1<<20)
			return errors.Join(err, file.Close())
		},
		computerGrowFilesystemBytes: func(context.Context, string) (int64, error) { return request.NewDiskBytes, nil },
	}
	response, err := engine.GrowComputerStorage(t.Context(), request)
	if err != nil || !response.Receipt.Applied {
		t.Fatalf("grow after filesystem sparse write = %#v err=%v", response, err)
	}
	if err := verifyComputerDiskAllocation(imagePath, request.NewDiskBytes); err != nil {
		t.Fatalf("grown image allocation = %v", err)
	}
}

func TestComputerGrowENOSPCRollsBackDeltaAndReturnsReceipt(t *testing.T) {
	root := t.TempDir()
	request := growTestRequest(16 << 20)
	imagePath := prepareGrowTestImage(t, root, request)
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root},
		capacityReservations: make(map[string]*capacityReservation), attempts: make(map[string]*containerdAttempt),
		computerGrowResize: func(_ context.Context, path, _ string, _, newBytes int64) error {
			if err := os.Truncate(path, newBytes); err != nil {
				return err
			}
			return unix.ENOSPC
		},
	}
	response, err := engine.GrowComputerStorage(t.Context(), request)
	if err != nil || response.Receipt.Applied || response.Receipt.FailureCode != "insufficient_disk" {
		t.Fatalf("ENOSPC grow = %#v err=%v", response, err)
	}
	if info, err := os.Stat(imagePath); err != nil || info.Size() != request.Storage.DiskBytes {
		t.Fatalf("ENOSPC image size = %v err=%v", info, err)
	}
	reservation := engine.capacityReservations[request.Authority.JobID]
	if reservation == nil || reservation.diskBytes != request.Storage.DiskBytes || reservation.pendingDiskBytes != 0 {
		t.Fatalf("ENOSPC reservation = %#v", reservation)
	}
}

func TestComputerGrowPostExpansionAllocationFailureIsUncertainAndNeverTruncates(t *testing.T) {
	root := t.TempDir()
	request := growTestRequest(16 << 20)
	imagePath := prepareGrowTestImage(t, root, request)
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root},
		capacityReservations: make(map[string]*capacityReservation), attempts: make(map[string]*containerdAttempt),
		computerGrowResize: func(_ context.Context, path, _ string, _, newBytes int64) error {
			return os.Truncate(path, newBytes)
		},
		computerGrowFilesystemBytes: func(context.Context, string) (int64, error) { return request.NewDiskBytes, nil },
		computerGrowAllocate:        func(string, int64) error { return unix.ENOSPC },
	}
	response, err := engine.GrowComputerStorage(t.Context(), request)
	var uncertain *ComputerStorageGrowUncertainError
	if !errors.As(err, &uncertain) || response.Receipt.Kind == "computer_storage_grow_failed_unchanged" {
		t.Fatalf("post-expansion allocation failure = %#v err=%v, want uncertain outcome", response, err)
	}
	if info, statErr := os.Stat(imagePath); statErr != nil || info.Size() != request.NewDiskBytes {
		t.Fatalf("post-expansion image was truncated: size=%v err=%v", info, statErr)
	}
	reservation := engine.capacityReservations[request.Authority.JobID]
	if reservation == nil || reservation.diskBytes != request.Storage.DiskBytes ||
		reservation.pendingDiskBytes != request.NewDiskBytes-request.Storage.DiskBytes {
		t.Fatalf("uncertain grow lost resumable reservation: %#v", reservation)
	}
	response, err = engine.GrowComputerStorage(t.Context(), request)
	if !errors.As(err, &uncertain) || response.Receipt.Kind == "computer_storage_grow_failed_unchanged" {
		t.Fatalf("expanded-image uncertain retry = %#v err=%v, want uncertain outcome", response, err)
	}
	reservation = engine.capacityReservations[request.Authority.JobID]
	if reservation == nil || reservation.diskBytes != request.Storage.DiskBytes ||
		reservation.pendingDiskBytes != request.NewDiskBytes-request.Storage.DiskBytes {
		t.Fatalf("expanded-image uncertain retry released held delta: %#v", reservation)
	}
	engine.computerGrowAllocate = nil
	response, err = engine.GrowComputerStorage(t.Context(), request)
	if err != nil || !response.Receipt.Applied || response.Receipt.Kind != "computer_storage_grow_applied" {
		t.Fatalf("uncertain grow retry = %#v err=%v", response, err)
	}
}

func TestComputerGrowHeldDeltaChargesOnlyNewcomer(t *testing.T) {
	root := t.TempDir()
	available, err := filesystemAvailableBytes(root)
	if err != nil {
		t.Fatal(err)
	}
	first := growTestRequest(firstSafeGrowBytes(available))
	engine := &ContainerdEngine{capacityReservations: make(map[string]*capacityReservation)}
	if _, admitted, err := engine.reserveGrowCapacity(first, root, first.Storage.DiskBytes); err != nil || !admitted {
		t.Fatalf("first held grow admitted=%t err=%v", admitted, err)
	}
	second := growTestRequest(first.Storage.DiskBytes + available/2 + 1)
	second.Authority.JobID = "job-2"
	if _, admitted, err := engine.reserveGrowCapacity(second, root, second.Storage.DiskBytes); err != nil || admitted {
		t.Fatalf("concurrent newcomer admitted=%t err=%v reservations=%#v", admitted, err, engine.capacityReservations)
	}
}

func firstSafeGrowBytes(available int64) int64 {
	return (8 << 20) + available/2
}
