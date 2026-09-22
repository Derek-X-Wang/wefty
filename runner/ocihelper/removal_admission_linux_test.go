//go:build linux

package ocihelper

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// expiredContext makes admission contention deterministic: the waiter's own
// deadline is already in the past, so the first failed TryLock refuses.
func expiredContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	t.Cleanup(cancel)
	return ctx
}

func detachedRemovalAuthority() ManagedVolumeRemovalAuthority {
	return ManagedVolumeRemovalAuthority{NodeID: "node-1", BootSessionID: "boot-a", JobID: "removal-job",
		PriorJobID: "job-1", RemovalGeneration: 1, CleanupFence: "removal-fence"}
}

// The payload of a fully allocated generation comes off under the generation
// flock alone. Holding Node-wide admission across that removal is what stalled
// attach, start, and reimage preflight for every other Computer on the Node.
func TestDeletionDropsAdmissionWhilePayloadComesOffAndRetakesItForTheProof(t *testing.T) {
	root, fake, storage, _ := prepareDetachedBackupSource(t)
	system := &removalAdmissionDiskSystem{fakeComputerDiskSystem: fake}
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system}
	name, _ := deterministicComputerDiskName(storage)
	diskRoot := filepath.Join(root, "computer-disks", name)
	imagePath := filepath.Join(diskRoot, "disk.ext4")

	requireAdmissionFree := func(stage string) {
		if !engine.computerReimageMu.TryLock() {
			t.Errorf("%s held attachment admission", stage)
		} else {
			engine.computerReimageMu.Unlock()
		}
		if !engine.computerStorageRootMu.TryLock() {
			t.Errorf("%s held root admission", stage)
		} else {
			engine.computerStorageRootMu.Unlock()
		}
		// Exclusion has not been given up: the root and the lock inode inside
		// it are still there, so no creator can take a replacement.
		lock, err := openComputerDiskLock(diskRoot)
		if lock != nil {
			closeComputerDiskLock(lock)
		}
		if !errors.Is(err, errComputerStorageAttachmentOwned) {
			t.Errorf("%s gave up the generation flock: %v", stage, err)
		}
	}

	inspected := false
	system.inspect = func() {
		inspected = true
		requireAdmissionFree("mount check before payload removal")
		if _, err := os.Lstat(imagePath); err != nil {
			t.Errorf("image was already gone before the payload phase: %v", err)
		}
	}
	stages := map[computerDiskCheckpoint]bool{}
	engine.computerDiskHook = func(checkpoint computerDiskCheckpoint) error {
		stages[checkpoint] = true
		switch checkpoint {
		case computerDiskPayloadRemoved:
			requireAdmissionFree("payload removal")
			if _, err := os.Lstat(imagePath); !errors.Is(err, os.ErrNotExist) {
				t.Errorf("payload phase kept the image: %v", err)
			}
			if _, err := os.Lstat(diskRoot); err != nil {
				t.Errorf("payload phase unlinked the root that carries the flock: %v", err)
			}
		case computerDiskRootRemoved, computerDiskRemovalAbsent:
			if engine.computerReimageMu.TryLock() {
				engine.computerReimageMu.Unlock()
				t.Errorf("%s released attachment admission", checkpoint)
			}
			if engine.computerStorageRootMu.TryLock() {
				engine.computerStorageRootMu.Unlock()
				t.Errorf("%s released root admission", checkpoint)
			}
		}
		return nil
	}
	if err := engine.deleteComputerDisk(storage, detachedRemovalAuthority()); err != nil {
		t.Fatal(err)
	}
	if !inspected {
		t.Fatal("deletion did not reach the pre-payload filesystem checks")
	}
	for _, checkpoint := range []computerDiskCheckpoint{computerDiskPayloadRemoved, computerDiskRootRemoved, computerDiskRemovalAbsent} {
		if !stages[checkpoint] {
			t.Errorf("deletion never reached %s", checkpoint)
		}
	}
	if _, err := os.Lstat(diskRoot); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("root survived removal: %v", err)
	}
}

// A creator whose own deadline expires waiting for admission gets a typed
// contention refusal it can replay, not an opaque engine failure.
func TestCloneWaitingOnRootAdmissionRefusesTyped(t *testing.T) {
	root, system, source := publishedStorageCopySource(t)
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system,
		storageCopyFinalize: fakeCloneFinalize(t)}
	clone := storageCopyTestRequest(source, "clone", source.Receipt.AllocatedSize)
	_, storage, _ := prepareDetachedSourceStorage(t, root, system)
	waited := false
	engine.computerDiskHook = func(checkpoint computerDiskCheckpoint) error {
		if checkpoint != computerDiskRootRemoved {
			return nil
		}
		waited = true
		_, err := engine.CopyComputerStorage(expiredContext(t), clone)
		var contended *computerStorageAdmissionContendedError
		if !errors.As(err, &contended) {
			t.Errorf("clone waiting on root admission = %v, want a typed contention refusal", err)
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("typed contention refusal lost its cause: %v", err)
		}
		return nil
	}
	if err := engine.deleteComputerDisk(storage, detachedRemovalAuthority()); err != nil {
		t.Fatal(err)
	}
	if !waited {
		t.Fatal("deletion never held admission across the unlink")
	}
}

// prepareDetachedSourceStorage republishes the detached fixture generation in
// an existing runtime root so one test can both clone and delete.
func prepareDetachedSourceStorage(t *testing.T, root string, system *fakeComputerDiskSystem) (*ContainerdEngine, ComputerStorageReference, AttemptAuthority) {
	t.Helper()
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system}
	storage := testComputerStorage()
	storage.ComputerID = "admission-source"
	storage.StorageID = "admission-storage"
	authority := testComputerAuthority("admission-source", "source-fence", "boot-a")
	attachment, err := engine.attachComputerDisk(t.Context(), storage, authority)
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.detachComputerDisk(attachment, computerDiskReapReceipt, ""); err != nil {
		t.Fatal(err)
	}
	return engine, storage, authority
}

// Both lock-free root creators now reach openComputerDiskLock -- which
// MkdirAlls the root -- only through root admission.
func TestLockFreeRootCreatorsTakeRootAdmission(t *testing.T) {
	t.Run("backup source", func(t *testing.T) {
		root, system, storage, authority := prepareDetachedBackupSource(t)
		engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system}
		request := backupTestRequest(storage, authority)
		engine.computerStorageRootMu.Lock()
		defer engine.computerStorageRootMu.Unlock()
		_, release, err := engine.lockDetachedBackupSource(expiredContext(t), request.Storage, request.Authority)
		if release != nil {
			release()
		}
		var contended *computerStorageAdmissionContendedError
		if !errors.As(err, &contended) {
			t.Fatalf("Backup source lock under held root admission = %v, want a typed contention refusal", err)
		}
	})
	t.Run("disk sweep", func(t *testing.T) {
		root, system, storage, _ := prepareDetachedBackupSource(t)
		engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system}
		name, _ := deterministicComputerDiskName(storage)
		engine.computerStorageRootMu.Lock()
		defer engine.computerStorageRootMu.Unlock()
		if err := engine.sweepComputerDisksWithRecoveryAttempt(expiredContext(t), "sweep-epoch", false); err != nil {
			t.Fatal(err)
		}
		if !slices.ContainsFunc(engine.computerDiskSweepEvidence, func(evidence SweepEvidence) bool {
			return evidence.ID == name && evidence.Method == "computer_disk_lock_open"
		}) {
			t.Fatalf("sweep classified a root without taking admission: %+v", engine.computerDiskSweepEvidence)
		}
	})
}

// A root holding nothing but its lock is residue, not bytes without an
// authority manifest: a refused clone's removal, or a removal interrupted
// between dropping the payload and unlinking the root, must never wedge.
func TestAuthorizedRemovalDeletesALockOnlyRoot(t *testing.T) {
	for _, frozen := range []bool{false, true} {
		name := "live"
		if frozen {
			name = "frozen-absence"
		}
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: newFakeComputerDiskSystem()}
			storage := testComputerStorage()
			diskName, _ := deterministicComputerDiskName(storage)
			diskRoot := filepath.Join(root, "computer-disks", diskName)
			if err := os.MkdirAll(diskRoot, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(diskRoot, "attachment.lock"), nil, 0o600); err != nil {
				t.Fatal(err)
			}
			proved := false
			engine.computerDiskHook = func(checkpoint computerDiskCheckpoint) error {
				if checkpoint == computerDiskRemovalAbsent {
					proved = true
				}
				return nil
			}
			if err := engine.deleteComputerDiskWithAbsence(storage, detachedRemovalAuthority(), frozen); err != nil {
				t.Fatalf("authorized removal of a lock-only root: %v", err)
			}
			if !proved {
				t.Fatal("removal returned without proving absence")
			}
			if _, err := os.Lstat(diskRoot); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("lock-only root survived removal: %v", err)
			}
		})
	}
}

// The tombstone binds to the Computer Storage identity, the Node, and the
// managed-root instance. `current_job_id` rotates on reconfiguration, so the
// refusing Job may be the removal's prior Job as easily as its current one.
func TestRefusedTombstoneInventoryAcceptsThePriorJob(t *testing.T) {
	root, system, source := publishedStorageCopySource(t)
	request := storageCopyTestRequest(source, "clone", source.Receipt.AllocatedSize)
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system,
		computerBackupAllocate: func(string, int64) error { return unix.ENOSPC }}
	response, err := engine.CopyComputerStorage(t.Context(), request)
	if err != nil || !response.Receipt.DestinationAbsent {
		t.Fatalf("clone refusal = %+v err=%v", response, err)
	}
	storage := request.Destination
	rotated := InventoryRemovalRequest{RootInstanceID: request.Authority.RootInstanceID, ComputerStorage: &storage,
		Removal: ManagedVolumeRemovalAuthority{NodeID: request.Authority.NodeID, BootSessionID: "removal-boot",
			JobID: "rotated-current-job", PriorJobID: request.Authority.JobID, RemovalGeneration: 2,
			CleanupFence: "remove-refused-clone"}}
	inventory, err := engine.InventoryRemoval(t.Context(), rotated)
	if err != nil || len(inventory.Attempts) != 1 || !inventory.Attempts[0].StorageAbsent {
		t.Fatalf("rotated current-job inventory = %+v err=%v", inventory, err)
	}
	for _, field := range []string{"node", "root", "job"} {
		wrong := rotated
		switch field {
		case "node":
			wrong.Removal.NodeID = "other-node"
		case "root":
			wrong.RootInstanceID = "other-root"
		case "job":
			wrong.Removal.PriorJobID = "another-prior-job"
		}
		if _, err := engine.InventoryRemoval(t.Context(), wrong); err == nil {
			t.Errorf("tombstone inventory accepted conflicting %s authority", field)
		}
	}
	// A tombstone recording a different Computer is not this generation's own.
	name, _ := deterministicComputerDiskName(storage)
	tombstone := filepath.Join(root, "computer-disks", name, "copy-refused.json")
	payload, err := os.ReadFile(tombstone)
	if err != nil {
		t.Fatal(err)
	}
	var record map[string]any
	if err := json.Unmarshal(payload, &record); err != nil {
		t.Fatal(err)
	}
	foreign := storage
	foreign.ComputerID = "another-computer"
	record["storage"] = foreign
	if payload, err = json.Marshal(record); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(tombstone, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.InventoryRemoval(t.Context(), rotated); err == nil {
		t.Error("tombstone inventory accepted another Computer's refusal")
	}
}

// A retained refusal tombstone is not a Storage generation, so it must not
// defer the custody write-record sweep for every Computer on the Node.
func TestTombstonedRootDoesNotDeferTheCustodyWriteRecordSweep(t *testing.T) {
	root, system, source := publishedStorageCopySource(t)
	mountRoot, externalRoot := custodyOperatorMountRoot(t)
	engine := &ContainerdEngine{config: custodyEngineConfig(root, mountRoot), diskSystem: system}
	exportRequest := custodyExportTestRequest(source, externalRoot)
	exported, err := engine.ExportComputerCustody(t.Context(), exportRequest)
	if err != nil || exported.Receipt.Kind != "computer_custody_export_verified" {
		t.Fatalf("Custody export = %+v err=%v", exported, err)
	}
	if _, recorded, err := engine.custodyWritePhase(exportRequest); err != nil || !recorded {
		t.Fatalf("export left no write record: recorded=%t err=%v", recorded, err)
	}
	// A refused clone of a different Computer keeps its lock and tombstone on
	// the node after the source generation is removed.
	refusing := &ContainerdEngine{config: custodyEngineConfig(root, mountRoot), diskSystem: system,
		computerBackupAllocate: func(string, int64) error { return unix.ENOSPC }}
	copyRequest := storageCopyTestRequest(source, "clone", source.Receipt.AllocatedSize)
	refusal, err := refusing.CopyComputerStorage(t.Context(), copyRequest)
	if err != nil || !refusal.Receipt.DestinationAbsent {
		t.Fatalf("clone refusal = %+v err=%v", refusal, err)
	}
	refusedName, _ := deterministicComputerDiskName(copyRequest.Destination)
	if err := engine.deleteComputerDisk(exportRequest.Storage, detachedRemovalAuthority()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(root, "computer-disks", refusedName, "copy-refused.json")); err != nil {
		t.Fatalf("the refusal tombstone was expected to outlive the removal: %v", err)
	}
	if _, recorded, err := engine.custodyWritePhase(exportRequest); err != nil || recorded {
		t.Fatalf("a retained refusal tombstone deferred the custody sweep: recorded=%t err=%v", recorded, err)
	}
}
