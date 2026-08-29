//go:build linux

package ocihelper

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"
)

type fakeComputerDiskSystem struct {
	mu             sync.Mutex
	allocationErr  error
	allocationRuns int
	mounts         map[string]string
	loops          map[string]string
	nextLoop       int
}

func newFakeComputerDiskSystem() *fakeComputerDiskSystem {
	return &fakeComputerDiskSystem{mounts: make(map[string]string), loops: make(map[string]string)}
}

func (system *fakeComputerDiskSystem) allocateAndFormat(_ context.Context, path string, bytes int64) error {
	system.mu.Lock()
	defer system.mu.Unlock()
	system.allocationRuns++
	if system.allocationErr != nil {
		if err := os.Truncate(path, min(bytes, 4096)); err != nil {
			return err
		}
		return system.allocationErr
	}
	if err := os.Truncate(path, bytes); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer file.Close()
	return unix.Fallocate(int(file.Fd()), 0, 0, bytes)
}

func (system *fakeComputerDiskSystem) attachAndMount(_ context.Context, imagePath, mountPath string) (string, error) {
	system.mu.Lock()
	defer system.mu.Unlock()
	system.nextLoop++
	loop := "/dev/loop-test-" + string(rune('0'+system.nextLoop))
	system.mounts[mountPath] = loop
	system.loops[loop] = imagePath
	return loop, nil
}

func (system *fakeComputerDiskSystem) detach(mountPath, loopPath, expectedImagePath string) error {
	system.mu.Lock()
	defer system.mu.Unlock()
	if loopPath != "" && system.loops[loopPath] != expectedImagePath {
		return nil
	}
	if mountPath != "" {
		delete(system.mounts, mountPath)
	}
	if loopPath != "" {
		delete(system.loops, loopPath)
	}
	return nil
}

func (system *fakeComputerDiskSystem) mountedSource(path string) (string, bool, error) {
	system.mu.Lock()
	defer system.mu.Unlock()
	value, ok := system.mounts[path]
	return value, ok, nil
}

func (system *fakeComputerDiskSystem) loopBackingFile(loop string) (string, bool, error) {
	system.mu.Lock()
	defer system.mu.Unlock()
	value, ok := system.loops[loop]
	return value, ok, nil
}

func (system *fakeComputerDiskSystem) loopsForRoot(root string) (map[string]string, error) {
	system.mu.Lock()
	defer system.mu.Unlock()
	result := make(map[string]string)
	for loop, path := range system.loops {
		if strings.HasPrefix(path, root+string(filepath.Separator)) {
			result[loop] = path
		}
	}
	return result, nil
}

func testComputerStorage() ComputerStorageReference {
	return ComputerStorageReference{ComputerID: "computer-1", StorageID: "storage-1", StorageGeneration: 1, IntentRevision: 1, DiskBytes: 16 << 20}
}

func testComputerAuthority(attempt, fence, boot string) AttemptAuthority {
	return AttemptAuthority{NodeID: "node-1", JobID: "job-1", AttemptID: attempt, FencingToken: fence, BootSessionID: boot, Class: "service", RemovalGeneration: "attempt"}
}

func TestComputerDiskENOSPCLeavesNoPublishedResource(t *testing.T) {
	root := t.TempDir()
	system := newFakeComputerDiskSystem()
	system.allocationErr = syscall.ENOSPC
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system}
	if _, err := engine.attachComputerDisk(t.Context(), testComputerStorage(), testComputerAuthority("attempt-a", "fence-a", "boot-a")); !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("forced allocation failure = %v, want ENOSPC", err)
	}
	name, _ := deterministicComputerDiskName(testComputerStorage())
	diskRoot := filepath.Join(root, "computer-disks", name)
	for _, path := range []string{filepath.Join(diskRoot, "disk.ext4"), filepath.Join(diskRoot, "attachment.json")} {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("partial Computer disk resource remains at %s: %v", path, err)
		}
	}
	if staging, err := filepath.Glob(filepath.Join(diskRoot, ".disk.ext4.tmp-*")); err != nil || len(staging) != 0 {
		t.Fatalf("failed allocation staging remains: %v err=%v", staging, err)
	}
	if len(system.mounts) != 0 || len(system.loops) != 0 || system.allocationRuns != 1 {
		t.Fatalf("failed allocation mechanics = runs %d mounts %v loops %v", system.allocationRuns, system.mounts, system.loops)
	}
}

func TestComputerDiskResumesManifestBeforeImageAndImageBeforePhaseCrashes(t *testing.T) {
	for _, checkpoint := range []computerDiskCheckpoint{computerDiskManifestBeforeImage, computerDiskImageBeforePhase} {
		t.Run(string(checkpoint), func(t *testing.T) {
			root := t.TempDir()
			system := newFakeComputerDiskSystem()
			engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system}
			crash := errors.New("injected crash")
			fired := false
			engine.computerDiskHook = func(observed computerDiskCheckpoint) error {
				if observed == checkpoint && !fired {
					fired = true
					return crash
				}
				return nil
			}
			if _, err := engine.attachComputerDisk(t.Context(), testComputerStorage(),
				testComputerAuthority("attempt-a", "fence-a", "boot-a")); !errors.Is(err, crash) {
				t.Fatalf("checkpoint %q error = %v", checkpoint, err)
			}
			name, _ := deterministicComputerDiskName(testComputerStorage())
			diskRoot := filepath.Join(root, "computer-disks", name)
			if _, present, err := readComputerDiskManifest(filepath.Join(diskRoot, "attachment.json")); err != nil || !present {
				t.Fatalf("crash lost authority manifest: present=%t err=%v", present, err)
			}
			engine.computerDiskHook = nil
			attachment, err := engine.attachComputerDisk(t.Context(), testComputerStorage(),
				testComputerAuthority("attempt-a", "fence-a", "boot-a"))
			if err != nil {
				t.Fatalf("resume from %q: %v", checkpoint, err)
			}
			if err := engine.detachComputerDisk(attachment, computerDiskReapReceipt, ""); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestComputerDiskRejectsDisappearedLockWhileManifestAttached(t *testing.T) {
	root := t.TempDir()
	system := newFakeComputerDiskSystem()
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system}
	attachment, err := engine.attachComputerDisk(t.Context(), testComputerStorage(), testComputerAuthority("attempt-a", "fence-a", "boot-a"))
	if err != nil {
		t.Fatal(err)
	}
	_ = unix.Flock(int(attachment.lock.Fd()), unix.LOCK_UN)
	_ = attachment.lock.Close()
	if _, err := engine.attachComputerDisk(t.Context(), testComputerStorage(), testComputerAuthority("attempt-b", "fence-b", "boot-a")); err == nil || !strings.Contains(err.Error(), "remains attached") {
		t.Fatalf("attach after lock disappearance = %v", err)
	}
}

func TestComputerDiskRejectsMismatchedManifestAndStaleGenerationReceipt(t *testing.T) {
	for _, mutate := range []func(*computerDiskManifest){
		func(manifest *computerDiskManifest) { manifest.Storage.StorageID = "foreign-storage" },
		func(manifest *computerDiskManifest) { manifest.PreviousDetachment.StorageGeneration++ },
	} {
		root := t.TempDir()
		system := newFakeComputerDiskSystem()
		engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system}
		attachment, err := engine.attachComputerDisk(t.Context(), testComputerStorage(), testComputerAuthority("attempt-a", "fence-a", "boot-a"))
		if err != nil {
			t.Fatal(err)
		}
		if err := engine.detachComputerDisk(attachment, computerDiskReapReceipt, ""); err != nil {
			t.Fatal(err)
		}
		rootPath := filepath.Dir(attachment.imagePath)
		manifest, _, _ := readComputerDiskManifest(filepath.Join(rootPath, "attachment.json"))
		mutate(&manifest)
		if err := writeComputerDiskManifest(rootPath, manifest); err != nil {
			t.Fatal(err)
		}
		if _, err := engine.attachComputerDisk(t.Context(), testComputerStorage(), testComputerAuthority("attempt-b", "fence-b", "boot-a")); err == nil {
			t.Fatal("mismatched manifest authorized attach")
		}
	}
}

func TestComputerDiskResizeIntentDoesNotChangeGenerationIdentity(t *testing.T) {
	storage := testComputerStorage()
	resized := storage
	resized.DiskBytes *= 2
	name, _ := deterministicComputerDiskName(storage)
	resizedName, _ := deterministicComputerDiskName(resized)
	if name != resizedName || !sameComputerStorageIdentity(storage, resized) {
		t.Fatalf("resize intent changed identity: %q %q", name, resizedName)
	}
	root := t.TempDir()
	system := newFakeComputerDiskSystem()
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system}
	first, err := engine.attachComputerDisk(t.Context(), storage, testComputerAuthority("attempt-a", "fence-a", "boot-a"))
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.detachComputerDisk(first, computerDiskReapReceipt, ""); err != nil {
		t.Fatal(err)
	}
	second, err := engine.attachComputerDisk(t.Context(), resized, testComputerAuthority("attempt-b", "fence-b", "boot-a"))
	if err != nil {
		t.Fatalf("changed size intent bricked existing generation: %v", err)
	}
	manifest, _, err := readComputerDiskManifest(filepath.Join(filepath.Dir(second.imagePath), "attachment.json"))
	if err != nil || manifest.Storage.DiskBytes != storage.DiskBytes {
		t.Fatalf("unimplemented resize changed allocation truth: %+v err=%v", manifest.Storage, err)
	}
	if err := engine.detachComputerDisk(second, computerDiskReapReceipt, ""); err != nil {
		t.Fatal(err)
	}
}

func TestComputerDiskRootOwnershipInitializesOnlyFreshFormat(t *testing.T) {
	mountPath := t.TempDir()
	uid, gid := uint32(os.Getuid()), uint32(os.Getgid())
	if err := initializeComputerDiskRoot(&computerDiskAttachment{mountPath: mountPath, fresh: true}, uid, gid); err != nil {
		t.Fatal(err)
	}
	wrongUID := uid + 1
	if err := initializeComputerDiskRoot(&computerDiskAttachment{mountPath: mountPath, fresh: false}, wrongUID, gid); err == nil {
		t.Fatal("existing Computer disk was silently re-owned")
	}
}

func TestComputerDiskSweepIgnoresRecycledForeignLoopNumber(t *testing.T) {
	root := t.TempDir()
	system := newFakeComputerDiskSystem()
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system, attempts: make(map[string]*containerdAttempt)}
	attachment, err := engine.attachComputerDisk(t.Context(), testComputerStorage(), testComputerAuthority("attempt-a", "fence-a", "boot-a"))
	if err != nil {
		t.Fatal(err)
	}
	_ = unix.Flock(int(attachment.lock.Fd()), unix.LOCK_UN)
	_ = attachment.lock.Close()
	delete(system.mounts, attachment.mountPath)
	system.loops[attachment.loopDevice] = "/var/lib/foreign/disk.ext4"
	if err := engine.sweepComputerDisks("sweep-boot-b"); err != nil {
		t.Fatalf("recycled foreign loop bricked sweep: %v", err)
	}
	if system.loops[attachment.loopDevice] != "/var/lib/foreign/disk.ext4" {
		t.Fatal("sweep detached recycled foreign loop")
	}
}

func TestComputerDiskSweepRecoversCrashBeforeAttachedManifestWrite(t *testing.T) {
	root := t.TempDir()
	system := newFakeComputerDiskSystem()
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system, attempts: make(map[string]*containerdAttempt)}
	storage := testComputerStorage()
	authority := testComputerAuthority("attempt-a", "fence-a", "boot-a")
	name, _ := deterministicComputerDiskName(storage)
	diskRoot := filepath.Join(root, "computer-disks", name)
	if err := os.MkdirAll(diskRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	imagePath := filepath.Join(diskRoot, "disk.ext4")
	if err := os.WriteFile(imagePath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := system.allocateAndFormat(t.Context(), imagePath, storage.DiskBytes); err != nil {
		t.Fatal(err)
	}
	mountPath := filepath.Join(root, "computer-mounts", name)
	if err := os.MkdirAll(mountPath, 0o700); err != nil {
		t.Fatal(err)
	}
	loop, _ := system.attachAndMount(t.Context(), imagePath, mountPath)
	manifest := computerDiskManifest{Version: computerDiskManifestVersion, Storage: storage, DiskImage: "disk.ext4", MountDirectory: name, LoopDevice: loop, Pending: &authority}
	if err := writeComputerDiskManifest(diskRoot, manifest); err != nil {
		t.Fatal(err)
	}
	if err := engine.sweepComputerDisks("sweep-boot-b"); err != nil {
		t.Fatal(err)
	}
	manifest, _, _ = readComputerDiskManifest(filepath.Join(diskRoot, "attachment.json"))
	if manifest.Pending != nil || manifest.PreviousDetachment == nil || manifest.PreviousDetachment.Kind != computerDiskSweepReceipt {
		t.Fatalf("crash recovery manifest = %+v", manifest)
	}
}

func TestComputerDiskSweepDetachesCrashedStorageCopyMount(t *testing.T) {
	root := t.TempDir()
	system := newFakeComputerDiskSystem()
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system, attempts: make(map[string]*containerdAttempt)}
	storage := testComputerStorage()
	name, _ := deterministicComputerDiskName(storage)
	diskRoot := filepath.Join(root, "computer-disks", name)
	if err := os.MkdirAll(diskRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	imagePath := filepath.Join(diskRoot, "disk.ext4.staging")
	if err := os.WriteFile(imagePath, []byte("staging"), 0o600); err != nil {
		t.Fatal(err)
	}
	mountPath := filepath.Join(root, "computer-copy-mounts", name)
	if err := os.MkdirAll(mountPath, 0o700); err != nil {
		t.Fatal(err)
	}
	loop, err := system.attachAndMount(t.Context(), imagePath, mountPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.sweepComputerDisks("copy-sweep"); err != nil {
		t.Fatal(err)
	}
	if _, mounted, err := system.mountedSource(mountPath); err != nil || mounted {
		t.Fatalf("copy mount survived sweep: mounted=%t err=%v", mounted, err)
	}
	if _, present, err := system.loopBackingFile(loop); err != nil || present {
		t.Fatalf("copy loop survived sweep: present=%t err=%v", present, err)
	}
}

func TestComputerDiskRequiresExactConsumedDetachmentEvidence(t *testing.T) {
	root := t.TempDir()
	system := newFakeComputerDiskSystem()
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system}
	storage := testComputerStorage()
	authorityA := testComputerAuthority("attempt-a", "fence-a", "boot-a")
	attachmentA, err := engine.attachComputerDisk(t.Context(), storage, authorityA)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.attachComputerDisk(t.Context(), storage, testComputerAuthority("attempt-b", "fence-b", "boot-a")); err == nil {
		t.Fatal("attempt B attached while A owned the generation")
	}
	staleLock, err := os.OpenFile(filepath.Join(filepath.Dir(attachmentA.imagePath), "attachment.lock"), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	stale := &computerDiskAttachment{name: attachmentA.name, storage: storage, imagePath: attachmentA.imagePath, mountPath: attachmentA.mountPath, loopDevice: attachmentA.loopDevice, authority: testComputerAuthority("attempt-a", "stale-fence", "boot-a"), lock: staleLock}
	if err := engine.detachComputerDisk(stale, computerDiskReapReceipt, ""); err == nil {
		t.Fatal("stale fence detached the live Computer disk")
	}
	if _, err := stale.lock.Stat(); err != nil {
		t.Fatalf("failed detach released its attachment lock: %v", err)
	}
	_ = stale.lock.Close()
	if _, mounted, _ := system.mountedSource(attachmentA.mountPath); !mounted {
		t.Fatal("stale fence changed live mount state")
	}
	if err := engine.detachComputerDisk(attachmentA, computerDiskReapReceipt, ""); err != nil {
		t.Fatal(err)
	}
	detachedManifest, _, err := readComputerDiskManifest(filepath.Join(filepath.Dir(attachmentA.imagePath), "attachment.json"))
	if err != nil || detachedManifest.PreviousDetachment == nil || detachedManifest.PreviousDetachment.Kind != computerDiskReapReceipt ||
		detachedManifest.PreviousDetachment.StorageGeneration != storage.StorageGeneration || detachedManifest.PreviousDetachment.AttemptID != authorityA.AttemptID ||
		detachedManifest.PreviousDetachment.FencingToken != authorityA.FencingToken || detachedManifest.PreviousDetachment.BootSessionID != authorityA.BootSessionID || detachedManifest.PreviousDetachment.SweepEpoch != "" {
		t.Fatalf("same-boot detachment receipt = %+v err=%v", detachedManifest.PreviousDetachment, err)
	}
	attachmentB, err := engine.attachComputerDisk(t.Context(), storage, testComputerAuthority("attempt-b", "fence-b", "boot-a"))
	if err != nil {
		t.Fatalf("attempt B did not consume exact reap evidence: %v", err)
	}
	manifest, present, err := readComputerDiskManifest(filepath.Join(filepath.Dir(attachmentB.imagePath), "attachment.json"))
	if err != nil || !present || manifest.Attached == nil || manifest.PreviousDetachment != nil {
		t.Fatalf("consumed attachment manifest = %+v present=%t err=%v", manifest, present, err)
	}
	if err := engine.detachComputerDisk(attachmentB, computerDiskReapReceipt, ""); err != nil {
		t.Fatal(err)
	}
}

func TestComputerDiskSweepRetainsBytesAndAuthorizesOneFreshAttach(t *testing.T) {
	root := t.TempDir()
	system := newFakeComputerDiskSystem()
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system, attempts: make(map[string]*containerdAttempt)}
	storage := testComputerStorage()
	attachmentA, err := engine.attachComputerDisk(t.Context(), storage, testComputerAuthority("attempt-a", "fence-a", "boot-a"))
	if err != nil {
		t.Fatal(err)
	}
	if err := unix.Flock(int(attachmentA.lock.Fd()), unix.LOCK_UN); err != nil {
		t.Fatal(err)
	}
	if err := attachmentA.lock.Close(); err != nil {
		t.Fatal(err)
	}
	if err := engine.sweepComputerDisks("sweep-boot-b"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(attachmentA.imagePath); err != nil {
		t.Fatalf("boot sweep lost Computer disk bytes: %v", err)
	}
	if len(system.mounts) != 0 || len(system.loops) != 0 {
		t.Fatalf("boot sweep retained runtime attachment: mounts %v loops %v", system.mounts, system.loops)
	}
	manifest, present, err := readComputerDiskManifest(filepath.Join(filepath.Dir(attachmentA.imagePath), "attachment.json"))
	if err != nil || !present || manifest.Attached != nil || manifest.PreviousDetachment == nil || manifest.PreviousDetachment.SweepEpoch != "sweep-boot-b" {
		t.Fatalf("sweep manifest = %+v present=%t err=%v", manifest, present, err)
	}
	attachmentB, err := engine.attachComputerDisk(t.Context(), storage, testComputerAuthority("attempt-b", "fence-b", "boot-b"))
	if err != nil {
		t.Fatalf("post-sweep attach failed: %v", err)
	}
	if _, err := engine.attachComputerDisk(t.Context(), storage, testComputerAuthority("attempt-c", "fence-c", "boot-b")); err == nil {
		t.Fatal("sweep evidence authorized more than one replacement attach")
	}
	if err := engine.detachComputerDisk(attachmentB, computerDiskReapReceipt, ""); err != nil {
		t.Fatal(err)
	}
}

func TestComputerDiskInventoryEnumeratesAllocationAndAttachmentClasses(t *testing.T) {
	root := t.TempDir()
	system := newFakeComputerDiskSystem()
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system}
	attachment, err := engine.attachComputerDisk(t.Context(), testComputerStorage(), testComputerAuthority("attempt-a", "fence-a", "boot-a"))
	if err != nil {
		t.Fatal(err)
	}
	inventory := ResourceInventory{}
	if err := engine.inventoryComputerDiskResources(&inventory); err != nil {
		t.Fatal(err)
	}
	if len(inventory.ComputerDiskImages) != 1 || len(inventory.ComputerDiskAllocations) != 1 || len(inventory.ComputerDiskQuotas) != 1 || len(inventory.ComputerDiskManifests) != 1 || len(inventory.ComputerDiskMounts) != 1 || len(inventory.ComputerDiskLoops) != 1 || len(inventory.ComputerAttachments) != 1 {
		t.Fatalf("Computer disk inventory = %+v", inventory)
	}
	if err := engine.detachComputerDisk(attachment, computerDiskReapReceipt, ""); err != nil {
		t.Fatal(err)
	}
}

func TestComputerDiskInventoryContainsPerDiskAllocationAnomaly(t *testing.T) {
	root := t.TempDir()
	system := newFakeComputerDiskSystem()
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system}
	attachment, err := engine.attachComputerDisk(t.Context(), testComputerStorage(), testComputerAuthority("attempt-a", "fence-a", "boot-a"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(attachment.imagePath, 4096); err != nil {
		t.Fatal(err)
	}
	var inventory ResourceInventory
	if err := engine.inventoryComputerDiskResources(&inventory); err != nil {
		t.Fatalf("one disk anomaly failed inventory: %v", err)
	}
	if len(inventory.ComputerDiskAnomalies) != 1 || !strings.Contains(inventory.ComputerDiskAnomalies[0], "allocation_mismatch") {
		t.Fatalf("Computer anomaly inventory = %+v", inventory)
	}
}

func TestComputerDiskRemovalRequiresReceiptAndVerifiesAbsence(t *testing.T) {
	root := t.TempDir()
	system := newFakeComputerDiskSystem()
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system}
	storage := testComputerStorage()
	authority := testComputerAuthority("attempt-a", "fence-a", "boot-a")
	attachment, err := engine.attachComputerDisk(t.Context(), storage, authority)
	if err != nil {
		t.Fatal(err)
	}
	removal := ManagedVolumeRemovalAuthority{NodeID: authority.NodeID, BootSessionID: authority.BootSessionID, JobID: authority.JobID, RemovalGeneration: 1, CleanupFence: "cleanup"}
	if err := engine.deleteComputerDisk(storage, removal); err == nil {
		t.Fatal("attached Computer disk was deleted")
	}
	if err := engine.detachComputerDisk(attachment, computerDiskReapReceipt, ""); err != nil {
		t.Fatal(err)
	}
	if err := engine.deleteComputerDisk(storage, removal); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{filepath.Dir(attachment.imagePath), attachment.imagePath, attachment.mountPath} {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("Computer removal left %s: %v", path, err)
		}
	}
}
