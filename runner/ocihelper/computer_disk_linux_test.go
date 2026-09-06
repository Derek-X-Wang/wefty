//go:build linux

package ocihelper

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

type fakeComputerDiskSystem struct {
	mu             sync.Mutex
	allocationErr  error
	allocationRuns int
	allocationGate chan struct{}
	allocationHit  chan struct{}
	allocationOnce sync.Once
	mounts         map[string]string
	loops          map[string]string
	nextLoop       int
}

func newFakeComputerDiskSystem() *fakeComputerDiskSystem {
	return &fakeComputerDiskSystem{mounts: make(map[string]string), loops: make(map[string]string)}
}

func (system *fakeComputerDiskSystem) allocateAndFormat(_ context.Context, path string, bytes int64) error {
	if system.allocationHit != nil {
		system.allocationOnce.Do(func() { close(system.allocationHit) })
	}
	if system.allocationGate != nil {
		<-system.allocationGate
	}
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

func prepareSameBootSweptComputerDisk(t *testing.T) (*ContainerdEngine, ComputerStorageReference, AttemptAuthority) {
	t.Helper()
	root := t.TempDir()
	system := newFakeComputerDiskSystem()
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system}
	storage := testComputerStorage()
	prior := testComputerAuthority("attempt-a", "fence-a", "boot-a")
	attachment, err := engine.attachComputerDisk(t.Context(), storage, prior)
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.detachComputerDisk(attachment, computerDiskReapReceipt, ""); err != nil {
		t.Fatal(err)
	}
	engine = &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system}
	if err := engine.sweepComputerDisks(t.Context(), "same-boot-helper-sweep"); err != nil {
		t.Fatal(err)
	}
	return engine, storage, prior
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

func TestComputerDiskRejectsConcurrentAttachmentAsScopedConflict(t *testing.T) {
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: t.TempDir()}, diskSystem: newFakeComputerDiskSystem()}
	first, err := engine.attachComputerDisk(t.Context(), testComputerStorage(), testComputerAuthority("attempt-a", "fence-a", "boot-a"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = engine.detachComputerDisk(first, computerDiskReapReceipt, "") })
	_, err = engine.attachComputerDisk(t.Context(), testComputerStorage(), testComputerAuthority("attempt-b", "fence-b", "boot-a"))
	if !errors.Is(err, errComputerStorageAttachmentOwned) {
		t.Fatalf("concurrent Computer attachment = %v, want scoped ownership conflict", err)
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
	identity, err := ensureComputerStorageIdentity(mountPath)
	if err != nil || !identity.Repaired {
		t.Fatalf("initialize missing Computer machine-id = %+v err=%v", identity, err)
	}
	if err := initializeComputerDiskRoot(&computerDiskAttachment{mountPath: mountPath, fresh: true}, uid, gid, false); err != nil {
		t.Fatal(err)
	}
	machineIDPath := computerStorageIdentityAt(mountPath).MachineID
	machineID, err := readRegularFile(machineIDPath)
	if err != nil || !validComputerMachineID(machineID) {
		t.Fatalf("fresh Computer machine-id = %q err=%v", machineID, err)
	}
	if err := os.Chmod(machineIDPath, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(machineIDPath, []byte("tenant-corruption"), 0o444); err != nil {
		t.Fatal(err)
	}
	repaired, err := ensureComputerStorageIdentity(mountPath)
	if err != nil || !repaired.Repaired || repaired.MachineIDDigest == identity.MachineIDDigest {
		t.Fatalf("repair malformed Computer machine-id = %+v err=%v", repaired, err)
	}
	if err := initializeComputerDiskRoot(&computerDiskAttachment{mountPath: mountPath, fresh: false}, uid, gid, false); err != nil {
		t.Fatal(err)
	}
	wrongUID := uid + 1
	if err := initializeComputerDiskRoot(&computerDiskAttachment{mountPath: mountPath, fresh: false}, wrongUID, gid, false); err == nil {
		t.Fatal("existing Computer disk was silently re-owned")
	}
}

func TestComputerStorageIdentityPermissionFailureIsTyped(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("requires an unprivileged test process")
	}

	t.Run("unreadable identity", func(t *testing.T) {
		root := t.TempDir()
		paths := computerStorageIdentityAt(root)
		if err := os.Mkdir(paths.Directory, 0o700); err != nil {
			t.Fatal(err)
		}
		machineID := []byte("0123456789abcdef0123456789abcdef\n")
		if err := os.WriteFile(paths.MachineID, machineID, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(paths.MachineID, 0); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(paths.MachineID, 0o600) })

		_, err := ensureComputerStorageIdentity(root)
		var permission *computerStorageIdentityPermissionError
		if !errors.As(err, &permission) || permission.Operation != "read machine-id" ||
			!errors.Is(err, os.ErrPermission) || engineFailureReason(err) != EngineFailurePermissionDenied {
			t.Fatalf("unreadable Computer machine-id error = %v, want typed permission failure", err)
		}
		if err := os.Chmod(paths.MachineID, 0o600); err != nil {
			t.Fatal(err)
		}
		preserved, err := os.ReadFile(paths.MachineID)
		if err != nil || string(preserved) != string(machineID) {
			t.Fatalf("unreadable Computer machine-id was replaced = %q err=%v", preserved, err)
		}
	})

	t.Run("unwritable identity directory", func(t *testing.T) {
		root := t.TempDir()
		paths := computerStorageIdentityAt(root)
		if err := os.Mkdir(paths.Directory, 0o700); err != nil {
			t.Fatal(err)
		}
		malformed := []byte("tenant-corruption")
		if err := os.WriteFile(paths.MachineID, malformed, 0o400); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(paths.Directory, 0o500); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(paths.Directory, 0o700) })

		_, err := ensureComputerStorageIdentity(root)
		var permission *computerStorageIdentityPermissionError
		if !errors.As(err, &permission) || permission.Operation != "remove invalid machine-id" ||
			!errors.Is(err, os.ErrPermission) || engineFailureReason(err) != EngineFailurePermissionDenied {
			t.Fatalf("unwritable Computer identity directory error = %v, want typed permission failure", err)
		}
		if err := os.Chmod(paths.Directory, 0o700); err != nil {
			t.Fatal(err)
		}
		preserved, err := os.ReadFile(paths.MachineID)
		if err != nil || string(preserved) != string(malformed) {
			t.Fatalf("unwritable Computer machine-id was replaced = %q err=%v", preserved, err)
		}
	})
}

func TestComputerStorageIdentityRepairsTenantReplacedEtc(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, computerStorageIdentityDirectory), []byte("tenant-junk"), 0o600); err != nil {
		t.Fatal(err)
	}
	facts, err := ensureComputerStorageIdentity(root)
	if err != nil || !facts.Repaired || facts.RepairReason != "identity directory was not a real directory" {
		t.Fatalf("repaired identity = %+v err=%v", facts, err)
	}
	payload, err := readRegularFile(computerStorageIdentityAt(root).MachineID)
	if err != nil || !validComputerMachineID(payload) {
		t.Fatalf("repaired machine-id = %q err=%v", payload, err)
	}
}

func TestPreparedTenantDataIsNotFreshComputerStorage(t *testing.T) {
	root := t.TempDir()
	system := newFakeComputerDiskSystem()
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system}
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
	if err := os.WriteFile(imagePath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := system.allocateAndFormat(t.Context(), imagePath, storage.DiskBytes); err != nil {
		t.Fatal(err)
	}
	if err := writeComputerDiskManifest(diskRoot, computerDiskManifest{
		Version: computerDiskManifestVersion, Storage: storage, DiskImage: "disk.ext4", MountDirectory: name, Prepared: true,
	}); err != nil {
		t.Fatal(err)
	}
	attachment, err := engine.attachComputerDisk(t.Context(), storage, testComputerAuthority("attempt", "fence", "boot-a"))
	if err != nil {
		t.Fatal(err)
	}
	if attachment.fresh {
		t.Fatal("copy/grow prepared tenant data was classified as a fresh empty filesystem")
	}
	if err := engine.detachComputerDisk(attachment, computerDiskReapReceipt, ""); err != nil {
		t.Fatal(err)
	}
}

func TestComputerDiskOwnershipMigrationNeverFollowsSymlinks(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "tenant-file"), []byte("inside"), 0o600); err != nil {
		t.Fatal(err)
	}
	outsideFile := filepath.Join(outside, "must-not-touch")
	if err := os.WriteFile(outsideFile, []byte("outside"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "tenant-link")
	if err := os.Symlink(outside, link); err != nil {
		t.Fatal(err)
	}
	visited := map[string]bool{}
	if err := migrateComputerDiskOwnership(root, 1001, 1002, func(path string, uid, gid int) error {
		if uid != 1001 || gid != 1002 {
			t.Fatalf("ownership = %d:%d", uid, gid)
		}
		visited[path] = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !visited[root] || !visited[filepath.Join(root, "tenant-file")] || !visited[link] || visited[outsideFile] {
		t.Fatalf("lstat traversal paths = %#v", visited)
	}
}

func TestComputerDiskOwnershipMigrationRetriesAfterMidWalkFailure(t *testing.T) {
	root := t.TempDir()
	for _, name := range []string{"a", "b", "c"} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(name), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	injected := errors.New("injected lchown failure")
	calls := 0
	if err := migrateComputerDiskOwnership(root, 1001, 1002, func(string, int, int) error {
		calls++
		if calls == 3 {
			return injected
		}
		return nil
	}); !errors.Is(err, injected) {
		t.Fatalf("mid-walk failure = %v", err)
	}
	visited := map[string]int{}
	if err := migrateComputerDiskOwnership(root, 1001, 1002, func(path string, uid, gid int) error {
		if uid != 1001 || gid != 1002 {
			t.Fatalf("retry ownership = %d:%d", uid, gid)
		}
		visited[path]++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(visited) != 4 {
		t.Fatalf("retry did not walk every entry: %#v", visited)
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
	if err := engine.sweepComputerDisks(t.Context(), "sweep-boot-b"); err != nil {
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
	if err := engine.sweepComputerDisks(t.Context(), "sweep-boot-b"); err != nil {
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
	if err := engine.sweepComputerDisks(t.Context(), "copy-sweep"); err != nil {
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
	if err := engine.sweepComputerDisks(t.Context(), "sweep-boot-b"); err != nil {
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

func TestComputerDiskSweepAuthorizesSuccessorAcrossHelperAndJobReplacement(t *testing.T) {
	for _, test := range []struct {
		name          string
		successorJob  string
		successorBoot string
	}{
		{name: "helper replacement within agent boot", successorJob: "job-1", successorBoot: "boot-a"},
		{name: "replacement Job within agent boot", successorJob: "job-2", successorBoot: "boot-a"},
		{name: "same Job after agent boot", successorJob: "job-1", successorBoot: "boot-b"},
		{name: "replacement Job after agent boot", successorJob: "job-2", successorBoot: "boot-b"},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			system := newFakeComputerDiskSystem()
			engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system, attempts: make(map[string]*containerdAttempt)}
			storage := testComputerStorage()
			attachment, err := engine.attachComputerDisk(t.Context(), storage,
				testComputerAuthority("attempt-a", "fence-a", "boot-a"))
			if err != nil {
				t.Fatal(err)
			}
			if err := unix.Flock(int(attachment.lock.Fd()), unix.LOCK_UN); err != nil {
				t.Fatal(err)
			}
			if err := attachment.lock.Close(); err != nil {
				t.Fatal(err)
			}
			if err := engine.sweepComputerDisks(t.Context(), "helper-replacement-sweep"); err != nil {
				t.Fatal(err)
			}

			successor := testComputerAuthority("attempt-b", "fence-b", test.successorBoot)
			successor.JobID = test.successorJob
			reattached, err := engine.attachComputerDisk(t.Context(), storage, successor)
			if err != nil {
				t.Fatalf("successor did not consume the exact helper sweep detachment: %v", err)
			}
			if _, err := engine.attachComputerDisk(t.Context(), storage, successor); err == nil {
				t.Fatal("helper sweep detachment authorized a second successor")
			}
			if err := engine.detachComputerDisk(reattached, computerDiskReapReceipt, ""); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestComputerDiskCleanReapThenBootSweepAuthorizesNextBoot(t *testing.T) {
	engine, storage, prior := prepareSameBootSweptComputerDisk(t)
	successor := prior
	successor.AttemptID = "attempt-b"
	successor.FencingToken = "fence-b"
	successor.BootSessionID = "boot-b"
	attachment, err := engine.attachComputerDisk(t.Context(), storage, successor)
	if err != nil {
		t.Fatalf("boot sweep did not refresh the clean reap receipt: %v", err)
	}
	if err := engine.detachComputerDisk(attachment, computerDiskReapReceipt, ""); err != nil {
		t.Fatal(err)
	}
}

func TestComputerDiskSweepQuarantinesPerDiskIdentityState(t *testing.T) {
	for _, test := range []struct {
		name       string
		mutate     func(*computerDiskManifest, string)
		wantReason string
	}{
		{
			name: "manifest paths mismatch a healthy allocation",
			mutate: func(manifest *computerDiskManifest, _ string) {
				manifest.DiskImage = "foreign.ext4"
			},
			wantReason: "identity_mismatch",
		},
		{
			name: "deterministic directory mismatches healthy Storage authority",
			mutate: func(manifest *computerDiskManifest, name string) {
				manifest.Storage.ComputerID = "foreign-computer"
				manifest.MountDirectory = name
			},
			wantReason: "identity_mismatch",
		},
		{
			name: "clean reap evidence is invalid",
			mutate: func(manifest *computerDiskManifest, _ string) {
				manifest.Attached = nil
				manifest.PreviousDetachment = &computerDiskEvidence{Kind: computerDiskReapReceipt}
			},
			wantReason: "detachment_evidence_invalid",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			storage := testComputerStorage()
			name, _ := deterministicComputerDiskName(storage)
			diskRoot := filepath.Join(root, "computer-disks", name)
			if err := os.MkdirAll(diskRoot, 0o700); err != nil {
				t.Fatal(err)
			}
			system := newFakeComputerDiskSystem()
			image, err := os.OpenFile(filepath.Join(diskRoot, "disk.ext4"), os.O_CREATE|os.O_RDWR, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			if err := image.Close(); err != nil {
				t.Fatal(err)
			}
			if err := system.allocateAndFormat(t.Context(), filepath.Join(diskRoot, "disk.ext4"), storage.DiskBytes); err != nil {
				t.Fatal(err)
			}
			authority := testComputerAuthority("attempt-a", "fence-a", "boot-a")
			manifest := computerDiskManifest{Version: computerDiskManifestVersion, Storage: storage, DiskImage: "disk.ext4", MountDirectory: name, Attached: &authority}
			test.mutate(&manifest, name)
			if err := writeComputerDiskManifest(diskRoot, manifest); err != nil {
				t.Fatal(err)
			}
			engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system}
			if err := engine.sweepComputerDisks(t.Context(), "identity-state-sweep"); err != nil {
				t.Fatalf("one disk's invalid state failed the whole sweep: %v", err)
			}
			if !slices.ContainsFunc(engine.computerDiskSweepEvidence, func(item SweepEvidence) bool {
				return item.ID == name && item.Action == SweepActionQuarantined && item.Method == test.wantReason
			}) {
				t.Fatalf("quarantine evidence = %+v", engine.computerDiskSweepEvidence)
			}
			if _, err := os.Lstat(diskRoot); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("invalid generation remained admissible: %v", err)
			}
		})
	}
}

func TestComputerDiskSweepDoesNotMutateIdentityMismatchedCopyRecovery(t *testing.T) {
	for _, phase := range []computerStorageCopyPhase{computerStorageCopyReserved, computerStorageCopyManifestWritten} {
		t.Run(string(phase), func(t *testing.T) {
			root := t.TempDir()
			storageX := testComputerStorage()
			nameX, _ := deterministicComputerDiskName(storageX)
			storageY := storageX
			storageY.StorageGeneration++
			storageY.IntentRevision++
			diskRoot := filepath.Join(root, "computer-disks", nameX)
			if err := os.MkdirAll(diskRoot, 0o700); err != nil {
				t.Fatal(err)
			}
			manifest := computerDiskManifest{Version: computerDiskManifestVersion, Storage: storageX,
				DiskImage: "disk.ext4", MountDirectory: nameX, Prepared: true}
			if err := writeComputerDiskManifest(diskRoot, manifest); err != nil {
				t.Fatal(err)
			}
			imageName := "disk.ext4.staging"
			if phase == computerStorageCopyManifestWritten {
				imageName = "disk.ext4"
			}
			imagePayload := []byte("tenant-bytes-must-not-change-" + string(phase))
			if err := os.WriteFile(filepath.Join(diskRoot, imageName), imagePayload, 0o600); err != nil {
				t.Fatal(err)
			}
			request := CopyComputerStorageRequest{Operation: "restore", BackupID: "backup", CopyID: "copy",
				SourceComputerID: "source-computer", SourceStorageID: "source-storage", SourceGeneration: 1,
				SourceSize: int64(len(imagePayload)), SourceDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				Destination: storageY, Authority: ComputerStorageCopyAuthority{NodeID: "node", BootSessionID: "boot",
					HelperGeneration: 1, RootInstanceID: "root", JobID: "job", OperationRevision: 1, CleanupFence: "fence"}}
			copyRecord := computerStorageCopyManifest{Version: 1, Request: request, Phase: phase}
			if phase == computerStorageCopyManifestWritten {
				copyRecord.DestinationDigest = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
				copyRecord.SourceUnchanged = true
			}
			if err := writeComputerStorageCopyManifest(diskRoot, copyRecord); err != nil {
				t.Fatal(err)
			}
			beforeManifest, err := os.ReadFile(filepath.Join(diskRoot, "attachment.json"))
			if err != nil {
				t.Fatal(err)
			}
			beforeCopy, err := os.ReadFile(filepath.Join(diskRoot, "storage-copy.json"))
			if err != nil {
				t.Fatal(err)
			}

			engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: newFakeComputerDiskSystem()}
			if err := engine.sweepComputerDisks(t.Context(), "identity-copy-probe"); err != nil {
				t.Fatalf("identity-mismatched copy failed the node sweep: %v", err)
			}
			entries, err := os.ReadDir(filepath.Join(root, "computer-disk-quarantine"))
			if err != nil || len(entries) != 1 {
				t.Fatalf("quarantine entries = %v err=%v", entries, err)
			}
			quarantinedRoot := filepath.Join(root, "computer-disk-quarantine", entries[0].Name())
			for path, want := range map[string][]byte{
				"attachment.json":   beforeManifest,
				"storage-copy.json": beforeCopy,
				imageName:           imagePayload,
			} {
				got, readErr := os.ReadFile(filepath.Join(quarantinedRoot, path))
				if readErr != nil || !slices.Equal(got, want) {
					t.Fatalf("%s changed during identity quarantine: got=%q want=%q err=%v", path, got, want, readErr)
				}
			}
			persistedCopy, present, err := readComputerStorageCopyManifest(filepath.Join(quarantinedRoot, "storage-copy.json"))
			if err != nil || !present || persistedCopy.Phase != phase || persistedCopy.Receipt != nil {
				t.Fatalf("copy authority was advanced: present=%t record=%+v err=%v", present, persistedCopy, err)
			}
		})
	}
}

func TestComputerDiskSweepQuarantinesReservedCopyWithoutRollingBackPublishedAttachment(t *testing.T) {
	root := t.TempDir()
	storage := testComputerStorage()
	name, _ := deterministicComputerDiskName(storage)
	diskRoot := filepath.Join(root, "computer-disks", name)
	if err := os.MkdirAll(diskRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	manifest := computerDiskManifest{Version: computerDiskManifestVersion, Storage: storage,
		DiskImage: "disk.ext4", MountDirectory: name, Prepared: true}
	if err := writeComputerDiskManifest(diskRoot, manifest); err != nil {
		t.Fatal(err)
	}
	tenantBytes := []byte("published tenant bytes")
	if err := os.WriteFile(filepath.Join(diskRoot, "disk.ext4"), tenantBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	request := CopyComputerStorageRequest{Operation: "restore", BackupID: "backup", CopyID: "copy",
		SourceComputerID: "source-computer", SourceStorageID: "source-storage", SourceGeneration: 1,
		SourceSize: int64(len(tenantBytes)), SourceDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Destination: storage, Authority: ComputerStorageCopyAuthority{NodeID: "node", BootSessionID: "boot",
			HelperGeneration: 1, RootInstanceID: "root", JobID: "job", OperationRevision: 1, CleanupFence: "fence"}}
	if err := writeComputerStorageCopyManifest(diskRoot, computerStorageCopyManifest{Version: 1, Request: request, Phase: computerStorageCopyReserved}); err != nil {
		t.Fatal(err)
	}
	beforeManifest, err := os.ReadFile(filepath.Join(diskRoot, "attachment.json"))
	if err != nil {
		t.Fatal(err)
	}
	beforeCopy, err := os.ReadFile(filepath.Join(diskRoot, "storage-copy.json"))
	if err != nil {
		t.Fatal(err)
	}

	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: newFakeComputerDiskSystem()}
	if err := engine.sweepComputerDisks(t.Context(), "published-attachment-copy-rollback"); err != nil {
		t.Fatalf("published attachment conflict failed node sweep: %v", err)
	}
	entries, err := os.ReadDir(filepath.Join(root, "computer-disk-quarantine"))
	if err != nil || len(entries) != 1 {
		t.Fatalf("published attachment conflict was not quarantined: entries=%v err=%v", entries, err)
	}
	quarantineRoot := filepath.Join(root, "computer-disk-quarantine", entries[0].Name())
	for path, want := range map[string][]byte{
		"attachment.json":   beforeManifest,
		"storage-copy.json": beforeCopy,
		"disk.ext4":         tenantBytes,
	} {
		if got, readErr := os.ReadFile(filepath.Join(quarantineRoot, path)); readErr != nil || !slices.Equal(got, want) {
			t.Fatalf("%s changed during published-attachment quarantine: got=%q want=%q err=%v", path, got, want, readErr)
		}
	}
	if !slices.ContainsFunc(engine.computerDiskSweepEvidence, func(item SweepEvidence) bool {
		return item.ID == name && item.Action == SweepActionQuarantined && item.Method == "copy_recovery_authority_invalid"
	}) {
		t.Fatalf("published attachment conflict evidence = %+v", engine.computerDiskSweepEvidence)
	}
}

func TestComputerDiskSweepQuarantinesMissingManifestAfterOperationalDeferral(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("permission-denial recovery proof requires the non-root helper test lane")
	}
	root := t.TempDir()
	storage := testComputerStorage()
	name, _ := deterministicComputerDiskName(storage)
	diskRoot := filepath.Join(root, "computer-disks", name)
	if err := os.MkdirAll(diskRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	system := newFakeComputerDiskSystem()
	imagePath := filepath.Join(diskRoot, "disk.ext4")
	if err := os.WriteFile(imagePath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := system.allocateAndFormat(t.Context(), imagePath, storage.DiskBytes); err != nil {
		t.Fatal(err)
	}
	if err := writeComputerDiskManifest(diskRoot, computerDiskManifest{Version: computerDiskManifestVersion,
		Storage: storage, DiskImage: "disk.ext4", MountDirectory: name, Prepared: true}); err != nil {
		t.Fatal(err)
	}
	manifestPath := filepath.Join(diskRoot, "attachment.json")
	if err := os.Chmod(manifestPath, 0); err != nil {
		t.Fatal(err)
	}
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system}
	if err := engine.sweepComputerDisks(t.Context(), "manifest-eacces"); err != nil {
		t.Fatalf("operational manifest fault failed node sweep: %v", err)
	}
	if err := os.Chmod(manifestPath, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(manifestPath); err != nil {
		t.Fatal(err)
	}
	engine = &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system}
	if err := engine.sweepComputerDisks(t.Context(), "manifest-missing"); err != nil {
		t.Fatalf("missing authority failed node sweep: %v", err)
	}
	entries, err := os.ReadDir(filepath.Join(root, "computer-disk-quarantine"))
	if err != nil || len(entries) != 1 {
		t.Fatalf("missing authority was not quarantined: entries=%v err=%v", entries, err)
	}
	quarantinedImage := filepath.Join(root, "computer-disk-quarantine", entries[0].Name(), "disk.ext4")
	if info, err := os.Stat(quarantinedImage); err != nil || info.Size() != storage.DiskBytes {
		t.Fatalf("tenant bytes were deleted: info=%v err=%v", info, err)
	}
}

func TestComputerDiskSweepSurfacesUnreadableDeferralWhenManifestIsMissing(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("permission-denial recovery proof requires the non-root helper test lane")
	}
	root := t.TempDir()
	storage := testComputerStorage()
	name, _ := deterministicComputerDiskName(storage)
	diskRoot := filepath.Join(root, "computer-disks", name)
	if err := os.MkdirAll(diskRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(diskRoot, "disk.ext4"), []byte("tenant bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	seed := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root, Clock: newManualClock(now)}}
	seed.resolveOperationalComputerRecoveryFailure(diskRoot, name, "computer_disk_manifest", storage, syscall.EIO, true)
	fallbackPath := operationalComputerRecoveryDeferralFaultPath(diskRoot, name)
	rootInspections := 0
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root, Clock: newManualClock(now.Add(time.Minute))}, diskSystem: newFakeComputerDiskSystem()}
	engine.computerLstat = func(path string) (os.FileInfo, error) {
		if path == diskRoot {
			rootInspections++
			if rootInspections == 3 {
				if err := os.Chmod(fallbackPath, 0); err != nil {
					t.Fatal(err)
				}
				return nil, &os.PathError{Op: "lstat", Path: path, Err: syscall.EIO}
			}
		}
		return os.Lstat(path)
	}
	if err := engine.sweepComputerDisks(t.Context(), "missing-manifest-unreadable-deferral"); err != nil {
		t.Fatalf("unreadable recovery evidence failed node sweep: %v", err)
	}
	var inventory ResourceInventory
	if err := engine.inventoryComputerDiskResources(&inventory); err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(inventory.ComputerStorageQuarantined, func(item ComputerStorageRecoveryInventoryEntry) bool {
		return item.DiskName == name && item.Reason == "manifest_missing" && item.DeferredReason == "operational_failure" &&
			item.Storage == storage && item.Attempts == 1 && item.FirstDeferredAt.Equal(now)
	}) {
		t.Fatalf("missing-manifest quarantine lost readable primary deferral evidence: %+v", inventory.ComputerStorageQuarantined)
	}
	if !slices.ContainsFunc(engine.computerDiskSweepEvidence, func(item SweepEvidence) bool {
		return item.ID == name && item.Method == "manifest_missing"
	}) {
		t.Fatalf("missing-manifest sweep evidence lost readable primary deferral: %+v", engine.computerDiskSweepEvidence)
	}
}

func TestOperationalComputerRecoveryDeferralAccumulatesAcrossOperations(t *testing.T) {
	root := t.TempDir()
	storage := testComputerStorage()
	name, _ := deterministicComputerDiskName(storage)
	diskRoot := filepath.Join(root, "computer-disks", name)
	if err := os.MkdirAll(diskRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	first := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root, Clock: newManualClock(now)}}
	first.resolveOperationalComputerRecoveryFailure(diskRoot, name, "computer_disk_manifest", storage, syscall.EIO, true)
	for attempt := 2; attempt <= 6; attempt++ {
		engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root, Clock: newManualClock(now.Add(time.Duration(attempt) * time.Minute))}}
		evidence := engine.resolveOperationalComputerRecoveryFailure(diskRoot, name, "computer_disk_image", storage, syscall.EIO, true)
		if evidence.Method != "computer_disk_image" {
			t.Fatalf("attempt %d evidence = %+v", attempt, evidence)
		}
	}
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}}
	var inventory ResourceInventory
	if err := engine.inventoryComputerDiskResources(&inventory); err != nil {
		t.Fatal(err)
	}
	if len(inventory.ComputerStorageDeferred) != 1 || inventory.ComputerStorageDeferred[0].Operation != "computer_disk_image" ||
		inventory.ComputerStorageDeferred[0].Attempts != 6 || !inventory.ComputerStorageDeferred[0].FirstDeferredAt.Equal(now) {
		t.Fatalf("generation deferral did not advance across operations: %+v", inventory.ComputerStorageDeferred)
	}
}

func TestUnreadableComputerRecoverySidecarReachesAbandonment(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("permission-denial recovery proof requires the non-root helper test lane")
	}
	root := t.TempDir()
	storage := testComputerStorage()
	name, _ := deterministicComputerDiskName(storage)
	diskRoot := filepath.Join(root, "computer-disks", name)
	if err := os.MkdirAll(diskRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	system := newFakeComputerDiskSystem()
	imagePath := filepath.Join(diskRoot, "disk.ext4")
	if err := os.WriteFile(imagePath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := system.allocateAndFormat(t.Context(), imagePath, storage.DiskBytes); err != nil {
		t.Fatal(err)
	}
	if err := writeComputerDiskManifest(diskRoot, computerDiskManifest{Version: computerDiskManifestVersion,
		Storage: storage, DiskImage: "disk.ext4", MountDirectory: name, Prepared: true}); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	seed := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root, Clock: newManualClock(now)}}
	seed.resolveOperationalComputerRecoveryFailure(diskRoot, name, "computer_disk_manifest", storage, syscall.EIO, true)
	sidecar := filepath.Join(diskRoot, computerOperationalDeferralRecordName)
	if err := os.Chmod(sidecar, 0); err != nil {
		t.Fatal(err)
	}
	for attempt := 2; attempt <= defaultComputerStorageRecoveryAttempts; attempt++ {
		clock := newManualClock(now.Add(time.Duration(attempt) * time.Minute))
		if attempt == defaultComputerStorageRecoveryAttempts {
			clock.now = now.Add(defaultComputerDiskQuarantineRetention)
		}
		engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root, Clock: clock}, diskSystem: system}
		if err := engine.sweepComputerDisks(t.Context(), fmt.Sprintf("sidecar-unreadable-%d", attempt)); err != nil {
			t.Fatalf("attempt %d failed whole sweep: %v", attempt, err)
		}
		var inventory ResourceInventory
		if err := engine.inventoryComputerDiskResources(&inventory); err != nil {
			t.Fatal(err)
		}
		if attempt < defaultComputerStorageRecoveryAttempts {
			if !slices.ContainsFunc(inventory.ComputerStorageDeferred, func(item ComputerStorageRecoveryInventoryEntry) bool {
				return item.DiskName == name && item.Operation == "computer_disk_manifest" &&
					item.Reason == "deferral_record_unreadable" && item.Attempts == attempt && item.FirstDeferredAt.Equal(now)
			}) {
				t.Fatalf("attempt %d unreadable-sidecar deferral = %+v", attempt, inventory.ComputerStorageDeferred)
			}
		} else if !slices.ContainsFunc(inventory.ComputerStorageQuarantined, func(item ComputerStorageRecoveryInventoryEntry) bool {
			return item.DiskName == name && item.Reason == "resume_abandoned" && item.Attempts == attempt && item.FirstDeferredAt.Equal(now)
		}) {
			t.Fatalf("unreadable sidecar did not reach abandonment: %+v", inventory)
		}
	}
}

func TestComputerDiskQuarantineMoveFailureIsTypedDeferral(t *testing.T) {
	prepareRoot := func(t *testing.T, root string) (ComputerStorageReference, string, string) {
		t.Helper()
		storage := testComputerStorage()
		name, _ := deterministicComputerDiskName(storage)
		diskRoot := filepath.Join(root, "computer-disks", name)
		if err := os.MkdirAll(diskRoot, 0o700); err != nil {
			t.Fatal(err)
		}
		return storage, name, diskRoot
	}
	writeManifest := func(t *testing.T, diskRoot, name string, storage ComputerStorageReference, mutate func(*computerDiskManifest)) {
		t.Helper()
		manifest := computerDiskManifest{Version: computerDiskManifestVersion, Storage: storage, DiskImage: "disk.ext4", MountDirectory: name, Prepared: true}
		if mutate != nil {
			mutate(&manifest)
		}
		if err := writeComputerDiskManifest(diskRoot, manifest); err != nil {
			t.Fatal(err)
		}
	}
	prepareAllocated := func(t *testing.T, diskRoot string, storage ComputerStorageReference, system *fakeComputerDiskSystem) {
		t.Helper()
		imagePath := filepath.Join(diskRoot, "disk.ext4")
		if err := os.WriteFile(imagePath, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := system.allocateAndFormat(t.Context(), imagePath, storage.DiskBytes); err != nil {
			t.Fatal(err)
		}
	}

	tests := []struct {
		name    string
		prepare func(*testing.T, string, *fakeComputerDiskSystem) string
	}{
		{name: "pending quarantine authority", prepare: func(t *testing.T, root string, _ *fakeComputerDiskSystem) string {
			_, name, diskRoot := prepareRoot(t, root)
			if err := os.WriteFile(filepath.Join(diskRoot, "quarantine.json"), []byte("{"), 0o600); err != nil {
				t.Fatal(err)
			}
			return name
		}},
		{name: "invalid manifest", prepare: func(t *testing.T, root string, _ *fakeComputerDiskSystem) string {
			_, name, diskRoot := prepareRoot(t, root)
			if err := os.WriteFile(filepath.Join(diskRoot, "attachment.json"), []byte("{"), 0o600); err != nil {
				t.Fatal(err)
			}
			return name
		}},
		{name: "manifest identity mismatch", prepare: func(t *testing.T, root string, _ *fakeComputerDiskSystem) string {
			storage, name, diskRoot := prepareRoot(t, root)
			writeManifest(t, diskRoot, name, storage, func(manifest *computerDiskManifest) { manifest.DiskImage = "foreign.ext4" })
			return name
		}},
		{name: "copy recovery authority", prepare: func(t *testing.T, root string, _ *fakeComputerDiskSystem) string {
			_, name, diskRoot := prepareRoot(t, root)
			if err := os.WriteFile(filepath.Join(diskRoot, "storage-copy.json"), []byte("{"), 0o600); err != nil {
				t.Fatal(err)
			}
			return name
		}},
		{name: "missing manifest", prepare: func(t *testing.T, root string, _ *fakeComputerDiskSystem) string {
			_, name, _ := prepareRoot(t, root)
			return name
		}},
		{name: "grow recovery authority", prepare: func(t *testing.T, root string, _ *fakeComputerDiskSystem) string {
			storage, name, diskRoot := prepareRoot(t, root)
			writeManifest(t, diskRoot, name, storage, nil)
			if err := os.WriteFile(filepath.Join(diskRoot, computerStorageGrowIntentName), []byte("{"), 0o600); err != nil {
				t.Fatal(err)
			}
			return name
		}},
		{name: "unverified reset identity", prepare: func(t *testing.T, root string, _ *fakeComputerDiskSystem) string {
			storage, name, diskRoot := prepareRoot(t, root)
			writeManifest(t, diskRoot, name, storage, func(manifest *computerDiskManifest) {
				manifest.Prepared = false
				manifest.Preparation = &ComputerStorageResetAuthority{NodeID: "node", BootSessionID: "boot", HelperGeneration: 1,
					RootInstanceID: "root", JobID: "job", PriorJobID: "prior", IntentRevision: storage.IntentRevision, CleanupFence: "fence"}
			})
			return name
		}},
		{name: "invalid detachment evidence", prepare: func(t *testing.T, root string, system *fakeComputerDiskSystem) string {
			storage, name, diskRoot := prepareRoot(t, root)
			prepareAllocated(t, diskRoot, storage, system)
			writeManifest(t, diskRoot, name, storage, func(manifest *computerDiskManifest) {
				manifest.PreviousDetachment = &computerDiskEvidence{Kind: computerDiskReapReceipt}
			})
			return name
		}},
		{name: "allocation mismatch", prepare: func(t *testing.T, root string, _ *fakeComputerDiskSystem) string {
			request := growTestRequest(16 << 20)
			imagePath := prepareGrowTestImage(t, root, request)
			if err := os.Truncate(imagePath, request.NewDiskBytes); err != nil {
				t.Fatal(err)
			}
			name, _ := deterministicComputerDiskName(request.Storage)
			return name
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			system := newFakeComputerDiskSystem()
			name := test.prepare(t, root, system)
			engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system,
				computerQuarantineHook: func(phase computerDiskQuarantinePhase) error {
					if phase == computerDiskQuarantineRecordWritten {
						return &os.PathError{Op: "rename", Path: name, Err: syscall.EPERM}
					}
					return nil
				}}
			if err := engine.sweepComputerDisks(t.Context(), "quarantine-move-fault"); err != nil {
				t.Fatalf("one quarantine move failed whole node sweep: %v", err)
			}
			var inventory ResourceInventory
			if err := engine.inventoryComputerDiskResources(&inventory); err != nil {
				t.Fatal(err)
			}
			if !slices.ContainsFunc(inventory.ComputerStorageDeferred, func(item ComputerStorageRecoveryInventoryEntry) bool {
				return item.DiskName == name && item.Operation == "quarantine_move_failed" && item.Attempts == 1
			}) {
				t.Fatalf("quarantine move deferral = %+v, sweep evidence=%+v", inventory.ComputerStorageDeferred, engine.computerDiskSweepEvidence)
			}
		})
	}
}

func TestComputerDiskDirectoryIOFaultIsOperationalDeferral(t *testing.T) {
	root := t.TempDir()
	storage := testComputerStorage()
	name, _ := deterministicComputerDiskName(storage)
	diskRoot := filepath.Join(root, "computer-disks", name)
	if err := os.MkdirAll(diskRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: newFakeComputerDiskSystem()}
	engine.computerLstat = func(path string) (os.FileInfo, error) {
		if path == diskRoot {
			return nil, &os.PathError{Op: "lstat", Path: path, Err: syscall.EIO}
		}
		return os.Lstat(path)
	}
	if err := engine.sweepComputerDisks(t.Context(), "directory-io-fault"); err != nil {
		t.Fatalf("one disk directory I/O fault failed node sweep: %v", err)
	}
	var inventory ResourceInventory
	if err := engine.inventoryComputerDiskResources(&inventory); err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(inventory.ComputerStorageDeferred, func(item ComputerStorageRecoveryInventoryEntry) bool {
		return item.DiskName == name && item.Operation == "computer_disk_directory" && item.Reason == "operational_failure" && item.Attempts == 1
	}) {
		t.Fatalf("directory I/O deferral = %+v, anomalies=%+v", inventory.ComputerStorageDeferred, inventory.ComputerDiskAnomalies)
	}
}

func TestComputerDiskPerGenerationSyscallFailuresAreDurableDeferrals(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("permission-denial recovery proof requires the non-root helper test lane")
	}
	for _, test := range []struct {
		name      string
		operation string
		prepare   func(*testing.T, string, string, *computerDiskManifest)
	}{
		{name: "staging remove", operation: "computer_disk_staging_cleanup", prepare: func(t *testing.T, _ string, diskRoot string, _ *computerDiskManifest) {
			if err := os.WriteFile(filepath.Join(diskRoot, ".disk.ext4.tmp-probe"), []byte("staged"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(diskRoot, 0o500); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "lock open", operation: "computer_disk_lock_open", prepare: func(t *testing.T, _ string, diskRoot string, _ *computerDiskManifest) {
			if err := os.Chmod(diskRoot, 0o500); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "receipt write", operation: "computer_disk_receipt_write", prepare: func(t *testing.T, _ string, diskRoot string, manifest *computerDiskManifest) {
			authority := testComputerAuthority("attempt", "fence", "boot")
			manifest.Attached = &authority
			if err := writeComputerDiskManifest(diskRoot, *manifest); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(diskRoot, "attachment.lock"), nil, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(diskRoot, 0o500); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "unverified reset remove", operation: "computer_storage_reset_cleanup", prepare: func(t *testing.T, diskRootParent, diskRoot string, manifest *computerDiskManifest) {
			manifest.Storage.StorageGeneration = 2
			manifest.Storage.IntentRevision = 2
			name, _ := deterministicComputerDiskName(manifest.Storage)
			if filepath.Base(diskRoot) != name {
				t.Fatal("reset fixture directory identity is inconsistent")
			}
			manifest.Prepared = false
			manifest.Preparation = &ComputerStorageResetAuthority{NodeID: "node", BootSessionID: "boot", HelperGeneration: 1,
				RootInstanceID: "root", JobID: "job", PriorJobID: "prior", IntentRevision: 2, CleanupFence: "fence"}
			if err := writeComputerDiskManifest(diskRoot, *manifest); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(diskRootParent, 0o500); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			storage := testComputerStorage()
			if test.operation == "computer_storage_reset_cleanup" {
				storage.StorageGeneration = 2
				storage.IntentRevision = 2
			}
			name, _ := deterministicComputerDiskName(storage)
			diskRootParent := filepath.Join(root, "computer-disks")
			diskRoot := filepath.Join(diskRootParent, name)
			if err := os.MkdirAll(diskRoot, 0o700); err != nil {
				t.Fatal(err)
			}
			system := newFakeComputerDiskSystem()
			imagePath := filepath.Join(diskRoot, "disk.ext4")
			if err := os.WriteFile(imagePath, nil, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := system.allocateAndFormat(t.Context(), imagePath, storage.DiskBytes); err != nil {
				t.Fatal(err)
			}
			manifest := computerDiskManifest{Version: computerDiskManifestVersion, Storage: storage,
				DiskImage: "disk.ext4", MountDirectory: name, Prepared: true}
			if err := writeComputerDiskManifest(diskRoot, manifest); err != nil {
				t.Fatal(err)
			}
			test.prepare(t, diskRootParent, diskRoot, &manifest)
			t.Cleanup(func() {
				_ = os.Chmod(diskRootParent, 0o700)
				_ = os.Chmod(diskRoot, 0o700)
			})
			engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system}
			if err := engine.sweepComputerDisks(t.Context(), "syscall-fault"); err != nil {
				t.Fatalf("per-generation syscall failed the node sweep: %v", err)
			}
			_ = os.Chmod(diskRootParent, 0o700)
			_ = os.Chmod(diskRoot, 0o700)
			var inventory ResourceInventory
			if err := engine.inventoryComputerDiskResources(&inventory); err != nil {
				t.Fatal(err)
			}
			if !slices.ContainsFunc(inventory.ComputerStorageDeferred, func(item ComputerStorageRecoveryInventoryEntry) bool {
				return item.DiskName == name && item.Operation == test.operation && item.Attempts == 1
			}) {
				t.Fatalf("%s deferral = %+v, evidence=%+v", test.operation, inventory.ComputerStorageDeferred, engine.computerDiskSweepEvidence)
			}
		})
	}
}

func TestComputerDiskSweepQuarantinesInvalidDurableDeferral(t *testing.T) {
	root := t.TempDir()
	storage := testComputerStorage()
	name, _ := deterministicComputerDiskName(storage)
	diskRoot := filepath.Join(root, "computer-disks", name)
	if err := os.MkdirAll(diskRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	system := newFakeComputerDiskSystem()
	imagePath := filepath.Join(diskRoot, "disk.ext4")
	image, err := os.OpenFile(imagePath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if err := image.Close(); err != nil {
		t.Fatal(err)
	}
	if err := system.allocateAndFormat(t.Context(), imagePath, storage.DiskBytes); err != nil {
		t.Fatal(err)
	}
	if err := writeComputerDiskManifest(diskRoot, computerDiskManifest{Version: computerDiskManifestVersion,
		Storage: storage, DiskImage: "disk.ext4", MountDirectory: name, Prepared: true}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(diskRoot, computerOperationalDeferralRecordName), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system}
	if err := engine.sweepComputerDisks(t.Context(), "invalid-deferral"); err != nil {
		t.Fatalf("one disk's invalid durable deferral failed the whole sweep: %v", err)
	}
	if !slices.ContainsFunc(engine.computerDiskSweepEvidence, func(item SweepEvidence) bool {
		return item.ID == name && item.Action == SweepActionQuarantined && item.Method == "recovery_deferral_invalid"
	}) {
		t.Fatalf("invalid deferral quarantine evidence = %+v", engine.computerDiskSweepEvidence)
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

func TestComputerDiskInventoryDefersPerDiskLstatIOFaults(t *testing.T) {
	for _, test := range []struct {
		name      string
		faultBase string
		operation string
	}{
		{name: "quarantine receipt", faultBase: "quarantine.json", operation: "computer_disk_quarantine"},
		{name: "disk image", faultBase: "disk.ext4", operation: "computer_disk_image"},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			storage := testComputerStorage()
			name, _ := deterministicComputerDiskName(storage)
			diskRoot := filepath.Join(root, "computer-disks", name)
			if err := os.MkdirAll(diskRoot, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := writeComputerDiskManifest(diskRoot, computerDiskManifest{Version: computerDiskManifestVersion,
				Storage: storage, DiskImage: "disk.ext4", MountDirectory: name, Prepared: true}); err != nil {
				t.Fatal(err)
			}
			engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}}
			engine.computerLstat = func(path string) (os.FileInfo, error) {
				if filepath.Base(path) == test.faultBase {
					return nil, &os.PathError{Op: "lstat", Path: path, Err: syscall.EIO}
				}
				return os.Lstat(path)
			}
			var inventory ResourceInventory
			if err := engine.inventoryComputerDiskResources(&inventory); err != nil {
				t.Fatalf("one disk's Lstat fault failed namespace verification: %v", err)
			}
			if !slices.ContainsFunc(inventory.ComputerStorageDeferred, func(item ComputerStorageRecoveryInventoryEntry) bool {
				return item.DiskName == name && item.Operation == test.operation && item.Reason == "operational_failure"
			}) {
				t.Fatalf("typed per-disk deferral = %+v", inventory.ComputerStorageDeferred)
			}
		})
	}
}

func TestComputerDiskQuarantineRootReadFaultIsTypedDeferral(t *testing.T) {
	root := t.TempDir()
	quarantineRoot := filepath.Join(root, "computer-disk-quarantine")
	if err := os.MkdirAll(quarantineRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}}
	engine.computerReadDir = func(path string) ([]os.DirEntry, error) {
		if path == quarantineRoot {
			return nil, &os.PathError{Op: "readdir", Path: path, Err: syscall.EIO}
		}
		return os.ReadDir(path)
	}
	if err := engine.expireComputerDiskQuarantinePayloads(t.Context()); err != nil {
		t.Fatalf("quarantine-root read fault failed whole sweep: %v", err)
	}
	if !slices.ContainsFunc(engine.computerDiskSweepEvidence, func(item SweepEvidence) bool {
		return item.Class == RemovalResourceComputerQuarantine && item.Action == SweepActionResumeDeferred && item.Method == "quarantine_root_inventory"
	}) {
		t.Fatalf("quarantine-root deferral evidence = %+v", engine.computerDiskSweepEvidence)
	}
}

func TestUnreadableComputerRecoveryDeferralPersistsAcrossHelperProcesses(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("permission-denial recovery proof requires the non-root helper test lane")
	}
	root := t.TempDir()
	storage := testComputerStorage()
	name, _ := deterministicComputerDiskName(storage)
	diskRoot := filepath.Join(root, "computer-disks", name)
	if err := os.MkdirAll(diskRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	manifestPath := filepath.Join(diskRoot, "attachment.json")
	if err := writeComputerDiskManifest(diskRoot, computerDiskManifest{Version: computerDiskManifestVersion,
		Storage: storage, DiskImage: "disk.ext4", MountDirectory: name, Prepared: true}); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(manifestPath, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(manifestPath, 0o600) })
	now := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	for attempt := 1; attempt <= defaultComputerStorageRecoveryAttempts; attempt++ {
		clock := newManualClock(now)
		if attempt == defaultComputerStorageRecoveryAttempts {
			clock.now = now.Add(defaultComputerDiskQuarantineRetention)
		}
		engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root, Clock: clock}, diskSystem: newFakeComputerDiskSystem()}
		if err := engine.sweepComputerDisks(t.Context(), fmt.Sprintf("unreadable-%d", attempt)); err != nil {
			t.Fatalf("attempt %d failed whole sweep: %v", attempt, err)
		}
		var inventory ResourceInventory
		if err := engine.inventoryComputerDiskResources(&inventory); err != nil {
			t.Fatal(err)
		}
		if attempt < defaultComputerStorageRecoveryAttempts {
			if !slices.ContainsFunc(inventory.ComputerStorageDeferred, func(item ComputerStorageRecoveryInventoryEntry) bool {
				return item.DiskName == name && item.Attempts == attempt && item.FirstDeferredAt.Equal(now)
			}) {
				t.Fatalf("attempt %d durable deferral = %+v", attempt, inventory.ComputerStorageDeferred)
			}
		} else if !slices.ContainsFunc(inventory.ComputerStorageQuarantined, func(item ComputerStorageRecoveryInventoryEntry) bool {
			return item.DiskName == name && item.Reason == "resume_abandoned" && item.Attempts == attempt && item.FirstDeferredAt.Equal(now)
		}) {
			t.Fatalf("bounded unreadable deferral did not escalate: %+v", inventory.ComputerStorageQuarantined)
		}
	}
}

func TestStartupQuarantinesUnrecordedAllocationMismatchAndKeepsNamespaceAdmissible(t *testing.T) {
	root := t.TempDir()
	request := growTestRequest(16 << 20)
	imagePath := prepareGrowTestImage(t, root, request)
	if err := os.Truncate(imagePath, request.NewDiskBytes); err != nil {
		t.Fatal(err)
	}
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: newFakeComputerDiskSystem()}
	if err := engine.sweepComputerDisks(t.Context(), "startup-sweep"); err != nil {
		t.Fatal(err)
	}
	evidence := engine.computerDiskSweepEvidence
	name, _ := deterministicComputerDiskName(request.Storage)
	if !slices.ContainsFunc(evidence, func(item SweepEvidence) bool {
		return item.ID == name && item.Action == SweepActionQuarantined && item.Method == "allocation_mismatch"
	}) {
		t.Fatalf("quarantine evidence = %+v", evidence)
	}
	inventory := ResourceInventory{}
	if err := engine.inventoryComputerDiskResources(&inventory); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(inventory.ComputerQuarantines, []string{name}) || len(inventory.ComputerDiskAnomalies) != 0 {
		t.Fatalf("quarantined inventory = %+v", inventory)
	}
	projected, err := projectRuntimeAbsenceInventory(inventory, func(string, string) (bool, error) { return false, nil }, func(string) (bool, error) { return false, nil })
	if err != nil || !InventoryEmpty(projected) {
		t.Fatalf("quarantine blocked namespace admission: projected=%+v err=%v", projected, err)
	}
	if _, err := engine.attachComputerDisk(t.Context(), request.Storage, testComputerAuthority("quarantined", "fence", "boot")); err == nil || !strings.Contains(err.Error(), "quarantined") {
		t.Fatalf("quarantined generation was attachable: %v", err)
	}
}

func TestComputerDiskInventoryRejectsUntypedQuarantineEntries(t *testing.T) {
	storage := testComputerStorage()
	name, _ := deterministicComputerDiskName(storage)
	for _, test := range []struct {
		name    string
		payload string
	}{
		{name: "missing receipt"},
		{name: "corrupt receipt", payload: "{"},
		{name: "mismatched identity", payload: `{"kind":"computer_disk_anomaly_quarantined","receipt_id":"receipt","disk_name":"` + name + `","storage":{"computer_id":"other","storage_id":"storage","storage_generation":1,"intent_revision":1,"disk_bytes":8388608},"reason":"allocation_mismatch","created_at":"2026-09-03T00:00:00Z","retain_until":"2026-09-04T00:00:00Z"}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			entry := filepath.Join(root, "computer-disk-quarantine", name+"-anomaly-receipt")
			if err := os.MkdirAll(entry, 0o700); err != nil {
				t.Fatal(err)
			}
			if test.payload != "" {
				if err := os.WriteFile(filepath.Join(entry, "quarantine.json"), []byte(test.payload), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: newFakeComputerDiskSystem()}
			inventory := ResourceInventory{}
			if err := engine.inventoryComputerDiskResources(&inventory); err != nil {
				t.Fatal(err)
			}
			if len(inventory.ComputerQuarantines) != 0 || len(inventory.ComputerDiskAnomalies) != 1 ||
				!strings.Contains(inventory.ComputerDiskAnomalies[0], "quarantine_authority_invalid") {
				t.Fatalf("invalid quarantine inventory = %+v", inventory)
			}
			if err := engine.sweepComputerDisks(t.Context(), "invalid-quarantine"); err != nil {
				t.Fatal(err)
			}
			if !slices.ContainsFunc(engine.computerDiskSweepEvidence, func(item SweepEvidence) bool {
				return item.ID == filepath.Base(entry) && item.Action == SweepActionRetained && item.Method == "quarantine_authority_invalid"
			}) {
				t.Fatalf("invalid quarantine evidence = %+v", engine.computerDiskSweepEvidence)
			}
		})
	}
}

func TestComputerDiskQuarantineCrashBoundariesRetainTypedReceipt(t *testing.T) {
	for _, phase := range []computerDiskQuarantinePhase{computerDiskQuarantineRecordWritten, computerDiskQuarantineRenamed} {
		t.Run(string(phase), func(t *testing.T) {
			root := t.TempDir()
			request := growTestRequest(16 << 20)
			imagePath := prepareGrowTestImage(t, root, request)
			diskRoot := filepath.Dir(imagePath)
			name, _ := deterministicComputerDiskName(request.Storage)
			crash := errors.New("injected quarantine crash")
			engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: newFakeComputerDiskSystem(),
				computerQuarantineHook: func(observed computerDiskQuarantinePhase) error {
					if observed == phase {
						return crash
					}
					return nil
				}}
			if err := engine.quarantineComputerDiskAnomaly(diskRoot, name, request.Storage, "allocation_mismatch"); !errors.Is(err, crash) {
				t.Fatalf("checkpoint error = %v", err)
			}
			if phase == computerDiskQuarantineRecordWritten {
				if _, err := os.Stat(filepath.Join(diskRoot, "quarantine.json")); err != nil {
					t.Fatalf("pre-rename receipt missing: %v", err)
				}
				engine.computerQuarantineHook = nil
				if err := engine.sweepComputerDisks(t.Context(), "restart-after-record"); err != nil {
					t.Fatal(err)
				}
			}
			restarted := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: newFakeComputerDiskSystem(), attempts: make(map[string]*containerdAttempt)}
			for restart := 0; restart < 2; restart++ {
				if err := restarted.sweepComputerDisks(t.Context(), fmt.Sprintf("restart-%d", restart)); err != nil {
					t.Fatal(err)
				}
				inventory := ResourceInventory{}
				if err := restarted.inventoryComputerDiskResources(&inventory); err != nil || !slices.Contains(inventory.ComputerQuarantines, name) {
					t.Fatalf("restart %d quarantine inventory=%+v err=%v", restart, inventory, err)
				}
			}
		})
	}
}

func TestComputerDiskConsumersFailClosedOnTypedQuarantineAndRemovalClears(t *testing.T) {
	tests := []struct {
		name      string
		target    func(ComputerStorageReference) ComputerStorageReference
		operation func(*ContainerdEngine, ComputerStorageReference) error
		removes   bool
	}{
		{name: "attach", operation: func(engine *ContainerdEngine, storage ComputerStorageReference) error {
			_, err := engine.attachComputerDisk(t.Context(), storage, testComputerAuthority("attempt", "fence", "boot"))
			return err
		}},
		{name: "grow", operation: func(engine *ContainerdEngine, storage ComputerStorageReference) error {
			request := growTestRequest(storage.DiskBytes * 2)
			request.Storage = storage
			_, err := engine.GrowComputerStorage(t.Context(), request)
			return err
		}},
		{name: "copy", operation: func(engine *ContainerdEngine, storage ComputerStorageReference) error {
			_, err := engine.CopyComputerStorage(t.Context(), CopyComputerStorageRequest{Operation: "clone", BackupID: "backup", CopyID: "copy",
				SourceComputerID: "source", SourceStorageID: "source-storage", SourceGeneration: 1, SourceSize: 4096,
				SourceDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Destination: storage,
				Authority: ComputerStorageCopyAuthority{NodeID: "node", BootSessionID: "boot", HelperGeneration: 1, RootInstanceID: "root", JobID: "job", OperationRevision: storage.IntentRevision, CleanupFence: "fence"}})
			return err
		}},
		{name: "reset successor", target: func(storage ComputerStorageReference) ComputerStorageReference {
			storage.StorageGeneration++
			return storage
		}, operation: func(engine *ContainerdEngine, storage ComputerStorageReference) error {
			predecessor := storage
			predecessor.StorageGeneration--
			_, err := engine.ResetComputerStorage(t.Context(), ResetComputerStorageRequest{Storage: predecessor, NewGeneration: storage.StorageGeneration,
				Authority: ComputerStorageResetAuthority{NodeID: "node", BootSessionID: "boot", HelperGeneration: 1, RootInstanceID: "root", JobID: "job", PriorJobID: "prior", IntentRevision: storage.IntentRevision, CleanupFence: "fence"}})
			return err
		}},
		{name: "custody", operation: func(engine *ContainerdEngine, storage ComputerStorageReference) error {
			_, err := engine.ExportComputerCustody(t.Context(), ExportComputerCustodyRequest{ExportID: "export", BackupID: "backup", CopyID: "copy", Storage: storage,
				SourceSize: 4096, SourceDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", ExternalPath: "/tmp/export", JobSpecHash: "hash",
				Authority: ComputerCustodyExportAuthority{HelperGeneration: 1}})
			return err
		}},
		{name: "authorized removal", removes: true, operation: func(engine *ContainerdEngine, storage ComputerStorageReference) error {
			return engine.deleteComputerDisk(storage, ManagedVolumeRemovalAuthority{NodeID: "node", BootSessionID: "boot", JobID: "job", PriorJobID: "prior", RemovalGeneration: 1, CleanupFence: "fence"})
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			storage := testComputerStorage()
			if test.target != nil {
				storage = test.target(storage)
			}
			name, _ := deterministicComputerDiskName(storage)
			diskRoot := filepath.Join(root, "computer-disks", name)
			if err := os.MkdirAll(diskRoot, 0o700); err != nil {
				t.Fatal(err)
			}
			engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: newFakeComputerDiskSystem(), capacityReservations: make(map[string]*capacityReservation), attempts: make(map[string]*containerdAttempt)}
			if err := engine.quarantineComputerDiskAnomaly(diskRoot, name, storage, "allocation_mismatch"); err != nil {
				t.Fatal(err)
			}
			err := test.operation(engine, storage)
			if test.removes {
				if err != nil {
					t.Fatal(err)
				}
				if quarantined, err := computerDiskQuarantined(root, storage); err != nil || quarantined {
					t.Fatalf("authorized removal left quarantine=%t err=%v", quarantined, err)
				}
				return
			}
			var quarantined *ComputerStorageQuarantinedError
			if !errors.As(err, &quarantined) {
				t.Fatalf("consumer error = %v", err)
			}
		})
	}
}

func TestComputerDiskQuarantineExpiryDropsPayloadButKeepsGenerationTombstone(t *testing.T) {
	root := t.TempDir()
	storage := testComputerStorage()
	name, _ := deterministicComputerDiskName(storage)
	diskRoot := filepath.Join(root, "computer-disks", name)
	if err := os.MkdirAll(diskRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(diskRoot, "disk.ext4"), []byte("tenant bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}}
	if err := engine.quarantineComputerDiskAnomaly(diskRoot, name, storage, "allocation_mismatch"); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(filepath.Join(root, "computer-disk-quarantine"))
	if err != nil || len(entries) != 1 {
		t.Fatalf("quarantine entries=%v err=%v", entries, err)
	}
	quarantineRoot := filepath.Join(root, "computer-disk-quarantine", entries[0].Name())
	receiptPath := filepath.Join(quarantineRoot, "quarantine.json")
	payload, err := os.ReadFile(receiptPath)
	if err != nil {
		t.Fatal(err)
	}
	var receipt computerDiskQuarantineReceipt
	if err := json.Unmarshal(payload, &receipt); err != nil {
		t.Fatal(err)
	}
	receipt.CreatedAt = time.Now().Add(-defaultComputerDiskQuarantineRetention - time.Hour)
	receipt.RetainUntil = receipt.CreatedAt.Add(defaultComputerDiskQuarantineRetention)
	payload, _ = json.Marshal(receipt)
	if err := os.WriteFile(receiptPath, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := engine.expireComputerDiskQuarantinePayloads(t.Context()); err != nil {
		t.Fatal(err)
	}
	children, err := os.ReadDir(quarantineRoot)
	if err != nil || len(children) != 2 || children[0].Name() != "attachment.lock" || children[1].Name() != "quarantine.json" {
		t.Fatalf("expired quarantine children=%v err=%v", children, err)
	}
	updated, err := readAndValidateComputerDiskQuarantineReceipt(receiptPath)
	if err != nil || updated.PayloadDroppedAt == nil || !slices.ContainsFunc(engine.computerDiskSweepEvidence, func(item SweepEvidence) bool { return item.Action == SweepActionQuarantinePayloadDropped }) {
		t.Fatalf("payload drop receipt=%+v evidence=%+v err=%v", updated, engine.computerDiskSweepEvidence, err)
	}
	if quarantined, err := computerDiskQuarantined(root, storage); err != nil || !quarantined {
		t.Fatalf("expired generation tombstone=%t err=%v", quarantined, err)
	}
	inventory := ResourceInventory{}
	if err := engine.inventoryComputerDiskResources(&inventory); err != nil || len(inventory.ComputerStorageQuarantined) != 1 ||
		!inventory.ComputerStorageQuarantined[0].PayloadDroppedAt.Equal(updated.PayloadDroppedAt.UTC()) {
		t.Fatalf("payload drop inventory=%+v err=%v", inventory.ComputerStorageQuarantined, err)
	}
}

func TestComputerDiskQuarantineExpiryNeverDeletesPayloadUnderInvalidAuthority(t *testing.T) {
	root := t.TempDir()
	entry := filepath.Join(root, "computer-disk-quarantine", "wefty-computer-disk-forged-anomaly-receipt")
	if err := os.MkdirAll(entry, 0o700); err != nil {
		t.Fatal(err)
	}
	payloadPath := filepath.Join(entry, "disk.ext4")
	if err := os.WriteFile(payloadPath, []byte("tenant bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	receipt := computerDiskQuarantineReceipt{Kind: computerDiskQuarantineKindGeneration, ReceiptID: "receipt", DiskName: "wefty-computer-disk-forged",
		Reason: "allocation_mismatch", CreatedAt: time.Now().Add(-48 * time.Hour), RetainUntil: time.Now().Add(-24 * time.Hour)}
	payload, _ := json.Marshal(receipt)
	if err := os.WriteFile(filepath.Join(entry, "quarantine.json"), payload, 0o600); err != nil {
		t.Fatal(err)
	}
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}}
	if err := engine.expireComputerDiskQuarantinePayloads(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(payloadPath); err != nil || string(got) != "tenant bytes" {
		t.Fatalf("invalid authority payload = %q err=%v", got, err)
	}
}

func TestComputerDiskQuarantineGCFailureIsEvidenceNotSweepFailure(t *testing.T) {
	root := t.TempDir()
	storage := testComputerStorage()
	name, _ := deterministicComputerDiskName(storage)
	diskRoot := filepath.Join(root, "computer-disks", name)
	if err := os.MkdirAll(diskRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(diskRoot, "disk.ext4"), []byte("tenant bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}}
	if err := engine.quarantineComputerDiskAnomaly(diskRoot, name, storage, "allocation_mismatch"); err != nil {
		t.Fatal(err)
	}
	entries, _ := os.ReadDir(filepath.Join(root, "computer-disk-quarantine"))
	quarantineRoot := filepath.Join(root, "computer-disk-quarantine", entries[0].Name())
	receiptPath := filepath.Join(quarantineRoot, "quarantine.json")
	receipt, err := readAndValidateComputerDiskQuarantineReceipt(receiptPath)
	if err != nil {
		t.Fatal(err)
	}
	receipt.CreatedAt = time.Now().Add(-defaultComputerDiskQuarantineRetention - time.Hour)
	receipt.RetainUntil = receipt.CreatedAt.Add(defaultComputerDiskQuarantineRetention)
	payload, _ := json.Marshal(receipt)
	if err := os.WriteFile(receiptPath, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	removeAttempts := 0
	engine.computerQuarantineRemoveAll = func(string) error {
		removeAttempts++
		return errors.New("injected remove failure")
	}
	for attempt := 1; attempt <= defaultComputerDiskQuarantineGCFailures+1; attempt++ {
		if err := engine.expireComputerDiskQuarantinePayloads(t.Context()); err != nil {
			t.Fatalf("GC attempt %d failed whole sweep: %v", attempt, err)
		}
	}
	if _, err := os.Stat(filepath.Join(quarantineRoot, "disk.ext4")); err != nil {
		t.Fatalf("failed GC removed payload: %v", err)
	}
	updated, err := readAndValidateComputerDiskQuarantineReceipt(receiptPath)
	if err != nil || updated.GCFailures != defaultComputerDiskQuarantineGCFailures || updated.GCFirstFailedAt == nil || updated.GCEscalatedAt == nil {
		t.Fatalf("bounded GC receipt = %+v err=%v", updated, err)
	}
	if removeAttempts != defaultComputerDiskQuarantineGCFailures || !slices.ContainsFunc(engine.computerDiskSweepEvidence, func(item SweepEvidence) bool {
		return item.Action == SweepActionQuarantineGCEscalated && item.Method == "remove_payload" &&
			item.GCEvidenceStorage == ComputerDiskQuarantineGCEvidencePrimary
	}) || !slices.ContainsFunc(engine.computerDiskSweepEvidence, func(item SweepEvidence) bool {
		return item.Action == SweepActionRetained && item.Method == "quarantine_gc_escalated" &&
			item.GCEvidenceStorage == ComputerDiskQuarantineGCEvidencePrimary && item.GCStopReason == ComputerDiskQuarantineGCStopFailureLimit
	}) {
		t.Fatalf("GC failure evidence = %+v", engine.computerDiskSweepEvidence)
	}
}

func TestComputerDiskQuarantineGCReceiptPublicationFailureReachesDurableBound(t *testing.T) {
	root := t.TempDir()
	storage := testComputerStorage()
	name, _ := deterministicComputerDiskName(storage)
	diskRoot := filepath.Join(root, "computer-disks", name)
	if err := os.MkdirAll(diskRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	payloadPath := filepath.Join(diskRoot, "disk.ext4")
	if err := os.WriteFile(payloadPath, []byte("tenant bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	seed := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}}
	if err := seed.quarantineComputerDiskAnomaly(diskRoot, name, storage, "allocation_mismatch"); err != nil {
		t.Fatal(err)
	}
	entries, _ := os.ReadDir(filepath.Join(root, "computer-disk-quarantine"))
	quarantineRoot := filepath.Join(root, "computer-disk-quarantine", entries[0].Name())
	receiptPath := filepath.Join(quarantineRoot, "quarantine.json")
	receipt, err := readAndValidateComputerDiskQuarantineReceipt(receiptPath)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	receipt.CreatedAt = now.Add(-defaultComputerDiskQuarantineRetention - time.Hour)
	receipt.RetainUntil = receipt.CreatedAt.Add(defaultComputerDiskQuarantineRetention)
	payload, _ := json.Marshal(receipt)
	if err := os.WriteFile(receiptPath, payload, 0o600); err != nil {
		t.Fatal(err)
	}

	for attempt := 1; attempt <= defaultComputerDiskQuarantineGCFailures+1; attempt++ {
		engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root, Clock: newManualClock(now.Add(time.Duration(attempt) * time.Minute))}}
		engine.computerQuarantineWrite = func(directory, temporaryPattern, target string, payload []byte, mode os.FileMode) error {
			if directory == quarantineRoot && target == "quarantine.json" {
				return errors.New("injected quarantine receipt publication failure")
			}
			return writeDurableFile(directory, temporaryPattern, target, payload, mode)
		}
		if err := engine.expireComputerDiskQuarantinePayloads(t.Context()); err != nil {
			t.Fatalf("GC publication attempt %d failed whole sweep: %v", attempt, err)
		}
		if attempt == defaultComputerDiskQuarantineGCFailures && !slices.ContainsFunc(engine.computerDiskSweepEvidence, func(item SweepEvidence) bool {
			return item.Action == SweepActionQuarantineGCEscalated && item.Method == "record_payload_drop" &&
				item.GCEvidenceStorage == ComputerDiskQuarantineGCEvidenceMirror
		}) {
			t.Fatalf("GC publication attempt %d did not escalate: %+v", attempt, engine.computerDiskSweepEvidence)
		}
		if attempt == defaultComputerDiskQuarantineGCFailures+1 && !slices.ContainsFunc(engine.computerDiskSweepEvidence, func(item SweepEvidence) bool {
			return item.Action == SweepActionRetained && item.Method == "quarantine_gc_escalated" &&
				item.GCEvidenceStorage == ComputerDiskQuarantineGCEvidenceMirror && item.GCStopReason == ComputerDiskQuarantineGCStopFailureLimit
		}) {
			t.Fatalf("GC publication failure was retried past its bound: %+v", engine.computerDiskSweepEvidence)
		}
	}
	if _, err := os.Stat(filepath.Join(quarantineRoot, "disk.ext4")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("payload removal was not the injected failure: %v", err)
	}
	mirrored, err := readComputerDiskQuarantineGCFailure(quarantineRoot)
	if err != nil || mirrored.GCFailures != defaultComputerDiskQuarantineGCFailures || mirrored.GCEscalatedAt == nil {
		t.Fatalf("durable mirrored GC bound = %+v err=%v", mirrored, err)
	}
	observer := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: newFakeComputerDiskSystem()}
	var inventory ResourceInventory
	if err := observer.inventoryComputerDiskResources(&inventory); err != nil {
		t.Fatal(err)
	}
	inventoryJSON, err := json.Marshal(inventory.ComputerStorageQuarantined)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(inventoryJSON, []byte(`"gc_failures":3`)) || !bytes.Contains(inventoryJSON, []byte(`"gc_escalated_at":`)) ||
		!bytes.Contains(inventoryJSON, []byte(`"gc_evidence_storage":"mirror_receipt"`)) {
		t.Fatalf("operator inventory omitted mirrored GC escalation: %s", inventoryJSON)
	}
	remover := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: newFakeComputerDiskSystem()}
	if err := remover.deleteComputerDisk(storage, ManagedVolumeRemovalAuthority{NodeID: "node", BootSessionID: "boot", JobID: "job", PriorJobID: "prior", RemovalGeneration: 1, CleanupFence: "fence"}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(computerDiskQuarantineGCFailurePath(quarantineRoot)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("authorized removal left mirrored GC evidence: %v", err)
	}
}

func TestComputerDiskQuarantineGCMirrorPublicationFailureUsesPrimaryReceipt(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("permission-denial GC proof requires the non-root helper test lane")
	}
	root := t.TempDir()
	storage := testComputerStorage()
	name, _ := deterministicComputerDiskName(storage)
	diskRoot := filepath.Join(root, "computer-disks", name)
	if err := os.MkdirAll(diskRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(diskRoot, "disk.ext4"), []byte("tenant bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	seed := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}}
	if err := seed.quarantineComputerDiskAnomaly(diskRoot, name, storage, "allocation_mismatch"); err != nil {
		t.Fatal(err)
	}
	quarantineParent := filepath.Join(root, "computer-disk-quarantine")
	entries, err := os.ReadDir(quarantineParent)
	if err != nil || len(entries) != 1 {
		t.Fatalf("quarantine entries=%v err=%v", entries, err)
	}
	quarantineRoot := filepath.Join(quarantineParent, entries[0].Name())
	receiptPath := filepath.Join(quarantineRoot, "quarantine.json")
	receipt, err := readAndValidateComputerDiskQuarantineReceipt(receiptPath)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	receipt.CreatedAt = now.Add(-defaultComputerDiskQuarantineRetention - time.Hour)
	receipt.RetainUntil = receipt.CreatedAt.Add(defaultComputerDiskQuarantineRetention)
	payload, _ := json.Marshal(receipt)
	if err := os.WriteFile(receiptPath, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(quarantineParent, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(quarantineParent, 0o700) })

	removeAttempts := 0
	for attempt := 1; attempt <= defaultComputerDiskQuarantineGCFailures+1; attempt++ {
		engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root, Clock: newManualClock(now.Add(time.Duration(attempt) * time.Minute))}}
		engine.computerQuarantineRemoveAll = func(string) error {
			removeAttempts++
			return errors.New("injected remove failure")
		}
		if err := engine.expireComputerDiskQuarantinePayloads(t.Context()); err != nil {
			t.Fatalf("GC attempt %d failed whole sweep: %v", attempt, err)
		}
		if attempt == defaultComputerDiskQuarantineGCFailures+1 && !slices.ContainsFunc(engine.computerDiskSweepEvidence, func(item SweepEvidence) bool {
			return item.Action == SweepActionRetained && item.Method == "quarantine_gc_escalated" &&
				item.GCEvidenceStorage == ComputerDiskQuarantineGCEvidencePrimary && item.GCStopReason == ComputerDiskQuarantineGCStopFailureLimit
		}) {
			t.Fatalf("primary receipt did not retain after mirror failure: %+v", engine.computerDiskSweepEvidence)
		}
	}
	updated, err := readAndValidateComputerDiskQuarantineReceipt(receiptPath)
	if err != nil || updated.GCFailures != defaultComputerDiskQuarantineGCFailures || updated.GCEscalatedAt == nil {
		t.Fatalf("primary GC receipt = %+v err=%v", updated, err)
	}
	if removeAttempts != defaultComputerDiskQuarantineGCFailures {
		t.Fatalf("payload removal attempts = %d, want %d", removeAttempts, defaultComputerDiskQuarantineGCFailures)
	}
}

func TestComputerDiskQuarantineGCStopsInMemoryWhenNeitherReceiptIsWritable(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("permission-denial GC proof requires the non-root helper test lane")
	}
	root := t.TempDir()
	storage := testComputerStorage()
	name, _ := deterministicComputerDiskName(storage)
	diskRoot := filepath.Join(root, "computer-disks", name)
	if err := os.MkdirAll(diskRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(diskRoot, "disk.ext4"), []byte("tenant bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root, Clock: newManualClock(now)}}
	if err := engine.quarantineComputerDiskAnomaly(diskRoot, name, storage, "allocation_mismatch"); err != nil {
		t.Fatal(err)
	}
	quarantineParent := filepath.Join(root, "computer-disk-quarantine")
	entries, err := os.ReadDir(quarantineParent)
	if err != nil || len(entries) != 1 {
		t.Fatalf("quarantine entries=%v err=%v", entries, err)
	}
	quarantineRoot := filepath.Join(quarantineParent, entries[0].Name())
	receiptPath := filepath.Join(quarantineRoot, "quarantine.json")
	receipt, err := readAndValidateComputerDiskQuarantineReceipt(receiptPath)
	if err != nil {
		t.Fatal(err)
	}
	receipt.CreatedAt = now.Add(-defaultComputerDiskQuarantineRetention - time.Hour)
	receipt.RetainUntil = receipt.CreatedAt.Add(defaultComputerDiskQuarantineRetention)
	payload, _ := json.Marshal(receipt)
	if err := os.WriteFile(receiptPath, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(quarantineParent, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(quarantineParent, 0o700) })
	engine.computerQuarantineWrite = func(string, string, string, []byte, os.FileMode) error {
		return errors.New("injected primary receipt failure")
	}
	removeAttempts := 0
	engine.computerQuarantineRemoveAll = func(string) error {
		removeAttempts++
		return errors.New("injected remove failure")
	}
	for attempt := 1; attempt <= defaultComputerDiskQuarantineGCFailures+1; attempt++ {
		if err := engine.expireComputerDiskQuarantinePayloads(t.Context()); err != nil {
			t.Fatalf("GC attempt %d failed whole sweep: %v", attempt, err)
		}
	}
	if removeAttempts != defaultComputerDiskQuarantineGCFailures {
		t.Fatalf("payload removal attempts = %d, want in-memory bound %d; evidence=%+v", removeAttempts, defaultComputerDiskQuarantineGCFailures, engine.computerDiskSweepEvidence)
	}
	if !slices.ContainsFunc(engine.computerDiskSweepEvidence, func(item SweepEvidence) bool {
		return item.Action == SweepActionQuarantineGCEscalated && item.Method == "remove_payload" &&
			item.GCEvidenceStorage == ComputerDiskQuarantineGCEvidenceMemory
	}) || !slices.ContainsFunc(engine.computerDiskSweepEvidence, func(item SweepEvidence) bool {
		return item.Action == SweepActionRetained && item.Method == "quarantine_gc_escalated" &&
			item.GCEvidenceStorage == ComputerDiskQuarantineGCEvidenceMemory && item.GCStopReason == ComputerDiskQuarantineGCStopFailureLimit
	}) {
		t.Fatalf("in-memory GC escalation evidence = %+v", engine.computerDiskSweepEvidence)
	}
	var inventory ResourceInventory
	if err := engine.inventoryComputerDiskResources(&inventory); err != nil || len(inventory.ComputerStorageQuarantined) != 1 {
		t.Fatalf("in-memory GC inventory=%+v err=%v", inventory.ComputerStorageQuarantined, err)
	}
	gc := inventory.ComputerStorageQuarantined[0]
	if gc.GCFailures != defaultComputerDiskQuarantineGCFailures || gc.GCFirstFailedAt.IsZero() || gc.GCEscalatedAt.IsZero() ||
		gc.GCLastFailure != "remove_payload" || gc.GCEvidenceStorage != ComputerDiskQuarantineGCEvidenceMemory ||
		gc.GCRetryStopReason != ComputerDiskQuarantineGCStopFailureLimit || !gc.GCRetryStoppedAt.Equal(gc.GCEscalatedAt) {
		t.Fatalf("in-memory GC facts = %+v", gc)
	}
}

func TestComputerDiskQuarantineGCUnrecordedRetryHasAbsoluteWallClockBound(t *testing.T) {
	runtimeRoot := t.TempDir()
	name := "wefty-computer-disk-example"
	quarantineRoot := filepath.Join(runtimeRoot, "computer-disk-quarantine", name+"-anomaly-receipt")
	if err := os.MkdirAll(quarantineRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(quarantineRoot, "disk.ext4"), []byte("tenant bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	receipt := computerDiskQuarantineReceipt{
		Kind: computerDiskQuarantineKindAuthority, ReceiptID: "receipt", DiskName: name,
		Reason: "manifest_missing", CreatedAt: now.Add(-2 * defaultComputerDiskQuarantineRetention),
		RetainUntil: now.Add(-defaultComputerDiskQuarantineGCUnrecordedRetryWindow),
	}
	payload, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(quarantineRoot, "quarantine.json"), payload, 0o600); err != nil {
		t.Fatal(err)
	}
	removeAttempts := 0
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: runtimeRoot, Clock: newManualClock(now)}, diskSystem: newFakeComputerDiskSystem()}
	engine.computerQuarantineRemoveAll = func(string) error {
		removeAttempts++
		return errors.New("unexpected removal attempt")
	}
	if err := engine.expireComputerDiskQuarantinePayloads(t.Context()); err != nil {
		t.Fatal(err)
	}
	if removeAttempts != 0 || !slices.ContainsFunc(engine.computerDiskSweepEvidence, func(item SweepEvidence) bool {
		return item.Action == SweepActionRetained && item.Method == "quarantine_gc_unrecorded_retry_window_elapsed" &&
			item.GCStopReason == ComputerDiskQuarantineGCStopUnrecordedWindow
	}) {
		t.Fatalf("absolute unrecorded GC bound attempted removal=%d evidence=%+v", removeAttempts, engine.computerDiskSweepEvidence)
	}
	var inventory ResourceInventory
	if err := engine.inventoryComputerDiskResources(&inventory); err != nil || len(inventory.ComputerStorageQuarantined) != 1 {
		t.Fatalf("absolute GC inventory=%+v err=%v", inventory.ComputerStorageQuarantined, err)
	}
	gc := inventory.ComputerStorageQuarantined[0]
	if gc.GCRetryStopReason != ComputerDiskQuarantineGCStopUnrecordedWindow || !gc.GCRetryStoppedAt.Equal(now) {
		t.Fatalf("absolute GC inventory facts = %+v", gc)
	}
}

func TestLegacyResetQuarantineWithoutManifestNeverDeletesPayload(t *testing.T) {
	root := t.TempDir()
	storage := testComputerStorage()
	name, _ := deterministicComputerDiskName(storage)
	legacyRoot := filepath.Join(root, "computer-disk-quarantine", name+"-reset-2")
	if err := os.MkdirAll(legacyRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(legacyRoot, "disk.ext4"), []byte("legacy tenant bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	createdAt := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	if err := os.Chtimes(legacyRoot, createdAt, createdAt); err != nil {
		t.Fatal(err)
	}
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root, Clock: newManualClock(createdAt.Add(defaultComputerDiskQuarantineRetention))}}
	if err := engine.expireComputerDiskQuarantinePayloads(t.Context()); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(filepath.Join(root, "computer-disk-quarantine"))
	if err != nil || len(entries) != 1 || entries[0].Name() != name+"-reset-2" {
		t.Fatalf("legacy quarantine tombstone changed: entries=%v err=%v", entries, err)
	}
	if payload, err := os.ReadFile(filepath.Join(legacyRoot, "disk.ext4")); err != nil || string(payload) != "legacy tenant bytes" {
		t.Fatalf("legacy payload changed: %q err=%v", payload, err)
	}
	if !slices.ContainsFunc(engine.computerDiskSweepEvidence, func(item SweepEvidence) bool {
		return item.ID == name && item.Action == SweepActionRetained && item.Method == "legacy_reset_quarantine"
	}) {
		t.Fatalf("legacy GC evidence = %+v", engine.computerDiskSweepEvidence)
	}
}

func TestLegacyResetQuarantineRequiresValidatedManifestBeforeRetention(t *testing.T) {
	root := t.TempDir()
	storage := testComputerStorage()
	storage.IntentRevision = 2
	name, _ := deterministicComputerDiskName(storage)
	legacyName := name + "-reset-2"
	legacyRoot := filepath.Join(root, "computer-disk-quarantine", legacyName)
	if err := os.MkdirAll(legacyRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(legacyRoot, "disk.ext4"), []byte("legacy tenant bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	manifest := legacyComputerStorageResetManifest{Version: legacyComputerStorageResetManifestVersion, Storage: storage,
		NewGeneration: storage.StorageGeneration + 1, QuarantineName: legacyName, Phase: "quarantined",
		Authority: ComputerStorageResetAuthority{NodeID: "node-a", BootSessionID: "boot-a", HelperGeneration: 1,
			JobID: "job-a", IntentRevision: storage.IntentRevision, CleanupFence: "reset-fence-a"}}
	manifestPayload, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	manifestRoot := filepath.Join(root, "computer-storage-resets")
	if err := os.MkdirAll(manifestRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(manifestRoot, name+".json"), manifestPayload, 0o600); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	clock := newManualClock(now)
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root, Clock: clock}}
	if err := engine.expireComputerDiskQuarantinePayloads(t.Context()); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(filepath.Join(root, "computer-disk-quarantine"))
	if err != nil || len(entries) != 1 || strings.Contains(entries[0].Name(), "-reset-") {
		t.Fatalf("validated legacy quarantine was not normalized: entries=%v err=%v", entries, err)
	}
	normalizedRoot := filepath.Join(root, "computer-disk-quarantine", entries[0].Name())
	if _, err := os.Stat(filepath.Join(normalizedRoot, "disk.ext4")); err != nil {
		t.Fatalf("validated legacy payload was dropped before retention: %v", err)
	}
	clock.Advance(defaultComputerDiskQuarantineRetention)
	if err := engine.expireComputerDiskQuarantinePayloads(t.Context()); err != nil {
		t.Fatal(err)
	}
	receipt, err := readAndValidateComputerDiskQuarantineReceipt(filepath.Join(normalizedRoot, "quarantine.json"))
	if err != nil || receipt.PayloadDroppedAt == nil {
		t.Fatalf("validated legacy GC receipt = %+v err=%v", receipt, err)
	}
	if _, err := os.Lstat(filepath.Join(normalizedRoot, "disk.ext4")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("validated expired legacy payload remained: %v", err)
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
	removal := ManagedVolumeRemovalAuthority{NodeID: authority.NodeID, BootSessionID: authority.BootSessionID, JobID: authority.JobID, PriorJobID: authority.JobID, RemovalGeneration: 1, CleanupFence: "cleanup"}
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

func TestComputerDiskRemovalBindsSweepReceiptToNamedPriorJob(t *testing.T) {
	for _, test := range []struct {
		name          string
		priorJobID    string
		mutateReceipt func(*computerDiskEvidence)
		wantErr       bool
	}{
		{name: "same-boot helper sweep", priorJobID: "job-1"},
		{name: "wrong prior Job", priorJobID: "job-other", wantErr: true},
		{name: "unnamed prior Job", wantErr: true},
		{name: "missing receipt capability", priorJobID: "job-1", mutateReceipt: func(evidence *computerDiskEvidence) { evidence.ReceiptID = "" }, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			engine, storage, prior := prepareSameBootSweptComputerDisk(t)
			if test.mutateReceipt != nil {
				name, _ := deterministicComputerDiskName(storage)
				root := filepath.Join(engine.config.RuntimeRoot, "computer-disks", name)
				manifest, present, err := readComputerDiskManifest(filepath.Join(root, "attachment.json"))
				if err != nil || !present || manifest.PreviousDetachment == nil {
					t.Fatalf("swept manifest = %+v present=%t err=%v", manifest, present, err)
				}
				test.mutateReceipt(manifest.PreviousDetachment)
				if err := writeComputerDiskManifest(root, manifest); err != nil {
					t.Fatal(err)
				}
			}
			removal := ManagedVolumeRemovalAuthority{NodeID: prior.NodeID, BootSessionID: prior.BootSessionID,
				JobID: "removal-job", PriorJobID: test.priorJobID, RemovalGeneration: 1, CleanupFence: "cleanup"}
			err := engine.deleteComputerDisk(storage, removal)
			if (err != nil) != test.wantErr {
				t.Fatalf("delete Computer disk error = %v, wantErr=%t", err, test.wantErr)
			}
		})
	}
}
