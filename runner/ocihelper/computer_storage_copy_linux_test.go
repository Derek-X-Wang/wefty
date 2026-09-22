//go:build linux

package ocihelper

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func publishedStorageCopySource(t *testing.T) (string, *fakeComputerDiskSystem, CreateComputerBackupResponse) {
	t.Helper()
	root, system, storage, authority := prepareDetachedBackupSource(t)
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system}
	response, err := engine.CreateComputerBackup(t.Context(), backupTestRequest(storage, authority))
	if err != nil {
		t.Fatal(err)
	}
	return root, system, response
}

func TestRealCloneIdentityRekeyChangesMachineIDAndPreservesBrowserProfile(t *testing.T) {
	root := t.TempDir()
	profile := filepath.Join(root, "home", "agent", ".config", "chromium", "Default")
	if err := os.MkdirAll(filepath.Join(root, "etc"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(profile, 0o700); err != nil {
		t.Fatal(err)
	}
	oldMachineID := []byte("0123456789abcdef0123456789abcdef\n")
	browserSecret := []byte("browser-profile-secret-must-remain-byte-identical")
	if err := os.WriteFile(filepath.Join(root, "etc", "machine-id"), oldMachineID, 0o600); err != nil {
		t.Fatal(err)
	}
	markerPath := filepath.Join(profile, "Login Data")
	if err := os.WriteFile(markerPath, browserSecret, 0o600); err != nil {
		t.Fatal(err)
	}
	rekeyed, err := rekeyCloneIdentity(root)
	if err != nil || !rekeyed.OSIdentityRekeyed || rekeyed.MachineIDBeforeDigest == rekeyed.MachineIDAfterDigest {
		t.Fatalf("real identity rekey = %+v err=%v", rekeyed, err)
	}
	newMachineID, err := os.ReadFile(filepath.Join(root, "etc", "machine-id"))
	if err != nil || bytes.Equal(newMachineID, oldMachineID) || len(strings.TrimSpace(string(newMachineID))) != 32 {
		t.Fatalf("observed machine-id = %q err=%v", newMachineID, err)
	}
	marker, err := os.ReadFile(markerPath)
	if err != nil || !bytes.Equal(marker, browserSecret) {
		t.Fatalf("browser marker changed: %q err=%v", marker, err)
	}
}

func TestCloneIdentityRekeyInitializesLegacyStorageWithoutMachineID(t *testing.T) {
	root := t.TempDir()
	facts, err := rekeyCloneIdentity(root)
	if err != nil || !facts.OSIdentityRekeyed || !facts.MachineIDRepaired ||
		facts.MachineIDBeforeDigest == facts.MachineIDAfterDigest {
		t.Fatalf("legacy identity rekey = %+v err=%v", facts, err)
	}
	identity, err := readRegularFile(computerStorageIdentityAt(root).MachineID)
	if err != nil || !validComputerMachineID(identity) {
		t.Fatalf("legacy machine-id = %q err=%v", identity, err)
	}
}

func TestComputerStorageCopyRejectsMutatedStagingBytesBeforeFilesystemMutation(t *testing.T) {
	root, system, source := publishedStorageCopySource(t)
	request := storageCopyTestRequest(source, "clone", source.Receipt.AllocatedSize)
	finalizeCalls := 0
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system,
		computerBackupCopyN: func(destination io.Writer, source io.Reader, size int64) (int64, error) {
			payload, err := io.ReadAll(io.LimitReader(source, size))
			if err != nil {
				return 0, err
			}
			payload[4096] ^= 0xff
			written, err := destination.Write(payload)
			return int64(written), err
		}, storageCopyFinalize: func(context.Context, string, string, string, int64, bool) (computerStorageCopyFacts, error) {
			finalizeCalls++
			return computerStorageCopyFacts{}, nil
		}}
	if _, err := engine.CopyComputerStorage(t.Context(), request); err == nil || !strings.Contains(err.Error(), "staging digest mismatch") {
		t.Fatalf("mutated staging error = %v", err)
	}
	if finalizeCalls != 0 {
		t.Fatalf("filesystem mutation ran %d times before staging digest verification", finalizeCalls)
	}
}

func storageCopyTestRequest(response CreateComputerBackupResponse, operation string, destinationSize int64) CopyComputerStorageRequest {
	receipt := response.Receipt
	request := CopyComputerStorageRequest{Operation: operation, BackupID: receipt.BackupID, CopyID: receipt.CopyID,
		SourceComputerID: receipt.ComputerID, SourceStorageID: receipt.StorageID,
		SourceGeneration: receipt.StorageGeneration, SourceSize: receipt.AllocatedSize, SourceDigest: receipt.ContentDigest,
		Destination: ComputerStorageReference{ComputerID: receipt.ComputerID, StorageID: receipt.StorageID,
			StorageGeneration: receipt.StorageGeneration + 1, IntentRevision: receipt.OperationRevision + 1, DiskBytes: destinationSize},
		Authority: ComputerStorageCopyAuthority{NodeID: receipt.NodeID, BootSessionID: "copy-boot",
			HelperGeneration: 3, RootInstanceID: receipt.RootInstanceID, JobID: receipt.JobID,
			OperationRevision: receipt.OperationRevision + 1, CleanupFence: "storage-copy-fence"}}
	if operation == "clone" {
		request.Destination.ComputerID = "clone-computer"
		request.Destination.StorageID = "clone-storage"
		request.Destination.StorageGeneration = 1
	}
	return request
}

func fakeCloneFinalize(_ *testing.T) func(context.Context, string, string, string, int64, bool) (computerStorageCopyFacts, error) {
	return func(_ context.Context, operation, imagePath, _ string, _ int64, expanded bool) (computerStorageCopyFacts, error) {
		facts := computerStorageCopyFacts{OSIdentityRekeyed: operation == "clone", FilesystemExpanded: expanded}
		if operation == "clone" {
			facts.MachineIDBeforeDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
			facts.MachineIDAfterDigest = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
		}
		file, err := os.OpenFile(imagePath, os.O_WRONLY, 0)
		if err != nil {
			return facts, err
		}
		if operation != "clone" {
			return facts, file.Close()
		}
		_, writeErr := file.WriteAt([]byte("machine-id=rekeyed\n"), 8192)
		return facts, errors.Join(writeErr, file.Sync(), file.Close())
	}
}

func TestComputerStorageCopyResumesEveryCrashBoundaryAndPreservesBrowserBytes(t *testing.T) {
	for _, operation := range []string{"restore", "clone"} {
		for _, checkpoint := range []computerStorageCopyPhase{
			computerStorageCopyReserved, computerStorageCopyAllocated, computerStorageCopyCopied,
			computerStorageCopySourceVerified, computerStorageCopyMountedRekey, computerStorageCopyManifestWritten, computerStorageCopyPublished,
			computerStorageCopyIdentityRekeyed, computerStorageCopyExpanded,
		} {
			if operation == "restore" && (checkpoint == computerStorageCopyMountedRekey || checkpoint == computerStorageCopyIdentityRekeyed || checkpoint == computerStorageCopyExpanded) {
				continue
			}
			t.Run(operation+"/"+string(checkpoint), func(t *testing.T) {
				root, system, source := publishedStorageCopySource(t)
				destinationSize := source.Receipt.AllocatedSize
				if operation == "clone" {
					destinationSize += 1 << 20
				}
				request := storageCopyTestRequest(source, operation, destinationSize)
				engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system,
					storageCopyFinalize: fakeCloneFinalize(t)}
				crash := errors.New("injected Storage copy crash")
				fired := false
				engine.storageCopyHook = func(observed computerStorageCopyPhase) error {
					if observed == checkpoint && !fired {
						fired = true
						return crash
					}
					return nil
				}
				if _, err := engine.CopyComputerStorage(t.Context(), request); !errors.Is(err, crash) {
					t.Fatalf("checkpoint %q error = %v, want injected crash", checkpoint, err)
				}
				name, _ := deterministicComputerDiskName(request.Destination)
				rootPath := filepath.Join(root, "computer-disks", name)
				if _, err := os.Lstat(filepath.Join(rootPath, "storage-copy.json")); err != nil {
					t.Fatalf("checkpoint %q left untracked destination: %v", checkpoint, err)
				}
				engine = &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system,
					storageCopyFinalize: fakeCloneFinalize(t)}
				response, err := engine.CopyComputerStorage(t.Context(), request)
				if err != nil || response.Receipt.Kind != "computer_storage_copy_verified" ||
					response.Receipt.OSIdentityRekeyed != (operation == "clone") ||
					!response.Receipt.SourceUnchanged || !response.Receipt.DestinationPrepared ||
					response.Receipt.PreparationReceipt || response.Receipt.DestinationChown ||
					response.Receipt.FilesystemExpanded != (operation == "clone") ||
					response.Receipt.SourceDigest != source.Receipt.ContentDigest {
					t.Fatalf("resumed Storage copy = %+v err=%v", response, err)
				}
				payload, err := os.ReadFile(filepath.Join(rootPath, "disk.ext4"))
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Contains(payload, []byte("browser-secret=survives")) ||
					!bytes.Contains(payload, []byte("user-marker=alice")) ||
					!bytes.Contains(payload, []byte("old-credential=copied-but-not-authority")) {
					t.Fatal("Storage copy did not preserve user, browser, and deliberately copied old credential bytes")
				}
				if operation == "clone" && !bytes.Contains(payload, []byte("machine-id=rekeyed")) {
					t.Fatal("clone did not narrowly rekey OS identity")
				}
			})
		}
	}
}

func TestComputerStorageCopyReturnsDeferredAfterStartupRecoveryDefers(t *testing.T) {
	root, system, source := publishedStorageCopySource(t)
	request := storageCopyTestRequest(source, "clone", source.Receipt.AllocatedSize)
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system,
		storageCopyFinalize: fakeCloneFinalize(t)}
	crash := errors.New("injected helper runtime loss")
	engine.storageCopyHook = func(phase computerStorageCopyPhase) error {
		if phase == computerStorageCopyAllocated {
			return crash
		}
		return nil
	}
	if _, err := engine.CopyComputerStorage(t.Context(), request); !errors.Is(err, crash) {
		t.Fatalf("initial copy error = %v", err)
	}
	name, _ := deterministicComputerDiskName(request.Destination)
	destinationRoot := filepath.Join(root, "computer-disks", name)
	manifest, present, err := readComputerStorageCopyManifest(filepath.Join(destinationRoot, "storage-copy.json"))
	if err != nil || !present {
		t.Fatalf("durable copy manifest = %+v present=%t err=%v", manifest, present, err)
	}
	manifest.Recovery = computerStorageRecoveryDeferral{Attempts: 1, FirstDeferredAt: time.Now().UTC(), Reason: "operational_failure"}
	if err := writeComputerStorageCopyManifest(destinationRoot, manifest); err != nil {
		t.Fatal(err)
	}
	_, err = engine.CopyComputerStorage(t.Context(), request)
	var deferred *ComputerStorageResumeDeferredError
	if !errors.As(err, &deferred) || deferred.Storage != request.Destination {
		t.Fatalf("copy after deferred startup recovery = %T %v", err, err)
	}
}

func TestStartupCompletesCopyKilledAfterAttachmentPublication(t *testing.T) {
	root, system, source := publishedStorageCopySource(t)
	request := storageCopyTestRequest(source, "restore", source.Receipt.AllocatedSize)
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system,
		storageCopyFinalize: fakeCloneFinalize(t), capacityReservations: make(map[string]*capacityReservation),
		attempts: make(map[string]*containerdAttempt)}
	crash := errors.New("killed after attachment publication")
	engine.storageCopyHook = func(phase computerStorageCopyPhase) error {
		if phase == computerStorageCopyPublished {
			return crash
		}
		return nil
	}
	if _, err := engine.CopyComputerStorage(t.Context(), request); !errors.Is(err, crash) {
		t.Fatalf("copy checkpoint error = %v", err)
	}
	name, _ := deterministicComputerDiskName(request.Destination)
	destinationRoot := filepath.Join(root, "computer-disks", name)
	if _, present, err := readComputerDiskManifest(filepath.Join(destinationRoot, "attachment.json")); err != nil || !present {
		t.Fatalf("published attachment present=%t err=%v", present, err)
	}
	engine = &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system,
		capacityReservations: make(map[string]*capacityReservation), attempts: make(map[string]*containerdAttempt)}
	if err := engine.sweepComputerDisks(t.Context(), "startup-copy"); err != nil {
		t.Fatal(err)
	}
	manifest, present, err := readComputerStorageCopyManifest(filepath.Join(destinationRoot, "storage-copy.json"))
	if err != nil || !present || manifest.Phase != computerStorageCopyPublished || manifest.Receipt == nil {
		t.Fatalf("startup copy record = %+v present=%t err=%v", manifest, present, err)
	}
	if !slices.ContainsFunc(engine.computerDiskSweepEvidence, func(item SweepEvidence) bool {
		return item.ID == name && item.Action == SweepActionResumed && item.Method == "computer_storage_copy"
	}) {
		t.Fatalf("startup copy evidence = %+v", engine.computerDiskSweepEvidence)
	}
}

func TestComputerRestoreRejectsDigestMismatchAndTruncationBeforeDestinationPublication(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(string, CreateComputerBackupResponse) error
	}{
		{name: "digest mismatch", mutate: func(root string, response CreateComputerBackupResponse) error {
			name, _ := deterministicComputerBackupCopyName(response.Receipt.CopyID)
			file, err := os.OpenFile(filepath.Join(root, "computer-backups", name, "backup.ext4"), os.O_WRONLY, 0)
			if err != nil {
				return err
			}
			_, writeErr := file.WriteAt([]byte("tampered"), 4096)
			return errors.Join(writeErr, file.Close())
		}},
		{name: "truncation", mutate: func(root string, response CreateComputerBackupResponse) error {
			name, _ := deterministicComputerBackupCopyName(response.Receipt.CopyID)
			return os.Truncate(filepath.Join(root, "computer-backups", name, "backup.ext4"), response.Receipt.AllocatedSize-1)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			root, system, source := publishedStorageCopySource(t)
			if err := test.mutate(root, source); err != nil {
				t.Fatal(err)
			}
			request := storageCopyTestRequest(source, "restore", source.Receipt.AllocatedSize)
			engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system}
			if _, err := engine.CopyComputerStorage(t.Context(), request); err == nil {
				t.Fatal("corrupt Backup source was restored")
			}
			name, _ := deterministicComputerDiskName(request.Destination)
			if _, err := os.Lstat(filepath.Join(root, "computer-disks", name, "disk.ext4")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("corrupt source published destination: %v", err)
			}
		})
	}
}

// A clone that cannot fit its destination is a capacity fact with its own
// typed receipt; every other clone failure keeps the destination and stays on
// the integrity path, and a quarantined generation stays quarantined.
func TestCloneCapacityRefusalIsTypedAndOwnsItsGenerationThroughCleanup(t *testing.T) {
	root, system, source := publishedStorageCopySource(t)
	request := storageCopyTestRequest(source, "clone", source.Receipt.AllocatedSize+(1<<20))
	destinationName, err := deterministicComputerDiskName(request.Destination)
	if err != nil {
		t.Fatal(err)
	}
	destinationRoot := filepath.Join(root, "computer-disks", destinationName)
	stagingPath := filepath.Join(destinationRoot, "disk.ext4.staging")
	var ownershipErr error
	cleanupObserved := false
	creator := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system}
	exhausted := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system,
		computerBackupAllocate: func(path string, size int64) error {
			if size > request.SourceSize {
				return unix.ENOSPC
			}
			return fullyAllocateComputerDisk(path, size)
		}, storageCopyFinalize: fakeCloneFinalize(t),
		// Observed at the one instant a creator could race the refusal: the
		// payload is gone and the tombstone is not written yet. The
		// generation lock inode must still be the one this call holds, so no
		// attachment admission can take the destination and create a disk in
		// the root the refusal is finishing.
		storageCopyHook: func(phase computerStorageCopyPhase) error {
			if phase != computerStorageCopyRefusalCleanup {
				return nil
			}
			cleanupObserved = true
			if _, err := os.Lstat(stagingPath); !errors.Is(err, os.ErrNotExist) {
				ownershipErr = errors.Join(ownershipErr, fmt.Errorf("payload survived refusal cleanup: %v", err))
			}
			if lock, err := openComputerDiskLock(destinationRoot); !errors.Is(err, errComputerStorageAttachmentOwned) {
				closeComputerDiskLock(lock)
				ownershipErr = errors.Join(ownershipErr,
					fmt.Errorf("destination generation was not owned during refusal cleanup: %v", err))
			}
			if _, err := creator.attachComputerDisk(t.Context(), request.Destination,
				testComputerAuthority("racing-creator", "racing-fence", "racing-boot")); !errors.Is(err, errComputerStorageAttachmentOwned) {
				ownershipErr = errors.Join(ownershipErr,
					fmt.Errorf("a creator was admitted during refusal cleanup: %v", err))
			}
			if _, err := os.Lstat(filepath.Join(destinationRoot, "disk.ext4")); !errors.Is(err, os.ErrNotExist) {
				ownershipErr = errors.Join(ownershipErr, fmt.Errorf("a disk appeared during refusal cleanup: %v", err))
			}
			return nil
		}}
	response, err := exhausted.CopyComputerStorage(t.Context(), request)
	if !cleanupObserved || ownershipErr != nil {
		t.Fatalf("refusal cleanup ownership: observed=%t err=%v", cleanupObserved, ownershipErr)
	}
	if err != nil {
		t.Fatalf("over-capacity clone error = %v, want a typed receipt", err)
	}
	receipt := response.Receipt
	if receipt.Kind != "computer_storage_copy_failed_absent" || receipt.Operation != "clone" ||
		receipt.FailureCode != "insufficient_disk" || !receipt.DestinationAbsent ||
		receipt.ObservedAvailableBytes <= 0 || receipt.DestinationSize != request.Destination.DiskBytes ||
		receipt.DestinationDigest != "" || receipt.OSIdentityRekeyed || receipt.FilesystemExpanded ||
		receipt.DestinationPrepared || receipt.HelperGeneration != request.Authority.HelperGeneration {
		t.Fatalf("over-capacity clone receipt = %+v", receipt)
	}
	// The root survives with its lock and tombstone and nothing else.
	entries, err := os.ReadDir(destinationRoot)
	if err != nil {
		t.Fatal(err)
	}
	remaining := []string{}
	for _, entry := range entries {
		remaining = append(remaining, entry.Name())
	}
	slices.Sort(remaining)
	if !slices.Equal(remaining, []string{"attachment.lock", "copy-refused.json"}) {
		t.Fatalf("refused destination root = %v", remaining)
	}
	// Every later reader treats that root as an absent generation.
	if _, err := creator.attachComputerDisk(t.Context(), request.Destination,
		testComputerAuthority("late-creator", "late-fence", "late-boot")); err == nil ||
		!strings.Contains(err.Error(), "refused and holds no bytes") {
		t.Fatalf("preparing a refused generation = %v", err)
	}
	replay, err := exhausted.CopyComputerStorage(t.Context(), request)
	if err != nil || replay.Receipt.ReceiptID != receipt.ReceiptID || replay.Receipt.FailureCode != "insufficient_disk" {
		t.Fatalf("refused clone replay = %+v err=%v", replay.Receipt, err)
	}
	sweeper := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system}
	if err := sweeper.sweepComputerDisks(t.Context(), "refusal-sweep"); err != nil {
		t.Fatalf("sweep with a refused generation = %v", err)
	}
	for _, evidence := range sweeper.computerDiskSweepEvidence {
		if evidence.ID == destinationName && evidence.Action == SweepActionQuarantined {
			t.Fatalf("sweep quarantined a refused generation: %+v", evidence)
		}
	}
	if quarantined, err := computerDiskQuarantined(root, request.Destination); err != nil || quarantined {
		t.Fatalf("refused generation quarantined=%t err=%v", quarantined, err)
	}
	if _, err := os.Lstat(filepath.Join(destinationRoot, "copy-refused.json")); err != nil {
		t.Fatalf("sweep removed the refusal tombstone: %v", err)
	}
}

// A clone whose bytes are in doubt keeps its destination and its ordinary
// integrity path: it is never converted into proven absence, and a generation
// already quarantined stays quarantined.
func TestCloneIntegrityFailureKeepsTheDestinationAndItsQuarantine(t *testing.T) {
	root, system, source := publishedStorageCopySource(t)
	request := storageCopyTestRequest(source, "clone", source.Receipt.AllocatedSize+(1<<20))
	destinationName, err := deterministicComputerDiskName(request.Destination)
	if err != nil {
		t.Fatal(err)
	}
	destinationRoot := filepath.Join(root, "computer-disks", destinationName)
	stalled := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system,
		computerBackupCopyN: func(io.Writer, io.Reader, int64) (int64, error) {
			return 0, errors.New("Computer Storage copy stalled mid-flight")
		}, storageCopyFinalize: fakeCloneFinalize(t)}
	if response, err := stalled.CopyComputerStorage(t.Context(), request); err == nil || response.Receipt.Kind != "" {
		t.Fatalf("integrity failure = %+v err=%v, want an untyped error", response.Receipt, err)
	}
	if _, err := os.Lstat(destinationRoot); err != nil {
		t.Fatalf("integrity failure discarded the destination in doubt: %v", err)
	}
	if err := stalled.quarantineComputerDiskAnomaly(destinationRoot, destinationName, request.Destination, "identity_mismatch"); err != nil {
		t.Fatal(err)
	}
	exhausted := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system,
		computerBackupAllocate: func(path string, size int64) error {
			if size > request.SourceSize {
				return unix.ENOSPC
			}
			return fullyAllocateComputerDisk(path, size)
		}, storageCopyFinalize: fakeCloneFinalize(t)}
	var quarantined *ComputerStorageQuarantinedError
	if response, err := exhausted.CopyComputerStorage(t.Context(), request); !errors.As(err, &quarantined) || response.Receipt.Kind != "" {
		t.Fatalf("quarantined over-capacity clone = %+v err=%v, want the quarantine result", response.Receipt, err)
	}
}

func TestDelayedAttachmentRechecksRefusalAfterAcquiringGeneration(t *testing.T) {
	root, system, source := publishedStorageCopySource(t)
	request := storageCopyTestRequest(source, "clone", source.Receipt.AllocatedSize)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	checked := make(chan struct{})
	resume := make(chan struct{})
	attached := make(chan error, 1)
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system,
		computerBackupAllocate: func(string, int64) error { return unix.ENOSPC },
		computerDiskHook: func(checkpoint computerDiskCheckpoint) error {
			if checkpoint == computerDiskRefusalChecked {
				close(checked)
				select {
				case <-resume:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		}}
	engine.storageCopyHook = func(phase computerStorageCopyPhase) error {
		if phase != computerStorageCopyRefusalCleanup {
			return nil
		}
		go func() {
			attachment, err := engine.attachComputerDisk(ctx, request.Destination,
				testComputerAuthority("delayed-creator", "delayed-fence", "boot"))
			if attachment != nil {
				_ = engine.detachComputerDisk(attachment, computerDiskReapReceipt, "")
			}
			attached <- err
		}()
		select {
		case <-checked:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	response, err := engine.CopyComputerStorage(ctx, request)
	// The attachment has passed its first refusal check. Only now let it
	// acquire the flock that the completed refusal just released.
	close(resume)
	if err != nil || !response.Receipt.DestinationAbsent {
		t.Fatalf("clone refusal = %+v err=%v", response, err)
	}
	select {
	case err := <-attached:
		if err == nil || !strings.Contains(err.Error(), "refused and holds no bytes") {
			t.Fatalf("delayed attachment bypassed refusal: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("delayed attachment did not complete", ctx.Err())
	}
	name, _ := deterministicComputerDiskName(request.Destination)
	if _, err := os.Lstat(filepath.Join(root, "computer-disks", name, "disk.ext4")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("delayed attachment created a disk after refusal: %v", err)
	}
}

// The disk sweep quarantines a generation by renaming diskRoot into
// computer-disk-quarantine while holding that same root's own flock -- the
// same door #512 already closed for the clone-refusal tombstone. Losing that
// race against a concurrent quarantine must fail exactly the same way: the
// fresh, empty diskRoot MkdirAll recreates on a new inode must never be
// mistaken for the durable first-allocation checkpoint the sweep just
// displaced, or attach would format an empty disk under a quarantined
// identity -- the outcome #512 exists to prevent, through the quarantine
// door instead of the refusal door.
func TestDelayedAttachmentRechecksQuarantineAfterAcquiringGeneration(t *testing.T) {
	root := t.TempDir()
	system := newFakeComputerDiskSystem()
	storage := testComputerStorage()
	name, err := deterministicComputerDiskName(storage)
	if err != nil {
		t.Fatal(err)
	}
	diskRoot := filepath.Join(root, "computer-disks", name)

	// Leave behind the durable first-allocation checkpoint: an authority
	// manifest with no image and no attachment history at all.
	seed := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system,
		computerDiskHook: func(checkpoint computerDiskCheckpoint) error {
			if checkpoint == computerDiskManifestBeforeImage {
				return errors.New("injected: stop before the image is written")
			}
			return nil
		}}
	if _, err := seed.attachComputerDisk(t.Context(), storage, testComputerAuthority("seed", "seed-fence", "boot-a")); err == nil {
		t.Fatal("seed attach did not stop before the image")
	}
	if _, err := os.Lstat(filepath.Join(diskRoot, "disk.ext4")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("seed left a disk image behind: %v", err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	checked := make(chan struct{})
	resume := make(chan struct{})
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system,
		computerDiskHook: func(checkpoint computerDiskCheckpoint) error {
			if checkpoint == computerDiskPreLockChecked {
				close(checked)
				select {
				case <-resume:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		}}
	attached := make(chan error, 1)
	go func() {
		attachment, err := engine.attachComputerDisk(ctx, storage, testComputerAuthority("attempt-a", "fence-a", "boot-a"))
		if attachment != nil {
			_ = engine.detachComputerDisk(attachment, computerDiskReapReceipt, "")
		}
		attached <- err
	}()

	select {
	case <-checked:
	case <-ctx.Done():
		t.Fatal("attach never reached its pre-lock checkpoint", ctx.Err())
	}

	// Quarantine diskRoot exactly as the sweep does: take its own flock, then
	// rename it away while still holding that flock.
	sweepLock, err := openComputerDiskLock(diskRoot)
	if err != nil {
		t.Fatalf("sweep could not take diskRoot's flock: %v", err)
	}
	sweeper := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system}
	if err := sweeper.quarantineComputerDiskAnomaly(diskRoot, name, storage, "test_injected"); err != nil {
		t.Fatalf("sweep quarantine: %v", err)
	}
	closeComputerDiskLock(sweepLock)

	// The attachment has passed its pre-lock checks. Only now let it acquire
	// the flock on the fresh, empty root the sweep's rename left behind.
	close(resume)

	var quarantined *ComputerStorageQuarantinedError
	select {
	case err := <-attached:
		if !errors.As(err, &quarantined) {
			t.Fatalf("delayed attachment raced the quarantine sweep: %v, want a typed quarantine refusal", err)
		}
	case <-ctx.Done():
		t.Fatal("delayed attachment did not complete", ctx.Err())
	}

	if _, err := os.Lstat(filepath.Join(diskRoot, "disk.ext4")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("attach formatted a fresh disk under the quarantined identity: %v", err)
	}
	quarantineRoot := filepath.Join(root, "computer-disk-quarantine")
	entries, err := os.ReadDir(quarantineRoot)
	if err != nil {
		t.Fatalf("read quarantine directory: %v", err)
	}
	found := false
	for _, entry := range entries {
		if !entry.IsDir() || !strings.HasPrefix(entry.Name(), name+"-anomaly-") {
			continue
		}
		found = true
		if _, err := os.Lstat(filepath.Join(quarantineRoot, entry.Name(), "disk.ext4")); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("the quarantined root gained a disk image after quarantine: %v", err)
		}
	}
	if !found {
		t.Fatal("quarantine directory does not hold the renamed root")
	}
}

// Unlinking a pathname is not absence. While the copied filesystem is still
// mounted or loop-attached, its bytes remain reachable, so no receipt may
// certify that the destination holds nothing.
func TestCloneCapacityRefusalWithholdsAbsenceWhileBytesRemainReachable(t *testing.T) {
	for _, retained := range []string{"mount", "loop"} {
		t.Run(retained, func(t *testing.T) {
			root, system, source := publishedStorageCopySource(t)
			request := storageCopyTestRequest(source, "clone", source.Receipt.AllocatedSize+(1<<20))
			destinationName, err := deterministicComputerDiskName(request.Destination)
			if err != nil {
				t.Fatal(err)
			}
			destinationRoot := filepath.Join(root, "computer-disks", destinationName)
			mountPath := filepath.Join(root, "computer-copy-mounts", destinationName)
			switch retained {
			case "mount":
				system.mounts[mountPath] = "/dev/loop-test-retained"
			case "loop":
				system.loops["/dev/loop-test-retained"] = filepath.Join(destinationRoot, "disk.ext4.staging")
			}
			engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system,
				computerBackupAllocate: func(path string, size int64) error {
					if size > request.SourceSize {
						return unix.ENOSPC
					}
					return fullyAllocateComputerDisk(path, size)
				}, storageCopyFinalize: fakeCloneFinalize(t)}
			response, err := engine.CopyComputerStorage(t.Context(), request)
			if err == nil || response.Receipt.Kind != "" || !strings.Contains(err.Error(), "remains") {
				t.Fatalf("retained %s produced %+v err=%v, want a withheld receipt", retained, response.Receipt, err)
			}
			if _, err := os.Lstat(destinationRoot); err != nil {
				t.Fatalf("destination with reachable bytes was deleted: %v", err)
			}
		})
	}
}

// ENOSPC is host destination capacity only where this call writes to the
// Node's Computer-disk filesystem. A full filesystem inside the copied image,
// and any failure joined with a failed detach, stay engine failures.
func TestCloneENOSPCOutsideDestinationWritesIsNeverACapacityRefusal(t *testing.T) {
	for _, arm := range []struct {
		name     string
		finalize error
	}{
		{"inside the copied image", unix.ENOSPC},
		{"joined with a failed detach", errors.Join(unix.ENOSPC, errors.New("unmount Computer disk: device or resource busy"))},
		{"tool output naming no space left on device", errors.New("resize2fs: no space left on device while checking")},
	} {
		t.Run(arm.name, func(t *testing.T) {
			root, system, source := publishedStorageCopySource(t)
			request := storageCopyTestRequest(source, "clone", source.Receipt.AllocatedSize)
			destinationName, err := deterministicComputerDiskName(request.Destination)
			if err != nil {
				t.Fatal(err)
			}
			engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system,
				storageCopyFinalize: func(context.Context, string, string, string, int64, bool) (computerStorageCopyFacts, error) {
					return computerStorageCopyFacts{}, arm.finalize
				}}
			response, err := engine.CopyComputerStorage(t.Context(), request)
			if err == nil || response.Receipt.Kind != "" {
				t.Fatalf("%s produced %+v err=%v, want an engine failure", arm.name, response.Receipt, err)
			}
			if _, err := os.Lstat(filepath.Join(root, "computer-disks", destinationName)); err != nil {
				t.Fatalf("%s discarded the destination in doubt: %v", arm.name, err)
			}
		})
	}
}

// A recognized source-validation failure is terminal for the reserved
// destination, so it must carry the same proven-absence receipt the agent
// acknowledges. An engine error would leave the reserved import with nothing
// to close.
func TestCustodyImportSourceValidationFailuresStillProveAbsence(t *testing.T) {
	for _, arm := range []struct {
		name     string
		wantCode string
		tamper   func(t *testing.T, externalRoot string, request *CopyComputerStorageRequest)
	}{
		{"missing manifest", "manifest_invalid", func(t *testing.T, externalRoot string, _ *CopyComputerStorageRequest) {
			if err := os.Remove(filepath.Join(externalRoot, "custody.json")); err != nil {
				t.Fatal(err)
			}
		}},
		{"conflicting manifest", "manifest_invalid", func(_ *testing.T, _ string, request *CopyComputerStorageRequest) {
			request.ManifestDigest = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
		}},
		{"disk digest mismatch", "digest_mismatch", func(t *testing.T, externalRoot string, _ *CopyComputerStorageRequest) {
			disk, err := os.OpenFile(filepath.Join(externalRoot, "storage.ext4"), os.O_WRONLY, 0)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := disk.WriteAt([]byte("tenant-bytes-changed"), 8192); err != nil {
				t.Fatal(err)
			}
			if err := errors.Join(disk.Sync(), disk.Close()); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(arm.name, func(t *testing.T) {
			root, system, source := publishedStorageCopySource(t)
			externalRoot := filepath.Join(t.TempDir(), "operator-custody")
			exportRequest := custodyExportTestRequest(source, externalRoot)
			engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system,
				storageCopyFinalize: importFinalize}
			exported, err := engine.ExportComputerCustody(t.Context(), exportRequest)
			if err != nil {
				t.Fatal(err)
			}
			request := storageCopyTestRequest(source, "import", source.Receipt.AllocatedSize)
			request.Destination.ComputerID = "import-computer"
			request.Destination.StorageID = "import-storage"
			request.Destination.StorageGeneration = 1
			request.ExportID = exportRequest.ExportID
			request.ExternalPath = externalRoot
			request.ManifestDigest = exported.Receipt.ManifestDigest
			request.Authority.JobID = "import-job"
			arm.tamper(t, externalRoot, &request)
			response, err := engine.CopyComputerStorage(t.Context(), request)
			if err != nil || response.Receipt.Kind != "computer_storage_copy_failed_absent" ||
				response.Receipt.FailureCode != arm.wantCode || !response.Receipt.DestinationAbsent ||
				response.Receipt.ObservedAvailableBytes != 0 {
				t.Fatalf("%s = %+v err=%v", arm.name, response.Receipt, err)
			}
			destinationName, err := deterministicComputerDiskName(request.Destination)
			if err != nil {
				t.Fatal(err)
			}
			if err := requireRefusedComputerStorageAbsence(filepath.Join(root, "computer-disks", destinationName)); err != nil {
				t.Fatalf("%s left a destination holding bytes: %v", arm.name, err)
			}
		})
	}
}

// An allocation ENOSPC that arrives joined with a failed close is not a clean
// capacity fact: the second failure must not disappear behind a receipt.
func TestAllocationENOSPCJoinedWithACloseFailureStaysAnEngineFailure(t *testing.T) {
	for _, arm := range []struct {
		name      string
		expansion bool
	}{{"initial allocation", false}, {"expansion allocation", true}} {
		t.Run(arm.name, func(t *testing.T) {
			root, system, source := publishedStorageCopySource(t)
			destinationSize := source.Receipt.AllocatedSize
			if arm.expansion {
				destinationSize += 1 << 20
			}
			request := storageCopyTestRequest(source, "clone", destinationSize)
			engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system,
				computerBackupAllocate: func(path string, size int64) error {
					if arm.expansion != (size > request.SourceSize) {
						return fullyAllocateComputerDisk(path, size)
					}
					return errors.Join(unix.ENOSPC, unix.EIO)
				}, storageCopyFinalize: fakeCloneFinalize(t)}
			response, err := engine.CopyComputerStorage(t.Context(), request)
			if err == nil || response.Receipt.Kind != "" || !errors.Is(err, unix.EIO) {
				t.Fatalf("%s = %+v err=%v, want an engine failure carrying the close failure", arm.name, response.Receipt, err)
			}
			destinationName, err := deterministicComputerDiskName(request.Destination)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := os.Lstat(filepath.Join(root, "computer-disks", destinationName)); err != nil {
				t.Fatalf("%s discarded the destination in doubt: %v", arm.name, err)
			}
		})
	}
}
