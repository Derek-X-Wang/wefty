package ocihelper

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
)

// #1607: L1 replaces a stopped Computer's Job without running it, so a Storage
// consumer names the new Job while the generation's detach evidence still
// names the Job that last attached. Consumers no longer compare those Jobs.
// These tests hold the rule that replaces that comparison: the evidence is
// the generation's latest attach/detach record, the slot holds no newer
// attachment, and the generation is positively unmounted and loop-free.

const (
	rotationJobA = "job-1" // testComputerAuthority's Job: the Job that attached first.
	rotationJobB = "job-2" // A later Job that attaches the same generation.
	rotationJobC = "job-3" // The Job L1 rotated to; it never ran.
)

type detachedStorageConsumer struct {
	name    string
	consume func(t *testing.T, engine *ContainerdEngine, storage ComputerStorageReference, source AttemptAuthority, priorJobID string) error
}

func detachedStorageConsumers() []detachedStorageConsumer {
	return []detachedStorageConsumer{
		{name: "Backup", consume: func(t *testing.T, engine *ContainerdEngine, storage ComputerStorageReference, source AttemptAuthority, priorJobID string) error {
			request := backupTestRequest(storage, source)
			request.Authority.PriorJobID = priorJobID
			_, err := engine.CreateComputerBackup(t.Context(), request)
			return err
		}},
		{name: "reset", consume: func(t *testing.T, engine *ContainerdEngine, storage ComputerStorageReference, source AttemptAuthority, priorJobID string) error {
			request := resetTestRequest(storage, source)
			request.Authority.PriorJobID = priorJobID
			request.Storage.IntentRevision = request.Authority.IntentRevision
			_, err := engine.ResetComputerStorage(t.Context(), request)
			return err
		}},
		{name: "removal deletion", consume: func(_ *testing.T, engine *ContainerdEngine, storage ComputerStorageReference, source AttemptAuthority, priorJobID string) error {
			return engine.deleteComputerDisk(storage, ManagedVolumeRemovalAuthority{NodeID: source.NodeID, BootSessionID: source.BootSessionID,
				JobID: "removal-job", PriorJobID: priorJobID, RemovalGeneration: 1, CleanupFence: "cleanup"})
		}},
		{name: "reimage preflight", consume: func(t *testing.T, engine *ContainerdEngine, storage ComputerStorageReference, source AttemptAuthority, priorJobID string) error {
			engine.computerReimageImageInspect = func(context.Context, PreflightComputerReimageRequest) (computerReimageImageFacts, error) {
				return computerReimageImageFacts{platform: OCIPlatform{OS: "linux", Architecture: "amd64"}, uid: 1000, gid: 1000}, nil
			}
			engine.computerReimageDiskOwner = func(context.Context, string) (uint32, uint32, error) { return 1000, 1000, nil }
			request := testComputerReimageRequest(storage, source)
			request.Authority.OldJobID = priorJobID
			response, err := engine.PreflightComputerReimage(t.Context(), request)
			if err != nil {
				return err
			}
			if response.Receipt.Kind != "computer_reimage_preflight_verified" {
				return fmt.Errorf("reimage preflight %s at %s", response.Receipt.Kind, response.Receipt.FailureStage)
			}
			return nil
		}},
	}
}

// detachedByJobA attaches and same-boot detaches the test generation under
// Job A, leaving A's reap receipt in the generation's manifest slot.
func detachedByJobA(t *testing.T) (*ContainerdEngine, *fakeComputerDiskSystem, ComputerStorageReference, AttemptAuthority, string) {
	t.Helper()
	root := t.TempDir()
	system := newFakeComputerDiskSystem()
	engine := &ContainerdEngine{config: NativeEngineConfig{RuntimeRoot: root}, diskSystem: system, attempts: make(map[string]*containerdAttempt)}
	storage := testComputerStorage()
	jobA := testComputerAuthority("attempt-a", "fence-a", "boot-a")
	attachment, err := engine.attachComputerDisk(t.Context(), storage, jobA)
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.detachComputerDisk(attachment, computerDiskReapReceipt, ""); err != nil {
		t.Fatal(err)
	}
	name, _ := deterministicComputerDiskName(storage)
	return engine, system, storage, jobA, filepath.Join(root, "computer-disks", name)
}

func readRotationManifest(t *testing.T, diskRoot string) computerDiskManifest {
	t.Helper()
	manifest, present, err := readComputerDiskManifest(filepath.Join(diskRoot, "attachment.json"))
	if err != nil || !present {
		t.Fatalf("read generation manifest present=%t err=%v", present, err)
	}
	return manifest
}

// An older detach followed by a newer attach, by any Job, must never be
// accepted: the attach clears the older record, every consumer refuses while
// the newer attachment holds the slot, and only the newer detach authorizes.
func TestStorageConsumersRefuseOlderDetachAfterANewerAttach(t *testing.T) {
	for _, consumer := range detachedStorageConsumers() {
		t.Run(consumer.name, func(t *testing.T) {
			engine, _, storage, jobA, diskRoot := detachedByJobA(t)
			jobB := testComputerAuthority("attempt-b", "fence-b", "boot-a")
			jobB.JobID = rotationJobB
			attachment, err := engine.attachComputerDisk(t.Context(), storage, jobB)
			if err != nil {
				t.Fatal(err)
			}
			if manifest := readRotationManifest(t, diskRoot); manifest.PreviousDetachment != nil || manifest.Attached == nil {
				t.Fatalf("a newer attach left the older detach record in place: %+v", manifest)
			}
			for _, priorJobID := range []string{rotationJobA, rotationJobB, rotationJobC} {
				if err := consumer.consume(t, engine, storage, jobA, priorJobID); err == nil {
					t.Fatalf("%s consumed attached Storage naming prior Job %s", consumer.name, priorJobID)
				}
			}
			if err := engine.detachComputerDisk(attachment, computerDiskReapReceipt, ""); err != nil {
				t.Fatal(err)
			}
			if evidence := readRotationManifest(t, diskRoot).PreviousDetachment; evidence == nil ||
				evidence.JobID != rotationJobB || evidence.AttemptID != "attempt-b" {
				t.Fatalf("newer detach record = %+v, want Job B's attempt", evidence)
			}
			// Positive control: the same setup is consumable once B detached,
			// by a Job L1 rotated to that never ran.
			if err := consumer.consume(t, engine, storage, jobA, rotationJobC); err != nil {
				t.Fatalf("%s refused the generation's latest detach record after a Job rotation: %v", consumer.name, err)
			}
		})
	}
}

// A newer attach that crashed at its durable Pending or Attached write, with
// no rollback and no mount or loop left behind, still holds the slot: the
// manifest alone refuses every consumer. The next boot sweep turns it into a
// fresh receipt for B's attempt, which a rotated Job may consume.
func TestStorageConsumersRefuseACrashedNewerAttachUntilTheSweep(t *testing.T) {
	for _, crash := range []string{"pending", "attached"} {
		for _, consumer := range detachedStorageConsumers() {
			t.Run(crash+"/"+consumer.name, func(t *testing.T) {
				engine, system, storage, jobA, diskRoot := detachedByJobA(t)
				olderReceipt := readRotationManifest(t, diskRoot).PreviousDetachment.ReceiptID
				jobB := testComputerAuthority("attempt-b", "fence-b", "boot-a")
				jobB.JobID = rotationJobB
				manifest := readRotationManifest(t, diskRoot)
				manifest.PreviousDetachment = nil
				if crash == "pending" {
					manifest.Pending = &jobB
				} else {
					manifest.Attached = &jobB
				}
				if err := writeComputerDiskManifest(diskRoot, manifest); err != nil {
					t.Fatal(err)
				}
				if len(system.mounts) != 0 || len(system.loops) != 0 {
					t.Fatalf("fixture left mechanics behind: mounts %v loops %v", system.mounts, system.loops)
				}
				for _, priorJobID := range []string{rotationJobA, rotationJobB} {
					if err := consumer.consume(t, engine, storage, jobA, priorJobID); err == nil {
						t.Fatalf("%s consumed a generation whose newer attach crashed at %s", consumer.name, crash)
					}
				}
				swept := &ContainerdEngine{config: engine.config, diskSystem: system, attempts: make(map[string]*containerdAttempt)}
				if err := swept.sweepComputerDisks(t.Context(), "sweep-after-crash"); err != nil {
					t.Fatal(err)
				}
				evidence := readRotationManifest(t, diskRoot).PreviousDetachment
				if evidence == nil || evidence.Kind != computerDiskSweepReceipt || evidence.SweepEpoch != "sweep-after-crash" ||
					evidence.JobID != rotationJobB || evidence.AttemptID != "attempt-b" || evidence.FencingToken != "fence-b" ||
					evidence.ReceiptID == "" || evidence.ReceiptID == olderReceipt {
					t.Fatalf("sweep receipt after a crashed attach = %+v", evidence)
				}
				if err := consumer.consume(t, swept, storage, jobA, rotationJobC); err != nil {
					t.Fatalf("%s refused the sweep receipt for the crashed attach: %v", consumer.name, err)
				}
			})
		}
	}
}

// An attach that failed after it began can restore the earlier detach record.
// That record is usable only when the generation is positively unmounted and
// loop-free: a remaining mount, a remaining loop, or an unreadable inspection
// refuses every consumer, and the cleaned generation is consumable again.
func TestStorageConsumersRequireMountAndLoopAbsenceBesideTheRecord(t *testing.T) {
	for _, leftover := range []string{"mount", "loop", "inspection error"} {
		for _, consumer := range detachedStorageConsumers() {
			t.Run(leftover+"/"+consumer.name, func(t *testing.T) {
				engine, system, storage, jobA, diskRoot := detachedByJobA(t)
				name, _ := deterministicComputerDiskName(storage)
				mountPath := filepath.Join(engine.config.RuntimeRoot, "computer-mounts", name)
				imagePath := filepath.Join(diskRoot, "disk.ext4")
				system.mu.Lock()
				switch leftover {
				case "mount":
					system.mounts[mountPath] = "/dev/loop-left"
				case "loop":
					system.loops["/dev/loop-left"] = imagePath
				default:
					system.inspectErr = errors.New("mount table unreadable")
				}
				system.mu.Unlock()
				if err := consumer.consume(t, engine, storage, jobA, rotationJobC); err == nil {
					t.Fatalf("%s consumed a generation with a remaining %s", consumer.name, leftover)
				}
				system.mu.Lock()
				delete(system.mounts, mountPath)
				delete(system.loops, "/dev/loop-left")
				system.inspectErr = nil
				system.mu.Unlock()
				if err := consumer.consume(t, engine, storage, jobA, rotationJobC); err != nil {
					t.Fatalf("%s refused the cleaned generation: %v", consumer.name, err)
				}
			})
		}
	}
}

// The reimage detach branch now uses the shared evidence rules, so the boot
// and sweep-epoch rules it skipped before apply, and a rotated OldJobID is
// accepted as many times as L1 rotated.
func TestReimageDetachEvidenceUsesTheSharedRules(t *testing.T) {
	storage := testComputerStorage()
	valid := computerDiskEvidence{Kind: computerDiskReapReceipt, ReceiptID: "receipt-a", ComputerID: storage.ComputerID,
		StorageID: storage.StorageID, StorageGeneration: storage.StorageGeneration, NodeID: "node-1", JobID: rotationJobA,
		AttemptID: "attempt-a", FencingToken: "fence-a", BootSessionID: "boot-a"}
	request := testComputerReimageRequest(storage, testComputerAuthority("attempt-a", "fence-a", "boot-a"))
	check := func(evidence computerDiskEvidence, oldJobID string) bool {
		manifest := computerDiskManifest{Version: computerDiskManifestVersion, Storage: storage, DiskImage: "disk.ext4",
			MountDirectory: "disk", PreviousDetachment: &evidence}
		changed := request
		changed.Authority.OldJobID = oldJobID
		_, ok := reimageDetachmentEvidence(manifest, changed)
		return ok
	}
	for _, oldJobID := range []string{rotationJobA, rotationJobC, "rotated-again"} {
		if !check(valid, oldJobID) {
			t.Fatalf("reimage refused the generation's detach record for OldJobID %q", oldJobID)
		}
	}
	for _, test := range []struct {
		name     string
		mutate   func(*computerDiskEvidence)
		oldJobID string
	}{
		{name: "unnamed OldJobID", oldJobID: ""},
		{name: "reap from another boot", mutate: func(e *computerDiskEvidence) { e.BootSessionID = "boot-other" }, oldJobID: rotationJobC},
		{name: "reap with a sweep epoch", mutate: func(e *computerDiskEvidence) { e.SweepEpoch = "epoch" }, oldJobID: rotationJobC},
		{name: "sweep without an epoch", mutate: func(e *computerDiskEvidence) { e.Kind = computerDiskSweepReceipt }, oldJobID: rotationJobC},
		{name: "foreign Node", mutate: func(e *computerDiskEvidence) { e.NodeID = "node-other" }, oldJobID: rotationJobC},
		{name: "foreign generation", mutate: func(e *computerDiskEvidence) { e.StorageGeneration++ }, oldJobID: rotationJobC},
		{name: "missing historical Job", mutate: func(e *computerDiskEvidence) { e.JobID = "" }, oldJobID: rotationJobC},
		{name: "missing historical attempt", mutate: func(e *computerDiskEvidence) { e.AttemptID = "" }, oldJobID: rotationJobC},
	} {
		evidence := valid
		if test.mutate != nil {
			test.mutate(&evidence)
		}
		if check(evidence, test.oldJobID) {
			t.Fatalf("reimage accepted detach evidence: %s", test.name)
		}
	}
}

// PriorJobID is no longer compared to the evidence, but it stays part of an
// operation's retry identity: a replay that names a different prior Job is a
// different operation and conflicts with the recorded one.
func TestRecordedStorageOperationsStillBindTheirPriorJob(t *testing.T) {
	t.Run("Backup", func(t *testing.T) {
		engine, _, storage, jobA, _ := detachedByJobA(t)
		request := backupTestRequest(storage, jobA)
		request.Authority.PriorJobID = rotationJobC
		if _, err := engine.CreateComputerBackup(t.Context(), request); err != nil {
			t.Fatal(err)
		}
		if _, err := engine.CreateComputerBackup(t.Context(), request); err != nil {
			t.Fatalf("identical Backup replay = %v", err)
		}
		request.Authority.PriorJobID = "job-replayed-differently"
		if _, err := engine.CreateComputerBackup(t.Context(), request); err == nil {
			t.Fatal("a Backup replay naming a different prior Job matched the recorded operation")
		}
	})
	t.Run("reset", func(t *testing.T) {
		engine, _, storage, jobA, _ := detachedByJobA(t)
		request := resetTestRequest(storage, jobA)
		request.Authority.PriorJobID = rotationJobC
		request.Storage.IntentRevision = request.Authority.IntentRevision
		if _, err := engine.ResetComputerStorage(t.Context(), request); err != nil {
			t.Fatal(err)
		}
		if _, err := engine.ResetComputerStorage(t.Context(), request); err != nil {
			t.Fatalf("identical reset replay = %v", err)
		}
		request.Authority.PriorJobID = "job-replayed-differently"
		if _, err := engine.ResetComputerStorage(t.Context(), request); err == nil {
			t.Fatal("a reset replay naming a different prior Job matched the recorded operation")
		}
	})
}
