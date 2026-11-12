package ocihelper

import (
	"bytes"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
)

// #1611: removal of a once-attached generation whose current Job never ran.
// The helper answers with the detached Storage-only arm, read from the
// generation's own latest detach record under the flock.

func detachedRemovalRequest(storage ComputerStorageReference, boot string) InventoryRemovalRequest {
	return InventoryRemovalRequest{Removal: ManagedVolumeRemovalAuthority{
		NodeID: "node-1", BootSessionID: boot, JobID: "rotated-job", RemovalGeneration: 2, CleanupFence: "cleanup",
	}, RootInstanceID: "root", ComputerStorage: &storage}
}

func requireDetachedRow(t *testing.T, response InventoryRemovalResponse, storage ComputerStorageReference) RemovalAttemptManifest {
	t.Helper()
	if response.NoStorageEvidence || response.NoRuntimeAttempts || len(response.Attempts) != 1 {
		t.Fatalf("inventory = %+v, want one detached row", response)
	}
	attempt := response.Attempts[0]
	if !attempt.StorageOnly || attempt.StorageAbsent || attempt.StoragePreparation != nil || attempt.StorageDetachment == nil ||
		attempt.Authority.AttemptID != contract.StorageDetachedRemovalAttemptID(storage.StorageGeneration) ||
		attempt.Authority.JobID != "rotated-job" || attempt.Authority.FencingToken != "cleanup" || len(attempt.Resources) != 9 {
		t.Fatalf("detached row = %+v", attempt)
	}
	return attempt
}

func TestDetachedStorageInventoryNamesTheLatestDetach(t *testing.T) {
	engine, system, storage, jobA, diskRoot := detachedByJobA(t)
	before, err := os.ReadFile(filepath.Join(diskRoot, "attachment.json"))
	if err != nil {
		t.Fatal(err)
	}
	record := readRotationManifest(t, diskRoot).PreviousDetachment
	response, err := engine.inventoryComputerStorageRemoval(t.Context(), detachedRemovalRequest(storage, "boot-a"), ResourceInventory{})
	if err != nil {
		t.Fatal(err)
	}
	witness := requireDetachedRow(t, response, storage).StorageDetachment
	if witness.Kind != contract.ComputerStorageDetachedKind || witness.EvidenceKind != contract.ComputerStorageDetachSameBootReap ||
		witness.ReceiptID != record.ReceiptID || witness.NodeID != "node-1" || witness.AttachedJobID != jobA.JobID ||
		witness.AttachedAttemptID != jobA.AttemptID || witness.AttachedBootSessionID != "boot-a" || witness.SweepEpoch != "" {
		t.Fatalf("witness = %+v, record = %+v", witness, record)
	}
	after, err := os.ReadFile(filepath.Join(diskRoot, "attachment.json"))
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("inventory wrote the generation manifest (err=%v)", err)
	}
	_ = system
}

// After a restart, a reap record from the earlier boot counts only once the
// sweep has re-proven it; the sweep keeps the last attachment.
func TestDetachedStorageInventoryAcrossARestart(t *testing.T) {
	engine, system, storage, jobA, diskRoot := detachedByJobA(t)
	if response, err := engine.inventoryComputerStorageRemoval(t.Context(), detachedRemovalRequest(storage, "boot-b"), ResourceInventory{}); err != nil ||
		!response.NoStorageEvidence || len(response.Attempts) != 0 {
		t.Fatalf("an unswept reap from another boot = %+v err=%v, want no_storage_evidence", response, err)
	}
	reapReceipt := readRotationManifest(t, diskRoot).PreviousDetachment.ReceiptID
	swept := &ContainerdEngine{config: engine.config, diskSystem: system, attempts: make(map[string]*containerdAttempt)}
	if err := swept.sweepComputerDisks(t.Context(), "restart-sweep"); err != nil {
		t.Fatal(err)
	}
	response, err := swept.inventoryComputerStorageRemoval(t.Context(), detachedRemovalRequest(storage, "boot-b"), ResourceInventory{})
	if err != nil {
		t.Fatal(err)
	}
	witness := requireDetachedRow(t, response, storage).StorageDetachment
	if witness.EvidenceKind != contract.ComputerStorageDetachPriorBootSweep || witness.SweepEpoch != "restart-sweep" ||
		witness.ReceiptID == reapReceipt || witness.AttachedJobID != jobA.JobID || witness.AttachedAttemptID != jobA.AttemptID ||
		witness.AttachedBootSessionID != "boot-a" {
		t.Fatalf("swept witness = %+v", witness)
	}
}

// Nothing short of a positively detached latest record is proof: an attached
// or pending slot, a remaining loop, a foreign record, or a live attachment
// holding the flock all refuse. A remaining mount is the existing error. A
// reset fence does not block the proof: removal deletes every generation.
func TestDetachedStorageInventoryRefusesAnythingButAProvenDetach(t *testing.T) {
	for _, test := range []struct {
		name    string
		prepare func(t *testing.T, engine *ContainerdEngine, system *fakeComputerDiskSystem, storage ComputerStorageReference, diskRoot string)
		want    string // "detached", "none", or "error"
	}{
		{name: "attached slot", want: "none", prepare: func(t *testing.T, _ *ContainerdEngine, _ *fakeComputerDiskSystem, _ ComputerStorageReference, diskRoot string) {
			manifest := readRotationManifest(t, diskRoot)
			b := testComputerAuthority("attempt-b", "fence-b", "boot-a")
			manifest.Attached = &b
			writeRotationManifest(t, diskRoot, manifest)
		}},
		{name: "pending slot", want: "none", prepare: func(t *testing.T, _ *ContainerdEngine, _ *fakeComputerDiskSystem, _ ComputerStorageReference, diskRoot string) {
			manifest := readRotationManifest(t, diskRoot)
			b := testComputerAuthority("attempt-b", "fence-b", "boot-a")
			manifest.Pending = &b
			writeRotationManifest(t, diskRoot, manifest)
		}},
		{name: "remaining loop", want: "none", prepare: func(t *testing.T, _ *ContainerdEngine, system *fakeComputerDiskSystem, _ ComputerStorageReference, diskRoot string) {
			system.mu.Lock()
			system.loops["/dev/loop-left"] = filepath.Join(diskRoot, "disk.ext4")
			system.mu.Unlock()
		}},
		{name: "remaining mount", want: "error", prepare: func(t *testing.T, engine *ContainerdEngine, system *fakeComputerDiskSystem, storage ComputerStorageReference, _ string) {
			name, _ := deterministicComputerDiskName(storage)
			system.mu.Lock()
			system.mounts[filepath.Join(engine.config.RuntimeRoot, "computer-mounts", name)] = "/dev/loop-left"
			system.mu.Unlock()
		}},
		{name: "record from another Node", want: "none", prepare: func(t *testing.T, _ *ContainerdEngine, _ *fakeComputerDiskSystem, _ ComputerStorageReference, diskRoot string) {
			manifest := readRotationManifest(t, diskRoot)
			manifest.PreviousDetachment.NodeID = "node-other"
			writeRotationManifest(t, diskRoot, manifest)
		}},
		{name: "record for another generation", want: "none", prepare: func(t *testing.T, _ *ContainerdEngine, _ *fakeComputerDiskSystem, _ ComputerStorageReference, diskRoot string) {
			manifest := readRotationManifest(t, diskRoot)
			manifest.PreviousDetachment.StorageGeneration++
			writeRotationManifest(t, diskRoot, manifest)
		}},
		{name: "live attachment holds the flock", want: "error", prepare: func(t *testing.T, engine *ContainerdEngine, _ *fakeComputerDiskSystem, storage ComputerStorageReference, _ string) {
			b := testComputerAuthority("attempt-b", "fence-b", "boot-a")
			b.JobID = "job-2"
			attachment, err := engine.attachComputerDisk(t.Context(), storage, b)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = engine.detachComputerDisk(attachment, computerDiskReapReceipt, "") })
		}},
		{name: "reset fence", want: "detached", prepare: func(t *testing.T, _ *ContainerdEngine, _ *fakeComputerDiskSystem, _ ComputerStorageReference, diskRoot string) {
			manifest := readRotationManifest(t, diskRoot)
			manifest.Retirement = &ComputerStorageResetAuthority{NodeID: "node-1", BootSessionID: "boot-a", HelperGeneration: 1,
				RootInstanceID: "root", JobID: "reset-job", PriorJobID: "job-1", IntentRevision: 2, CleanupFence: "reset-fence"}
			writeRotationManifest(t, diskRoot, manifest)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			engine, system, storage, _, diskRoot := detachedByJobA(t)
			test.prepare(t, engine, system, storage, diskRoot)
			response, err := engine.inventoryComputerStorageRemoval(t.Context(), detachedRemovalRequest(storage, "boot-a"), ResourceInventory{})
			switch test.want {
			case "detached":
				if err != nil {
					t.Fatal(err)
				}
				requireDetachedRow(t, response, storage)
			case "none":
				if err != nil || !response.NoStorageEvidence || len(response.Attempts) != 0 {
					t.Fatalf("inventory = %+v err=%v, want no_storage_evidence", response, err)
				}
			default:
				if err == nil {
					t.Fatalf("inventory = %+v, want a refusal", response)
				}
			}
		})
	}
}

func writeRotationManifest(t *testing.T, diskRoot string, manifest computerDiskManifest) {
	t.Helper()
	if err := writeComputerDiskManifest(diskRoot, manifest); err != nil {
		t.Fatal(err)
	}
}

// The proof deletes nothing; deletion revalidates under the flock. A rotated
// Job's removal proves, attests and deletes only its own generation, and an
// attach after the proof blocks deletion until it detaches again.
func TestDetachedStorageProofThenDeletion(t *testing.T) {
	engine, system, storage, _, diskRoot := detachedByJobA(t)
	neighbour := storage
	neighbour.ComputerID, neighbour.StorageID = "computer-2", "storage-2"
	neighbourAuthority := testComputerAuthority("attempt-n", "fence-n", "boot-a")
	neighbourAttachment, err := engine.attachComputerDisk(t.Context(), neighbour, neighbourAuthority)
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.detachComputerDisk(neighbourAttachment, computerDiskReapReceipt, ""); err != nil {
		t.Fatal(err)
	}
	neighbourName, _ := deterministicComputerDiskName(neighbour)
	neighbourRoot := filepath.Join(engine.config.RuntimeRoot, "computer-disks", neighbourName)
	neighbourBefore := map[string][]byte{}
	for _, file := range []string{"attachment.json", "disk.ext4"} {
		payload, err := os.ReadFile(filepath.Join(neighbourRoot, file))
		if err != nil {
			t.Fatal(err)
		}
		neighbourBefore[file] = payload
	}

	response, err := engine.inventoryComputerStorageRemoval(t.Context(), detachedRemovalRequest(storage, "boot-a"), ResourceInventory{})
	if err != nil {
		t.Fatal(err)
	}
	attempt := requireDetachedRow(t, response, storage)
	stamped := *attempt.StorageDetachment
	stamped.HelperGeneration = 1 // the server stamps the session generation
	attempt.StorageDetachment = &stamped
	if err := validateAttestRemovalRequest(AttestRemovalRequest{JobID: "rotated-job", RemovalGeneration: "2",
		Attempts: []RemovalAttemptManifest{attempt}}, "node-1"); err != nil {
		t.Fatalf("detached row is not attestable: %v", err)
	}

	// An attach after the proof: deletion refuses while it holds the generation.
	b := testComputerAuthority("attempt-b", "fence-b", "boot-a")
	b.JobID = "job-2"
	attachment, err := engine.attachComputerDisk(t.Context(), storage, b)
	if err != nil {
		t.Fatal(err)
	}
	removal := ManagedVolumeRemovalAuthority{NodeID: "node-1", BootSessionID: "boot-a", JobID: "rotated-job",
		PriorJobID: "rotated-job", RemovalGeneration: 2, CleanupFence: "cleanup"}
	deleteRequest := DeleteManagedVolumeRequest{Kind: ManagedVolumeComputerDisk, ComputerStorage: &storage, Removal: &removal}
	if response, err := engine.DeleteManagedVolume(t.Context(), deleteRequest); err == nil || response.Deleted {
		t.Fatalf("deletion removed an attached generation: %+v err=%v", response, err)
	}
	if err := engine.detachComputerDisk(attachment, computerDiskReapReceipt, ""); err != nil {
		t.Fatal(err)
	}
	if response, err := engine.DeleteManagedVolume(t.Context(), deleteRequest); err != nil || !response.Deleted {
		t.Fatalf("delete the detached generation: %+v err=%v", response, err)
	}

	// Post-delete attestation from a fresh inventory: every named resource of
	// the detached generation is absent, and the neighbour is still there,
	// byte for byte.
	inventory := ResourceInventory{}
	if err := engine.inventoryComputerDiskResources(&inventory); err != nil {
		t.Fatal(err)
	}
	attestation, err := attestRemovalInventory(inventory, AttestRemovalRequest{JobID: "rotated-job", RemovalGeneration: "2",
		Attempts: []RemovalAttemptManifest{attempt}})
	if err != nil || len(attestation.Assertions) != len(attempt.Resources) {
		t.Fatalf("post-delete attestation = %+v err=%v", attestation, err)
	}
	for _, assertion := range attestation.Assertions {
		if !assertion.Absent {
			t.Fatalf("post-delete attestation kept %+v", assertion)
		}
	}
	if !slices.Contains(inventory.ComputerDiskImages, neighbourName) || !slices.Contains(inventory.ComputerDiskManifests, neighbourName) {
		t.Fatalf("the neighbour Computer's generation left the inventory: %+v", inventory)
	}
	for file, before := range neighbourBefore {
		after, err := os.ReadFile(filepath.Join(neighbourRoot, file))
		if err != nil || !bytes.Equal(before, after) {
			t.Fatalf("deletion changed the neighbour's %s (err=%v)", file, err)
		}
	}
	_, _ = system, diskRoot
}
