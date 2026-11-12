package ocihelper

import (
	"context"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

// After a Job rotation the Job that attaches a generation can differ from the
// Job being removed, so a per-generation inventory is fenced by a live
// attempt of any Job on the same Storage (#1611). A Job-scoped inventory
// keeps its old scope.
func TestRemovalInventoryIsFencedByAnyJobOnTheSameStorage(t *testing.T) {
	storage := &ComputerStorageReference{ComputerID: "computer", StorageID: "storage", StorageGeneration: 1}
	other := &ComputerStorageReference{ComputerID: "computer", StorageID: "storage", StorageGeneration: 2}
	session := &serverSession{attempts: map[string]*serverAttempt{
		"other-job": {authority: AttemptAuthority{JobID: "job-that-attaches"}, state: attemptStarting, computerStorage: storage},
	}}
	if !session.hasLiveRemovalAttemptLocked("rotated-job", storage) {
		t.Fatal("another Job's live attempt on the same Storage did not fence the per-generation inventory")
	}
	if session.hasLiveRemovalAttemptLocked("rotated-job", other) {
		t.Fatal("another Job's attempt fenced a different generation")
	}
	if session.hasLiveRemovalAttemptLocked("rotated-job", nil) {
		t.Fatal("another Job's attempt fenced the Job-scoped inventory")
	}
	session.attempts["other-job"].state = attemptTombstoned
	if session.hasLiveRemovalAttemptLocked("rotated-job", storage) {
		t.Fatal("a reaped tombstone of another Job still fenced inventory")
	}
}

type detachedInventoryEngine struct {
	*fakeEngine
	response InventoryRemovalResponse
}

func (engine *detachedInventoryEngine) InventoryRemoval(context.Context, InventoryRemovalRequest) (InventoryRemovalResponse, error) {
	return engine.response, nil
}

// The engine cannot know the session it answers in; the server stamps the
// detachment witness with that session's generation, so the proof of no
// mount and no loop cannot outlive the session that made it.
func TestRemovalInventoryStampsTheDetachmentWitnessWithItsSession(t *testing.T) {
	storage := testComputerManagedVolumes()[0].ComputerStorage
	witness := &contract.ComputerStorageDetachmentWitness{Kind: contract.ComputerStorageDetachedKind,
		EvidenceKind: contract.ComputerStorageDetachSameBootReap, ReceiptID: "receipt", NodeID: "node-1",
		ComputerID: storage.ComputerID, StorageID: storage.StorageID, StorageGeneration: storage.StorageGeneration,
		AttachedJobID: "job-that-ran", AttachedAttemptID: "attempt-a", AttachedBootSessionID: "boot-1"}
	engine := &detachedInventoryEngine{fakeEngine: newFakeEngine(), response: InventoryRemovalResponse{Attempts: []RemovalAttemptManifest{{
		Authority: AttemptAuthority{NodeID: "node-1", BootSessionID: "boot-1", JobID: "rotated-job",
			AttemptID: contract.StorageDetachedRemovalAttemptID(storage.StorageGeneration), FencingToken: "cleanup",
			Class: contract.JobClassService, RemovalGeneration: "1"},
		ComputerStorage: storage, StorageOnly: true, StorageDetachment: witness,
		Resources: expectedComputerStorageRemovalResources(storage),
	}}}}
	client, stop := startTestServer(t, engine, ServerConfig{HeartbeatTimeout: 5 * time.Second})
	defer stop()
	session, err := client.OpenSession(t.Context(), testSessionRequest())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	requireSweep(t, session)
	response, err := session.InventoryRemoval(t.Context(), InventoryRemovalRequest{
		Removal: ManagedVolumeRemovalAuthority{NodeID: "node-1", BootSessionID: "boot-1", JobID: "rotated-job",
			RemovalGeneration: 1, CleanupFence: "cleanup"},
		RootInstanceID: "root", ComputerStorage: storage,
	})
	if err != nil {
		t.Fatal(err)
	}
	generation := session.Handshake().SessionGeneration
	if len(response.Attempts) != 1 || response.Attempts[0].StorageDetachment == nil || generation == 0 ||
		response.Attempts[0].StorageDetachment.HelperGeneration != generation || response.HelperSession.SessionGeneration != generation {
		t.Fatalf("inventory = %+v, want the witness stamped with session generation %d", response, generation)
	}
	if !response.Attempts[0].StorageDetachment.Valid() {
		t.Fatalf("stamped witness is invalid: %+v", response.Attempts[0].StorageDetachment)
	}
	if witness.HelperGeneration != 0 {
		t.Fatal("the server stamped the engine's own witness value in place")
	}
}
