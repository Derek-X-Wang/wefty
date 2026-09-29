//go:build darwin || linux

package agent

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	ocirunner "github.com/Derek-X-Wang/wefty/runner/oci"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

// backupRefusalEngine answers Backup creation the way run 4's helper answered
// a Backup of a clone that had never started: first as the untyped refusal it
// was, then -- once typed is set -- with the source_never_detached failure
// receipt the helper now returns for it. Everything else is the pre-admission
// engine, whose Watch holds a neighbour attempt running until released.
type backupRefusalEngine struct {
	*preAdmissionRenewalEngine
	mu      sync.Mutex
	creates int
	typed   bool
}

func (engine *backupRefusalEngine) CreateComputerBackup(_ context.Context, request ocihelper.CreateComputerBackupRequest) (ocihelper.CreateComputerBackupResponse, error) {
	engine.mu.Lock()
	engine.creates++
	typed := engine.typed
	engine.mu.Unlock()
	if !typed {
		return ocihelper.CreateComputerBackupResponse{}, errors.New("Computer Backup lacks exact detached source-generation evidence")
	}
	return ocihelper.CreateComputerBackupResponse{Receipt: ocihelper.ComputerBackupCopyReceipt{
		Kind: "computer_backup_copy_failed_absent", ReceiptID: "never-detached-receipt",
		BackupID: request.BackupID, CopyID: request.CopyID, ComputerID: request.Storage.ComputerID,
		StorageID: request.Storage.StorageID, StorageGeneration: request.Storage.StorageGeneration,
		NodeID: request.Authority.NodeID, RootInstanceID: request.Authority.RootInstanceID,
		JobID: request.Authority.JobID, OperationRevision: request.Authority.OperationRevision,
		CleanupFence: request.Authority.CleanupFence, HelperGeneration: request.Authority.HelperGeneration,
		AllocatedSize: request.Storage.DiskBytes, Encryption: "none",
		FailureCode: string(l1.ComputerBackupFailureSourceNeverDetached), CopyAbsent: true,
	}}, nil
}

func (*backupRefusalEngine) DeleteComputerBackupCopy(context.Context, ocihelper.DeleteComputerBackupCopyRequest) (ocihelper.DeleteComputerBackupCopyResponse, error) {
	return ocihelper.DeleteComputerBackupCopyResponse{}, errors.New("backupRefusalEngine deletes no Backup copy")
}

func (engine *backupRefusalEngine) createCount() int {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	return engine.creates
}

// TestRefusedBackupLeavesHelperSessionAndNeighbourAndSettlesOnce is the run-4
// wedge end to end through the real helper protocol (#558). A Backup of a
// never-started clone must not cost the node its helper session or a running
// neighbour its attempt; an untyped refusal is retried on a doubling backoff,
// not on every heartbeat; and the typed answer reaches L1 exactly once as a
// failed Backup carrying its reason.
func TestRefusedBackupLeavesHelperSessionAndNeighbourAndSettlesOnce(t *testing.T) {
	engine := &backupRefusalEngine{preAdmissionRenewalEngine: newPreAdmissionRenewalEngine()}
	barrier, stopHelper := startPreAdmissionHelper(t, engine, time.Now)
	defer stopHelper()
	if err := barrier.Ensure(t.Context()); err != nil {
		t.Fatal(err)
	}
	session, err := barrier.Session()
	if err != nil {
		t.Fatal(err)
	}
	handshake := session.Handshake()

	neighbour := ocihelper.AttemptAuthority{NodeID: "pre-admission-node", BootSessionID: "pre-admission-boot",
		JobID: "neighbour-job", AttemptID: "neighbour-attempt", FencingToken: "neighbour-fence",
		Class: contract.JobClassService, RemovalGeneration: "1"}
	if _, err := session.Run(t.Context(), ocihelper.RunRequest{Authority: neighbour, InitialDeadman: 5 * time.Second,
		Workload: ocihelper.WorkloadInput{ImageDigest: preAdmissionImageDigest, Argv: []string{"/bin/neighbour"}}}); err != nil {
		t.Fatal(err)
	}
	neighbourDone := make(chan error, 1)
	go func() {
		neighbourDone <- session.Watch(context.Background(), ocihelper.WatchRequest{Authority: neighbour}, nil)
	}()
	<-engine.watchEntered

	var acknowledgementsMu sync.Mutex
	var acknowledgements []l1.ComputerBackupAcknowledgementRequest
	client := newRoundTripClient(t, http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		var acknowledgement l1.ComputerBackupAcknowledgementRequest
		if err := json.NewDecoder(request.Body).Decode(&acknowledgement); err != nil {
			t.Errorf("decode Backup acknowledgement: %v", err)
		}
		acknowledgementsMu.Lock()
		acknowledgements = append(acknowledgements, acknowledgement)
		acknowledgementsMu.Unlock()
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{}`))
	}))
	acknowledged := func() []l1.ComputerBackupAcknowledgementRequest {
		acknowledgementsMu.Lock()
		defer acknowledgementsMu.Unlock()
		return append([]l1.ComputerBackupAcknowledgementRequest(nil), acknowledgements...)
	}

	now := time.Date(2026, 9, 28, 21, 12, 45, 0, time.UTC)
	var nowMu sync.Mutex
	advance := func(by time.Duration) {
		nowMu.Lock()
		now = now.Add(by)
		nowMu.Unlock()
	}
	backups := newBackupController(client, nil, ocirunner.NewAdapter(barrier), "pre-admission-node",
		"pre-admission-boot", "managed-root-1", t.Logf)
	backups.now = func() time.Time {
		nowMu.Lock()
		defer nowMu.Unlock()
		return now
	}
	directive := l1.ComputerBackupDirective{BackupID: "backup-of-clone", CopyID: "copy-of-clone",
		ComputerID: "clone", StorageID: "clone-storage", StorageGeneration: 1, AllocatedSize: 128 << 20,
		BoundNodeID: "pre-admission-node", RootInstanceID: "managed-root-1", JobID: "clone-job",
		OperationRevision: 2, CleanupFence: "backup-fence"}
	heartbeat := func() {
		backups.enqueueCreate(t.Context(), directive, nil)
		backups.wait()
	}
	requireSessionAndNeighbourIntact := func(stage string) {
		t.Helper()
		if err := session.HealthError(); err != nil {
			t.Fatalf("%s: the helper session was withdrawn: %v", stage, err)
		}
		current, err := barrier.Session()
		if err != nil {
			t.Fatalf("%s: the boot barrier lost its session: %v", stage, err)
		}
		if got := current.Handshake(); got.SessionGeneration != handshake.SessionGeneration || got.HelperInstanceID != handshake.HelperInstanceID {
			t.Fatalf("%s: helper session generation %d/%s, want %d/%s", stage,
				got.SessionGeneration, got.HelperInstanceID, handshake.SessionGeneration, handshake.HelperInstanceID)
		}
		select {
		case err := <-neighbourDone:
			t.Fatalf("%s: the running neighbour's attempt ended: %v", stage, err)
		default:
		}
		if reaps := engine.attemptReapCount(); reaps != 0 {
			t.Fatalf("%s: the helper reaped %d attempts", stage, reaps)
		}
	}

	// The untyped refusal: one dispatch, then every heartbeat inside the
	// backoff is a no-op rather than another trip to the helper.
	for range 4 {
		heartbeat()
	}
	if creates := engine.createCount(); creates != 1 {
		t.Fatalf("Backup creations inside the first backoff = %d, want 1", creates)
	}
	requireSessionAndNeighbourIntact("after an untyped refusal")
	advance(backupCreateRetryBase)
	heartbeat()
	advance(backupCreateRetryBase)
	heartbeat()
	if creates := engine.createCount(); creates != 2 {
		t.Fatalf("Backup creations after one base interval and then one more = %d, want 2 (the wait doubles)", creates)
	}
	if got := acknowledged(); len(got) != 0 {
		t.Fatalf("an untyped refusal was reported to L1: %+v", got)
	}
	requireSessionAndNeighbourIntact("after a retried untyped refusal")

	// The typed answer: settled once, as a failed Backup with its reason.
	engine.mu.Lock()
	engine.typed = true
	engine.mu.Unlock()
	advance(backupCreateRetryCeiling)
	heartbeat()
	got := acknowledged()
	if len(got) != 1 || got[0].Receipt.Kind != "computer_backup_copy_failed_absent" ||
		got[0].Receipt.FailureCode != string(l1.ComputerBackupFailureSourceNeverDetached) || !got[0].Receipt.CopyAbsent ||
		got[0].Receipt.HelperGeneration != handshake.SessionGeneration {
		t.Fatalf("typed Backup refusal acknowledgements = %+v, want one source_never_detached failure", got)
	}
	backups.mu.Lock()
	_, pending := backups.createRetries["create\x00"+directive.CopyID]
	backups.mu.Unlock()
	if pending {
		t.Fatal("a settled Backup kept its retry backoff")
	}
	requireSessionAndNeighbourIntact("after the typed refusal settled")

	close(engine.releaseWatch)
	if err := <-neighbourDone; err != nil {
		t.Fatalf("the neighbour's attempt did not complete normally: %v", err)
	}
}
