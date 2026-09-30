package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

// A restore aborted by its failed predecessor Backup copy returns the Computer
// to `stable` exactly as a completed restore does, so `restore --wait` has to
// judge the restore's own record, and exit non-zero when it failed (#592).

// stoppedRestoreSource leaves one published Backup and the Computer stopped,
// ready to restore, with room under the cap for a kept predecessor.
func stoppedRestoreSource(t *testing.T, h *storageCLIHarness) (l1.Computer, string) {
	t.Helper()
	runStorageCLI(t, h.ctx, h.clients, true, "services", "backup", "set-cap", h.computer.ComputerID,
		"--cap", "2", "--expect-current")
	runStorageCLI(t, h.ctx, h.clients, true, "services", "backup", "create", h.computer.ComputerID,
		"--expect-current", "--idempotency-key", "restore-source", "--allow-power-off")
	h.completeBackupHelper(t)
	backups, err := h.store.ListComputerBackups(h.ctx, h.computer.ComputerID)
	if err != nil || len(backups.Backups) != 1 {
		t.Fatalf("restore source = %#v err=%v", backups, err)
	}
	current := mustStorageComputer(t, h)
	stopped, err := h.store.SetComputerDesiredState(h.ctx, current.ComputerID, l1.ComputerDesiredStateRequest{
		ComputerMutationPrecondition: l1.ComputerMutationPrecondition{IntentRevision: current.IntentRevision,
			StorageID: current.StorageID, StorageGeneration: current.StorageGeneration, Actor: "operator"},
		DesiredState: contract.ServiceDesiredStopped,
	})
	if err != nil {
		t.Fatal(err)
	}
	return stopped, backups.Backups[0].BackupID
}

// restoreArgs names the restore under its original CAS tuple, so a later call
// replays it.
func restoreArgs(source l1.Computer, backupID, predecessor, key string) []string {
	return []string{"services", "restore", source.ComputerID, backupID, predecessor,
		"--intent-revision", fmt.Sprint(source.IntentRevision), "--storage-id", source.StorageID,
		"--storage-generation", fmt.Sprint(source.StorageGeneration), "--idempotency-key", key}
}

func withRestoreWait(args []string) []string {
	return append(append([]string{}, args...), "--wait", "2s", "--poll-interval", "1ms")
}

// failRestorePredecessorCopy settles the one reserved restore with a failed
// predecessor Backup copy receipt, so L1 aborts it.
func (h *storageCLIHarness) failRestorePredecessorCopy(t *testing.T, failureCode string) {
	t.Helper()
	computer := mustStorageComputer(t, h)
	if computer.ReconfigurationPhase != l1.ComputerReconfigurationRestoring {
		t.Fatalf("Computer is not restoring: %#v", computer)
	}
	if err := h.store.RecordComputerRestoreAuthorityRevoked(h.ctx, computer.ComputerID, computer.IntentRevision, l1.ComputerRestoreRevocationEvidence{
		RevokeAll: true, TokenRevocation: contract.ComputerTokenRevocationReceipt{
			ComputerID: computer.ComputerID, RestoreOperationRevision: computer.IntentRevision,
			SubmitIntentRevision: 1, CommittedAt: time.Now().UTC(),
		},
	}); err != nil {
		t.Fatal(err)
	}
	directives, err := h.store.ListNodeComputerStorageCopyDirectives(h.ctx, "fabric-storage-node", h.node.NodeID, h.node.BootSessionID)
	if err != nil || len(directives) != 1 || !directives[0].KeepOldBackup {
		t.Fatalf("restore directives = %#v err=%v", directives, err)
	}
	d := directives[0]
	receipt := l1.ComputerStorageCopyReceipt{Kind: "computer_storage_copy_verified",
		ReceiptID: "receipt-failed-" + d.CopyID, Operation: d.Operation, BackupID: d.BackupID, CopyID: d.CopyID,
		SourceComputerID: d.SourceComputerID, SourceStorageID: d.SourceStorageID, SourceGeneration: d.SourceGeneration,
		DestinationComputerID: d.DestinationComputerID, DestinationStorageID: d.DestinationStorageID,
		DestinationGeneration: d.DestinationGeneration, NodeID: d.BoundNodeID, RootInstanceID: d.RootInstanceID,
		JobID: d.JobID, OperationRevision: d.OperationRevision, CleanupFence: d.CleanupFence, HelperGeneration: 9,
		SourceSize: d.SourceSize, DestinationSize: d.DestinationSize, SourceDigest: d.SourceDigest,
		DestinationDigest: d.SourceDigest, SourceUnchanged: true, DestinationPrepared: true}
	old := l1.ComputerBackupCopyReceipt{Kind: "computer_backup_copy_failed_absent", ReceiptID: "old-failed-" + d.OldCopyID,
		BackupID: d.OldBackupID, CopyID: d.OldCopyID, ComputerID: d.DestinationComputerID,
		StorageID: d.DestinationStorageID, StorageGeneration: d.OldGeneration, NodeID: d.BoundNodeID,
		RootInstanceID: d.RootInstanceID, JobID: d.JobID, OperationRevision: d.OperationRevision,
		CleanupFence: d.CleanupFence, HelperGeneration: 9, AllocatedSize: d.DestinationSize,
		Encryption: l1.BackupEncryptionNone, FailureCode: failureCode, CopyAbsent: true}
	if _, err := h.store.AcknowledgeComputerStorageCopy(h.ctx, "fabric-storage-node", d.DestinationComputerID,
		l1.ComputerStorageCopyAcknowledgementRequest{NodeID: h.node.NodeID, BootSessionID: h.node.BootSessionID,
			IdempotencyKey: receipt.ReceiptID, Receipt: receipt, OldBackupReceipt: &old}); err == nil {
		t.Fatal("failed predecessor copy was accepted as a completed restore")
	}
	aborted := mustStorageComputer(t, h)
	if aborted.ReconfigurationPhase != l1.ComputerReconfigurationStable || aborted.AppliedRevision != d.OperationRevision {
		t.Fatalf("aborted restore left Computer = %#v", aborted)
	}
}

func assertFailedRestoreWait(t *testing.T, waitErr error, args []string, stdout []byte, wantCode contract.ErrorCode,
	failureCode string, operationRevision int64) {
	t.Helper()
	assertCLIAPIError(t, waitErr, wantCode)
	if got := commandExitCodeForArgs(waitErr, args); got != exitConflict {
		t.Fatalf("failed restore wait exit = %d, want %d (%v)", got, exitConflict, waitErr)
	}
	var refusal *apiResponseError
	if !errors.As(waitErr, &refusal) || refusal.APIError.Details["failure_code"] != failureCode ||
		refusal.APIError.Details["operation_revision"] != operationRevision || refusal.APIError.Details["backup_id"] == "" ||
		refusal.APIError.Details["computer_id"] == "" || !strings.Contains(waitErr.Error(), failureCode) {
		t.Fatalf("failed restore wait error = %v (%#v)", waitErr, refusal)
	}
	var output storageMutationOutput
	if err := json.Unmarshal(stdout, &output); err != nil || output.Computer == nil || output.Computer.RestoreOperation == nil ||
		output.Computer.RestoreOperation.Status != "failed" ||
		string(output.Computer.RestoreOperation.FailureCode) != failureCode ||
		output.Observation == nil || output.Observation.Status != "observed" || output.StorageProvenance == nil {
		t.Fatalf("failed restore wait output changed: %s err=%v", stdout, err)
	}
}

func TestRestoreWaitExitsNonZeroWhenThePredecessorCopyFails(t *testing.T) {
	for _, test := range []struct {
		failureCode l1.ComputerBackupFailureCode
		wantCode    contract.ErrorCode
	}{
		{l1.ComputerBackupFailureDigestMismatch, contract.ErrorConflict},
		{l1.ComputerBackupFailureInsufficientDisk, contract.ErrorCapacityExhausted},
	} {
		t.Run(string(test.failureCode), func(t *testing.T) {
			h := newStorageCLIHarness(t)
			source, backupID := stoppedRestoreSource(t, h)
			args := restoreArgs(source, backupID, "--keep-old-as-backup", "aborted-restore")
			runStorageCLI(t, h.ctx, h.clients, true, args...)
			h.failRestorePredecessorCopy(t, string(test.failureCode))

			var stdout, stderr bytes.Buffer
			waitArgs := withRestoreWait(args)
			waitErr := execute(h.ctx, h.clients, true, waitArgs, &stdout, &stderr)
			assertFailedRestoreWait(t, waitErr, waitArgs, stdout.Bytes(), test.wantCode, string(test.failureCode), source.IntentRevision+1)
		})
	}
}

func TestRestoreWaitFollowsTheRestoreItStarted(t *testing.T) {
	h := newStorageCLIHarness(t)
	sourceA, backupID := stoppedRestoreSource(t, h)

	// Restore A completes under --wait and exits 0.
	argsA := restoreArgs(sourceA, backupID, "--retire-old", "restore-a")
	done := h.startStorageCopyHelper("restore", sourceA.ComputerID)
	var stdout, stderr bytes.Buffer
	if err := execute(h.ctx, h.clients, true, withRestoreWait(argsA), &stdout, &stderr); err != nil {
		t.Fatalf("succeeded restore wait = %v stdout=%s", err, stdout.String())
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	var output storageMutationOutput
	if err := json.Unmarshal(stdout.Bytes(), &output); err != nil || output.Computer == nil || output.Computer.RestoreOperation == nil ||
		output.Computer.RestoreOperation.Status != "retired" || output.Computer.RestoreOperation.OperationRevision != sourceA.IntentRevision+1 {
		t.Fatalf("succeeded restore wait output = %s err=%v", stdout.String(), err)
	}

	// Restore B, started later, is aborted by its failed predecessor copy.
	sourceB := mustStorageComputer(t, h)
	argsB := restoreArgs(sourceB, backupID, "--keep-old-as-backup", "restore-b")
	runStorageCLI(t, h.ctx, h.clients, true, argsB...)
	h.failRestorePredecessorCopy(t, string(l1.ComputerBackupFailureDigestMismatch))

	// L1 names each key's own restore, fresh and on every replay.
	request := func(source l1.Computer, keepOld bool, key string) l1.ComputerRestoreRequest {
		return l1.ComputerRestoreRequest{ComputerMutationPrecondition: l1.ComputerMutationPrecondition{
			IntentRevision: source.IntentRevision, StorageID: source.StorageID, StorageGeneration: source.StorageGeneration},
			KeepOldBackup: keepOld, IdempotencyKey: key}
	}
	_, revisionA, replayedA, err := h.clients.restoreComputerBackup(h.ctx, sourceA.ComputerID, backupID, request(sourceA, false, "restore-a"))
	if err != nil || !replayedA || revisionA != sourceA.IntentRevision+1 {
		t.Fatalf("replay A = revision %d replayed=%t err=%v", revisionA, replayedA, err)
	}
	_, revisionB, replayedB, err := h.clients.restoreComputerBackup(h.ctx, sourceB.ComputerID, backupID, request(sourceB, true, "restore-b"))
	if err != nil || !replayedB || revisionB != sourceB.IntentRevision+1 || revisionB == revisionA {
		t.Fatalf("replay B = revision %d (A=%d) replayed=%t err=%v", revisionB, revisionA, replayedB, err)
	}

	stdout.Reset()
	if err := execute(h.ctx, h.clients, true, withRestoreWait(argsA), &stdout, &stderr); err != nil {
		t.Fatalf("replaying the succeeded restore with --wait after a later one failed: %v", err)
	}
	output = storageMutationOutput{}
	if err := json.Unmarshal(stdout.Bytes(), &output); err != nil || output.Computer == nil || output.Computer.RestoreOperation == nil ||
		output.Computer.RestoreOperation.OperationRevision != revisionA || output.Computer.RestoreOperation.Status != "retired" {
		t.Fatalf("replay A output = %s err=%v", stdout.String(), err)
	}

	stdout.Reset()
	waitArgsB := withRestoreWait(argsB)
	waitErr := execute(h.ctx, h.clients, true, waitArgsB, &stdout, &stderr)
	assertFailedRestoreWait(t, waitErr, waitArgsB, stdout.Bytes(), contract.ErrorConflict,
		string(l1.ComputerBackupFailureDigestMismatch), revisionB)
}

// TestFailedRestoreWaitExitsFiveFromTheRealBinary proves the code reaches the
// process boundary through the typed-exit whitelist in main.go.
func TestFailedRestoreWaitExitsFiveFromTheRealBinary(t *testing.T) {
	t.Parallel()

	binary := buildWefty(t)
	const computerID = "computer-under-test"
	now := time.Now().UTC()
	var stub http.HandlerFunc = func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		base := "/v1/computers/" + computerID
		switch {
		case r.Method == http.MethodGet && r.URL.Path == base:
			// A later restore has already succeeded; the Computer is stable
			// and applied, exactly as after the aborted one.
			computer := l1.Computer{ComputerID: computerID, IntentRevision: 9, AppliedRevision: 9, StorageID: "storage-1",
				StorageGeneration: 2, ReconfigurationPhase: l1.ComputerReconfigurationStable}
			if r.URL.Query().Get("restore_operation_revision") == "3" {
				computer.RestoreOperation = &l1.ComputerRestoreOperation{OperationRevision: 3, BackupID: "backup-1",
					Status: "failed", FailureCode: l1.ComputerBackupFailureDigestMismatch, CompletedAt: &now}
			}
			_ = json.NewEncoder(w).Encode(computer)
		case r.Method == http.MethodPost && r.URL.Path == base+"/backups/backup-1/restore":
			w.Header().Set("Restore-Operation-Revision", "3")
			w.Header().Set("Idempotent-Replay", "true")
			_ = json.NewEncoder(w).Encode(l1.Computer{ComputerID: computerID, IntentRevision: 9, AppliedRevision: 9,
				StorageID: "storage-1", StorageGeneration: 2, ReconfigurationPhase: l1.ComputerReconfigurationStable})
		case r.Method == http.MethodGet && r.URL.Path == base+"/storage-provenance":
			_ = json.NewEncoder(w).Encode(l1.ComputerStorageProvenance{})
		default:
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(contract.APIError{Code: contract.ErrorNotFound, Message: r.Method + " " + r.URL.Path})
		}
	}
	l1Address := startStubLedger(t, stub)
	args := []string{"--json", "--l1=" + l1Address, "services", "restore", computerID, "backup-1", "--keep-old-as-backup",
		"--intent-revision", "2", "--storage-id", "storage-1", "--storage-generation", "1",
		"--idempotency-key", "k", "--wait", "5s", "--poll-interval", "10ms"}
	code, output := runWefty(t, binary, 60*time.Second, args...)
	if code != exitConflict || !strings.Contains(output, string(l1.ComputerBackupFailureDigestMismatch)) {
		t.Fatalf("failed `restore --wait` exited %d, want %d:\n%s", code, exitConflict, output)
	}
}
