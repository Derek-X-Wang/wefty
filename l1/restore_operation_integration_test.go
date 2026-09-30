package l1

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

// A restore aborted by its failed predecessor Backup copy and a restore that
// completed both leave the Computer `stable`, so a client waiting on a restore
// has to read that restore's own record (#592). The begin response names the
// operation its idempotency key started, fresh and on every replay, and a
// Computer read returns that operation's terminal state.
func TestRestoreBeginNamesItsOwnOperationAndTheReadReturnsItsOutcome(t *testing.T) {
	h, node, computer, source, _ := publishedBackupForStorageCopy(t, 3)
	operator := h.client(fabric.Identity{NodeID: "operator", Tags: []string{DefaultClientPrincipalTag}})
	restorePath := "/v1/computers/" + computer.ComputerID + "/backups/" + source.BackupID + "/restore"
	begin := func(precondition Computer, keepOld bool, key string, wantStatus int) int64 {
		t.Helper()
		status, headers, body := h.do(operator, http.MethodPost, restorePath, map[string]any{
			"intent_revision": precondition.IntentRevision, "storage_id": precondition.StorageID,
			"storage_generation": precondition.StorageGeneration, "keep_old_as_backup": keepOld, "idempotency_key": key,
		})
		if status != wantStatus {
			t.Fatalf("restore %s status = %d, want %d: %s", key, status, wantStatus, body)
		}
		revision, err := strconv.ParseInt(headers.Get("Restore-Operation-Revision"), 10, 64)
		if err != nil {
			t.Fatalf("restore %s response has no Restore-Operation-Revision: %v (%v)", key, err, headers)
		}
		return revision
	}
	settleCopy := func(revision int64, oldFailure ComputerBackupFailureCode) {
		t.Helper()
		if err := h.store.RecordComputerRestoreAuthorityRevoked(context.Background(), computer.ComputerID, revision,
			testRestoreRevocationEvidence(computer.ComputerID, revision)); err != nil {
			t.Fatal(err)
		}
		directives, err := h.store.ListNodeComputerStorageCopyDirectives(context.Background(), "fabric-computer-node", node.NodeID, node.BootSessionID)
		if err != nil || len(directives) != 1 || directives[0].OperationRevision != revision {
			t.Fatalf("restore %d directives = %#v err=%v", revision, directives, err)
		}
		d := directives[0]
		receipt := successfulStorageCopyReceipt(d)
		request := ComputerStorageCopyAcknowledgementRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID,
			IdempotencyKey: receipt.ReceiptID, Receipt: receipt}
		if oldFailure != "" {
			old := successfulOldGenerationBackupReceipt(d)
			old.Kind, old.ContentDigest, old.FailureCode, old.CopyAbsent = computerBackupFailureReceiptKind, "", string(oldFailure), true
			request.OldBackupReceipt = &old
			if _, err := h.store.AcknowledgeComputerStorageCopy(context.Background(), "fabric-computer-node", computer.ComputerID,
				request); errorCode(err) != contract.ErrorConflict {
				t.Fatalf("failed predecessor copy = %v, want the typed abort", err)
			}
			return
		}
		if _, err := h.store.AcknowledgeComputerStorageCopy(context.Background(), "fabric-computer-node", computer.ComputerID, request); err != nil {
			t.Fatal(err)
		}
		if _, err := h.store.AcknowledgeComputerRestoreRetirement(context.Background(), "fabric-computer-node", computer.ComputerID,
			RemovalAcknowledgementRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID,
				RemovalGeneration: uint64(d.OperationRevision), CleanupFence: d.CleanupFence,
				RootInstanceID: d.RootInstanceID, IdempotencyKey: fmt.Sprintf("retired-%d", revision)}); err != nil {
			t.Fatal(err)
		}
	}
	read := func(query string) (int, Computer) {
		t.Helper()
		status, _, body := h.do(operator, http.MethodGet, "/v1/computers/"+computer.ComputerID+query, nil)
		var observed Computer
		if status == http.StatusOK {
			if err := json.Unmarshal(body, &observed); err != nil {
				t.Fatal(err)
			}
		}
		return status, observed
	}

	revisionA := begin(computer, false, "restore-a", http.StatusAccepted)
	if revisionA != computer.IntentRevision+1 {
		t.Fatalf("fresh restore A names revision %d, want %d", revisionA, computer.IntentRevision+1)
	}
	settleCopy(revisionA, "")
	afterA, err := h.store.GetComputer(context.Background(), computer.ComputerID)
	if err != nil {
		t.Fatal(err)
	}
	revisionB := begin(afterA, true, "restore-b", http.StatusAccepted)
	settleCopy(revisionB, ComputerBackupFailureInsufficientDisk)
	aborted, err := h.store.GetComputer(context.Background(), computer.ComputerID)
	if err != nil || aborted.ReconfigurationPhase != ComputerReconfigurationStable || aborted.AppliedRevision != revisionB {
		t.Fatalf("aborted restore left Computer = %#v err=%v", aborted, err)
	}

	// Replays name the key's own operation, not the Computer's latest one.
	if replayA := begin(computer, false, "restore-a", http.StatusOK); replayA != revisionA {
		t.Fatalf("replayed restore A names revision %d, want its own %d (latest is %d)", replayA, revisionA, revisionB)
	}
	if replayB := begin(afterA, true, "restore-b", http.StatusOK); replayB != revisionB {
		t.Fatalf("replayed restore B names revision %d, want %d", replayB, revisionB)
	}

	if status, plain := read(""); status != http.StatusOK || plain.RestoreOperation != nil {
		t.Fatalf("plain Computer read = %d restore_operation=%#v", status, plain.RestoreOperation)
	}
	status, observedA := read(fmt.Sprintf("?restore_operation_revision=%d", revisionA))
	if status != http.StatusOK || observedA.RestoreOperation == nil || observedA.RestoreOperation.OperationRevision != revisionA ||
		observedA.RestoreOperation.Status != "retired" || observedA.RestoreOperation.FailureCode != "" ||
		observedA.RestoreOperation.BackupID != source.BackupID || observedA.RestoreOperation.CompletedAt == nil {
		t.Fatalf("restore A read = %d %#v", status, observedA.RestoreOperation)
	}
	status, observedB := read(fmt.Sprintf("?restore_operation_revision=%d", revisionB))
	if status != http.StatusOK || observedB.RestoreOperation == nil || observedB.RestoreOperation.OperationRevision != revisionB ||
		observedB.RestoreOperation.Status != "failed" ||
		observedB.RestoreOperation.FailureCode != ComputerBackupFailureInsufficientDisk ||
		observedB.RestoreOperation.CompletedAt == nil {
		t.Fatalf("aborted restore B read = %d %#v", status, observedB.RestoreOperation)
	}
	if status, _ := read("?restore_operation_revision=999"); status != http.StatusNotFound {
		t.Fatalf("unknown restore operation read = %d, want 404", status)
	}
	if status, _ := read("?restore_operation_revision=zero"); status != http.StatusBadRequest {
		t.Fatalf("malformed restore operation read = %d, want 400", status)
	}
}
