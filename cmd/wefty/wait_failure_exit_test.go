package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

// A `--wait` that observes its operation end `failed` is a failed command
// (#588). The JSON already said so; the exit status is what a script reads.

// backupCreateWaitArgs replays a Backup create with --wait. The replay carries
// the original CAS tuple, so it observes the operation the first call started
// after the helper has finished it.
func backupCreateWaitArgs(h *storageCLIHarness, computer l1.Computer, key string) []string {
	return []string{"services", "backup", "create", computer.ComputerID,
		"--intent-revision", fmt.Sprint(computer.IntentRevision), "--storage-id", computer.StorageID,
		"--storage-generation", fmt.Sprint(computer.StorageGeneration),
		"--idempotency-key", key, "--allow-power-off", "--wait", "2s", "--poll-interval", "1ms"}
}

func TestBackupCreateWaitExitsNonZeroWhenTheBackupFails(t *testing.T) {
	for _, test := range []struct {
		failureCode l1.ComputerBackupFailureCode
		wantCode    contract.ErrorCode
	}{
		{l1.ComputerBackupFailureSourceNeverDetached, contract.ErrorConflict},
		{l1.ComputerBackupFailureDigestMismatch, contract.ErrorConflict},
		{l1.ComputerBackupFailureInsufficientDisk, contract.ErrorCapacityExhausted},
	} {
		t.Run(string(test.failureCode), func(t *testing.T) {
			h := newStorageCLIHarness(t)
			capped := runStorageCLI(t, h.ctx, h.clients, true, "services", "backup", "set-cap", h.computer.ComputerID,
				"--cap", "1", "--expect-current")
			var capOutput storageMutationOutput
			if err := json.Unmarshal(capped, &capOutput); err != nil || capOutput.Computer == nil {
				t.Fatalf("set-cap output = %s err=%v", capped, err)
			}
			source := *capOutput.Computer
			runStorageCLI(t, h.ctx, h.clients, true, "services", "backup", "create", source.ComputerID,
				"--intent-revision", fmt.Sprint(source.IntentRevision), "--storage-id", source.StorageID,
				"--storage-generation", fmt.Sprint(source.StorageGeneration),
				"--idempotency-key", "failed-backup", "--allow-power-off")
			h.completeBackupHelperFailure(t, string(test.failureCode))

			var stdout, stderr bytes.Buffer
			args := backupCreateWaitArgs(h, source, "failed-backup")
			waitErr := execute(h.ctx, h.clients, true, args, &stdout, &stderr)
			assertCLIAPIError(t, waitErr, test.wantCode)
			if got := commandExitCodeForArgs(waitErr, args); got != exitConflict {
				t.Fatalf("failed Backup wait exit = %d, want %d (%v)", got, exitConflict, waitErr)
			}
			if !strings.Contains(waitErr.Error(), string(test.failureCode)) {
				t.Fatalf("failed Backup wait error %q does not name %q", waitErr, test.failureCode)
			}
			var output storageMutationOutput
			if err := json.Unmarshal(stdout.Bytes(), &output); err != nil || output.Backups == nil ||
				output.Backups.LastOperation == nil || output.Backups.LastOperation.Status != "failed" ||
				output.Backups.LastOperation.FailureCode != test.failureCode ||
				output.Observation == nil || output.Observation.Status != "observed" {
				t.Fatalf("failed Backup wait output changed: %s err=%v", stdout.String(), err)
			}
		})
	}
}

func TestBackupCreateWaitExitsZeroWhenTheBackupSucceeds(t *testing.T) {
	h := newStorageCLIHarness(t)
	capped := runStorageCLI(t, h.ctx, h.clients, true, "services", "backup", "set-cap", h.computer.ComputerID,
		"--cap", "1", "--expect-current")
	var capOutput storageMutationOutput
	if err := json.Unmarshal(capped, &capOutput); err != nil || capOutput.Computer == nil {
		t.Fatalf("set-cap output = %s err=%v", capped, err)
	}
	source := *capOutput.Computer
	runStorageCLI(t, h.ctx, h.clients, true, "services", "backup", "create", source.ComputerID,
		"--intent-revision", fmt.Sprint(source.IntentRevision), "--storage-id", source.StorageID,
		"--storage-generation", fmt.Sprint(source.StorageGeneration),
		"--idempotency-key", "good-backup", "--allow-power-off")
	h.completeBackupHelper(t)
	var stdout, stderr bytes.Buffer
	args := backupCreateWaitArgs(h, source, "good-backup")
	waitErr := execute(h.ctx, h.clients, true, args, &stdout, &stderr)
	if waitErr != nil {
		t.Fatalf("succeeded Backup wait = %v stdout=%s", waitErr, stdout.String())
	}
	var output storageMutationOutput
	if err := json.Unmarshal(stdout.Bytes(), &output); err != nil || output.Backups == nil ||
		output.Backups.LastOperation == nil || output.Backups.LastOperation.Status == "failed" || len(output.Backups.Backups) != 1 {
		t.Fatalf("succeeded Backup wait output = %s err=%v", stdout.String(), err)
	}
}

func TestCustodyExportWaitExitsNonZeroOnlyWhenTheExportFails(t *testing.T) {
	for _, failed := range []bool{true, false} {
		name := "succeeded"
		if failed {
			name = "failed"
		}
		t.Run(name, func(t *testing.T) {
			h := newStorageCLIHarness(t)
			capped := runStorageCLI(t, h.ctx, h.clients, true, "services", "backup", "set-cap", h.computer.ComputerID,
				"--cap", "1", "--expect-current")
			var capOutput storageMutationOutput
			if err := json.Unmarshal(capped, &capOutput); err != nil || capOutput.Computer == nil {
				t.Fatalf("set-cap output = %s err=%v", capped, err)
			}
			runStorageCLI(t, h.ctx, h.clients, true, "services", "backup", "create", h.computer.ComputerID,
				"--expect-current", "--idempotency-key", "export-source", "--allow-power-off")
			h.completeBackupHelper(t)
			backups, err := h.store.ListComputerBackups(h.ctx, h.computer.ComputerID)
			if err != nil || len(backups.Backups) != 1 {
				t.Fatalf("export Backup fixture = %#v err=%v", backups, err)
			}
			source := mustStorageComputer(t, h)
			path := filepath.Join(t.TempDir(), "external")
			exportArgs := []string{"services", "custody", "export", source.ComputerID, backups.Backups[0].BackupID,
				"--path", path, "--intent-revision", fmt.Sprint(source.IntentRevision), "--storage-id", source.StorageID,
				"--storage-generation", fmt.Sprint(source.StorageGeneration), "--idempotency-key", "export-under-test"}
			runStorageCLI(t, h.ctx, h.clients, true, exportArgs...)
			directives, err := h.store.ListNodeComputerCustodyExportDirectives(h.ctx, "fabric-storage-node", h.node.NodeID, h.node.BootSessionID)
			if err != nil || len(directives) != 1 {
				t.Fatalf("Custody export directives = %#v err=%v", directives, err)
			}
			d := directives[0]
			receipt := contract.ComputerCustodyExportReceipt{Kind: "computer_custody_export_verified", ReceiptID: "receipt-" + d.ExportID,
				ExportID: d.ExportID, BackupID: d.BackupID, CopyID: d.CopyID, ComputerID: d.ComputerID, StorageID: d.StorageID,
				StorageGeneration: d.StorageGeneration, NodeID: d.BoundNodeID, RootInstanceID: d.RootInstanceID,
				OperationRevision: d.OperationRevision, CustodyFence: d.CustodyFence, HelperGeneration: 1,
				ExternalPath: d.ExternalPath, AllocatedSize: d.AllocatedSize, ContentDigest: d.ContentDigest,
				ManifestDigest: storageCLIPlatformDigest, OwnershipApplied: true, PrivateModeApplied: true}
			if failed {
				receipt.Kind, receipt.ManifestDigest = "computer_custody_export_failed", ""
				receipt.OwnershipApplied, receipt.PrivateModeApplied = false, false
				receipt.FailureCode = "destination_not_empty"
			}
			if _, err := h.store.AcknowledgeComputerCustodyExport(h.ctx, "fabric-storage-node", d.ComputerID,
				l1.ComputerCustodyExportAcknowledgementRequest{NodeID: h.node.NodeID, BootSessionID: h.node.BootSessionID,
					IdempotencyKey: receipt.ReceiptID, Receipt: receipt}); err != nil {
				t.Fatal(err)
			}

			args := append(exportArgs, "--wait", "2s", "--poll-interval", "1ms")
			var stdout, stderr bytes.Buffer
			waitErr := execute(h.ctx, h.clients, true, args, &stdout, &stderr)
			var output storageMutationOutput
			if err := json.Unmarshal(stdout.Bytes(), &output); err != nil || output.CustodyExport == nil ||
				output.Observation == nil || output.Observation.Status != "observed" {
				t.Fatalf("Custody export wait output = %s err=%v", stdout.String(), err)
			}
			if !failed {
				if waitErr != nil || output.CustodyExport.Status != "available" {
					t.Fatalf("succeeded Custody export wait = %v status=%q", waitErr, output.CustodyExport.Status)
				}
				return
			}
			assertCLIAPIError(t, waitErr, contract.ErrorConflict)
			if got := commandExitCodeForArgs(waitErr, args); got != exitConflict ||
				!strings.Contains(waitErr.Error(), "destination_not_empty") || output.CustodyExport.Status != "failed" {
				t.Fatalf("failed Custody export wait exit=%d err=%v status=%q", got, waitErr, output.CustodyExport.Status)
			}
		})
	}
}

// TestFailedWaitsExitFiveFromTheRealBinary proves the code reaches the process
// boundary: the typed-exit whitelist in main.go has to let `backup` and
// `custody` through, or every in-process assertion above passes while the real
// binary still exits 1.
func TestFailedWaitsExitFiveFromTheRealBinary(t *testing.T) {
	t.Parallel()

	binary := buildWefty(t)
	const computerID = "computer-under-test"
	failedOperation := &l1.ComputerBackupOperationOutcome{OperationRevision: 3, BackupID: "backup-1",
		Status: "failed", FailureCode: l1.ComputerBackupFailureSourceNeverDetached}
	now := time.Now().UTC()
	exportStatus := "failed"
	var stub http.HandlerFunc = func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		base := "/v1/computers/" + computerID
		switch {
		case r.Method == http.MethodGet && r.URL.Path == base:
			_ = json.NewEncoder(w).Encode(l1.Computer{ComputerID: computerID, IntentRevision: 2, StorageID: "storage-1", StorageGeneration: 1})
		case r.Method == http.MethodPost && r.URL.Path == base+"/backups":
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(l1.Computer{ComputerID: computerID, IntentRevision: 3, StorageID: "storage-1", StorageGeneration: 1})
		case r.Method == http.MethodGet && r.URL.Path == base+"/backups":
			operation := *failedOperation
			operation.CompletedAt = &now
			_ = json.NewEncoder(w).Encode(l1.BackupList{Backups: []l1.Backup{}, LastOperation: &operation})
		case r.Method == http.MethodPost && r.URL.Path == base+"/backups/backup-1/export":
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(l1.ComputerCustodyExport{ExportID: "export-1", ComputerID: computerID, BackupID: "backup-1", Status: "planned"})
		case r.Method == http.MethodGet && r.URL.Path == base+"/custody-exports":
			_ = json.NewEncoder(w).Encode([]l1.ComputerCustodyExport{{ExportID: "export-1", ComputerID: computerID,
				BackupID: "backup-1", Status: exportStatus, FailureCode: contract.CustodyExportPathUnconfined, CompletedAt: &now}})
		case r.Method == http.MethodGet && r.URL.Path == base+"/storage-provenance":
			_ = json.NewEncoder(w).Encode(l1.ComputerStorageProvenance{})
		default:
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(contract.APIError{Code: contract.ErrorNotFound, Message: r.Method + " " + r.URL.Path})
		}
	}
	l1Address := startStubLedger(t, stub)
	cas := []string{"--intent-revision", "2", "--storage-id", "storage-1", "--storage-generation", "1"}
	backupArgs := append([]string{"--json", "--l1=" + l1Address, "services", "backup", "create", computerID,
		"--idempotency-key", "k", "--allow-power-off", "--wait", "5s", "--poll-interval", "10ms"}, cas...)
	exportArgs := append([]string{"--json", "--l1=" + l1Address, "services", "custody", "export", computerID, "backup-1",
		"--path", "/mnt/export", "--idempotency-key", "k", "--wait", "5s", "--poll-interval", "10ms"}, cas...)

	code, output := runWefty(t, binary, 60*time.Second, backupArgs...)
	if code != exitConflict || !strings.Contains(output, `"status": "failed"`) && !strings.Contains(output, `"status":"failed"`) {
		t.Fatalf("failed `backup create --wait` exited %d, want %d:\n%s", code, exitConflict, output)
	}
	code, output = runWefty(t, binary, 60*time.Second, exportArgs...)
	if code != exitConflict || !strings.Contains(output, contract.CustodyExportPathUnconfined) {
		t.Fatalf("failed `custody export --wait` exited %d, want %d:\n%s", code, exitConflict, output)
	}
}
