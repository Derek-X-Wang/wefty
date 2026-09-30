package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

// A `--wait` ends when its own operation's status is terminal, not when a
// completion time appears: removal supersedes a planned Backup or export
// without one (#602). And a clone `--wait` follows the clone its key started,
// never the destination's latest revision or Job failure.

// terminalWaitBudget is far longer than a status-judged wait needs, so a wait
// that only ends at its timeout is caught by the elapsed-time assertion.
const terminalWaitBudget = "10s"

func removeStorageComputer(t *testing.T, h *storageCLIHarness) {
	t.Helper()
	current := mustStorageComputer(t, h)
	if _, err := h.store.RemoveComputer(h.ctx, current.ComputerID, l1.ComputerRemoveRequest{
		ComputerMutationPrecondition: l1.ComputerMutationPrecondition{IntentRevision: current.IntentRevision,
			StorageID: current.StorageID, StorageGeneration: current.StorageGeneration, Actor: "operator"},
	}); err != nil {
		t.Fatal(err)
	}
}

// executePromptly runs one waited command and fails when it only returned at
// its wait budget.
func executePromptly(t *testing.T, h *storageCLIHarness, args []string) ([]byte, error) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	started := time.Now()
	err := execute(h.ctx, h.clients, true, args, &stdout, &stderr)
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("wefty %s took %s: the wait ran to its budget instead of seeing a terminal status (err=%v)",
			strings.Join(args, " "), elapsed, err)
	}
	return stdout.Bytes(), err
}

func assertSupersededWait(t *testing.T, waitErr error, args []string, idKey, wantID string) {
	t.Helper()
	assertCLIAPIError(t, waitErr, contract.ErrorConflict)
	if got := commandExitCodeForArgs(waitErr, args); got != exitConflict {
		t.Fatalf("superseded wait exit = %d, want %d (%v)", got, exitConflict, waitErr)
	}
	var refusal *apiResponseError
	if !errors.As(waitErr, &refusal) || refusal.APIError.Details["status"] != "superseded" ||
		refusal.APIError.Details[idKey] != wantID || !strings.Contains(waitErr.Error(), "superseded") {
		t.Fatalf("superseded wait error = %v (%#v)", waitErr, refusal)
	}
}

func TestBackupCreateWaitExitsFivePromptlyWhenRemovalSupersedesIt(t *testing.T) {
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
		"--idempotency-key", "superseded-backup", "--allow-power-off")
	removeStorageComputer(t, h)

	args := backupCreateWaitArgs(h, source, "superseded-backup")
	args[len(args)-3] = terminalWaitBudget
	stdout, waitErr := executePromptly(t, h, args)
	var output storageMutationOutput
	if err := json.Unmarshal(stdout, &output); err != nil || output.Backups == nil || output.Backups.Operation == nil ||
		output.Backups.Operation.Status != "superseded" || output.Backups.Operation.CompletedAt != nil ||
		output.Observation == nil || output.Observation.Status != "observed" {
		t.Fatalf("superseded Backup wait output = %s err=%v", stdout, err)
	}
	assertSupersededWait(t, waitErr, args, "backup_id", output.Backups.Operation.BackupID)
}

func TestCustodyExportWaitExitsFivePromptlyWhenRemovalSupersedesIt(t *testing.T) {
	h := newStorageCLIHarness(t)
	runStorageCLI(t, h.ctx, h.clients, true, "services", "backup", "set-cap", h.computer.ComputerID,
		"--cap", "1", "--expect-current")
	runStorageCLI(t, h.ctx, h.clients, true, "services", "backup", "create", h.computer.ComputerID,
		"--expect-current", "--idempotency-key", "export-source", "--allow-power-off")
	h.completeBackupHelper(t)
	backups, err := h.store.ListComputerBackups(h.ctx, h.computer.ComputerID)
	if err != nil || len(backups.Backups) != 1 {
		t.Fatalf("export Backup fixture = %#v err=%v", backups, err)
	}
	source := mustStorageComputer(t, h)
	exportArgs := []string{"services", "custody", "export", source.ComputerID, backups.Backups[0].BackupID,
		"--path", filepath.Join(t.TempDir(), "external"), "--intent-revision", fmt.Sprint(source.IntentRevision),
		"--storage-id", source.StorageID, "--storage-generation", fmt.Sprint(source.StorageGeneration),
		"--idempotency-key", "superseded-export"}
	runStorageCLI(t, h.ctx, h.clients, true, exportArgs...)
	removeStorageComputer(t, h)

	args := append(append([]string{}, exportArgs...), "--wait", terminalWaitBudget, "--poll-interval", "1ms")
	stdout, waitErr := executePromptly(t, h, args)
	var output storageMutationOutput
	if err := json.Unmarshal(stdout, &output); err != nil || output.CustodyExport == nil ||
		output.CustodyExport.Status != "superseded" || output.CustodyExport.CompletedAt != nil ||
		output.Observation == nil || output.Observation.Status != "observed" {
		t.Fatalf("superseded Custody export wait output = %s err=%v", stdout, err)
	}
	assertSupersededWait(t, waitErr, args, "export_id", output.CustodyExport.ExportID)
}

// refuseCloneCopy settles the one reserved clone with the helper's typed
// capacity refusal.
func (h *storageCLIHarness) refuseCloneCopy(t *testing.T, observedAvailableBytes int64) {
	t.Helper()
	directives, err := h.store.ListNodeComputerStorageCopyDirectives(h.ctx, "fabric-storage-node", h.node.NodeID, h.node.BootSessionID)
	if err != nil || len(directives) != 1 || directives[0].Operation != "clone" {
		t.Fatalf("clone directives = %#v err=%v", directives, err)
	}
	d := directives[0]
	receipt := l1.ComputerStorageCopyReceipt{Kind: "computer_storage_copy_failed_absent",
		ReceiptID: "refused-" + d.DestinationComputerID, Operation: d.Operation, BackupID: d.BackupID, CopyID: d.CopyID,
		SourceComputerID: d.SourceComputerID, SourceStorageID: d.SourceStorageID, SourceGeneration: d.SourceGeneration,
		DestinationComputerID: d.DestinationComputerID, DestinationStorageID: d.DestinationStorageID,
		DestinationGeneration: d.DestinationGeneration, NodeID: d.BoundNodeID, RootInstanceID: d.RootInstanceID,
		JobID: d.JobID, OperationRevision: d.OperationRevision, CleanupFence: d.CleanupFence, HelperGeneration: 9,
		SourceSize: d.SourceSize, DestinationSize: d.DestinationSize, SourceDigest: d.SourceDigest,
		FailureCode: "insufficient_disk", DestinationAbsent: true, ObservedAvailableBytes: observedAvailableBytes}
	if _, err := h.store.AcknowledgeComputerStorageCopy(h.ctx, "fabric-storage-node", d.DestinationComputerID,
		l1.ComputerStorageCopyAcknowledgementRequest{NodeID: h.node.NodeID, BootSessionID: h.node.BootSessionID,
			IdempotencyKey: receipt.ReceiptID, Receipt: receipt}); err != nil {
		t.Fatal(err)
	}
}

// refuseGrow runs a grow on a Computer and settles it with the helper's
// capacity refusal, which latches `insufficient_disk` on the Computer's Job.
func (h *storageCLIHarness) refuseGrow(t *testing.T, computerID string) {
	t.Helper()
	current, err := h.store.GetComputer(h.ctx, computerID)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := h.store.BeginComputerGrow(h.ctx, computerID, l1.ComputerGrowRequest{
		ComputerMutationPrecondition: l1.ComputerMutationPrecondition{IntentRevision: current.IntentRevision,
			StorageID: current.StorageID, StorageGeneration: current.StorageGeneration, Actor: "operator"},
		DiskBytes: current.DesiredDiskBytes * 2, IdempotencyKey: "refused-grow",
	}); err != nil {
		t.Fatal(err)
	}
	directives, err := h.store.ListNodeComputerStorageGrowDirectives(h.ctx, "fabric-storage-node", h.node.NodeID, h.node.BootSessionID)
	if err != nil || len(directives) != 1 {
		t.Fatalf("grow directives = %#v err=%v", directives, err)
	}
	d := directives[0]
	receipt := l1.ComputerStorageGrowReceipt{Kind: "computer_storage_grow_failed_unchanged", ReceiptID: "refused-grow-" + d.ComputerID,
		ComputerID: d.ComputerID, StorageID: d.StorageID, StorageGeneration: d.StorageGeneration, NodeID: d.BoundNodeID,
		RootInstanceID: d.RootInstanceID, JobID: d.JobID, OperationRevision: d.OperationRevision, OperationFence: d.OperationFence,
		HelperGeneration: 1, OldDiskBytes: d.OldDiskBytes, NewDiskBytes: d.NewDiskBytes, FailureCode: "insufficient_disk",
		ObservedAvailableBytes: 1 << 30}
	if _, err := h.store.AcknowledgeComputerStorageGrow(h.ctx, "fabric-storage-node", d.ComputerID,
		l1.ComputerStorageGrowAcknowledgementRequest{NodeID: h.node.NodeID, BootSessionID: h.node.BootSessionID,
			IdempotencyKey: receipt.ReceiptID, Receipt: receipt}); err != nil {
		t.Fatal(err)
	}
	grown, err := h.store.GetComputer(h.ctx, computerID)
	var failure contract.SpawnFailure
	if err != nil || grown.ReconfigurationPhase != l1.ComputerReconfigurationStable || grown.AppliedRevision != d.OperationRevision ||
		json.Unmarshal(grown.CurrentJob.LastFailure, &failure) != nil || failure.Code != contract.SpawnFailureInsufficientDisk {
		t.Fatalf("refused grow left Computer = %#v err=%v", grown, err)
	}
}

func TestCloneWaitFollowsTheCloneItStarted(t *testing.T) {
	h := newStorageCLIHarness(t)
	runStorageCLI(t, h.ctx, h.clients, true, "services", "backup", "set-cap", h.computer.ComputerID,
		"--cap", "1", "--expect-current")
	runStorageCLI(t, h.ctx, h.clients, true, "services", "backup", "create", h.computer.ComputerID,
		"--expect-current", "--idempotency-key", "clone-source", "--allow-power-off")
	h.completeBackupHelper(t)
	backups, err := h.store.ListComputerBackups(h.ctx, h.computer.ComputerID)
	if err != nil || len(backups.Backups) != 1 {
		t.Fatalf("clone source = %#v err=%v", backups, err)
	}
	backupID := backups.Backups[0].BackupID
	source := mustStorageComputer(t, h)
	cloneArgs := func(name, key string, diskBytes int64) []string {
		return []string{"services", "clone", source.ComputerID, backupID, "--name", name,
			"--disk-bytes", fmt.Sprint(diskBytes), "--intent-revision", fmt.Sprint(source.IntentRevision),
			"--storage-id", source.StorageID, "--storage-generation", fmt.Sprint(source.StorageGeneration),
			"--idempotency-key", key}
	}
	request := func(name, key string, diskBytes int64) l1.ComputerCloneRequest {
		return l1.ComputerCloneRequest{ComputerMutationPrecondition: l1.ComputerMutationPrecondition{
			IntentRevision: source.IntentRevision, StorageID: source.StorageID, StorageGeneration: source.StorageGeneration},
			Name: name, DiskBytes: diskBytes, IdempotencyKey: key}
	}
	const diskA, diskB = int64(2) << 30, int64(64) << 30

	// Clone A completes, and a later grow on its destination is refused,
	// latching `insufficient_disk` on the destination Job.
	_, operationA, replayedA, err := h.clients.cloneComputerBackup(h.ctx, source.ComputerID, backupID, request("clone-a", "clone-a", diskA))
	if err != nil || replayedA || operationA.computerID == "" || operationA.computerID == source.ComputerID || operationA.operationRevision != 1 {
		t.Fatalf("fresh clone A = %#v replayed=%t err=%v", operationA, replayedA, err)
	}
	if err := <-h.startStorageCopyHelper("clone", operationA.computerID); err != nil {
		t.Fatal(err)
	}
	h.refuseGrow(t, operationA.computerID)

	// Clone B, started later from the same Backup, is refused.
	_, operationB, _, err := h.clients.cloneComputerBackup(h.ctx, source.ComputerID, backupID, request("clone-b", "clone-b", diskB))
	if err != nil || operationB.computerID == "" || operationB.computerID == operationA.computerID {
		t.Fatalf("fresh clone B = %#v (A=%#v) err=%v", operationB, operationA, err)
	}
	h.refuseCloneCopy(t, 25112510464)

	// Replays name each key's own clone.
	replayA, replayOperationA, replayedA, err := h.clients.cloneComputerBackup(h.ctx, source.ComputerID, backupID, request("clone-a", "clone-a", diskA))
	if err != nil || !replayedA || replayOperationA != operationA || replayA.IntentRevision <= operationA.operationRevision {
		t.Fatalf("replayed clone A = %#v (fresh %#v) intent=%d replayed=%t err=%v", replayOperationA, operationA,
			replayA.IntentRevision, replayedA, err)
	}
	if _, replayOperationB, replayedB, err := h.clients.cloneComputerBackup(h.ctx, source.ComputerID, backupID,
		request("clone-b", "clone-b", diskB)); err != nil || !replayedB || replayOperationB != operationB {
		t.Fatalf("replayed clone B = %#v (fresh %#v) replayed=%t err=%v", replayOperationB, operationB, replayedB, err)
	}
	if _, err := h.clients.getComputerCloneOperation(h.ctx, operationA.computerID, 999); err == nil {
		t.Fatal("an unknown clone_operation_revision was not refused")
	}

	var stdout, stderr bytes.Buffer
	argsA := append(cloneArgs("clone-a", "clone-a", diskA), "--wait", "2s", "--poll-interval", "1ms")
	if err := execute(h.ctx, h.clients, true, argsA, &stdout, &stderr); err != nil {
		t.Fatalf("replaying the completed clone with --wait after a later refusal: %v stdout=%s", err, stdout.String())
	}
	var output storageMutationOutput
	if err := json.Unmarshal(stdout.Bytes(), &output); err != nil || output.Computer == nil ||
		output.Computer.ComputerID != operationA.computerID || output.Computer.CloneOperation == nil ||
		output.Computer.CloneOperation.Status != "complete" || output.Computer.CloneOperation.BackupID != backupID ||
		output.Observation == nil || output.Observation.Status != "observed" {
		t.Fatalf("replay clone A output = %s err=%v", stdout.String(), err)
	}

	stdout.Reset()
	argsB := append(cloneArgs("clone-b", "clone-b", diskB), "--wait", "2s", "--poll-interval", "1ms")
	waitErr := execute(h.ctx, h.clients, true, argsB, &stdout, &stderr)
	assertCLIAPIError(t, waitErr, contract.ErrorCapacityExhausted)
	var refusal *apiResponseError
	if !errors.As(waitErr, &refusal) || commandExitCodeForArgs(waitErr, argsB) != exitConflict ||
		refusal.APIError.Details["computer_id"] != operationB.computerID || refusal.APIError.Details["status"] != "failed" ||
		refusal.APIError.Details["failure_code"] != string(contract.SpawnFailureInsufficientDisk) ||
		fmt.Sprint(refusal.APIError.Details["requested_bytes"]) != fmt.Sprint(diskB) ||
		fmt.Sprint(refusal.APIError.Details["observed_available_bytes"]) != "25112510464" {
		t.Fatalf("replay clone B wait error = %v (%#v)", waitErr, refusal)
	}
}

// TestSupersededWaitsExitFiveFromTheRealBinary proves the status-judged waits
// reach the process boundary: neither superseded operation carries a
// completion time, and the clone's destination has since moved on.
func TestSupersededWaitsExitFiveFromTheRealBinary(t *testing.T) {
	t.Parallel()

	binary := buildWefty(t)
	const computerID = "computer-under-test"
	const cloneID = "computer-clone"
	var stub http.HandlerFunc = func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		base := "/v1/computers/" + computerID
		switch {
		case r.Method == http.MethodGet && r.URL.Path == base:
			_ = json.NewEncoder(w).Encode(l1.Computer{ComputerID: computerID, IntentRevision: 2, StorageID: "storage-1", StorageGeneration: 1})
		case r.Method == http.MethodPost && r.URL.Path == base+"/backups":
			w.Header().Set("Backup-Id", "backup-1")
			w.Header().Set("Backup-Operation-Revision", "3")
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(l1.Computer{ComputerID: computerID, IntentRevision: 3, StorageID: "storage-1", StorageGeneration: 1})
		case r.Method == http.MethodGet && r.URL.Path == base+"/backups":
			list := l1.BackupList{Backups: []l1.Backup{}}
			if r.URL.Query().Get("backup_id") == "backup-1" {
				list.Operation = &l1.ComputerBackupOperationOutcome{OperationRevision: 3, BackupID: "backup-1", Status: "superseded"}
			}
			_ = json.NewEncoder(w).Encode(list)
		case r.Method == http.MethodPost && r.URL.Path == base+"/backups/backup-1/export":
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(l1.ComputerCustodyExport{ExportID: "export-1", ComputerID: computerID, BackupID: "backup-1", Status: "planned"})
		case r.Method == http.MethodGet && r.URL.Path == base+"/custody-exports":
			_ = json.NewEncoder(w).Encode([]l1.ComputerCustodyExport{{ExportID: "export-1", ComputerID: computerID,
				BackupID: "backup-1", Status: "superseded"}})
		case r.Method == http.MethodPost && r.URL.Path == base+"/backups/backup-1/clone":
			w.Header().Set("Clone-Computer-Id", cloneID)
			w.Header().Set("Clone-Operation-Revision", "1")
			w.Header().Set("Idempotent-Replay", "true")
			// The destination has moved on to a later, stable revision.
			_ = json.NewEncoder(w).Encode(l1.Computer{ComputerID: cloneID, IntentRevision: 7, AppliedRevision: 7,
				StorageID: "storage-clone", StorageGeneration: 1, ReconfigurationPhase: l1.ComputerReconfigurationStable})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/computers/"+cloneID:
			computer := l1.Computer{ComputerID: cloneID, IntentRevision: 7, AppliedRevision: 7, StorageID: "storage-clone",
				StorageGeneration: 1, ReconfigurationPhase: l1.ComputerReconfigurationStable}
			if r.URL.Query().Get("clone_operation_revision") == "1" {
				computer.CloneOperation = &l1.ComputerCloneOperation{OperationRevision: 1, SourceComputerID: computerID,
					BackupID: "backup-1", Status: "superseded"}
			}
			_ = json.NewEncoder(w).Encode(computer)
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/storage-provenance"):
			_ = json.NewEncoder(w).Encode(l1.ComputerStorageProvenance{})
		default:
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(contract.APIError{Code: contract.ErrorNotFound, Message: r.Method + " " + r.URL.Path})
		}
	}
	l1Address := startStubLedger(t, stub)
	cas := []string{"--intent-revision", "2", "--storage-id", "storage-1", "--storage-generation", "1"}
	wait := []string{"--wait", "30s", "--poll-interval", "10ms"}
	for _, test := range []struct {
		name string
		args []string
	}{
		{"backup", append([]string{"services", "backup", "create", computerID, "--idempotency-key", "k", "--allow-power-off"}, cas...)},
		{"export", append([]string{"services", "custody", "export", computerID, "backup-1", "--path", "/mnt/export",
			"--idempotency-key", "k"}, cas...)},
		{"clone", append([]string{"services", "clone", computerID, "backup-1", "--name", "clone", "--disk-bytes", "1024",
			"--idempotency-key", "k"}, cas...)},
	} {
		args := append(append([]string{"--json", "--l1=" + l1Address}, test.args...), wait...)
		started := time.Now()
		code, output := runWefty(t, binary, 60*time.Second, args...)
		if code != exitConflict || !strings.Contains(output, "superseded") || time.Since(started) > 20*time.Second {
			t.Fatalf("superseded `%s --wait` exited %d after %s, want %d promptly:\n%s", test.name, code,
				time.Since(started), exitConflict, output)
		}
	}
}
