package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/l3"
)

const (
	storageCLITopDigest      = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	storageCLIPlatformDigest = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

type storageCLIHarness struct {
	ctx      context.Context
	store    *l1.Store
	clients  *apiClients
	node     l1.Node
	computer l1.Computer
	claim    *l1.Claim
}

func TestComputerStorageCLIOverRealL1AndHelperSeams(t *testing.T) {
	assertComputerStorageCLIOverRealL1AndHelperSeams(t)
}

func assertComputerStorageCLIOverRealL1AndHelperSeams(t *testing.T) {
	h := newStorageCLIHarness(t)

	setCap := runStorageCLI(t, h.ctx, h.clients, true, "services", "backup", "set-cap", h.computer.ComputerID,
		"--cap", "2", "--expect-current")
	var capOutput storageMutationOutput
	if err := json.Unmarshal(setCap, &capOutput); err != nil || capOutput.Computer == nil ||
		!capOutput.MutationApplied || capOutput.Computer.BackupCap != 2 {
		t.Fatalf("set-cap output = %s err=%v", setCap, err)
	}
	h.computer = *capOutput.Computer

	created := runStorageCLI(t, h.ctx, h.clients, true, "services", "backup", "create", h.computer.ComputerID,
		"--expect-current", "--idempotency-key", "storage-cli-backup", "--allow-power-off")
	var createOutput storageMutationOutput
	if err := json.Unmarshal(created, &createOutput); err != nil || createOutput.Computer == nil ||
		!createOutput.MutationApplied || createOutput.IdempotentReplay {
		t.Fatalf("Backup create output = %s err=%v", created, err)
	}
	h.completeBackupHelper(t)

	listed := runStorageCLI(t, h.ctx, h.clients, true, "services", "backup", "list", h.computer.ComputerID)
	var backups l1.BackupList
	if err := json.Unmarshal(listed, &backups); err != nil || len(backups.Backups) != 1 ||
		backups.Backups[0].Provenance.Kind != "backup" || backups.Backups[0].Encryption != l1.BackupEncryptionNone ||
		len(backups.Backups[0].Copies) != 1 || backups.Backups[0].Copies[0].Phase != "published" {
		t.Fatalf("Backup list = %s err=%v", listed, err)
	}
	backup := backups.Backups[0]

	replay := runStorageCLI(t, h.ctx, h.clients, true, "services", "backup", "create", h.computer.ComputerID,
		"--intent-revision", "2", "--storage-id", h.computer.StorageID, "--storage-generation", "1",
		"--idempotency-key", "storage-cli-backup", "--allow-power-off")
	if err := json.Unmarshal(replay, &createOutput); err != nil || !createOutput.IdempotentReplay || createOutput.MutationApplied {
		t.Fatalf("Backup replay output = %s err=%v", replay, err)
	}

	staleErr := execute(h.ctx, h.clients, true, []string{"services", "backup", "create", h.computer.ComputerID,
		"--intent-revision", "1", "--storage-id", h.computer.StorageID, "--storage-generation", "1",
		"--idempotency-key", "storage-cli-stale", "--allow-power-off"}, &bytes.Buffer{}, &bytes.Buffer{})
	assertCLIAPIError(t, staleErr, contract.ErrorStaleIntentRevision)

	h.computer = mustStorageComputer(t, h)
	h.startPruneHelper(t)
	pruned := runStorageCLI(t, h.ctx, h.clients, true, "services", "backup", "prune", h.computer.ComputerID,
		backup.BackupID, "--expect-current", "--idempotency-key", "storage-cli-prune", "--wait", "2s", "--poll-interval", "1ms")
	var pruneOutput storageMutationOutput
	if err := json.Unmarshal(pruned, &pruneOutput); err != nil || pruneOutput.Backup == nil ||
		pruneOutput.Backup.Status != "pruned" || pruneOutput.Observation == nil || pruneOutput.Observation.Status != "observed" {
		t.Fatalf("Backup prune wait output = %s err=%v", pruned, err)
	}

	var human bytes.Buffer
	if err := execute(h.ctx, h.clients, false, []string{"services", "backup", "list", h.computer.ComputerID}, &human, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	for _, fact := range []string{"SOURCE STORAGE", "PROVENANCE", "COPY PHASE", "pruned", h.computer.StorageID} {
		if !strings.Contains(human.String(), fact) {
			t.Fatalf("human Backup projection missing %q:\n%s", fact, human.String())
		}
	}
}

func TestComputerStorageCLIAdversarialRows(t *testing.T) {
	assertComputerStorageCLIAdversarialRows(t)
}

func assertComputerStorageCLIAdversarialRows(t *testing.T) {
	t.Run("foreign identifiers stay server-typed", func(t *testing.T) {
		h := newStorageCLIHarness(t)
		for _, args := range [][]string{
			{"services", "backup", "list", "foreign-computer"},
			{"services", "custody", "attest", "foreign-export", "--idempotency-key", "foreign-export"},
		} {
			err := execute(h.ctx, h.clients, true, args, &bytes.Buffer{}, &bytes.Buffer{})
			assertCLIAPIError(t, err, contract.ErrorNotFound)
		}
	})

	t.Run("default zero and cap reached", func(t *testing.T) {
		h := newStorageCLIHarness(t)
		err := execute(h.ctx, h.clients, true, []string{"services", "backup", "create", h.computer.ComputerID,
			"--expect-current", "--idempotency-key", "cap-zero", "--allow-power-off"}, &bytes.Buffer{}, &bytes.Buffer{})
		assertCLIAPIError(t, err, contract.ErrorConflict)

		runStorageCLI(t, h.ctx, h.clients, true, "services", "backup", "set-cap", h.computer.ComputerID,
			"--cap", "1", "--expect-current")
		err = execute(h.ctx, h.clients, true, []string{"services", "backup", "create", h.computer.ComputerID,
			"--expect-current", "--idempotency-key", "power-off-not-allowed"}, &bytes.Buffer{}, &bytes.Buffer{})
		assertCLIAPIError(t, err, contract.ErrorConflict)
		runStorageCLI(t, h.ctx, h.clients, true, "services", "backup", "create", h.computer.ComputerID,
			"--expect-current", "--idempotency-key", "cap-one", "--allow-power-off")
		h.completeBackupHelper(t)
		err = execute(h.ctx, h.clients, true, []string{"services", "backup", "create", h.computer.ComputerID,
			"--expect-current", "--idempotency-key", "cap-reached", "--allow-power-off"}, &bytes.Buffer{}, &bytes.Buffer{})
		assertCLIAPIError(t, err, contract.ErrorConflict)
	})

	t.Run("ENOSPC remains a typed server-returned operation failure", func(t *testing.T) {
		h := newStorageCLIHarness(t)
		runStorageCLI(t, h.ctx, h.clients, true, "services", "backup", "set-cap", h.computer.ComputerID,
			"--cap", "1", "--expect-current")
		runStorageCLI(t, h.ctx, h.clients, true, "services", "backup", "create", h.computer.ComputerID,
			"--expect-current", "--idempotency-key", "enospc-source", "--allow-power-off")
		h.completeBackupHelperFailure(t, string(l1.ComputerBackupFailureInsufficientDisk))
		listed := runStorageCLI(t, h.ctx, h.clients, true, "services", "backup", "list", h.computer.ComputerID)
		var inventory computerBackupInventory
		if err := json.Unmarshal(listed, &inventory); err != nil || len(inventory.Backups) != 0 ||
			inventory.LastOperation == nil || inventory.LastOperation.Status != "failed" ||
			inventory.LastOperation.FailureCode != l1.ComputerBackupFailureInsufficientDisk {
			t.Fatalf("ENOSPC CLI projection = %s err=%v", listed, err)
		}
	})

	t.Run("running restore and copy-creating identity", func(t *testing.T) {
		h := newStorageCLIHarness(t)
		runStorageCLI(t, h.ctx, h.clients, true, "services", "backup", "set-cap", h.computer.ComputerID,
			"--cap", "2", "--expect-current")
		runStorageCLI(t, h.ctx, h.clients, true, "services", "backup", "create", h.computer.ComputerID,
			"--expect-current", "--idempotency-key", "copy-source", "--allow-power-off")
		h.completeBackupHelper(t)
		backups, err := h.store.ListComputerBackups(h.ctx, h.computer.ComputerID)
		if err != nil || len(backups.Backups) != 1 {
			t.Fatalf("Backup fixture = %#v err=%v", backups, err)
		}
		h.computer = mustStorageComputer(t, h)
		err = execute(h.ctx, h.clients, true, []string{"services", "restore", h.computer.ComputerID,
			backups.Backups[0].BackupID, "--retire-old", "--expect-current", "--idempotency-key", "running-restore"},
			&bytes.Buffer{}, &bytes.Buffer{})
		assertCLIAPIError(t, err, contract.ErrorConflict)

		cloneJSON := runStorageCLI(t, h.ctx, h.clients, true, "services", "clone", h.computer.ComputerID,
			backups.Backups[0].BackupID, "--name", "storage-cli-clone", "--disk-bytes", fmt.Sprint(2<<30),
			"--expect-current", "--idempotency-key", "clone-one")
		var clone storageMutationOutput
		cloneProvenance := false
		if err := json.Unmarshal(cloneJSON, &clone); err != nil || clone.Computer == nil ||
			len(clone.Computer.Grants) != 0 || clone.Computer.ComputerID == h.computer.ComputerID ||
			clone.Computer.StorageID == h.computer.StorageID || clone.Computer.ReconfigurationPhase != l1.ComputerReconfigurationCloning ||
			clone.StorageProvenance == nil || len(clone.StorageProvenance.CustodyForks) != 2 ||
			len(clone.StorageProvenance.Provenance) != 2 {
			t.Fatalf("clone identity output = %s err=%v", cloneJSON, err)
		}
		for _, provenance := range clone.StorageProvenance.Provenance {
			cloneProvenance = cloneProvenance || provenance.Kind == "clone" &&
				provenance.DestinationComputerID == clone.Computer.ComputerID &&
				provenance.DestinationStorageID == clone.Computer.StorageID
		}
		if !cloneProvenance {
			t.Fatalf("clone output omitted its custody fork: %s", cloneJSON)
		}
	})

	t.Run("tampered import manifest", func(t *testing.T) {
		h := newStorageCLIHarness(t)
		manifest := storageCLICustodyManifest(h)
		digest, err := contract.DigestComputerCustodyManifest(manifest)
		if err != nil {
			t.Fatal(err)
		}
		manifest.AllocatedSize++
		path := filepath.Join(t.TempDir(), "custody.json")
		payload, err := json.Marshal(manifest)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, payload, 0o600); err != nil {
			t.Fatal(err)
		}
		err = execute(h.ctx, h.clients, true, []string{"services", "custody", "import", manifest.ExportID,
			"--name", "tampered-import", "--disk-bytes", fmt.Sprint(2 << 30), "--node", h.node.NodeID,
			"--path", t.TempDir(), "--manifest", path, "--manifest-digest", digest,
			"--idempotency-key", "tampered-import"}, &bytes.Buffer{}, &bytes.Buffer{})
		assertCLIAPIError(t, err, contract.ErrorStorageReferenceConflict)
	})

	t.Run("export taint and immutable attestation replay", func(t *testing.T) {
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
		h.computer = mustStorageComputer(t, h)
		var exportBuffer bytes.Buffer
		exportErr := execute(h.ctx, h.clients, true, []string{"services", "custody", "export",
			h.computer.ComputerID, backups.Backups[0].BackupID, "--path", filepath.Join(t.TempDir(), "external"),
			"--expect-current", "--idempotency-key", "export-before-bytes", "--wait", "2ms",
			"--poll-interval", "1ms"}, &exportBuffer, &bytes.Buffer{})
		var observationErr *storageObservationError
		if !errors.As(exportErr, &observationErr) {
			t.Fatalf("Custody export wait error = %T %v", exportErr, exportErr)
		}
		exportJSON := exportBuffer.Bytes()
		var exportOutput storageMutationOutput
		if err := json.Unmarshal(exportJSON, &exportOutput); err != nil || exportOutput.CustodyExport == nil ||
			!exportOutput.MutationApplied || exportOutput.CustodyExport.Status != "planned" ||
			exportOutput.StorageProvenance == nil || !exportOutput.StorageProvenance.CustodyTainted ||
			len(exportOutput.StorageProvenance.CustodyExports) != 1 || exportOutput.Observation == nil ||
			exportOutput.Observation.Status != "failed" {
			t.Fatalf("Custody export output = %s err=%v", exportJSON, err)
		}
		attestArgs := []string{"services", "custody", "attest", exportOutput.CustodyExport.ExportID,
			"--idempotency-key", "operator-attested-deleted"}
		first := runStorageCLI(t, h.ctx, h.clients, true, attestArgs...)
		var attested storageMutationOutput
		if err := json.Unmarshal(first, &attested); err != nil || attested.CustodyExport == nil ||
			!attested.MutationApplied || !attested.CustodyExport.OperatorAttestedDeleted {
			t.Fatalf("Custody attestation output = %s err=%v", first, err)
		}
		second := runStorageCLI(t, h.ctx, h.clients, true, attestArgs...)
		if err := json.Unmarshal(second, &attested); err != nil || attested.MutationApplied || !attested.IdempotentReplay {
			t.Fatalf("Custody attestation replay = %s err=%v", second, err)
		}
		exports, err := h.store.ListComputerCustodyExports(h.ctx, h.computer.ComputerID)
		if err != nil || len(exports) != 1 || exports[0].Status != "planned" {
			t.Fatalf("attestation changed export taint = %#v err=%v", exports, err)
		}
	})
}

func TestComputerStorageCLIWaitPathsOverHelperSeam(t *testing.T) {
	assertComputerStorageCLIWaitPathsOverHelperSeam(t)
}

func assertComputerStorageCLIWaitPathsOverHelperSeam(t *testing.T) {
	t.Run("restore", func(t *testing.T) {
		h := newStorageCLIHarness(t)
		runStorageCLI(t, h.ctx, h.clients, true, "services", "backup", "set-cap", h.computer.ComputerID,
			"--cap", "2", "--expect-current")
		runStorageCLI(t, h.ctx, h.clients, true, "services", "backup", "create", h.computer.ComputerID,
			"--expect-current", "--idempotency-key", "restore-wait-source", "--allow-power-off")
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
		done := h.startStorageCopyHelper("restore", stopped.ComputerID)
		payload := runStorageCLI(t, h.ctx, h.clients, true, "services", "restore", stopped.ComputerID,
			backups.Backups[0].BackupID, "--keep-old-as-backup", "--expect-current", "--idempotency-key", "restore-wait",
			"--wait", "2s", "--poll-interval", "1ms")
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		var output storageMutationOutput
		if err := json.Unmarshal(payload, &output); err != nil || output.Computer == nil || output.Observation == nil ||
			output.Observation.Status != "observed" || output.StorageProvenance == nil {
			t.Fatalf("restore wait = %s err=%v", payload, err)
		}
	})

	t.Run("clone", func(t *testing.T) {
		h := newStorageCLIHarness(t)
		runStorageCLI(t, h.ctx, h.clients, true, "services", "backup", "set-cap", h.computer.ComputerID,
			"--cap", "1", "--expect-current")
		runStorageCLI(t, h.ctx, h.clients, true, "services", "backup", "create", h.computer.ComputerID,
			"--expect-current", "--idempotency-key", "clone-wait-source", "--allow-power-off")
		h.completeBackupHelper(t)
		backups, err := h.store.ListComputerBackups(h.ctx, h.computer.ComputerID)
		if err != nil || len(backups.Backups) != 1 {
			t.Fatalf("clone source = %#v err=%v", backups, err)
		}
		done := h.startStorageCopyHelper("clone", "")
		payload := runStorageCLI(t, h.ctx, h.clients, true, "services", "clone", h.computer.ComputerID,
			backups.Backups[0].BackupID, "--name", "waited-clone", "--disk-bytes", fmt.Sprint(2<<30),
			"--expect-current", "--idempotency-key", "clone-wait", "--wait", "2s", "--poll-interval", "1ms")
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		var output storageMutationOutput
		if err := json.Unmarshal(payload, &output); err != nil || output.Computer == nil || len(output.Computer.Grants) != 0 ||
			output.Observation == nil || output.Observation.Status != "observed" || output.StorageProvenance == nil {
			t.Fatalf("clone wait = %s err=%v", payload, err)
		}
	})

	t.Run("import", func(t *testing.T) {
		h := newStorageCLIHarness(t)
		manifest := storageCLICustodyManifest(h)
		encodedSpec, err := json.Marshal(manifest.JobSpec)
		if err != nil {
			t.Fatal(err)
		}
		specHash := sha256.Sum256(encodedSpec)
		manifest.JobSpecHash = hex.EncodeToString(specHash[:])
		manifestDigest, err := contract.DigestComputerCustodyManifest(manifest)
		if err != nil {
			t.Fatal(err)
		}
		manifestBytes, err := json.Marshal(manifest)
		if err != nil {
			t.Fatal(err)
		}
		manifestPath := filepath.Join(t.TempDir(), "custody.json")
		if err := os.WriteFile(manifestPath, manifestBytes, 0o600); err != nil {
			t.Fatal(err)
		}
		done := h.startStorageCopyHelper("import", "")
		payload := runStorageCLI(t, h.ctx, h.clients, true, "services", "custody", "import", manifest.ExportID,
			"--name", "waited-import", "--disk-bytes", fmt.Sprint(2<<30), "--node", h.node.NodeID,
			"--path", t.TempDir(), "--manifest", manifestPath, "--manifest-digest", manifestDigest,
			"--idempotency-key", "import-wait", "--wait", "2s", "--poll-interval", "1ms")
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		var output storageMutationOutput
		if err := json.Unmarshal(payload, &output); err != nil || output.CustodyImport == nil || output.Computer == nil ||
			output.CustodyImport.OperationRevision < 1 || output.Observation == nil || output.Observation.Status != "observed" ||
			output.StorageProvenance == nil || !output.StorageProvenance.CustodyTainted {
			t.Fatalf("import wait = %s err=%v", payload, err)
		}
	})
}

func TestComputerStorageCLIUsageIsTypedJSONAndWaitReportsAcceptedMutation(t *testing.T) {
	for _, args := range [][]string{
		{"services", "backup", "create", "computer-one"},
		{"services", "restore", "computer-one", "backup-one", "--keep-old-as-backup", "--retire-old"},
		{"services", "clone", "computer-one", "backup-one", "--name", "clone"},
		{"services", "custody", "export", "computer-one", "backup-one", "--path", "relative"},
		{"services", "custody", "import", "export-one"},
		{"services", "custody", "attest", "export-one"},
		{"services", "backup", "create", "computer-one", "--poll-interval", "1ms"},
	} {
		err := execute(context.Background(), nil, true, args, &bytes.Buffer{}, &bytes.Buffer{})
		var usage usageError
		if !errors.As(err, &usage) || commandExitCodeForArgs(err, args) != exitUsage {
			t.Fatalf("args %q error = %T %v exit=%d", args, err, err, commandExitCodeForArgs(err, args))
		}
		var encoded bytes.Buffer
		writeCommandError(&encoded, err, true)
		var response contract.ErrorResponse
		if json.Unmarshal(encoded.Bytes(), &response) != nil || response.Error.Code != contract.ErrorInvalidRequest {
			t.Fatalf("typed JSON usage = %s", encoded.String())
		}
	}
	if got := commandExitCodeForArgs(usageError("bad"), []string{"services", "status"}); got != exitFailure {
		t.Fatalf("pre-existing services status exit = %d, want %d", got, exitFailure)
	}

	output := storageMutationOutput{MutationApplied: true, Observation: &storageWaitObservation{Status: "failed", Error: "context deadline exceeded"}}
	var encoded bytes.Buffer
	err := writeStorageMutationThenError(&encoded, output, true, &storageObservationError{cause: context.DeadlineExceeded})
	if err == nil || !bytes.Contains(encoded.Bytes(), []byte(`"mutation_applied": true`)) ||
		!bytes.Contains(encoded.Bytes(), []byte(`"status": "failed"`)) {
		t.Fatalf("accepted mutation observation failure = %s err=%v", encoded.String(), err)
	}
}

type storageRoundTripFunc func(*http.Request) (*http.Response, error)

func (function storageRoundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

func TestStorageMutationRenderingKeepsEveryResourceAndAppliedFactsOnReadFailure(t *testing.T) {
	completed := time.Date(2026, 8, 28, 20, 1, 0, 0, time.UTC)
	output := storageMutationOutput{
		MutationApplied: true,
		Computer: &l1.Computer{ComputerID: "computer-clone", Name: "clone", StorageID: "storage-clone",
			StorageGeneration: 1, Grants: []l1.ComputerGrant{}, ReconfigurationPhase: l1.ComputerReconfigurationStable},
		Backups: &l1.BackupList{Backups: []l1.Backup{{BackupID: "backup-old", ComputerID: "computer-clone",
			SourceStorageID: "storage-clone", SourceGeneration: 1, Copies: []l1.BackupCopy{}}},
			LastOperation: &l1.ComputerBackupOperationOutcome{BackupID: "backup-old", OperationRevision: 3,
				Status: "failed", FailureCode: l1.ComputerBackupFailureInsufficientDisk, CompletedAt: &completed}},
		CustodyImport: &l1.ComputerCustodyImport{ImportID: "import-one", ExportID: "export-one", OperationRevision: 4,
			DestinationComputerID: "computer-clone", DestinationStorageID: "storage-clone", Status: "reserved"},
		CustodyExport: &l1.ComputerCustodyExport{ExportID: "export-one", ComputerID: "computer-clone", OperatorAttestedDeleted: true},
	}
	var human bytes.Buffer
	if err := writeStorageMutation(&human, output, false); err != nil {
		t.Fatal(err)
	}
	for _, fact := range []string{"computer-clone", "GRANTS", "backup-old", "LAST OPERATION BACKUP", "FAILURE CODE",
		"import-one", "OPERATION REVISION", "never upgrades removed_reduced"} {
		if !strings.Contains(human.String(), fact) {
			t.Fatalf("human storage mutation omitted %q:\n%s", fact, human.String())
		}
	}

	clients := &apiClients{l1: &apiClient{name: "L1", client: &http.Client{Transport: storageRoundTripFunc(
		func(*http.Request) (*http.Response, error) { return nil, errors.New("provenance unavailable") },
	)}}}
	applied := storageMutationOutput{MutationApplied: true, Computer: output.Computer}
	err := attachStorageProvenance(t.Context(), clients, output.Computer.ComputerID, &applied, nil)
	var observationErr *storageObservationError
	if !errors.As(err, &observationErr) || applied.Observation == nil || applied.Observation.Status != "failed" {
		t.Fatalf("provenance failure = %#v err=%v", applied, err)
	}
	var structured bytes.Buffer
	if err := writeStorageMutationThenError(&structured, applied, true, err); !errors.As(err, &observationErr) ||
		!bytes.Contains(structured.Bytes(), []byte(`"mutation_applied": true`)) ||
		!bytes.Contains(structured.Bytes(), []byte(`"computer_id": "computer-clone"`)) {
		t.Fatalf("applied mutation facts lost on observation failure: %s err=%v", structured.String(), err)
	}
}

func newStorageCLIHarness(t *testing.T) *storageCLIHarness {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	network := plain.NewNetwork()
	controlFabric := network.NewFabric(fabric.Identity{NodeID: "control-plane"})
	operatorFabric := network.NewFabric(fabric.Identity{NodeID: "operator", Tags: []string{l1.DefaultClientPrincipalTag}})
	fixed := time.Date(2026, 8, 28, 20, 0, 0, 0, time.UTC)
	store, err := l1.OpenStore(filepath.Join(t.TempDir(), "l1.sqlite"), l1.StoreOptions{Clock: l1.ClockFunc(func() time.Time { return fixed })})
	if err != nil {
		t.Fatal(err)
	}
	server, err := l1.NewServer(controlFabric, store, l1.ServerConfig{NodePolicies: map[string]l1.NodePolicy{
		"storage-node": {Tags: []string{contract.StableNodeTagPrefix + "storage-node"}, MaxOneshotSlots: 1, MaxServiceSlots: 2},
	}})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := controlFabric.Listen("tcp", l3.DefaultL1Address)
	if err != nil {
		t.Fatal(err)
	}
	served := serveTestServer(ctx, func() error { return server.Serve(ctx, listener) })
	clients := mustTestAPIClients(t, operatorFabric)
	node, err := store.RegisterNode(ctx, fabric.Identity{NodeID: "fabric-storage-node"}, contract.NodeRegistration{
		NodeID: "storage-node", BootSessionID: "boot-storage-node", RootInstanceID: "root-storage-node",
		OS: "linux", Architecture: "amd64", AgentVersion: "storage-cli-test",
		Capabilities:       map[string]bool{"kind:oci": true, "cgroup_v2": true, "computer": true},
		CapabilityRevision: 1, CapabilityObservedAt: fixed, MissingCapabilities: []string{},
	}, l1.NodePolicy{Tags: []string{contract.StableNodeTagPrefix + "storage-node"}, MaxOneshotSlots: 1, MaxServiceSlots: 2}, true)
	if err != nil {
		t.Fatal(err)
	}
	computer, _, err := store.CreateComputer(ctx, l1.CreateComputerRequest{
		Name: "storage-cli-source", Spec: storageCLIComputerSpec("computer:storage-cli"), Actor: "operator",
	})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimJob(ctx, "fabric-storage-node", node.NodeID, node.BootSessionID, contract.JobClassService)
	if err != nil || claim == nil {
		t.Fatalf("claim Computer = %#v err=%v", claim, err)
	}
	observation := l1.ImageObservationRequest{FencingToken: claim.Lease.FencingToken,
		SubmittedReference: "ghcr.io/example/computer:latest", TopLevelDigest: storageCLITopDigest,
		TopLevelMediaType: "application/vnd.oci.image.index.v1+json", PlatformManifestDigest: storageCLIPlatformDigest,
		Platform: l1.OCIPlatform{OS: "linux", Architecture: "amd64"}, RuntimeHandler: "io.containerd.runc.v2", Snapshotter: "overlayfs"}
	if _, err := store.ObserveAttemptImage(ctx, "fabric-storage-node", claim.Job.JobID, claim.Lease.AttemptID, observation); err != nil {
		t.Fatal(err)
	}
	if _, err := store.StartAttempt(ctx, "fabric-storage-node", claim.Job.JobID, claim.Lease.AttemptID,
		l1.StartedRequest{FencingToken: claim.Lease.FencingToken}); err != nil {
		t.Fatal(err)
	}
	computer, err = store.GetComputer(ctx, computer.ComputerID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		clients.close()
		cancel()
		if err := <-served; err != nil {
			t.Errorf("serve L1: %v", err)
		}
		if err := store.Close(); err != nil {
			t.Errorf("close L1: %v", err)
		}
	})
	return &storageCLIHarness{ctx: ctx, store: store, clients: clients, node: node, computer: computer, claim: claim}
}

func (h *storageCLIHarness) completeBackupHelper(t *testing.T) {
	t.Helper()
	if _, err := h.store.CompleteAttempt(h.ctx, "fabric-storage-node", h.claim.Job.JobID, h.claim.Lease.AttemptID,
		l1.CompletionRequest{FencingToken: h.claim.Lease.FencingToken, IdempotencyKey: "storage-cli-quiesced",
			Result: l1.ProcessResult{OutputError: "quiesced for Backup"}, RuntimeQuiescenceEvidence: l1.RuntimeQuiescenceAttempt}); err != nil {
		t.Fatal(err)
	}
	directives, err := h.store.ListNodeComputerBackupDirectives(h.ctx, "fabric-storage-node", h.node.NodeID, h.node.BootSessionID)
	if err != nil || len(directives) != 1 {
		t.Fatalf("Backup helper directives = %#v err=%v", directives, err)
	}
	directive := directives[0]
	receipt := l1.ComputerBackupCopyReceipt{Kind: "computer_backup_copy_verified", ReceiptID: "receipt-" + directive.CopyID,
		BackupID: directive.BackupID, CopyID: directive.CopyID, ComputerID: directive.ComputerID,
		StorageID: directive.StorageID, StorageGeneration: directive.StorageGeneration, NodeID: directive.BoundNodeID,
		RootInstanceID: directive.RootInstanceID, JobID: directive.JobID, OperationRevision: directive.OperationRevision,
		CleanupFence: directive.CleanupFence, HelperGeneration: 1, AllocatedSize: directive.AllocatedSize,
		ContentDigest: storageCLITopDigest, Encryption: l1.BackupEncryptionNone}
	if _, _, err := h.store.AcknowledgeComputerBackup(h.ctx, "fabric-storage-node", directive.ComputerID,
		l1.ComputerBackupAcknowledgementRequest{NodeID: h.node.NodeID, BootSessionID: h.node.BootSessionID,
			IdempotencyKey: receipt.ReceiptID, Receipt: receipt}); err != nil {
		t.Fatal(err)
	}
}

func (h *storageCLIHarness) completeBackupHelperFailure(t *testing.T, failureCode string) {
	t.Helper()
	if _, err := h.store.CompleteAttempt(h.ctx, "fabric-storage-node", h.claim.Job.JobID, h.claim.Lease.AttemptID,
		l1.CompletionRequest{FencingToken: h.claim.Lease.FencingToken, IdempotencyKey: "storage-cli-quiesced-failure",
			Result: l1.ProcessResult{OutputError: "quiesced for failed Backup"}, RuntimeQuiescenceEvidence: l1.RuntimeQuiescenceAttempt}); err != nil {
		t.Fatal(err)
	}
	directives, err := h.store.ListNodeComputerBackupDirectives(h.ctx, "fabric-storage-node", h.node.NodeID, h.node.BootSessionID)
	if err != nil || len(directives) != 1 {
		t.Fatalf("failed Backup helper directives = %#v err=%v", directives, err)
	}
	directive := directives[0]
	receipt := l1.ComputerBackupCopyReceipt{Kind: "computer_backup_copy_failed_absent", ReceiptID: "failed-" + directive.CopyID,
		BackupID: directive.BackupID, CopyID: directive.CopyID, ComputerID: directive.ComputerID,
		StorageID: directive.StorageID, StorageGeneration: directive.StorageGeneration, NodeID: directive.BoundNodeID,
		RootInstanceID: directive.RootInstanceID, JobID: directive.JobID, OperationRevision: directive.OperationRevision,
		CleanupFence: directive.CleanupFence, HelperGeneration: 1, AllocatedSize: directive.AllocatedSize,
		Encryption: l1.BackupEncryptionNone, FailureCode: failureCode, CopyAbsent: true}
	if _, _, err := h.store.AcknowledgeComputerBackup(h.ctx, "fabric-storage-node", directive.ComputerID,
		l1.ComputerBackupAcknowledgementRequest{NodeID: h.node.NodeID, BootSessionID: h.node.BootSessionID,
			IdempotencyKey: receipt.ReceiptID, Receipt: receipt}); err != nil {
		t.Fatal(err)
	}
}

func (h *storageCLIHarness) startPruneHelper(t *testing.T) {
	t.Helper()
	go func() {
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			directives, err := h.store.ListNodeComputerBackupPruneDirectives(h.ctx, "fabric-storage-node", h.node.NodeID, h.node.BootSessionID)
			if err == nil && len(directives) == 1 {
				directive := directives[0]
				receipt := l1.ComputerBackupCopyRemovalReceipt{Kind: "computer_backup_copy_removed",
					ReceiptID: "removed-" + directive.CopyID, BackupID: directive.BackupID, CopyID: directive.CopyID,
					ComputerID: directive.ComputerID, StorageID: directive.StorageID,
					StorageGeneration: directive.StorageGeneration, NodeID: directive.BoundNodeID,
					RootInstanceID: directive.RootInstanceID, OperationRevision: directive.OperationRevision,
					CleanupFence: directive.CleanupFence, HelperGeneration: 1, Absent: true}
				_, _ = h.store.AcknowledgeComputerBackupPrune(h.ctx, "fabric-storage-node", directive.ComputerID,
					l1.ComputerBackupPruneAcknowledgementRequest{NodeID: h.node.NodeID, BootSessionID: h.node.BootSessionID,
						IdempotencyKey: receipt.ReceiptID, Receipt: receipt})
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()
}

func (h *storageCLIHarness) startStorageCopyHelper(operation, destinationComputerID string) <-chan error {
	done := make(chan error, 1)
	go func() {
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if operation == "restore" {
				computer, err := h.store.GetComputer(h.ctx, destinationComputerID)
				if err == nil && computer.ReconfigurationPhase == l1.ComputerReconfigurationRestoring {
					if err := h.store.RecordComputerRestoreAuthorityRevoked(h.ctx, destinationComputerID); err != nil {
						done <- err
						return
					}
				}
			}
			directives, err := h.store.ListNodeComputerStorageCopyDirectives(h.ctx, "fabric-storage-node", h.node.NodeID, h.node.BootSessionID)
			if err != nil {
				done <- err
				return
			}
			for _, directive := range directives {
				if directive.Operation != operation {
					continue
				}
				destinationDigest := directive.SourceDigest
				if operation == "clone" || operation == "import" {
					destinationDigest = storageCLIPlatformDigest
				}
				receipt := l1.ComputerStorageCopyReceipt{Kind: "computer_storage_copy_verified",
					ReceiptID: "receipt-" + directive.DestinationComputerID, Operation: directive.Operation,
					BackupID: directive.BackupID, CopyID: directive.CopyID, ExportID: directive.ExportID,
					ExternalPath: directive.ExternalPath, ManifestDigest: directive.ManifestDigest,
					SourceComputerID: directive.SourceComputerID, SourceStorageID: directive.SourceStorageID,
					SourceGeneration: directive.SourceGeneration, DestinationComputerID: directive.DestinationComputerID,
					DestinationStorageID: directive.DestinationStorageID, DestinationGeneration: directive.DestinationGeneration,
					NodeID: directive.BoundNodeID, RootInstanceID: directive.RootInstanceID, JobID: directive.JobID,
					OperationRevision: directive.OperationRevision, CleanupFence: directive.CleanupFence, HelperGeneration: 9,
					SourceSize: directive.SourceSize, DestinationSize: directive.DestinationSize,
					SourceDigest: directive.SourceDigest, DestinationDigest: destinationDigest,
					OSIdentityRekeyed: operation == "clone" || operation == "import",
					MachineIDBeforeDigest: func() string {
						if operation == "restore" {
							return ""
						}
						return "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
					}(),
					MachineIDAfterDigest: func() string {
						if operation == "restore" {
							return ""
						}
						return "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
					}(),
					SourceUnchanged: true, DestinationPrepared: true,
					FilesystemExpanded: (operation == "clone" || operation == "import") && directive.DestinationSize > directive.SourceSize}
				request := l1.ComputerStorageCopyAcknowledgementRequest{NodeID: h.node.NodeID, BootSessionID: h.node.BootSessionID,
					IdempotencyKey: receipt.ReceiptID, Receipt: receipt}
				if directive.KeepOldBackup {
					old := l1.ComputerBackupCopyReceipt{Kind: "computer_backup_copy_verified", ReceiptID: "old-" + directive.OldCopyID,
						BackupID: directive.OldBackupID, CopyID: directive.OldCopyID, ComputerID: directive.DestinationComputerID,
						StorageID: directive.DestinationStorageID, StorageGeneration: directive.OldGeneration,
						NodeID: directive.BoundNodeID, RootInstanceID: directive.RootInstanceID, JobID: directive.JobID,
						OperationRevision: directive.OperationRevision, CleanupFence: directive.CleanupFence, HelperGeneration: 9,
						AllocatedSize: directive.DestinationSize,
						ContentDigest: "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
						Encryption:    l1.BackupEncryptionNone}
					request.OldBackupReceipt = &old
				}
				_, err = h.store.AcknowledgeComputerStorageCopy(h.ctx, "fabric-storage-node", directive.DestinationComputerID, request)
				if err == nil && operation == "restore" {
					_, err = h.store.AcknowledgeComputerRestoreRetirement(h.ctx, "fabric-storage-node", directive.DestinationComputerID,
						l1.RemovalAcknowledgementRequest{NodeID: h.node.NodeID, BootSessionID: h.node.BootSessionID,
							RemovalGeneration: uint64(directive.OperationRevision), CleanupFence: directive.CleanupFence,
							RootInstanceID: directive.RootInstanceID, IdempotencyKey: "retired-" + directive.DestinationComputerID})
				}
				done <- err
				return
			}
			time.Sleep(time.Millisecond)
		}
		done <- errors.New("timed out waiting for Storage copy directive")
	}()
	return done
}

func mustStorageComputer(t *testing.T, h *storageCLIHarness) l1.Computer {
	t.Helper()
	computer, err := h.store.GetComputer(h.ctx, h.computer.ComputerID)
	if err != nil {
		t.Fatal(err)
	}
	return computer
}

func runStorageCLI(t *testing.T, ctx context.Context, clients *apiClients, jsonOutput bool, args ...string) []byte {
	t.Helper()
	var stdout, stderr bytes.Buffer
	if err := execute(ctx, clients, jsonOutput, args, &stdout, &stderr); err != nil {
		t.Fatalf("wefty %s: %v stderr=%s stdout=%s", strings.Join(args, " "), err, stderr.String(), stdout.String())
	}
	return stdout.Bytes()
}

func storageCLIComputerSpec(dispatchKey string) contract.JobSpec {
	memoryBytes := int64(64 << 20)
	diskBytes := int64(1 << 30)
	digest := storageCLITopDigest
	return contract.JobSpec{SchemaVersion: contract.SchemaVersionV1, DispatchKey: dispatchKey,
		Kind: contract.JobKindOCI, Class: contract.JobClassService, Restart: contract.RestartAlways,
		RoutingTags: []string{contract.StableNodeTagPrefix + "storage-node"},
		Execution: contract.ExecutionSpec{OCI: &contract.OCIExecutionSpec{
			Image:  contract.OCIImageSpec{Reference: "ghcr.io/example/computer:latest", Digest: &digest},
			Limits: &contract.OCILimits{MemoryBytes: &memoryBytes}, Computer: &contract.OCIComputerSpec{
				Display: contract.OCIComputerDisplaySpec{Protocol: contract.ComputerDisplayProtocolRFBWebSocketV1}, DiskBytes: diskBytes,
			},
		}},
	}
}

func storageCLICustodyManifest(h *storageCLIHarness) contract.ComputerCustodyManifest {
	return contract.ComputerCustodyManifest{Version: 1, ExportID: "custody-export-tampered", BackupID: "backup-portable",
		CopyID: "copy-portable", ComputerID: h.computer.ComputerID, StorageID: h.computer.StorageID,
		StorageGeneration: h.computer.StorageGeneration, AllocatedSize: 1 << 30, ContentDigest: storageCLITopDigest,
		Encryption: l1.BackupEncryptionNone, NodeID: h.node.NodeID, RootInstanceID: "root-storage-node",
		OperationRevision: 2, CustodyFence: "custody-fence-portable", JobSpec: storageCLIComputerSpec("portable-source"),
		JobSpecHash: "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
		DiskFile:    "storage.ext4", Phase: "complete"}
}
