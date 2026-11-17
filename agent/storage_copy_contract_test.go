package agent

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

// TestStorageCopyAcknowledgementMatchesTheAgentSchema drives the real copy
// controller and validates each acknowledgement body it sends against the
// published L1 agent request schema. A restore that keeps its old generation
// as a Backup acknowledges either the copied destination or, when the
// old-generation Backup failed or is absent, only that Backup's receipt
// (#1717).
func TestStorageCopyAcknowledgementMatchesTheAgentSchema(t *testing.T) {
	schema := compileAgentRequestSchema(t, "/v1/agent/computers/{computer_id}/storage-copy-acknowledgement")
	directive := restoreDirective()
	directive.KeepOldBackup, directive.OldBackupID, directive.OldCopyID = true, "backup-old", "copy-old"
	directive.BackupID, directive.CopyID = "backup-source", "copy-source"
	directive.SourceComputerID, directive.SourceStorageID, directive.SourceGeneration = "computer-source", "storage-source", 3
	directive.SourceSize, directive.SourceDigest = 4<<30, "sha256:"+strings.Repeat("a", 64)
	for _, row := range []struct {
		name          string
		backupFailure string
		wantReceipt   bool
	}{
		{"copied destination", "", true},
		{"failed predecessor Backup", "insufficient_disk", false},
	} {
		t.Run(row.name, func(t *testing.T) {
			acknowledgements := &capturedAcknowledgements{}
			controller := newStorageCopyController(newRoundTripClient(t, acknowledgements),
				verifiedCopier{directive: directive}, predecessorBackupper{directive: directive, failure: row.backupFailure},
				"node-1", "boot-1", "root-1", t.Logf)
			if err := controller.attempt(t.Context(), directive); err != nil {
				t.Fatal(err)
			}
			bodies := acknowledgements.sent()
			if len(bodies) != 1 {
				t.Fatalf("sent %d acknowledgements, want 1", len(bodies))
			}
			if got := bytes.Contains(bodies[0], []byte(`"receipt":`)); got != row.wantReceipt {
				t.Errorf("body carries receipt=%t, want %t: %s", got, row.wantReceipt, bodies[0])
			}
			instance, err := jsonschema.UnmarshalJSON(bytes.NewReader(bodies[0]))
			if err != nil {
				t.Fatal(err)
			}
			if err := schema.Validate(instance); err != nil {
				t.Errorf("acknowledgement %s does not match the L1 agent schema: %v", bodies[0], err)
			}
		})
	}
}

// compileAgentRequestSchema compiles one published L1 agent request schema
// with every document it can reach registered locally, so nothing is fetched.
func compileAgentRequestSchema(t *testing.T, route string) *jsonschema.Schema {
	t.Helper()
	compiler := jsonschema.NewCompiler()
	openapi := filepath.Join("..", "api", "openapi")
	resources := map[string]string{
		"file:///api/openapi/l1-agent.v1.json": filepath.Join(openapi, "l1-agent.v1.json"),
		"file:///api/openapi/common.v1.json":   filepath.Join(openapi, "common.v1.json"),
	}
	for _, name := range []string{"job-spec", "envelope", "gate-result", "run-record", "limit-reached"} {
		path := filepath.Join("..", "contract", "schemas", "v1", name+".schema.json")
		resources["file:///contract/schemas/v1/"+name+".schema.json"] = path
		resources["https://wefty.dev/schemas/v1/"+name+".schema.json"] = path
	}
	for id, path := range resources {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
		if err != nil {
			t.Fatal(err)
		}
		if err := compiler.AddResource(id, decoded); err != nil {
			t.Fatal(err)
		}
	}
	pointer := strings.ReplaceAll(strings.ReplaceAll(route, "~", "~0"), "/", "~1")
	schema, err := compiler.Compile("file:///api/openapi/l1-agent.v1.json#/paths/" + pointer +
		"/post/requestBody/content/application~1json/schema")
	if err != nil {
		t.Fatal(err)
	}
	return schema
}

// capturedAcknowledgements is a fake L1 that records each acknowledgement
// body exactly as the agent put it on the wire.
type capturedAcknowledgements struct {
	mu     sync.Mutex
	bodies [][]byte
}

func (fake *capturedAcknowledgements) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	body, _ := io.ReadAll(request.Body)
	fake.mu.Lock()
	fake.bodies = append(fake.bodies, body)
	fake.mu.Unlock()
	response.Header().Set("Content-Type", "application/json")
	_, _ = response.Write([]byte(`{}`))
}

func (fake *capturedAcknowledgements) sent() [][]byte {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	return append([][]byte(nil), fake.bodies...)
}

// verifiedCopier answers with the complete verified receipt the helper writes
// for the directive's exact copy.
type verifiedCopier struct {
	directive l1.ComputerStorageCopyDirective
}

func (copier verifiedCopier) CopyComputerStorage(context.Context, workloadrunner.ComputerStorageCopyRequest) (workloadrunner.ComputerStorageCopyReceipt, error) {
	directive := copier.directive
	return workloadrunner.ComputerStorageCopyReceipt{Kind: "computer_storage_copy_verified", ReceiptID: "copy-receipt",
		Operation: directive.Operation, BackupID: directive.BackupID, CopyID: directive.CopyID,
		SourceComputerID: directive.SourceComputerID, SourceStorageID: directive.SourceStorageID,
		SourceGeneration: directive.SourceGeneration, DestinationComputerID: directive.DestinationComputerID,
		DestinationStorageID: directive.DestinationStorageID, DestinationGeneration: directive.DestinationGeneration,
		NodeID: directive.BoundNodeID, RootInstanceID: directive.RootInstanceID, JobID: directive.JobID,
		OperationRevision: directive.OperationRevision, CleanupFence: directive.CleanupFence, HelperGeneration: 13,
		SourceSize: directive.SourceSize, DestinationSize: directive.DestinationSize, SourceDigest: directive.SourceDigest,
		DestinationDigest: "sha256:" + strings.Repeat("b", 64), SourceUnchanged: true, DestinationPrepared: true}, nil
}

// predecessorBackupper answers the old-generation Backup with a verified
// receipt, or with a failed-absent one when failure names a code.
type predecessorBackupper struct {
	directive l1.ComputerStorageCopyDirective
	failure   string
}

func (backupper predecessorBackupper) CreateComputerBackup(context.Context, workloadrunner.ComputerBackupRequest) (workloadrunner.ComputerBackupCopyReceipt, error) {
	directive := backupper.directive
	receipt := workloadrunner.ComputerBackupCopyReceipt{Kind: "computer_backup_copy_verified", ReceiptID: "old-backup-receipt",
		BackupID: directive.OldBackupID, CopyID: directive.OldCopyID, ComputerID: directive.DestinationComputerID,
		StorageID: directive.DestinationStorageID, StorageGeneration: directive.OldGeneration, NodeID: directive.BoundNodeID,
		RootInstanceID: directive.RootInstanceID, JobID: directive.JobID, OperationRevision: directive.OperationRevision,
		CleanupFence: directive.CleanupFence, HelperGeneration: 13, AllocatedSize: directive.DestinationSize,
		ContentDigest: "sha256:" + strings.Repeat("c", 64), Encryption: "none"}
	if backupper.failure != "" {
		receipt.Kind, receipt.FailureCode, receipt.CopyAbsent, receipt.ContentDigest =
			"computer_backup_copy_failed_absent", backupper.failure, true, ""
	}
	return receipt, nil
}

func (predecessorBackupper) DeleteComputerBackupCopy(context.Context, workloadrunner.ComputerBackupCopyRemovalRequest) (workloadrunner.ComputerBackupCopyRemovalReceipt, error) {
	return workloadrunner.ComputerBackupCopyRemovalReceipt{}, nil
}
