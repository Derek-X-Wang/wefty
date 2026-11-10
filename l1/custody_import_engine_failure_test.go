package l1

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

// A Custody import whose copy the live helper session refused with an engine
// failure is terminal (#1581). Before, the failure was read as helper runtime
// loss: the agent closed the session, ending every attempt on the Node, and
// the import stayed retryable. With the session alive, retrying the same
// copy on the same session fails the same way, so a retryable outcome would
// put the directive back on every sweep (the #526 shape). Runtime loss stays
// retryable for an import: the next session's startup sweep recovers the copy.
func TestCustodyImportEngineFailureIsTerminalAndRuntimeLossStaysRetryable(t *testing.T) {
	ctx := context.Background()
	h, node, source, backup, _ := publishedBackupForStorageCopy(t, 3)
	export, exportDirective := beginCustodyExport(t, h, node, source, backup, "engine-failure")
	exportReceipt := successfulCustodyExportReceipt(exportDirective)
	if _, err := h.store.AcknowledgeComputerCustodyExport(ctx, "fabric-computer-node", source.ComputerID,
		ComputerCustodyExportAcknowledgementRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID,
			IdempotencyKey: exportReceipt.ReceiptID, Receipt: exportReceipt}); err != nil {
		t.Fatal(err)
	}
	operation, _, err := h.store.BeginComputerCustodyImport(ctx, export.ExportID, ComputerCustodyImportRequest{
		Name: "refused-import", DiskBytes: backup.AllocatedSize, NodeID: node.NodeID,
		ExternalPath: exportDirective.ExternalPath, Manifest: custodyManifest(exportDirective),
		ManifestDigest: exportReceipt.ManifestDigest, IdempotencyKey: "import-engine-failure", Actor: "operator"})
	if err != nil {
		t.Fatal(err)
	}
	listImports := func() []ComputerStorageCopyDirective {
		t.Helper()
		directives, err := h.store.ListNodeComputerStorageCopyDirectives(ctx, "fabric-computer-node", node.NodeID, node.BootSessionID)
		if err != nil {
			t.Fatal(err)
		}
		return directives
	}
	if directives := listImports(); len(directives) != 1 || directives[0].Operation != "import" {
		t.Fatalf("Custody import directives = %#v", directives)
	}
	recordedAt := time.Date(2026, 11, 10, 18, 0, 0, 0, time.UTC)
	outcome := func(reason string, helperGeneration uint64, at time.Time) *ComputerStoragePreparationOutcome {
		return &ComputerStoragePreparationOutcome{Code: ComputerStoragePreparationInterrupted,
			DestinationComputerID: operation.DestinationComputerID, DestinationStorageID: operation.DestinationStorageID,
			DestinationGeneration: 1, IntentRevision: operation.OperationRevision, DiskBytes: operation.DestinationSize,
			HelperGeneration: helperGeneration, Operation: "computer_storage_copy", Reason: reason, RecordedAt: &at}
	}
	acknowledge := func(key string, prepared *ComputerStoragePreparationOutcome) (Computer, error) {
		return h.store.AcknowledgeComputerStorageCopy(ctx, "fabric-computer-node", operation.DestinationComputerID,
			ComputerStorageCopyAcknowledgementRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID,
				IdempotencyKey: key, PreparationOutcome: prepared})
	}

	// Runtime loss: recorded, and the directive is listed again.
	if _, err := acknowledge("runtime-loss", outcome(ComputerStoragePreparationReasonRuntimeLoss, 4, recordedAt)); err != nil {
		t.Fatalf("record runtime-loss import outcome: %v", err)
	}
	if directives := listImports(); len(directives) != 1 || directives[0].DestinationComputerID != operation.DestinationComputerID {
		t.Fatalf("runtime-loss import is no longer retryable: directives = %#v", directives)
	}

	// Engine failure on the next, live session: terminal, exactly once.
	refused := outcome(ComputerStoragePreparationReasonEngineFailure, 5, recordedAt.Add(time.Minute))
	closed, err := acknowledge("engine-failure", refused)
	if err != nil || closed.ReconfigurationPhase != ComputerReconfigurationStable || closed.ReconfigurationRevision != nil ||
		closed.AppliedRevision != 1 || closed.DesiredState != contract.ServiceDesiredStopped ||
		closed.CurrentJob.State != contract.JobFailed {
		t.Fatalf("engine-failure import = %#v err=%v", closed, err)
	}
	var failure contract.SpawnFailure
	if closed.CurrentJob.ServiceJob == nil || json.Unmarshal(closed.CurrentJob.LastFailure, &failure) != nil ||
		failure.Code != contract.SpawnFailureComputerStoragePreparationInterrupted || failure.NodeID != node.NodeID ||
		failure.ObservedAvailableBytes != 0 || closed.CurrentJob.NextRestartAt != nil {
		t.Fatalf("engine-failure import latch = %s failure=%#v next_restart=%v",
			closed.CurrentJob.LastFailure, failure, closed.CurrentJob.NextRestartAt)
	}
	if directives := listImports(); len(directives) != 0 {
		t.Fatalf("engine-failure import is listed again on the next sweep: %#v", directives)
	}
	observed, err := testReads(h.store).GetComputerCustodyImport(ctx, operation.ImportID)
	if err != nil || observed.Status != "failed" || observed.FailureCode != ComputerStoragePreparationInterrupted ||
		observed.PreparationOutcome == nil || observed.PreparationOutcome.Reason != ComputerStoragePreparationReasonEngineFailure ||
		observed.PreparationOutcome.HelperGeneration != 5 || observed.CompletedAt == nil {
		t.Fatalf("engine-failure import observation = %#v err=%v", observed, err)
	}
	// Nothing on the Node is deleted and the identity is kept: only the
	// never-published generation row is retired, so no later start formats an
	// empty disk under it, and ordinary removal remains the exit.
	generations, err := testReads(h.store).ListComputerStorageGenerations(ctx, operation.DestinationComputerID)
	if err != nil || len(generations.Generations) != 1 || generations.Generations[0].Phase != "retired" {
		t.Fatalf("engine-failure import generations = %#v err=%v", generations.Generations, err)
	}
	if _, err := testReads(h.store).GetComputer(ctx, operation.DestinationComputerID); err != nil {
		t.Fatalf("engine-failure import released its Computer identity without an absence receipt: %v", err)
	}

	// A node that never saw the acknowledgement land replays it unchanged.
	if replayed, err := acknowledge("engine-failure", refused); err != nil || replayed.CurrentJob.State != contract.JobFailed {
		t.Fatalf("engine-failure import replay = %#v err=%v", replayed, err)
	}
	if _, err := acknowledge("engine-failure-again", outcome(ComputerStoragePreparationReasonEngineFailure, 5,
		recordedAt.Add(2*time.Minute))); errorCode(err) != contract.ErrorConflict {
		t.Fatalf("second outcome on a closed import error = %v", err)
	}
}
