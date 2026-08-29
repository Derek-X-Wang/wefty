package l1

import (
	"context"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
)

func beginCustodyExport(t *testing.T, h *integrationHarness, node Node, computer Computer, backup Backup, key string) (ComputerCustodyExport, ComputerCustodyExportDirective) {
	t.Helper()
	export, replayed, err := h.store.BeginComputerCustodyExport(context.Background(), computer.ComputerID,
		ComputerCustodyExportRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"),
			BackupID: backup.BackupID, ExternalPath: "/operator/export-" + key, IdempotencyKey: key})
	if err != nil || replayed {
		t.Fatalf("begin Custody export = %#v replayed=%t err=%v", export, replayed, err)
	}
	directives, err := h.store.ListNodeComputerCustodyExportDirectives(context.Background(),
		"fabric-computer-node", node.NodeID, node.BootSessionID)
	if err != nil || len(directives) != 1 || directives[0].ExportID != export.ExportID {
		t.Fatalf("Custody export directives = %#v err=%v", directives, err)
	}
	return export, directives[0]
}

func successfulCustodyExportReceipt(directive ComputerCustodyExportDirective) ComputerCustodyExportReceipt {
	return ComputerCustodyExportReceipt{Kind: "computer_custody_export_verified", ReceiptID: "receipt-" + directive.ExportID,
		ExportID: directive.ExportID, BackupID: directive.BackupID, CopyID: directive.CopyID,
		ComputerID: directive.ComputerID, StorageID: directive.StorageID,
		StorageGeneration: directive.StorageGeneration, NodeID: directive.BoundNodeID,
		RootInstanceID: directive.RootInstanceID, OperationRevision: directive.OperationRevision,
		CustodyFence: directive.CustodyFence, HelperGeneration: 9, ExternalPath: directive.ExternalPath,
		AllocatedSize: directive.AllocatedSize, ContentDigest: directive.ContentDigest,
		ManifestDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
}

func TestCustodyExportCommitsTaintBeforeBytesAndAttestationNeverUpgrades(t *testing.T) {
	h, node, computer, backup, _ := publishedBackupForStorageCopy(t, 2)
	export, directive := beginCustodyExport(t, h, node, computer, backup, "race")
	current, err := h.store.GetComputer(context.Background(), computer.ComputerID)
	if err != nil {
		t.Fatal(err)
	}
	removed, err := h.store.RemoveComputer(context.Background(), current.ComputerID,
		ComputerRemoveRequest{ComputerMutationPrecondition: computerPrecondition(current, "operator-remove")})
	if err != nil {
		t.Fatal(err)
	}
	removalDirectives, err := h.store.ListNodeRemovalDirectives(context.Background(),
		"fabric-computer-node", node.NodeID, node.BootSessionID)
	if err != nil || len(removalDirectives) != 1 || removalDirectives[0].ComputerCustodyExports == nil ||
		len(removalDirectives[0].ComputerCustodyExports.Exports) != 1 ||
		removalDirectives[0].ComputerCustodyExports.Exports[0].ExportID != export.ExportID {
		t.Fatalf("composite removal Custody facts = %#v err=%v", removalDirectives, err)
	}
	tx, err := h.store.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := finalizeComputerCustodyOutcome(context.Background(), tx, removed.ComputerID, h.clock.Now()); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	removed, err = h.store.GetComputer(context.Background(), removed.ComputerID)
	if err != nil || removed.RemovalOutcome != "removed_reduced" {
		t.Fatalf("pre-byte Custody event removal = %#v err=%v", removed, err)
	}
	attested, err := h.store.AttestComputerCustodyDeleted(context.Background(), export.ExportID,
		ComputerCustodyAttestationRequest{IdempotencyKey: "operator-deleted", Actor: "operator"})
	if err != nil || !attested.OperatorAttestedDeleted {
		t.Fatalf("operator_attested_deleted = %#v err=%v", attested, err)
	}
	tx, err = h.store.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := finalizeComputerCustodyOutcome(context.Background(), tx, removed.ComputerID, h.clock.Now()); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	removed, err = h.store.GetComputer(context.Background(), removed.ComputerID)
	if err != nil || removed.RemovalOutcome != "removed_reduced" {
		t.Fatalf("attestation upgraded removal = %#v err=%v", removed, err)
	}
	receipt := successfulCustodyExportReceipt(directive)
	completed, err := h.store.AcknowledgeComputerCustodyExport(context.Background(), "fabric-computer-node",
		computer.ComputerID, ComputerCustodyExportAcknowledgementRequest{NodeID: node.NodeID,
			BootSessionID: node.BootSessionID, IdempotencyKey: receipt.ReceiptID, Receipt: receipt})
	if err != nil || completed.Status != "exported" {
		t.Fatalf("late Custody export acknowledgement = %#v err=%v", completed, err)
	}
	removed, _ = h.store.GetComputer(context.Background(), removed.ComputerID)
	if removed.RemovalOutcome != "removed_reduced" || removed.ReconfigurationPhase != ComputerReconfigurationRemoving {
		t.Fatalf("late export acknowledgement changed removal truth = %#v", removed)
	}
}

func TestCustodyImportValidatesReceiptCreatesNoGrantIdentityAndTaintsDescendants(t *testing.T) {
	h, node, source, backup, _ := publishedBackupForStorageCopy(t, 3)
	export, directive := beginCustodyExport(t, h, node, source, backup, "import")
	exportReceipt := successfulCustodyExportReceipt(directive)
	if _, err := h.store.AcknowledgeComputerCustodyExport(context.Background(), "fabric-computer-node",
		source.ComputerID, ComputerCustodyExportAcknowledgementRequest{NodeID: node.NodeID,
			BootSessionID: node.BootSessionID, IdempotencyKey: exportReceipt.ReceiptID, Receipt: exportReceipt}); err != nil {
		t.Fatal(err)
	}
	operation, replayed, err := h.store.BeginComputerCustodyImport(context.Background(), export.ExportID,
		ComputerCustodyImportRequest{Name: "returned-custody", DiskBytes: backup.AllocatedSize,
			ExternalPath: directive.ExternalPath, IdempotencyKey: "import-one", Actor: "operator"})
	if err != nil || replayed {
		t.Fatalf("begin Custody import = %#v replayed=%t err=%v", operation, replayed, err)
	}
	if _, err := h.store.GetComputer(context.Background(), operation.DestinationComputerID); errorCode(err) != contract.ErrorNotFound {
		t.Fatalf("unverified import created Computer = %v", err)
	}
	copyDirectives, err := h.store.ListNodeComputerStorageCopyDirectives(context.Background(),
		"fabric-computer-node", node.NodeID, node.BootSessionID)
	if err != nil || len(copyDirectives) != 1 || copyDirectives[0].Operation != "import" {
		t.Fatalf("Custody import directives = %#v err=%v", copyDirectives, err)
	}
	receipt := successfulStorageCopyReceipt(copyDirectives[0])
	mutations := []func(*ComputerStorageCopyReceipt){
		func(value *ComputerStorageCopyReceipt) { value.ExportID = "wrong" },
		func(value *ComputerStorageCopyReceipt) { value.ExternalPath = "/wrong" },
		func(value *ComputerStorageCopyReceipt) {
			value.ManifestDigest = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
		},
		func(value *ComputerStorageCopyReceipt) { value.DestinationDigest = "sha256:short" },
		func(value *ComputerStorageCopyReceipt) { value.OSIdentityRekeyed = false },
		func(value *ComputerStorageCopyReceipt) { value.FilesystemExpanded = true },
	}
	for index, mutate := range mutations {
		tampered := receipt
		mutate(&tampered)
		if _, err := h.store.AcknowledgeComputerStorageCopy(context.Background(), "fabric-computer-node",
			operation.DestinationComputerID, ComputerStorageCopyAcknowledgementRequest{NodeID: node.NodeID,
				BootSessionID: node.BootSessionID, IdempotencyKey: "tampered", Receipt: tampered}); errorCode(err) != contract.ErrorStorageReferenceConflict {
			t.Fatalf("tampered Custody import receipt row %d = %v", index, err)
		}
	}
	imported, err := h.store.AcknowledgeComputerStorageCopy(context.Background(), "fabric-computer-node",
		operation.DestinationComputerID, ComputerStorageCopyAcknowledgementRequest{NodeID: node.NodeID,
			BootSessionID: node.BootSessionID, IdempotencyKey: receipt.ReceiptID, Receipt: receipt})
	if err != nil || imported.ComputerID == source.ComputerID || imported.StorageID == source.StorageID ||
		len(imported.Grants) != 0 || imported.DesiredState != contract.ServiceDesiredStopped {
		t.Fatalf("verified Custody import = %#v err=%v", imported, err)
	}

	provenanceID, importedBackupID, importedCopyID := newID("storage-provenance"), newID("backup"), newID("backup-copy")
	if _, err := h.store.db.Exec(`INSERT INTO backups(backup_id, computer_id, source_storage_id,
		source_generation, created_ns, allocated_size, content_digest, encryption, provenance_id, status)
		VALUES(?, ?, ?, 1, ?, ?, ?, 'none', ?, 'available')`, importedBackupID, imported.ComputerID,
		imported.StorageID, h.clock.Now().UnixNano(), imported.DesiredDiskBytes, receipt.DestinationDigest, provenanceID); err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.db.Exec(`INSERT INTO backup_copies(copy_id, backup_id, node_id, root_instance_id,
		allocated_size, content_digest, phase, created_ns) VALUES(?, ?, ?, ?, ?, ?, 'published', ?)`,
		importedCopyID, importedBackupID, node.NodeID, node.RootInstanceID, imported.DesiredDiskBytes,
		receipt.DestinationDigest, h.clock.Now().UnixNano()); err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.db.Exec(`INSERT INTO storage_provenance(provenance_id, kind, source_storage_id,
		source_generation, backup_id, created_ns) VALUES(?, 'backup', ?, 1, ?, ?)`, provenanceID,
		imported.StorageID, importedBackupID, h.clock.Now().UnixNano()); err != nil {
		t.Fatal(err)
	}
	clone, _, err := h.store.BeginComputerClone(context.Background(), ComputerCloneRequest{
		ComputerMutationPrecondition: computerPrecondition(imported, "operator"), BackupID: importedBackupID,
		Name: "second-generation", DiskBytes: imported.DesiredDiskBytes, IdempotencyKey: "clone-import", Actor: "operator"})
	if err != nil {
		t.Fatal(err)
	}
	copyDirectives, err = h.store.ListNodeComputerStorageCopyDirectives(context.Background(),
		"fabric-computer-node", node.NodeID, node.BootSessionID)
	if err != nil || len(copyDirectives) != 1 || copyDirectives[0].DestinationComputerID != clone.ComputerID {
		t.Fatalf("second-generation clone directive = %#v err=%v", copyDirectives, err)
	}
	cloneReceipt := successfulStorageCopyReceipt(copyDirectives[0])
	clone, err = h.store.AcknowledgeComputerStorageCopy(context.Background(), "fabric-computer-node", clone.ComputerID,
		ComputerStorageCopyAcknowledgementRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID,
			IdempotencyKey: cloneReceipt.ReceiptID, Receipt: cloneReceipt})
	if err != nil {
		t.Fatal(err)
	}
	tx, err := h.store.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`UPDATE computers SET removal_outcome='removal_pending' WHERE computer_id=?`, clone.ComputerID); err != nil {
		t.Fatal(err)
	}
	if err := finalizeComputerCustodyOutcome(context.Background(), tx, clone.ComputerID, h.clock.Now()); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	clone, err = h.store.GetComputer(context.Background(), clone.ComputerID)
	if err != nil || clone.RemovalOutcome != "removed_reduced" {
		t.Fatalf("second-generation Custody descendant removal = %#v err=%v", clone, err)
	}
}

func TestCustodyExportReceiptMutationRowsFail(t *testing.T) {
	h, node, computer, backup, _ := publishedBackupForStorageCopy(t, 2)
	_, directive := beginCustodyExport(t, h, node, computer, backup, "mutations")
	valid := successfulCustodyExportReceipt(directive)
	mutations := []func(*ComputerCustodyExportReceipt){
		func(r *ComputerCustodyExportReceipt) { r.Kind = "wrong" },
		func(r *ComputerCustodyExportReceipt) { r.ReceiptID = "" },
		func(r *ComputerCustodyExportReceipt) { r.ExportID = "wrong" },
		func(r *ComputerCustodyExportReceipt) { r.BackupID = "wrong" },
		func(r *ComputerCustodyExportReceipt) { r.CopyID = "wrong" },
		func(r *ComputerCustodyExportReceipt) { r.ComputerID = "wrong" },
		func(r *ComputerCustodyExportReceipt) { r.StorageID = "wrong" },
		func(r *ComputerCustodyExportReceipt) { r.StorageGeneration++ },
		func(r *ComputerCustodyExportReceipt) { r.NodeID = "wrong" },
		func(r *ComputerCustodyExportReceipt) { r.RootInstanceID = "wrong" },
		func(r *ComputerCustodyExportReceipt) { r.OperationRevision++ },
		func(r *ComputerCustodyExportReceipt) { r.CustodyFence = "wrong" },
		func(r *ComputerCustodyExportReceipt) { r.HelperGeneration = 0 },
		func(r *ComputerCustodyExportReceipt) { r.ExternalPath = "/other" },
		func(r *ComputerCustodyExportReceipt) { r.AllocatedSize++ },
		func(r *ComputerCustodyExportReceipt) {
			r.ContentDigest = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
		},
		func(r *ComputerCustodyExportReceipt) { r.ManifestDigest = "" },
	}
	for index, mutate := range mutations {
		receipt := valid
		mutate(&receipt)
		if _, err := h.store.AcknowledgeComputerCustodyExport(context.Background(), "fabric-computer-node",
			computer.ComputerID, ComputerCustodyExportAcknowledgementRequest{NodeID: node.NodeID,
				BootSessionID: node.BootSessionID, IdempotencyKey: "mutation", Receipt: receipt}); err == nil {
			t.Fatalf("Custody export receipt mutation %d was accepted", index)
		}
	}
}
