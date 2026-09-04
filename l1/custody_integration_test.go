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
	manifest := custodyManifest(directive)
	manifestDigest, err := contract.DigestComputerCustodyManifest(manifest)
	if err != nil {
		panic(err)
	}
	return ComputerCustodyExportReceipt{Kind: "computer_custody_export_verified", ReceiptID: "receipt-" + directive.ExportID,
		ExportID: directive.ExportID, BackupID: directive.BackupID, CopyID: directive.CopyID,
		ComputerID: directive.ComputerID, StorageID: directive.StorageID,
		StorageGeneration: directive.StorageGeneration, NodeID: directive.BoundNodeID,
		RootInstanceID: directive.RootInstanceID, OperationRevision: directive.OperationRevision,
		CustodyFence: directive.CustodyFence, HelperGeneration: 9, ExternalPath: directive.ExternalPath,
		AllocatedSize: directive.AllocatedSize, ContentDigest: directive.ContentDigest,
		ManifestDigest: manifestDigest, ExternalOwnerUID: 1000, ExternalOwnerGID: 1000,
		OwnershipApplied: true, PrivateModeApplied: true}
}

func custodyManifest(directive ComputerCustodyExportDirective) contract.ComputerCustodyManifest {
	return contract.ComputerCustodyManifest{Version: 1, ExportID: directive.ExportID,
		BackupID: directive.BackupID, CopyID: directive.CopyID, ComputerID: directive.ComputerID,
		StorageID: directive.StorageID, StorageGeneration: directive.StorageGeneration,
		AllocatedSize: directive.AllocatedSize, ContentDigest: directive.ContentDigest, Encryption: "none",
		NodeID: directive.BoundNodeID, RootInstanceID: directive.RootInstanceID,
		OperationRevision: directive.OperationRevision, CustodyFence: directive.CustodyFence,
		JobSpec: directive.SourceSpec, JobSpecHash: directive.SourceSpecHash, DiskFile: "storage.ext4", Phase: "complete"}
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
	exportDirectives, err := h.store.ListNodeComputerCustodyExportDirectives(context.Background(),
		"fabric-computer-node", node.NodeID, node.BootSessionID)
	if err != nil || len(exportDirectives) != 0 {
		t.Fatalf("removed Computer still emitted Custody export directives = %#v err=%v", exportDirectives, err)
	}
	for _, copy := range removalDirectives[0].ComputerBackupCopies.Copies {
		if _, err := h.store.AcknowledgeComputerBackupPrune(context.Background(), "fabric-computer-node", computer.ComputerID,
			ComputerBackupPruneAcknowledgementRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID,
				IdempotencyKey: "removed-" + copy.CopyID, Receipt: backupRemovalReceipt(copy)}); err != nil {
			t.Fatal(err)
		}
	}
	removalReceipt := RemovalAcknowledgementRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID,
		RemovalGeneration: removalDirectives[0].RemovalGeneration, CleanupFence: removalDirectives[0].CleanupFence,
		RootInstanceID: removalDirectives[0].RootInstanceID, IdempotencyKey: "custody-service-removed"}
	if _, err := h.store.AcknowledgeServiceRemoval(context.Background(), "fabric-computer-node",
		removalDirectives[0].JobID, removalReceipt); err != nil {
		t.Fatal(err)
	}
	if _, changed, err := h.store.FinalizeServiceRemoval(context.Background(), removalDirectives[0].JobID); err != nil || !changed {
		t.Fatalf("finalize Custody removal changed=%t err=%v", changed, err)
	}
	removed, err = h.store.GetComputer(context.Background(), removed.ComputerID)
	if err != nil || removed.RemovalOutcome != "removed_reduced" {
		t.Fatalf("pre-byte Custody event removal = %#v err=%v", removed, err)
	}
	attested, replayed, err := h.store.AttestComputerCustodyDeletedWithReplay(context.Background(), export.ExportID,
		ComputerCustodyAttestationRequest{IdempotencyKey: "operator-deleted", Actor: "operator"})
	if err != nil || replayed || !attested.OperatorAttestedDeleted {
		t.Fatalf("operator_attested_deleted = %#v err=%v", attested, err)
	}
	attested, replayed, err = h.store.AttestComputerCustodyDeletedWithReplay(context.Background(), export.ExportID,
		ComputerCustodyAttestationRequest{IdempotencyKey: "operator-deleted", Actor: "operator"})
	if err != nil || !replayed || !attested.OperatorAttestedDeleted {
		t.Fatalf("operator_attested_deleted replay = %#v replay=%t err=%v", attested, replayed, err)
	}
	removed, err = h.store.GetComputer(context.Background(), removed.ComputerID)
	if err != nil || removed.RemovalOutcome != "removed_reduced" {
		t.Fatalf("attestation upgraded removal = %#v err=%v", removed, err)
	}
	provenance, err := h.store.ListComputerStorageProvenance(context.Background(), removed.ComputerID)
	if err != nil || !provenance.CustodyTainted || len(provenance.CustodyForks) != 1 ||
		provenance.CustodyForks[0].RemovalOutcome != "removed_reduced" || len(provenance.CustodyExports) != 1 ||
		!provenance.CustodyExports[0].OperatorAttestedDeleted {
		t.Fatalf("reduced removal provenance projection = %#v err=%v", provenance, err)
	}
	receipt := successfulCustodyExportReceipt(directive)
	completed, err := h.store.AcknowledgeComputerCustodyExport(context.Background(), "fabric-computer-node",
		computer.ComputerID, ComputerCustodyExportAcknowledgementRequest{NodeID: node.NodeID,
			BootSessionID: node.BootSessionID, IdempotencyKey: receipt.ReceiptID, Receipt: receipt})
	if errorCode(err) != contract.ErrorConflict {
		t.Fatalf("late Custody export acknowledgement = %#v err=%v, want conflict", completed, err)
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
	completedExport, err := h.store.AcknowledgeComputerCustodyExport(context.Background(), "fabric-computer-node",
		source.ComputerID, ComputerCustodyExportAcknowledgementRequest{NodeID: node.NodeID,
			BootSessionID: node.BootSessionID, IdempotencyKey: exportReceipt.ReceiptID, Receipt: exportReceipt})
	if err != nil {
		t.Fatal(err)
	}
	if completedExport.Status != "available" || completedExport.ManifestDigest != exportReceipt.ManifestDigest {
		t.Fatalf("verified Custody export = %#v, want available with manifest evidence", completedExport)
	}
	manifest := custodyManifest(directive)
	operation, replayed, err := h.store.BeginComputerCustodyImport(context.Background(), export.ExportID,
		ComputerCustodyImportRequest{Name: "returned-custody", DiskBytes: backup.AllocatedSize,
			NodeID: node.NodeID, ExternalPath: directive.ExternalPath, Manifest: manifest,
			ManifestDigest: exportReceipt.ManifestDigest, IdempotencyKey: "import-one", Actor: "operator"})
	if err != nil || replayed {
		t.Fatalf("begin Custody import = %#v replayed=%t err=%v", operation, replayed, err)
	}
	reserved, err := h.store.GetComputer(context.Background(), operation.DestinationComputerID)
	if err != nil || reserved.ReconfigurationPhase != ComputerReconfigurationImporting || reserved.Name != "returned-custody" {
		t.Fatalf("unverified import did not atomically reserve Computer = %#v err=%v", reserved, err)
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
		func(value *ComputerStorageCopyReceipt) { value.MachineIDBeforeDigest = "sha256:short" },
		func(value *ComputerStorageCopyReceipt) { value.MachineIDAfterDigest = value.MachineIDBeforeDigest },
		func(value *ComputerStorageCopyReceipt) { value.SourceUnchanged = false },
		func(value *ComputerStorageCopyReceipt) { value.DestinationPrepared = false },
		func(value *ComputerStorageCopyReceipt) { value.PreparationReceipt = true },
		func(value *ComputerStorageCopyReceipt) { value.DestinationChown = true },
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
		func(r *ComputerCustodyExportReceipt) { r.OwnershipApplied = false },
		func(r *ComputerCustodyExportReceipt) { r.PrivateModeApplied = false },
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

func TestCustodyExportFailureClosesPhaseAndPathCanBeReused(t *testing.T) {
	h, node, computer, backup, _ := publishedBackupForStorageCopy(t, 2)
	_, directive := beginCustodyExport(t, h, node, computer, backup, "failure")
	if _, _, err := h.store.BeginComputerCustodyExport(context.Background(), computer.ComputerID,
		ComputerCustodyExportRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"),
			BackupID: "different-backup", ExternalPath: directive.ExternalPath, IdempotencyKey: "failure"}); errorCode(err) != contract.ErrorIdempotencyConflict {
		t.Fatalf("Backup-bound export idempotency = %v", err)
	}
	failure := successfulCustodyExportReceipt(directive)
	failure.Kind, failure.ManifestDigest, failure.FailureCode = "computer_custody_export_failed", "", "cancelled"
	failure.ExternalOwnerUID, failure.ExternalOwnerGID = 0, 0
	failure.OwnershipApplied, failure.PrivateModeApplied = false, false
	failed, err := h.store.AcknowledgeComputerCustodyExport(context.Background(), "fabric-computer-node",
		computer.ComputerID, ComputerCustodyExportAcknowledgementRequest{NodeID: node.NodeID,
			BootSessionID: node.BootSessionID, IdempotencyKey: failure.ReceiptID, Receipt: failure})
	if err != nil || failed.Status != "failed" || failed.FailureCode != "cancelled" {
		t.Fatalf("failed Custody export = %#v err=%v", failed, err)
	}
	current, err := h.store.GetComputer(context.Background(), computer.ComputerID)
	if err != nil || current.ReconfigurationPhase != ComputerReconfigurationStable {
		t.Fatalf("failed export stranded Computer = %#v err=%v", current, err)
	}
	if _, _, err := h.store.BeginComputerCustodyExport(context.Background(), computer.ComputerID,
		ComputerCustodyExportRequest{ComputerMutationPrecondition: computerPrecondition(current, "operator"),
			BackupID: backup.BackupID, ExternalPath: directive.ExternalPath, IdempotencyKey: "path-reuse"}); err != nil {
		t.Fatalf("new export event could not reuse operator mount point: %v", err)
	}
}

func TestCustodyImportFailureUsesStoredVerbAndReleasesName(t *testing.T) {
	h, node, source, backup, _ := publishedBackupForStorageCopy(t, 2)
	export, directive := beginCustodyExport(t, h, node, source, backup, "import-failure")
	exportReceipt := successfulCustodyExportReceipt(directive)
	if _, err := h.store.AcknowledgeComputerCustodyExport(context.Background(), "fabric-computer-node",
		source.ComputerID, ComputerCustodyExportAcknowledgementRequest{NodeID: node.NodeID,
			BootSessionID: node.BootSessionID, IdempotencyKey: exportReceipt.ReceiptID, Receipt: exportReceipt}); err != nil {
		t.Fatal(err)
	}
	manifest := custodyManifest(directive)
	operation, _, err := h.store.BeginComputerCustodyImport(context.Background(), export.ExportID,
		ComputerCustodyImportRequest{Name: "reusable-import", DiskBytes: backup.AllocatedSize,
			NodeID: node.NodeID, ExternalPath: directive.ExternalPath, Manifest: manifest,
			ManifestDigest: exportReceipt.ManifestDigest, IdempotencyKey: "failed-import", Actor: "operator"})
	if err != nil {
		t.Fatal(err)
	}
	directives, err := h.store.ListNodeComputerStorageCopyDirectives(context.Background(),
		"fabric-computer-node", node.NodeID, node.BootSessionID)
	if err != nil || len(directives) != 1 || directives[0].Operation != "import" {
		t.Fatalf("import directive = %#v err=%v", directives, err)
	}
	preparation := ComputerStoragePreparationOutcome{Code: ComputerStoragePreparationResumeDeferred,
		DestinationComputerID: operation.DestinationComputerID, DestinationStorageID: operation.DestinationStorageID,
		DestinationGeneration: 1, IntentRevision: operation.OperationRevision, DiskBytes: operation.DestinationSize,
		HelperGeneration: 4, SweepEpoch: "sweep-import", DiskName: "computer-import",
		Operation: "computer_storage_copy", Reason: "resume_deferred", DeferredReason: "recovery_attempt_budget", Attempts: 3}
	tampered := preparation
	tampered.IntentRevision++
	if _, err := h.store.AcknowledgeComputerStorageCopy(context.Background(), "fabric-computer-node",
		operation.DestinationComputerID, ComputerStorageCopyAcknowledgementRequest{NodeID: node.NodeID,
			BootSessionID: node.BootSessionID, IdempotencyKey: "tampered-preparation", PreparationOutcome: &tampered}); errorCode(err) != contract.ErrorStorageReferenceConflict {
		t.Fatalf("foreign preparation outcome error = %v", err)
	}
	if _, err := h.store.AcknowledgeComputerStorageCopy(context.Background(), "fabric-computer-node",
		operation.DestinationComputerID, ComputerStorageCopyAcknowledgementRequest{NodeID: node.NodeID,
			BootSessionID: node.BootSessionID, IdempotencyKey: "deferred-preparation", PreparationOutcome: &preparation}); err != nil {
		t.Fatal(err)
	}
	observed, err := h.store.GetComputerCustodyImport(context.Background(), operation.ImportID)
	if err != nil || observed.OperationRevision != operation.OperationRevision || observed.Status != "reserved" ||
		observed.PreparationOutcome == nil || observed.PreparationOutcome.RecordedAt == nil ||
		observed.PreparationOutcome.Code != ComputerStoragePreparationResumeDeferred {
		t.Fatalf("durable preparation outcome = %#v err=%v", observed, err)
	}
	directives, err = h.store.ListNodeComputerStorageCopyDirectives(context.Background(),
		"fabric-computer-node", node.NodeID, node.BootSessionID)
	if err != nil || len(directives) != 1 {
		t.Fatalf("deferred import was not retryable: %#v err=%v", directives, err)
	}
	failure := successfulStorageCopyReceipt(directives[0])
	failure.Kind = "computer_storage_copy_failed_absent"
	failure.Operation = "clone"
	failure.DestinationDigest = ""
	failure.OSIdentityRekeyed = false
	failure.MachineIDBeforeDigest = ""
	failure.MachineIDAfterDigest = ""
	failure.SourceUnchanged = false
	failure.DestinationPrepared = false
	failure.FilesystemExpanded = false
	failure.FailureCode = "manifest_invalid"
	failure.DestinationAbsent = true
	if _, err := h.store.AcknowledgeComputerStorageCopy(context.Background(), "fabric-computer-node",
		operation.DestinationComputerID, ComputerStorageCopyAcknowledgementRequest{NodeID: node.NodeID,
			BootSessionID: node.BootSessionID, IdempotencyKey: failure.ReceiptID, Receipt: failure}); err == nil {
		t.Fatal("receipt-selected clone validator bypassed the stored import verb")
	}
	failure.Operation = "import"
	if _, err := h.store.AcknowledgeComputerStorageCopy(context.Background(), "fabric-computer-node",
		operation.DestinationComputerID, ComputerStorageCopyAcknowledgementRequest{NodeID: node.NodeID,
			BootSessionID: node.BootSessionID, IdempotencyKey: failure.ReceiptID, Receipt: failure}); err != nil {
		t.Fatal(err)
	}
	var status string
	if err := h.store.db.QueryRow(`SELECT status FROM computer_storage_copy_operations
		WHERE destination_computer_id=?`, operation.DestinationComputerID).Scan(&status); err != nil || status != "failed" {
		t.Fatalf("durable failed import status=%q err=%v", status, err)
	}
	if _, err := h.store.GetComputer(context.Background(), operation.DestinationComputerID); errorCode(err) != contract.ErrorNotFound {
		t.Fatalf("failed import retained reserved identity: %v", err)
	}
	observed, err = h.store.GetComputerCustodyImport(context.Background(), operation.ImportID)
	if err != nil || observed.Status != "failed" || observed.FailureCode != "manifest_invalid" ||
		observed.PreparationOutcome != nil || observed.CompletedAt == nil {
		t.Fatalf("failed import durable observation = %#v err=%v", observed, err)
	}
	if _, _, err := h.store.BeginComputerCustodyImport(context.Background(), export.ExportID,
		ComputerCustodyImportRequest{Name: "reusable-import", DiskBytes: backup.AllocatedSize,
			NodeID: node.NodeID, ExternalPath: directive.ExternalPath, Manifest: manifest,
			ManifestDigest: exportReceipt.ManifestDigest, IdempotencyKey: "retry-import", Actor: "operator"}); err != nil {
		t.Fatalf("failed import burned destination name: %v", err)
	}
}

func TestCustodyExportAndImportAbortWhenTheirDestinationNodeDies(t *testing.T) {
	t.Run("export", func(t *testing.T) {
		h, node, computer, backup, _ := publishedBackupForStorageCopy(t, 2)
		_, _ = beginCustodyExport(t, h, node, computer, backup, "abort-export")
		current, err := h.store.GetComputer(t.Context(), computer.ComputerID)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := h.store.db.Exec(`UPDATE nodes SET state=? WHERE node_id=?`, contract.NodeDead, node.NodeID); err != nil {
			t.Fatal(err)
		}
		aborted, _, err := h.store.AbortComputerReconfiguration(t.Context(), computer.ComputerID,
			ComputerReconfigurationAbortRequest{ComputerMutationPrecondition: computerPrecondition(current, "operator"),
				IdempotencyKey: "abort-export"})
		if err != nil || aborted.ReconfigurationPhase != ComputerReconfigurationStable {
			t.Fatalf("export abort = %#v err=%v", aborted, err)
		}
		var status, failureCode string
		if err := h.store.db.QueryRow(`SELECT status, failure_code FROM computer_custody_exports
			WHERE computer_id=?`, computer.ComputerID).Scan(&status, &failureCode); err != nil ||
			status != "failed" || failureCode != "aborted_dead_node" {
			t.Fatalf("aborted export status=%q failure=%q err=%v", status, failureCode, err)
		}
	})

	t.Run("import", func(t *testing.T) {
		h, node, source, backup, _ := publishedBackupForStorageCopy(t, 2)
		export, directive := beginCustodyExport(t, h, node, source, backup, "abort-import")
		exportReceipt := successfulCustodyExportReceipt(directive)
		if _, err := h.store.AcknowledgeComputerCustodyExport(t.Context(), "fabric-computer-node",
			source.ComputerID, ComputerCustodyExportAcknowledgementRequest{NodeID: node.NodeID,
				BootSessionID: node.BootSessionID, IdempotencyKey: exportReceipt.ReceiptID, Receipt: exportReceipt}); err != nil {
			t.Fatal(err)
		}
		operation, _, err := h.store.BeginComputerCustodyImport(t.Context(), export.ExportID,
			ComputerCustodyImportRequest{Name: "abort-import-name", DiskBytes: backup.AllocatedSize,
				NodeID: node.NodeID, ExternalPath: directive.ExternalPath, Manifest: custodyManifest(directive),
				ManifestDigest: exportReceipt.ManifestDigest, IdempotencyKey: "abort-import", Actor: "operator"})
		if err != nil {
			t.Fatal(err)
		}
		reserved, err := h.store.GetComputer(t.Context(), operation.DestinationComputerID)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := h.store.db.Exec(`UPDATE nodes SET state=? WHERE node_id=?`, contract.NodeDead, node.NodeID); err != nil {
			t.Fatal(err)
		}
		aborted, _, err := h.store.AbortComputerReconfiguration(t.Context(), reserved.ComputerID,
			ComputerReconfigurationAbortRequest{ComputerMutationPrecondition: computerPrecondition(reserved, "operator"),
				IdempotencyKey: "abort-import"})
		if err != nil || aborted.ReconfigurationPhase != ComputerReconfigurationStable || aborted.Name == "abort-import-name" {
			t.Fatalf("import abort = %#v err=%v", aborted, err)
		}
		var status string
		if err := h.store.db.QueryRow(`SELECT status FROM computer_storage_copy_operations
			WHERE destination_computer_id=?`, reserved.ComputerID).Scan(&status); err != nil || status != "superseded" {
			t.Fatalf("aborted import status=%q err=%v", status, err)
		}
	})
}

func TestCustodyImportSurvivesExportingDatabaseLossAndUsesCurrentDestinationRoot(t *testing.T) {
	origin, originNode, source, backup, _ := publishedBackupForStorageCopy(t, 2)
	export, directive := beginCustodyExport(t, origin, originNode, source, backup, "portable")
	exportReceipt := successfulCustodyExportReceipt(directive)
	manifest := custodyManifest(directive)

	destination := newIntegrationHarnessWithOptions(t, StoreOptions{}, map[string]NodePolicy{
		"import-node": {Tags: []string{contract.StableNodeTagPrefix + "import-node"}, MaxOneshotSlots: 1, MaxServiceSlots: 1},
	})
	destinationNode := registerCapabilityNodeWithTags(t, destination, "import-node", map[string]bool{
		"kind:oci": true, "cgroup_v2": true, "computer": true,
	}, []string{contract.StableNodeTagPrefix + "import-node"})
	if destinationNode.RootInstanceID == manifest.RootInstanceID || destinationNode.NodeID == manifest.NodeID {
		t.Fatal("portable import fixture did not replace the export Node/root identity")
	}
	operation, replayed, err := destination.store.BeginComputerCustodyImport(t.Context(), export.ExportID,
		ComputerCustodyImportRequest{Name: "portable-import", DiskBytes: backup.AllocatedSize,
			NodeID: destinationNode.NodeID, ExternalPath: directive.ExternalPath, Manifest: manifest,
			ManifestDigest: exportReceipt.ManifestDigest, IdempotencyKey: "portable-import", Actor: "operator"})
	if err != nil || replayed {
		t.Fatalf("portable import reservation = %#v replayed=%t err=%v", operation, replayed, err)
	}
	directives, err := destination.store.ListNodeComputerStorageCopyDirectives(t.Context(),
		"fabric-import-node", destinationNode.NodeID, destinationNode.BootSessionID)
	if err != nil || len(directives) != 1 || directives[0].RootInstanceID != destinationNode.RootInstanceID ||
		directives[0].RootInstanceID == manifest.RootInstanceID {
		t.Fatalf("portable import directive = %#v err=%v", directives, err)
	}
	receipt := successfulStorageCopyReceipt(directives[0])
	imported, err := destination.store.AcknowledgeComputerStorageCopy(t.Context(), "fabric-import-node",
		operation.DestinationComputerID, ComputerStorageCopyAcknowledgementRequest{NodeID: destinationNode.NodeID,
			BootSessionID: destinationNode.BootSessionID, IdempotencyKey: receipt.ReceiptID, Receipt: receipt})
	if err != nil {
		t.Fatal(err)
	}
	tx, err := destination.store.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`UPDATE computers SET removal_outcome='removal_pending' WHERE computer_id=?`, imported.ComputerID); err != nil {
		t.Fatal(err)
	}
	if err := finalizeComputerCustodyOutcome(t.Context(), tx, imported.ComputerID, destination.clock.Now()); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	imported, err = destination.store.GetComputer(t.Context(), imported.ComputerID)
	if err != nil || imported.RemovalOutcome != "removed_reduced" {
		t.Fatalf("portable import lost external-custody taint = %#v err=%v", imported, err)
	}
}
