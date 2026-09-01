package l1

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

func publishedBackupForStorageCopy(t *testing.T, keepCapacity int64) (*integrationHarness, Node, Computer, Backup, *Claim) {
	t.Helper()
	h, node, computer := backupHarness(t, keepCapacity, nil)
	computer, claim := startBackupComputer(t, h, node, computer)
	reserved, _, err := h.store.BeginComputerBackup(context.Background(), computer.ComputerID,
		ComputerBackupCreateRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"), IdempotencyKey: "copy-source", AllowPowerOff: true})
	if err != nil {
		t.Fatal(err)
	}
	finishBackupQuiescence(t, h, claim, "copy-source-stop")
	directives, err := h.store.ListNodeComputerBackupDirectives(context.Background(), "fabric-computer-node", node.NodeID, node.BootSessionID)
	if err != nil || len(directives) != 1 {
		t.Fatalf("source Backup directive = %#v err=%v", directives, err)
	}
	backup, resumed := acknowledgeBackup(t, h, node, directives[0], successfulBackupReceipt(directives[0]))
	stopped, err := h.store.SetComputerDesiredState(context.Background(), resumed.ComputerID,
		ComputerDesiredStateRequest{ComputerMutationPrecondition: computerPrecondition(resumed, "operator"), DesiredState: contract.ServiceDesiredStopped})
	if err != nil {
		t.Fatal(err)
	}
	if stopped.CurrentJob.State != contract.JobStopped || stopped.DesiredState != contract.ServiceDesiredStopped ||
		reserved.IntentRevision+1 != stopped.IntentRevision {
		t.Fatalf("stopped restore source = %#v", stopped)
	}
	return h, node, stopped, backup, claim
}

func TestRestoreHeartbeatFailsClosedAndRecordsRevocationBeforeDirective(t *testing.T) {
	h, node, computer, source, claim := publishedBackupForStorageCopy(t, 2)
	for index, event := range []struct {
		id, kind, sessionID, reason string
	}{
		{"restore-live:open", "session_open", "restore-live", ""},
		{"restore-closed:open", "session_open", "restore-closed", ""},
		{"restore-closed:close", "session_close", "restore-closed", string(ComputerTakeoverClientClosed)},
	} {
		if _, err := h.store.db.Exec(`INSERT INTO computer_takeover_audit(attempt_id, event_id, event_kind,
			computer_id, job_id, session_id, fabric_id, user_id, device_id, authorized_role, admitted_mode,
			policy_revision, authority_generation, occurred_ns, stored_ns, reason, event_count, request_hash)
			VALUES(?, ?, ?, ?, ?, ?, 'fabric-test', 'person-test', 'device-test', 'view', 'view', 1, 1, ?, ?, ?, 0, ?)`,
			claim.Lease.AttemptID, event.id, event.kind, computer.ComputerID, computer.CurrentJobID, event.sessionID,
			h.clock.Now().UnixNano()+int64(index), h.clock.Now().UnixNano()+int64(index), event.reason, event.id); err != nil {
			t.Fatal(err)
		}
	}
	reserved, _, err := h.store.BeginComputerRestore(context.Background(), computer.ComputerID,
		ComputerRestoreRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"), BackupID: source.BackupID, IdempotencyKey: "heartbeat-revoke"})
	if err != nil {
		t.Fatal(err)
	}
	agentClient := h.client(fabric.Identity{NodeID: "fabric-computer-node", Tags: []string{DefaultAgentPrincipalTag}})
	status, _, body := h.do(agentClient, http.MethodPost, "/v1/agent/nodes/"+node.NodeID+"/heartbeat", heartbeatRequestForNode(node))
	if status != http.StatusInternalServerError {
		t.Fatalf("heartbeat without revoker status=%d body=%s", status, body)
	}
	revocations := 0
	h.server.computerTokenRevoker = recordingComputerTokenRevoker{revoke: func(_ context.Context, request ComputerTokenRevocation) (contract.ComputerTokenRevocationReceipt, error) {
		if request.ComputerID != computer.ComputerID || !request.RevokeAll || request.Reason != "computer_restoring" {
			t.Fatalf("restore revocation = %#v", request)
		}
		revocations++
		return contract.ComputerTokenRevocationReceipt{ComputerID: request.ComputerID,
			SubmitIntentRevision: request.NewSubmitIntentRevision, CommittedAt: h.clock.Now()}, nil
	}}
	for heartbeat := 1; heartbeat <= 2; heartbeat++ {
		status, _, body = h.do(agentClient, http.MethodPost, "/v1/agent/nodes/"+node.NodeID+"/heartbeat", heartbeatRequestForNode(node))
		var response HeartbeatResponse
		if status != http.StatusOK || json.Unmarshal(body, &response) != nil || len(response.StorageCopyDirectives) != 1 {
			t.Fatalf("heartbeat %d status=%d response=%#v body=%s", heartbeat, status, response, body)
		}
		if revocations != 1 {
			t.Fatalf("heartbeat %d revocations=%d", heartbeat, revocations)
		}
	}
	revoked, err := h.store.GetComputer(context.Background(), computer.ComputerID)
	if err != nil || revoked.LastRestoreRevocation == nil ||
		revoked.LastRestoreRevocation.OperationRevision != reserved.IntentRevision ||
		!revoked.LastRestoreRevocation.RevokeAll ||
		revoked.LastRestoreRevocation.TokenRevocation.ComputerID != computer.ComputerID {
		t.Fatalf("restore revocation receipt = %#v err=%v", revoked.LastRestoreRevocation, err)
	}
}

func testRestoreRevocationEvidence(computerID string) ComputerRestoreRevocationEvidence {
	return ComputerRestoreRevocationEvidence{RevokeAll: true, TokenRevocation: contract.ComputerTokenRevocationReceipt{
		ComputerID: computerID, SubmitIntentRevision: 1, CommittedAt: time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC),
	}}
}

func TestRestoreRevocationReceiptRejectsWrongOperationRevision(t *testing.T) {
	h, _, computer, source, _ := publishedBackupForStorageCopy(t, 2)
	reserved, _, err := h.store.BeginComputerRestore(context.Background(), computer.ComputerID,
		ComputerRestoreRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"),
			BackupID: source.BackupID, IdempotencyKey: "revision-bound-revocation"})
	if err != nil {
		t.Fatal(err)
	}
	err = h.store.RecordComputerRestoreAuthorityRevoked(context.Background(), computer.ComputerID,
		reserved.IntentRevision+1, testRestoreRevocationEvidence(computer.ComputerID))
	if errorCode(err) != contract.ErrorStaleIntentRevision {
		t.Fatalf("wrong-revision restore revocation = %v, want stale intent", err)
	}
	var receipt []byte
	if err := h.store.db.QueryRow(`SELECT authority_revocation_receipt_json FROM computer_storage_copy_operations
		WHERE destination_computer_id=? AND operation_revision=?`, computer.ComputerID, reserved.IntentRevision).Scan(&receipt); err != nil {
		t.Fatal(err)
	}
	if len(receipt) != 0 {
		t.Fatalf("wrong-revision revocation populated current receipt: %s", receipt)
	}
}

func TestRestoreRevocationReceiptRejectsEvidenceBeforeReservation(t *testing.T) {
	h, _, computer, source, _ := publishedBackupForStorageCopy(t, 2)
	reserved, _, err := h.store.BeginComputerRestore(context.Background(), computer.ComputerID,
		ComputerRestoreRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"),
			BackupID: source.BackupID, IdempotencyKey: "chronological-revocation"})
	if err != nil {
		t.Fatal(err)
	}
	evidence := testRestoreRevocationEvidence(computer.ComputerID)
	evidence.TokenRevocation.CommittedAt = h.clock.Now().Add(-time.Nanosecond)
	if err := h.store.RecordComputerRestoreAuthorityRevoked(context.Background(), computer.ComputerID,
		reserved.IntentRevision, evidence); errorCode(err) != contract.ErrorConflict {
		t.Fatalf("pre-reservation restore revocation = %v, want conflict", err)
	}
}

func successfulStorageCopyReceipt(directive ComputerStorageCopyDirective) ComputerStorageCopyReceipt {
	destinationDigest := directive.SourceDigest
	if directive.Operation == "clone" {
		destinationDigest = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	}
	return ComputerStorageCopyReceipt{Kind: computerStorageCopyReceiptKind, ReceiptID: "receipt-" + directive.DestinationComputerID,
		Operation: directive.Operation, BackupID: directive.BackupID, CopyID: directive.CopyID,
		ExportID: directive.ExportID, ExternalPath: directive.ExternalPath, ManifestDigest: directive.ManifestDigest,
		SourceComputerID: directive.SourceComputerID, SourceStorageID: directive.SourceStorageID,
		SourceGeneration: directive.SourceGeneration, DestinationComputerID: directive.DestinationComputerID,
		DestinationStorageID: directive.DestinationStorageID, DestinationGeneration: directive.DestinationGeneration,
		NodeID: directive.BoundNodeID, RootInstanceID: directive.RootInstanceID, JobID: directive.JobID,
		OperationRevision: directive.OperationRevision, CleanupFence: directive.CleanupFence, HelperGeneration: 9,
		SourceSize: directive.SourceSize, DestinationSize: directive.DestinationSize,
		SourceDigest: directive.SourceDigest, DestinationDigest: destinationDigest,
		OSIdentityRekeyed: directive.Operation == "clone" || directive.Operation == "import",
		MachineIDBeforeDigest: func() string {
			if directive.Operation == "restore" {
				return ""
			}
			return "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		}(),
		MachineIDAfterDigest: func() string {
			if directive.Operation == "restore" {
				return ""
			}
			return "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
		}(),
		SourceUnchanged: true, DestinationPrepared: true,
		FilesystemExpanded: (directive.Operation == "clone" || directive.Operation == "import") && directive.DestinationSize > directive.SourceSize}
}

func successfulOldGenerationBackupReceipt(directive ComputerStorageCopyDirective) ComputerBackupCopyReceipt {
	return ComputerBackupCopyReceipt{Kind: computerBackupCopyReceiptKind, ReceiptID: "old-" + directive.OldCopyID,
		BackupID: directive.OldBackupID, CopyID: directive.OldCopyID, ComputerID: directive.DestinationComputerID,
		StorageID: directive.DestinationStorageID, StorageGeneration: directive.OldGeneration,
		NodeID: directive.BoundNodeID, RootInstanceID: directive.RootInstanceID, JobID: directive.JobID,
		OperationRevision: directive.OperationRevision, CleanupFence: directive.CleanupFence,
		HelperGeneration: 9, AllocatedSize: directive.DestinationSize,
		ContentDigest: "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
		Encryption:    BackupEncryptionNone}
}

func TestComputerRestorePublishesExactlyOneStoppedGenerationAndKeepsSource(t *testing.T) {
	h, node, computer, source, oldClaim := publishedBackupForStorageCopy(t, 3)
	request := ComputerRestoreRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"),
		BackupID: source.BackupID, KeepOldBackup: true, IdempotencyKey: "restore-1"}
	reserved, replayed, err := h.store.BeginComputerRestore(context.Background(), computer.ComputerID, request)
	if err != nil || replayed {
		t.Fatalf("begin restore = %#v replayed=%t err=%v", reserved, replayed, err)
	}
	if reserved.ComputerID != computer.ComputerID || reserved.StorageID != computer.StorageID ||
		reserved.StorageGeneration != computer.StorageGeneration || reserved.IntentRevision != computer.IntentRevision+1 ||
		reserved.ReconfigurationPhase != ComputerReconfigurationRestoring || reserved.DesiredState != contract.ServiceDesiredStopped {
		t.Fatalf("reserved restore authority = %#v", reserved)
	}
	replayedRestore, replayed, err := h.store.BeginComputerRestore(context.Background(), computer.ComputerID, request)
	if err != nil || !replayed || replayedRestore.IntentRevision != reserved.IntentRevision {
		t.Fatalf("restore replay = %#v replayed=%t err=%v", replayedRestore, replayed, err)
	}
	if _, err := h.store.ProveComputerTokenScope(context.Background(), computer.ComputerID, oldClaim.Lease.AttemptID,
		"fabric-computer-node", ""); errorCode(err) != contract.ErrorForbidden {
		t.Fatalf("old planted credential scope = %v, want forbidden", err)
	}
	revocationEvidence := testRestoreRevocationEvidence(computer.ComputerID)
	if err := h.store.RecordComputerRestoreAuthorityRevoked(context.Background(), computer.ComputerID, reserved.IntentRevision, revocationEvidence); err != nil {
		t.Fatal(err)
	}
	directives, err := h.store.ListNodeComputerStorageCopyDirectives(context.Background(), "fabric-computer-node", node.NodeID, node.BootSessionID)
	if err != nil || len(directives) != 1 || directives[0].DestinationGeneration != computer.StorageGeneration+1 ||
		!directives[0].KeepOldBackup || directives[0].OldBackupID == "" || directives[0].OldCopyID == "" {
		t.Fatalf("restore directive = %#v err=%v", directives, err)
	}
	receipt := successfulStorageCopyReceipt(directives[0])
	oldReceipt := successfulOldGenerationBackupReceipt(directives[0])
	published, err := h.store.AcknowledgeComputerStorageCopy(context.Background(), "fabric-computer-node", computer.ComputerID,
		ComputerStorageCopyAcknowledgementRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID,
			IdempotencyKey: receipt.ReceiptID, Receipt: receipt, OldBackupReceipt: &oldReceipt})
	if err != nil {
		t.Fatal(err)
	}
	if published.StorageGeneration != computer.StorageGeneration+1 || published.StorageID != computer.StorageID ||
		published.DesiredState != contract.ServiceDesiredStopped || published.CurrentJob.State != contract.JobStopped ||
		published.ReconfigurationPhase != ComputerReconfigurationRestoring || published.LastRestoreRevocation == nil ||
		published.LastRestoreRevocation.OperationRevision != reserved.IntentRevision ||
		!published.LastRestoreRevocation.RevokeAll ||
		published.LastRestoreRevocation.TokenRevocation.ComputerID != computer.ComputerID {
		t.Fatalf("published restored generation = %#v", published)
	}
	backups, err := h.store.ListComputerBackups(context.Background(), computer.ComputerID)
	if err != nil || len(backups.Backups) != 2 {
		t.Fatalf("source and precommitted old Backup = %#v err=%v", backups, err)
	}
	if backups.Backups[0].BackupID != source.BackupID && backups.Backups[1].BackupID != source.BackupID {
		t.Fatalf("restore changed source Backup = %#v", backups.Backups)
	}
	retirementRequest := RemovalAcknowledgementRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID,
		RemovalGeneration: uint64(directives[0].OperationRevision), CleanupFence: directives[0].CleanupFence,
		RootInstanceID: directives[0].RootInstanceID, IdempotencyKey: "retired"}
	completed, err := h.store.AcknowledgeComputerRestoreRetirement(context.Background(), "fabric-computer-node", computer.ComputerID,
		retirementRequest)
	if err != nil || completed.ReconfigurationPhase != ComputerReconfigurationStable ||
		completed.AppliedRevision != completed.IntentRevision || completed.DesiredState != contract.ServiceDesiredStopped {
		t.Fatalf("completed restore = %#v err=%v", completed, err)
	}
	if replay, err := h.store.AcknowledgeComputerRestoreRetirement(context.Background(), "fabric-computer-node", computer.ComputerID,
		retirementRequest); err != nil || replay.IntentRevision != completed.IntentRevision {
		t.Fatalf("retirement acknowledgement replay = %#v err=%v", replay, err)
	}
	if _, err := h.store.ProveComputerTokenScope(context.Background(), completed.ComputerID, oldClaim.Lease.AttemptID,
		"fabric-computer-node", ""); errorCode(err) != contract.ErrorForbidden {
		t.Fatalf("copied old restore credential after publication = %v, want forbidden", err)
	}
	generations, err := h.store.ListComputerStorageGenerations(context.Background(), computer.ComputerID)
	if err != nil || len(generations.Generations) != 2 || generations.Generations[0].Phase != ComputerStorageGenerationRetired ||
		generations.Generations[1].Phase != ComputerStorageGenerationCurrent {
		t.Fatalf("restore generation phases = %#v err=%v", generations, err)
	}
	second, replayed, err := h.store.BeginComputerRestore(context.Background(), completed.ComputerID,
		ComputerRestoreRequest{ComputerMutationPrecondition: computerPrecondition(completed, "operator"),
			BackupID: source.BackupID, IdempotencyKey: "restore-2"})
	if err != nil || replayed || second.IntentRevision != completed.IntentRevision+1 {
		t.Fatalf("second restore reservation = %#v replayed=%t err=%v", second, replayed, err)
	}
	if err := h.store.RecordComputerRestoreAuthorityRevoked(context.Background(), completed.ComputerID, second.IntentRevision, testRestoreRevocationEvidence(completed.ComputerID)); err != nil {
		t.Fatal(err)
	}
	directives, err = h.store.ListNodeComputerStorageCopyDirectives(context.Background(), "fabric-computer-node", node.NodeID, node.BootSessionID)
	if err != nil || len(directives) != 1 || directives[0].DestinationGeneration != completed.StorageGeneration+1 {
		t.Fatalf("second restore generation = %#v err=%v", directives, err)
	}
}

func TestComputerRemovalSupersedesAndAttestsRestorePrecommittedBackup(t *testing.T) {
	h, node, computer, source, _ := publishedBackupForStorageCopy(t, 3)
	reserved, _, err := h.store.BeginComputerRestore(context.Background(), computer.ComputerID,
		ComputerRestoreRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"),
			BackupID: source.BackupID, KeepOldBackup: true, IdempotencyKey: "restore-remove"})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.store.RecordComputerRestoreAuthorityRevoked(context.Background(), computer.ComputerID, reserved.IntentRevision, testRestoreRevocationEvidence(computer.ComputerID)); err != nil {
		t.Fatal(err)
	}
	storageCopies, err := h.store.ListNodeComputerStorageCopyDirectives(context.Background(),
		"fabric-computer-node", node.NodeID, node.BootSessionID)
	if err != nil || len(storageCopies) != 1 || storageCopies[0].OldCopyID == "" {
		t.Fatalf("reserved restore precommit = %#v err=%v", storageCopies, err)
	}
	// Model a crash after helper evidence was durably accepted but before L1
	// publication. The precommitted predecessor copy is then known to exist and
	// must not be confused with the operation acknowledgement itself.
	if _, err := h.store.db.Exec(`UPDATE computer_storage_copy_operations SET status='prepared',
		acknowledgement_key='verified-copy', acknowledgement_hash='verified-hash'
		WHERE destination_computer_id=? AND operation_revision=?`, computer.ComputerID, reserved.IntentRevision); err != nil {
		t.Fatal(err)
	}
	removed, err := h.store.RemoveComputer(context.Background(), computer.ComputerID,
		ComputerRemoveRequest{ComputerMutationPrecondition: computerPrecondition(reserved, "operator-remove")})
	if err != nil || removed.DesiredState != contract.ServiceDesiredRemoved {
		t.Fatalf("remove reserved restore = %#v err=%v", removed, err)
	}
	removals, err := h.store.ListNodeRemovalDirectives(context.Background(),
		"fabric-computer-node", node.NodeID, node.BootSessionID)
	if err != nil || len(removals) != 1 || removals[0].ComputerBackupCopies == nil {
		t.Fatalf("restore composite removal = %#v err=%v", removals, err)
	}
	var foundPrecommit bool
	for _, copy := range removals[0].ComputerBackupCopies.Copies {
		if copy.CopyID == storageCopies[0].OldCopyID {
			foundPrecommit = copy.Superseded && copy.BackupID == storageCopies[0].OldBackupID
		}
		if _, err := h.store.AcknowledgeComputerBackupPrune(context.Background(), "fabric-computer-node", computer.ComputerID,
			ComputerBackupPruneAcknowledgementRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID,
				IdempotencyKey: "removed-" + copy.CopyID, Receipt: backupRemovalReceipt(copy)}); err != nil {
			t.Fatalf("acknowledge composite copy %q: %v", copy.CopyID, err)
		}
	}
	if !foundPrecommit {
		t.Fatalf("composite removal omitted precommitted restore Backup: %#v", removals[0].ComputerBackupCopies.Copies)
	}
	serviceReceipt := RemovalAcknowledgementRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID,
		RemovalGeneration: removals[0].RemovalGeneration, CleanupFence: removals[0].CleanupFence,
		RootInstanceID: removals[0].RootInstanceID, IdempotencyKey: "restore-service-removed"}
	if _, err := h.store.AcknowledgeServiceRemoval(context.Background(), "fabric-computer-node", removals[0].JobID, serviceReceipt); err != nil {
		t.Fatalf("service removal after all restore copies absent: %v", err)
	}
}

func TestComputerCloneCreatesNewStoppedIdentityWithoutGrantsAndExpands(t *testing.T) {
	h, node, sourceComputer, sourceBackup, oldClaim := publishedBackupForStorageCopy(t, 2)
	if _, _, err := h.store.BeginComputerClone(context.Background(), ComputerCloneRequest{BackupID: sourceBackup.BackupID,
		ComputerMutationPrecondition: computerPrecondition(sourceComputer, "operator"), Name: "too-small", DiskBytes: sourceBackup.AllocatedSize - 1, IdempotencyKey: "small", Actor: "operator"}); errorCode(err) != contract.ErrorConflict {
		t.Fatalf("smaller clone error = %v, want conflict", err)
	}
	clone, replayed, err := h.store.BeginComputerClone(context.Background(), ComputerCloneRequest{BackupID: sourceBackup.BackupID,
		ComputerMutationPrecondition: computerPrecondition(sourceComputer, "operator"), Name: "clone-computer", DiskBytes: sourceBackup.AllocatedSize + (1 << 20), IdempotencyKey: "clone-1", Actor: "operator"})
	if err != nil || replayed {
		t.Fatalf("begin clone = %#v replayed=%t err=%v", clone, replayed, err)
	}
	if clone.ComputerID == sourceComputer.ComputerID || clone.StorageID == sourceComputer.StorageID ||
		clone.Name != "clone-computer" || len(clone.Grants) != 0 || clone.DesiredState != contract.ServiceDesiredStopped ||
		clone.CurrentJob.State != contract.JobStopped || clone.ReconfigurationPhase != ComputerReconfigurationCloning ||
		clone.CurrentJob.Spec.DispatchKey == sourceComputer.CurrentJob.Spec.DispatchKey {
		t.Fatalf("staged clone identity = %#v", clone)
	}
	directives, err := h.store.ListNodeComputerStorageCopyDirectives(context.Background(), "fabric-computer-node", node.NodeID, node.BootSessionID)
	if err != nil || len(directives) != 1 || directives[0].Operation != "clone" ||
		directives[0].DestinationSize <= directives[0].SourceSize {
		t.Fatalf("clone directive = %#v err=%v", directives, err)
	}
	receipt := successfulStorageCopyReceipt(directives[0])
	completed, err := h.store.AcknowledgeComputerStorageCopy(context.Background(), "fabric-computer-node", clone.ComputerID,
		ComputerStorageCopyAcknowledgementRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID,
			IdempotencyKey: receipt.ReceiptID, Receipt: receipt})
	if err != nil || completed.ReconfigurationPhase != ComputerReconfigurationStable || completed.AppliedRevision != 1 ||
		completed.DesiredState != contract.ServiceDesiredStopped || len(completed.Grants) != 0 {
		t.Fatalf("completed clone = %#v err=%v", completed, err)
	}
	if _, err := h.store.ProveComputerTokenScope(context.Background(), completed.ComputerID, oldClaim.Lease.AttemptID,
		"fabric-computer-node", ""); errorCode(err) != contract.ErrorForbidden {
		t.Fatalf("copied old clone credential = %v, want forbidden", err)
	}
	unchanged, err := h.store.GetComputer(context.Background(), sourceComputer.ComputerID)
	if err != nil || unchanged.StorageID != sourceComputer.StorageID || unchanged.StorageGeneration != sourceComputer.StorageGeneration ||
		unchanged.IntentRevision != sourceComputer.IntentRevision {
		t.Fatalf("clone changed source Computer = %#v err=%v", unchanged, err)
	}
	var kind, destinationComputerID, destinationStorageID string
	var destinationGeneration int64
	if err := h.store.db.QueryRow(`SELECT kind, destination_computer_id, destination_storage_id, destination_generation
		FROM storage_provenance WHERE backup_id=? AND kind='clone'`, sourceBackup.BackupID).Scan(
		&kind, &destinationComputerID, &destinationStorageID, &destinationGeneration); err != nil {
		t.Fatal(err)
	}
	if kind != "clone" || destinationComputerID != clone.ComputerID || destinationStorageID != clone.StorageID || destinationGeneration != 1 {
		t.Fatalf("clone Storage provenance = %q/%q/%q/%d", kind, destinationComputerID, destinationStorageID, destinationGeneration)
	}
	projection, err := h.store.ListComputerStorageProvenance(context.Background(), clone.ComputerID)
	var observedReceipt *ComputerStorageCopyReceipt
	for _, provenance := range projection.Provenance {
		if provenance.Kind == "clone" {
			observedReceipt = provenance.CopyReceipt
		}
	}
	if err != nil || observedReceipt == nil || observedReceipt.ReceiptID != receipt.ReceiptID {
		t.Fatalf("clone receipt provenance = %#v err=%v", projection.Provenance, err)
	}
}

func TestComputerCloneCustodyRemovalReducesThenCoordinatedRemovalVerifies(t *testing.T) {
	h, node, sourceComputer, sourceBackup, _ := publishedBackupForStorageCopy(t, 2)
	clone, _, err := h.store.BeginComputerClone(context.Background(), ComputerCloneRequest{BackupID: sourceBackup.BackupID,
		ComputerMutationPrecondition: computerPrecondition(sourceComputer, "operator"), Name: "custody-clone", DiskBytes: sourceBackup.AllocatedSize, IdempotencyKey: "custody-clone", Actor: "operator"})
	if err != nil {
		t.Fatal(err)
	}
	directives, err := h.store.ListNodeComputerStorageCopyDirectives(context.Background(), "fabric-computer-node", node.NodeID, node.BootSessionID)
	if err != nil || len(directives) != 1 {
		t.Fatalf("custody clone directive = %#v err=%v", directives, err)
	}
	receipt := successfulStorageCopyReceipt(directives[0])
	clone, err = h.store.AcknowledgeComputerStorageCopy(context.Background(), "fabric-computer-node", clone.ComputerID,
		ComputerStorageCopyAcknowledgementRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID,
			IdempotencyKey: receipt.ReceiptID, Receipt: receipt})
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
		t.Fatalf("single clone-branch removal = %#v err=%v", clone, err)
	}

	tx, err = h.store.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`UPDATE computers SET removal_outcome='removal_pending' WHERE computer_id=?`, sourceComputer.ComputerID); err != nil {
		t.Fatal(err)
	}
	if err := finalizeComputerCustodyOutcome(context.Background(), tx, sourceComputer.ComputerID, h.clock.Now()); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	sourceComputer, err = h.store.GetComputer(context.Background(), sourceComputer.ComputerID)
	if err != nil || sourceComputer.RemovalOutcome != "removed_verified" {
		t.Fatalf("final source-branch removal = %#v err=%v", sourceComputer, err)
	}
	clone, err = h.store.GetComputer(context.Background(), clone.ComputerID)
	if err != nil || clone.RemovalOutcome != "removed_verified" {
		t.Fatalf("coordinated branch upgrade = %#v err=%v", clone, err)
	}
}

func TestComputerStorageCopyReceiptMutationRowsFailTwentyOfTwenty(t *testing.T) {
	h, node, sourceComputer, sourceBackup, _ := publishedBackupForStorageCopy(t, 2)
	clone, _, err := h.store.BeginComputerClone(context.Background(), ComputerCloneRequest{BackupID: sourceBackup.BackupID,
		ComputerMutationPrecondition: computerPrecondition(sourceComputer, "operator"), Name: "mutation-clone", DiskBytes: sourceBackup.AllocatedSize, IdempotencyKey: "mutation", Actor: "operator"})
	if err != nil {
		t.Fatal(err)
	}
	directives, err := h.store.ListNodeComputerStorageCopyDirectives(context.Background(), "fabric-computer-node", node.NodeID, node.BootSessionID)
	if err != nil || len(directives) != 1 {
		t.Fatalf("mutation directive = %#v err=%v", directives, err)
	}
	valid := successfulStorageCopyReceipt(directives[0])
	mutations := []func(*ComputerStorageCopyReceipt){
		func(r *ComputerStorageCopyReceipt) { r.Kind = "wrong" },
		func(r *ComputerStorageCopyReceipt) { r.ReceiptID = "" },
		func(r *ComputerStorageCopyReceipt) { r.Operation = "restore" },
		func(r *ComputerStorageCopyReceipt) { r.BackupID = "wrong" },
		func(r *ComputerStorageCopyReceipt) { r.CopyID = "wrong" },
		func(r *ComputerStorageCopyReceipt) { r.SourceComputerID = "wrong" },
		func(r *ComputerStorageCopyReceipt) { r.SourceStorageID = "wrong" },
		func(r *ComputerStorageCopyReceipt) { r.SourceGeneration++ },
		func(r *ComputerStorageCopyReceipt) { r.DestinationComputerID = "wrong" },
		func(r *ComputerStorageCopyReceipt) { r.DestinationStorageID = "wrong" },
		func(r *ComputerStorageCopyReceipt) { r.DestinationGeneration++ },
		func(r *ComputerStorageCopyReceipt) { r.NodeID = "wrong" },
		func(r *ComputerStorageCopyReceipt) { r.RootInstanceID = "wrong" },
		func(r *ComputerStorageCopyReceipt) { r.JobID = "wrong" },
		func(r *ComputerStorageCopyReceipt) { r.OperationRevision++ },
		func(r *ComputerStorageCopyReceipt) { r.CleanupFence = "wrong" },
		func(r *ComputerStorageCopyReceipt) { r.HelperGeneration = 0 },
		func(r *ComputerStorageCopyReceipt) { r.SourceSize++ },
		func(r *ComputerStorageCopyReceipt) { r.DestinationSize++ },
		func(r *ComputerStorageCopyReceipt) { r.SourceDigest = "sha256:short" },
		func(r *ComputerStorageCopyReceipt) { r.DestinationDigest = "sha256:short" },
		func(r *ComputerStorageCopyReceipt) { r.OSIdentityRekeyed = false },
		func(r *ComputerStorageCopyReceipt) { r.MachineIDBeforeDigest = "sha256:short" },
		func(r *ComputerStorageCopyReceipt) { r.MachineIDAfterDigest = r.MachineIDBeforeDigest },
		func(r *ComputerStorageCopyReceipt) { r.SourceUnchanged = false },
		func(r *ComputerStorageCopyReceipt) { r.DestinationPrepared = false },
		func(r *ComputerStorageCopyReceipt) { r.PreparationReceipt = true },
		func(r *ComputerStorageCopyReceipt) { r.DestinationChown = true },
		func(r *ComputerStorageCopyReceipt) { r.FilesystemExpanded = true },
	}
	if len(mutations) != 29 {
		t.Fatalf("negative rows = %d, want 29", len(mutations))
	}
	for index, mutate := range mutations {
		receipt := valid
		mutate(&receipt)
		if _, err := h.store.AcknowledgeComputerStorageCopy(context.Background(), "fabric-computer-node", clone.ComputerID,
			ComputerStorageCopyAcknowledgementRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID,
				IdempotencyKey: "negative", Receipt: receipt}); err == nil {
			t.Errorf("negative receipt row %d was accepted", index)
		}
	}
	stillStaged, err := h.store.GetComputer(context.Background(), clone.ComputerID)
	if err != nil || stillStaged.ReconfigurationPhase != ComputerReconfigurationCloning {
		t.Fatalf("negative rows changed clone = %#v err=%v", stillStaged, err)
	}
}

func TestRestoreAndCloneFailClosedOnRunningAttachedAndStaleAuthority(t *testing.T) {
	t.Run("restore after runtime stop", func(t *testing.T) {
		h, node, computer, source, _ := publishedBackupForStorageCopy(t, 2)
		resumed, err := h.store.SetComputerDesiredState(context.Background(), computer.ComputerID,
			computerDesiredRequest(computer, contract.ServiceDesiredRunning, "resume-before-restore"))
		if err != nil {
			t.Fatal(err)
		}
		resumed, claim := startBackupComputer(t, h, node, resumed)
		if _, err := h.store.SetComputerDesiredState(context.Background(), resumed.ComputerID,
			computerDesiredRequest(resumed, contract.ServiceDesiredStopped, "stop-before-restore")); err != nil {
			t.Fatal(err)
		}
		finishBackupQuiescence(t, h, claim, "restore-source-detached")
		stopped, err := h.store.GetComputer(context.Background(), resumed.ComputerID)
		if err != nil || stopped.CurrentJob.State != contract.JobStopped || stopped.CurrentJob.CurrentAttemptID != "" {
			t.Fatalf("runtime-stopped restore source = %#v err=%v", stopped, err)
		}
		if _, replayed, err := h.store.BeginComputerRestore(context.Background(), stopped.ComputerID,
			ComputerRestoreRequest{ComputerMutationPrecondition: computerPrecondition(stopped, "operator"), BackupID: source.BackupID, IdempotencyKey: "runtime-stopped"}); err != nil || replayed {
			t.Fatalf("runtime-stopped restore replayed=%t err=%v", replayed, err)
		}
	})
	t.Run("restore after unverified runtime stop", func(t *testing.T) {
		h, node, computer, source, _ := publishedBackupForStorageCopy(t, 2)
		resumed, err := h.store.SetComputerDesiredState(context.Background(), computer.ComputerID,
			computerDesiredRequest(computer, contract.ServiceDesiredRunning, "resume-before-unverified-stop"))
		if err != nil {
			t.Fatal(err)
		}
		resumed, claim := startBackupComputer(t, h, node, resumed)
		if _, err := h.store.SetComputerDesiredState(context.Background(), resumed.ComputerID,
			computerDesiredRequest(resumed, contract.ServiceDesiredStopped, "unverified-stop-before-restore")); err != nil {
			t.Fatal(err)
		}
		if _, err := h.store.CompleteAttempt(context.Background(), "fabric-computer-node", claim.Job.JobID,
			claim.Lease.AttemptID, CompletionRequest{FencingToken: claim.Lease.FencingToken,
				IdempotencyKey: "restore-source-unverified", Result: ProcessResult{OutputError: "runtime detach was not proven"}}); err != nil {
			t.Fatal(err)
		}
		failed, err := h.store.GetComputer(context.Background(), resumed.ComputerID)
		if err != nil || failed.CurrentJob.State != contract.JobFailed || failed.CurrentJob.CurrentAttemptID != claim.Lease.AttemptID {
			t.Fatalf("unverified runtime-stopped restore source = %#v err=%v", failed, err)
		}
		if _, _, err := h.store.BeginComputerRestore(context.Background(), failed.ComputerID,
			ComputerRestoreRequest{ComputerMutationPrecondition: computerPrecondition(failed, "operator"), BackupID: source.BackupID, IdempotencyKey: "unverified-runtime-stop"}); errorCode(err) != contract.ErrorConflict {
			t.Fatalf("unverified runtime-stopped restore error = %v, want %q", err, contract.ErrorConflict)
		}
	})
	t.Run("restore running", func(t *testing.T) {
		h, _, computer, source, _ := publishedBackupForStorageCopy(t, 2)
		if _, err := h.store.db.Exec(`UPDATE computers SET desired_state='running' WHERE computer_id=?`, computer.ComputerID); err != nil {
			t.Fatal(err)
		}
		if _, _, err := h.store.BeginComputerRestore(context.Background(), computer.ComputerID,
			ComputerRestoreRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"), BackupID: source.BackupID, IdempotencyKey: "running"}); errorCode(err) != contract.ErrorConflict {
			t.Fatalf("running restore error = %v", err)
		}
	})
	t.Run("restore attached", func(t *testing.T) {
		h, _, computer, source, claim := publishedBackupForStorageCopy(t, 2)
		if _, err := h.store.db.Exec(`UPDATE jobs SET current_attempt_id=? WHERE job_id=?`, claim.Lease.AttemptID, computer.CurrentJobID); err != nil {
			t.Fatal(err)
		}
		if _, _, err := h.store.BeginComputerRestore(context.Background(), computer.ComputerID,
			ComputerRestoreRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"), BackupID: source.BackupID, IdempotencyKey: "attached"}); errorCode(err) != contract.ErrorConflict {
			t.Fatalf("attached restore error = %v", err)
		}
	})
	t.Run("restore while reconfiguring", func(t *testing.T) {
		h, _, computer, source, _ := publishedBackupForStorageCopy(t, 2)
		if _, err := h.store.db.Exec(`UPDATE computers SET reconfiguration_phase=? WHERE computer_id=?`,
			ComputerReconfigurationReimaging, computer.ComputerID); err != nil {
			t.Fatal(err)
		}
		if _, _, err := h.store.BeginComputerRestore(context.Background(), computer.ComputerID,
			ComputerRestoreRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"), BackupID: source.BackupID, IdempotencyKey: "reconfiguring"}); errorCode(err) != contract.ErrorConflict {
			t.Fatalf("reconfiguring restore error = %v", err)
		}
	})
	t.Run("restore stale revision", func(t *testing.T) {
		h, _, computer, source, _ := publishedBackupForStorageCopy(t, 2)
		precondition := computerPrecondition(computer, "operator")
		precondition.IntentRevision--
		if _, _, err := h.store.BeginComputerRestore(context.Background(), computer.ComputerID,
			ComputerRestoreRequest{ComputerMutationPrecondition: precondition, BackupID: source.BackupID, IdempotencyKey: "stale"}); errorCode(err) != contract.ErrorStaleIntentRevision {
			t.Fatalf("stale restore error = %v", err)
		}
	})
	t.Run("clone stale source revision", func(t *testing.T) {
		h, _, computer, source, _ := publishedBackupForStorageCopy(t, 2)
		precondition := computerPrecondition(computer, "operator")
		precondition.IntentRevision--
		if _, _, err := h.store.BeginComputerClone(context.Background(), ComputerCloneRequest{
			ComputerMutationPrecondition: precondition, BackupID: source.BackupID, Name: "stale-clone",
			DiskBytes: source.AllocatedSize, IdempotencyKey: "stale-clone", Actor: "operator"}); errorCode(err) != contract.ErrorStaleIntentRevision {
			t.Fatalf("stale clone source error = %v", err)
		}
	})
}

func TestComputerStorageCopyAcknowledgementRequiresBoundNodeSessionAndRoot(t *testing.T) {
	h, node, sourceComputer, sourceBackup, _ := publishedBackupForStorageCopy(t, 2)
	clone, _, err := h.store.BeginComputerClone(context.Background(), ComputerCloneRequest{BackupID: sourceBackup.BackupID,
		ComputerMutationPrecondition: computerPrecondition(sourceComputer, "operator"), Name: "authority-clone", DiskBytes: sourceBackup.AllocatedSize, IdempotencyKey: "authority", Actor: "operator"})
	if err != nil {
		t.Fatal(err)
	}
	directives, err := h.store.ListNodeComputerStorageCopyDirectives(context.Background(), "fabric-computer-node", node.NodeID, node.BootSessionID)
	if err != nil || len(directives) != 1 {
		t.Fatalf("authority directive = %#v err=%v", directives, err)
	}
	receipt := successfulStorageCopyReceipt(directives[0])
	request := ComputerStorageCopyAcknowledgementRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID,
		IdempotencyKey: receipt.ReceiptID, Receipt: receipt}
	if _, err := h.store.AcknowledgeComputerStorageCopy(context.Background(), "different-fabric-node", clone.ComputerID, request); err == nil {
		t.Fatal("different Fabric node acknowledged Computer Storage copy")
	}
	wrongBoot := request
	wrongBoot.BootSessionID = "stale-boot"
	if _, err := h.store.AcknowledgeComputerStorageCopy(context.Background(), "fabric-computer-node", clone.ComputerID, wrongBoot); err == nil {
		t.Fatal("stale boot session acknowledged Computer Storage copy")
	}
	wrongRoot := request
	wrongRoot.Receipt.RootInstanceID = "stale-root"
	if _, err := h.store.AcknowledgeComputerStorageCopy(context.Background(), "fabric-computer-node", clone.ComputerID, wrongRoot); err == nil {
		t.Fatal("stale managed-root instance acknowledged Computer Storage copy")
	}
	completed, err := h.store.AcknowledgeComputerStorageCopy(context.Background(), "fabric-computer-node", clone.ComputerID, request)
	if err != nil || completed.ReconfigurationPhase != ComputerReconfigurationStable {
		t.Fatalf("current bound acknowledgement = %#v err=%v", completed, err)
	}
}

func TestComputerRestoreRejectsFailedPredecessorBackupBeforeSwitchover(t *testing.T) {
	h, node, computer, source, _ := publishedBackupForStorageCopy(t, 3)
	reserved, _, err := h.store.BeginComputerRestore(context.Background(), computer.ComputerID,
		ComputerRestoreRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"),
			BackupID: source.BackupID, KeepOldBackup: true, IdempotencyKey: "failed-old-copy"})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.store.RecordComputerRestoreAuthorityRevoked(context.Background(), computer.ComputerID, reserved.IntentRevision, testRestoreRevocationEvidence(computer.ComputerID)); err != nil {
		t.Fatal(err)
	}
	directives, err := h.store.ListNodeComputerStorageCopyDirectives(context.Background(), "fabric-computer-node", node.NodeID, node.BootSessionID)
	if err != nil || len(directives) != 1 {
		t.Fatalf("restore directives = %#v err=%v", directives, err)
	}
	copyReceipt := successfulStorageCopyReceipt(directives[0])
	oldReceipt := successfulOldGenerationBackupReceipt(directives[0])
	oldReceipt.Kind = computerBackupFailureReceiptKind
	oldReceipt.ContentDigest = ""
	oldReceipt.FailureCode = "insufficient_disk"
	oldReceipt.CopyAbsent = true
	if _, err := h.store.AcknowledgeComputerStorageCopy(context.Background(), "fabric-computer-node", computer.ComputerID,
		ComputerStorageCopyAcknowledgementRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID,
			IdempotencyKey: copyReceipt.ReceiptID, Receipt: copyReceipt, OldBackupReceipt: &oldReceipt}); errorCode(err) != contract.ErrorConflict {
		t.Fatalf("failed predecessor receipt error = %v, want typed conflict", err)
	}
	unchanged, err := h.store.GetComputer(context.Background(), computer.ComputerID)
	if err != nil || unchanged.StorageGeneration != computer.StorageGeneration || unchanged.IntentRevision != reserved.IntentRevision ||
		unchanged.ReconfigurationPhase != ComputerReconfigurationStable || unchanged.AppliedRevision != reserved.IntentRevision {
		t.Fatalf("failed predecessor receipt switched authority = %#v err=%v", unchanged, err)
	}
	var status string
	if err := h.store.db.QueryRow(`SELECT status FROM computer_storage_copy_operations WHERE destination_computer_id=?`, computer.ComputerID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "failed" {
		t.Fatalf("failed predecessor receipt operation status = %q", status)
	}
	if _, err := h.store.db.Exec(`UPDATE backups SET content_digest='not-a-digest' WHERE backup_id=?`, source.BackupID); err == nil {
		t.Fatal("Backups table accepted a malformed content digest")
	}
}

func TestRestorePublicationRequiresDurableRetriedAuthorityRevocation(t *testing.T) {
	h, node, computer, source, _ := publishedBackupForStorageCopy(t, 2)
	reserved, _, err := h.store.BeginComputerRestore(context.Background(), computer.ComputerID,
		ComputerRestoreRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"), BackupID: source.BackupID, IdempotencyKey: "revoke-first"})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.store.RecordComputerRestoreAuthorityRevoked(context.Background(), computer.ComputerID, reserved.IntentRevision, testRestoreRevocationEvidence(computer.ComputerID)); err != nil {
		t.Fatal(err)
	}
	directives, err := h.store.ListNodeComputerStorageCopyDirectives(context.Background(), "fabric-computer-node", node.NodeID, node.BootSessionID)
	if err != nil || len(directives) != 1 {
		t.Fatalf("restore directives = %#v err=%v", directives, err)
	}
	receipt := successfulStorageCopyReceipt(directives[0])
	if _, err := h.store.db.Exec(`UPDATE computer_storage_copy_operations SET authority_revocation_receipt_json=NULL WHERE destination_computer_id=?`, computer.ComputerID); err != nil {
		t.Fatal(err)
	}
	if directives, err := h.store.ListNodeComputerStorageCopyDirectives(context.Background(), "fabric-computer-node", node.NodeID, node.BootSessionID); err != nil || len(directives) != 0 {
		t.Fatalf("legacy timestamp-only revocation admitted helper directive = %#v err=%v", directives, err)
	}
	request := ComputerStorageCopyAcknowledgementRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID,
		IdempotencyKey: receipt.ReceiptID, Receipt: receipt}
	if _, err := h.store.AcknowledgeComputerStorageCopy(context.Background(), "fabric-computer-node", computer.ComputerID, request); errorCode(err) != contract.ErrorConflict {
		t.Fatalf("publication without durable revocation = %v", err)
	}
	if err := h.store.RecordComputerRestoreAuthorityRevoked(context.Background(), computer.ComputerID, reserved.IntentRevision, testRestoreRevocationEvidence(computer.ComputerID)); err != nil {
		t.Fatal(err)
	}
	if err := h.store.RecordComputerRestoreAuthorityRevoked(context.Background(), computer.ComputerID, reserved.IntentRevision, testRestoreRevocationEvidence(computer.ComputerID)); err != nil {
		t.Fatalf("reissued revocation was not idempotent: %v", err)
	}
	published, err := h.store.AcknowledgeComputerStorageCopy(context.Background(), "fabric-computer-node", computer.ComputerID, request)
	if err != nil || published.StorageGeneration != computer.StorageGeneration+1 {
		t.Fatalf("publish after revocation = %#v err=%v", published, err)
	}
}

func TestRemovingCloneSourceSupersedesReservedForkAndCannotVerifyCustody(t *testing.T) {
	h, node, sourceComputer, sourceBackup, _ := publishedBackupForStorageCopy(t, 2)
	clone, _, err := h.store.BeginComputerClone(context.Background(), ComputerCloneRequest{
		ComputerMutationPrecondition: computerPrecondition(sourceComputer, "operator"), BackupID: sourceBackup.BackupID,
		Name: "inflight-custody", DiskBytes: sourceBackup.AllocatedSize, IdempotencyKey: "inflight-custody", Actor: "operator"})
	if err != nil {
		t.Fatal(err)
	}
	removed, err := h.store.RemoveComputer(context.Background(), sourceComputer.ComputerID,
		ComputerRemoveRequest{ComputerMutationPrecondition: computerPrecondition(sourceComputer, "operator-remove")})
	if err != nil {
		t.Fatal(err)
	}
	var status string
	if err := h.store.db.QueryRow(`SELECT status FROM computer_storage_copy_operations WHERE destination_computer_id=?`, clone.ComputerID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "superseded" {
		t.Fatalf("source removal left clone operation %q", status)
	}
	removals, err := h.store.ListNodeRemovalDirectives(context.Background(), "fabric-computer-node", node.NodeID, node.BootSessionID)
	if err != nil || len(removals) != 1 {
		t.Fatalf("source removal directives = %#v err=%v", removals, err)
	}
	foundDestinationFence := false
	for _, generation := range removals[0].ComputerStorageGenerations.Generations {
		if generation.ComputerID == clone.ComputerID && generation.StorageID == clone.StorageID {
			foundDestinationFence = true
		}
	}
	if !foundDestinationFence {
		t.Fatalf("source removal omitted durable destination staging fence: %#v", removals[0].ComputerStorageGenerations)
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
		t.Fatalf("in-flight fork custody outcome = %#v err=%v", removed, err)
	}
}
