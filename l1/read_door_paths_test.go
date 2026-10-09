package l1

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

func occupyOperatorPool(t *testing.T, store *Store) {
	t.Helper()
	var held []*sql.Conn
	t.Cleanup(func() {
		for _, conn := range held {
			conn.Close()
		}
	})
	for range readSnapshotLimit {
		conn, err := store.readDB.Conn(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, conn)
	}
}

func TestProtocolProofsIndependentOfOperatorPool(t *testing.T) {
	h := newIntegrationHarnessWithOptions(t, StoreOptions{LeaseDuration: 2 * time.Second}, map[string]NodePolicy{
		"computer-node": DefaultNodePolicy(contract.StableNodeTagPrefix + "computer-node"),
	})
	h.stopServer()
	ctx := t.Context()
	node := registerCapabilityNodeWithTags(t, h, "computer-node", map[string]bool{
		"kind:oci": true, "cgroup_v2": true, "computer": true,
	}, []string{contract.StableNodeTagPrefix + "computer-node"})
	admin := fabric.Identity{FabricID: "fabric-test", UserID: "admin", DeviceID: "device-1"}
	challenge, err := h.store.InitiateAdminBootstrap(ctx)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := h.store.BootstrapAdmin(ctx, admin, challenge.Nonce)
	if err != nil {
		t.Fatal(err)
	}
	computer, _, err := h.store.CreateComputer(ctx, CreateComputerRequest{Name: "scope-proof",
		Spec: computerCapabilityJobSpec("computer:scope-proof"), Actor: "operator"})
	if err != nil {
		t.Fatal(err)
	}
	computer, _, _, err = h.store.MutateComputerSubmission(ctx, admin, computer.ComputerID, ComputerSubmissionRequest{
		PolicyRevision: policy.Revision, SubmitIntentRevision: 0, SubmitEnabled: boolPointer(true),
		IdempotencyKey: "scope-enable",
	})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := h.store.ClaimJob(ctx, "fabric-computer-node", node.NodeID, node.BootSessionID, contract.JobClassService)
	if err != nil || claim == nil {
		t.Fatalf("claim = (%#v, %v)", claim, err)
	}
	if _, err := h.store.ProveComputerTokenScope(ctx, computer.ComputerID, claim.Lease.AttemptID,
		"fabric-computer-node", ""); errorCode(err) != contract.ErrorForbidden {
		t.Fatalf("scope proof before installed policy = %v", err)
	}
	snapshot, err := h.store.IssueComputerPolicySnapshot(ctx, "fabric-computer-node", "fabric-test",
		node.NodeID, node.BootSessionID, time.Minute)
	if err != nil || snapshot == nil {
		t.Fatalf("snapshot = (%#v, %v)", snapshot, err)
	}
	if err := h.store.AcknowledgeComputerPolicyInstallation(ctx, "fabric-computer-node", acknowledgementFor(*snapshot)); err != nil {
		t.Fatal(err)
	}

	occupyOperatorPool(t, h.store)
	t.Run("binding", func(t *testing.T) {
		bound, err := h.store.ProveServiceBinding(ctx, "fabric-computer-node", computer.CurrentJobID, ServiceBindingProofRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID})
		if err != nil || !bound {
			t.Fatalf("bound=%t err=%v", bound, err)
		}
	})
	t.Run("token-scope", func(t *testing.T) {
		proof, err := h.store.ProveComputerTokenScope(ctx, computer.ComputerID, claim.Lease.AttemptID, "fabric-computer-node", "")
		if err != nil || proof.ComputerID != computer.ComputerID {
			t.Fatalf("proof=%+v err=%v", proof, err)
		}
	})
	t.Run("host-boot", func(t *testing.T) {
		if err := h.store.ProveHostBootSession(ctx, "fabric-computer-node", node.NodeID, node.BootSessionID); err != nil {
			t.Fatal(err)
		}
	})
}

func TestCustodyAttestationReloadFailureMarksApplied(t *testing.T) {
	h, node, computer, backup, _ := publishedBackupForStorageCopy(t, 2)
	h.stopServer()
	exported, _ := beginCustodyExport(t, h, node, computer, backup, "applied-attestation")
	occupyOperatorPool(t, h.store)
	request := ComputerCustodyAttestationRequest{IdempotencyKey: "attested", Actor: "operator"}
	_, replayed, err := h.store.AttestComputerCustodyDeletedWithReplay(t.Context(), exported.ExportID, request)
	api := apiErrorFromDecision(err)
	if replayed || api == nil || api.Code != contract.ErrorUnavailable || !api.Retryable || api.Details["mutation_applied"] != true || api.Details["export_id"] != exported.ExportID || api.Details["read_reason"] != "read_snapshot_admission_expired" {
		t.Fatalf("replayed=%t api=%+v err=%v", replayed, api, err)
	}
	var key, actor string
	if err := h.store.db.QueryRowContext(t.Context(), `SELECT operator_attestation_key,operator_attestation_actor FROM computer_custody_exports WHERE export_id=?`, exported.ExportID).Scan(&key, &actor); err != nil || key != request.IdempotencyKey || actor != request.Actor {
		t.Fatalf("evidence key=%q actor=%q err=%v", key, actor, err)
	}
	_, replayed, err = h.store.AttestComputerCustodyDeletedWithReplay(t.Context(), exported.ExportID, request)
	if !replayed || apiErrorFromDecision(err).Details["mutation_applied"] != true {
		t.Fatalf("replay marker=%t err=%v", replayed, err)
	}
}

func TestReimageReplaySnapshotExpiryRemainsRetryable(t *testing.T) {
	h, _, computer := backupHarness(t, 2, nil)
	h.stopServer()
	var seq int
	var name, path string
	if err := h.store.db.QueryRowContext(t.Context(), "PRAGMA database_list").Scan(&seq, &name, &path); err != nil {
		t.Fatal(err)
	}
	original := h.store.readDB
	// The real driver blocks precisely at replay lookup until the read door expires.
	reader := sql.OpenDB(bindingSnapshotConnector{driver: original.Driver(), dsn: sqliteDSN(path, sqliteBusyTimeout), beforeQuery: func(query string) error {
		if strings.Contains(query, "computer_reimage_operations") {
			time.Sleep(250 * time.Millisecond)
		}
		return nil
	}})
	h.store.readDB = reader
	defer func() { h.store.readDB = original; reader.Close() }()
	_, err := h.store.ReimageComputer(t.Context(), computer.ComputerID, ComputerReimageRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"), IdempotencyKey: "expiry", Image: reimageTarget('b')})
	assertSnapshotUnavailable(t, err, "read_snapshot_expired")
}

func TestUnpinnedWriteUsesOneClock(t *testing.T) {
	store, clock := snapshotStore(t)
	_, _, err := store.CreateJob(t.Context(), operatorServiceSpec("one-write-clock", nil))
	if err != nil || clock.calls.Load() != 1 {
		t.Fatalf("write clock calls=%d err=%v", clock.calls.Load(), err)
	}
	// Force the real completion-marker write, rather than the already-done path.
	if _, err := store.db.ExecContext(t.Context(), "DELETE FROM l1_data_migrations WHERE name=?", logEventDocumentMigration); err != nil {
		t.Fatal(err)
	}
	clock.calls.Store(0)
	if _, err := store.CompactLogEventDocuments(t.Context()); err != nil || clock.calls.Load() != 1 {
		t.Fatalf("compaction clock calls=%d err=%v", clock.calls.Load(), err)
	}
}

func TestCustodyImportAcknowledgementUsesOneWrite(t *testing.T) {
	h, node, source, backup, _ := publishedBackupForStorageCopy(t, 2)
	h.stopServer()
	exported, directive := beginCustodyExport(t, h, node, source, backup, "snapshot-import")
	receipt := successfulCustodyExportReceipt(directive)
	_, err := h.store.AcknowledgeComputerCustodyExport(t.Context(), "fabric-computer-node", source.ComputerID, ComputerCustodyExportAcknowledgementRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID, IdempotencyKey: receipt.ReceiptID, Receipt: receipt})
	if err != nil {
		t.Fatal(err)
	}
	op, _, err := h.store.BeginComputerCustodyImport(t.Context(), exported.ExportID, ComputerCustodyImportRequest{Name: "snapshot-import", DiskBytes: backup.AllocatedSize, NodeID: node.NodeID, ExternalPath: directive.ExternalPath, Manifest: custodyManifest(directive), ManifestDigest: receipt.ManifestDigest, IdempotencyKey: "snapshot-import", Actor: "operator"})
	if err != nil {
		t.Fatal(err)
	}
	directives, err := h.store.ListNodeComputerStorageCopyDirectives(t.Context(), "fabric-computer-node", node.NodeID, node.BootSessionID)
	if err != nil || len(directives) != 1 {
		t.Fatalf("directives: %+v %v", directives, err)
	}
	failure := successfulStorageCopyReceipt(directives[0])
	failure.Kind = "computer_storage_copy_failed_absent"
	failure.Operation = "import"
	failure.DestinationDigest = ""
	failure.OSIdentityRekeyed = false
	failure.MachineIDBeforeDigest = ""
	failure.MachineIDAfterDigest = ""
	failure.SourceUnchanged = false
	failure.DestinationPrepared = false
	failure.FilesystemExpanded = false
	failure.FailureCode = "manifest_invalid"
	failure.DestinationAbsent = true

	clock := &snapshotClock{}
	clock.at.Store(h.clock.Now().UnixNano())
	h.store.clock = clock
	_, err = h.store.AcknowledgeComputerStorageCopy(t.Context(), "fabric-computer-node", op.DestinationComputerID, ComputerStorageCopyAcknowledgementRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID, IdempotencyKey: failure.ReceiptID, Receipt: failure})
	if err != nil || clock.calls.Load() != 1 {
		t.Fatalf("import write clock samples=%d err=%v", clock.calls.Load(), err)
	}
	// The settlement committed, rather than merely returning an empty authority.
	var status string
	if err := h.store.db.QueryRowContext(t.Context(), "SELECT status FROM computer_storage_copy_operations WHERE destination_computer_id=?", op.DestinationComputerID).Scan(&status); err != nil || status != "failed" {
		t.Fatalf("status=%q err=%v", status, err)
	}
}

func TestAuthorityLossReadUsesClosedOperatorSnapshot(t *testing.T) {
	h := newIntegrationHarnessWithPolicies(t, map[string]NodePolicy{})
	h.stopServer()
	computer, _, err := h.store.CreateComputer(t.Context(), CreateComputerRequest{Name: "authority-read", Spec: computerCapabilityJobSpec("computer:authority-read"), Actor: "operator"})
	if err != nil {
		t.Fatal(err)
	}
	stopped, err := h.store.SetComputerDesiredState(t.Context(), computer.ComputerID, computerDesiredRequest(computer, contract.ServiceDesiredStopped, "operator"))
	if err != nil || stopped.owedRevocationID == 0 {
		t.Fatalf("stopped=%+v err=%v", stopped, err)
	}
	t.Run("operator-admission", func(t *testing.T) {
		occupyOperatorPool(t, h.store)
		_, err := h.store.owedComputerRevocation(t.Context(), stopped.owedRevocationID)
		assertSnapshotUnavailable(t, err, "read_snapshot_admission_expired")
	})
	called := false
	h.server.computerTokenRevoker = recordingComputerTokenRevoker{revoke: func(ctx context.Context, request ComputerTokenRevocation) (contract.ComputerTokenRevocationReceipt, error) {
		called = true
		if used := h.store.readDB.Stats().InUse; used != 0 {
			t.Fatalf("external revocation retained %d snapshot slots", used)
		}
		return contract.ComputerTokenRevocationReceipt{ComputerID: computer.ComputerID, SubmitIntentRevision: 1, CommittedAt: h.clock.Now()}, nil
	}}
	if err := h.server.revokeAfterAuthorityLoss(t.Context(), stopped.owedRevocationID, computer.ComputerID, "", "computer_stopped"); err != nil || !called {
		t.Fatalf("called=%t err=%v", called, err)
	}
}
