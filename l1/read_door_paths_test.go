package l1

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
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

func TestAuthorityLossReadUsesClosedProtocolSnapshot(t *testing.T) {
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
		row, err := h.store.owedComputerRevocation(t.Context(), stopped.owedRevocationID)
		if err != nil || row.owed.RevocationID != stopped.owedRevocationID {
			t.Fatalf("row=%+v err=%v", row, err)
		}
	})
	called := false
	h.server.computerTokenRevoker = recordingComputerTokenRevoker{revoke: func(ctx context.Context, request ComputerTokenRevocation) (contract.ComputerTokenRevocationReceipt, error) {
		called = true
		if used := h.store.db.Stats().InUse + h.store.readDB.Stats().InUse; used != 0 {
			t.Fatalf("external revocation retained %d snapshot slots", used)
		}
		return contract.ComputerTokenRevocationReceipt{ComputerID: computer.ComputerID, SubmitIntentRevision: 1, CommittedAt: h.clock.Now()}, nil
	}}
	if err := h.server.revokeAfterAuthorityLoss(t.Context(), stopped.owedRevocationID, computer.ComputerID, "", "computer_stopped"); err != nil || !called {
		t.Fatalf("called=%t err=%v", called, err)
	}
}

// Exercise the real agent HTTP route while every operator snapshot slot is held.
func TestComputerCompletionIndependentOfOperatorPool(t *testing.T) {
	h, _, node, agent := computerCompletionHarness(t)
	ledger := newOutageLedger(h)
	computer, _, err := h.store.CreateComputer(t.Context(), CreateComputerRequest{Name: "completion-admission", Spec: computerCapabilityJobSpec("computer:completion-admission"), Actor: "operator"})
	if err != nil {
		t.Fatal(err)
	}
	claim := startComputerAttempt(t, h, node, nil)
	ledger.mint(claim.Lease.AttemptID)
	occupyOperatorPool(t, h.store)
	postCompletion(t, h, agent, claim, CompletionRequest{FencingToken: claim.Lease.FencingToken, IdempotencyKey: "completion-admission", Result: ProcessResult{ExitCode: intPointer(0)}, RuntimeQuiescenceEvidence: RuntimeQuiescenceAttempt})
	audit := owedRevocationAuditRows(t, h, computer.ComputerID)
	if len(audit) != 1 || audit[0].verb != ComputerRevocationVerbAttemptCompletion || audit[0].scope != ComputerRevocationScopeAttempt || len(audit[0].recorded) != 1 || audit[0].recorded[0] != claim.Lease.AttemptID || audit[0].settlement != owedRevocationSettledRevoked || audit[0].record == nil || len(audit[0].record.Attempts) != 1 || audit[0].record.Attempts[0].ComputerAttemptID != claim.Lease.AttemptID || ledger.active(claim.Lease.AttemptID) {
		t.Fatalf("completion revocation audit=%+v active=%t", audit, ledger.active(claim.Lease.AttemptID))
	}
	requests := ledger.taken()
	if len(requests) != 1 || requests[0].ComputerAttemptID != claim.Lease.AttemptID || requests[0].RevokeAll {
		t.Fatalf("revocations=%+v", requests)
	}
}

// A protocol-door timeout must describe the committed change even before L3 is called.
func TestAuthorityLossReadExpiryMarksApplied(t *testing.T) {
	for _, route := range []string{"completion", "stop"} {
		t.Run(route, func(t *testing.T) {
			h, _, node, _ := computerCompletionHarness(t)
			computer, _, err := h.store.CreateComputer(t.Context(), CreateComputerRequest{Name: "expiry-" + route, Spec: computerCapabilityJobSpec("computer:expiry-" + route), Actor: "operator"})
			if err != nil {
				t.Fatal(err)
			}
			claim := startComputerAttempt(t, h, node, nil)
			computer = mustGetComputer(t, h, computer.ComputerID)
			h.stopServer()
			called := false
			h.server.computerTokenRevoker = recordingComputerTokenRevoker{revoke: func(context.Context, ComputerTokenRevocation) (contract.ComputerTokenRevocationReceipt, error) {
				called = true
				return contract.ComputerTokenRevocationReceipt{}, nil
			}}
			ctx := context.WithValue(t.Context(), readSnapshotHardLimitContextKey{}, time.Nanosecond)
			var payload any
			method, path := http.MethodPost, fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/complete", claim.Job.JobID, claim.Lease.AttemptID)
			identity := fabric.Identity{NodeID: "fabric-computer-node", Tags: []string{DefaultAgentPrincipalTag}}
			handler := h.server.completeAttempt
			if route == "completion" {
				payload = CompletionRequest{FencingToken: claim.Lease.FencingToken, IdempotencyKey: "expiry-completion", Result: ProcessResult{ExitCode: intPointer(0)}, RuntimeQuiescenceEvidence: RuntimeQuiescenceAttempt}
			} else {
				payload = computerDesiredRequest(computer, contract.ServiceDesiredStopped, "operator")
				method, path = http.MethodPut, "/v1/computers/"+computer.ComputerID+"/desired-state"
				identity = fabric.Identity{NodeID: "operator", Tags: []string{DefaultClientPrincipalTag}}
				handler = h.server.setComputerDesiredState
			}
			encoded, err := json.Marshal(payload)
			if err != nil {
				t.Fatal(err)
			}
			r := httptest.NewRequest(method, path, strings.NewReader(string(encoded))).WithContext(context.WithValue(ctx, identityContextKey{}, identity))
			r.SetPathValue("computer_id", computer.ComputerID)
			r.SetPathValue("job_id", claim.Job.JobID)
			r.SetPathValue("attempt_id", claim.Lease.AttemptID)
			w := httptest.NewRecorder()
			handler(w, r)
			var response struct {
				Error contract.APIError `json:"error"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
				t.Fatal(err)
			}
			api := response.Error
			if w.Code != http.StatusServiceUnavailable || api.Code != contract.ErrorUnavailable || !api.Retryable || api.Details["mutation_applied"] != true || api.Details["computer_id"] != computer.ComputerID || api.Details["reason"] != "read_snapshot_post_change_failed" || api.Details["read_reason"] != "read_snapshot_admission_expired" || called {
				t.Fatalf("status=%d body=%s revoker_called=%t", w.Code, w.Body.String(), called)
			}
			audit := owedRevocationAuditRows(t, h, computer.ComputerID)
			if len(audit) != 1 || audit[0].settlement != "" || len(audit[0].recorded) != 1 || audit[0].recorded[0] != claim.Lease.AttemptID {
				t.Fatalf("owed audit=%+v", audit)
			}
			// The write itself succeeded, independently of the failed observation.
			job, err := h.store.GetJob(t.Context(), claim.Job.JobID)
			if err != nil {
				t.Fatal(err)
			}
			if route == "completion" && job.State == contract.JobRunning {
				t.Fatalf("completion did not commit: %+v", job)
			}
			if route == "stop" && job.DesiredState != contract.ServiceDesiredStopped {
				t.Fatalf("stop did not commit: %+v", job)
			}
		})
	}
}
