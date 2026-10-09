package l1

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

func computerSnapshotAdmin(t *testing.T, h *integrationHarness) (fabric.Identity, AdminPolicy) {
	t.Helper()
	admin := fabric.Identity{FabricID: "fabric-test", UserID: "snapshot-admin", DeviceID: "snapshot-admin-device"}
	challenge, err := h.store.InitiateAdminBootstrap(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	policy, err := h.store.BootstrapAdmin(t.Context(), admin, challenge.Nonce)
	if err != nil {
		t.Fatal(err)
	}
	return admin, policy
}

// A real failed import removes the provisional authority and its intents. The
// observation barrier commits that failure between existence and history reads.
func TestComputerSnapshotImportFailureBetweenIntentReads(t *testing.T) {
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
	before, err := h.store.ListComputerIntents(t.Context(), op.DestinationComputerID, "", 10)
	if err != nil || len(before.Intents) == 0 {
		t.Fatalf("before: %+v %v", before, err)
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
	ctx, fired := observeOnce(t, t.Context(), "computer_intents_owner", func() {
		_, err := h.store.AcknowledgeComputerStorageCopy(t.Context(), "fabric-computer-node", op.DestinationComputerID, ComputerStorageCopyAcknowledgementRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID, IdempotencyKey: failure.ReceiptID, Receipt: failure})
		if err != nil {
			t.Fatal(err)
		}
	})
	page, err := h.store.ListComputerIntents(ctx, op.DestinationComputerID, "", 10)
	if !*fired || err != nil || !reflect.DeepEqual(page, before) {
		t.Fatalf("mixed import history: fired=%v page=%+v err=%v", *fired, page, err)
	}
	if _, err := h.store.ListComputerIntents(t.Context(), op.DestinationComputerID, "", 10); errorCode(err) != contract.ErrorNotFound {
		t.Fatalf("deleted intents: %v", err)
	}
	if _, err := h.store.GetComputer(t.Context(), op.DestinationComputerID); errorCode(err) != contract.ErrorNotFound {
		t.Fatalf("deleted Computer: %v", err)
	}
	observed, err := h.store.GetComputerCustodyImport(t.Context(), op.ImportID)
	if err != nil || observed.Status != "failed" {
		t.Fatalf("retained import: %+v %v", observed, err)
	}
}

func TestComputerSnapshotGrantBetweenRevisionAndRows(t *testing.T) {
	h, _, computer := backupHarness(t, 2, nil)
	h.stopServer()
	admin, policy := computerSnapshotAdmin(t, h)
	viewer := fabric.Identity{FabricID: admin.FabricID, UserID: "snapshot-viewer", DeviceID: "snapshot-viewer-device"}
	if _, err := h.store.ObserveAuthenticatedPerson(t.Context(), viewer); err != nil {
		t.Fatal(err)
	}
	granted, err := h.store.MutateComputerGrant(t.Context(), admin, computer.ComputerID, viewer.UserID, ComputerGrantMutationRequest{PolicyRevision: policy.Revision, Permission: ComputerGrantView, IdempotencyKey: "snapshot-view"})
	if err != nil {
		t.Fatal(err)
	}
	before, err := h.store.ListComputerGrants(t.Context(), admin, computer.ComputerID)
	if err != nil {
		t.Fatal(err)
	}
	ctx, fired := observeOnce(t, t.Context(), "computer_grants_revision", func() {
		_, err := h.store.MutateComputerGrant(t.Context(), admin, computer.ComputerID, viewer.UserID, ComputerGrantMutationRequest{PolicyRevision: granted.Grant.PolicyRevision, Permission: ComputerGrantControl, IdempotencyKey: "snapshot-control"})
		if err != nil {
			t.Fatal(err)
		}
	})
	page, err := h.store.ListComputerGrants(ctx, admin, computer.ComputerID)
	if !*fired || err != nil || !reflect.DeepEqual(page, before) {
		t.Fatalf("mixed grants: fired=%v page=%+v err=%v", *fired, page, err)
	}
	next, err := h.store.ListComputerGrants(t.Context(), admin, computer.ComputerID)
	if err != nil || next.PolicyRevision != before.PolicyRevision+1 || len(next.Grants) != 1 || next.Grants[0].PolicyRevision != next.PolicyRevision || next.Grants[0].Permission != ComputerGrantControl {
		t.Fatalf("grant did not commit: %+v %v", next, err)
	}
}

func TestComputerSnapshotBackupCreatedBetweenIDsAndOperation(t *testing.T) {
	h, node, computer, _, _ := publishedBackupForStorageCopy(t, 3)
	h.stopServer()
	before, err := h.store.ListComputerBackups(t.Context(), computer.ComputerID)
	if err != nil {
		t.Fatal(err)
	}
	ctx, fired := observeOnce(t, t.Context(), "computer_backups_ids", func() {
		_, _, err := h.store.BeginComputerBackup(t.Context(), computer.ComputerID, ComputerBackupCreateRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"), IdempotencyKey: "snapshot-new-backup"})
		if err != nil {
			t.Fatal(err)
		}
		directives, err := h.store.ListNodeComputerBackupDirectives(t.Context(), "fabric-computer-node", node.NodeID, node.BootSessionID)
		if err != nil || len(directives) != 1 {
			t.Fatalf("directives=%+v %v", directives, err)
		}
		acknowledgeBackup(t, h, node, directives[0], successfulBackupReceipt(directives[0]))
	})
	page, err := h.store.ListComputerBackups(ctx, computer.ComputerID)
	if !*fired || err != nil || !reflect.DeepEqual(page, before) {
		t.Fatalf("mixed Backup view: fired=%v page=%+v err=%v", *fired, page, err)
	}
	next, err := h.store.ListComputerBackups(t.Context(), computer.ComputerID)
	if err != nil || len(next.Backups) != 2 || next.LastOperation.BackupID == before.LastOperation.BackupID {
		t.Fatalf("Backup not committed: %+v %v", next, err)
	}
}

func computerSnapshotRequest(ctx context.Context, computerID string) *http.Request {
	ctx = context.WithValue(ctx, identityContextKey{}, fabric.Identity{NodeID: "operator", Tags: []string{DefaultClientPrincipalTag}})
	r := httptest.NewRequest(http.MethodGet, "/v1/computers/"+computerID, nil).WithContext(ctx)
	r.SetPathValue("computer_id", computerID)
	return r
}

func TestComputerSnapshotStatusParity(t *testing.T) {
	for _, scenario := range []string{"restart-pending", "unschedulable"} {
		t.Run(scenario, func(t *testing.T) {
			h, _, computer := backupHarness(t, 2, nil)
			h.stopServer()
			admin, _ := computerSnapshotAdmin(t, h)
			if scenario == "restart-pending" {
				_, err := h.store.db.Exec(`UPDATE service_jobs SET bound_node_id='computer-node', next_restart_at=? WHERE job_id=?`, h.clock.Now().Add(time.Hour).UnixNano(), computer.CurrentJobID)
				if err != nil {
					t.Fatal(err)
				}
			} else {
				_, err := h.store.db.Exec(`UPDATE nodes SET capabilities_json='{}' WHERE node_id='computer-node'`)
				if err != nil {
					t.Fatal(err)
				}
			}
			var job Job
			err := diagnosticReadSnapshot(t, h.store, nil, func(reads readModel) error {
				var err error
				job, err = reads.job(t.Context(), computer.CurrentJobID)
				if err != nil {
					return err
				}
				job, err = reads.projectStatus(t.Context(), job)
				return err
			})
			if err != nil || job.Status != scenario {
				t.Fatalf("job=%+v %v", job, err)
			}

			t.Run("detail", func(t *testing.T) {
				got, err := h.store.GetComputer(t.Context(), computer.ComputerID)
				if err != nil || got.CurrentJob.Status != job.Status {
					t.Fatalf("detail status=%s want=%s err=%v", got.CurrentJob.Status, job.Status, err)
				}
			})
			t.Run("listing", func(t *testing.T) {
				listed, err := h.store.ListComputers(t.Context(), "", 10)
				if err != nil || len(listed.Computers) != 1 || listed.Computers[0].CurrentJob.Status != job.Status {
					t.Fatalf("listing=%+v %v", listed, err)
				}
			})
			t.Run("post-change", func(t *testing.T) {
				computer.CurrentJob.Status = "stale"
				w := httptest.NewRecorder()
				h.server.writeComputerForCaller(w, computerSnapshotRequest(t.Context(), computer.ComputerID), http.StatusOK, computer)
				var changed Computer
				if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &changed) != nil || changed.CurrentJob.Status != job.Status {
					t.Fatalf("post-change=%d %s", w.Code, w.Body.String())
				}
			})
			t.Run("submission", func(t *testing.T) {
				submission, err := h.store.GetComputerSubmissionState(t.Context(), admin, computer.ComputerID)
				if err != nil || submission.Status != job.Status {
					t.Fatalf("submission=%+v %v", submission, err)
				}
			})
			t.Run("submission-change", func(t *testing.T) {
				h.server.computerTokenRevoker = recordingComputerTokenRevoker{count: func(context.Context, string) (int, error) {
					if h.store.readDB.Stats().InUse != 0 {
						t.Fatal("snapshot held across L3")
					}
					return 7, nil
				}}
				result, err := h.server.computerSubmissionResult(t.Context(), admin, computer, true, nil)
				if err != nil || result.Status != job.Status || result.InflightCount == nil || *result.InflightCount != 7 || result.InflightObservation != "run-ledger" {
					t.Fatalf("mutation submission=%+v %v", result, err)
				}
			})

		})
	}
}

// Retained immutable rows permit inexpensive equal-time/clock-rollback probes.
func seedComputerSnapshotBackups(t *testing.T, h *integrationHarness, template Backup, n int, createdNS int64) {
	t.Helper()
	tx, err := h.store.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("snapshot-backup-%d-%d", createdNS, i)
		provenance := "provenance-" + id
		_, err := tx.Exec(`INSERT INTO backups(backup_id,computer_id,source_storage_id,source_generation,created_ns,allocated_size,content_digest,encryption,provenance_id,status)
 VALUES(?,?,?,?,?,?,?,?,?,'available')`, id, template.ComputerID, template.SourceStorageID, template.SourceGeneration, createdNS, template.AllocatedSize, template.ContentDigest, template.Encryption, provenance)
		if err != nil {
			t.Fatal(err)
		}
		_, err = tx.Exec(`INSERT INTO storage_provenance(provenance_id,kind,source_storage_id,source_generation,backup_id,created_ns) VALUES(?,'backup',?,?,?,?)`, provenance, template.SourceStorageID, template.SourceGeneration, id, createdNS)
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestComputerSnapshotBackupAdaptivePaging(t *testing.T) {
	h, _, computer, backup, _ := publishedBackupForStorageCopy(t, 2)
	h.stopServer()
	seedComputerSnapshotBackups(t, h, backup, 5, 1)
	ctx := context.WithValue(t.Context(), readPageCutoffContextKey{}, time.Nanosecond)
	cursor := ""
	seen := map[string]bool{}
	for i := 0; ; i++ {
		if i > 6 {
			t.Fatal("cursor failed to advance")
		}
		page, err := h.store.ListComputerBackupsPage(ctx, computer.ComputerID, cursor, "", 1000000)
		if err != nil || len(page.Backups) != 1 {
			t.Fatalf("page=%+v err=%v", page, err)
		}
		id := page.Backups[0].BackupID
		if strings.HasPrefix(id, "snapshot-backup-2-") {
			t.Fatal("mid-walk insert passed the Backup watermark: " + id)
		}
		if seen[id] {
			t.Fatal("duplicate " + id)
		}
		seen[id] = true
		if i == 0 {
			seedComputerSnapshotBackups(t, h, backup, 2, 2)
		} // new rows sort after the continuation: only the watermark excludes them
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	if len(seen) != 6 {
		t.Fatalf("membership=%v", seen)
	}
	fresh, err := h.store.ListComputerBackupsPage(context.WithValue(t.Context(), readPageCutoffContextKey{}, time.Hour), computer.ComputerID, "", "", 1000000)
	if err != nil || len(fresh.Backups) != 8 || fresh.NextCursor != "" {
		t.Fatalf("fresh=%+v %v", fresh, err)
	}
	if _, err := h.store.ListComputerBackupsPage(t.Context(), "other", cursor, "", 1); errorCode(err) != contract.ErrorInvalidRequest {
		t.Fatalf("foreign cursor=%v", err)
	}
	if _, err := h.store.ListComputerBackupsPage(t.Context(), computer.ComputerID, "invalid", "", 1); errorCode(err) != contract.ErrorInvalidRequest {
		t.Fatalf("invalid cursor=%v", err)
	}
}

func TestComputerSnapshotWriterHeld(t *testing.T) {
	h, _, computer := backupHarness(t, 2, nil)
	h.stopServer()
	admin, _ := computerSnapshotAdmin(t, h)
	tx, err := h.store.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	prior := h.store.db.Stats().MaxOpenConnections
	h.store.db.SetMaxOpenConns(1)
	defer h.store.db.SetMaxOpenConns(prior)
	if _, err := tx.Exec(`UPDATE computers SET updated_ns=updated_ns WHERE computer_id=?`, computer.ComputerID); err != nil {
		t.Fatal(err)
	}
	probes := []struct {
		name string
		read func() error
	}{
		{"detail", func() error { _, err := h.store.GetComputer(t.Context(), computer.ComputerID); return err }},
		{"listing", func() error { _, err := h.store.ListComputers(t.Context(), "", 10); return err }},
		{"intents", func() error {
			_, err := h.store.ListComputerIntents(t.Context(), computer.ComputerID, "", 10)
			return err
		}},
		{"backups", func() error { _, err := h.store.ListComputerBackups(t.Context(), computer.ComputerID); return err }},
		{"generations", func() error {
			_, err := h.store.ListComputerStorageGenerations(t.Context(), computer.ComputerID)
			return err
		}},
		{"provenance", func() error {
			_, err := h.store.ListComputerStorageProvenance(t.Context(), computer.ComputerID)
			return err
		}},
		{"exports", func() error {
			_, err := h.store.ListComputerCustodyExports(t.Context(), computer.ComputerID)
			return err
		}},
		{"grants", func() error {
			_, err := h.store.ListComputerGrants(t.Context(), admin, computer.ComputerID)
			return err
		}},
		{"grant audit", func() error {
			_, err := h.store.ListComputerPolicyAudit(t.Context(), admin, computer.ComputerID, "", 10)
			return err
		}},
		{"sessions", func() error {
			_, err := h.store.ListComputerTakeoverSessions(t.Context(), admin, computer.ComputerID)
			return err
		}},
		{"take-over audit", func() error {
			_, err := h.store.ListComputerTakeoverAudit(t.Context(), admin, computer.ComputerID, "", 10, false)
			return err
		}},
		{"submission", func() error {
			_, err := h.store.GetComputerSubmissionState(t.Context(), admin, computer.ComputerID)
			return err
		}},
	}
	for _, probe := range probes {
		t.Run(probe.name, func(t *testing.T) {
			done := make(chan error, 1)
			go func() { done <- probe.read() }()
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("read waited on write pool")
			}
		})
	}
}

func TestComputerSnapshotListingPinsOneClock(t *testing.T) {
	h, _, _ := backupHarness(t, 2, nil)
	h.stopServer()
	for i := 0; i < 2; i++ {
		_, _, err := h.store.CreateComputer(t.Context(), CreateComputerRequest{Name: fmt.Sprintf("clock-%d", i), Spec: computerCapabilityJobSpec(fmt.Sprintf("computer:clock-%d", i)), Actor: "operator"})
		if err != nil {
			t.Fatal(err)
		}
	}
	clock := &snapshotClock{}
	clock.at.Store(h.clock.Now().UnixNano())
	h.store.clock = clock
	page, err := h.store.ListComputers(context.WithValue(t.Context(), readPageCutoffContextKey{}, time.Hour), "", 100)
	if err != nil || len(page.Computers) != 3 || clock.calls.Load() != 1 {
		t.Fatalf("rows=%d clock=%d err=%v", len(page.Computers), clock.calls.Load(), err)
	}
}

func TestComputerSnapshotPostChangeFailureCommitStands(t *testing.T) {
	h, _, computer := backupHarness(t, 2, nil)
	h.stopServer()
	if err := h.store.readDB.Close(); err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(ComputerBackupCapRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"), BackupCap: 3})
	if err != nil {
		t.Fatal(err)
	}
	base := computerSnapshotRequest(t.Context(), computer.ComputerID)
	r := httptest.NewRequest(http.MethodPut, "/v1/computers/"+computer.ComputerID+"/backup-cap", strings.NewReader(string(payload))).WithContext(base.Context())
	r.SetPathValue("computer_id", computer.ComputerID)
	w := httptest.NewRecorder()
	h.server.setComputerBackupCap(w, r)
	var envelope contract.ErrorResponse
	if w.Code != http.StatusServiceUnavailable || json.Unmarshal(w.Body.Bytes(), &envelope) != nil || envelope.Error.Code != contract.ErrorUnavailable || envelope.Error.Retryable || envelope.Error.Details["reason"] != "read_snapshot_post_change_failed" || envelope.Error.Details["mutation_applied"] != true || envelope.Error.Details["computer_id"] != computer.ComputerID {
		t.Fatalf("post-change failure=%d %s", w.Code, w.Body.String())
	}
	var cap int64
	if err := h.store.db.QueryRow(`SELECT backup_cap FROM computers WHERE computer_id=?`, computer.ComputerID).Scan(&cap); err != nil || cap != 3 {
		t.Fatalf("commit cap=%d err=%v", cap, err)
	}
}

func TestComputerSnapshotNamedChangeUsesOneMoment(t *testing.T) {
	for _, verb := range []string{"backup", "restore", "clone"} {
		t.Run(verb, func(t *testing.T) {
			h, _, computer, backup, _ := publishedBackupForStorageCopy(t, 3)
			h.stopServer()
			if verb == "clone" {
				if _, err := h.store.db.Exec(`UPDATE nodes SET capabilities_json='{}' WHERE node_id='computer-node'`); err != nil {
					t.Fatal(err)
				}
			}
			clock := &snapshotClock{}
			clock.at.Store(h.clock.Now().UnixNano())
			h.store.clock = clock
			var body any
			switch verb {
			case "backup":
				body = ComputerBackupCreateRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"), IdempotencyKey: "snapshot-named-backup"}
			case "restore":
				body = ComputerRestoreRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"), IdempotencyKey: "snapshot-named-restore"}
			case "clone":
				body = ComputerCloneRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"), Name: "snapshot-named-clone", DiskBytes: backup.AllocatedSize, IdempotencyKey: "snapshot-named-clone"}
			}
			payload, err := json.Marshal(body)
			if err != nil {
				t.Fatal(err)
			}
			base := computerSnapshotRequest(t.Context(), computer.ComputerID)
			r := httptest.NewRequest(http.MethodPost, "/v1/computers/"+computer.ComputerID+"/backups", strings.NewReader(string(payload))).WithContext(base.Context())
			r.SetPathValue("computer_id", computer.ComputerID)
			r.SetPathValue("backup_id", backup.BackupID)
			w := httptest.NewRecorder()
			switch verb {
			case "backup":
				h.server.createComputerBackup(w, r)
			case "restore":
				h.server.restoreComputerBackup(w, r)
			case "clone":
				h.server.cloneComputerBackup(w, r)
			}
			var changed Computer
			if w.Code != http.StatusAccepted || json.Unmarshal(w.Body.Bytes(), &changed) != nil {
				t.Fatalf("response=%d %s", w.Code, w.Body.String())
			}
			// One write clock and one response clock. Key selection is in that response's snapshot.
			if calls := clock.calls.Load(); calls != 2 {
				t.Fatalf("clock reads=%d, want one write and one response", calls)
			}
			var job Job
			err = diagnosticReadSnapshot(t, h.store, nil, func(reads readModel) error {
				var err error
				job, err = reads.job(t.Context(), changed.CurrentJobID)
				if err != nil {
					return err
				}
				job, err = reads.projectStatus(t.Context(), job)
				return err
			})
			if err != nil || changed.CurrentJob.Status != job.Status || changed.CurrentJob.ServiceJob.NodeState != job.ServiceJob.NodeState {
				t.Fatalf("change status=%s node=%s job status=%s node=%s err=%v", changed.CurrentJob.Status, changed.CurrentJob.ServiceJob.NodeState, job.Status, job.ServiceJob.NodeState, err)
			}
			header := map[string]string{"backup": "Backup-Operation-Revision", "restore": "Restore-Operation-Revision", "clone": "Clone-Operation-Revision"}[verb]
			if w.Header().Get(header) == "" {
				t.Fatalf("missing %s", header)
			}
		})
	}
}
