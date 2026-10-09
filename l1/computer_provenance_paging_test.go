package l1

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

func provenanceReviewPage(t *testing.T, h *integrationHarness, computerID, cursor string, limit int, ctx context.Context) (ComputerStorageProvenance, string) {
	t.Helper()
	r := computerSnapshotRequest(ctx, computerID)
	q := r.URL.Query()
	q.Set("cursor", cursor)
	q.Set("limit", strconv.Itoa(limit))
	r.URL.RawQuery = q.Encode()
	w := httptest.NewRecorder()
	h.server.listComputerStorageProvenance(w, r)
	var page struct {
		ComputerStorageProvenance
		Next string `json:"next_cursor"`
	}
	if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &page) != nil {
		t.Fatalf("provenance page=%d %s", w.Code, w.Body.String())
	}
	return page.ComputerStorageProvenance, page.Next
}

func TestComputerProvenanceAdaptivePagingWatermark(t *testing.T) {
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
		page, next := provenanceReviewPage(t, h, computer.ComputerID, cursor, 1000000, ctx)
		if len(page.Provenance) != 1 {
			t.Fatalf("adaptive page size=%d", len(page.Provenance))
		}
		id := page.Provenance[0].ProvenanceID
		if strings.HasPrefix(id, "provenance-snapshot-backup-2-") {
			t.Fatal("mid-walk insert passed the provenance watermark: " + id)
		}
		if seen[id] {
			t.Fatal("duplicate " + id)
		}
		seen[id] = true
		if i == 0 {
			seedComputerSnapshotBackups(t, h, backup, 2, 2)
		} // After the cursor, but earlier than the original wall clock.
		if next == "" {
			break
		}
		cursor = next
	}
	if len(seen) != 6 {
		t.Fatalf("membership=%v", seen)
	}
	page, next := provenanceReviewPage(t, h, computer.ComputerID, "", 1000000, context.WithValue(t.Context(), readPageCutoffContextKey{}, time.Hour))
	if len(page.Provenance) != 8 || next != "" {
		t.Fatalf("fresh membership=%d cursor=%s", len(page.Provenance), next)
	}
	for _, bad := range []struct {
		id, cursor string
		limit      int
	}{{"other", cursor, 1}, {computer.ComputerID, "invalid", 1}, {computer.ComputerID, "", 0}} {
		r := computerSnapshotRequest(t.Context(), bad.id)
		q := r.URL.Query()
		q.Set("cursor", bad.cursor)
		q.Set("limit", strconv.Itoa(bad.limit))
		r.URL.RawQuery = q.Encode()
		w := httptest.NewRecorder()
		h.server.listComputerStorageProvenance(w, r)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("invalid cursor/limit=%d %s", w.Code, w.Body.String())
		}
	}
}

func TestComputerProvenanceOver1000RowsPages(t *testing.T) {
	h, _, computer, backup, _ := publishedBackupForStorageCopy(t, 2)
	h.stopServer()
	seedComputerSnapshotBackups(t, h, backup, 1001, 1)
	ctx := context.WithValue(t.Context(), readPageCutoffContextKey{}, time.Hour)
	cursor := ""
	total := 0
	for {
		page, next := provenanceReviewPage(t, h, computer.ComputerID, cursor, 1000000, ctx)
		if len(page.Provenance) > 250 {
			t.Fatal("unclamped page")
		}
		total += len(page.Provenance)
		if next == "" {
			break
		}
		cursor = next
	}
	if total != 1002 {
		t.Fatalf("inventory=%d", total)
	}
}

func TestComputerBoundedCustodyQueryDeclared(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "storage_provenance.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, decl := range file.Decls {
		if g, ok := decl.(*ast.GenDecl); ok && g.Tok == token.CONST {
			for _, spec := range g.Specs {
				v := spec.(*ast.ValueSpec)
				for i, name := range v.Names {
					if name.Name == "boundedComputerCustodyGraph" {
						lit, ok := v.Values[i].(*ast.BasicLit)
						if !ok {
							t.Fatal("bounded graph must be declared explicitly")
						}
						query, _ := strconv.Unquote(lit.Value)
						if !strings.Contains(query, "LIMIT 1001\n)") {
							t.Fatal("recursive graph lacks explicit limit")
						}
						return
					}
				}
			}
		}
	}
	t.Fatal("bounded recursive graph constant is missing")
}

func seedReviewCustodyChain(t *testing.T, h *integrationHarness, backup Backup, n int, kind string, created int64) {
	t.Helper()
	tx, err := h.store.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	source := backup.SourceStorageID
	for i := 0; i < n; i++ {
		dest := fmt.Sprintf("review-storage-%d", i)
		_, err = tx.Exec(`INSERT INTO storage_provenance(provenance_id,kind,source_storage_id,source_generation,backup_id,destination_storage_id,destination_generation,created_ns) VALUES(?,?,?,1,?,?,1,?)`, fmt.Sprintf("review-edge-%d", i), kind, source, backup.BackupID, dest, created)
		if err != nil {
			t.Fatal(err)
		}
		source = dest
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestComputerCustodyGraphLimitPermanent(t *testing.T) {
	h, _, computer, backup, _ := publishedBackupForStorageCopy(t, 2)
	h.stopServer()
	seedReviewCustodyChain(t, h, backup, 1000, "clone", 1)
	r := computerSnapshotRequest(t.Context(), computer.ComputerID)
	w := httptest.NewRecorder()
	h.server.listComputerStorageProvenance(w, r)
	var envelope contract.ErrorResponse
	if w.Code != http.StatusConflict || json.Unmarshal(w.Body.Bytes(), &envelope) != nil || envelope.Error.Code != contract.ErrorConflict || envelope.Error.Retryable || envelope.Error.Details["reason"] != "storage_custody_limit" {
		t.Fatalf("graph bound=%d %s", w.Code, w.Body.String())
	}
}

func TestComputerProvenanceTaintOutsidePage(t *testing.T) {
	h, _, computer, backup, _ := publishedBackupForStorageCopy(t, 2)
	h.stopServer()
	seedComputerSnapshotBackups(t, h, backup, 3, 1)
	seedReviewCustodyChain(t, h, backup, 1, "import", 2)
	page, next := provenanceReviewPage(t, h, computer.ComputerID, "", 1, context.WithValue(t.Context(), readPageCutoffContextKey{}, time.Hour))
	if len(page.Provenance) != 1 || next == "" || !page.CustodyTainted || page.Provenance[0].Kind != "backup" {
		t.Fatalf("taint must cover imports outside the page: %+v next=%s", page, next)
	}
}

func TestComputerStoreBackupInventoryWalksPages(t *testing.T) {
	h, _, computer, backup, _ := publishedBackupForStorageCopy(t, 2)
	h.stopServer()
	seedComputerSnapshotBackups(t, h, backup, 5, 1)
	page, err := h.store.ListComputerBackups(context.WithValue(t.Context(), readPageCutoffContextKey{}, time.Nanosecond), computer.ComputerID)
	if err != nil || len(page.Backups) != 6 || page.NextCursor != "" {
		t.Fatalf("inventory=%+v %v", page, err)
	}
}

func TestComputerSubmissionReceiptSurvivesPostChangeReadFailure(t *testing.T) {
	for _, revoked := range []bool{true, false} {
		t.Run(fmt.Sprint(revoked), func(t *testing.T) {
			h, _, computer := backupHarness(t, 2, nil)
			h.stopServer()
			admin, policy := computerSnapshotAdmin(t, h)
			h.server.computerTokenRevoker = recordingComputerTokenRevoker{revoke: func(ctx context.Context, request ComputerTokenRevocation) (contract.ComputerTokenRevocationReceipt, error) {
				if err := h.store.readDB.Close(); err != nil {
					t.Fatal(err)
				} // Failure only after the write and revocation.
				if !revoked {
					return contract.ComputerTokenRevocationReceipt{}, errors.New("revocation unavailable")
				}
				return contract.ComputerTokenRevocationReceipt{ComputerID: request.ComputerID, SubmitIntentRevision: request.NewSubmitIntentRevision, CommittedAt: h.clock.Now()}, nil
			}}
			payload, _ := json.Marshal(ComputerSubmissionRequest{PolicyRevision: policy.Revision, SubmitIntentRevision: 0, SubmitEnabled: boolPointer(true), IdempotencyKey: "review-submission"})
			ctx := context.WithValue(t.Context(), identityContextKey{}, admin)
			r := httptest.NewRequest(http.MethodPut, "/submission", strings.NewReader(string(payload))).WithContext(ctx)
			r.SetPathValue("computer_id", computer.ComputerID)
			w := httptest.NewRecorder()
			h.server.mutateComputerSubmission(w, r)
			var result ComputerSubmissionMutationResult
			if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &result) != nil || !result.MutationApplied || !result.SubmitEnabled || result.SubmitIntentRevision != 1 || result.PolicyRevision != policy.Revision+1 || (result.Revoked != nil) != revoked || (!revoked && result.RevocationNotice == "") {
				t.Fatalf("receipt lost=%d %s", w.Code, w.Body.String())
			}
		})
	}
}

func TestComputerAgentBackupAcknowledgementIndependentOfOperatorPool(t *testing.T) {
	h, node, computer, _, _ := publishedBackupForStorageCopy(t, 2)
	h.stopServer()
	_, _, err := h.store.BeginComputerBackup(t.Context(), computer.ComputerID, ComputerBackupCreateRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"), IdempotencyKey: "review-ack", AllowPowerOff: true})
	if err != nil {
		t.Fatal(err)
	}
	directives, err := h.store.ListNodeComputerBackupDirectives(t.Context(), "fabric-computer-node", node.NodeID, node.BootSessionID)
	if err != nil || len(directives) != 1 {
		t.Fatalf("directives=%+v %v", directives, err)
	}
	var occupied []*sql.Conn
	for i := 0; i < readSnapshotLimit; i++ {
		conn, err := h.store.readDB.Conn(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		occupied = append(occupied, conn)
	}
	defer func() {
		for _, conn := range occupied {
			conn.Close()
		}
	}()
	receipt := successfulBackupReceipt(directives[0])
	payload, _ := json.Marshal(ComputerBackupAcknowledgementRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID, IdempotencyKey: receipt.ReceiptID, Receipt: receipt})
	ctx := context.WithValue(t.Context(), identityContextKey{}, fabric.Identity{NodeID: "fabric-computer-node", Tags: []string{DefaultAgentPrincipalTag}})
	r := httptest.NewRequest(http.MethodPost, "/ack", strings.NewReader(string(payload))).WithContext(ctx)
	r.SetPathValue("computer_id", computer.ComputerID)
	w := httptest.NewRecorder()
	h.server.acknowledgeComputerBackup(w, r)
	var response ComputerBackupAcknowledgementResponse
	if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &response) != nil || response.Backup == nil || response.Backup.BackupID != directives[0].BackupID || response.Computer.ReconfigurationPhase != ComputerReconfigurationStable {
		t.Fatalf("agent starved=%d %s", w.Code, w.Body.String())
	}
}

func TestComputerCustodyExportLimitPermanent(t *testing.T) {
	h, node, computer, backup, _ := publishedBackupForStorageCopy(t, 2)
	h.stopServer()
	exported, _ := beginCustodyExport(t, h, node, computer, backup, "review-export-limit")
	tx, err := h.store.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for i := 0; i < 1000; i++ {
		id := fmt.Sprintf("review-export-%d", i)
		_, err := tx.Exec(`INSERT INTO computer_custody_exports(export_id,computer_id,operation_revision,backup_id,copy_id,source_storage_id,source_generation,allocated_size,content_digest,bound_node_id,root_instance_id,external_path,custody_fence,source_spec_json,source_spec_hash,idempotency_key,request_hash,status,requested_ns)
 SELECT ?,computer_id,operation_revision,backup_id,copy_id,source_storage_id,source_generation,allocated_size,content_digest,bound_node_id,root_instance_id,external_path,custody_fence,source_spec_json,source_spec_hash,?,request_hash,status,requested_ns FROM computer_custody_exports WHERE export_id=?`, id, id, exported.ExportID)
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	r := computerSnapshotRequest(t.Context(), computer.ComputerID)
	w := httptest.NewRecorder()
	h.server.listComputerStorageProvenance(w, r)
	var envelope contract.ErrorResponse
	if w.Code != http.StatusConflict || json.Unmarshal(w.Body.Bytes(), &envelope) != nil || envelope.Error.Code != contract.ErrorConflict || envelope.Error.Retryable || envelope.Error.Details["reason"] != "storage_custody_limit" {
		t.Fatalf("export bound=%d %s", w.Code, w.Body.String())
	}
}

// A Computer with no provenance rows walks to an empty list, not null, so
// JSON consumers can iterate the field unconditionally.
func TestComputerProvenanceEmptyWalkEncodesEmptyList(t *testing.T) {
	h, computer, _ := liveComputerTokenScope(t, "provenance-empty")
	got, err := h.store.ListComputerStorageProvenance(t.Context(), computer.ComputerID)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Provenance) != 0 || !strings.Contains(string(encoded), `"storage_provenance":[]`) {
		t.Fatalf("empty provenance walk encoded as %s", encoded)
	}
}
