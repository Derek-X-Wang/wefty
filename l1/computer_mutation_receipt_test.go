package l1

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

// Expire only the response snapshot, after the real keyed store mutation.
// The injected hold limit needs no sleep and leaves write authority intact.
func TestComputerMutationReceiptBeforePostChangeRead(t *testing.T) {
	for _, verb := range []string{"backup", "restore", "clone", "custody attest", "grow"} {
		t.Run(verb, func(t *testing.T) {
			h, node, computer, backup, _ := publishedBackupForStorageCopy(t, 3)
			h.stopServer()
			var body any
			var handler http.HandlerFunc
			exportID := ""
			switch verb {
			case "backup":
				body = ComputerBackupCreateRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"), IdempotencyKey: "receipt-backup"}
				handler = h.server.createComputerBackup
			case "restore":
				body = ComputerRestoreRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"), IdempotencyKey: "receipt-restore"}
				handler = h.server.restoreComputerBackup
			case "clone":
				if _, err := h.store.db.Exec(`UPDATE nodes SET capabilities_json='{}' WHERE node_id='computer-node'`); err != nil {
					t.Fatal(err)
				}
				body = ComputerCloneRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"), Name: "receipt-clone", DiskBytes: backup.AllocatedSize, IdempotencyKey: "receipt-clone"}
				handler = h.server.cloneComputerBackup
			case "custody attest":
				exported, _ := beginCustodyExport(t, h, node, computer, backup, "receipt-export")
				exportID = exported.ExportID
				body = ComputerCustodyAttestationRequest{IdempotencyKey: "receipt-attest"}
				handler = h.server.attestComputerCustodyDeleted
			case "grow":
				body = ComputerGrowRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"), DiskBytes: computer.DesiredDiskBytes + 1<<30, IdempotencyKey: "receipt-grow"}
				handler = h.server.growComputer
			}
			payload, err := json.Marshal(body)
			if err != nil {
				t.Fatal(err)
			}
			for _, replay := range []bool{false, true} {
				base := computerSnapshotRequest(t.Context(), computer.ComputerID)
				ctx := context.WithValue(base.Context(), readSnapshotHardLimitContextKey{}, time.Nanosecond)
				r := httptest.NewRequest(http.MethodPost, "/mutation", strings.NewReader(string(payload))).WithContext(ctx)
				r.SetPathValue("computer_id", computer.ComputerID)
				r.SetPathValue("backup_id", backup.BackupID)
				r.SetPathValue("export_id", exportID)
				w := httptest.NewRecorder()
				handler(w, r)
				var answer contract.ErrorResponse
				if w.Code != http.StatusServiceUnavailable || json.Unmarshal(w.Body.Bytes(), &answer) != nil || answer.Error.Code != contract.ErrorUnavailable || !answer.Error.Retryable || answer.Error.Details["reason"] != "read_snapshot_post_change_failed" || answer.Error.Details["mutation_applied"] != true {
					t.Fatalf("replay=%t response=%d %s", replay, w.Code, w.Body.String())
				}
				headers := w.Result().Header
				wantReplay := ""
				if replay {
					wantReplay = "true"
				}
				if headers.Get("Mutation-Applied") != strconv.FormatBool(!replay) || headers.Get("Idempotent-Replay") != wantReplay {
					t.Fatalf("replay=%t receipt=%v", replay, headers)
				}
			}
		})
	}
}
