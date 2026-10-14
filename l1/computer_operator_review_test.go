package l1

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

func addOperatorBackupRecord(t *testing.T, tx *sql.Tx, sourceID, id, status, phase string) {
	t.Helper()
	for _, statement := range []struct {
		query string
		args  []any
	}{
		{`INSERT INTO backups SELECT ?, computer_id, source_storage_id, source_generation,
			created_ns, allocated_size, content_digest, encryption, ?, ?, NULL
			FROM backups WHERE backup_id=?`, []any{id, "provenance-" + id, status, sourceID}},
		{`INSERT INTO storage_provenance(provenance_id, kind, source_storage_id, source_generation, backup_id, created_ns)
			SELECT ?, 'backup', source_storage_id, source_generation, backup_id, created_ns FROM backups WHERE backup_id=?`, []any{"provenance-" + id, id}},
		{`INSERT INTO backup_copies(copy_id, backup_id, node_id, root_instance_id, allocated_size, content_digest, phase, created_ns)
			SELECT ?, ?, node_id, root_instance_id, allocated_size, content_digest, ?, created_ns
			FROM backup_copies WHERE backup_id=?`, []any{"copy-" + id, id, phase, sourceID}},
	} {
		if _, err := tx.Exec(statement.query, statement.args...); err != nil {
			t.Fatal(err)
		}
	}
}

func TestComputerBackupChoicesBoundedByLiveHistory(t *testing.T) {
	h, _, computer, backup, _ := publishedBackupForStorageCopy(t, 4)
	caller := h.client(fabric.Identity{NodeID: "operator", Tags: []string{DefaultClientPrincipalTag}})
	paths := []string{"/v1/computers/" + computer.ComputerID, "/v1/computers?limit=1"}
	before := make([]any, len(paths))
	for i, path := range paths {
		status, _, body := h.do(caller, http.MethodGet, path, nil)
		if status != http.StatusOK || json.Unmarshal(body, &before[i]) != nil {
			t.Fatalf("baseline %s: %d %s", path, status, body)
		}
	}
	tx, err := h.store.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for i := range 1500 {
		addOperatorBackupRecord(t, tx, backup.BackupID, fmt.Sprintf("history-%04d", i), "pruned", "removed")
	}
	addOperatorBackupRecord(t, tx, backup.BackupID, "pending-prune", "pruning", "removal_pending")
	values := computerActionValues{}
	calls := 0
	var unavailable string
	err = computerBackupChoiceWithReads(t.Context(), transactionReads(tx, h.clock.Now()), computer, &values, func() error {
		calls++
		if values.BackupID != backup.BackupID {
			unavailable = values.BackupID
		}
		return protocolError(contract.ErrorConflict, "exercise all choices")
	})
	if calls != 1 || unavailable != "" || errorCode(err) != contract.ErrorConflict {
		t.Fatalf("choice work grew with history: calls=%d unavailable=%q err=%v", calls, unavailable, err)
	}
	// Leave only the original live record in the cap count, so the full HTTP
	// snapshots (which share this choice evaluator) must remain identical.
	if _, err := tx.Exec(`UPDATE backups SET status='pruned' WHERE backup_id='pending-prune'`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	for i, path := range paths {
		status, _, body := h.do(caller, http.MethodGet, path, nil)
		var after any
		if status != http.StatusOK || json.Unmarshal(body, &after) != nil || !reflect.DeepEqual(before[i], after) {
			t.Fatalf("history changed %s facts: %d %s", path, status, body)
		}
	}
}

func operatorProductionQuery(t *testing.T, file, function, beginning string) string {
	t.Helper()
	data, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	source := string(data)
	start := strings.Index(source, "func "+function+"(")
	if start < 0 {
		start = strings.Index(source, "func (r *databaseReads) "+function+"(")
	}
	if start < 0 {
		t.Fatalf("missing production function %s", function)
	}
	source = source[start:]
	start = strings.Index(source, "`"+beginning)
	if start < 0 {
		t.Fatalf("missing production query %s", beginning)
	}
	return strings.SplitN(source[start+1:], "`", 2)[0]
}

func TestComputerBackupOperatorQueryPlansExcludeHistory(t *testing.T) {
	h, _, computer := backupHarness(t, 4, nil)
	for _, test := range []struct {
		name, file, function, beginning, index, predicate string
	}{
		{"choices", "computer_operator.go", "backupChoices", "SELECT backup_id FROM backups", "backups_computer_status", "computer_id=? AND status=?"},
		{"backup-cap", "computer_decisions.go", "retainedBackups", "SELECT COUNT(*) FROM backups", "backups_computer_status", "computer_id=? AND status=?"},
		{"restore-cap", "computer_decisions.go", "retainedBackups", "SELECT COUNT(*) FROM backups", "backups_computer_status", "computer_id=? AND status=?"},
		{"copies", "backups.go", "readBackup", "SELECT copy_id", "backup_copies_backup_id", "backup_id=?"},
	} {
		t.Run(test.name, func(t *testing.T) {
			query := operatorProductionQuery(t, test.file, test.function, test.beginning)
			rows, err := h.store.db.Query("EXPLAIN QUERY PLAN "+query, computer.ComputerID)
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			var plan string
			for rows.Next() {
				var id, parent, unused int
				var detail string
				if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
					t.Fatal(err)
				}
				plan += detail + "\n"
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(plan, "SEARCH ") || !strings.Contains(plan, "INDEX "+test.index) ||
				!strings.Contains(plan, test.predicate) || strings.Contains(plan, "SCAN ") || strings.Contains(plan, "TEMP B-TREE") {
				t.Fatalf("operator query scans historical rows: %s", plan)
			}
			t.Log(plan)
		})
	}
}

func TestComputerBackupOperatorIndexesReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "upgrade.sqlite")
	store, err := OpenStore(path, StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`DROP INDEX IF EXISTS backups_computer_status; DROP INDEX IF EXISTS backup_copies_backup_id`); err != nil {
		store.Close()
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		store, err = OpenStore(path, StoreOptions{})
		if err != nil {
			t.Fatal(err)
		}
		var count int
		err = store.db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name IN ('backups_computer_status', 'backup_copies_backup_id')`).Scan(&count)
		closeErr := store.Close()
		if err != nil || closeErr != nil || count != 2 {
			t.Fatalf("indexes after upgrade/reopen: count=%d err=%v close=%v", count, err, closeErr)
		}
	}
}

func TestComputerProjectionDispatchConflictPrecedesFailedJob(t *testing.T) {
	h, _, computer, _, _ := publishedBackupForStorageCopy(t, 4)
	if _, err := h.store.db.Exec(`UPDATE jobs SET state='failed' WHERE job_id=?`, computer.CurrentJobID); err != nil {
		t.Fatal(err)
	}
	request := ComputerProjectionRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"), Spec: computer.CurrentJob.Spec}
	_, err := h.store.InstallComputerProjection(t.Context(), computer.ComputerID, request)
	if errorCode(err) != contract.ErrorDispatchKeyConflict {
		t.Fatalf("reused key on failed Computer: %v, want dispatch_key_conflict", err)
	}
	request.Spec.DispatchKey = "fresh-key-on-failed-computer"
	if _, err := h.store.InstallComputerProjection(t.Context(), computer.ComputerID, request); errorCode(err) != contract.ErrorConflict {
		t.Fatalf("fresh key must still refuse failed Computer: %v", err)
	}
	current, err := testReads(h.store).GetComputer(t.Context(), computer.ComputerID)
	if err != nil || current.IntentRevision != computer.IntentRevision || current.CurrentJobID != computer.CurrentJobID || current.ReconfigurationPhase != ComputerReconfigurationStable {
		t.Fatalf("refusal changed Computer authority: %#v err=%v", current, err)
	}
}

func TestComputerBackupChoicesContinuePastCorruptRecord(t *testing.T) {
	h, _, computer, backup, _ := publishedBackupForStorageCopy(t, 4)
	tx, err := h.store.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	// Sort the corrupt choice before the valid one to exercise continuation.
	corruptID := "000-corrupt"
	addOperatorBackupRecord(t, tx, backup.BackupID, corruptID, "available", "published")
	if _, err := tx.Exec(`UPDATE backups SET source_generation=source_generation+1 WHERE backup_id=?`, corruptID); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	caller := h.client(fabric.Identity{NodeID: "operator", Tags: []string{DefaultClientPrincipalTag}})
	for _, onlyCorrupt := range []bool{false, true} {
		if onlyCorrupt {
			if _, err := h.store.db.Exec(`UPDATE backups SET status='pruned' WHERE backup_id=?`, backup.BackupID); err != nil {
				t.Fatal(err)
			}
		}
		scenario := "corrupt-first-with-valid-choice"
		if onlyCorrupt {
			scenario = "only-corrupt-choice"
		}
		t.Run(scenario, func(t *testing.T) {
			for _, path := range []string{"/v1/computers/" + computer.ComputerID, "/v1/computers?limit=1"} {
				t.Run(path, func(t *testing.T) {
					before := backupAbsenceRows(t, h.store, computer.ComputerID, corruptID, "copy-"+corruptID)
					status, _, body := h.do(caller, http.MethodGet, path, nil)
					var facts computerOperatorFacts
					if strings.Contains(path, "?") {
						var page struct {
							Computers []computerOperatorFacts `json:"computers"`
						}
						if err := json.Unmarshal(body, &page); err != nil || len(page.Computers) != 1 {
							t.Fatalf("list: %s err=%v", body, err)
						}
						facts = page.Computers[0]
					} else if err := json.Unmarshal(body, &facts); err != nil {
						t.Fatal(err)
					}
					if status != http.StatusOK {
						t.Fatalf("read %s: %d %s", path, status, body)
					}
					found := make(map[string]bool)
					for _, action := range facts.AllowedActions {
						if action.Verb != "restore" && action.Verb != "clone" && action.Verb != "prune" && action.Verb != "custody-export" {
							continue
						}
						found[action.Verb] = true
						if !onlyCorrupt && action.RefusedBecause != nil {
							t.Fatalf("valid choice hidden by corruption: %#v", action)
						}
						if onlyCorrupt && (action.RefusedBecause == nil || action.RefusedBecause.Code != contract.ErrorInternal ||
							!action.RefusedBecause.Retryable || action.RefusedBecause.Message != "internal server error" || len(action.RefusedBecause.Details) != 0) {
							t.Errorf("%s must return scrubbed, retryable internal: %#v", action.Verb, action.RefusedBecause)
						}
					}
					for _, verb := range []string{"restore", "clone", "prune", "custody-export"} {
						if !found[verb] {
							t.Errorf("missing Backup verb %s", verb)
						}
					}
					if after := backupAbsenceRows(t, h.store, computer.ComputerID, corruptID, "copy-"+corruptID); after != before {
						t.Fatalf("advertisement changed prune state: before=%s after=%s", before, after)
					}
				})
			}
			t.Run("fresh-http-prune", func(t *testing.T) {
				before := backupAbsenceRows(t, h.store, computer.ComputerID, corruptID, "copy-"+corruptID)
				beforeComputer, err := testReads(h.store).GetComputer(t.Context(), computer.ComputerID)
				if err != nil {
					t.Fatal(err)
				}
				method, path, body := computerActionHTTP(computer, "prune", corruptID)
				body["idempotency_key"] = "corrupt-prune-" + scenario
				status, _, wire := h.do(caller, method, path, body)
				var refusal contract.ErrorResponse
				if err := json.Unmarshal(wire, &refusal); err != nil {
					t.Fatal(err)
				}
				if status != http.StatusInternalServerError || refusal.Error.Code != contract.ErrorInternal ||
					!refusal.Error.Retryable || refusal.Error.Message != "internal server error" || len(refusal.Error.Details) != 0 {
					t.Errorf("corrupt prune must return scrubbed, retryable internal: HTTP=%d %s", status, wire)
				}
				if after := backupAbsenceRows(t, h.store, computer.ComputerID, corruptID, "copy-"+corruptID); after != before {
					t.Fatalf("refused prune changed state: before=%s after=%s", before, after)
				}
				current, err := testReads(h.store).GetComputer(t.Context(), computer.ComputerID)
				if err != nil || !reflect.DeepEqual(current, beforeComputer) {
					t.Fatalf("refused prune changed Computer: %#v err=%v", current, err)
				}
			})
		})
	}
}

func TestComputerBackupPruneMissingAndForeignRemainNotFound(t *testing.T) {
	for _, target := range []string{"missing", "foreign"} {
		t.Run(target, func(t *testing.T) {
			h, _, computer, backup, _ := publishedBackupForStorageCopy(t, 4)
			backupID := "missing-backup"
			if target == "foreign" {
				var err error
				computer, _, err = h.store.CreateComputer(t.Context(), CreateComputerRequest{
					Name: "other-computer", Spec: computerCapabilityJobSpec("computer:other"), Actor: "operator",
				})
				if err != nil {
					t.Fatal(err)
				}
				backupID = backup.BackupID
			}
			before := backupAbsenceRows(t, h.store, backup.ComputerID, backup.BackupID, backup.Copies[0].CopyID)
			caller := h.client(fabric.Identity{NodeID: "operator", Tags: []string{DefaultClientPrincipalTag}})
			method, path, body := computerActionHTTP(computer, "prune", backupID)
			status, _, wire := h.do(caller, method, path, body)
			var refusal contract.ErrorResponse
			if err := json.Unmarshal(wire, &refusal); err != nil {
				t.Fatal(err)
			}
			if status != http.StatusNotFound || refusal.Error.Code != contract.ErrorNotFound || refusal.Error.Retryable ||
				refusal.Error.Message != fmt.Sprintf("Backup %q was not found", backupID) {
				t.Fatalf("%s prune must remain not_found: HTTP=%d %s", target, status, wire)
			}
			if after := backupAbsenceRows(t, h.store, backup.ComputerID, backup.BackupID, backup.Copies[0].CopyID); after != before {
				t.Fatalf("refused prune changed state: before=%s after=%s", before, after)
			}
		})
	}
}
