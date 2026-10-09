package l1

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"
)

// Clone a valid durable event so lifecycle evidence remains parseable, while
// display timestamps are deliberately earlier than the original event.
func seedLifetimeRows(t *testing.T, h *integrationHarness, computer Computer, template ComputerCustodyExport, n int, batch int64) {
	t.Helper()
	tx, err := h.store.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for i := 0; i < n; i++ {
		generation := batch*10000 + int64(i) + 2
		if _, err = tx.Exec(`INSERT INTO computer_storage_generations(computer_id,storage_id,storage_generation,disk_bytes,phase,created_ns)
 VALUES(?,?,?,?, 'retired',?)`, computer.ComputerID, computer.StorageID, generation, computer.DesiredDiskBytes, batch); err != nil {
			t.Fatal(err)
		}
		id := fmt.Sprintf("lifetime-%d-%06d", batch, i)
		if _, err = tx.Exec(`INSERT INTO computer_custody_exports(export_id,computer_id,operation_revision,backup_id,copy_id,source_storage_id,source_generation,allocated_size,content_digest,bound_node_id,root_instance_id,external_path,custody_fence,source_spec_json,source_spec_hash,idempotency_key,request_hash,status,requested_ns)
 SELECT ?,computer_id,?,backup_id,copy_id,source_storage_id,source_generation,allocated_size,content_digest,bound_node_id,root_instance_id,?,custody_fence,source_spec_json,source_spec_hash,?,request_hash,status,?
 FROM computer_custody_exports WHERE export_id=?`, id, generation, "/operator/"+id, id, batch, template.ExportID); err != nil {
			t.Fatal(err)
		}
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestComputerLifetimeAdaptivePages(t *testing.T) {
	for _, kind := range []string{"generations", "exports"} {
		t.Run(kind, func(t *testing.T) {
			h, node, computer, backup, _ := publishedBackupForStorageCopy(t, 2)
			h.stopServer()
			export, _ := beginCustodyExport(t, h, node, computer, backup, "lifetime")
			seedLifetimeRows(t, h, computer, export, 5, 2)
			ctx := context.WithValue(t.Context(), readPageCutoffContextKey{}, time.Nanosecond)
			cursor := ""
			seen := map[string]bool{}
			for pages := 0; ; pages++ {
				if pages > 8 {
					t.Fatal("walk failed to advance")
				}
				r := computerSnapshotRequest(ctx, computer.ComputerID)
				q := r.URL.Query()
				q.Set("limit", "1000")
				q.Set("cursor", cursor)
				r.URL.RawQuery = q.Encode()
				w := httptest.NewRecorder()
				var keys []string
				if kind == "generations" {
					h.server.listComputerStorageGenerations(w, r)
					var page ComputerStorageGenerationList
					if json.Unmarshal(w.Body.Bytes(), &page) != nil {
						t.Fatal(w.Body.String())
					}
					cursor = page.NextCursor
					for _, g := range page.Generations {
						keys = append(keys, strconv.FormatInt(g.StorageGeneration, 10))
					}
				} else {
					h.server.listComputerCustodyExports(w, r)
					var page ComputerCustodyExportList
					if json.Unmarshal(w.Body.Bytes(), &page) != nil {
						t.Fatal(w.Body.String())
					}
					cursor = page.NextCursor
					for _, e := range page.Exports {
						keys = append(keys, e.ExportID)
					}
				}
				if w.Code != http.StatusOK || len(keys) != 1 {
					t.Fatalf("adaptive page=%d %s", w.Code, w.Body.String())
				}
				if seen[keys[0]] {
					t.Fatalf("duplicate %s", keys[0])
				}
				seen[keys[0]] = true
				if pages == 0 {
					seedLifetimeRows(t, h, computer, export, 1, 1) // Backdated, with smaller display keys.
					seedLifetimeRows(t, h, computer, export, 1, 3) // Still backdated, but after the continuation key.
					for _, bad := range []string{"invalid", cursor + "!", encodeComputerLifetimeCursor(computerLifetimeCursor{Version: 1, Kind: kind, ComputerID: "other", HighWater: 9, Generation: 1, ID: "x"})} {
						rr := computerSnapshotRequest(ctx, computer.ComputerID)
						rr.URL.RawQuery = "cursor=" + bad
						ww := httptest.NewRecorder()
						if kind == "generations" {
							h.server.listComputerStorageGenerations(ww, rr)
						} else {
							h.server.listComputerCustodyExports(ww, rr)
						}
						if ww.Code != http.StatusBadRequest {
							t.Fatalf("bad cursor accepted: %d", ww.Code)
						}
					}
				}
				if cursor == "" {
					break
				}
			}
			if len(seen) != 6 {
				t.Fatalf("watermark walk=%v", seen)
			}
			if kind == "generations" {
				all, err := h.store.ListComputerStorageGenerations(ctx, computer.ComputerID)
				if err != nil || len(all.Generations) != 8 || all.NextCursor != "" {
					t.Fatalf("fresh inventory=%+v %v", all, err)
				}
			} else {
				all, err := h.store.ListComputerCustodyExports(ctx, computer.ComputerID)
				if err != nil || len(all) != 8 {
					t.Fatalf("fresh inventory=%+v %v", all, err)
				}
			}
		})
	}
}

func TestComputerLifetimeMaximumPages(t *testing.T) {
	h, node, computer, backup, _ := publishedBackupForStorageCopy(t, 2)
	h.stopServer()
	export, _ := beginCustodyExport(t, h, node, computer, backup, "large-lifetime")
	seedLifetimeRows(t, h, computer, export, 1001, 1)
	ctx := context.WithValue(t.Context(), readPageCutoffContextKey{}, time.Hour)
	for _, kind := range []string{"generations", "exports"} {
		total := 0
		cursor := ""
		for {
			count := 0
			if kind == "generations" {
				page, err := h.store.ListComputerStorageGenerationsPage(ctx, computer.ComputerID, cursor, 1000000)
				if err != nil {
					t.Fatal(err)
				}
				count = len(page.Generations)
				cursor = page.NextCursor
			} else {
				page, err := h.store.ListComputerCustodyExportsPage(ctx, computer.ComputerID, cursor, 1000000)
				if err != nil {
					t.Fatal(err)
				}
				count = len(page.Exports)
				cursor = page.NextCursor
			}
			if count > 250 {
				t.Fatalf("%s cap=%d", kind, count)
			}
			total += count
			if cursor == "" {
				break
			}
			if total > 1002 {
				t.Fatal("duplicate walk")
			}
		}
		if total != 1002 {
			t.Fatalf("%s total=%d", kind, total)
		}
	}
}

func TestAppliedComputerReadErrorNamesPostChangeRead(t *testing.T) {
	err := appliedComputerReadError(snapshotUnavailable("read_snapshot_expired", context.DeadlineExceeded), "computer")
	api := apiErrorFromDecision(err)
	if api.Message != "Computer change committed but its post-change read is unavailable" || api.Details["reason"] != "read_snapshot_post_change_failed" || api.Details["read_reason"] != "read_snapshot_expired" || api.Details["mutation_applied"] != true || api.Details["computer_id"] != "computer" || !api.Retryable {
		t.Fatalf("post-change read error=%+v", api)
	}
}

func TestReadSnapshotLoadJobPageCap(t *testing.T) {
	h := newIntegrationHarnessWithReconcileInterval(t, StoreOptions{}, nil, false, time.Hour)
	h.stopServer()
	seedJobListingRows(t, h.store, 401)
	parent, _, err := h.store.CreateJob(t.Context(), operatorServiceSpec("load-cap-parent", nil))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = h.store.db.Exec("UPDATE jobs SET parent_job_id=? WHERE job_id<>?", parent.JobID, parent.JobID); err != nil {
		t.Fatal(err)
	}
	ctx := context.WithValue(t.Context(), readPageCutoffContextKey{}, time.Hour)
	ctx = context.WithValue(ctx, readSnapshotHardLimitContextKey{}, 10*time.Second)
	for _, children := range []bool{false, true} {
		var page JobList
		err = h.store.withReadSnapshot(ctx, nil, func(ctx context.Context, r readModel) error {
			var err error
			if children {
				page, err = r.childrenPage(ctx, parent.JobID, "", 1000)
			} else {
				page, err = r.jobsPage(ctx, jobListFilters{}, "", 1000)
			}
			return err
		})
		if err != nil || len(page.Jobs) != 150 || page.NextCursor == "" {
			t.Fatalf("children=%t rows=%d cursor=%s err=%v", children, len(page.Jobs), page.NextCursor, err)
		}
	}
}
