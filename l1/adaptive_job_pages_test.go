package l1

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"
	"time"
)

// Diagnostic snapshots exercise the production projection in one read-only
// transaction, but exclude the hard deadline and soft cutoff. Slow test writes
// and cost measurements must not turn machine speed into a functional verdict.
func diagnosticReadSnapshot(t *testing.T, store *Store, actor *serviceActionActor, use func(readModel) error) error {
	t.Helper()
	conn, err := store.readDB.Conn(t.Context())
	if err != nil {
		return err
	}
	defer conn.Close()
	tx, err := conn.BeginTx(t.Context(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := tx.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM sqlite_schema").Scan(new(int)); err != nil {
		return err
	}
	return use(newDatabaseReads(tx, canonicalTime(store.clock.Now()), actor))
}

func enforceReadMeasurementBudget(t *testing.T, elapsed time.Duration) {
	t.Helper()
	if os.Getenv("WEFTY_ENFORCE_READ_BUDGET") == "1" && elapsed > readSnapshotBudget {
		t.Fatalf("read measurement %s exceeds opt-in budget %s", elapsed, readSnapshotBudget)
	}
}

func TestJobListingSoftCutoffCursorWalk(t *testing.T) {
	for _, route := range []string{"jobs", "services", "children"} {
		t.Run(route, func(t *testing.T) {
			h := newIntegrationHarnessWithReconcileInterval(t, StoreOptions{}, nil, false, time.Hour)
			seedJobListingRows(t, h.store, 7)
			parent, _, err := h.store.CreateJob(t.Context(), operatorServiceSpec("cutoff-parent", nil))
			if err != nil {
				t.Fatal(err)
			}
			// Equal creation times exercise the cursor's Job-ID tie-breaker.
			if _, err := h.store.db.Exec("UPDATE jobs SET created_ns=1, parent_job_id=? WHERE job_id<>?", parent.JobID, parent.JobID); err != nil {
				t.Fatal(err)
			}
			expected := map[string]bool{}
			for i := 1; i <= 7; i++ {
				if route != "services" || i%2 == 0 {
					expected[fmt.Sprintf("listing-%06d", i)] = true
				}
			}
			if route != "children" {
				expected[parent.JobID] = true
			}
			path := "/v1/jobs?limit=1000"
			if route == "services" {
				path += "&class=service"
			}
			if route == "children" {
				path = "/v1/jobs/" + parent.JobID + "/children?limit=1000"
			}
			seen := map[string]bool{}
			cursor := ""
			for n := 0; ; n++ {
				if n >= len(expected) {
					t.Fatal("cursor walk failed to advance")
				}
				// Force expiry before the first row: one row must still be returned.
				ctx := context.WithValue(t.Context(), readPageCutoffContextKey{}, time.Nanosecond)
				r := jobReadRequest(ctx, http.MethodGet, path+"&cursor="+url.QueryEscape(cursor), parent.JobID)
				w := httptest.NewRecorder()
				if route == "children" {
					h.server.listChildJobs(w, r)
				} else {
					h.server.listJobs(w, r)
				}
				var page JobList
				if w.Code != http.StatusOK {
					t.Fatalf("page %d: status=%d %s", n, w.Code, w.Body.String())
				}
				if err := json.Unmarshal(w.Body.Bytes(), &page); err != nil {
					t.Fatal(err)
				}
				if len(page.Jobs) != 1 {
					t.Fatalf("page %d: rows=%d, want one after forced cutoff", n, len(page.Jobs))
				}
				for _, job := range page.Jobs {
					if !expected[job.JobID] || seen[job.JobID] {
						t.Fatalf("unexpected or duplicate row %s", job.JobID)
					}
					seen[job.JobID] = true
					assertCurrentAttemptPresent(t, job)
				}
				cursor = page.NextCursor
				if cursor == "" {
					break
				}
			}
			if len(seen) != len(expected) {
				t.Fatalf("walk has %d rows, want %d", len(seen), len(expected))
			}
		})
	}
}
