package l1

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
)

func assertNoJobLogJSONLTable(t *testing.T, store *Store) {
	t.Helper()
	var tables int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE name='job_log_jsonl'`).Scan(&tables); err != nil {
		t.Fatal(err)
	}
	if tables != 0 {
		t.Fatal("job_log_jsonl still exists")
	}
}

// #52 S3: job_log_jsonl kept one empty row per job and nothing read it; the
// JSONL export is derived from log_events. A new database never has the
// table, creating jobs writes nothing there, and an upgraded database drops
// it and its rows on open without disturbing the jobs or their export.
func TestJobLogJSONLTableIsRetired(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jsonl.sqlite")
	store, err := OpenStore(path, StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	assertNoJobLogJSONLTable(t, store)
	job, err := submitDirect(store, validJobSpec("jsonl-retired", nil))
	if err != nil {
		t.Fatal(err)
	}
	insertDetachedLogRows(t, store, job.JobID, "jsonl-attempt", contract.AttemptRunning, "a", "b")
	assertNoJobLogJSONLTable(t, store)
	want, err := store.RawJobLogJSONL(context.Background(), job.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	// The database as an earlier L1 left it: the table and one row per job.
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = database.Exec(`CREATE TABLE job_log_jsonl (
		job_id TEXT PRIMARY KEY REFERENCES jobs(job_id) ON DELETE CASCADE,
		jsonl BLOB NOT NULL);
		INSERT INTO job_log_jsonl(job_id, jsonl) SELECT job_id, X'' FROM jobs;`)
	closeErr := database.Close()
	if err != nil || closeErr != nil {
		t.Fatal(errors.Join(err, closeErr))
	}

	store, err = OpenStore(path, StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	assertNoJobLogJSONLTable(t, store)
	if _, err := store.GetJob(context.Background(), job.JobID); err != nil {
		t.Fatalf("job after retiring job_log_jsonl: %v", err)
	}
	got, err := store.RawJobLogJSONL(context.Background(), job.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("JSONL export after retiring job_log_jsonl = %q, want %q", got, want)
	}
	if _, err := submitDirect(store, validJobSpec("jsonl-retired-after-upgrade", nil)); err != nil {
		t.Fatalf("submit after retiring job_log_jsonl: %v", err)
	}
	assertNoJobLogJSONLTable(t, store)
}
