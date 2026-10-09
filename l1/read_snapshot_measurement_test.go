package l1

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"
)

// A diagnostic transaction without a deadline lets the identical 1000-row
// projection finish on both revisions, instead of measuring a truncated page.
// Production transaction expiry is independently exercised by behaviour tests.
func TestReadSnapshotMeasure1000ProjectedRows(t *testing.T) {
	h, _, original, _ := jobProjectionFixture(t, "capability-missing")
	jobs := make([]Job, 1000)
	for i := range jobs {
		spec := original.Spec
		spec.DispatchKey = fmt.Sprintf("measure-%04d", i)
		job, _, err := h.store.CreateJob(t.Context(), spec)
		if err != nil {
			t.Fatal(err)
		}
		jobs[i] = job
	}
	if _, err := h.store.db.ExecContext(t.Context(), "UPDATE service_jobs SET bound_node_id=(SELECT bound_node_id FROM service_jobs WHERE job_id=?)", original.JobID); err != nil {
		t.Fatal(err)
	}
	conn, err := h.store.db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(t.Context(), "PRAGMA query_only=ON"); err != nil {
		t.Fatal(err)
	}
	defer conn.ExecContext(context.Background(), "PRAGMA query_only=OFF")
	tx, err := conn.BeginTx(t.Context(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	started := time.Now()
	if err := tx.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM sqlite_schema").Scan(new(int)); err != nil {
		t.Fatal(err)
	}
	counter := &projectionNodeCounter{queryer: tx}
	reads := newDatabaseReads(counter, canonicalTime(h.clock.Now()), nil)
	for _, job := range jobs {
		current, err := reads.job(t.Context(), job.JobID)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := projectJobWithReads(t.Context(), reads, current, projectJobAll); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(started)
	t.Logf("1000 projected rows anchor-to-rollback=%s node_queries=%d", elapsed, counter.nodes)
	enforceReadMeasurementBudget(t, elapsed)
}
