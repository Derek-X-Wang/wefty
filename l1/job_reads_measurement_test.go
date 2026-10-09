package l1

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/fabric"
)

// Diagnostic runs without the production deadline, so a 1000-row page can
// finish even when it exceeds the hold limit. It uses the production page
// method, dedicated pool, pinned clock and per-caller projector. Production
// deadline/refusal tests are separate. #752 supplies sustained-load evidence.
func TestJobReadSnapshotMeasureMaximumPage(t *testing.T) {
	for _, scenario := range []string{"queued-bound", "queued-unbound", "running-bound", "failed-bound"} {
		t.Run(scenario, func(t *testing.T) {
			fixture := "claimed"
			if scenario == "failed-bound" {
				fixture = "latch"
			}
			h, _, seed, _ := jobProjectionFixture(t, fixture)
			for i := 0; i < 1000; i++ {
				spec := seed.Spec
				spec.DispatchKey = fmt.Sprintf("page-measure-%04d", i)
				if _, _, err := h.store.CreateJob(t.Context(), spec); err != nil {
					t.Fatal(err)
				}
			}
			var node string
			if err := h.store.db.QueryRow("SELECT bound_node_id FROM service_jobs WHERE job_id=?", seed.JobID).Scan(&node); err != nil {
				t.Fatal(err)
			}
			bound := node
			state := "queued"
			if scenario == "queued-unbound" {
				bound = ""
			}
			if scenario == "running-bound" {
				state = "running"
			}
			if scenario == "failed-bound" {
				state = "failed"
			}
			if _, err := h.store.db.Exec(`INSERT INTO attempts(attempt_id,job_id,node_id,boot_session_id,state,fencing_token,lease_expires_ns,authority_generation,result_json,created_ns,updated_ns)
 SELECT 'measure-' || jobs.job_id,jobs.job_id,a.node_id,a.boot_session_id,a.state,'measure-' || jobs.job_id,a.lease_expires_ns,a.authority_generation,a.result_json,jobs.created_ns,a.updated_ns
 FROM jobs CROSS JOIN attempts a WHERE a.job_id=? AND jobs.job_id<>?`, seed.JobID, seed.JobID); err != nil {
				t.Fatal(err)
			}
			if _, err := h.store.db.Exec("UPDATE jobs SET current_attempt_id='measure-' || job_id WHERE job_id<>?", seed.JobID); err != nil {
				t.Fatal(err)
			}
			if _, err := h.store.db.Exec("DELETE FROM jobs WHERE job_id=?", seed.JobID); err != nil {
				t.Fatal(err)
			}
			if _, err := h.store.db.Exec("UPDATE service_jobs SET bound_node_id=NULLIF(?, '')", bound); err != nil {
				t.Fatal(err)
			}
			if _, err := h.store.db.Exec("UPDATE jobs SET state=?", state); err != nil {
				t.Fatal(err)
			}
			actor := &serviceActionActor{Identity: fabric.Identity{NodeID: "client", Tags: []string{DefaultClientPrincipalTag}}, ClientPrincipalTag: DefaultClientPrincipalTag}
			for _, size := range []int{1000, MaxJobListingPageLimit} {
				for sample := 0; sample < 3; sample++ {
					conn, err := h.store.readDB.Conn(t.Context())
					if err != nil {
						t.Fatal(err)
					}
					tx, err := conn.BeginTx(t.Context(), &sql.TxOptions{ReadOnly: true})
					if err != nil {
						conn.Close()
						t.Fatal(err)
					}
					start := time.Now()
					if err := tx.QueryRow("SELECT COUNT(*) FROM sqlite_schema").Scan(new(int)); err != nil {
						tx.Rollback()
						conn.Close()
						t.Fatal(err)
					}
					counter := &projectionNodeCounter{queryer: tx}
					reads := newDatabaseReads(counter, h.clock.Now(), actor)
					// The diagnostic 1000-row probe bypasses only validation of a lowered cap.
					page, err := measureJobPage(t.Context(), reads, size)
					tx.Rollback()
					conn.Close()
					elapsed := time.Since(start)
					if err != nil || len(page.Jobs) != size {
						t.Fatalf("rows=%d err=%v", len(page.Jobs), err)
					}
					if counter.nodes > 3 {
						t.Fatalf("node facts grew with page: %d queries", counter.nodes)
					}
					t.Logf("rows=%d anchor-to-rollback=%s node_queries=%d", size, elapsed, counter.nodes)
				}
			}
		})
	}
}

func measureJobPage(ctx context.Context, r *databaseReads, size int) (JobList, error) {
	// Large diagnostic pages page through membership only when the configured
	// cap is smaller. All rows and projections still use this one transaction,
	// clock and memo; this is not how the production API acquires a page.
	var combined JobList
	cursor := ""
	for len(combined.Jobs) < size {
		limit := size - len(combined.Jobs)
		if limit > MaxJobListingPageLimit {
			limit = MaxJobListingPageLimit
		}
		page, err := r.jobsPage(ctx, jobListFilters{}, cursor, limit)
		if err != nil {
			return JobList{}, err
		}
		combined.Jobs = append(combined.Jobs, page.Jobs...)
		cursor = page.NextCursor
		if cursor == "" {
			break
		}
	}
	return combined, nil
}

// Measure the real children page with the production 200 ms hold limit,
// authenticated action projection, and one retained attempt per service.
func TestJobReadSnapshotMeasureChildrenPage(t *testing.T) {
	for _, scenario := range []string{"queued-bound", "queued-unbound", "running-bound", "failed-bound"} {
		t.Run(scenario, func(t *testing.T) {
			fixture := "claimed"
			if scenario == "failed-bound" {
				fixture = "latch"
			}
			h, _, parent, _ := jobProjectionFixture(t, fixture)
			for i := 0; i < 251; i++ {
				spec := parent.Spec
				spec.DispatchKey = fmt.Sprintf("child-measure-%04d", i)
				if _, _, err := h.store.CreateJob(t.Context(), spec); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := h.store.db.Exec(`INSERT INTO attempts(attempt_id,job_id,node_id,boot_session_id,state,fencing_token,lease_expires_ns,authority_generation,result_json,created_ns,updated_ns)
 SELECT 'child-measure-' || jobs.job_id,jobs.job_id,a.node_id,a.boot_session_id,a.state,'child-measure-' || jobs.job_id,a.lease_expires_ns,a.authority_generation,a.result_json,jobs.created_ns,a.updated_ns
 FROM jobs CROSS JOIN attempts a WHERE a.job_id=? AND jobs.job_id<>?`, parent.JobID, parent.JobID); err != nil {
				t.Fatal(err)
			}
			state := "queued"
			if scenario == "running-bound" {
				state = "running"
			}
			if scenario == "failed-bound" {
				state = "failed"
			}
			if _, err := h.store.db.Exec("UPDATE jobs SET current_attempt_id='child-measure-' || job_id, parent_job_id=?, state=? WHERE job_id<>?", parent.JobID, state, parent.JobID); err != nil {
				t.Fatal(err)
			}
			if scenario != "queued-unbound" {
				if _, err := h.store.db.Exec("UPDATE service_jobs SET bound_node_id=(SELECT bound_node_id FROM service_jobs WHERE job_id=?) WHERE job_id<>?", parent.JobID, parent.JobID); err != nil {
					t.Fatal(err)
				}
			}
			at := h.clock.Now()
			actor := &serviceActionActor{Identity: fabric.Identity{NodeID: "client", Tags: []string{DefaultClientPrincipalTag}}, ClientPrincipalTag: DefaultClientPrincipalTag}
			for sample := 0; sample < 3; sample++ {
				calls := 0
				h.store.clock = ClockFunc(func() time.Time { calls++; return at })
				start := time.Now()
				page, err := h.store.listChildJobsForCaller(t.Context(), parent.JobID, "", 1000, actor)
				elapsed := time.Since(start)
				if err != nil || len(page.Jobs) != 250 || page.NextCursor == "" || calls != 1 {
					t.Fatalf("rows=%d cursor=%q clocks=%d err=%v", len(page.Jobs), page.NextCursor, calls, err)
				}
				for _, job := range page.Jobs {
					assertCurrentAttemptPresent(t, job)
				}
				t.Logf("children=%d one-snapshot-door=%s clocks=%d", len(page.Jobs), elapsed, calls)
			}
		})
	}
}
