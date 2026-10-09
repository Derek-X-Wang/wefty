package l1

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

// Counts the production per-row projector used by listReadableJobsForCaller.
// Membership, row decoding and operator facts are outside this regression.
func TestLegacyJobListingProjectionQueryBudget(t *testing.T) {
	for _, scenario := range []struct {
		name   string
		bound  bool
		state  string
		budget int
	}{
		{"queued-bound", true, "queued", 4}, {"queued-unbound", false, "queued", 4}, {"running-bound", true, "running", 1},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			h, _, original, _ := jobProjectionFixture(t, "claimed")
			jobs := make([]Job, 1000)
			for i := range jobs {
				spec := original.Spec
				spec.DispatchKey = fmt.Sprintf("listing-%04d", i)
				job, _, err := h.store.CreateJob(t.Context(), spec)
				if err != nil {
					t.Fatal(err)
				}
				jobs[i] = job
			}
			bound := ""
			if scenario.bound {
				if err := h.store.db.QueryRowContext(t.Context(), "SELECT bound_node_id FROM service_jobs WHERE job_id=?", original.JobID).Scan(&bound); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := h.store.db.ExecContext(t.Context(), "UPDATE service_jobs SET bound_node_id=NULLIF(?, '')", bound); err != nil {
				t.Fatal(err)
			}
			if _, err := h.store.db.ExecContext(t.Context(), "UPDATE jobs SET state=?", scenario.state); err != nil {
				t.Fatal(err)
			}
			// Exclude the seed so the actual listing contains exactly 1000 rows.
			if _, err := h.store.db.ExecContext(t.Context(), "DELETE FROM jobs WHERE job_id=?", original.JobID); err != nil {
				t.Fatal(err)
			}
			started := time.Now()
			page, err := h.store.listReadableJobsForCaller(t.Context(), jobListFilters{}, "", 1000, nil)
			if err != nil || len(page.Jobs) != 1000 {
				t.Fatalf("listing: rows=%d err=%v", len(page.Jobs), err)
			}
			elapsed := time.Since(started)
			counter := &snapshotQueryCounter{queryer: h.store.db}
			perJob := 0
			for _, job := range page.Jobs {
				before := counter.count
				if _, err := h.store.projectJobWithQueryer(t.Context(), counter, job); err != nil {
					t.Fatal(err)
				}
				perJob = counter.count - before
				if queries := perJob; queries > scenario.budget {
					t.Errorf("legacy per-job queries=%d exceeds main=%d", queries, scenario.budget)
					break
				}
			}
			t.Logf("1000-row legacy listing=%s projection_queries_per_job=%d main_budget=%d", elapsed, perJob, scenario.budget)
		})
	}
}

func TestReadSnapshotIndependentBackgroundContexts(t *testing.T) {
	s, _ := snapshotStore(t)
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	done := make(chan error, 2)
	for range 2 {
		go func() {
			done <- s.withReadSnapshot(context.Background(), nil, func(context.Context, readModel) error {
				entered <- struct{}{}
				<-release
				return nil
			})
		}()
	}
	timer := time.NewTimer(150 * time.Millisecond)
	defer timer.Stop()
	count := 0
	for count < 2 {
		select {
		case <-entered:
			count++
		case <-timer.C:
			close(release)
			for range 2 {
				<-done
			}
			t.Fatalf("only %d independent Background snapshots entered", count)
		}
	}
	close(release)
	for range 2 {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
}

func TestReadSnapshotNestedIsProgrammingError(t *testing.T) {
	s, _ := snapshotStore(t)
	if err := s.withReadSnapshot(t.Context(), nil, func(ctx context.Context, _ readModel) error {
		err := s.withReadSnapshot(ctx, nil, func(context.Context, readModel) error { t.Error("nested callback ran"); return nil })
		api := apiErrorFromDecision(err)
		if api == nil || api.Code != contract.ErrorInternal || api.Retryable {
			t.Fatalf("nested error=%#v", api)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestReadPoolCannotEnableWrites(t *testing.T) {
	s, _ := snapshotStore(t)
	conn, err := s.readDB.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(t.Context(), "PRAGMA query_only=OFF"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(t.Context(), "CREATE TABLE forbidden_round2(value TEXT)"); err == nil {
		t.Fatal("read pool connection re-enabled writes")
	}
}

func TestQueuedPlacementUsesStoredTags(t *testing.T) {
	h, _, job, _ := jobProjectionFixture(t, "capability-missing")
	if _, err := h.store.db.ExecContext(t.Context(), "UPDATE nodes SET capabilities_json=(SELECT json_group_object(capability, json('true')) FROM job_required_capabilities WHERE job_id=?)", job.JobID); err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.db.ExecContext(t.Context(), "INSERT INTO job_tags(job_id, tag) VALUES(?, 'no-matching-node')", job.JobID); err != nil {
		t.Fatal(err)
	}
	job.State = contract.JobQueued
	job.Spec.RoutingTags = nil
	got, err := h.store.projectQueuedJobCapabilities(t.Context(), job)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != "unschedulable" || !strings.Contains(got.UnschedulableReason, "kind:process") {
		t.Fatalf("placement ignored stored routing tags: %#v", got)
	}
}

func TestReadSnapshotPlacementSkipsOccupancy(t *testing.T) {
	h, _, job, _ := jobProjectionFixture(t, "capability-missing")
	counter := &snapshotQueryCounter{queryer: h.store.db}
	// The legacy projection must not need service or active-attempt occupancy.
	q := &rejectOccupancyReads{queryer: counter}
	if _, err := h.store.projectJobWithQueryer(t.Context(), q, job); err != nil {
		t.Fatal(err)
	}
}

type rejectOccupancyReads struct{ queryer }

func (q *rejectOccupancyReads) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	if strings.Contains(query, "COUNT(*)") {
		return q.queryer.QueryRowContext(ctx, "SELECT * FROM forbidden_occupancy_read")
	}
	return q.queryer.QueryRowContext(ctx, query, args...)
}
func (q *rejectOccupancyReads) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if strings.Contains(query, "COUNT(*)") {
		return nil, errors.New("placement read occupancy")
	}
	return q.queryer.QueryContext(ctx, query, args...)
}

func TestReadSnapshotOverrunLoggerRateLimit(t *testing.T) {
	s, _ := snapshotStore(t)
	var lines []string
	s.logf = func(format string, args ...any) { lines = append(lines, fmt.Sprintf(format, args...)) }
	slow := func() {
		t.Helper()
		if err := s.withReadSnapshot(t.Context(), nil, func(context.Context, readModel) error { time.Sleep(110 * time.Millisecond); return nil }); err != nil {
			t.Fatal(err)
		}
	}
	slow()
	slow()
	if len(lines) != 1 || !strings.HasPrefix(lines[0], "event=l1_read_snapshot_target_overrun ") || s.readSnapshotOverrunCount() != 2 {
		t.Fatalf("lines=%v count=%d", lines, s.readSnapshotOverrunCount())
	}
	// Advance only the log gate; the transaction still exercises real timing.
	s.readSnapshotLastLog.Add(-int64(time.Second))
	slow()
	if len(lines) != 2 || !strings.Contains(lines[1], "count=3") {
		t.Fatalf("running count not reported: %v", lines)
	}
}
