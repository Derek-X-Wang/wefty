package l1

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

func TestServiceConditionSinceSurvivesStop(t *testing.T) {
	for _, interrupted := range []bool{false, true} {
		t.Run(fmt.Sprintf("interrupted=%v", interrupted), func(t *testing.T) {
			h, client, agent, _, job, claim := neverFixture(t, "process")
			neverStarted(t, h, agent, job, claim)
			result := ProcessResult{OutputError: "durable output failed"}
			code := "failure_latched"
			if interrupted {
				result = ProcessResult{Signal: "terminated", TerminationCause: contract.TerminationCauseAgent}
				code = "never_automatic_restart_suppressed"
			}
			neverComplete(t, h, agent, job, claim, CompletionRequest{Result: result})
			before := serviceOperatorRead(t, h, client, job.JobID).Condition
			if before == nil || before.Code != code {
				t.Fatalf("condition=%+v; want %s", before, code)
			}
			var completedNS int64
			if err := h.store.db.QueryRow("SELECT updated_ns FROM attempts WHERE attempt_id=?", claim.Lease.AttemptID).Scan(&completedNS); err != nil {
				t.Fatal(err)
			}
			h.clock.Advance(time.Second)
			status, body := serviceOperatorWrite(t, h, client, job.JobID, "stop")
			if status != http.StatusAccepted {
				t.Fatalf("stop=%d %s", status, body)
			}
			after := serviceOperatorRead(t, h, client, job.JobID).Condition
			if !reflect.DeepEqual(before, after) || !after.Since.Equal(time.Unix(0, completedNS)) {
				t.Fatalf("stop moved condition: %+v -> %+v; completion=%d", before, after, completedNS)
			}
			updated, err := h.store.GetJob(t.Context(), job.JobID)
			if err != nil || !updated.UpdatedAt.After(after.Since) {
				t.Fatalf("fixture did not advance Job timestamp: %+v %v", updated, err)
			}
		})
	}
}

type countedServiceQueries struct {
	queryer
	ownership, capacity, root, total int
}

func (q *countedServiceQueries) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	q.total++
	switch {
	case strings.Contains(query, "SELECT computer_id FROM computer_job_projections"):
		q.ownership++
	case strings.Contains(query, "COUNT(*) FROM service_jobs"):
		q.capacity++
	case strings.Contains(query, "SELECT root_instance_id FROM nodes"):
		q.root++
	}
	return q.queryer.QueryRowContext(ctx, query, args...)
}

func TestServiceOperatorPageQueryCost(t *testing.T) {
	for _, refused := range []bool{false, true} {
		t.Run(fmt.Sprintf("refused=%v", refused), func(t *testing.T) {
			h, _, _, node, _, _ := neverFixture(t, "process")
			capacity, root := 2, node.RootInstanceID
			if refused {
				capacity, root = 1, ""
			}
			if _, err := h.store.db.Exec("UPDATE nodes SET max_service_slots=?, root_instance_id=? WHERE node_id=?", capacity, root, node.NodeID); err != nil {
				t.Fatal(err)
			}
			const pageSize = 64
			var jobs []Job
			for i := range pageSize {
				job, _, err := h.store.CreateJob(t.Context(), operatorServiceSpec(fmt.Sprintf("page-%d", i), nil))
				if err != nil {
					t.Fatal(err)
				}
				if _, err := h.store.db.Exec("UPDATE jobs SET state='stopped' WHERE job_id=?", job.JobID); err != nil {
					t.Fatal(err)
				}
				if _, err := h.store.db.Exec("UPDATE service_jobs SET desired_state='stopped', bound_node_id=? WHERE job_id=?", node.NodeID, job.JobID); err != nil {
					t.Fatal(err)
				}
				job, err = h.store.GetJob(t.Context(), job.JobID)
				if err != nil {
					t.Fatal(err)
				}
				jobs = append(jobs, job)
			}
			tx, err := h.store.db.BeginTx(t.Context(), &sql.TxOptions{ReadOnly: true})
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			queries := &countedServiceQueries{queryer: tx}
			actor := &serviceActionActor{Identity: fabric.Identity{Tags: []string{DefaultClientPrincipalTag}}, ClientPrincipalTag: DefaultClientPrincipalTag}
			if err := projectOperatorPageProbe(t.Context(), newDatabaseReads(queries, h.clock.Now(), actor), jobs); err != nil {
				t.Fatal(err)
			}
			// The occupancy COUNT visits this node's services once for the whole
			// page, not once per row or verb. Ownership remains an indexed lookup.
			if queries.ownership != pageSize || queries.capacity != 1 || queries.root != 1 || queries.total != pageSize+2 {
				t.Fatalf("page query work: ownership=%d capacity=%d root=%d total=%d; want %d/1/1/%d", queries.ownership, queries.capacity, queries.root, queries.total, pageSize, pageSize+2)
			}
			for _, job := range jobs {
				for _, action := range job.ServiceOperatorFacts.AllowedActions {
					want := contract.ErrorCode("")
					if refused {
						switch action.Verb {
						case "start", "restart":
							want = contract.ErrorCapacityExhausted
						case "remove", "forget":
							want = contract.ErrorConflict
						}
					}
					if (action.RefusedBecause == nil) != (want == "") || action.RefusedBecause != nil && action.RefusedBecause.Code != want {
						t.Fatalf("action=%+v; want refusal %q", action, want)
					}
				}
			}
			t.Logf("%d services: %d ownership reads, %d occupancy count, %d root read", pageSize, queries.ownership, queries.capacity, queries.root)
		})
	}
}

func TestChildListingSurvivesDeletedRow(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(fmt.Sprintf("failed=%v", failed), func(t *testing.T) {
			h, _, agent, _, child, claim := neverFixture(t, "process")
			parent, _, err := h.store.CreateJob(t.Context(), operatorServiceSpec("parent", nil))
			if err != nil {
				t.Fatal(err)
			}
			if failed {
				neverStarted(t, h, agent, child, claim)
				neverComplete(t, h, agent, child, claim, CompletionRequest{Result: ProcessResult{Signal: "terminated", TerminationCause: contract.TerminationCauseAgent}})
			} else {
				if _, err := h.store.db.Exec("UPDATE jobs SET state='stopped' WHERE job_id=?", child.JobID); err != nil {
					t.Fatal(err)
				}
				if _, err := h.store.db.Exec("UPDATE service_jobs SET desired_state='stopped' WHERE job_id=?", child.JobID); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := h.store.db.Exec("UPDATE jobs SET parent_job_id=? WHERE job_id=?", parent.JobID, child.JobID); err != nil {
				t.Fatal(err)
			}
			// SQLite is anchored before sampling the clock. Delete on another
			// connection there: the snapshot must retain the Job and its attempt
			// evidence. Exempt this fsync seam from projection timing.
			h.stopServer()
			oldClock := h.store.clock
			deleted := false
			h.store.clock = ClockFunc(func() time.Time {
				if !deleted {
					deleted = true
					if _, err := h.store.db.Exec("DELETE FROM jobs WHERE job_id=?", child.JobID); err != nil {
						t.Fatal(err)
					}
				}
				return oldClock.Now()
			})
			defer func() { h.store.clock = oldClock }()
			var page JobList
			err = diagnosticReadSnapshot(t, h.store, nil, func(reads readModel) (err error) {
				page, err = reads.childrenPage(t.Context(), parent.JobID, "", 10)
				return err
			})
			if err != nil || len(page.Jobs) != 1 || page.Jobs[0].JobID != child.JobID {
				t.Fatalf("deleted row broke listing snapshot: page=%+v err=%v", page, err)
			}
			if failed && !strings.Contains(page.Jobs[0].RestartSuppressed, "agent interruption") {
				t.Fatalf("lost attempt evidence: %+v", page.Jobs[0])
			}
			if !deleted {
				t.Fatal("deletion boundary was not exercised")
			}
		})
	}
}

func TestChildListingOperatorFactsSnapshot(t *testing.T) {
	h, _, _, node, parent, _ := neverFixture(t, "process")
	if _, err := h.store.db.Exec("UPDATE nodes SET max_service_slots=2 WHERE node_id=?", node.NodeID); err != nil {
		t.Fatal(err)
	}
	child, _, err := h.store.CreateJob(t.Context(), operatorServiceSpec("snapshot-child", nil))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.db.Exec("UPDATE jobs SET parent_job_id=?, state='stopped' WHERE job_id=?", parent.JobID, child.JobID); err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.db.Exec("UPDATE service_jobs SET desired_state='stopped', bound_node_id=? WHERE job_id=?", node.NodeID, child.JobID); err != nil {
		t.Fatal(err)
	}
	h.stopServer()
	oldClock := h.store.clock
	changed := false
	h.store.clock = ClockFunc(func() time.Time {
		if !changed {
			changed = true
			if _, err := h.store.db.Exec("UPDATE nodes SET max_service_slots=0, root_instance_id='' WHERE node_id=?", node.NodeID); err != nil {
				t.Fatal(err)
			}
			if _, err := h.store.db.Exec("DELETE FROM jobs WHERE job_id=?", child.JobID); err != nil {
				t.Fatal(err)
			}
		}
		return oldClock.Now()
	})
	defer func() { h.store.clock = oldClock }()
	// The injected writes synchronize the snapshot; their fsync time is not
	// projection cost. Exercise the production page method without a deadline.
	actor := &serviceActionActor{Identity: fabric.Identity{Tags: []string{DefaultClientPrincipalTag}}, ClientPrincipalTag: DefaultClientPrincipalTag}
	var page JobList
	err = diagnosticReadSnapshot(t, h.store, actor, func(reads readModel) (err error) {
		page, err = reads.childrenPage(t.Context(), parent.JobID, "", 100)
		return err
	})
	if err != nil || len(page.Jobs) != 1 {
		t.Fatalf("child snapshot=%+v err=%v", page, err)
	}
	facts := page.Jobs[0].ServiceOperatorFacts
	if facts == nil || len(facts.AllowedActions) != 5 {
		t.Fatalf("missing child operator facts: %+v", page)
	}
	for _, action := range facts.AllowedActions {
		if action.RefusedBecause != nil {
			t.Fatalf("child facts mixed node snapshots: %+v", action)
		}
	}
	if !changed {
		t.Fatal("concurrent mutation boundary was not exercised")
	}
}

func TestChildListingDeletedBeforeOperatorProjection(t *testing.T) {
	h := newIntegrationHarness(t, nil)
	parent, _, err := h.store.CreateJob(t.Context(), operatorServiceSpec("projection-parent", nil))
	if err != nil {
		t.Fatal(err)
	}
	var firstID string
	for i := range 2 {
		h.clock.Advance(time.Second)
		child, _, err := h.store.CreateJob(t.Context(), operatorServiceSpec(fmt.Sprintf("projection-child-%d", i), nil))
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			firstID = child.JobID
		}
		if _, err := h.store.db.Exec("UPDATE jobs SET parent_job_id=?, state='stopped' WHERE job_id=?", parent.JobID, child.JobID); err != nil {
			t.Fatal(err)
		}
		if _, err := h.store.db.Exec("UPDATE service_jobs SET desired_state='stopped' WHERE job_id=?", child.JobID); err != nil {
			t.Fatal(err)
		}
	}
	h.stopServer()
	oldClock := h.store.clock
	calls := 0
	h.store.clock = ClockFunc(func() time.Time { calls++; return oldClock.Now() })
	// Delete after the first selected Job is decoded, before its operator
	// projection, without depending on the legacy multiple-clock stitching.
	ctx, deleted := observeOnce(t, t.Context(), "job", func() {
		if _, err := h.store.db.Exec("DELETE FROM jobs WHERE job_id=?", firstID); err != nil {
			t.Fatal(err)
		}
	})
	defer func() { h.store.clock = oldClock }()
	actor := &serviceActionActor{Identity: fabric.Identity{Tags: []string{DefaultClientPrincipalTag}}, ClientPrincipalTag: DefaultClientPrincipalTag}
	var page JobList
	// Exempt this synchronization seam from the time budget: the deletion's
	// fsync is test coordination, not read projection cost. Membership and
	// operator projection still use the production method and one anchored view.
	err = diagnosticReadSnapshot(t, h.store, actor, func(reads readModel) (err error) {
		page, err = reads.childrenPage(ctx, parent.JobID, "", 100)
		return err
	})
	if err != nil || calls != 1 || !*deleted || len(page.Jobs) != 2 {
		t.Fatalf("deletion before operator projection: calls=%d rows=%d err=%v", calls, len(page.Jobs), err)
	}
	for _, job := range page.Jobs {
		if job.State != contract.JobStopped || job.ServiceOperatorFacts == nil || len(job.ServiceOperatorFacts.AllowedActions) != 5 {
			t.Fatalf("half-applied deletion: %+v", job)
		}
	}
	// A fresh production page sees the deletion fully applied.
	fresh, err := h.store.listChildJobsForCaller(t.Context(), parent.JobID, "", 100, actor)
	if err != nil || len(fresh.Jobs) != 1 || fresh.Jobs[0].JobID == firstID || fresh.Jobs[0].ServiceOperatorFacts == nil {
		t.Fatalf("fresh page after deletion: %+v err=%v", fresh, err)
	}
}

func projectOperatorPageProbe(ctx context.Context, reads readModel, jobs []Job) error {
	for i, job := range jobs {
		projected, err := projectJobWithReads(ctx, reads, job, projectJobOperatorPart)
		if err != nil {
			return err
		}
		jobs[i] = projected
	}
	return nil
}
