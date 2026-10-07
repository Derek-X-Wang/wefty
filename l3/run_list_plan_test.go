package l3

import (
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
)

// insertPagingTestRun writes the same creation time to both tables, as submit
// and rerun do, with a chosen ID so cursor ties are deterministic.
func insertPagingTestRun(s *Store, runID, actor string, status contract.RunState, createdNS int64) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`INSERT INTO runs(run_id, dispatch_key, idempotency_key, request_hash, status, params_json, tags_json, created_ns, updated_ns)
VALUES(?, ?, ?, '', ?, '{}', '[]', ?, ?)`, runID, "run:"+runID, runID, status, createdNS, createdNS); err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT INTO run_triggers(run_id, actor, source, params_json, created_ns)
VALUES(?, ?, 'manual', '{}', ?)`, runID, actor, createdNS); err != nil {
		return err
	}
	return tx.Commit()
}

func assertRunListPlans(t *testing.T, store *Store) {
	t.Helper()
	for _, filter := range []struct {
		name, status, submitter, index, alias string
	}{
		{"all", "", "", "runs_created", "r"},
		{"status", "succeeded", "", "runs_status_created", "r"},
		{"submitter", "", "actor-0", "run_triggers_actor_created", "t"},
		{"status-submitter", "succeeded", "actor-0", "run_triggers_actor_created", "t"},
	} {
		for _, position := range []struct {
			name   string
			cursor runListCursor
		}{
			{"head", runListCursor{}},
			{"cursor", runListCursor{CreatedNS: 51, RunID: "run_00000503"}},
		} {
			t.Run(filter.name+"/"+position.name, func(t *testing.T) {
				// Explain the exact query and arguments used by ListRuns, with
				// ANALYZE statistics, so adding an index alone cannot pass.
				query, args := runListQuery(filter.status, filter.submitter, position.cursor, 500)
				rows, err := store.db.QueryContext(t.Context(), "EXPLAIN QUERY PLAN "+query, args...)
				if err != nil {
					t.Fatal(err)
				}
				defer rows.Close()
				var details []string
				for rows.Next() {
					var id, parent, unused int
					var detail string
					if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
						t.Fatal(err)
					}
					details = append(details, detail)
				}
				if err := rows.Err(); err != nil {
					t.Fatal(err)
				}
				plan := strings.Join(details, "\n")
				t.Log(plan)
				seek := "SEARCH " + filter.alias + " USING "
				usesIndex := strings.Contains(plan, seek+"INDEX "+filter.index+" ") ||
					strings.Contains(plan, seek+"COVERING INDEX "+filter.index+" ")
				if !usesIndex ||
					!strings.Contains(plan, "(created_ns,run_id)<(?,?)") ||
					strings.Contains(plan, "TEMP B-TREE") || strings.Contains(plan, "SCAN ") {
					t.Fatalf("page must seek using %s without scanning or sorting:\n%s", filter.index, plan)
				}
				if filter.submitter != "" && !strings.Contains(plan, "actor=? AND (created_ns,run_id)<(?,?)") {
					t.Fatalf("submitter page must seek inside the actor's range:\n%s", plan)
				}
				if filter.submitter == "" && filter.status != "" && !strings.Contains(plan, "status=? AND (created_ns,run_id)<(?,?)") {
					t.Fatalf("status page must seek inside the status range:\n%s", plan)
				}
			})
		}
	}
}

func TestRunListQueryPlans(t *testing.T) {
	store, err := OpenStore(filepath.Join(t.TempDir(), "plans.sqlite"), StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for i := 0; i < 2000; i++ {
		status := contract.RunSucceeded
		if i%3 == 0 {
			status = contract.RunFailed
		}
		if err := insertPagingTestRun(store, fmt.Sprintf("run_%08d", i), fmt.Sprintf("actor-%d", i%7), status, int64(i/10+1)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.db.Exec("ANALYZE"); err != nil {
		t.Fatal(err)
	}
	assertRunListPlans(t, store)
}

func TestRunListIndexUpgradeAndReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "upgrade.sqlite")
	store, err := OpenStore(path, StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { store.Close() }()
	record, _, err := store.CreateRun(t.Context(), CreateRunInput{IdempotencyKey: "upgrade", Actor: "actor-0", Request: inlineRunRequest("#!/bin/sh\nexit 0\n")})
	if err != nil {
		t.Fatal(err)
	}
	// Simulate the existing schema without the three new indexes, retaining
	// a real Run and immutable provenance across upgrade and repeated opens.
	for _, index := range []string{"runs_created", "runs_status_created", "run_triggers_actor_created"} {
		if _, err := store.db.Exec("DROP INDEX IF EXISTS " + index); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 2; i++ {
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
		store, err = OpenStore(path, StoreOptions{})
		if err != nil {
			t.Fatal(err)
		}
		got, err := store.GetRun(t.Context(), record.RunID)
		if err != nil || !reflect.DeepEqual(got, record) {
			t.Fatalf("open %d changed the Run: %+v, %v", i, got, err)
		}
		assertRunListPlans(t, store)
	}
}
