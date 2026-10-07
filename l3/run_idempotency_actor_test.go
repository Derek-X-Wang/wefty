package l3

import (
	"database/sql"
	"errors"
	"github.com/Derek-X-Wang/wefty/contract"
	"path/filepath"
	"strings"
	"testing"
)

// The pre-actor-scoping table definitions from 988e2b4, including the
// non-droppable column UNIQUE and child tables with foreign keys.
const legacyRunKeySchema = `CREATE TABLE IF NOT EXISTS runs (
  run_id TEXT PRIMARY KEY,
  parent_run_id TEXT REFERENCES runs(run_id),
  dispatch_key TEXT NOT NULL UNIQUE,
  idempotency_key TEXT NOT NULL UNIQUE,
  request_hash TEXT NOT NULL,
  status TEXT NOT NULL,
  params_json BLOB NOT NULL,
  tags_json BLOB NOT NULL,
  limits_json BLOB,
  envelope_schema_json BLOB,
  required_envelope INTEGER NOT NULL DEFAULT 0,
  dispatch_authority INTEGER NOT NULL DEFAULT 0,
  l1_job_id TEXT,
  node_id TEXT,
  failure_reason TEXT,
  node_attribution_pending INTEGER NOT NULL DEFAULT 0,
  job_link_settled INTEGER NOT NULL DEFAULT 0,
  job_link_failures INTEGER NOT NULL DEFAULT 0,
  job_link_retry_ns INTEGER NOT NULL DEFAULT 0,
  dispatch_attempt_ns INTEGER,
  created_ns INTEGER NOT NULL,
  updated_ns INTEGER NOT NULL,
  started_ns INTEGER,
  finished_ns INTEGER
);

CREATE TABLE IF NOT EXISTS run_scripts (
  run_id TEXT PRIMARY KEY REFERENCES runs(run_id) ON DELETE RESTRICT,
  content BLOB NOT NULL,
  sha256 TEXT NOT NULL,
  interpreter_json BLOB NOT NULL,
  mode INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS run_images (
  run_id TEXT PRIMARY KEY REFERENCES runs(run_id) ON DELETE RESTRICT,
  program_json BLOB NOT NULL
);

CREATE TABLE IF NOT EXISTS run_triggers (
  run_id TEXT PRIMARY KEY REFERENCES runs(run_id) ON DELETE RESTRICT,
  actor TEXT NOT NULL,
  source TEXT NOT NULL,
  source_run_id TEXT,
  computer_id TEXT,
  computer_attempt_id TEXT,
  computer_storage_generation INTEGER,
  submit_intent_revision INTEGER,
  params_json BLOB NOT NULL,
  created_ns INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS dispatch_outbox (
  run_id TEXT PRIMARY KEY REFERENCES runs(run_id) ON DELETE RESTRICT,
  dispatch_key TEXT NOT NULL UNIQUE,
  job_id TEXT,
  attempt_count INTEGER NOT NULL DEFAULT 0,
	  token_delivery TEXT,
  last_error TEXT,
  dispatched_ns INTEGER
);
`

func legacyRunKeyLedger(t *testing.T, provenance bool) (string, CreateRunInput) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "legacy.sqlite")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(legacyRunKeySchema); err != nil {
		t.Fatal(err)
	}
	input := CreateRunInput{Actor: "person-owner", IdempotencyKey: "existing-key", Request: inlineRunRequest("echo legacy\n")}
	input.Request.Tags = []string{"wefty:node:legacy"}
	normalized, _, mode, err := normalizeCreateRun(input)
	if err != nil {
		t.Fatal(err)
	}
	input = normalized
	for _, id := range []string{"old-parent", "old-child"} {
		parent := ""
		if id == "old-child" {
			parent = "old-parent"
		}
		rowInput := input
		rowInput.Request.ParentRunID = parent
		if id == "old-child" {
			rowInput.Actor = "legacy-child-actor"
			rowInput.IdempotencyKey = "child-key"
		}
		_, hash, _, err := normalizeCreateRun(rowInput)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO runs(run_id,parent_run_id,dispatch_key,idempotency_key,request_hash,status,params_json,tags_json,created_ns,updated_ns) VALUES(?,NULLIF(?,''),?,?,?,'pending',?, ?,123,456)`, id, parent, "run:"+id, rowInput.IdempotencyKey, hash, []byte(input.Request.Params), []byte(`["wefty:node:legacy"]`)); err != nil {
			t.Fatal(err)
		}
		if provenance {
			actor := input.Actor
			if id == "old-child" {
				actor = "legacy-child-actor"
			}
			if _, err := db.Exec(`INSERT INTO run_triggers(run_id,actor,source,params_json,created_ns) VALUES(?,?,'manual',?,123)`, id, actor, []byte(input.Request.Params)); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := db.Exec(`INSERT INTO run_scripts(run_id,content,sha256,interpreter_json,mode) VALUES(?,?,?,'[]',?)`, id, []byte(input.Request.InlineScript.Content), input.Request.InlineScript.SHA256, mode); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO dispatch_outbox(run_id,dispatch_key,attempt_count,last_error) VALUES(?,?,2,'legacy diagnostic')`, id, "run:"+id); err != nil {
			t.Fatal(err)
		}
	}
	return path, input
}

func TestRunIdempotencyActorUpgradeAndReopen(t *testing.T) {
	path, input := legacyRunKeyLedger(t, true)
	var otherRun, otherRerun string
	for open := 0; open < 2; open++ {
		store, err := OpenStore(path, StoreOptions{})
		if err != nil {
			t.Fatal(err)
		}
		func() {
			defer store.Close()
			original, replay, err := store.CreateRun(t.Context(), input)
			if err != nil || !replay || original.RunID != "old-parent" {
				t.Fatalf("legacy replay = %s/%t/%v", original.RunID, replay, err)
			}
			childInput := input
			childInput.Actor = "legacy-child-actor"
			childInput.IdempotencyKey = "child-key"
			childInput.Request.ParentRunID = "old-parent"
			child, replay, err := store.CreateRun(t.Context(), childInput)
			if err != nil || !replay || child.RunID != "old-child" {
				t.Fatalf("legacy child replay = %s/%t/%v", child.RunID, replay, err)
			}
			other := input
			other.Actor = "other-node"
			created, replay, err := store.CreateRun(t.Context(), other)
			if err != nil || replay != (open > 0) || created.RunID == original.RunID {
				t.Fatalf("other actor submit = %s/%t/%v", created.RunID, replay, err)
			}
			if open == 0 {
				otherRun = created.RunID
			} else if created.RunID != otherRun {
				t.Fatal("other actor replay changed after reopen")
			}
			// The node races ahead of the owner on a known source run.
			node, replay, err := store.CreateRerun(t.Context(), CreateRerunInput{Actor: other.Actor, IdempotencyKey: "shared-rerun", SourceRunID: original.RunID})
			if err != nil || replay != (open > 0) {
				t.Fatalf("node rerun = %t/%v", replay, err)
			}
			if open == 0 {
				otherRerun = node.RunID
			} else if node.RunID != otherRerun {
				t.Fatal("node rerun replay changed after reopen")
			}
			owner, replay, err := store.CreateRerun(t.Context(), CreateRerunInput{Actor: input.Actor, IdempotencyKey: "shared-rerun", SourceRunID: original.RunID})
			if err != nil || replay != (open > 0) || owner.RunID == node.RunID {
				t.Fatalf("owner rerun = %s/%t/%v", owner.RunID, replay, err)
			}
			changed := input
			changed.Request.Params = []byte(`{"changed":true}`)
			_, _, err = store.CreateRun(t.Context(), changed)
			var protocol *Error
			if !errors.As(err, &protocol) || protocol.Code != contract.ErrorIdempotencyConflict || !strings.Contains(protocol.Message, "different request") {
				t.Fatalf("same actor changed submit: %v", err)
			}
			_, _, err = store.CreateRerun(t.Context(), CreateRerunInput{Actor: input.Actor, IdempotencyKey: "shared-rerun", SourceRunID: node.RunID})
			if !errors.As(err, &protocol) || protocol.Code != contract.ErrorIdempotencyConflict {
				t.Fatalf("same actor changed rerun: %v", err)
			}
			var parent, content, diagnostic, actor string
			var attempts int
			if err := store.db.QueryRow(`SELECT r.parent_run_id,s.content,o.attempt_count,o.last_error,r.actor FROM runs r JOIN run_scripts s USING(run_id) JOIN dispatch_outbox o USING(run_id) WHERE r.run_id='old-child'`).Scan(&parent, &content, &attempts, &diagnostic, &actor); err != nil || actor != "legacy-child-actor" || parent != "old-parent" || content != "echo legacy\n" || attempts != 2 || diagnostic != "legacy diagnostic" {
				t.Fatalf("legacy data lost: %q %q %d %q %v", parent, content, attempts, diagnostic, err)
			}
			rows, err := store.db.Query(`PRAGMA foreign_key_check`)
			if err != nil {
				t.Fatal(err)
			}
			if rows.Next() {
				t.Fatal("broken foreign key after upgrade")
			}
			rows.Close()
			var enabled int
			if err := store.db.QueryRow(`PRAGMA foreign_keys`).Scan(&enabled); err != nil || enabled != 1 {
				t.Fatalf("foreign keys=%d err=%v", enabled, err)
			}
			// Constraint enforcement and indexes must survive the table replacement.
			if _, err := store.db.Exec(`UPDATE runs SET parent_run_id='missing' WHERE run_id='old-child'`); err == nil {
				t.Fatal("foreign key enforcement disabled")
			}
			if _, err := store.db.Exec(`UPDATE run_scripts SET content='changed' WHERE run_id='old-child'`); err == nil {
				t.Fatal("snapshot immutability lost")
			}
			var indexes int
			if err := store.db.QueryRow(`SELECT count(*) FROM sqlite_schema WHERE type='index' AND name IN ('runs_projection','runs_node_attribution_pending','runs_job_link_recovery')`).Scan(&indexes); err != nil || indexes != 3 {
				t.Fatalf("indexes=%d err=%v", indexes, err)
			}
		}()
	}
}

func TestRunActorUpgradeRefusesMissingProvenance(t *testing.T) {
	path, _ := legacyRunKeyLedger(t, false)
	store, err := OpenStore(path, StoreOptions{})
	if err == nil {
		store.Close()
		t.Fatal("upgrade accepted runs with no recorded actor")
	}
	// Failed upgrades leave the original constraint and rows intact.
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var schema string
	if err := db.QueryRow(`SELECT sql FROM sqlite_schema WHERE name='runs'`).Scan(&schema); err != nil || !strings.Contains(schema, "idempotency_key TEXT NOT NULL UNIQUE") {
		t.Fatalf("failed upgrade changed legacy table: %v %s", err, schema)
	}
	var count int
	if err := db.QueryRow(`SELECT count(*) FROM runs`).Scan(&count); err != nil || count != 2 {
		t.Fatalf("failed upgrade lost rows: %d %v", count, err)
	}
}

// Force failure after the replacement has happened, rather than only during
// the preflight provenance check, and verify transactional rollback.
func TestRunActorUpgradeRollsBackBrokenReferences(t *testing.T) {
	path, _ := legacyRunKeyLedger(t, true)
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE runs SET parent_run_id='missing' WHERE run_id='old-child'`); err != nil {
		t.Fatal(err)
	}
	db.Close()
	store, err := OpenStore(path, StoreOptions{})
	if err == nil {
		store.Close()
		t.Fatal("upgrade accepted broken lineage foreign key")
	}
	if !strings.Contains(err.Error(), "foreign key validation") {
		t.Fatalf("wrong upgrade failure: %v", err)
	}
	db, err = sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var schema string
	if err := db.QueryRow(`SELECT sql FROM sqlite_schema WHERE name='runs'`).Scan(&schema); err != nil || !strings.Contains(schema, "idempotency_key TEXT NOT NULL UNIQUE") {
		t.Fatalf("rollback lost original constraint: %v %s", err, schema)
	}
	var count int
	if err := db.QueryRow(`SELECT count(*) FROM runs`).Scan(&count); err != nil || count != 2 {
		t.Fatalf("rollback lost rows: %d %v", count, err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM sqlite_schema WHERE name='runs_actor_scoped'`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("rollback left replacement table: %d %v", count, err)
	}
}
