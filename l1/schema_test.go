package l1

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestStoreDeclaresCompleteServiceSchema(t *testing.T) {
	store, err := OpenStore(filepath.Join(t.TempDir(), "schema.sqlite"), StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	wantColumns := map[string][]string{
		"jobs":                      {"image_resolution_json", "image_resolution_hash", "prestart_budget_deadline_ns", "prestart_terminal_reason"},
		"log_events":                {"sequence_end"},
		"job_required_capabilities": {"job_id", "capability"},
		"nodes": {
			"max_oneshot_slots", "max_service_slots", "authority_generation", "claims_enabled",
			"intent_revision", "intent_reason", "intent_updated_at", "intent_actor", "root_instance_id", "connect_host",
			"capability_revision", "capability_observed_ns", "missing_capabilities_json", "capability_reason_code",
		},
		"attempts": {
			"authority_generation", "result_json", "late_result_json", "late_result_observed_ns",
			"late_result_authority_lost_ns", "late_result_is_late",
		},
		"service_jobs": {
			"job_id", "desired_state", "bound_node_id", "restart_streak", "lifetime_restart_count",
			"next_restart_at", "published_port", "last_failure", "healthy_since_ns", "published_attempt_id",
		},
		"computers": {
			"computer_id", "name", "placement_node_id", "bound_node_id", "grants_json", "storage_id",
			"storage_generation", "desired_state", "intent_revision", "applied_revision", "current_job_id",
			"current_spec_revision", "reconfiguration_phase", "reconfiguration_revision", "created_ns", "updated_ns",
		},
		"computer_job_projections": {
			"computer_id", "job_id", "spec_revision", "current", "created_ns", "retired_ns",
		},
		"computer_intent_history": {
			"computer_id", "intent_revision", "operation", "desired_state", "storage_id", "storage_generation",
			"job_id", "spec_revision", "actor", "created_ns",
		},
		"computer_storage_generations": {
			"computer_id", "storage_id", "storage_generation", "disk_bytes", "phase", "reset_revision", "created_ns", "retired_ns",
		},
		"computer_storage_resets": {
			"computer_id", "intent_revision", "storage_id", "old_generation", "new_generation", "disk_bytes",
			"bound_node_id", "job_id", "cleanup_fence", "idempotency_key", "request_hash", "status",
			"verification_receipt_json", "verification_receipt_hash", "acknowledgement_key", "acknowledgement_hash",
			"requested_ns", "verified_ns", "published_ns",
		},
		"admin_policy": {"singleton", "revision", "bootstrap_open", "authority_generation", "updated_ns"},
		"admins":       {"fabric_id", "user_id", "added_revision", "added_ns"},
		"admin_policy_audit": {
			"revision", "operation", "actor_kind", "actor_fabric_id", "actor_user_id",
			"actor_device_id", "subject_fabric_id", "subject_user_id", "created_ns",
		},
		"admin_bootstrap_challenges": {
			"singleton", "nonce_hash", "deployment_hash", "authority_generation", "created_ns", "expires_ns",
		},
		"service_restart_requests": {"job_id", "idempotency_key", "request_hash", "created_ns"},
		"service_log_truncations": {
			"job_id", "bound_kind", "evicted_event_count", "evicted_byte_count",
			"evicted_through_ordinal", "earliest_retained_ns", "updated_ns",
		},
		"service_removals": {
			"job_id", "bound_node_id", "removal_generation", "cleanup_fence", "root_instance_id", "status",
			"requested_ns", "cleanup_acknowledgement_key", "cleanup_acknowledgement_hash", "agent_cleaned_ns", "removed_ns",
		},
		"service_tombstones": {
			"job_id", "dispatch_key_hash", "request_hash", "created_ns", "removal_requested_ns", "removed_ns",
			"outcome", "last_bound_node_id", "removal_generation", "root_instance_id", "cleanup_acknowledged_ns",
		},
	}
	for table, columns := range wantColumns {
		got := tableColumns(t, store, table)
		for _, column := range columns {
			if !slices.Contains(got, column) {
				t.Errorf("table %s missing column %s; columns = %v", table, column, got)
			}
		}
	}

	assertNonUniqueIndex(t, store, "service_jobs", "service_jobs_bound_desired", []string{"bound_node_id", "desired_state"})

	var secureDelete int
	if err := store.db.QueryRowContext(context.Background(), "PRAGMA secure_delete").Scan(&secureDelete); err != nil {
		t.Fatal(err)
	}
	if secureDelete != 1 {
		t.Fatalf("secure_delete = %d, want 1", secureDelete)
	}
}

func TestStoreMigratesPreResetComputerConstraints(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pre-reset.sqlite")
	store, err := OpenStore(path, StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = database.Exec(`PRAGMA foreign_keys=OFF;
		INSERT INTO jobs(job_id, dispatch_key, request_hash, spec_json, state, created_ns, updated_ns)
		VALUES('migration-job', 'computer:migration', 'hash', '{}', 'stopped', 100, 100);
		INSERT INTO service_jobs(job_id, desired_state, bound_node_id, restart_streak, lifetime_restart_count)
		VALUES('migration-job', 'stopped', NULL, 0, 0);
		INSERT INTO computers(computer_id, name, placement_node_id, bound_node_id, grants_json,
			storage_id, storage_generation, desired_state, intent_revision, applied_revision,
			current_job_id, current_spec_revision, reconfiguration_phase, reconfiguration_revision,
			created_ns, updated_ns)
		VALUES('migration-computer', 'migration-name', 'migration-node', NULL,
			'[{"user_id":"migration-user","permission":"control"}]', 'migration-storage', 1,
			'stopped', 1, 1, 'migration-job', 1, 'stable', NULL, 100, 100);
		INSERT INTO computer_job_projections(computer_id, job_id, spec_revision, current, created_ns)
		VALUES('migration-computer', 'migration-job', 1, 1, 100);
		INSERT INTO computer_intent_history(computer_id, intent_revision, operation, desired_state,
			storage_id, storage_generation, job_id, spec_revision, actor, created_ns)
		VALUES('migration-computer', 1, 'create', 'stopped', 'migration-storage', 1,
			'migration-job', 1, 'migration-actor', 100);
		CREATE TABLE computers_old_constraint (
			computer_id TEXT PRIMARY KEY, name TEXT NOT NULL UNIQUE, placement_node_id TEXT NOT NULL,
			bound_node_id TEXT, grants_json BLOB NOT NULL, storage_id TEXT NOT NULL UNIQUE,
			storage_generation INTEGER NOT NULL CHECK(storage_generation > 0),
			desired_state TEXT NOT NULL CHECK(desired_state IN ('running', 'stopped', 'removed')),
			intent_revision INTEGER NOT NULL CHECK(intent_revision > 0),
			applied_revision INTEGER NOT NULL CHECK(applied_revision >= 0 AND applied_revision <= intent_revision),
			current_job_id TEXT NOT NULL UNIQUE REFERENCES jobs(job_id), current_spec_revision INTEGER NOT NULL CHECK(current_spec_revision > 0),
			reconfiguration_phase TEXT NOT NULL CHECK(reconfiguration_phase IN ('stable', 'projecting', 'removing')),
			reconfiguration_revision INTEGER CHECK(reconfiguration_revision > 0), created_ns INTEGER NOT NULL, updated_ns INTEGER NOT NULL
			);
		INSERT INTO computers_old_constraint SELECT * FROM computers;
		DROP TABLE computers;
		ALTER TABLE computers_old_constraint RENAME TO computers;
		CREATE INDEX computers_binding ON computers(bound_node_id, desired_state);
		CREATE TABLE computer_intent_history_old_constraint (
			computer_id TEXT NOT NULL REFERENCES computers(computer_id) ON DELETE CASCADE,
			intent_revision INTEGER NOT NULL CHECK(intent_revision > 0),
			operation TEXT NOT NULL CHECK(operation IN ('create', 'start', 'stop', 'restart', 'remove', 'project')),
			desired_state TEXT NOT NULL CHECK(desired_state IN ('running', 'stopped', 'removed')),
			storage_id TEXT NOT NULL, storage_generation INTEGER NOT NULL CHECK(storage_generation > 0),
			job_id TEXT NOT NULL REFERENCES jobs(job_id), spec_revision INTEGER NOT NULL CHECK(spec_revision > 0),
			actor TEXT NOT NULL, created_ns INTEGER NOT NULL, PRIMARY KEY(computer_id, intent_revision)
			);
		INSERT INTO computer_intent_history_old_constraint SELECT * FROM computer_intent_history;
		DROP TABLE computer_intent_history;
		ALTER TABLE computer_intent_history_old_constraint RENAME TO computer_intent_history;
		PRAGMA foreign_keys=ON;`)
	closeErr := database.Close()
	if err != nil || closeErr != nil {
		t.Fatal(errors.Join(err, closeErr))
	}
	store, err = OpenStore(path, StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	var computersSQL, intentsSQL string
	if err := store.db.QueryRow(`SELECT sql FROM sqlite_master WHERE type='table' AND name='computers'`).Scan(&computersSQL); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRow(`SELECT sql FROM sqlite_master WHERE type='table' AND name='computer_intent_history'`).Scan(&intentsSQL); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(computersSQL, "'resetting'") || !strings.Contains(intentsSQL, "'reset'") {
		t.Fatalf("reset constraints were not migrated: computers=%s intents=%s", computersSQL, intentsSQL)
	}
	var name, placement, grantsJSON, currentJob string
	var intentRevision int64
	if err := store.db.QueryRow(`SELECT name, placement_node_id, grants_json, current_job_id, intent_revision
		FROM computers WHERE computer_id='migration-computer'`).Scan(&name, &placement, &grantsJSON, &currentJob, &intentRevision); err != nil {
		t.Fatal(err)
	}
	if name != "migration-name" || placement != "migration-node" ||
		grantsJSON != `[{"user_id":"migration-user","permission":"control"}]` ||
		currentJob != "migration-job" || intentRevision != 1 {
		t.Fatalf("migrated Computer authority = %q %q %s %q %d", name, placement, grantsJSON, currentJob, intentRevision)
	}
	var projectionCount, intentCount int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM computer_job_projections
		WHERE computer_id='migration-computer' AND job_id='migration-job' AND current=1`).Scan(&projectionCount); err != nil {
		t.Fatal(err)
	}
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM computer_intent_history
		WHERE computer_id='migration-computer' AND job_id='migration-job' AND operation='create' AND actor='migration-actor'`).Scan(&intentCount); err != nil {
		t.Fatal(err)
	}
	if projectionCount != 1 || intentCount != 1 {
		t.Fatalf("migrated projection/intent counts = %d/%d", projectionCount, intentCount)
	}
}

func TestStoreConfiguresLateEvidenceWindowIndependently(t *testing.T) {
	store, err := OpenStore(filepath.Join(t.TempDir(), "late-window.sqlite"), StoreOptions{LateEvidenceWindow: 6 * time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if store.lateEvidenceWindow != 6*time.Hour {
		t.Fatalf("late evidence window = %s, want 6h", store.lateEvidenceWindow)
	}
}

func TestStoreConfiguresBoundedAdminBootstrapTTL(t *testing.T) {
	store, err := OpenStore(filepath.Join(t.TempDir(), "admin-bootstrap.sqlite"), StoreOptions{
		AdminBootstrapTTL: 2 * time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if store.adminBootstrapTTL != 2*time.Minute {
		t.Fatalf("admin bootstrap TTL = %s, want 2m", store.adminBootstrapTTL)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if store, err := OpenStore(filepath.Join(t.TempDir(), "invalid-admin-bootstrap.sqlite"), StoreOptions{
		AdminBootstrapTTL: -time.Second,
	}); err == nil {
		store.Close()
		t.Fatal("OpenStore accepted negative admin bootstrap TTL")
	}
}

func TestStoreConfiguresBoundedServiceLogRetention(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		store, err := OpenStore(filepath.Join(t.TempDir(), "service-retention-default.sqlite"), StoreOptions{})
		if err != nil {
			t.Fatal(err)
		}
		defer store.Close()
		if store.serviceLogRetentionBytes != DefaultServiceLogRetentionBytes || store.serviceLogRetentionAge != DefaultServiceLogRetentionAge {
			t.Fatalf("service retention defaults = %d/%s, want %d/%s", store.serviceLogRetentionBytes,
				store.serviceLogRetentionAge, DefaultServiceLogRetentionBytes, DefaultServiceLogRetentionAge)
		}
	})

	t.Run("configured", func(t *testing.T) {
		store, err := OpenStore(filepath.Join(t.TempDir(), "service-retention-configured.sqlite"), StoreOptions{
			ServiceLogRetentionBytes: 1234,
			ServiceLogRetentionAge:   36 * time.Hour,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer store.Close()
		if store.serviceLogRetentionBytes != 1234 || store.serviceLogRetentionAge != 36*time.Hour {
			t.Fatalf("configured service retention = %d/%s", store.serviceLogRetentionBytes, store.serviceLogRetentionAge)
		}
	})

	for _, test := range []struct {
		name    string
		options StoreOptions
	}{
		{name: "negative bytes", options: StoreOptions{ServiceLogRetentionBytes: -1}},
		{name: "negative age", options: StoreOptions{ServiceLogRetentionAge: -time.Second}},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, err := OpenStore(filepath.Join(t.TempDir(), "invalid-service-retention.sqlite"), test.options)
			if store != nil {
				store.Close()
			}
			if err == nil {
				t.Fatal("negative service retention option succeeded")
			}
		})
	}
}

func assertNonUniqueIndex(t *testing.T, store *Store, table, index string, wantColumns []string) {
	t.Helper()
	rows, err := store.db.QueryContext(context.Background(), fmt.Sprintf("PRAGMA index_list(%q)", table))
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for rows.Next() {
		var sequence, unique, partial int
		var name, origin string
		if err := rows.Scan(&sequence, &name, &unique, &origin, &partial); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		if name == index {
			found = true
			if unique != 0 {
				rows.Close()
				t.Fatalf("index %s is unique; service capacity requires a non-unique occupancy index", index)
			}
		}
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatalf("missing index %s on %s", index, table)
	}

	indexRows, err := store.db.QueryContext(context.Background(), fmt.Sprintf("PRAGMA index_info(%q)", index))
	if err != nil {
		t.Fatal(err)
	}
	defer indexRows.Close()
	var columns []string
	for indexRows.Next() {
		var sequence, cid int
		var name string
		if err := indexRows.Scan(&sequence, &cid, &name); err != nil {
			t.Fatal(err)
		}
		columns = append(columns, name)
	}
	if err := indexRows.Err(); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(columns, wantColumns) {
		t.Fatalf("index %s columns = %v, want %v", index, columns, wantColumns)
	}
}

func tableColumns(t *testing.T, store *Store, table string) []string {
	t.Helper()
	rows, err := store.db.QueryContext(context.Background(), fmt.Sprintf("PRAGMA table_info(%q)", table))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var columns []string
	for rows.Next() {
		var cid, notNull, primaryKey int
		var name, columnType string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			t.Fatal(err)
		}
		columns = append(columns, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return columns
}
