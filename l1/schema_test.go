package l1

import (
	"context"
	"fmt"
	"path/filepath"
	"slices"
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
