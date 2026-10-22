//go:build sqlite_integration

package l3

import (
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/Derek-X-Wang/wefty/internal/upgradestate"
)

func TestRestartableRunMigrationReopenAndFingerprint(t *testing.T) {
	path := filepath.Join(t.TempDir(), "l3.sqlite")
	store, err := OpenStore(path, StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	legacy, _, err := store.CreateRun(t.Context(), CreateRunInput{Actor: "migration-client", IdempotencyKey: "legacy", FabricClient: true, Request: inlineRunRequest("#!/bin/sh\nexit 0\n")})
	if err != nil {
		t.Fatal(err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`ALTER TABLE runs DROP COLUMN restartable`); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := upgradestate.Fingerprint(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	migratedFingerprint := ""
	for i := 0; i < 2; i++ {
		store, err = OpenStore(path, StoreOptions{})
		if err != nil {
			t.Fatal(err)
		}
		record, err := store.GetRun(t.Context(), legacy.RunID)
		if err != nil || record.Restartable {
			t.Fatalf("legacy run policy=%t err=%v", record.Restartable, err)
		}
		var defaultPolicy string
		if err = store.db.QueryRow(`SELECT dflt_value FROM pragma_table_info('runs') WHERE name='restartable'`).Scan(&defaultPolicy); err != nil || defaultPolicy != "0" {
			t.Fatalf("policy default=%s %v", defaultPolicy, err)
		}
		if err = store.Close(); err != nil {
			t.Fatal(err)
		}
		after, err := upgradestate.Fingerprint(t.Context(), path)
		if err != nil || after == before || (i > 0 && after != migratedFingerprint) {
			t.Fatalf("DDL fingerprint unchanged %s %v", after, err)
		}
		migratedFingerprint = after
	}
}
