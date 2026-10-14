package upgradestate

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
)

func TestSnapshotIncludesWALWithoutMigratingLiveState(t *testing.T) {
	live := filepath.Join(t.TempDir(), "live.sqlite")
	db, err := sql.Open("sqlite", live)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("PRAGMA journal_mode=WAL; CREATE TABLE authority(fence INTEGER); INSERT INTO authority VALUES (99)"); err != nil {
		t.Fatal(err)
	}
	before, err := Fingerprint(t.Context(), live)
	if err != nil {
		t.Fatal(err)
	}
	copy := filepath.Join(t.TempDir(), "copy.sqlite")
	if err := Snapshot(t.Context(), live, copy); err != nil {
		t.Fatal(err)
	}
	copied, err := sql.Open("sqlite", copy)
	if err != nil {
		t.Fatal(err)
	}
	defer copied.Close()
	var fence int
	if err := copied.QueryRow("SELECT fence FROM authority").Scan(&fence); err != nil || fence != 99 {
		t.Fatalf("missing WAL commit: %d %v", fence, err)
	}
	if _, err := copied.Exec("CREATE TABLE new_format(id INTEGER); UPDATE authority SET fence=1"); err != nil {
		t.Fatal(err)
	}
	after, err := Fingerprint(t.Context(), live)
	if err != nil || before != after {
		t.Fatalf("live format changed: %s %s %v", before, after, err)
	}
	liveReader, err := sql.Open("sqlite", live)
	if err != nil {
		t.Fatal(err)
	}
	defer liveReader.Close()
	if err := liveReader.QueryRow("SELECT fence FROM authority").Scan(&fence); err != nil || fence != 99 {
		t.Fatalf("live authority changed: %d %v", fence, err)
	}
}

func TestFingerprintDetectsFormatAndMigrationMarkersOnly(t *testing.T) {
	for _, change := range []string{"ddl", "version", "application", "migration", "cursor", "authority"} {
		t.Run(change, func(t *testing.T) {
			live := filepath.Join(t.TempDir(), "live.sqlite")
			db, err := sql.Open("sqlite", live)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if _, err := db.Exec("CREATE TABLE authority(fence INTEGER); INSERT INTO authority VALUES (1); CREATE TABLE l1_data_migrations(name TEXT, applied_ns INTEGER); CREATE TABLE l1_data_migration_cursors(name TEXT)"); err != nil {
				t.Fatal(err)
			}
			before, err := Fingerprint(t.Context(), live)
			if err != nil {
				t.Fatal(err)
			}
			statements := map[string]string{"ddl": "CREATE INDEX next_format ON authority(fence)", "version": "PRAGMA user_version=1", "application": "PRAGMA application_id=3", "migration": "INSERT INTO l1_data_migrations VALUES ('new-format', 1)", "cursor": "INSERT INTO l1_data_migration_cursors VALUES ('new-format')", "authority": "UPDATE authority SET fence=2"}
			if _, err := db.Exec(statements[change]); err != nil {
				t.Fatal(err)
			}
			after, err := Fingerprint(t.Context(), live)
			if err != nil {
				t.Fatal(err)
			}
			same := before == after
			if same != (change == "authority") {
				t.Fatalf("format comparison for %s: unchanged=%t", change, same)
			}
		})
	}
}

func TestStateReadsRefuseMissingLinkedAndInvalidDatabases(t *testing.T) {
	for _, kind := range []string{"missing", "linked", "invalid", "canceled"} {
		t.Run(kind, func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, "state.sqlite")
			switch kind {
			case "linked":
				target := filepath.Join(root, "target")
				if err := os.WriteFile(target, []byte("sqlite"), 0o600); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(target, path); err != nil {
					t.Fatal(err)
				}
			case "invalid":
				if err := os.WriteFile(path, []byte("not sqlite"), 0o600); err != nil {
					t.Fatal(err)
				}
			case "canceled":
				db, err := sql.Open("sqlite", path)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := db.Exec("CREATE TABLE authority(fence INTEGER)"); err != nil {
					t.Fatal(err)
				}
				db.Close()
			}
			ctx := t.Context()
			if kind == "canceled" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			if _, err := Fingerprint(ctx, path); err == nil {
				t.Fatal("fingerprint accepted unknown state")
			}
			if err := Snapshot(ctx, path, filepath.Join(root, "copy.sqlite")); err == nil {
				t.Fatal("snapshot accepted unknown state")
			}
		})
	}
}
