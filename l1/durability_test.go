package l1

import (
	"context"
	"database/sql"
	"encoding/hex"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/internal/durable"
)

// wantFullFsync is what production's PRAGMA fullfsync and checkpoint_fullfsync must read
// back on this platform: on darwin a plain fsync leaves an acknowledged commit
// in the drive cache (#599); elsewhere fsync is enough and nothing is added.
func wantFullFsync() int {
	if runtime.GOOS == "darwin" {
		return 1
	}
	return 0
}

func assertDurabilityPragmas(t *testing.T, label string, conn interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}) {
	t.Helper()
	for _, pragma := range []string{"fullfsync", "checkpoint_fullfsync"} {
		var value int
		if err := conn.QueryRowContext(t.Context(), "PRAGMA "+pragma).Scan(&value); err != nil {
			t.Fatalf("%s: read PRAGMA %s: %v", label, pragma, err)
		}
		if value != wantFullFsync() {
			t.Fatalf("%s: PRAGMA %s = %d on %s, want %d", label, pragma, value, runtime.GOOS, wantFullFsync())
		}
	}
}

// TestEveryL1SQLiteHandleCarriesTheDurabilityPragmas: the pragmas are per
// connection, so every handle L1 opens, and more than one pooled connection of
// the main handle, must carry them.
func TestEveryL1SQLiteHandleCarriesTheDurabilityPragmas(t *testing.T) {
	durable.EnableSQLiteFullFsyncForTests()
	t.Cleanup(durable.DisableSQLiteFullFsyncForTests)
	store, err := OpenStore(filepath.Join(t.TempDir(), "l1.sqlite"), StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	first, err := store.db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := store.db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	assertDurabilityPragmas(t, "main pool, first connection", first)
	assertDurabilityPragmas(t, "main pool, second connection", second)
	assertDurabilityPragmas(t, "owed-revocation settlement handle", store.settlementDB)
	assertDurabilityPragmas(t, "secret WAL checkpoint handle", store.checkpointDB)
}

func TestTheAuthorityInstanceIdentityIsCreatedOnceAndKept(t *testing.T) {
	database := filepath.Join(t.TempDir(), "l1.sqlite")
	created, err := loadOrCreateDeploymentID(database)
	if err != nil {
		t.Fatal(err)
	}
	assertIdentity(t, created)
	again, err := loadOrCreateDeploymentID(database)
	if err != nil || again != created {
		t.Fatalf("second boot read %q, %v; want the identity the first boot created (%q)", again, err, created)
	}
	assertOnlyIdentityFile(t, database)
}

// TestAnEmptyAuthorityInstanceIdentityIsAFirstBootThatNeverFinished: an older
// L1 created the name and died before its bytes reached the disk. Nothing ever
// used an identity from that file, so it is created afresh rather than failing
// every later start.
func TestAnEmptyAuthorityInstanceIdentityIsAFirstBootThatNeverFinished(t *testing.T) {
	database := filepath.Join(t.TempDir(), "l1.sqlite")
	if err := os.WriteFile(database+".authority-instance", nil, 0o600); err != nil {
		t.Fatal(err)
	}
	identity, err := loadOrCreateDeploymentID(database)
	if err != nil {
		t.Fatalf("an empty identity file still fails startup: %v", err)
	}
	assertIdentity(t, identity)
	payload, err := os.ReadFile(database + ".authority-instance")
	if err != nil || string(payload) != identity {
		t.Fatalf("identity file holds %q, %v; want the regenerated identity %q", payload, err, identity)
	}
	assertOnlyIdentityFile(t, database)
}

// TestADamagedAuthorityInstanceIdentityStillFailsStartup: bytes that are not an
// identity are damage, not an unfinished first boot, and are never replaced.
func TestADamagedAuthorityInstanceIdentityStillFailsStartup(t *testing.T) {
	database := filepath.Join(t.TempDir(), "l1.sqlite")
	path := database + ".authority-instance"
	if err := os.WriteFile(path, []byte("not-an-identity"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadOrCreateDeploymentID(database); err == nil || !strings.Contains(err.Error(), "authority instance identity is invalid") {
		t.Fatalf("a damaged identity = %v, want the invalid-identity failure", err)
	}
	if payload, _ := os.ReadFile(path); string(payload) != "not-an-identity" {
		t.Fatalf("a damaged identity was rewritten: %q", payload)
	}
}

// TestRacingFirstBootsAgreeOnOneAuthorityInstanceIdentity: exclusive creation
// survives the move to a staged, linked publish, from nothing and from an
// empty file an older L1 left behind.
func TestRacingFirstBootsAgreeOnOneAuthorityInstanceIdentity(t *testing.T) {
	for _, start := range []string{"absent", "empty"} {
		t.Run(start, func(t *testing.T) {
			database := filepath.Join(t.TempDir(), "l1.sqlite")
			if start == "empty" {
				if err := os.WriteFile(database+".authority-instance", nil, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			const boots = 16
			identities := make([]string, boots)
			failures := make([]error, boots)
			var wait sync.WaitGroup
			for index := range boots {
				wait.Go(func() {
					identities[index], failures[index] = loadOrCreateDeploymentID(database)
				})
			}
			wait.Wait()
			for index := range boots {
				if failures[index] != nil {
					t.Fatalf("boot %d: %v", index, failures[index])
				}
				if identities[index] != identities[0] {
					t.Fatalf("boots disagree: %q and %q", identities[0], identities[index])
				}
			}
			assertIdentity(t, identities[0])
			payload, err := os.ReadFile(database + ".authority-instance")
			if err != nil || string(payload) != identities[0] {
				t.Fatalf("identity file holds %q, %v; want %q", payload, err, identities[0])
			}
			assertOnlyIdentityFile(t, database)
		})
	}
}

func assertIdentity(t *testing.T, identity string) {
	t.Helper()
	decoded, err := hex.DecodeString(identity)
	if err != nil || len(decoded) != 32 {
		t.Fatalf("identity %q is not 32 hex-encoded bytes", identity)
	}
}

func assertOnlyIdentityFile(t *testing.T, database string) {
	t.Helper()
	entries, err := os.ReadDir(filepath.Dir(database))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != filepath.Base(database)+".authority-instance" {
		names := make([]string, 0, len(entries))
		for _, entry := range entries {
			names = append(names, entry.Name())
		}
		t.Fatalf("directory holds %v; want only the identity file (no staging leftovers)", names)
	}
}

// BenchmarkSQLiteCommitLatency measures one single-row commit on L1's
// production DSN with the platform durability pragmas on and off, reporting
// p50 and p95 per commit. It is not run in CI; run it with
// -bench SQLiteCommitLatency -benchtime 200x.
func BenchmarkSQLiteCommitLatency(b *testing.B) {
	for _, mode := range []struct {
		name string
		set  func()
	}{
		{name: "pragmas-on", set: durable.EnableSQLiteFullFsyncForTests},
		{name: "pragmas-off", set: durable.DisableSQLiteFullFsyncForTests},
	} {
		b.Run(mode.name, func(b *testing.B) {
			mode.set()
			b.Cleanup(durable.DisableSQLiteFullFsyncForTests)
			db, err := sql.Open("sqlite", sqliteDSN(filepath.Join(b.TempDir(), "bench.sqlite"), sqliteBusyTimeout))
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()
			db.SetMaxOpenConns(1)
			for _, statement := range []string{"PRAGMA journal_mode=WAL", "CREATE TABLE rows (id INTEGER PRIMARY KEY, value BLOB NOT NULL)"} {
				if _, err := db.ExecContext(b.Context(), statement); err != nil {
					b.Fatal(err)
				}
			}
			value := make([]byte, 256)
			latencies := make([]time.Duration, 0, b.N)
			for b.Loop() {
				started := time.Now()
				if _, err := db.ExecContext(b.Context(), "INSERT INTO rows (value) VALUES (?)", value); err != nil {
					b.Fatal(err)
				}
				latencies = append(latencies, time.Since(started))
			}
			slices.Sort(latencies)
			percentile := func(fraction float64) float64 {
				return float64(latencies[int(fraction*float64(len(latencies)-1))].Microseconds())
			}
			b.ReportMetric(percentile(0.50), "p50-µs")
			b.ReportMetric(percentile(0.95), "p95-µs")
		})
	}
}
