package l3

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/internal/durable"
)

// TestTheL3LedgerCarriesTheDurabilityPragmas: on darwin a plain fsync leaves
// an acknowledged commit in the drive cache, so every connection must use
// F_FULLFSYNC for commits and checkpoints (#599). Elsewhere nothing is added.
// The pragmas are per connection, so two pooled connections are checked.
func TestTheL3LedgerCarriesTheDurabilityPragmas(t *testing.T) {
	sqliteFullFsyncOffInTests.Store(false)
	t.Cleanup(func() { sqliteFullFsyncOffInTests.Store(true) })
	want := 0
	if runtime.GOOS == "darwin" {
		want = 1
	}
	store, err := OpenStore(filepath.Join(t.TempDir(), "l3.sqlite"), StoreOptions{})
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
	for label, conn := range map[string]interface {
		QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	}{"first connection": first, "second connection": second} {
		for _, pragma := range []string{"fullfsync", "checkpoint_fullfsync"} {
			var value int
			if err := conn.QueryRowContext(t.Context(), "PRAGMA "+pragma).Scan(&value); err != nil {
				t.Fatalf("%s: read PRAGMA %s: %v", label, pragma, err)
			}
			if value != want {
				t.Fatalf("%s: PRAGMA %s = %d on %s, want %d", label, pragma, value, runtime.GOOS, want)
			}
		}
	}
}

// TestL3DurabilityPragmasMatchTheSharedOnes: l3 keeps its own copy of the
// pragma list because ADR-0006 keeps its non-test imports to contract, fabric
// and l1. The copy must not drift from internal/durable's.
func TestL3DurabilityPragmasMatchTheSharedOnes(t *testing.T) {
	sqliteFullFsyncOffInTests.Store(false)
	durable.EnableSQLiteFullFsyncForTests()
	t.Cleanup(func() {
		sqliteFullFsyncOffInTests.Store(true)
		durable.DisableSQLiteFullFsyncForTests()
	})
	if got, want := sqliteDurabilityPragmas(), durable.SQLitePragmas(); !slices.Equal(got, want) {
		t.Fatalf("l3 pragmas = %v, internal/durable = %v", got, want)
	}
}

// TestOnlyTestsTurnOffTheL3DurabilityPragmas: the switch must never reach a
// production binary, so no non-test l3 file may set it.
func TestOnlyTestsTurnOffTheL3DurabilityPragmas(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		payload, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(payload), "sqliteFullFsyncOffInTests.Store(") {
			t.Fatalf("%s sets the test-only full-fsync switch", file)
		}
	}
}
