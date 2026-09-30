package l3

import (
	"context"
	"database/sql"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/Derek-X-Wang/wefty/internal/durable"
)

// TestTheL3LedgerCarriesTheDurabilityPragmas: on darwin a plain fsync leaves
// an acknowledged commit in the drive cache, so every connection must use
// F_FULLFSYNC for commits and checkpoints (#599). Elsewhere nothing is added.
// The pragmas are per connection, so two pooled connections are checked.
func TestTheL3LedgerCarriesTheDurabilityPragmas(t *testing.T) {
	durable.EnableSQLiteFullFsyncForTests()
	t.Cleanup(durable.DisableSQLiteFullFsyncForTests)
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
