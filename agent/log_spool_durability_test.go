package agent

import (
	"runtime"
	"testing"

	"github.com/Derek-X-Wang/wefty/internal/durable"
)

// TestTheLogSpoolCarriesTheDurabilityPragmas: the spool deletes rows L1
// acknowledged and never re-sends them, so a commit it reported must survive
// power loss. On darwin that takes F_FULLFSYNC for commits and checkpoints
// (#599); elsewhere nothing is added.
func TestTheLogSpoolCarriesTheDurabilityPragmas(t *testing.T) {
	durable.EnableSQLiteFullFsyncForTests()
	t.Cleanup(durable.DisableSQLiteFullFsyncForTests)
	want := 0
	if runtime.GOOS == "darwin" {
		want = 1
	}
	spool, err := openLogSpool(t.TempDir(), "node-durability", 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer spool.Close()
	for _, pragma := range []string{"fullfsync", "checkpoint_fullfsync"} {
		var value int
		if err := spool.db.QueryRowContext(t.Context(), "PRAGMA "+pragma).Scan(&value); err != nil {
			t.Fatalf("read PRAGMA %s: %v", pragma, err)
		}
		if value != want {
			t.Fatalf("PRAGMA %s = %d on %s, want %d", pragma, value, runtime.GOOS, want)
		}
	}
}
