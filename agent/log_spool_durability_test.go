package agent

import (
	"context"
	"database/sql"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/durable"
)

// TestTheLogSpoolCarriesTheDurabilityPragmas: the spool deletes rows L1
// acknowledged and never re-sends them, so a commit it reported must survive
// power loss. On darwin that takes F_FULLFSYNC for commits and checkpoints
// (#599); elsewhere nothing is added. Both handles carry the pragmas; only the
// FULL handle's commits sync, and output appends use the NORMAL one.
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
	for _, handle := range []struct {
		name        string
		db          *sql.DB
		synchronous int
	}{
		{name: "FULL handle", db: spool.db, synchronous: 2},
		{name: "append handle", db: spool.appendDB, synchronous: 1},
	} {
		for pragma, expected := range map[string]int{
			"fullfsync": want, "checkpoint_fullfsync": want, "synchronous": handle.synchronous,
		} {
			var value int
			if err := handle.db.QueryRowContext(t.Context(), "PRAGMA "+pragma).Scan(&value); err != nil {
				t.Fatalf("%s: read PRAGMA %s: %v", handle.name, pragma, err)
			}
			if value != expected {
				t.Fatalf("%s: PRAGMA %s = %d on %s, want %d", handle.name, pragma, value, runtime.GOOS, expected)
			}
		}
	}
}

// TestOutputAppendsDoNotPayAFullSyncEach: on the production DSN a full sync
// costs about 4 ms on darwin, so 500 appends each paying one would take about
// 2 s. They go through the NORMAL handle and must take well under that.
func TestOutputAppendsDoNotPayAFullSyncEach(t *testing.T) {
	durable.EnableSQLiteFullFsyncForTests()
	t.Cleanup(durable.DisableSQLiteFullFsyncForTests)
	spool, err := openLogSpool(t.TempDir(), "node-append-cost", 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer spool.Close()
	claim := spoolTestClaim("append-cost")
	if err := spool.ensureAttempt(context.Background(), claim); err != nil {
		t.Fatal(err)
	}
	const appends = 500
	started := time.Now()
	for sequence := range appends {
		event := spoolTestEvent(claim.Lease.AttemptID, contract.LogStdout, uint64(sequence), fmt.Sprintf("line %d\n", sequence))
		if err := spool.append(t.Context(), event); err != nil {
			t.Fatal(err)
		}
	}
	elapsed := time.Since(started)
	t.Logf("%d output appends on the production DSN took %s", appends, elapsed)
	if runtime.GOOS != "darwin" {
		t.Skip("the full-sync cost this bounds is darwin's F_FULLFSYNC; elsewhere fsync is cheap and the timing proves nothing")
	}
	if elapsed >= time.Second {
		t.Fatalf("%d output appends took %s; each is paying a full sync", appends, elapsed)
	}
}
