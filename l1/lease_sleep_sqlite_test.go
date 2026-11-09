//go:build sqlite_integration

package l1

import (
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/internal/upgradestate"
)

func TestLeaseSleepSQLiteMigrationStartsAnchorWithoutCredit(t *testing.T) {
	f, c := newSleepLeaseFixture(t)
	fresh, err := upgradestate.Fingerprint(t.Context(), f.path)
	if err != nil {
		t.Fatal(err)
	}
	// A database from before this change has no saved sleep baseline. The
	// upgrade must create one without crediting sleep that L1 never measured.
	if _, err := f.store.db.Exec(`DROP TABLE lease_sleep_clock`); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.db.Exec(`DROP TRIGGER nodes_heartbeat_sleep_reset`); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.db.Exec(`ALTER TABLE nodes DROP COLUMN heartbeat_sleep_ns`); err != nil {
		t.Fatal(err)
	}
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	c.advance(0, 5*time.Minute)
	f.store, err = openTestStore(t, f.path, f.options)
	if err != nil {
		t.Fatal(err)
	}
	upgraded, err := upgradestate.Fingerprint(t.Context(), f.path)
	if err != nil || upgraded != fresh {
		t.Fatalf("fresh=%s upgraded=%s err=%v", fresh, upgraded, err)
	}
	f.reconcile(t, 1)
}
