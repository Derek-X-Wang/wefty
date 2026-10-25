//go:build sqlite_integration

package l1

import (
	"database/sql"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/internal/upgradestate"
)

func TestAttemptLeaseSQLiteMigrationPreservesLegacyAuthority(t *testing.T) {
	f := newAttemptClockFixture(t)
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	freshFingerprint, err := upgradestate.Fingerprint(t.Context(), f.path)
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", f.path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec("DROP INDEX IF EXISTS attempts_boot_lease_expiry"); err != nil {
		t.Fatal(err)
	}
	for _, column := range []string{"lease_boot_id", "lease_deadline_ns"} {
		var exists bool
		if err = db.QueryRow("SELECT EXISTS(SELECT 1 FROM pragma_table_info('attempts') WHERE name=?)", column).Scan(&exists); err != nil {
			t.Fatal(err)
		}
		if exists {
			if _, err = db.Exec("ALTER TABLE attempts DROP COLUMN " + column); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	// Reopening must migrate an actual unstamped Attempt, without making its
	// old wall lease into a new boot lease or changing its deletion rules.
	f.store, err = OpenStore(f.path, f.options)
	if err != nil {
		t.Fatal(err)
	}
	var boot string
	var deadline int64
	if err = f.store.db.QueryRow("SELECT lease_boot_id,lease_deadline_ns FROM attempts WHERE attempt_id=?", f.claim.Lease.AttemptID).Scan(&boot, &deadline); err != nil {
		t.Fatal(err)
	}
	upgradedFingerprint, err := upgradestate.Fingerprint(t.Context(), f.path)
	if err != nil {
		t.Fatal(err)
	}
	if upgradedFingerprint != freshFingerprint {
		t.Fatalf("fresh and upgraded schema differ: fresh=%s upgraded=%s", freshFingerprint, upgradedFingerprint)
	}
	if boot != "" || deadline != 0 {
		t.Fatalf("migration invented boot authority: %q %d", boot, deadline)
	}
	f.clock.elapsed += time.Hour
	if _, err = f.store.ResolveAttemptCredential(t.Context(), f.claim.AttemptToken, "agent-lease-node"); err != nil {
		t.Fatal(err)
	}
	f.reconcile(t, 0)
	if err = f.store.Close(); err != nil {
		t.Fatal(err)
	}
	f.store, err = OpenStore(f.path, f.options)
	if err != nil {
		t.Fatal(err)
	}
	f.clock.Advance(time.Minute)
	f.reconcile(t, 1)
	rows, err := f.store.db.Query("PRAGMA foreign_key_check")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if rows.Next() {
		t.Fatal("foreign key violation after migration and settlement")
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
}
