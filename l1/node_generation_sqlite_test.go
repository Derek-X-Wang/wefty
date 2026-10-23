//go:build sqlite_integration

package l1

import (
	"database/sql"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/Derek-X-Wang/wefty/fabric"
)

func TestNodeGenerationSQLiteOldSchemaReopenAndMigrationReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "old.sqlite")
	store, err := OpenStore(path, StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = store.CreateJob(t.Context(), validJobSpec("old-job", nil)); err != nil {
		t.Fatal(err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"node_generation_promotions", "node_generation_residents", "node_generations", "node_generation_catalog"} {
		if _, err = db.Exec("DROP TABLE " + table); err != nil {
			t.Fatal(err)
		}
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	var previous NodeGenerationInventory
	for i := 0; i < 3; i++ {
		store, err = OpenStore(path, StoreOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			r := generationRegistration(&integrationHarness{clock: &fakeClock{}}, "g1")
			r.CapabilityObservedAt = store.clock.Now()
			if _, err = store.RegisterNode(t.Context(), fabric.Identity{NodeID: "agent"}, r, DefaultNodePolicy(), true); err != nil {
				t.Fatal(err)
			}
		}
		inventory, err := store.NodeGenerations(t.Context(), "agent", "node")
		if err != nil || len(inventory.Generations) != 1 {
			t.Fatal(inventory, err)
		}
		if i > 0 && !reflect.DeepEqual(previous, inventory) {
			t.Fatal("migration replay changed authority", previous, inventory)
		}
		previous = inventory
		if err = store.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestNodeGenerationSQLitePromotionAndRetiringAuthoritySurviveReopen(t *testing.T) {
	f := newGenerationFixture(t)
	c := f.claim(t, "a")
	request := f.promote(t, "g2")
	var sequence int
	var databaseName, path string
	if err := f.h.store.db.QueryRowContext(t.Context(), `PRAGMA database_list`).Scan(&sequence, &databaseName, &path); err != nil {
		t.Fatal(err)
	}
	before := f.inventory
	if err := f.h.store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err := OpenStore(path, StoreOptions{Clock: f.h.clock})
	if err != nil {
		t.Fatal(err)
	}
	f.h.store = store
	inventory, err := store.NodeGenerations(t.Context(), "agent", "node")
	if err != nil || !reflect.DeepEqual(before, inventory) {
		t.Fatal("authority changed on reopen", err)
	}
	if _, err = store.RenewLease(t.Context(), "agent", c.Job.JobID, c.Lease.AttemptID, c.Lease.FencingToken); err != nil {
		t.Fatal(err)
	}
	if _, err = store.ResolveAttemptCredential(t.Context(), c.AttemptToken, "agent"); err != nil {
		t.Fatal(err)
	}
	replay, err := store.PromoteNodeGeneration(t.Context(), "agent", "node", request)
	if err != nil || !reflect.DeepEqual(before, replay) {
		t.Fatal("promotion receipt changed", err)
	}
}
