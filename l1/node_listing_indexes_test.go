package l1

import (
	"path/filepath"
	"testing"
)

func TestNodeListingDropsRecordedStateIndexesOnReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.db")
	store, err := OpenStore(path, StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	// Reproduce the indexes present in stores created before effective-liveness
	// filtering. Opening an existing store must remove these obsolete indexes.
	_, err = store.db.Exec(`CREATE INDEX IF NOT EXISTS nodes_listing_state ON nodes(state, node_id, identity_node_id);
 CREATE INDEX IF NOT EXISTS nodes_listing_state_claims ON nodes(state, claims_enabled, node_id, identity_node_id);`)
	if err != nil {
		store.Close()
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = OpenStore(path, StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for _, name := range []string{"nodes_listing_state", "nodes_listing_state_claims", "nodes_listing_order", "nodes_listing_claims"} {
		var count int
		if err := store.db.QueryRow("SELECT COUNT(*) FROM sqlite_schema WHERE type='index' AND name=?", name).Scan(&count); err != nil {
			t.Fatal(err)
		}
		want := 0
		if name == "nodes_listing_order" || name == "nodes_listing_claims" {
			want = 1
		}
		if count != want {
			t.Errorf("index %s count=%d, want %d", name, count, want)
		}
	}
}
