package l1

import (
	"path/filepath"
	"testing"

	"github.com/Derek-X-Wang/wefty/internal/upgradestate"
)

// Golden produced by main 93a37ed69e4918a289ff21e067c5bc21ff5f7258 through
// the same real store initialization and production rollback fingerprint.
func TestUpgradeSchemaMatchesMain(t *testing.T) {
	path := filepath.Join(t.TempDir(), "l1.sqlite")
	store, err := OpenStore(path, StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	got, err := upgradestate.Fingerprint(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("L1 schema fingerprint: %s", got)
	const mainFingerprint = "e1769cd16d9729a10dd2a75d220b7a2d0df03f811ef5c49974a73475ec3e2d78"
	if got != mainFingerprint {
		t.Fatalf("L1 schema differs from main: got %s want %s", got, mainFingerprint)
	}
}
