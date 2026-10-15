package l1

import (
	"path/filepath"
	"testing"

	"github.com/Derek-X-Wang/wefty/internal/upgradestate"
)

// Golden produced by main 93a37ed69e4918a289ff21e067c5bc21ff5f7258 through
// the same real store initialization and production rollback fingerprint,
// then by #939's service_jobs_published_attempt and attempts_job_created
// indexes and #942's service_jobs_healthy_node and service_jobs_stability_due
// indexes. A changed golden means an upgrade across it cannot restore the old
// binaries (ADR-0003).
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
	const mainFingerprint = "0ba07cd598094c1f39e6177c03276e3255e0982a030dc2b21039af39509a92ff"
	if got != mainFingerprint {
		t.Fatalf("L1 schema differs from main: got %s want %s", got, mainFingerprint)
	}
}
