package l1

import (
	"path/filepath"
	"testing"

	"github.com/Derek-X-Wang/wefty/internal/upgradestate"
)

// Golden produced by main 93a37ed69e4918a289ff21e067c5bc21ff5f7258 through
// the same real store initialization and production rollback fingerprint,
// then by #939's service_jobs_published_attempt and attempts_job_created
// indexes, #942's service_jobs_healthy_node and service_jobs_stability_due
// indexes, and #948's attempts_lease_expiry, attempts_node_state and
// jobs_cancel_settlement indexes, then #900's automatically_admitted Node
// provenance column, then #1085's additive one-shot restart columns, then
// #1090's admin person-device binding and immutable rebind audit schema, then
// #1134's Computer Seat settings and creation provenance columns, then
// #1135's boot-scoped Seat approval records and windows, then #1174's
// additive generation catalog, sessions, residents and promotion receipts,
// then #1137's wanted Seat lists, retained identities and attempt-bound
// observations, with restart exclusion and deletion cascades, then #1142
// lifecycle columns on seat_approvals and retained room reservations/limits,
// then exact Seat and memory Attempt authority/absence recovery columns,
// then #1143 caller-bound Seat input receipts, credentials and the fenced
// start-finished receipt; then request-node deletion, final credential scrub/
// retention triggers and permanent once-Live Seat history; then #1145
// retained Seat logs, credential digests and per-credential read cursors; then
// bounded never-live Seat log retention and retained once-live history; then
// approval-only and permanent-identity Seat log source migration.
// A changed golden means an upgrade across it cannot restore the old binaries
// (ADR-0003).
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
	// Seat identity recovery retains exact authority and positive Attempt absence.
	const mainFingerprint = "a0f43260670f070b941d5d23d896d61e8cbef198e0611c3cf75df0e7781c6cc6"
	if got != mainFingerprint {
		t.Fatalf("L1 schema differs from main: got %s want %s", got, mainFingerprint)
	}
}
