package l1

import (
	"path/filepath"
	"testing"
	"time"

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
// approval-only and permanent-identity Seat log source migration, then #1181
// accepted Lease renewal timestamps (attempts.last_renewal_ns); then #1146
// durable boot-relative Seat/approval/idle clocks, activity holds and owner summaries;
// then remove the global calendar anchor so general L1 timestamps remain wall time;
// then #1247 boot-scoped Attempt lease deadlines and their expiry index.
// Then #1153 adds Attempt-bound, boot-relative copy leases in seat_copy_holds.
// Then #1150 adds Seat Run identity, exact command Attempt bindings,
// boot-relative start stamps and the private admitted Computer fence.
// #1147 adds screen routes using the existing Seat logs and activity holds;
// it adds no persisted schema, so this fingerprint remains unchanged.
// A changed golden means an upgrade across it cannot restore the old binaries
// (ADR-0003).
func TestUpgradeSchemaMatchesMain(t *testing.T) {
	path := filepath.Join(t.TempDir(), "l1.sqlite")
	store, err := OpenStore(path, StoreOptions{Clock: ClockFunc(func() time.Time {
		return time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	})})
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
	const mainFingerprint = "8e024392e8bc23c8102c8f15ef277bbca6af5df9425eb479134c33ad705e3c7e"
	if got != mainFingerprint {
		t.Fatalf("L1 schema differs from main: got %s want %s", got, mainFingerprint)
	}
}
