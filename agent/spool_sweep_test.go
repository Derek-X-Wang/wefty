package agent

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

func spoolRowExists(t *testing.T, spool *logSpool, attemptID string) bool {
	t.Helper()
	var exists bool
	if err := spool.db.QueryRow(`SELECT EXISTS(SELECT 1 FROM spool_attempts WHERE attempt_id=?)`, attemptID).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	return exists
}

// #52 S3: one-shot spool rows L1 can no longer take -- incomplete-evidence
// tombstones, and completions whose process finished before L1's
// late-evidence window could still record them -- were kept forever. The
// sweep removes exactly those once they are older than the window plus a
// margin; everything L1 can still accept, services, and attempts this
// process still owns stay.
func TestSpoolSweepRemovesOnlyDeadOneShotRowsPastTheWindow(t *testing.T) {
	directory := t.TempDir()
	spool := openTestLogSpool(t, directory, "node-sweep", 1<<20)
	ctx := context.Background()
	now := time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)
	cutoff := now.Add(-DefaultLogSpoolSweepAfter)
	exitCode := 0
	claimRow := func(claim l1.Claim) string {
		t.Helper()
		if err := spool.ensureAttempt(ctx, claim); err != nil {
			t.Fatal(err)
		}
		return claim.Lease.AttemptID
	}
	withEvents := func(attemptID string) {
		t.Helper()
		for sequence, payload := range []string{"one", "two"} {
			if err := spool.append(ctx, spoolTestEvent(attemptID, contract.LogStdout, uint64(sequence), payload)); err != nil {
				t.Fatal(err)
			}
		}
	}
	finish := func(attemptID string, at time.Time) {
		t.Helper()
		if err := spool.storeCompletion(ctx, attemptID, l1.ProcessResult{ExitCode: &exitCode}, at); err != nil {
			t.Fatal(err)
		}
	}
	seal := func(attemptID string, at time.Time) {
		t.Helper()
		if err := spool.sealIncomplete(ctx, attemptID, "attempt authority no longer accepts evidence", contract.ErrorAttemptNotFound, at); err != nil {
			t.Fatal(err)
		}
	}

	oldTombstone := claimRow(spoolTestClaim("old-tombstone"))
	withEvents(oldTombstone)
	seal(oldTombstone, cutoff.Add(-time.Minute))
	freshTombstone := claimRow(spoolTestClaim("fresh-tombstone"))
	seal(freshTombstone, cutoff.Add(time.Minute))
	oldUndelivered := claimRow(spoolTestClaim("old-undelivered"))
	withEvents(oldUndelivered)
	finish(oldUndelivered, cutoff.Add(-time.Minute))
	freshUndelivered := claimRow(spoolTestClaim("fresh-undelivered"))
	finish(freshUndelivered, cutoff.Add(time.Minute))
	// Logs still pending and no completion: L1 may still take them.
	pendingOnly := claimRow(spoolTestClaim("pending-only"))
	withEvents(pendingOnly)
	serviceTombstone := claimRow(serviceSpoolTestClaim("service-tombstone"))
	seal(serviceTombstone, cutoff.Add(-24*time.Hour))
	serviceUndelivered := claimRow(serviceSpoolTestClaim("service-undelivered"))
	finish(serviceUndelivered, cutoff.Add(-24*time.Hour))
	liveTombstone := claimRow(spoolTestClaim("live-tombstone"))
	seal(liveTombstone, cutoff.Add(-time.Hour))

	// A tombstone sealed before sealed_ns existed is aged from its document.
	legacyOld := claimRow(spoolTestClaim("legacy-old-tombstone"))
	seal(legacyOld, cutoff.Add(-time.Hour))
	legacyFresh := claimRow(spoolTestClaim("legacy-fresh-tombstone"))
	seal(legacyFresh, cutoff.Add(time.Hour))
	if _, err := spool.db.Exec(`UPDATE spool_attempts SET sealed_ns=NULL WHERE attempt_id IN (?, ?)`, legacyOld, legacyFresh); err != nil {
		t.Fatal(err)
	}
	if err := spool.Close(); err != nil {
		t.Fatal(err)
	}
	spool = openTestLogSpool(t, directory, "node-sweep", 1<<20)
	defer spool.Close()

	live := func(attemptID string) bool { return attemptID == liveTombstone }
	sweep, err := spool.sweepDeadOneShotAttempts(ctx, cutoff, spoolSweepBatch, live)
	if err != nil {
		t.Fatal(err)
	}
	if sweep.sealed != 2 || sweep.undelivered != 1 || sweep.full {
		t.Fatalf("sweep = %+v, want 2 tombstones and 1 undelivered completion", sweep)
	}
	for _, gone := range []string{oldTombstone, oldUndelivered, legacyOld} {
		if spoolRowExists(t, spool, gone) {
			t.Fatalf("%s survived the sweep", gone)
		}
	}
	for _, kept := range []string{freshTombstone, freshUndelivered, pendingOnly, serviceTombstone, serviceUndelivered, liveTombstone, legacyFresh} {
		if !spoolRowExists(t, spool, kept) {
			t.Fatalf("%s was swept", kept)
		}
	}
	var orphans int
	if err := spool.db.QueryRow(`SELECT
  (SELECT COUNT(*) FROM spool_events WHERE attempt_id NOT IN (SELECT attempt_id FROM spool_attempts))
+ (SELECT COUNT(*) FROM spool_acknowledgements WHERE attempt_id NOT IN (SELECT attempt_id FROM spool_attempts))`).Scan(&orphans); err != nil {
		t.Fatal(err)
	}
	if orphans != 0 {
		t.Fatalf("%d spool events or acknowledgements outlived their swept attempt", orphans)
	}
	// The row L1 can still take is still pending recovery.
	attempts, err := spool.pendingAttempts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	pending := map[string]bool{}
	for _, attempt := range attempts {
		pending[attempt.attemptID] = true
	}
	if !pending[pendingOnly] || !pending[freshUndelivered] {
		t.Fatalf("pending after sweep = %v", pending)
	}
}

// A backlog is swept in bounded transactions; a full batch leaves the sweep
// due again so the backlog drains over successive recovery passes, and a
// batch that is not full waits for the interval.
func TestSpoolSweepIsBoundedAndReported(t *testing.T) {
	spool := openTestLogSpool(t, t.TempDir(), "node-sweep-bounded", 1<<20)
	defer spool.Close()
	ctx := context.Background()
	clock := newManualClock(time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC))
	sealedAt := clock.Now().Add(-DefaultLogSpoolSweepAfter - time.Minute)
	const backlog = spoolSweepBatch + 44
	for index := 0; index < backlog; index++ {
		claim := spoolTestClaim(fmt.Sprintf("backlog-%03d", index))
		if err := spool.ensureAttempt(ctx, claim); err != nil {
			t.Fatal(err)
		}
		if err := spool.sealIncomplete(ctx, claim.Lease.AttemptID, "test", contract.ErrorAttemptNotFound, sealedAt); err != nil {
			t.Fatal(err)
		}
	}
	outbox := &evidenceOutbox{spool: spool, clock: clock, retryInterval: DefaultLogRetryInterval,
		liveAttempts: map[string]struct{}{}, lateEvents: map[string]int{}}
	var reports []string
	report := func(err error) { reports = append(reports, err.Error()) }
	remaining := func() int {
		var rows int
		if err := spool.db.QueryRow(`SELECT COUNT(*) FROM spool_attempts`).Scan(&rows); err != nil {
			t.Fatal(err)
		}
		return rows
	}

	outbox.sweepDeadOneShotSpool(ctx, clock.Now(), report)
	if got := remaining(); got != backlog-spoolSweepBatch {
		t.Fatalf("rows after one full batch = %d, want %d", got, backlog-spoolSweepBatch)
	}
	// Due again at once: the batch was full.
	outbox.sweepDeadOneShotSpool(ctx, clock.Now(), report)
	if got := remaining(); got != 0 {
		t.Fatalf("rows after the backlog = %d, want 0", got)
	}
	want := []string{
		fmt.Sprintf("swept %d one-shot spool rows older than %s (%d incomplete-evidence tombstones, 0 undelivered completions L1 can no longer record as results)",
			spoolSweepBatch, DefaultLogSpoolSweepAfter, spoolSweepBatch),
		fmt.Sprintf("swept %d one-shot spool rows older than %s (%d incomplete-evidence tombstones, 0 undelivered completions L1 can no longer record as results)",
			44, DefaultLogSpoolSweepAfter, 44),
	}
	if strings.Join(reports, "\n") != strings.Join(want, "\n") {
		t.Fatalf("reports = %q, want %q", reports, want)
	}

	// A partial batch waits for the interval before the next sweep.
	claim := spoolTestClaim("after-backlog")
	if err := spool.ensureAttempt(ctx, claim); err != nil {
		t.Fatal(err)
	}
	if err := spool.sealIncomplete(ctx, claim.Lease.AttemptID, "test", contract.ErrorAttemptNotFound, sealedAt); err != nil {
		t.Fatal(err)
	}
	outbox.sweepDeadOneShotSpool(ctx, clock.Now().Add(spoolSweepInterval-time.Second), report)
	if !spoolRowExists(t, spool, claim.Lease.AttemptID) {
		t.Fatal("the sweep ran before its interval")
	}
	outbox.sweepDeadOneShotSpool(ctx, clock.Now().Add(spoolSweepInterval), report)
	if spoolRowExists(t, spool, claim.Lease.AttemptID) {
		t.Fatal("the sweep did not run after its interval")
	}
}
