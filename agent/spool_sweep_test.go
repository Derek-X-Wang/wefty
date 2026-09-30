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

// #52 S3: one-shot spool rows L1 can no longer take were kept forever. The
// sweep removes a row only once L1 has refused its evidence for good, or, as
// a disk-safety backstop, once a row L1 never refused is 30 days old.
// Everything L1 may still accept, services, and attempts this process still
// owns stay.
func TestSpoolSweepRemovesOnlyRefusedOrBackstopRows(t *testing.T) {
	directory := t.TempDir()
	spool := openTestLogSpool(t, directory, "node-sweep", 1<<20)
	ctx := context.Background()
	now := time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)
	backstop := now.Add(-DefaultLogSpoolBackstopAge)
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
	seal := func(attemptID string, code contract.ErrorCode, at time.Time) {
		t.Helper()
		if err := spool.sealIncomplete(ctx, attemptID, "test", code, at); err != nil {
			t.Fatal(err)
		}
	}

	// Refused for good a minute ago: swept at once.
	refused := claimRow(spoolTestClaim("refused"))
	withEvents(refused)
	finish(refused, now.Add(-time.Hour))
	seal(refused, contract.ErrorAttemptNotFound, now.Add(-time.Minute))
	refusedConflict := claimRow(spoolTestClaim("refused-conflict"))
	seal(refusedConflict, contract.ErrorConflict, now.Add(-time.Minute))
	// Never answered for five days: L1 may still take it.
	unanswered := claimRow(spoolTestClaim("unanswered"))
	withEvents(unanswered)
	finish(unanswered, now.Add(-5*24*time.Hour))
	// Sealed on an answer that is not a refusal for good.
	notOwned := claimRow(spoolTestClaim("not-owned"))
	seal(notOwned, contract.ErrorAttemptNotOwned, now.Add(-5*24*time.Hour))
	pendingOnly := claimRow(spoolTestClaim("pending-only"))
	withEvents(pendingOnly)
	// Past the backstop and never refused: swept, as backstop.
	backstopCompletion := claimRow(spoolTestClaim("backstop-completion"))
	withEvents(backstopCompletion)
	finish(backstopCompletion, backstop.Add(-time.Minute))
	backstopTombstone := claimRow(spoolTestClaim("backstop-tombstone"))
	seal(backstopTombstone, contract.ErrorAttemptNotOwned, backstop.Add(-time.Minute))
	// Services are never swept, refused or old.
	serviceRefused := claimRow(serviceSpoolTestClaim("service-refused"))
	seal(serviceRefused, contract.ErrorAttemptNotFound, now.Add(-time.Minute))
	serviceOld := claimRow(serviceSpoolTestClaim("service-old"))
	finish(serviceOld, backstop.Add(-24*time.Hour))
	liveRefused := claimRow(spoolTestClaim("live-refused"))
	seal(liveRefused, contract.ErrorAttemptNotFound, now.Add(-time.Minute))

	// Tombstones sealed before the refusal was recorded on the row get it
	// from their own document.
	legacyRefused := claimRow(spoolTestClaim("legacy-refused"))
	seal(legacyRefused, contract.ErrorStaleFence, now.Add(-time.Hour))
	legacyNotOwned := claimRow(spoolTestClaim("legacy-not-owned"))
	seal(legacyNotOwned, contract.ErrorAttemptNotOwned, now.Add(-time.Hour))
	if _, err := spool.db.Exec(`UPDATE spool_attempts SET sealed_ns=NULL, l1_refused_ns=NULL, l1_refusal_code=NULL
		WHERE attempt_id IN (?, ?)`, legacyRefused, legacyNotOwned); err != nil {
		t.Fatal(err)
	}
	if err := spool.Close(); err != nil {
		t.Fatal(err)
	}
	spool = openTestLogSpool(t, directory, "node-sweep", 1<<20)
	defer spool.Close()
	var code string
	if err := spool.db.QueryRow(`SELECT l1_refusal_code FROM spool_attempts WHERE attempt_id=?`, refused).Scan(&code); err != nil {
		t.Fatal(err)
	}
	if code != string(contract.ErrorAttemptNotFound) {
		t.Fatalf("recorded refusal = %q", code)
	}

	live := func(attemptID string) bool { return attemptID == liveRefused }
	sweep, err := spool.sweepDeadOneShotAttempts(ctx, backstop, spoolSweepBatch, live)
	if err != nil {
		t.Fatal(err)
	}
	if sweep.refused != 3 || fmt.Sprint(sweep.backstop) != fmt.Sprint([]string{backstopCompletion, backstopTombstone}) || sweep.full {
		t.Fatalf("sweep = %+v, want 3 refused and the two backstop rows", sweep)
	}
	for _, gone := range []string{refused, refusedConflict, legacyRefused, backstopCompletion, backstopTombstone} {
		if spoolRowExists(t, spool, gone) {
			t.Fatalf("%s survived the sweep", gone)
		}
	}
	for _, kept := range []string{unanswered, notOwned, pendingOnly, serviceRefused, serviceOld, liveRefused, legacyNotOwned} {
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
	attempts, err := spool.pendingAttempts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	pending := map[string]bool{}
	for _, attempt := range attempts {
		pending[attempt.attemptID] = true
	}
	if !pending[unanswered] || !pending[pendingOnly] {
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
	const backlog = spoolSweepBatch + 44
	for index := 0; index < backlog; index++ {
		claim := spoolTestClaim(fmt.Sprintf("backlog-%03d", index))
		if err := spool.ensureAttempt(ctx, claim); err != nil {
			t.Fatal(err)
		}
		if err := spool.sealIncomplete(ctx, claim.Lease.AttemptID, "test", contract.ErrorAttemptNotFound, clock.Now()); err != nil {
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
		fmt.Sprintf("swept %d one-shot spool rows whose evidence L1 refused for good", spoolSweepBatch),
		"swept 44 one-shot spool rows whose evidence L1 refused for good",
	}
	if strings.Join(reports, "\n") != strings.Join(want, "\n") {
		t.Fatalf("reports = %q, want %q", reports, want)
	}

	// A partial batch waits for the interval before the next sweep.
	claim := spoolTestClaim("after-backlog")
	if err := spool.ensureAttempt(ctx, claim); err != nil {
		t.Fatal(err)
	}
	if err := spool.sealIncomplete(ctx, claim.Lease.AttemptID, "test", contract.ErrorAttemptNotFound, clock.Now()); err != nil {
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

func TestL1ClosedEvidenceCodes(t *testing.T) {
	for _, code := range []contract.ErrorCode{
		contract.ErrorAttemptNotFound, contract.ErrorNotFound, contract.ErrorStaleFence, contract.ErrorAttemptMismatch,
		contract.ErrorConflict, contract.ErrorIdempotencyConflict, contract.ErrorInvalidRequest,
	} {
		if !l1ClosedEvidence(code) {
			t.Fatalf("%s is not a refusal for good", code)
		}
	}
	for _, code := range []contract.ErrorCode{
		"", contract.ErrorInternal, contract.ErrorLeaseExpired, contract.ErrorAttemptNotOwned,
		contract.ErrorNodeSessionReplaced, contract.ErrorIdentityBound, contract.ErrorPrincipalForbidden,
		contract.ErrorNodeNotRegistered, contract.ErrorNodeDead, contract.ErrorNodeDraining,
	} {
		if l1ClosedEvidence(code) {
			t.Fatalf("%q must not let the sweep take a row", code)
		}
	}
}
