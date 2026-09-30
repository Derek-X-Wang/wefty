package agent

import (
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

// storeClockCompletion stores a one-shot completion that finished at the
// outbox clock's now.
func storeClockCompletion(t *testing.T, outbox *evidenceOutbox, clock *manualClock, attemptID string) {
	t.Helper()
	if err := outbox.ensureAttempt(t.Context(), spoolTestClaim(attemptID)); err != nil {
		t.Fatal(err)
	}
	exitCode := 0
	if err := outbox.storeCompletion(t.Context(), attemptID, l1.ProcessResult{ExitCode: &exitCode}, clock.Now()); err != nil {
		t.Fatal(err)
	}
}

// waitForArmedRetry waits until recovery has taken in its last answer and
// armed its next retry: some timer is due after the clock's now. Only then
// may a test advance the clock. Advanced any earlier, while an ask is still
// in flight, recovery would arm its retry after the advance, relative to the
// new now, and the next advance would be the first to fire it.
func waitForArmedRetry(t *testing.T, clock *manualClock) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		clock.mu.Lock()
		armed := false
		for _, timer := range clock.timers {
			if timer.active && timer.deadline.After(clock.now) {
				armed = true
				break
			}
		}
		clock.mu.Unlock()
		if armed {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("recovery never armed its next retry")
		}
		time.Sleep(time.Millisecond)
	}
}

// advanceRecoveryDays drives recovery through days of simulated time, one
// day at a time, each to quiescence: the retry armed before the day is
// advanced past, and the day's ask of attemptID seen before the next. It
// asserts at least one ask per day, never a total that depends on how fast
// the runner schedules recovery.
func advanceRecoveryDays(t *testing.T, clock *manualClock, counter *completionCounter, attemptID string, days int) {
	t.Helper()
	for day := 1; day <= days; day++ {
		waitForArmedRetry(t, clock)
		calls := counter.count(attemptID)
		clock.Advance(24 * time.Hour)
		waitForCompletionCalls(t, counter, attemptID, calls+1)
	}
	waitForArmedRetry(t, clock)
}

func waitForSpoolRowGone(t *testing.T, outbox *evidenceOutbox, attemptID string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for spoolRowExists(t, outbox.spool, attemptID) {
		if time.Now().After(deadline) {
			t.Fatalf("spool row %s was not swept", attemptID)
		}
		time.Sleep(time.Millisecond)
	}
}

// #52 S3 review P1: L1's late-evidence window runs from when L1 records the
// loss, not from when the process finished, so an L1 that is unreachable for
// days still takes the completion when it returns. A completion L1 has not
// answered is kept, however long the outage, and delivered afterwards.
func TestRecoveryKeepsAnUnansweredCompletionThroughAFiveDayOutage(t *testing.T) {
	const attemptID = "attempt-through-outage"
	clock := newManualClock(time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC))
	var offline atomic.Bool
	offline.Store(true)
	counter := newCompletionCounter(nil)
	counter.decide = func(string) contract.ErrorCode {
		if offline.Load() {
			return contract.ErrorInternal
		}
		return ""
	}
	client, stopServer := startEvidenceReplayServer(t, counter, time.Second)
	defer stopServer()
	defer client.Close()
	outbox, err := newEvidenceOutbox(t.TempDir(), "outage-node", 1<<20, clock, 8, time.Hour, 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer outbox.Close()
	storeClockCompletion(t, outbox, clock, attemptID)
	outbox.startRecovery(t.Context(), client, func(error) {})
	waitForCompletionCalls(t, counter, attemptID, 1)

	// Five days of an unavailable L1: every recovery pass, and every sweep
	// it carries, runs while nothing has answered.
	advanceRecoveryDays(t, clock, counter, attemptID, 5)
	waitForCompletionState(t, outbox, attemptID, "durable_completion")
	assertAttemptPending(t, outbox, attemptID)

	offline.Store(false)
	clock.Advance(time.Minute) // past the capped transient backoff
	waitForCompletionState(t, outbox, attemptID, "delivered")
}

// A delivery L1 answers for good is swept on the next sweep; an answer that
// is not a refusal for good -- a parked node-session refusal (#549) -- keeps
// the row however long it lasts, and an answer past L1's late-evidence
// window is not a refusal at all: L1 keeps a gap and answers lease_expired,
// and the row is retired as delivered. Service rows are never swept.
func TestRecoverySweepsOnlyWhatL1RefusedForGood(t *testing.T) {
	const (
		refused  = "attempt-l1-refused"
		parked   = "attempt-l1-parked"
		tooLate  = "attempt-past-window"
		service  = "attempt-service-refused"
		barrier1 = "attempt-barrier-1"
	)
	clock := newManualClock(time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC))
	counter := newCompletionCounter(map[string][]contract.ErrorCode{
		refused: {contract.ErrorAttemptNotFound},
		parked:  {contract.ErrorNodeSessionReplaced},
		tooLate: {contract.ErrorLeaseExpired},
		service: {contract.ErrorAttemptNotFound},
	})
	client, stopServer := startEvidenceReplayServer(t, counter, time.Second)
	defer stopServer()
	defer client.Close()
	outbox, err := newEvidenceOutbox(t.TempDir(), "refusal-node", 1<<20, clock, 8, time.Hour, 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer outbox.Close()
	for _, attemptID := range []string{refused, parked, tooLate} {
		storeClockCompletion(t, outbox, clock, attemptID)
	}
	if err := outbox.ensureAttempt(t.Context(), serviceSpoolTestClaim(service)); err != nil {
		t.Fatal(err)
	}
	exitCode := 0
	if err := outbox.storeCompletion(t.Context(), service, l1.ProcessResult{ExitCode: &exitCode}, clock.Now()); err != nil {
		t.Fatal(err)
	}
	reports := make(chan error, 64)
	outbox.startRecovery(t.Context(), client, func(err error) { reports <- err })
	waitForCompletionState(t, outbox, refused, "sealed_incomplete")
	waitForCompletionState(t, outbox, service, "sealed_incomplete")
	waitForCompletionState(t, outbox, tooLate, "delivered")
	waitForCompletionCalls(t, counter, parked, 1)

	// The next sweep, an hour on, takes the refused one-shot only.
	clock.Advance(time.Hour)
	deliverBarrier(t, outbox, barrier1)
	waitForSpoolRowGone(t, outbox, refused)
	for _, kept := range []string{parked, service} {
		if !spoolRowExists(t, outbox.spool, kept) {
			t.Fatalf("%s was swept", kept)
		}
	}
	// Five more days of the parked refusal: still kept, still asked every
	// day.
	advanceRecoveryDays(t, clock, counter, parked, 5)
	waitForCompletionState(t, outbox, parked, "durable_completion")
	assertAttemptPending(t, outbox, parked)
	if !spoolRowExists(t, outbox.spool, service) {
		t.Fatal("a service row was swept")
	}
	sweptReported := false
	for len(reports) > 0 {
		if strings.Contains((<-reports).Error(), "swept 1 one-shot spool rows whose evidence L1 refused for good") {
			sweptReported = true
		}
	}
	if !sweptReported {
		t.Fatal("the sweep of the refused row was not logged")
	}
}

// #52 S3 review round 2: no age sweeps anything. A completion L1 has not
// answered for sixty days -- past L1's one-shot log retention, which covers
// logs, not results -- is still kept, and delivered when L1 returns.
func TestRecoveryKeepsAnUnansweredCompletionForSixtyDays(t *testing.T) {
	const attemptID = "attempt-sixty-day-outage"
	clock := newManualClock(time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC))
	var offline atomic.Bool
	offline.Store(true)
	counter := newCompletionCounter(nil)
	counter.decide = func(string) contract.ErrorCode {
		if offline.Load() {
			return contract.ErrorInternal
		}
		return ""
	}
	client, stopServer := startEvidenceReplayServer(t, counter, time.Second)
	defer stopServer()
	defer client.Close()
	outbox, err := newEvidenceOutbox(t.TempDir(), "long-outage-node", 1<<20, clock, 8, time.Hour, 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer outbox.Close()
	storeClockCompletion(t, outbox, clock, attemptID)
	reports := make(chan error, 256)
	outbox.startRecovery(t.Context(), client, func(err error) { reports <- err })
	waitForCompletionCalls(t, counter, attemptID, 1)
	advanceRecoveryDays(t, clock, counter, attemptID, 60)
	waitForCompletionState(t, outbox, attemptID, "durable_completion")
	assertAttemptPending(t, outbox, attemptID)

	offline.Store(false)
	clock.Advance(time.Minute) // past the capped transient backoff
	waitForCompletionState(t, outbox, attemptID, "delivered")
	for len(reports) > 0 {
		if report := (<-reports).Error(); strings.Contains(report, "swept") {
			t.Fatalf("recovery swept something during the outage: %s", report)
		}
	}
}
