package agent

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/testwait"
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

// recoveryDriver moves a manual clock for a recovery loop that keeps asking
// L1 about one attempt, and moves it only between recovery passes. A test
// that moves the clock from its own goroutine can land inside a pass: the pass
// reads the time, the move fires the retry timer, and the pass then replaces
// that timer with one armed from the old time -- due at once, or a whole move
// late -- which a manual clock fires only when it next moves. The attempt is
// never asked again (#921). The driver moves the clock from the recovery
// goroutine instead, at the end of a pass that has taken in the attempt's last
// answer and armed its retry. No pass is running then, and every later pass
// reads the new time.
type recoveryDriver struct {
	clock     *manualClock
	outbox    *evidenceOutbox
	counter   *completionCounter
	attemptID string

	mu   sync.Mutex
	move *recoveryMove
}

type recoveryMove struct {
	by   time.Duration
	done chan recoveryMoved
}

type recoveryMoved struct {
	asked int // how many times L1 had been asked about the attempt
	err   error
}

// newRecoveryDriver hooks a driver into outbox's recovery loop. Call it before
// startRecovery; from then on only the driver moves the clock.
func newRecoveryDriver(clock *manualClock, outbox *evidenceOutbox, counter *completionCounter, attemptID string) *recoveryDriver {
	driver := &recoveryDriver{clock: clock, outbox: outbox, counter: counter, attemptID: attemptID}
	outbox.recoveryScanFinished = driver.passFinished
	return driver
}

// advance moves the clock by duration once recovery has taken in the
// attempt's last answer and armed its next retry, and returns how many times
// L1 had been asked about the attempt when the clock moved.
func (driver *recoveryDriver) advance(t *testing.T, duration time.Duration) int {
	t.Helper()
	move := &recoveryMove{by: duration, done: make(chan recoveryMoved, 1)}
	driver.mu.Lock()
	driver.move = move
	// An idle loop ends no pass by itself; this wake makes it run one.
	driver.outbox.scheduleRecovery()
	driver.mu.Unlock()
	select {
	case moved := <-move.done:
		if moved.err != nil {
			t.Fatal(moved.err)
		}
		return moved.asked
	case <-time.After(30 * time.Second):
		driver.mu.Lock()
		if driver.move == move {
			driver.move = nil
		}
		driver.mu.Unlock()
		t.Fatalf("recovery never armed the next retry of attempt %s", driver.attemptID)
		return 0
	}
}

// passFinished runs on the recovery goroutine at the end of every pass, after
// the pass has armed its retry.
func (driver *recoveryDriver) passFinished() {
	driver.mu.Lock()
	defer driver.mu.Unlock()
	move := driver.move
	if move == nil || !retryArmed(driver.clock) {
		return
	}
	driver.move = nil
	// Recovery arms one timer, for its earliest retry, and only for attempts
	// still pending. With the attempt the only one pending, the armed retry is
	// its own: its answer is in.
	attempts, err := driver.outbox.spool.pendingAttempts(context.Background())
	if err == nil && (len(attempts) != 1 || attempts[0].attemptID != driver.attemptID) {
		err = fmt.Errorf("the armed retry may not be attempt %s's: pending replay is %+v", driver.attemptID, attempts)
	}
	if err != nil {
		move.done <- recoveryMoved{err: err}
		return
	}
	// The driver's wake has done its job. Dropped, it cannot ask about the
	// attempt in place of the retry the move is meant to fire.
	select {
	case <-driver.outbox.recoveryWake:
	default:
	}
	asked := driver.counter.count(driver.attemptID)
	driver.clock.Advance(move.by)
	move.done <- recoveryMoved{asked: asked}
}

// retryArmed reports whether a timer is armed to fire after the clock's now.
func retryArmed(clock *manualClock) bool {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	for _, timer := range clock.timers {
		if timer.active && timer.deadline.After(clock.now) {
			return true
		}
	}
	return false
}

// advanceRecoveryDays drives recovery through days of simulated time, one
// day at a time: each day starts once the attempt's last answer is in and its
// retry armed, and ends once L1 has been asked about the attempt again. It
// asserts at least one ask per day, never a total that depends on how fast
// the runner schedules recovery.
func advanceRecoveryDays(t *testing.T, driver *recoveryDriver, days int) {
	t.Helper()
	for day := 1; day <= days; day++ {
		asked := driver.advance(t, 24*time.Hour)
		waitForCompletionCalls(t, driver.counter, driver.attemptID, asked+1)
	}
}

func waitForSpoolRowGone(t *testing.T, outbox *evidenceOutbox, attemptID string) {
	t.Helper()
	testwait.Until(t, "swept spool row "+attemptID, func() (bool, error) {
		if !spoolRowExists(t, outbox.spool, attemptID) {
			return true, nil
		}
		return false, fmt.Errorf("spool row %s still exists", attemptID)
	})
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
	driver := newRecoveryDriver(clock, outbox, counter, attemptID)
	storeClockCompletion(t, outbox, clock, attemptID)
	outbox.startRecovery(t.Context(), client, func(error) {})
	waitForCompletionCalls(t, counter, attemptID, 1)

	// Five days of an unavailable L1: every recovery pass, and every sweep
	// it carries, runs while nothing has answered.
	advanceRecoveryDays(t, driver, 5)
	waitForCompletionState(t, outbox, attemptID, "durable_completion")
	assertAttemptPending(t, outbox, attemptID)

	offline.Store(false)
	driver.advance(t, time.Minute) // past the capped transient backoff
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
	driver := newRecoveryDriver(clock, outbox, counter, parked)
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
	driver.advance(t, time.Hour)
	deliverBarrier(t, outbox, barrier1)
	waitForSpoolRowGone(t, outbox, refused)
	for _, kept := range []string{parked, service} {
		if !spoolRowExists(t, outbox.spool, kept) {
			t.Fatalf("%s was swept", kept)
		}
	}
	// Five more days of the parked refusal: still kept, still asked every
	// day.
	advanceRecoveryDays(t, driver, 5)
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
	driver := newRecoveryDriver(clock, outbox, counter, attemptID)
	storeClockCompletion(t, outbox, clock, attemptID)
	reports := make(chan error, 256)
	outbox.startRecovery(t.Context(), client, func(err error) { reports <- err })
	waitForCompletionCalls(t, counter, attemptID, 1)
	advanceRecoveryDays(t, driver, 60)
	waitForCompletionState(t, outbox, attemptID, "durable_completion")
	assertAttemptPending(t, outbox, attemptID)

	offline.Store(false)
	driver.advance(t, time.Minute) // past the capped transient backoff
	waitForCompletionState(t, outbox, attemptID, "delivered")
	for len(reports) > 0 {
		if report := (<-reports).Error(); strings.Contains(report, "swept") {
			t.Fatalf("recovery swept something during the outage: %s", report)
		}
	}
}
