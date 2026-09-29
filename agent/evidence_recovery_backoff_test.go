package agent

import (
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

// completionCounter answers /complete per attempt from a script: call n gets
// answers[attempt][n], the last entry repeating, and an empty code is success.
// An attempt with no script always succeeds; decide, when set, answers instead
// of the scripts. It counts every request.
type completionCounter struct {
	mu      sync.Mutex
	calls   map[string]int
	answers map[string][]contract.ErrorCode
	decide  func(attemptID string) contract.ErrorCode
}

func newCompletionCounter(answers map[string][]contract.ErrorCode) *completionCounter {
	return &completionCounter{calls: make(map[string]int), answers: answers}
}

func (counter *completionCounter) count(attemptID string) int {
	counter.mu.Lock()
	defer counter.mu.Unlock()
	return counter.calls[attemptID]
}

func (counter *completionCounter) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	if !strings.HasSuffix(request.URL.Path, "/complete") {
		http.NotFound(w, request)
		return
	}
	segments := strings.Split(request.URL.Path, "/")
	attemptID := segments[len(segments)-2]
	counter.mu.Lock()
	call := counter.calls[attemptID]
	counter.calls[attemptID]++
	var code contract.ErrorCode
	if counter.decide != nil {
		code = counter.decide(attemptID)
	} else if script := counter.answers[attemptID]; len(script) > 0 {
		code = script[min(call, len(script)-1)]
	}
	counter.mu.Unlock()
	if code == "" {
		_ = json.NewEncoder(w).Encode(l1.Job{})
		return
	}
	status := http.StatusConflict
	if code == contract.ErrorInternal {
		status = http.StatusInternalServerError
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{
		Code: code, Message: "scripted L1 answer",
	}})
}

func storeRecoveryCompletion(t *testing.T, outbox *evidenceOutbox, attemptID string) {
	t.Helper()
	claim := spoolTestClaim(attemptID)
	if err := outbox.ensureAttempt(t.Context(), claim); err != nil {
		t.Fatal(err)
	}
	exitCode := 0
	if err := outbox.storeCompletion(t.Context(), attemptID, l1.ProcessResult{ExitCode: &exitCode}, time.Now()); err != nil {
		t.Fatal(err)
	}
}

func waitForCompletionState(t *testing.T, outbox *evidenceOutbox, attemptID, want string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		receipt := outbox.spool.inspectCompletion(t.Context(), attemptID)
		if receipt.State == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("attempt %s completion receipt = %+v, want %s", attemptID, receipt, want)
		}
		time.Sleep(time.Millisecond)
	}
}

// deliverBarrier stores a fresh completion, wakes the reconciler and waits for
// it to be delivered. Delivery is a pass over the whole spool, so any attempt
// the reconciler would still ask for is asked in that same pass.
func deliverBarrier(t *testing.T, outbox *evidenceOutbox, attemptID string) {
	t.Helper()
	storeRecoveryCompletion(t, outbox, attemptID)
	outbox.scheduleRecovery()
	waitForCompletionState(t, outbox, attemptID, "delivered")
}

func receiveReport(t *testing.T, reports <-chan error, want ...string) {
	t.Helper()
	select {
	case err := <-reports:
		for _, fragment := range want {
			if !strings.Contains(err.Error(), fragment) {
				t.Fatalf("report %q does not contain %q", err, fragment)
			}
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("no report containing %q", want)
	}
}

func waitForCompletionCalls(t *testing.T, counter *completionCounter, attemptID string, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for counter.count(attemptID) < want {
		if time.Now().After(deadline) {
			t.Fatalf("attempt %s was asked %d times, want %d", attemptID, counter.count(attemptID), want)
		}
		time.Sleep(time.Millisecond)
	}
}

// #549: a node whose registration moved on kept asking L1 to replay a
// completion L1 had already accepted from the older registration, ten times a
// second, forever, one log line each. Now a refusal is re-checked on a
// doubling schedule -- 10s, 20s, ... capped at an hour -- that never gives up, with one
// log line for the whole run of identical refusals, the evidence untouched on
// disk, and the next agent process asking again.
func TestEvidenceRecoveryRechecksRefusalSlowlyAndLogsOnce(t *testing.T) {
	for _, code := range []contract.ErrorCode{
		contract.ErrorNodeSessionReplaced, contract.ErrorIdentityBound, contract.ErrorPrincipalForbidden,
	} {
		t.Run(string(code), func(t *testing.T) {
			const refusedAttempt = "attempt-refused"
			counter := newCompletionCounter(map[string][]contract.ErrorCode{refusedAttempt: {code}})
			client, stopServer := startEvidenceReplayServer(t, counter, time.Second)
			defer stopServer()
			defer client.Close()
			directory := t.TempDir()
			clock := newManualClock(time.Date(2026, 9, 23, 3, 13, 0, 0, time.UTC))
			outbox, err := newEvidenceOutbox(directory, "stable-node", 1024*1024, clock, 8, time.Hour, 100*time.Millisecond)
			if err != nil {
				t.Fatal(err)
			}
			storeRecoveryCompletion(t, outbox, refusedAttempt)

			var reported atomic.Int32
			reports := make(chan error, 64)
			outbox.startRecovery(t.Context(), client, func(err error) {
				reported.Add(1)
				reports <- err
			})
			receiveReport(t, reports, string(code), refusedAttempt, "re-checking from 10s", "hourly")

			delays := []time.Duration{
				10 * time.Second, 20 * time.Second, 40 * time.Second, 80 * time.Second, 160 * time.Second,
				320 * time.Second, 640 * time.Second, 1280 * time.Second, 2560 * time.Second,
				time.Hour, time.Hour, time.Hour,
			}
			for index, delay := range delays {
				// Armed at exactly this delay, and not asked before it.
				clock.waitForDeadline(t, clock.Now().Add(delay))
				if calls := counter.count(refusedAttempt); calls != index+1 {
					t.Fatalf("before re-check %d L1 was asked %d times", index+1, calls)
				}
				clock.Advance(delay)
				waitForCompletionCalls(t, counter, refusedAttempt, index+2)
			}
			// A full pass right after a re-check does not ask again early.
			deliverBarrier(t, outbox, "attempt-barrier")
			if calls := counter.count(refusedAttempt); calls != len(delays)+1 {
				t.Fatalf("refused attempt was asked %d times, want %d", calls, len(delays)+1)
			}
			if got := reported.Load(); got != 1 {
				t.Fatalf("a run of identical refusals produced %d log lines, want 1", got)
			}
			// Untouched: neither delivered nor sealed, still pending replay.
			waitForCompletionState(t, outbox, refusedAttempt, "durable_completion")
			assertAttemptPending(t, outbox, refusedAttempt)
			if err := outbox.Close(); err != nil {
				t.Fatal(err)
			}

			// The next agent process owns the evidence and asks again.
			restarted, err := newEvidenceOutbox(directory, "stable-node", 1024*1024, clock, 8, time.Hour, 100*time.Millisecond)
			if err != nil {
				t.Fatal(err)
			}
			defer restarted.Close()
			restartReports := make(chan error, 64)
			restarted.startRecovery(t.Context(), client, func(err error) { restartReports <- err })
			receiveReport(t, restartReports, string(code), "re-checking from 10s")
			if calls := counter.count(refusedAttempt); calls != len(delays)+2 {
				t.Fatalf("restarted agent asked %d times in total, want %d", calls, len(delays)+2)
			}
			waitForCompletionState(t, restarted, refusedAttempt, "durable_completion")
		})
	}
}

// refusedUntilLeaseScenario plays the case the re-checks exist for: L1
// refuses a completion it has not accepted while the replaced registration's
// lease (lease, from the first refusal) is still running, then answers
// lease_expired and keeps the result as late evidence -- but only for window
// after the lease ran out; later it would keep a gap instead. It drives the
// injected clock through the re-check schedule until the completion lands and
// returns when that was, and whether it landed as a gap.
func refusedUntilLeaseScenario(t *testing.T, lease, window time.Duration) (landedAfter time.Duration, gap bool) {
	t.Helper()
	const attemptID = "attempt-replaced-in-lease"
	clock := newManualClock(time.Date(2026, 9, 23, 3, 13, 0, 0, time.UTC))
	start := clock.Now()
	var gapped atomic.Bool
	counter := newCompletionCounter(nil)
	counter.decide = func(string) contract.ErrorCode {
		now := clock.Now()
		if now.Before(start.Add(lease)) {
			return contract.ErrorNodeSessionReplaced
		}
		if now.After(start.Add(lease + window)) {
			gapped.Store(true)
		}
		return contract.ErrorLeaseExpired
	}
	client, stopServer := startEvidenceReplayServer(t, counter, time.Second)
	defer stopServer()
	defer client.Close()
	outbox, err := newEvidenceOutbox(t.TempDir(), "stable-node", 1024*1024, clock, 8, time.Hour, 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer outbox.Close()
	storeRecoveryCompletion(t, outbox, attemptID)
	var reported atomic.Int32
	reports := make(chan error, 64)
	outbox.startRecovery(t.Context(), client, func(err error) {
		reported.Add(1)
		reports <- err
	})
	receiveReport(t, reports, string(contract.ErrorNodeSessionReplaced), "re-checking from")
	for refusals := 1; ; refusals++ {
		if refusals > 64 {
			t.Fatal("completion never landed")
		}
		delay := evidenceRecoveryRefusalDelay(refusals)
		clock.waitForDeadline(t, clock.Now().Add(delay))
		// Still refused and untouched while waiting.
		waitForCompletionState(t, outbox, attemptID, "durable_completion")
		clock.Advance(delay)
		waitForCompletionCalls(t, counter, attemptID, refusals+1)
		if !clock.Now().Before(start.Add(lease)) {
			break
		}
	}
	waitForCompletionState(t, outbox, attemptID, "delivered")
	receiveReport(t, reports, attemptID, "now succeeded")
	if got := reported.Load(); got != 2 {
		t.Fatalf("%d log lines, want 2 (first refusal, then success)", got)
	}
	return clock.Now().Sub(start), gapped.Load()
}

// A lease longer than the early re-checks: they keep going and land it.
func TestEvidenceRecoveryRefusalLandsAfterLongLease(t *testing.T) {
	landed, gap := refusedUntilLeaseScenario(t, 5*time.Minute, 48*time.Hour)
	// 10s, 30s, 70s, 150s, 310s.
	if gap || landed != 310*time.Second {
		t.Fatalf("landed after %s (gap=%t), want 310s as late evidence", landed, gap)
	}
}

// An L1 run with the default 30 s lease and a one-minute late-evidence window
// (a supported configuration) keeps the result only until 90 s after the
// first refusal. The early re-checks land it inside that window, including
// when the lease runs out just after a re-check.
func TestEvidenceRecoveryRefusalLandsInsideShortLateEvidenceWindow(t *testing.T) {
	for _, tc := range []struct {
		lease time.Duration
		want  time.Duration
	}{
		{lease: 30 * time.Second, want: 30 * time.Second},
		{lease: 31 * time.Second, want: 70 * time.Second},
	} {
		t.Run(tc.lease.String(), func(t *testing.T) {
			landed, gap := refusedUntilLeaseScenario(t, tc.lease, time.Minute)
			if gap || landed != tc.want {
				t.Fatalf("landed after %s (gap=%t), want %s as late evidence", landed, gap, tc.want)
			}
		})
	}
}

func assertAttemptPending(t *testing.T, outbox *evidenceOutbox, attemptID string) {
	t.Helper()
	attempts, err := outbox.spool.pendingAttempts(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for _, attempt := range attempts {
		if attempt.attemptID == attemptID {
			return
		}
	}
	t.Fatalf("attempt %s is no longer pending replay: %+v", attemptID, attempts)
}

// A transient failure is still retried until it clears, but each retry waits
// twice as long as the one before, up to a ceiling, instead of hammering L1
// at the retry interval.
func TestEvidenceRecoveryBacksOffTransientFailuresToCeiling(t *testing.T) {
	const attemptID = "attempt-transient"
	// Twelve transient failures, then L1 accepts.
	script := make([]contract.ErrorCode, 12, 13)
	for i := range script {
		script[i] = contract.ErrorInternal
	}
	counter := newCompletionCounter(map[string][]contract.ErrorCode{attemptID: append(script, "")})
	client, stopServer := startEvidenceReplayServer(t, counter, time.Second)
	defer stopServer()
	defer client.Close()
	clock := newManualClock(time.Date(2026, 9, 23, 3, 13, 0, 0, time.UTC))
	retryInterval := 100 * time.Millisecond
	outbox, err := newEvidenceOutbox(t.TempDir(), "stable-node", 1024*1024, clock, 8, time.Hour, retryInterval)
	if err != nil {
		t.Fatal(err)
	}
	defer outbox.Close()
	storeRecoveryCompletion(t, outbox, attemptID)
	reports := make(chan error, 64)
	outbox.startRecovery(t.Context(), client, func(err error) { reports <- err })

	// 100ms, 200ms, ... 25.6s, then the 30s ceiling from the ninth retry on.
	want := []time.Duration{
		100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond,
		1600 * time.Millisecond, 3200 * time.Millisecond, 6400 * time.Millisecond, 12800 * time.Millisecond,
		25600 * time.Millisecond, 30 * time.Second, 30 * time.Second, 30 * time.Second,
	}
	for failure, delay := range want {
		select {
		case err := <-reports:
			if !strings.Contains(err.Error(), "internal") {
				t.Fatalf("failure %d report = %v", failure+1, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("transient failure %d was not retried", failure+1)
		}
		// The next retry is armed at exactly this delay and not before it.
		clock.waitForDeadline(t, clock.Now().Add(delay))
		if calls := counter.count(attemptID); calls != failure+1 {
			t.Fatalf("after failure %d L1 was asked %d times", failure+1, calls)
		}
		clock.Advance(delay)
	}
	waitForCompletionState(t, outbox, attemptID, "delivered")
	if calls := counter.count(attemptID); calls != len(want)+1 {
		t.Fatalf("L1 was asked %d times, want %d", calls, len(want)+1)
	}
}

func TestEvidenceRecoveryBackoffIsCapped(t *testing.T) {
	for _, tc := range []struct {
		interval time.Duration
		failures int
		want     time.Duration
	}{
		{100 * time.Millisecond, 1, 100 * time.Millisecond},
		{100 * time.Millisecond, 2, 200 * time.Millisecond},
		{100 * time.Millisecond, 9, 25600 * time.Millisecond},
		{100 * time.Millisecond, 10, maxEvidenceRecoveryBackoff},
		{100 * time.Millisecond, 1 << 20, maxEvidenceRecoveryBackoff},
		// An operator interval above the ceiling is honoured, never shortened.
		{time.Minute, 1, time.Minute},
		{time.Minute, 5, time.Minute},
	} {
		if got := evidenceRecoveryBackoff(tc.interval, tc.failures); got != tc.want {
			t.Errorf("evidenceRecoveryBackoff(%s, %d) = %s, want %s", tc.interval, tc.failures, got, tc.want)
		}
	}
	for refusals, want := range map[int]time.Duration{
		1: 10 * time.Second, 2: 20 * time.Second, 9: 2560 * time.Second, 10: time.Hour, 1 << 20: time.Hour,
	} {
		if got := evidenceRecoveryRefusalDelay(refusals); got != want {
			t.Errorf("evidenceRecoveryRefusalDelay(%d) = %s, want %s", refusals, got, want)
		}
	}
}
