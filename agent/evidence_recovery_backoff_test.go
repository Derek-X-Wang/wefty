package agent

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

// completionCounter answers /complete per attempt: refusedCode for any
// attempt listed in refused, success for everything else. It counts every
// completion request by attempt.
type completionCounter struct {
	mu      sync.Mutex
	calls   map[string]int
	refused map[string]contract.ErrorCode
}

func newCompletionCounter(refused map[string]contract.ErrorCode) *completionCounter {
	return &completionCounter{calls: make(map[string]int), refused: refused}
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
	counter.calls[attemptID]++
	code, refused := counter.refused[attemptID]
	counter.mu.Unlock()
	if !refused {
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
		Code: code, Message: "attempt authority belongs to a replaced node registration",
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

// #549: a node whose registration moved on kept asking L1 to replay a
// completion L1 had already accepted from the older registration, ten times a
// second, forever. The refusal is one log line and one request for the life of
// the agent process, the evidence stays on disk untouched, and the next agent
// process asks exactly once more.
func TestEvidenceRecoveryStopsAskingAfterReplacedSessionRefusal(t *testing.T) {
	for _, code := range []contract.ErrorCode{
		contract.ErrorNodeSessionReplaced, contract.ErrorIdentityBound, contract.ErrorPrincipalForbidden,
	} {
		t.Run(string(code), func(t *testing.T) {
			const refusedAttempt = "attempt-refused"
			counter := newCompletionCounter(map[string]contract.ErrorCode{refusedAttempt: code})
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
			select {
			case err := <-reports:
				if !strings.Contains(err.Error(), string(code)) || !strings.Contains(err.Error(), refusedAttempt) {
					t.Fatalf("refusal report = %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("refusal was not reported")
			}

			// Each round moves the injected clock far past any backoff and
			// wakes the reconciler with fresh work it must deliver. That
			// delivery is a pass over the whole spool, so a refused attempt
			// that was still being retried would be asked again in it.
			for round := range 4 {
				clock.Advance(10 * maxEvidenceRecoveryBackoff)
				barrier := fmt.Sprintf("attempt-barrier-%d", round)
				storeRecoveryCompletion(t, outbox, barrier)
				outbox.scheduleRecovery()
				waitForCompletionState(t, outbox, barrier, "delivered")
			}
			if calls := counter.count(refusedAttempt); calls != 1 {
				t.Fatalf("refused attempt was asked %d times in one session, want 1", calls)
			}
			if got := reported.Load(); got != 1 {
				t.Fatalf("refusal produced %d log lines in one session, want 1", got)
			}
			// Untouched: neither delivered nor sealed, still pending replay.
			waitForCompletionState(t, outbox, refusedAttempt, "durable_completion")
			assertAttemptPending(t, outbox, refusedAttempt)
			if err := outbox.Close(); err != nil {
				t.Fatal(err)
			}

			// The next agent process owns the evidence and asks once more.
			restarted, err := newEvidenceOutbox(directory, "stable-node", 1024*1024, clock, 8, time.Hour, 100*time.Millisecond)
			if err != nil {
				t.Fatal(err)
			}
			defer restarted.Close()
			restartReports := make(chan error, 64)
			restarted.startRecovery(t.Context(), client, func(err error) { restartReports <- err })
			select {
			case <-restartReports:
			case <-time.After(5 * time.Second):
				t.Fatal("restarted agent did not retry the refused attempt")
			}
			clock.Advance(10 * maxEvidenceRecoveryBackoff)
			storeRecoveryCompletion(t, restarted, "attempt-barrier-restart")
			restarted.scheduleRecovery()
			waitForCompletionState(t, restarted, "attempt-barrier-restart", "delivered")
			if calls := counter.count(refusedAttempt); calls != 2 {
				t.Fatalf("refused attempt was asked %d times over two sessions, want 2", calls)
			}
			if extra := len(restartReports); extra != 0 {
				t.Fatalf("restarted session logged the refusal %d extra times", extra)
			}
			waitForCompletionState(t, restarted, refusedAttempt, "durable_completion")
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
	counter := newCompletionCounter(map[string]contract.ErrorCode{attemptID: contract.ErrorInternal})
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
		if failure+1 == len(want) {
			counter.mu.Lock()
			delete(counter.refused, attemptID)
			counter.mu.Unlock()
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
}
