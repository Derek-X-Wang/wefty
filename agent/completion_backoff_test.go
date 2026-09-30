package agent

import (
	"encoding/json"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

// delayRecordingClock records every requested timer duration and fires it at
// once, so a retry schedule is observable without sleeping through it.
type delayRecordingClock struct {
	systemClock
	mu     sync.Mutex
	delays []time.Duration
}

func (clock *delayRecordingClock) NewTimer(duration time.Duration) Timer {
	clock.mu.Lock()
	clock.delays = append(clock.delays, duration)
	clock.mu.Unlock()
	return clock.systemClock.NewTimer(0)
}

func (clock *delayRecordingClock) recorded() []time.Duration {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return slices.Clone(clock.delays)
}

func TestCompletionBacksOffWhileTheRunLedgerIsUnavailable(t *testing.T) {
	// Each answer in order; after the last, the completion is accepted.
	answers := []contract.ErrorCode{
		contract.ErrorRunLedgerUnavailable, contract.ErrorRunLedgerUnavailable, contract.ErrorRunLedgerUnavailable,
		contract.ErrorRunLedgerUnavailable, contract.ErrorRunLedgerUnavailable, contract.ErrorInternal,
		contract.ErrorRunLedgerUnavailable,
	}
	var mu sync.Mutex
	calls := 0
	client, stopServer := startEvidenceReplayServer(t, http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if !strings.HasSuffix(request.URL.Path, "/complete") {
			http.NotFound(w, request)
			return
		}
		mu.Lock()
		call := calls
		calls++
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if call < len(answers) {
			status := http.StatusServiceUnavailable
			if answers[call] == contract.ErrorInternal {
				status = http.StatusInternalServerError
			}
			w.WriteHeader(status)
			// L1 marks a committed Computer completion's owed revocation not
			// retryable; the agent must still deliver, only more slowly.
			_ = json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{Code: answers[call], Retryable: false}})
			return
		}
		_ = json.NewEncoder(w).Encode(l1.Job{})
	}), time.Second)
	defer stopServer()
	defer client.Close()

	clock := &delayRecordingClock{}
	lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{client: client, clock: clock, completionRetry: 10 * time.Second})
	claim := spoolTestClaim("run-ledger-backoff")
	if failure := lifecycle.completeWithRetry(t.Context(), claim, l1.CompletionRequest{FencingToken: claim.Lease.FencingToken}); failure.err != nil {
		t.Fatalf("completion was not delivered: %+v", failure)
	}
	want := []time.Duration{
		10 * time.Second, 20 * time.Second, 30 * time.Second, 30 * time.Second, 30 * time.Second,
		10 * time.Second, // another transient failure keeps the base interval and resets the streak
		10 * time.Second,
	}
	if got := clock.recorded(); !slices.Equal(got, want) {
		t.Fatalf("completion retry delays = %v, want %v", got, want)
	}
}
