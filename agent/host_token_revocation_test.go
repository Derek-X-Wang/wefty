package agent

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l3"
)

type retryingHostTokenRevoker struct {
	mu       sync.Mutex
	failures int
	calls    []l3.HostComputerTokenRevocationRequest
	called   chan l3.HostComputerTokenRevocationRequest
}

func (revoker *retryingHostTokenRevoker) RevokeComputerAttemptTokens(context.Context, l3.ComputerAttemptTokenRevocationRequest) error {
	return nil
}

func (revoker *retryingHostTokenRevoker) MintComputerToken(context.Context, l3.ComputerTokenMintRequest) (l3.ComputerTokenGrant, error) {
	return l3.ComputerTokenGrant{}, nil
}

func (revoker *retryingHostTokenRevoker) RevokeHostComputerTokens(_ context.Context, request l3.HostComputerTokenRevocationRequest) error {
	revoker.mu.Lock()
	revoker.calls = append(revoker.calls, request)
	attempt := len(revoker.calls)
	failures := revoker.failures
	revoker.mu.Unlock()
	revoker.called <- request
	if attempt <= failures {
		return fmt.Errorf("injected L3 failure %d", attempt)
	}
	return nil
}

func activeManualTimerDelay(t *testing.T, clock *manualClock) time.Duration {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		clock.mu.Lock()
		now := clock.now
		for _, timer := range clock.timers {
			if timer.active {
				delay := timer.deadline.Sub(now)
				clock.mu.Unlock()
				return delay
			}
		}
		clock.mu.Unlock()
		time.Sleep(time.Millisecond)
	}
	t.Fatal("host token revocation did not arm a retry timer")
	return 0
}

func nextHostRevokeCall(t *testing.T, calls <-chan l3.HostComputerTokenRevocationRequest) l3.HostComputerTokenRevocationRequest {
	t.Helper()
	select {
	case call := <-calls:
		return call
	case <-time.After(5 * time.Second):
		t.Fatal("host token revocation did not call L3")
		return l3.HostComputerTokenRevocationRequest{}
	}
}

func TestHostTokenRevocationRetriesWithBackoffUnderInjectedClock(t *testing.T) {
	clock := newManualClock(time.Date(2026, 10, 1, 8, 0, 0, 0, time.UTC))
	revoker := &retryingHostTokenRevoker{failures: 3, called: make(chan l3.HostComputerTokenRevocationRequest, 8)}
	registered := make(chan struct{})
	var logMu sync.Mutex
	var logs []string
	retry := hostTokenRevocation{
		revoker: revoker, bootSessionID: "boot-current", clock: clock,
		backoff: newSessionBackoff(10*time.Second, 30*time.Second),
		logf: func(format string, args ...any) {
			logMu.Lock()
			defer logMu.Unlock()
			logs = append(logs, fmt.Sprintf(format, args...))
		},
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		retry.run(t.Context(), registered)
	}()
	select {
	case call := <-revoker.called:
		t.Fatalf("revoke-host ran before registration: %#v", call)
	case <-time.After(20 * time.Millisecond):
	}
	close(registered)

	for attempt, window := range [][2]time.Duration{
		{5 * time.Second, 10 * time.Second},
		{10 * time.Second, 20 * time.Second},
		{15 * time.Second, 30 * time.Second},
	} {
		call := nextHostRevokeCall(t, revoker.called)
		if call.Reason != "agent_restart" || call.BootSessionID != "boot-current" {
			t.Fatalf("attempt %d request = %#v", attempt+1, call)
		}
		delay := activeManualTimerDelay(t, clock)
		if delay < window[0] || delay > window[1] {
			t.Fatalf("attempt %d delay = %s, want jitter in [%s,%s]", attempt+1, delay, window[0], window[1])
		}
		select {
		case call := <-revoker.called:
			t.Fatalf("attempt %d retried before clock advanced: %#v", attempt+1, call)
		default:
		}
		clock.Advance(delay)
	}
	if call := nextHostRevokeCall(t, revoker.called); call.BootSessionID != "boot-current" {
		t.Fatalf("successful request = %#v", call)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("host token revocation did not stop after success")
	}
	clock.Advance(time.Hour)
	select {
	case call := <-revoker.called:
		t.Fatalf("revoke-host ran after success: %#v", call)
	default:
	}

	logMu.Lock()
	defer logMu.Unlock()
	if len(logs) != 2 || !strings.Contains(logs[0], "injected L3 failure 1") || !strings.Contains(logs[1], "recovered") {
		t.Fatalf("failure-burst logs = %#v, want first failure and recovery", logs)
	}
}

func TestHostTokenRevocationStopsOnShutdown(t *testing.T) {
	clock := newManualClock(time.Date(2026, 10, 1, 8, 0, 0, 0, time.UTC))
	revoker := &retryingHostTokenRevoker{failures: 100, called: make(chan l3.HostComputerTokenRevocationRequest, 8)}
	session := &agentSession{}
	agent := &Agent{
		registration: contractNodeRegistration("boot-current"),
		session:      session, computerTokens: revoker, clock: clock,
	}
	agent.startHostTokenRevocation(context.Background())
	session.markRegistered()
	nextHostRevokeCall(t, revoker.called)
	delay := activeManualTimerDelay(t, clock)
	agent.Close()
	clock.Advance(delay + time.Hour)
	// Give a worker that outlived Close time to fire on the advanced clock;
	// a non-blocking check would pass before such a worker was scheduled.
	select {
	case call := <-revoker.called:
		t.Fatalf("revoke-host ran after Close returned: %#v", call)
	case <-time.After(50 * time.Millisecond):
	}
}

type blockingHostTokenRevoker struct {
	started chan struct{}
	release chan struct{}
	mu      sync.Mutex
	calls   int
}

func (revoker *blockingHostTokenRevoker) MintComputerToken(context.Context, l3.ComputerTokenMintRequest) (l3.ComputerTokenGrant, error) {
	return l3.ComputerTokenGrant{}, nil
}

func (revoker *blockingHostTokenRevoker) RevokeComputerAttemptTokens(context.Context, l3.ComputerAttemptTokenRevocationRequest) error {
	return nil
}

func (revoker *blockingHostTokenRevoker) RevokeHostComputerTokens(ctx context.Context, _ l3.HostComputerTokenRevocationRequest) error {
	revoker.mu.Lock()
	revoker.calls++
	revoker.mu.Unlock()
	close(revoker.started)
	<-revoker.release
	return ctx.Err()
}

func TestAgentCloseWaitsForHostTokenRevocation(t *testing.T) {
	revoker := &blockingHostTokenRevoker{started: make(chan struct{}), release: make(chan struct{})}
	session := &agentSession{}
	agent := &Agent{
		registration:   contractNodeRegistration("boot-current"),
		session:        session,
		computerTokens: revoker,
		clock:          systemClock{},
	}
	agent.startHostTokenRevocation(context.Background())
	session.markRegistered()
	select {
	case <-revoker.started:
	case <-time.After(5 * time.Second):
		t.Fatal("host token revocation did not start")
	}
	closed := make(chan struct{})
	go func() {
		agent.Close()
		close(closed)
	}()
	select {
	case <-closed:
		t.Fatal("Close returned before the host token revocation worker")
	case <-time.After(20 * time.Millisecond):
	}
	close(revoker.release)
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return after the host token revocation worker stopped")
	}
	revoker.mu.Lock()
	defer revoker.mu.Unlock()
	if revoker.calls != 1 {
		t.Fatalf("revoke-host calls = %d after Close, want 1", revoker.calls)
	}
}

func contractNodeRegistration(bootSessionID string) contract.NodeRegistration {
	return contract.NodeRegistration{NodeID: "stable-node", BootSessionID: bootSessionID}
}
