//go:build darwin || linux

package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/testwait"
	"github.com/Derek-X-Wang/wefty/l1"
)

type removalACKStepClock struct {
	*manualClock
	armed chan time.Duration
}

func TestRemovalAcknowledgementBackgroundAuthorityFailureReachesSession(t *testing.T) {
	clock := newManualClock(time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC))
	registration := contract.NodeRegistration{NodeID: "removal-node", BootSessionID: "boot"}
	session := newAgentSession(nil, registration, newCapabilityState(nil, nil, clock, 0, nil), time.Second, time.Second, clock, newLifecycleObserver(clock), nil, 0, 0)
	session.computerPolicy = nil
	controller := newRemovalController(nil, nil, nil, session, registration.NodeID, registration.BootSessionID, nil)
	session.removals = controller
	defer session.close()
	refusal := &ProtocolError{APIError: contract.APIError{Code: contract.ErrorNodeSessionReplaced}}
	controller.ackFailures <- removalAcknowledgementFailure{err: refusal}
	failure := session.serveRegistered(t.Context(), func(context.Context, l1.Claim, time.Time) (errorDestination, error) {
		t.Error("authority failure admitted a claim")
		return errorDestinationUnclassified, nil
	})
	if failure.destination != errorDestinationNodeSession || !errors.Is(failure.err, refusal) {
		t.Fatalf("background authority failure = %+v, want the original node-session refusal", failure)
	}
}

func (clock *removalACKStepClock) NewTimer(delay time.Duration) Timer {
	timer := clock.manualClock.NewTimer(delay)
	clock.armed <- delay
	return timer
}

func TestRemovalAcknowledgementBackgroundDoesNotBlockResume(t *testing.T) {
	for _, name := range []string{"eventual-success", "session-close", "rejoin", "request-complete"} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			clock := &removalACKStepClock{manualClock: newManualClock(time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)), armed: make(chan time.Duration, 64)}
			spool := openTestLogSpool(t, t.TempDir(), "removal-node", 1<<20)
			defer spool.Close()
			pending := localRemoval{jobID: "pending", kind: contract.JobKindProcess, generation: 1, rootInstanceID: "root", cleanupFence: "pending-fence", processTreeReaped: true}
			other := pending
			other.jobID, other.cleanupFence = "other", "other-fence"
			for _, removal := range []localRemoval{pending, other} {
				if err := spool.beginRemoval(ctx, removal, clock.Now()); err != nil {
					t.Fatal(err)
				}
			}
			var mu sync.Mutex
			calls := make(map[string]int)
			bodies := make(map[string][]byte)
			var succeeds atomic.Bool
			var heartbeats atomic.Int32
			client := &Client{baseURL: "http://removal.invalid", operationTimeout: time.Second, transport: &http.Transport{}}
			client.httpClient = &http.Client{Transport: captureRoundTrip(func(request *http.Request) (*http.Response, error) {
				if strings.HasSuffix(request.URL.Path, "/heartbeat") {
					heartbeats.Add(1)
					return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("{}"))}, nil
				}
				body, err := io.ReadAll(request.Body)
				if err != nil {
					return nil, err
				}
				job := "other"
				if strings.Contains(request.URL.Path, "/pending/") {
					job = "pending"
				}
				mu.Lock()
				calls[job]++
				if previous, found := bodies[job]; found && !bytes.Equal(previous, body) {
					t.Error("ACK replay changed request bytes")
				}
				bodies[job] = body
				mu.Unlock()
				if job == "pending" && !succeeds.Load() {
					return nil, io.ErrUnexpectedEOF
				}
				payload, _ := json.Marshal(map[string]string{"job_id": job})
				return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(payload))}, nil
			})}
			registration := contract.NodeRegistration{NodeID: "removal-node", BootSessionID: "boot"}
			session := newAgentSession(client, registration, newCapabilityState(nil, nil, clock, 0, nil), time.Second, time.Second, clock, newLifecycleObserver(clock), nil, 1, 1)
			controller := newRemovalController(client, &evidenceOutbox{clock: clock, spool: spool}, &resumedManagedResource{completed: []localRemoval{pending, other}}, session, registration.NodeID, registration.BootSessionID, t.Logf)
			session.removals = controller
			resumed := make(chan struct{})
			var resumeErr error
			go func() {
				defer close(resumed)
				session.ociRecoveryMu.Lock()
				defer session.ociRecoveryMu.Unlock()
				resumeErr = controller.resume(ctx)
			}()
			defer func() { cancel(); controller.stopAcknowledgements(); <-resumed; controller.wait() }()
			nextDelay := func() time.Duration {
				t.Helper()
				select {
				case delay := <-clock.armed:
					return delay
				case <-time.After(testwait.Bound):
					t.Fatal("ACK replay did not arm its timer")
					return 0
				}
			}
			delay := nextDelay()
			select {
			case <-resumed:
				if resumeErr != nil {
					t.Fatal(resumeErr)
				}
			case <-time.After(testwait.Bound):
				t.Fatal("resume blocked inline on ambiguous ACK replay")
			}
			if _, n, err := spool.removalIntent(ctx, pending.jobID); err != nil || !n {
				t.Fatalf("pending intent found=%t err=%v", n, err)
			}
			if _, n, err := spool.removalIntent(ctx, other.jobID); err != nil || n {
				t.Fatalf("other removal did not finish: found=%t err=%v", n, err)
			}
			// Re-enter recovery and race direct ACK callers while delivery is pending.
			if err := controller.resume(ctx); err != nil {
				t.Fatal(err)
			}
			var duplicateCalls sync.WaitGroup
			for range 8 {
				duplicateCalls.Add(1)
				go func() { defer duplicateCalls.Done(); _ = controller.acknowledge(ctx, pending) }()
			}
			duplicateCalls.Wait()
			mu.Lock()
			firstCalls := calls[pending.jobID]
			mu.Unlock()
			if firstCalls != 1 {
				t.Fatalf("duplicate ACK tasks: calls=%d before any retry, want 1", firstCalls)
			}
			heartbeat := func() {
				t.Helper()
				if err := lockMutexContext(ctx, &session.ociRecoveryMu); err != nil {
					t.Fatal(err)
				}
				_, err := session.heartbeat(ctx)
				session.ociRecoveryMu.Unlock()
				if err != nil {
					t.Fatal(err)
				}
			}
			heartbeat()
			clock.Advance(delay)
			delay = nextDelay() // The pending ACK failed again; other work still proceeds.
			heartbeat()
			if heartbeats.Load() != 2 {
				t.Fatalf("heartbeats=%d, want 2", heartbeats.Load())
			}
			if name == "session-close" {
				session.close()
			} else if name == "request-complete" {
				cancel()
				if _, err := session.heartbeat(t.Context()); err != nil {
					t.Fatalf("node heartbeat after recovery request ended: %v", err)
				}
				succeeds.Store(true)
				clock.Advance(delay)
			} else if name == "rejoin" {
				cancel()
				succeeds.Store(true)
				if err := controller.resume(t.Context()); err != nil {
					t.Fatal(err)
				}
				clock.Advance(delay)
			} else {
				succeeds.Store(true)
				clock.Advance(delay)
			}
			joined := make(chan struct{})
			go func() { controller.wait(); close(joined) }()
			select {
			case <-joined:
			case <-time.After(testwait.Bound):
				t.Fatal("tracked ACK replay did not stop")
			}
			_, found, err := spool.removalIntent(t.Context(), pending.jobID)
			shutdown := name == "session-close"
			if err != nil || found != shutdown {
				t.Fatalf("remaining intent=%t err=%v, want %t", found, err, shutdown)
			}
			mu.Lock()
			before := calls[pending.jobID]
			mu.Unlock()
			clock.Advance(time.Hour)
			mu.Lock()
			after := calls[pending.jobID]
			mu.Unlock()
			if after != before {
				t.Fatalf("ACK replay continued after termination: %d -> %d", before, after)
			}
		})
	}
}
