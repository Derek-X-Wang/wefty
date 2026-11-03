//go:build darwin || linux

package agent

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/internal/testwait"
	"github.com/Derek-X-Wang/wefty/l1"
)

type removalACKResumeResource struct {
	resumedManagedResource
	onResume func()
}

func (resource *removalACKResumeResource) resumeRemovals(ctx context.Context) ([]localRemoval, error) {
	if resource.onResume != nil {
		resource.onResume()
	}
	return resource.resumedManagedResource.resumeRemovals(ctx)
}

// Register a real generation, but drive only its heartbeat and removal loops.
// Separate controlled clocks expose every delivery/heartbeat hand-off without
// starting claim workers, a listener, or a host boot-identity lookup.
func newRemovalACKHandoffFixture(t *testing.T, intercept func(*http.Request) (*http.Response, bool)) (*agentSession, *removalACKStepClock, *removalACKStepClock, localRemoval, *atomic.Int32) {
	t.Helper()
	clock := newManualClock(time.Now().UTC())
	store, err := l1.OpenStore(filepath.Join(t.TempDir(), "handoff.sqlite"), l1.StoreOptions{Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	participant := &mutableWhoIsFabric{identity: fabric.Identity{NodeID: "principal", Tags: []string{l1.DefaultAgentPrincipalTag}}}
	server, err := l1.NewServer(participant, store, l1.ServerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	client, err := newClient(participant, "wefty://control-plane", testwait.Bound)
	if err != nil {
		t.Fatal(err)
	}
	registrations := &atomic.Int32{}
	client.httpClient.Transport = captureRoundTrip(func(r *http.Request) (*http.Response, error) {
		if response, handled := intercept(r); handled {
			if response == nil {
				return nil, io.ErrUnexpectedEOF
			}
			return response, nil
		}
		recorder := httptest.NewRecorder()
		server.Handler().ServeHTTP(recorder, r)
		if r.URL.Path == "/v1/agent/nodes/register" && recorder.Code == http.StatusOK {
			registrations.Add(1)
		}
		return recorder.Result(), nil
	})
	registration := contract.NodeRegistration{NodeID: "node-1", BootSessionID: "boot-1", AgentGenerationID: "generation", OS: "darwin", Architecture: "arm64", AgentVersion: "test",
		Capabilities: map[string]bool{"kind:process": true, "upgrade:retirement:v1": true}}
	heartbeatClock := &removalACKStepClock{manualClock: clock, armed: make(chan time.Duration, 16)}
	ackClock := &removalACKStepClock{manualClock: newManualClock(clock.Now()), armed: make(chan time.Duration, 16)}
	session := newAgentSession(client, registration, newCapabilityState(registration.Capabilities, nil, clock, 0, nil), time.Hour, time.Hour, heartbeatClock, newLifecycleObserver(clock), t.Logf, 0, 0)
	session.computerPolicy.Close()
	// Policy expiry is independent of heartbeat-loop timer handshakes.
	session.computerPolicy = NewComputerPolicyCache(clock, registration.NodeID, registration.BootSessionID)
	spool := openTestLogSpool(t, t.TempDir(), registration.NodeID, 1<<20)
	t.Cleanup(func() { _ = spool.Close() })
	pending := localRemoval{jobID: "pending", kind: contract.JobKindProcess, generation: 1, rootInstanceID: "root", cleanupFence: "fence", processTreeReaped: true}
	if err := spool.beginRemoval(t.Context(), pending, clock.Now()); err != nil {
		t.Fatal(err)
	}
	managed := &removalACKResumeResource{resumedManagedResource: resumedManagedResource{completed: []localRemoval{pending}}}
	controller := newRemovalController(client, &evidenceOutbox{clock: ackClock, spool: spool}, managed, session, registration.NodeID, registration.BootSessionID, t.Logf)
	session.removals = controller
	t.Cleanup(func() { session.close(); controller.wait() })
	if _, err := session.register(t.Context()); err != nil {
		t.Fatal(err)
	}
	return session, heartbeatClock, ackClock, pending, registrations
}

func removalACKHandoffResponse(code contract.ErrorCode) *http.Response {
	recorder := httptest.NewRecorder()
	if code == "" {
		_, _ = recorder.WriteString("{}")
	} else {
		recorder.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(recorder).Encode(contract.ErrorResponse{Error: contract.APIError{Code: code}})
	}
	return recorder.Result()
}

func waitRemovalACKDelay(t *testing.T, clock *removalACKStepClock) time.Duration {
	t.Helper()
	select {
	case delay := <-clock.armed:
		return delay
	case <-time.After(testwait.Bound):
		t.Fatal("controlled loop did not arm its next timer")
		return 0
	}
}

func TestRemovalAcknowledgementRefusalHandoffResumesDurableRow(t *testing.T) {
	var calls atomic.Int32
	session, _, clock, pending, registrations := newRemovalACKHandoffFixture(t, func(r *http.Request) (*http.Response, bool) {
		if !strings.HasSuffix(r.URL.Path, "/removal-acknowledgement") {
			return nil, false
		}
		switch calls.Add(1) {
		case 1:
			return nil, true
		case 2:
			return removalACKHandoffResponse(contract.ErrorNodeNotRegistered), true
		default:
			return removalACKHandoffResponse(""), true
		}
	})
	controller := session.removals
	published, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	defer func() { releaseOnce.Do(func() { close(release) }) }()
	notify := controller.notifyAcknowledgementFailure
	controller.notifyAcknowledgementFailure = func(failure removalAcknowledgementFailure) {
		notify(failure)
		close(published)
		<-release // Suspend the sender after the notification is visible.
	}
	if err := controller.resume(t.Context()); err != nil {
		t.Fatal(err)
	}
	delay := waitRemovalACKDelay(t, clock)
	controller.mu.Lock()
	task := controller.ackTasks[removalAcknowledgementIdentity(pending)]
	controller.mu.Unlock()
	clock.Advance(delay)
	reviewWait(t, "ACK refusal published", published)
	failure := <-controller.ackFailures
	resuming, rejoined := make(chan struct{}), make(chan struct{})
	controller.managed.(*removalACKResumeResource).onResume = func() { close(resuming) }
	var rejoinErr error
	go func() {
		defer close(rejoined)
		rejoinErr = session.rejoinGeneration(t.Context(), failure.err, session.rejoinSequence.Load())
	}()
	reviewWait(t, "registration entered removal resumption", resuming)
	// With the fix, resumption joins this cancelled sender. Before the fix,
	// resumption returns after skipping its still-live task. Both schedules
	// progress deterministically while the sender remains suspended.
	select {
	case <-task.ctx.Done():
	case <-rejoined:
	case <-time.After(testwait.Bound):
		t.Fatal("resumption neither joined the terminal task nor completed")
	}
	releaseOnce.Do(func() { close(release) })
	reviewWait(t, "generation rejoined", rejoined)
	controller.wait()
	if rejoinErr != nil {
		t.Fatal(rejoinErr)
	}
	_, found, err := controller.outbox.spool.removalIntent(t.Context(), pending.jobID)
	if err != nil || found || calls.Load() != 3 || registrations.Load() != 2 {
		t.Fatalf("handoff stranded row=%t err=%v ACKs=%d registrations=%d, want false/nil/3/2", found, err, calls.Load(), registrations.Load())
	}
}

func TestRemovalAcknowledgementQueuedRefusalCoalescesWithHeartbeat(t *testing.T) {
	var calls atomic.Int32
	var refuseHeartbeat atomic.Bool
	heartbeatRefused, releaseHeartbeat := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	defer func() { releaseOnce.Do(func() { close(releaseHeartbeat) }) }()
	session, heartbeatClock, ackClock, _, registrations := newRemovalACKHandoffFixture(t, func(r *http.Request) (*http.Response, bool) {
		if strings.HasSuffix(r.URL.Path, "/removal-acknowledgement") {
			switch calls.Add(1) {
			case 1:
				return nil, true
			case 2:
				return removalACKHandoffResponse(contract.ErrorNodeNotRegistered), true
			default:
				return removalACKHandoffResponse(""), true
			}
		}
		if strings.HasSuffix(r.URL.Path, "/heartbeat") && refuseHeartbeat.CompareAndSwap(true, false) {
			close(heartbeatRefused)
			select {
			case <-releaseHeartbeat:
			case <-r.Context().Done():
			}
			return removalACKHandoffResponse(contract.ErrorNodeNotRegistered), true
		}
		return nil, false
	})
	// Keep a real cached authorization until after both refusals are handled.
	// Every repeated invalidation then changes installSerial, even when the
	// cache is already closed, exposing duplicate registration side effects.
	person := fabric.Identity{FabricID: "fabric-one", UserID: "person", DeviceID: "device"}
	policy := policySnapshot(t, ackClock.Now(), 1, 1, nil, l1.ComputerGrant{FabricID: person.FabricID, UserID: person.UserID, Permission: l1.ComputerGrantView})
	if _, err := session.computerPolicy.Install(policy); err != nil {
		t.Fatal(err)
	}
	authorization, err := session.computerPolicy.AcquireGrant("computer-1", person)
	if err != nil {
		t.Fatal(err)
	}
	defer authorization.Release()
	session.computerPolicy.mu.Lock()
	serial := session.computerPolicy.installSerial
	session.computerPolicy.mu.Unlock()
	ctx, cancel := context.WithCancel(t.Context())
	loopDone := make(chan struct{})
	failures := make(chan destinationError, 1)
	go func() { defer close(loopDone); session.heartbeatLoop(ctx, failures) }()
	defer func() { cancel(); reviewWait(t, "heartbeat loop stopped", loopDone) }()
	waitRemovalACKDelay(t, heartbeatClock)
	refuseHeartbeat.Store(true)
	session.heartbeatWake <- struct{}{}
	reviewWait(t, "heartbeat refusal held in flight", heartbeatRefused)
	if err := session.removals.resume(t.Context()); err != nil {
		t.Fatal(err)
	}
	ackClock.Advance(waitRemovalACKDelay(t, ackClock))
	session.removals.wait() // The ACK refusal is queued before heartbeat recovers.
	if len(session.removals.ackFailures) != 1 {
		t.Fatal("background ACK refusal was not queued")
	}
	releaseOnce.Do(func() { close(releaseHeartbeat) })
	waitRemovalACKDelay(t, heartbeatClock) // Heartbeat's rejoin completed.
	waitRemovalACKDelay(t, heartbeatClock) // Queued ACK refusal finished handling.
	cancel()
	reviewWait(t, "heartbeat loop stopped", loopDone)
	select {
	case failure := <-failures:
		t.Fatalf("coalescing returned a fatal refusal: %v", failure.err)
	default:
	}
	session.computerPolicy.mu.Lock()
	invalidations := session.computerPolicy.installSerial - serial
	session.computerPolicy.mu.Unlock()
	if registrations.Load() != 2 || session.rejoinSequence.Load() != 1 || invalidations != 1 {
		t.Fatalf("registrations=%d rejoin sequence=%d policy invalidations=%d, want 2/1/1", registrations.Load(), session.rejoinSequence.Load(), invalidations)
	}
}

func TestRemovalAcknowledgementQueuedRefusalKeepsNewestRecovery(t *testing.T) {
	var calls atomic.Int32
	session, heartbeatClock, ackClock, pending, registrations := newRemovalACKHandoffFixture(t, func(r *http.Request) (*http.Response, bool) {
		if !strings.HasSuffix(r.URL.Path, "/removal-acknowledgement") {
			return nil, false
		}
		switch calls.Add(1) {
		case 1:
			return nil, true
		case 2:
			return removalACKHandoffResponse(contract.ErrorNodeNotRegistered), true
		default:
			return removalACKHandoffResponse(""), true
		}
	})
	controller := session.removals
	refusal := &ProtocolError{APIError: contract.APIError{Code: contract.ErrorNodeNotRegistered}}
	stale := removalAcknowledgementFailure{err: refusal, observedRejoin: 0}
	controller.notifyAcknowledgementFailure(stale)
	// Another caller recovers the earlier loss before its queued notification
	// is handled. This row becomes resumable only after that recovery's scan.
	managed := controller.managed
	controller.managed = nil
	if err := session.rejoinGeneration(t.Context(), refusal, 0); err != nil {
		t.Fatal(err)
	}
	controller.managed = managed
	if err := controller.resume(t.Context()); err != nil {
		t.Fatal(err)
	}
	ackClock.Advance(waitRemovalACKDelay(t, ackClock))
	controller.wait()
	// A delayed sender from the old loss must not overwrite the newer loss,
	// just as a full queue must not discard the newer notification.
	controller.notifyAcknowledgementFailure(stale)
	ctx, cancel := context.WithCancel(t.Context())
	loopDone := make(chan struct{})
	failures := make(chan destinationError, 1)
	go func() { defer close(loopDone); session.heartbeatLoop(ctx, failures) }()
	defer func() { cancel(); reviewWait(t, "heartbeat loop stopped", loopDone) }()
	waitRemovalACKDelay(t, heartbeatClock)
	waitRemovalACKDelay(t, heartbeatClock) // The queued refusal has been handled.
	cancel()
	reviewWait(t, "heartbeat loop stopped", loopDone)
	_, found, err := controller.outbox.spool.removalIntent(t.Context(), pending.jobID)
	if err != nil || found || registrations.Load() != 3 || calls.Load() != 3 {
		t.Fatalf("newer queued loss stranded row=%t err=%v registrations=%d ACKs=%d, want false/nil/3/3", found, err, registrations.Load(), calls.Load())
	}
}
