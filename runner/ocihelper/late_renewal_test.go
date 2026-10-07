package ocihelper

import (
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

// Capture a renewal before expiry, then delay the frame until the helper has
// reaped or deleted the attempt. No client-side expiry check can close this gap.
func TestLateRenewalKeepsNeighbour(t *testing.T) {
	for _, end := range []string{"deadman", "delete"} {
		t.Run(end, func(t *testing.T) {
			engine := newFakeEngine()
			clock := newManualClock(time.Unix(30000, 0))
			budget := startBudgetServer(t, engine, ServerConfig{Clock: clock, HeartbeatTimeout: 5 * time.Minute, MaximumAttemptDeadman: 5 * time.Minute})
			client := NewUnixClient(budget.path, "checksum-test")
			client.disableHeartbeatPump = true
			client.Now = clock.Now
			session, err := client.OpenSession(t.Context(), testSessionRequest())
			if err != nil {
				t.Fatal(err)
			}
			defer session.Close()
			requireSweep(t, session)
			authority := testAuthority()
			neighbour := authority
			neighbour.JobID, neighbour.AttemptID, neighbour.FencingToken, neighbour.Class = "neighbour", "neighbour", "neighbour", contract.JobClassService
			for _, a := range []AttemptAuthority{authority, neighbour} {
				ttl := 2 * time.Second
				if a == neighbour {
					ttl = time.Minute
				}
				if _, err := session.Run(t.Context(), testRunRequest(a, ttl)); err != nil {
					t.Fatal(err)
				}
			}
			generation := session.Handshake().SessionGeneration
			lost, release := session.ObserveAttemptLoss(authority)
			defer release()
			neighbourLost, releaseNeighbour := session.ObserveAttemptLoss(neighbour)
			defer releaseNeighbour()
			for _, a := range []AttemptAuthority{authority, neighbour} {
				if err := session.QueueAttemptRenewalUntil(a, clock.Now().Add(2*time.Minute)); err != nil {
					t.Fatal(err)
				}
			}
			session.controlWire = newFramedConn(&lateRenewalPausedWrite{Conn: session.control, pause: func() {
				if end == "deadman" {
					clock.Advance(2500 * time.Millisecond)
					waitFor(t, time.Second, func() bool { return engine.attemptReapCount() == 1 }, "target deadman reap")
				} else {
					deleted, err := session.Delete(t.Context(), DeleteRequest{Authority: authority})
					if err != nil || !deleted.Deleted {
						t.Fatalf("Delete = %+v, %v", deleted, err)
					}
				}
			}})
			if err := session.flushHeartbeat(t.Context()); err != nil {
				t.Fatalf("late %s renewal invalidated neighbouring session: %v", end, err)
			}
			select {
			case refusal := <-lost:
				if refusal.Authority != authority {
					t.Fatalf("refused wrong authority: %+v", refusal)
				}
			default:
				t.Fatal("late renewal did not report typed attempt loss")
			}
			select {
			case refusal := <-neighbourLost:
				t.Fatalf("neighbour refused: %+v", refusal)
			default:
			}
			if err := session.QueueAttemptRenewal(authority, time.Minute); err == nil {
				t.Fatal("lost attempt renewal queued again")
			}
			budget.server.sessionMu.Lock()
			current := budget.server.active
			unchanged := current != nil && current.helper.SessionGeneration == generation
			budget.server.sessionMu.Unlock()
			if !unchanged || engine.sessionReapCount() != 0 || session.HealthError() != nil {
				t.Fatalf("session changed; reaps=%d health=%v", engine.sessionReapCount(), session.HealthError())
			}
			if err := session.Signal(t.Context(), SignalRequest{Authority: neighbour, Signal: SignalTERM}); err != nil {
				t.Fatalf("neighbour lost: %v", err)
			}
			// The neighbour's renewal in the same heartbeat must have applied.
			clock.Advance(time.Minute)
			current.mu.Lock()
			neighbourDeadline := current.attempts[neighbour.key()].deadline
			current.mu.Unlock()
			if !clock.Now().Before(neighbourDeadline) {
				t.Fatal("neighbour renewal in the mixed heartbeat was not applied")
			}
			if err := session.flushHeartbeat(t.Context()); err != nil {
				t.Fatal(err)
			}
			if engine.attemptReapCount() != 1 {
				t.Fatalf("neighbour deadman was not renewed: reaps=%d", engine.attemptReapCount())
			}
		})
	}
}

type lateRenewalPausedWrite struct {
	net.Conn
	once  sync.Once
	pause func()
}

func (c *lateRenewalPausedWrite) Write(p []byte) (int, error) {
	c.once.Do(c.pause)
	return c.Conn.Write(p)
}

func TestLateRenewalAuthorityViolationsRemainSessionFatal(t *testing.T) {
	for _, field := range []string{"unknown", "node", "boot", "job", "fence", "class", "removal", "invalid", "session", "instance"} {
		t.Run(field, func(t *testing.T) {
			engine := newFakeEngine()
			client, stop := startTestServer(t, engine, ServerConfig{})
			defer stop()
			session, err := client.OpenSession(t.Context(), testSessionRequest())
			if err != nil {
				t.Fatal(err)
			}
			defer session.Close()
			requireSweep(t, session)
			authority := testAuthority()
			if _, err := session.Run(t.Context(), testRunRequest(authority, time.Minute)); err != nil {
				t.Fatal(err)
			}
			if _, err := session.Delete(t.Context(), DeleteRequest{Authority: authority}); err != nil {
				t.Fatal(err)
			}
			forged := authority
			switch field {
			case "unknown":
				forged.AttemptID = "never-admitted"
			case "node":
				forged.NodeID = "other-node"
			case "boot":
				forged.BootSessionID = "other-boot"
			case "job":
				forged.JobID = "other-job"
			case "fence":
				forged.FencingToken = "other-fence"
			case "class":
				forged.Class = contract.JobClassService
			case "removal":
				forged.RemovalGeneration = "other-removal"
			case "invalid":
				forged.FencingToken = ""
			case "session", "instance":
				// Capabilities are random and helper-instance scoped, never textual IDs.
				// A valid capability minted by another helper must fail on this stream.
				otherClient, stopOther := startTestServer(t, newFakeEngine(), ServerConfig{})
				defer stopOther()
				other, err := otherClient.OpenSession(t.Context(), testSessionRequest())
				if err != nil {
					t.Fatal(err)
				}
				defer other.Close()
				session.capability = other.capability
				if field == "session" {
					session.capability = "wrong-session-capability"
				}
			}
			body, err := marshalBody(HeartbeatRequest{Sequence: 1, RenewedAttempts: []DeadmanRenewal{{Authority: forged, TTL: time.Second}}})
			if err != nil {
				t.Fatal(err)
			}
			if err := session.controlWire.write(frame{Version: ProtocolVersion, Method: MethodHeartbeat, SessionCapability: session.capability, Body: body}); err != nil {
				t.Fatal(err)
			}
			err = decodeResponse(session.controlWire, &HeartbeatResponse{})
			if err == nil {
				t.Fatal("forged renewal was accepted")
			}
			waitFor(t, time.Second, func() bool { return engine.sessionReapCount() == 1 }, "fatal authority session reap")
		})
	}
}

func TestExpiredAttemptRecordBoundedAndSessionScoped(t *testing.T) {
	engine := newFakeEngine()
	budget := startBudgetServer(t, engine, ServerConfig{HeartbeatTimeout: time.Minute})
	client := NewUnixClient(budget.path, "checksum-test")
	client.disableHeartbeatPump = true
	session, err := client.OpenSession(t.Context(), testSessionRequest())
	if err != nil {
		t.Fatal(err)
	}
	requireSweep(t, session)
	budget.server.sessionMu.Lock()
	active := budget.server.active
	budget.server.sessionMu.Unlock()
	first := testAuthority()
	for i := 0; i <= maximumExpiredAttempts; i++ {
		a := first
		a.AttemptID = fmt.Sprintf("expired-%d", i)
		if _, err := session.Run(t.Context(), testRunRequest(a, time.Minute)); err != nil {
			t.Fatal(err)
		}
		if _, err := session.Delete(t.Context(), DeleteRequest{Authority: a}); err != nil {
			t.Fatal(err)
		}
	}
	active.mu.Lock()
	count, order := len(active.expiredAttempts), len(active.expiredOrder)
	evicted := first
	evicted.AttemptID = "expired-0"
	_, known := active.expiredAttempts[evicted]
	active.mu.Unlock()
	if count != maximumExpiredAttempts || order != maximumExpiredAttempts || known {
		t.Fatalf("record count=%d order=%d evicted-known=%t", count, order, known)
	}
	if err := session.QueueAttemptRenewal(evicted, time.Second); err != nil {
		t.Fatal(err)
	}
	if err := session.flushHeartbeat(t.Context()); err == nil {
		t.Fatal("evicted identity still received attempt-scoped treatment")
	}
	waitFor(t, time.Second, func() bool { return engine.sessionReapCount() == 1 }, "evicted identity session reap")
	active.mu.Lock()
	count, order = len(active.expiredAttempts), len(active.expiredOrder)
	active.mu.Unlock()
	if count != 0 || order != 0 {
		t.Fatalf("record outlived session: %d/%d", count, order)
	}
	_ = session.Close()
	replacement, err := client.OpenSession(t.Context(), testSessionRequest())
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.Close()
	requireSweep(t, replacement)
	recent := first
	recent.AttemptID = fmt.Sprintf("expired-%d", maximumExpiredAttempts)
	if err := replacement.QueueAttemptRenewal(recent, time.Second); err != nil {
		t.Fatal(err)
	}
	if err := replacement.flushHeartbeat(t.Context()); err == nil {
		t.Fatal("prior-session identity survived into replacement")
	}
}

func TestLateRenewalBeforeGuardianSchedulingNeverRestoresAuthority(t *testing.T) {
	clock := newManualClock(time.Unix(40000, 0))
	server, err := NewServer(newFakeEngine(), ServerConfig{Clock: clock, AllowedUIDs: []uint32{0}})
	if err != nil {
		t.Fatal(err)
	}
	authority := testAuthority()
	deadline := clock.Now()
	attempt := &serverAttempt{authority: authority, state: attemptLive, deadline: deadline, deadlineChanged: make(chan struct{}, 1)}
	session := &serverSession{server: server, identity: SessionIdentity{NodeID: authority.NodeID, BootSessionID: authority.BootSessionID}, heartbeatDeadline: clock.Now().Add(time.Minute), attempts: map[string]*serverAttempt{authority.key(): attempt}, heartbeatChanged: make(chan struct{}, 1)}
	raw, err := marshalBody(HeartbeatRequest{Sequence: 1, RenewedAttempts: []DeadmanRenewal{{Authority: authority, TTL: time.Minute}}})
	if err != nil {
		t.Fatal(err)
	}
	response, rejection := session.applyHeartbeat(raw)
	if rejection != nil || len(response.RefusedAttempts) != 1 || response.RefusedAttempts[0].Code != CodeAttemptExpired || response.RefusedAttempts[0].Authority != authority || attempt.deadline != deadline {
		t.Fatalf("response=%+v rejection=%+v deadline=%s", response, rejection, attempt.deadline)
	}
}

// Delete has already withdrawn the payload, but its absence verification is
// still running. A captured renewal must not invalidate the neighbour here.
func TestLateRenewalDuringDeleteReapKeepsNeighbour(t *testing.T) {
	engine := &blockingReapEngine{fakeEngine: newFakeEngine(), entered: make(chan struct{}), release: make(chan struct{})}
	budget := startBudgetServer(t, engine, ServerConfig{HeartbeatTimeout: time.Minute})
	client := NewUnixClient(budget.path, "checksum-test")
	client.disableHeartbeatPump = true
	session, err := client.OpenSession(t.Context(), testSessionRequest())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(engine.release) }) }
	defer release()
	requireSweep(t, session)
	authority := testAuthority()
	neighbour := authority
	neighbour.JobID, neighbour.AttemptID, neighbour.FencingToken, neighbour.Class = "neighbour", "neighbour", "neighbour", contract.JobClassService
	for _, a := range []AttemptAuthority{authority, neighbour} {
		if _, err := session.Run(t.Context(), testRunRequest(a, time.Minute)); err != nil {
			t.Fatal(err)
		}
	}
	budget.server.sessionMu.Lock()
	active := budget.server.active
	budget.server.sessionMu.Unlock()
	active.mu.Lock()
	deadline := active.attempts[authority.key()].deadline
	active.mu.Unlock()
	deleteDone := make(chan error, 1)
	go func() {
		deleted, err := session.Delete(t.Context(), DeleteRequest{Authority: authority})
		if err == nil && !deleted.Deleted {
			err = fmt.Errorf("Delete did not report deletion")
		}
		deleteDone <- err
	}()
	select {
	case <-engine.entered:
	case <-time.After(time.Second):
		t.Fatal("Delete did not enter ReapAttempt")
	}
	active.mu.Lock()
	reaping := active.attempts[authority.key()].state == attemptReaping && !active.attempts[authority.key()].guardianReaping
	active.mu.Unlock()
	if !reaping {
		t.Fatal("Delete did not reach non-guardian reap")
	}
	body, err := marshalBody(HeartbeatRequest{Sequence: 1, RenewedAttempts: []DeadmanRenewal{{Authority: authority, TTL: 2 * time.Minute}, {Authority: neighbour, TTL: 2 * time.Minute}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := session.controlWire.write(frame{Version: ProtocolVersion, Method: MethodHeartbeat, SessionCapability: session.capability, Body: body}); err != nil {
		t.Fatal(err)
	}
	var response HeartbeatResponse
	if err := decodeResponse(session.controlWire, &response); err != nil {
		t.Fatalf("late renewal during Delete invalidated session: %v", err)
	}
	if len(response.RefusedAttempts) != 1 || response.RefusedAttempts[0].Authority != authority || response.RefusedAttempts[0].Code != CodeAttemptExpired {
		t.Fatalf("refusals = %+v", response.RefusedAttempts)
	}
	active.mu.Lock()
	unchanged := active.attempts[authority.key()].deadline == deadline && active.attempts[authority.key()].state == attemptReaping
	renewed := active.attempts[neighbour.key()].deadline.After(deadline)
	active.mu.Unlock()
	budget.server.sessionMu.Lock()
	sameSession := budget.server.active == active && budget.server.lastSessionInvalidation == nil
	budget.server.sessionMu.Unlock()
	if !unchanged || !renewed || !sameSession || engine.sessionReapCount() != 0 {
		t.Fatalf("unchanged=%t renewed=%t same-session=%t session-reaps=%d", unchanged, renewed, sameSession, engine.sessionReapCount())
	}
	if err := session.Signal(t.Context(), SignalRequest{Authority: neighbour, Signal: SignalTERM}); err != nil {
		t.Fatalf("neighbour lost during Delete: %v", err)
	}
	release()
	select {
	case err := <-deleteDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Delete did not finish")
	}
}
