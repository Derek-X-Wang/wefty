package ocihelper

import (
	"errors"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

// Delay only the first heartbeat reply. The real helper keeps consuming the
// pipelined frames; manual clocks drive both sides without a scheduling sleep.
func TestHeartbeatReplyDelayKeepsNeighbourUntilHelperBound(t *testing.T) {
	for _, expires := range []bool{false, true} {
		name := "reply_between_interval_and_timeout"
		if expires {
			name = "reply_past_helper_timeout"
		}
		t.Run(name, func(t *testing.T) {
			clock := newManualClock(time.Unix(50000, 0))
			engine := newFakeEngine()
			budget := startBudgetServer(t, engine, ServerConfig{Clock: clock, HeartbeatTimeout: 3 * time.Second})
			client := NewUnixClient(budget.path, "checksum-test")
			client.disableHeartbeatPump = true
			client.Now, client.heartbeatClock = clock.Now, clock
			session, err := client.OpenSession(t.Context(), testSessionRequest())
			if err != nil {
				t.Fatal(err)
			}
			defer session.Close()
			requireSweep(t, session)
			neighbour := testAuthority()
			neighbour.JobID, neighbour.AttemptID, neighbour.Class = "neighbour", "neighbour", contract.JobClassService
			if _, err := session.Run(t.Context(), testRunRequest(neighbour, 2*time.Second)); err != nil {
				t.Fatal(err)
			}
			expiring := testAuthority()
			if _, err := session.Run(t.Context(), testRunRequest(expiring, time.Second)); err != nil {
				t.Fatal(err)
			}
			attemptLost, releaseLoss := session.ObserveAttemptLoss(expiring)
			defer releaseLoss()
			budget.server.sessionMu.Lock()
			active := budget.server.active
			budget.server.sessionMu.Unlock()
			generation := session.Handshake().SessionGeneration
			gate := &delayedHeartbeatReplyConn{Conn: session.control, clock: clock, entered: make(chan struct{}), release: make(chan struct{}), closed: make(chan struct{})}
			session.control, session.controlWire = gate, newFramedConn(gate)
			client.disableHeartbeatPump = false
			session.pumpDone = make(chan struct{})
			go session.heartbeatPump()
			// Trigger the first send immediately, independent of wall-clock timers.
			if err := session.QueueAttemptRenewalUntil(neighbour, clock.Now().Add(2*time.Second)); err != nil {
				t.Fatal(err)
			}
			select {
			case <-gate.entered:
			case <-time.After(2 * time.Second):
				t.Fatal("pump did not await the first heartbeat reply")
			}
			waitFor(t, time.Second, func() bool {
				active.mu.Lock()
				defer active.mu.Unlock()
				return active.sequence == 1
			}, "first heartbeat delivery")
			clock.Advance(1250 * time.Millisecond)
			// An old one-interval socket deadline is driven by the same clock,
			// so the regression fails here without waiting for a real second.
			waitFor(t, time.Second, func() bool {
				if session.HealthError() != nil {
					return true
				}
				active.mu.Lock()
				defer active.mu.Unlock()
				return active.sequence >= 2
			}, "next cadence while the first reply is outstanding")
			if err := session.HealthError(); err != nil {
				t.Fatalf("reply delayed beyond one interval lost the session: %v", err)
			}
			waitFor(t, time.Second, func() bool { return engine.attemptReapCount() == 1 }, "target attempt deadman")
			// Renew before the neighbour's two-second deadman while a previous
			// reply is still blocked. This must not wait for that reply.
			if err := session.QueueAttemptRenewalUntil(neighbour, clock.Now().Add(time.Minute)); err != nil {
				t.Fatal(err)
			}
			// A later reply refuses this known expired tuple. FIFO attribution
			// must validate it against that later send, not the blocked first one.
			if err := session.QueueAttemptRenewalUntil(expiring, clock.Now().Add(time.Minute)); err != nil {
				t.Fatal(err)
			}
			waitFor(t, time.Second, func() bool {
				active.mu.Lock()
				defer active.mu.Unlock()
				return active.attempts[neighbour.key()].deadline.After(clock.Now().Add(50 * time.Second))
			}, "renewal delivery before deadman despite the outstanding reply")
			waitFor(t, time.Second, func() bool {
				session.queueMu.Lock()
				defer session.queueMu.Unlock()
				_, pending := session.pending[expiring.key()]
				return !pending
			}, "late renewal written while the first reply is outstanding")
			if expires {
				clock.Advance(2 * time.Second) // reply delay is now beyond the helper's 3 s timeout
				select {
				case <-session.pumpDone:
				case <-time.After(2 * time.Second):
					t.Fatal("reply past helper timeout did not stop the pump")
				}
				var loss *RuntimeLossError
				if !errors.As(session.HealthError(), &loss) {
					t.Fatalf("session health = %v, want runtime loss", session.HealthError())
				}
				waitFor(t, time.Second, func() bool { return engine.sessionReapCount() == 1 }, "timed-out session reap")
				return
			}
			clock.Advance(time.Second) // 2.25 s reply delay; beyond the old deadman
			close(gate.release)
			// This acknowledgement also fences FIFO consumption of every reply.
			if err := session.suppressHeartbeats(); err != nil {
				t.Fatal(err)
			}
			select {
			case loss := <-attemptLost:
				if loss.Authority != expiring {
					t.Fatalf("refused wrong attempt: %+v", loss)
				}
			default:
				t.Fatal("pipelined late renewal did not deliver attempt-scoped loss")
			}
			budget.server.sessionMu.Lock()
			unchanged := budget.server.active == active && active.helper.SessionGeneration == generation
			budget.server.sessionMu.Unlock()
			if !unchanged || session.HealthError() != nil || engine.sessionReapCount() != 0 || engine.attemptReapCount() != 1 {
				t.Fatalf("neighbour/session lost: unchanged=%t health=%v session-reaps=%d attempt-reaps=%d", unchanged, session.HealthError(), engine.sessionReapCount(), engine.attemptReapCount())
			}
			if err := session.Signal(t.Context(), SignalRequest{Authority: neighbour, Signal: SignalTERM}); err != nil {
				t.Fatalf("neighbour no longer authorized: %v", err)
			}
		})
	}
}

// Translate an old SetDeadline into the injected clock as well. Production's
// separate write deadline remains a real socket bound; reply delay is gated.
type delayedHeartbeatReplyConn struct {
	net.Conn
	clock                    *manualClock
	entered, release, closed chan struct{}
	once, closeOnce          sync.Once
	mu                       sync.Mutex
	readTimer                Timer
}

func (c *delayedHeartbeatReplyConn) SetDeadline(deadline time.Time) error {
	c.mu.Lock()
	if c.readTimer != nil {
		c.readTimer.Stop()
		c.readTimer = nil
	}
	if !deadline.IsZero() {
		c.readTimer = c.clock.NewTimerAt(c.clock.Now().Add(time.Until(deadline)))
	}
	c.mu.Unlock()
	return c.Conn.SetWriteDeadline(deadline)
}

func (c *delayedHeartbeatReplyConn) Read(p []byte) (int, error) {
	var timedOut bool
	c.once.Do(func() {
		c.mu.Lock()
		var expiry <-chan time.Time
		if c.readTimer != nil {
			expiry = c.readTimer.C()
		}
		c.mu.Unlock()
		close(c.entered)
		select {
		case <-c.release:
		case <-c.closed:
		case <-expiry:
			timedOut = true
		}
	})
	if timedOut {
		return 0, os.ErrDeadlineExceeded
	}
	return c.Conn.Read(p)
}

func (c *delayedHeartbeatReplyConn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return c.Conn.Close()
}
