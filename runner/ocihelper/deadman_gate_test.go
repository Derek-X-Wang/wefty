package ocihelper

import (
	"testing"
	"time"
)

// A renewal is sent only when the heartbeat carrying it is sure to be applied
// before the attempt's deadman: that heartbeat is answered within one interval
// or the session is lost. A renewal that might arrive later -- or that is for
// an attempt the helper already expired or deleted -- would be rejected, and
// the helper would end the whole session over it, reaping every other attempt.
// Instead it is dropped and only that attempt expires.
func TestRenewalThatMightMissItsDeadmanIsNeverSent(t *testing.T) {
	const (
		heartbeatTimeout = 3 * time.Second // one-second heartbeat interval
		initialDeadman   = 2 * time.Second
	)
	for _, tc := range []struct {
		name       string
		queueAfter time.Duration
		deleted    bool
		sent       bool
	}{
		{name: "surely in time", sent: true},
		{name: "within one heartbeat of the deadman", queueAfter: 1500 * time.Millisecond},
		{name: "after the deadman", queueAfter: 2500 * time.Millisecond},
		{name: "after delete", deleted: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			engine := newFakeEngine()
			clock := newManualClock(time.Unix(30_000, 0))
			client, stop := startTestServer(t, engine, ServerConfig{
				Clock: clock, HeartbeatTimeout: heartbeatTimeout, MaximumAttemptDeadman: 5 * time.Minute,
			})
			defer stop()
			client.Now = clock.Now
			session, err := client.OpenSession(t.Context(), testSessionRequest())
			if err != nil {
				t.Fatal(err)
			}
			defer session.Close()
			requireSweep(t, session)
			authority := testAuthority()
			neighbour := testAuthority()
			neighbour.JobID, neighbour.AttemptID, neighbour.FencingToken = "job-2", "attempt-2", "fence-2"
			if _, err := session.Run(t.Context(), testRunRequest(neighbour, 5*time.Minute)); err != nil {
				t.Fatal(err)
			}
			if _, err := session.Run(t.Context(), testRunRequest(authority, initialDeadman)); err != nil {
				t.Fatal(err)
			}
			if err := session.QueueAttemptRenewalUntil(authority, clock.Now().Add(time.Minute)); err != nil {
				t.Fatal(err)
			}
			if tc.deleted {
				if response, err := session.Delete(t.Context(), DeleteRequest{Authority: authority}); err != nil || !response.Deleted {
					t.Fatalf("delete = %+v %v", response, err)
				}
			}
			clock.Advance(tc.queueAfter)
			if tc.queueAfter >= initialDeadman {
				waitFor(t, time.Second, func() bool { return engine.attemptReapCount() == 1 }, "deadman reap of the attempt")
			}
			if err := session.flushHeartbeat(t.Context()); err != nil {
				t.Fatalf("heartbeat: %v", err)
			}
			if err := session.HealthError(); err != nil {
				t.Fatalf("the renewal ended the session: %v", err)
			}
			// Past the original deadman: a renewal the helper applied keeps the
			// attempt; one that was dropped leaves it to the deadman.
			if rest := initialDeadman + 500*time.Millisecond - tc.queueAfter; rest > 0 {
				clock.Advance(rest)
			}
			if tc.sent {
				time.Sleep(50 * time.Millisecond)
				if reaps := engine.attemptReapCount(); reaps != 0 {
					t.Fatalf("a renewal sent in time did not keep the attempt: reaps=%d", reaps)
				}
			} else if !tc.deleted {
				waitFor(t, time.Second, func() bool { return engine.attemptReapCount() == 1 }, "deadman reap of the attempt whose renewal was dropped")
			}
			if err := session.HealthError(); err != nil {
				t.Fatalf("session after the deadman: %v", err)
			}
			engine.mu.Lock()
			defer engine.mu.Unlock()
			for _, reaped := range engine.attemptReaps {
				if reaped.AttemptID == neighbour.AttemptID {
					t.Fatal("the neighbouring attempt was reaped")
				}
			}
		})
	}
}
