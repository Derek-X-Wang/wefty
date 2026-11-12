package agent

import (
	"net/http"
	"testing"
	"testing/synctest"
	"time"
)

// #1524: a loaded L1 needs several seconds to take its write lock. With ten
// seconds of authority left, the renewal may now wait nine, so a reply that
// takes six arrives and extends authority. Halving gave it five, so the
// request was abandoned and the payload was fenced at the old deadline.
func TestSlowL1RenewalNearTheEndStillRenews(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var timeouts []time.Duration
		f := newSleepRenewalFixture(t, func(f *sleepRenewalFixture, r *http.Request) (*http.Response, error) {
			deadline, _ := r.Context().Deadline()
			timeouts = append(timeouts, deadline.Sub(time.Now()))
			select {
			case <-time.After(6 * time.Second):
				return sleepLeaseReply(f.lease), nil
			case <-r.Context().Done():
				return nil, r.Context().Err()
			}
		}, func(f *sleepRenewalFixture) { f.lease.LeaseTTL = 20 * time.Second })
		f.clock.advance(10*time.Second, 0) // first renewal, ten seconds left
		// Let bubble time run past the six-second reply. The agent's own clock
		// stays at t=10, so only the request timeout decides the outcome.
		time.Sleep(7 * time.Second)
		synctest.Wait()
		if len(timeouts) != 1 || timeouts[0] != 9*time.Second {
			t.Fatalf("renewal request timeouts = %v, want one of 9s", timeouts)
		}
		// The old deadline passes; the renewed authority still holds.
		f.clock.advance(10*time.Second, 0)
		f.live(t)
	})
}
