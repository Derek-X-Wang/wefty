package l3

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestDispatchProbeConcurrentReservation(t *testing.T) {
	s, _, clock := recoveryStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := s.holdDispatch(ctx, "principal_forbidden"); err != nil {
		t.Fatal(err)
	}
	const contenders = 12
	for round := 0; round < 100; round++ {
		// The clock stays fixed until every contender has returned. Advancing
		// a minute makes the next probe due even at the maximum backoff.
		clock.now = clock.now.Add(time.Minute)
		start := make(chan struct{})
		probes := make([]*dispatchProbe, contenders)
		errs := make([]error, contenders)
		var ready, done sync.WaitGroup
		ready.Add(contenders)
		done.Add(contenders)
		for i := range contenders {
			go func() {
				defer done.Done()
				ready.Done()
				<-start
				probes[i], errs[i] = s.reserveDispatchProbe(ctx, time.Second)
			}()
		}
		ready.Wait()
		close(start)
		done.Wait()
		var winner *dispatchProbe
		reserved := 0
		for i, p := range probes {
			if errs[i] != nil {
				t.Fatalf("round %d contender %d: %v", round, i, errs[i])
			}
			if p != nil {
				reserved++
				winner = p
			}
		}
		if reserved != 1 {
			t.Fatalf("round %d: got %d reservations, want exactly one", round, reserved)
		}
		var storedID string
		if err := s.db.QueryRowContext(ctx, `SELECT probe_id FROM dispatch_hold WHERE singleton=1`).Scan(&storedID); err != nil {
			t.Fatal(err)
		}
		if storedID != winner.id {
			t.Fatalf("round %d: stored probe %q, reserved %q", round, storedID, winner.id)
		}
		if err := s.finishDispatchProbe(ctx, winner, false); err != nil {
			t.Fatal(err)
		}
	}
}
