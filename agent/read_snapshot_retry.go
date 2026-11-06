package agent

import (
	"context"
	"time"
)

// Only typed snapshot-expiry refusals may replay these client operations;
// transport failures and other protocol errors are final.
// The five-second window limits retry starts, never an individual request.
func retryReadSnapshotWithClock(ctx context.Context, clock Clock, call func(context.Context) error) error {
	deadline := clock.Now().Add(5 * time.Second)
	var err error
	for attempt := range 3 {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if attempt > 0 && !clock.Now().Before(deadline) {
			return err
		}
		err = call(ctx)
		if err == nil || !readSnapshotExpired(err) || ctx.Err() != nil || attempt == 2 {
			return err
		}
		delay := time.Duration(attempt+1) * 100 * time.Millisecond
		if clock.Now().Add(delay).After(deadline) {
			return err
		}
		timer := clock.NewTimer(delay)
		select {
		case <-ctx.Done():
			stopTimer(timer)
			return ctx.Err()
		case <-timer.C():
		}
		stopTimer(timer)
	}
	return err
}
