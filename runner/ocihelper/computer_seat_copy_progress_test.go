package ocihelper

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

func TestSeatCopyStallBoundWithInjectedClock(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clock := newManualClock(time.Now())
		ctx, _, stop := watchSeatCopy(t.Context(), clock)
		defer stop()
		clock.Advance(seatCopyIdleTimeout)
		synctest.Wait()
		if !errors.Is(context.Cause(ctx), errSeatCopyStalled) {
			t.Fatalf("stalled Seat copy remains open: %v", context.Cause(ctx))
		}
	})
}

func TestSeatCopyProgressHasNoTotalRuntimeLimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clock := newManualClock(time.Now())
		ctx, progress, stop := watchSeatCopy(t.Context(), clock)
		defer stop()
		// A progressing copy can exceed the idle bound many times.
		for range 10 {
			clock.Advance(seatCopyIdleTimeout / 2)
			progress()
			synctest.Wait()
			if ctx.Err() != nil {
				t.Fatalf("progressing copy stopped: %v", context.Cause(ctx))
			}
		}
		clock.Advance(seatCopyIdleTimeout)
		synctest.Wait()
		if !errors.Is(context.Cause(ctx), errSeatCopyStalled) {
			t.Fatalf("copy did not stop after progress ceased: %v", context.Cause(ctx))
		}
	})
}
