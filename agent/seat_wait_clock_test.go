package agent

import (
	"testing"
	"time"
)

func TestSeatWaitClockUsesSleepCountingSource(t *testing.T) {
	elapsed := time.Hour
	clock := seatWaitClock{read: func() (time.Duration, error) { return elapsed, nil }}
	start := clock.Now()
	elapsed += 2 * time.Hour
	if got := clock.Now().Sub(start); got != 2*time.Hour {
		t.Fatalf("sleep wait advanced %s", got)
	}
}
