package agent

import (
	"sync"
	"time"

	"github.com/Derek-X-Wang/wefty/internal/bootclock"
)

// Only the approval controller uses this elapsed timeline. Its wall labels
// remain separate. Existing injected clocks retain their controlled timers.
type seatWaitClock struct{ read func() (time.Duration, error) }

func (c seatWaitClock) elapsed() (time.Duration, error) {
	if c.read != nil {
		return c.read()
	}
	return bootclock.Now()
}
func (c seatWaitClock) Now() time.Time {
	ns, err := c.elapsed()
	if err != nil {
		panic(err)
	}
	return time.Unix(0, int64(ns))
}
func (c seatWaitClock) WallNow() time.Time { return time.Now() }
func (c seatWaitClock) NewTimer(d time.Duration) Timer {
	t := &seatWaitTimer{clock: c, ch: make(chan time.Time, 1)}
	t.Reset(d)
	return t
}

type seatWaitTimer struct {
	mu     sync.Mutex
	clock  seatWaitClock
	ch     chan time.Time
	cancel chan struct{}
	active bool
}

func (t *seatWaitTimer) C() <-chan time.Time { return t.ch }
func (t *seatWaitTimer) Stop() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	active := t.active
	if active {
		close(t.cancel)
		t.active = false
	}
	return active
}
func (t *seatWaitTimer) Reset(d time.Duration) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	active := t.active
	if active {
		close(t.cancel)
	}
	select {
	case <-t.ch:
	default:
	}
	cancel := make(chan struct{})
	t.cancel = cancel
	t.active = true
	deadline := t.clock.Now().Add(d)
	go func() {
		for {
			now := t.clock.Now()
			remaining := deadline.Sub(now)
			if remaining <= 0 {
				t.mu.Lock()
				defer t.mu.Unlock()
				if t.cancel == cancel && t.active {
					t.active = false
					t.ch <- now
				}
				return
			}
			// Go timers pause through sleep on Darwin. A short poll bounds the delay
			// after wake while authority is always checked against the elapsed source.
			timer := time.NewTimer(min(remaining, 250*time.Millisecond))
			select {
			case <-cancel:
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}()
	return active
}
