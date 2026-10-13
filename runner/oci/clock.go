package oci

import "time"

// Clock supplies the attempt runtime and termination-grace clocks.
type Clock interface {
	Now() time.Time
	NewTimer(time.Duration) Timer
}

type Timer interface {
	C() <-chan time.Time
	Stop() bool
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }
func (realClock) NewTimer(duration time.Duration) Timer {
	return &realTimer{timer: time.NewTimer(duration)}
}

type realTimer struct{ timer *time.Timer }

func (timer *realTimer) C() <-chan time.Time { return timer.timer.C }
func (timer *realTimer) Stop() bool          { return timer.timer.Stop() }

func WithClock(clock Clock) Option {
	return func(adapter *Adapter) {
		if clock != nil {
			adapter.clock = clock
		}
	}
}
