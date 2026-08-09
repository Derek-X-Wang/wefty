package agent

import "time"

// Clock supplies all scheduling time used by the agent. Production uses the
// wall clock; tests can advance timers without sleeping through lease or
// heartbeat intervals.
type Clock interface {
	Now() time.Time
	NewTimer(time.Duration) Timer
}

type Timer interface {
	C() <-chan time.Time
	Stop() bool
	Reset(time.Duration) bool
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

func (systemClock) NewTimer(duration time.Duration) Timer {
	return &systemTimer{timer: time.NewTimer(duration)}
}

type systemTimer struct{ timer *time.Timer }

func (timer *systemTimer) C() <-chan time.Time { return timer.timer.C }
func (timer *systemTimer) Stop() bool          { return timer.timer.Stop() }
func (timer *systemTimer) Reset(duration time.Duration) bool {
	return timer.timer.Reset(duration)
}

func stopTimer(timer Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C():
		default:
		}
	}
}
