package process

import "time"

// Clock supplies every time source used by the runner. Tests can replace it
// without waiting for wall-clock time.
type Clock interface {
	Now() time.Time
	NewTimer(time.Duration) Timer
}

// Timer is the subset of time.Timer needed by the runner.
type Timer interface {
	C() <-chan time.Time
	Stop() bool
	Reset(time.Duration) bool
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

func (realClock) NewTimer(duration time.Duration) Timer {
	return &realTimer{timer: time.NewTimer(duration)}
}

type realTimer struct {
	timer *time.Timer
}

func (timer *realTimer) C() <-chan time.Time               { return timer.timer.C }
func (timer *realTimer) Stop() bool                        { return timer.timer.Stop() }
func (timer *realTimer) Reset(duration time.Duration) bool { return timer.timer.Reset(duration) }

func stopTimer(timer Timer) {
	if timer == nil {
		return
	}
	if !timer.Stop() {
		select {
		case <-timer.C():
		default:
		}
	}
}

func resetTimer(timer Timer, duration time.Duration) {
	stopTimer(timer)
	timer.Reset(duration)
}
