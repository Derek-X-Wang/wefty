package ocihelper

import "time"

type Clock interface {
	Now() time.Time
	NewTimerAt(time.Time) Timer
}

type Timer interface {
	C() <-chan time.Time
	Stop() bool
	ResetAt(time.Time) bool
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }
func (systemClock) NewTimerAt(deadline time.Time) Timer {
	return &systemTimer{timer: time.NewTimer(durationUntil(deadline))}
}

type systemTimer struct{ timer *time.Timer }

func (timer *systemTimer) C() <-chan time.Time { return timer.timer.C }
func (timer *systemTimer) Stop() bool          { return timer.timer.Stop() }
func (timer *systemTimer) ResetAt(deadline time.Time) bool {
	return timer.timer.Reset(durationUntil(deadline))
}

func durationUntil(deadline time.Time) time.Duration {
	duration := time.Until(deadline)
	if duration < 0 {
		return 0
	}
	return duration
}
