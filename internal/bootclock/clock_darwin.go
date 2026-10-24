//go:build darwin

// Package bootclock measures elapsed time including system sleep. Values are
// meaningful only within one host boot, never as calendar timestamps.
package bootclock

import (
	"golang.org/x/sys/unix"
	"time"
)

// Now uses Darwin's sleep-counting CLOCK_MONOTONIC, not Go's
// mach_absolute_time (which pauses during sleep).
func Now() (time.Duration, error) {
	var ts unix.Timespec
	if err := unix.ClockGettime(unix.CLOCK_MONOTONIC, &ts); err != nil {
		return 0, err
	}
	return time.Duration(ts.Nano()), nil
}
