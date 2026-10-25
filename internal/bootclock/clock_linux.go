//go:build linux

package bootclock

import (
	"golang.org/x/sys/unix"
	"time"
)

// Now uses CLOCK_BOOTTIME: Linux CLOCK_MONOTONIC pauses during suspend.
func Now() (time.Duration, error) {
	var ts unix.Timespec
	if err := unix.ClockGettime(unix.CLOCK_BOOTTIME, &ts); err != nil {
		return 0, err
	}
	return time.Duration(ts.Nano()), nil
}
