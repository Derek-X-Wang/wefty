//go:build darwin

package main

import (
	"errors"
	"time"

	"golang.org/x/sys/unix"
)

// A PID can be reused across reboots. Pair it with the ready timestamp and
// the kernel's current boot time; this is separate from agent boot sessions.
func setupSystemBootTime() (time.Time, error) {
	boot, err := unix.SysctlTimeval("kern.boottime")
	if err != nil {
		return time.Time{}, err
	}
	if boot.Sec <= 0 || boot.Usec < 0 || boot.Usec >= 1000000 {
		return time.Time{}, errors.New("invalid system boot time")
	}
	return time.Unix(boot.Sec, int64(boot.Usec)*1000), nil
}
