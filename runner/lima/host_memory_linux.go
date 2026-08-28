//go:build linux

package lima

import (
	"errors"
	"math"

	"golang.org/x/sys/unix"
)

func hostPhysicalMemoryBytes() (int64, error) {
	var info unix.Sysinfo_t
	if err := unix.Sysinfo(&info); err != nil {
		return 0, err
	}
	unit := uint64(info.Unit)
	if unit == 0 || uint64(info.Totalram) > math.MaxUint64/unit {
		return 0, errors.New("host physical memory is unavailable")
	}
	memory := uint64(info.Totalram) * unit
	if memory == 0 || memory > math.MaxInt64 {
		return 0, errors.New("host physical memory is unavailable")
	}
	return int64(memory), nil
}
