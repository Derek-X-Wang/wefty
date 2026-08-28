//go:build darwin

package lima

import (
	"errors"
	"fmt"
	"math"

	"golang.org/x/sys/unix"
)

func hostPhysicalMemoryBytes() (int64, error) {
	memory, err := unix.SysctlUint64("hw.memsize")
	if err != nil {
		return 0, fmt.Errorf("inspect host physical memory: %w", err)
	}
	if memory == 0 || memory > math.MaxInt64 {
		return 0, errors.New("host physical memory is unavailable")
	}
	return int64(memory), nil
}
