//go:build !darwin && !linux

package lima

import "errors"

func hostPhysicalMemoryBytes() (int64, error) {
	return 0, errors.New("host physical memory is unavailable on this platform")
}
