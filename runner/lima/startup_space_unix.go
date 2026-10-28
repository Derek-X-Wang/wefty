//go:build darwin || linux

package lima

import (
	"errors"
	"os"
	"path/filepath"

	"github.com/Derek-X-Wang/wefty/internal/hostspace"
)

// CheckHostVMFreeSpace shares the filesystem measurement used by setup's
// conversion copy check. The sparse host disk needs room to grow safely.
func CheckHostVMFreeSpace(path string, minimum uint64) error {
	if !filepath.IsAbs(path) {
		return &HostVMFreeSpaceError{Cause: errors.New("host VM disk volume path is unavailable")}
	}
	if minimum == 0 {
		minimum = DefaultHostVMMinimumFreeBytes
	}
	for {
		available, err := hostspace.AvailableBytes(path)
		if errors.Is(err, os.ErrNotExist) && filepath.Dir(path) != path {
			path = filepath.Dir(path)
			continue
		}
		if err != nil {
			return &HostVMFreeSpaceError{Cause: err}
		}
		if available >= minimum {
			return nil
		}
		return &HostVMFreeSpaceError{Minimum: minimum, Available: available}
	}
}
