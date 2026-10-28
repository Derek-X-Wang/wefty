//go:build !darwin && !linux

package lima

import "errors"

func CheckHostVMFreeSpace(string, uint64) error {
	return errors.New("host VM free-space check is unavailable on this platform")
}
