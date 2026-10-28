//go:build !darwin && !linux

package hostspace

import "errors"

func AvailableBytes(string) (uint64, error) {
	return 0, errors.New("filesystem free-space inspection is unavailable on this platform")
}
