//go:build !darwin && !linux

package participantstate

import "errors"

func Acquire(string, string) (func() error, error) {
	return nil, errors.New("participant folder locking is unavailable on this platform")
}
