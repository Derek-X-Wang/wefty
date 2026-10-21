//go:build !darwin && !linux

package local

import "errors"

func acquireSocketLock(string) (func() error, error) {
	return nil, errors.New("client-service socket locking is unavailable on this platform")
}
