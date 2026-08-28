//go:build !darwin && !linux

package ocicontrol

import (
	"errors"
	"os"
)

func acquireSocketLock(string) (*os.File, error) {
	return nil, errors.New("Unix socket locking is unavailable on this platform")
}

func releaseSocketLock(*os.File) {}
