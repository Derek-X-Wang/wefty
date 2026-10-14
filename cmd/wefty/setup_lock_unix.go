//go:build darwin || linux

package main

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// Lock the state directory's inode, so process exit releases the lock without
// leaving a stale lock file. Setup and upgrade retain this descriptor throughout.
func lockSetup(paths setupPaths, create bool) (func(), error) {
	root, err := os.OpenRoot(paths.home)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	if create {
		if err := root.MkdirAll(paths.dataRel, 0700); err != nil {
			return nil, err
		}
	}
	directory, err := root.Open(paths.dataRel)
	if !create && errors.Is(err, os.ErrNotExist) {
		return func() {}, nil
	}
	if err != nil {
		return nil, err
	}
	if err := unix.Flock(int(directory.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		directory.Close()
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return nil, fmt.Errorf("another setup or upgrade is in progress")
		}
		return nil, fmt.Errorf("lock setup state folder: %w", err)
	}
	return func() { _ = unix.Flock(int(directory.Fd()), unix.LOCK_UN); _ = directory.Close() }, nil
}
