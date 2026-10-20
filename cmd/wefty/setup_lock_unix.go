//go:build darwin || linux

package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"golang.org/x/sys/unix"
)

// flock has no query operation. Hold a POSIX write lock on this companion file
// while holding the directory flock; F_GETLK lets status observe it without
// acquiring any lock. Keep same-process queries off the inode: POSIX locks are
// per-process and closing another descriptor would release our own write lock.
var setupLockOwners = struct {
	sync.Mutex
	held map[string]bool
}{held: make(map[string]bool)}

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
	setupLockOwners.Lock()
	defer setupLockOwners.Unlock()
	key := filepath.Join(paths.data, setupLockProbeFile)
	flags := os.O_RDWR | unix.O_NONBLOCK
	if create {
		flags |= os.O_CREATE
	}
	probe, err := root.OpenFile(filepath.Join(paths.dataRel, setupLockProbeFile), flags, 0600)
	if !create && errors.Is(err, os.ErrNotExist) {
		err = nil // A repeated removal must not recreate the purged companion.
	}
	if err == nil && probe != nil {
		info, statErr := probe.Stat()
		if statErr != nil || !info.Mode().IsRegular() {
			err = errors.New("setup lock companion must be a regular file")
		}
	}
	if err == nil && probe != nil {
		lock := unix.Flock_t{Type: unix.F_WRLCK, Whence: 0}
		err = unix.FcntlFlock(probe.Fd(), unix.F_SETLK, &lock)
	}
	if err != nil {
		if probe != nil {
			_ = probe.Close()
		}
		_ = unix.Flock(int(directory.Fd()), unix.LOCK_UN)
		_ = directory.Close()
		return nil, fmt.Errorf("publish setup lock: %w", err)
	}
	setupLockOwners.held[key] = true
	return func() {
		setupLockOwners.Lock()
		defer setupLockOwners.Unlock()
		delete(setupLockOwners.held, key)
		if probe != nil {
			_ = probe.Close()
		}
		_ = unix.Flock(int(directory.Fd()), unix.LOCK_UN)
		_ = directory.Close()
	}, nil
}

func setupLockHeld(paths setupPaths) bool {
	setupLockOwners.Lock()
	defer setupLockOwners.Unlock()
	if setupLockOwners.held[filepath.Join(paths.data, setupLockProbeFile)] {
		return true
	}
	root, err := os.OpenRoot(paths.home)
	if err != nil {
		return false
	}
	defer root.Close()
	probe, err := root.OpenFile(filepath.Join(paths.dataRel, setupLockProbeFile), os.O_RDONLY|unix.O_NONBLOCK, 0)
	if err != nil {
		return false
	}
	defer probe.Close()
	info, err := probe.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return false
	}
	lock := unix.Flock_t{Type: unix.F_WRLCK, Whence: 0}
	return unix.FcntlFlock(probe.Fd(), unix.F_GETLK, &lock) == nil && lock.Type != unix.F_UNLCK
}
