//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package agent

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

type flockNodeLock struct {
	file *os.File
}

func acquireNodeLock(directory, nodeID string) (nodeLock, error) {
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return nil, fmt.Errorf("agent: create node lock directory: %w", err)
	}
	path := filepath.Join(directory, spoolFileName(nodeID)+".lock")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("agent: open node lock: %w", err)
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = file.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, fmt.Errorf("agent: stable node %q is already active", nodeID)
		}
		return nil, fmt.Errorf("agent: lock stable node %q: %w", nodeID, err)
	}
	return &flockNodeLock{file: file}, nil
}

func (lock *flockNodeLock) Close() error {
	if lock == nil || lock.file == nil {
		return nil
	}
	unlockErr := syscall.Flock(int(lock.file.Fd()), syscall.LOCK_UN)
	closeErr := lock.file.Close()
	lock.file = nil
	return errors.Join(unlockErr, closeErr)
}
