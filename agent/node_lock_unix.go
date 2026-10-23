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
	return acquireFileLock(directory, spoolFileName(nodeID)+".lock", syscall.LOCK_EX)
}

// Generations share the cohort lock but each process incarnation is exclusive.
// A legacy process still takes the cohort lock exclusively, so modes cannot mix.
func acquireAgentNodeLock(directory, nodeID, generationID string) (nodeLock, error) {
	if generationID == "" {
		return acquireNodeLock(directory, nodeID)
	}
	cohort, err := acquireGenerationCohortLock(directory, nodeID)
	if err != nil {
		return nil, err
	}
	generation, err := acquireNodeLock(directory, nodeID+":generation:"+generationID)
	if err != nil {
		_ = cohort.Close()
		return nil, err
	}
	return &agentNodeLocks{cohort: cohort, generation: generation}, nil
}

func acquireGenerationCohortLock(directory, nodeID string) (nodeLock, error) {
	return acquireFileLock(directory, spoolFileName(nodeID)+".lock", syscall.LOCK_SH)
}

func acquireFileLock(directory, name string, mode int) (nodeLock, error) {
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return nil, fmt.Errorf("agent: create node lock directory: %w", err)
	}
	path := filepath.Join(directory, name)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("agent: open node lock: %w", err)
	}
	if err := syscall.Flock(int(file.Fd()), mode|syscall.LOCK_NB); err != nil {
		_ = file.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, fmt.Errorf("agent: node-local ownership %q is already active: %w", name, errSharedLeaseBusy)
		}
		return nil, fmt.Errorf("agent: lock node-local ownership %q: %w", name, err)
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
