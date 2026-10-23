package agent

import (
	"context"
	"errors"
	"time"
)

// nodeLock is held for the entire agent process lifetime. Its platform
// implementation must release automatically when the process exits so a
// SIGKILL cannot strand the stable node indefinitely.
type nodeLock interface {
	Close() error
}

type agentNodeLocks struct{ cohort, generation nodeLock }

func (locks *agentNodeLocks) Close() error {
	return errors.Join(locks.generation.Close(), locks.cohort.Close())
}

var errSharedLeaseBusy = errors.New("node-local ownership is busy")

func waitNodeLock(ctx context.Context, directory, key string) (nodeLock, error) {
	for {
		lock, err := acquireNodeLock(directory, key)
		if !errors.Is(err, errSharedLeaseBusy) {
			return lock, err
		}
		timer := time.NewTimer(25 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

// Installed startup already holds the generation-directory lifetime lock.
// Consume its ownership proof once and retain only the shared cohort lock here.
func acquireConfiguredAgentNodeLock(directory, nodeID, generationID string, lifetime func(string, string) error) (nodeLock, error) {
	if lifetime == nil {
		return acquireAgentNodeLock(directory, nodeID, generationID)
	}
	if generationID == "" {
		return nil, errors.New("agent: generation lifetime ownership requires a generation identity")
	}
	cohort, err := acquireGenerationCohortLock(directory, nodeID)
	if err != nil {
		return nil, err
	}
	if err := lifetime(nodeID, generationID); err != nil {
		_ = cohort.Close()
		return nil, err
	}
	return cohort, nil
}
