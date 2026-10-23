//go:build !darwin && !dragonfly && !freebsd && !linux && !netbsd && !openbsd && !solaris

package agent

import "fmt"

func acquireNodeLock(_ string, _ string) (nodeLock, error) {
	return nil, fmt.Errorf("agent: stable-node locking is unsupported on this platform")
}

func acquireAgentNodeLock(directory, nodeID, generationID string) (nodeLock, error) {
	return acquireNodeLock(directory, nodeID)
}

func acquireGenerationCohortLock(directory, nodeID string) (nodeLock, error) {
	return acquireNodeLock(directory, nodeID)
}
