//go:build darwin || linux

package agent

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/Derek-X-Wang/wefty/agent/managedroot"
)

const initialServiceRemovalGeneration uint64 = 1

type processManagedResource struct {
	root *managedroot.Manager
}

func initializeManagedResource(rootDirectory, nodeID, bootSessionID string) (managedResourceManager, error) {
	if rootDirectory == "" {
		return nil, nil
	}
	root, err := managedroot.Initialize(managedroot.Config{
		Root: rootDirectory, NodeID: nodeID, BootSessionID: bootSessionID,
	})
	if err != nil {
		return nil, fmt.Errorf("agent: initialize managed service root: %w", err)
	}
	return &processManagedResource{root: root}, nil
}

func (resource *processManagedResource) prepareAttempt(jobID, attemptID string) (managedResourceAttempt, func(), error) {
	paths, err := resource.root.CreateService(jobID, initialServiceRemovalGeneration)
	if err != nil {
		return managedResourceAttempt{}, func() {}, fmt.Errorf("create service container: %w", err)
	}
	attemptComponent := managedroot.EncodeID(attemptID)
	attemptDirectory := filepath.Join(paths.Attempts, attemptComponent)
	runtimeDirectory := filepath.Join(paths.Runtime, attemptComponent)
	for _, path := range []string{attemptDirectory, runtimeDirectory} {
		if err := os.RemoveAll(path); err != nil {
			return managedResourceAttempt{}, func() {}, fmt.Errorf("clear service attempt resource %q: %w", path, err)
		}
	}
	if err := os.Mkdir(attemptDirectory, 0o700); err != nil {
		return managedResourceAttempt{}, func() {}, fmt.Errorf("create service attempt scratch: %w", err)
	}
	cleanup := func() {
		_ = os.RemoveAll(attemptDirectory)
		_ = os.RemoveAll(runtimeDirectory)
	}
	return managedResourceAttempt{
		dataDirectory: paths.Data, runtimeDirectory: runtimeDirectory,
	}, cleanup, nil
}
