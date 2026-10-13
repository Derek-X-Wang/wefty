//go:build darwin || linux

package agent

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/Derek-X-Wang/wefty/agent/managedroot"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
)

type processManagedResource struct {
	root                  *managedroot.Manager
	bootSessionID         string
	previousBootSessionID string
}

func (resource *processManagedResource) rootInstanceID() string {
	return resource.root.Manifest().RootInstanceID
}

func initializeManagedResource(rootDirectory, nodeID, bootSessionID string, logf func(string, ...any)) (managedResourceManager, error) {
	if rootDirectory == "" {
		return nil, nil
	}
	root, err := managedroot.Initialize(managedroot.Config{
		Root: rootDirectory, NodeID: nodeID, BootSessionID: bootSessionID,
	})
	if err != nil {
		return nil, fmt.Errorf("agent: initialize managed service root: %w", err)
	}
	if logf != nil {
		for _, err := range root.LegacyStampErrors() {
			logf("agent: skipped legacy service stamping: %v", err)
		}
	}
	return &processManagedResource{
		root: root, bootSessionID: bootSessionID,
		previousBootSessionID: root.PreviousBootSessionID(),
	}, nil
}

func (resource *processManagedResource) remove(ctx context.Context, removal localRemoval) error {
	return resource.root.Remove(ctx, managedroot.Removal{
		JobID: removal.jobID, Generation: removal.generation,
		RootInstanceID: removal.rootInstanceID, CleanupFence: removal.cleanupFence,
		BootSessionID: resource.bootSessionID, ProcessTreeReaped: removal.processTreeReaped,
	})
}

func (resource *processManagedResource) resumeRemovals(ctx context.Context) ([]localRemoval, error) {
	resumed, err := resource.root.Resume(ctx)
	if err != nil {
		return nil, err
	}
	completed := make([]localRemoval, 0, len(resumed))
	for _, removal := range resumed {
		completed = append(completed, localRemoval{
			jobID: removal.JobID, generation: removal.Generation,
			rootInstanceID: removal.RootInstanceID, cleanupFence: removal.CleanupFence,
			processTreeReaped: removal.ProcessTreeReaped,
		})
	}
	return completed, nil
}

func (resource *processManagedResource) prepareAttempt(jobID, attemptID string) (managedResourceAttempt, func(), error) {
	paths, err := resource.root.CreateService(jobID, l1.InitialServiceRemovalGeneration)
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
		prepareProcess:    func() error { return resource.root.RecordProcessAttempt(jobID, attemptID, 0) },
		processNotSpawned: func() error { return resource.root.RecordProcessNotSpawned(jobID, attemptID) },
		processStarted:    func(pgid int) error { return resource.root.RecordProcessAttempt(jobID, attemptID, pgid) },
	}, cleanup, nil
}

func (resource *processManagedResource) processGroups(jobID string) ([]workloadrunner.ProcessGroupRecord, error) {
	records, err := resource.root.ProcessAttempts(jobID)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", workloadrunner.ErrPriorBootEvidenceUnavailable, err)
	}
	groups := make([]workloadrunner.ProcessGroupRecord, 0, len(records))
	for _, record := range records {
		groups = append(groups, workloadrunner.ProcessGroupRecord{NotSpawned: record.NotSpawned, AttemptID: record.AttemptID, BootSessionID: record.BootSessionID, HostBootID: record.HostBootID, ProcessGroupID: record.ProcessGroupID})
	}
	return groups, nil
}
