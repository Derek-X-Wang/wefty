//go:build darwin || linux

package agent

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"github.com/Derek-X-Wang/wefty/agent/managedroot"
	"github.com/Derek-X-Wang/wefty/contract"
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

// verifyStoppedAttempt is read-only recovery for this exact agent boot and
// Attempt. Only a durable not-spawned record or ESRCH for its recorded process
// group proves absence. EPERM, reused IDs, unknown groups and missing records
// refuse; no signal, directory deletion or process adoption occurs here.
func (resource *processManagedResource) verifyStoppedAttempt(ctx context.Context, authority workloadrunner.AttemptAuthority) (workloadrunner.ReapReceipt, error) {
	if err := ctx.Err(); err != nil {
		return workloadrunner.ReapReceipt{}, err
	}
	if authority.NodeID != resource.root.Manifest().NodeID || authority.BootSessionID != resource.bootSessionID || authority.WorkloadClass != contract.JobClassService || authority.JobID == "" || authority.AttemptID == "" || authority.FencingToken == "" {
		return workloadrunner.ReapReceipt{}, errors.New("process verification requires exact current service authority")
	}
	records, err := resource.root.ProcessAttempts(authority.JobID)
	if err != nil {
		return workloadrunner.ReapReceipt{}, err
	}
	for _, record := range records {
		if record.AttemptID != authority.AttemptID || record.BootSessionID != authority.BootSessionID {
			continue
		}
		receipt := workloadrunner.ReapReceipt{RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidenceAttempt, BootSessionID: authority.BootSessionID}
		if record.NotSpawned && record.ProcessGroupID == 0 {
			receipt.Evidence = workloadrunner.ReapEvidenceNoRuntime
			return receipt, nil
		}
		if record.ProcessGroupID <= 0 {
			return workloadrunner.ReapReceipt{}, errors.New("process group identity is unavailable")
		}
		if err := syscall.Kill(-record.ProcessGroupID, 0); !errors.Is(err, syscall.ESRCH) {
			if err == nil {
				return workloadrunner.ReapReceipt{}, fmt.Errorf("process group %d still exists", record.ProcessGroupID)
			}
			return workloadrunner.ReapReceipt{}, fmt.Errorf("process group %d absence is unverified: %w", record.ProcessGroupID, err)
		}
		return receipt, nil
	}
	return workloadrunner.ReapReceipt{}, errors.New("exact process attempt inventory is unavailable")
}
