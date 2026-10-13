//go:build darwin || linux

package agent

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/agent/managedroot"
	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	wr "github.com/Derek-X-Wang/wefty/runner"
	p "github.com/Derek-X-Wang/wefty/runner/process"
)

func TestNotSpawnedWriteFailureRetainsPositiveReceipt(t *testing.T) {
	dir, root, resource, claim := processInventoryLifecycleFixture(t)
	writeErr := errors.New("injected not_spawned inventory write failure")
	failing := &notSpawnedWriteFailingResource{processManagedResource: resource, err: writeErr}
	executor := &inventorySpawnFailureExecutor{}
	var receipt wr.ReapReceipt
	var reapErr error
	var logs []string
	lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{
		managedResource: failing, runtimes: testRuntimeSet(executor), nodeID: "node", bootSessionID: "boot", clock: systemClock{}, observer: newLifecycleObserver(systemClock{}),
		runtimeReaped: func(_ string, got wr.ReapReceipt, err error) { receipt, reapErr = got, err },
		logf:          func(format string, args ...any) { logs = append(logs, fmt.Sprintf(format, args...)) },
	})
	result, err := lifecycle.runWorkload(context.Background(), claim)
	if !failing.called || reapErr != nil || !receipt.RuntimeQuiesced || receipt.Evidence != wr.ReapEvidenceAttempt || receipt.BootSessionID != "boot" {
		t.Fatalf("write called=%t receipt=%+v reapErr=%v", failing.called, receipt, reapErr)
	}
	if err != nil || result.SpawnError == nil || result.OutputError != "" {
		t.Fatalf("inventory failure withdrew successful reap: result=%+v err=%v", result, err)
	}
	if log := strings.Join(logs, "\n"); !strings.Contains(log, writeErr.Error()) || !strings.Contains(log, "attempt") {
		t.Fatalf("missing inventory failure diagnostic: %s", log)
	}
	// The failed write leaves the durable unknown-group record intact. A new
	// agent on this host boot cannot mistake it for a known non-spawn.
	assertPendingProcessInventory(t, root)
	reopened, err := managedroot.Open(managedroot.Config{Root: dir, NodeID: "node", BootSessionID: "next", HostBootIdentity: rootHostIdentity})
	if err != nil {
		t.Fatal(err)
	}
	assertPendingProcessInventory(t, reopened)
}

func TestNotSpawnedStartedPayloadKeepsPendingInventory(t *testing.T) {
	_, root, resource, claim := processInventoryLifecycleFixture(t)
	// Exercise the real unguarded runner, which calls Started but does not
	// call ProcessStarted. Then report a later spawn_error from the executor.
	executor := &inventorySpawnFailureExecutor{inner: p.New(p.Config{})}
	lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{
		managedResource: resource, runtimes: testRuntimeSet(executor), nodeID: "node", bootSessionID: "boot", clock: systemClock{}, observer: newLifecycleObserver(systemClock{}),
	})
	result, err := lifecycle.runWorkload(context.Background(), claim)
	if err != nil || result.SpawnError == nil || !executor.started || executor.processStarted {
		t.Fatalf("unguarded fixture started=%t processStarted=%t result=%+v spawnError=%+v err=%v", executor.started, executor.processStarted, result, result.SpawnError, err)
	}
	assertPendingProcessInventory(t, root)
}

func processInventoryLifecycleFixture(t *testing.T) (string, *managedroot.Manager, *processManagedResource, l1.Claim) {
	t.Helper()
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	root, err := managedroot.Initialize(managedroot.Config{Root: dir, NodeID: "node", BootSessionID: "boot", HostBootIdentity: rootHostIdentity})
	if err != nil {
		t.Fatal(err)
	}
	resource := &processManagedResource{root: root, bootSessionID: "boot"}
	claim := l1.Claim{Job: l1.Job{JobID: "job", Spec: contract.JobSpec{Kind: contract.JobKindProcess, Class: contract.JobClassService, Restart: contract.RestartAlways, Execution: contract.ExecutionSpec{Executable: contract.ExecutableSpec{Path: "/bin/sh"}, Argv: []string{"sh", "-c", "exit 0"}, WorkingDirectory: dir}}}, Lease: l1.AttemptLease{AttemptID: "attempt", FencingToken: "fence"}}
	return dir, root, resource, claim
}

func rootHostIdentity() (string, error) {
	return "linux:00000000-0000-0000-0000-000000000001", nil
}

func assertPendingProcessInventory(t *testing.T, root *managedroot.Manager) {
	t.Helper()
	records, err := root.ProcessAttempts("job")
	if err != nil || len(records) != 1 || records[0].NotSpawned || records[0].ProcessGroupID != 0 {
		t.Fatalf("pending inventory=%+v err=%v", records, err)
	}
}

type notSpawnedWriteFailingResource struct {
	*processManagedResource
	err    error
	called bool
}

func (resource *notSpawnedWriteFailingResource) prepareAttempt(jobID, attemptID string) (managedResourceAttempt, func(), error) {
	attempt, cleanup, err := resource.processManagedResource.prepareAttempt(jobID, attemptID)
	attempt.processNotSpawned = func() error {
		resource.called = true
		return resource.err
	}
	return attempt, cleanup, err
}

type inventorySpawnFailureExecutor struct {
	inner          *p.Runner
	started        bool
	processStarted bool
}

func (executor *inventorySpawnFailureExecutor) Run(ctx context.Context, request p.Request, sink p.OutputSink) (contract.ProcessResult, error) {
	if executor.inner != nil {
		started, processStarted := request.Started, request.ProcessStarted
		request.Started = func() {
			executor.started = true
			if started != nil {
				started()
			}
		}
		request.ProcessStarted = func(pgid int) error {
			executor.processStarted = true
			if processStarted != nil {
				return processStarted(pgid)
			}
			return nil
		}
		request.Guarded = false
		result, err := executor.inner.Run(ctx, request, sink)
		if err != nil || result.ExitCode == nil || *result.ExitCode != 0 {
			return result, err
		}
	}
	return spawnFailure(contract.SpawnFailureProcessRequest, errors.New("later executor spawn failure")), nil
}
