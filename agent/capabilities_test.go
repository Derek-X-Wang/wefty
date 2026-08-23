package agent

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
	processrunner "github.com/Derek-X-Wang/wefty/runner/process"
)

type capabilityProbeFunc func(context.Context) (CapabilityProbeResult, error)

func (probe capabilityProbeFunc) Probe(ctx context.Context) (CapabilityProbeResult, error) {
	return probe(ctx)
}

func TestCapabilityRevisionChangesOnlyOnPublishableTransition(t *testing.T) {
	clock := newManualClock(time.Date(2026, 8, 22, 11, 0, 0, 0, time.UTC))
	probe := capabilityProbeFunc(func(context.Context) (CapabilityProbeResult, error) {
		return CapabilityProbeResult{
			Capabilities:        map[string]bool{" KIND:OCI ": true},
			MissingCapabilities: []string{"kind:process"},
			ReasonCode:          contract.CapabilityReasonProbeFailed,
		}, nil
	})
	state := newCapabilityState(map[string]bool{" PROCESS ": true, "KIND:OCI": true}, probe, clock, 0)
	if initial := state.snapshot(); initial.Revision != 1 || !initial.Capabilities["kind:process"] || initial.Capabilities["kind:oci"] {
		t.Fatalf("normalized unprobed snapshot = %#v", initial)
	}
	if err := state.refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	first := state.capabilitySnapshot()
	clock.Advance(time.Second)
	if err := state.refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	second := state.capabilitySnapshot()
	if first.Revision != 2 || second.Revision != first.Revision || !second.ObservedAt.After(first.ObservedAt) ||
		!second.LastProbeAt.Equal(second.ObservedAt) || len(second.MissingCapabilities) != 0 || second.ReasonCode != "" {
		t.Fatalf("unchanged probe snapshots = first %#v second %#v", first, second)
	}
}

func TestLegacyConfiguredProcessCapabilityAllowsProcess(t *testing.T) {
	state := newCapabilityState(map[string]bool{" Process ": true}, nil, systemClock{}, 0)
	processSpec := contract.JobSpec{Kind: contract.JobKindProcess}
	if !state.allows(processSpec) || state.snapshot().Capabilities["process"] {
		t.Fatalf("legacy configured process snapshot = %#v", state.snapshot())
	}
}

func TestFailedCapabilityProbeSuppressesLocalOCIStartImmediately(t *testing.T) {
	assertFailedCapabilityProbeSuppressesLocalOCIStartImmediately(t)
}

func assertFailedCapabilityProbeSuppressesLocalOCIStartImmediately(t *testing.T) {
	t.Helper()
	clock := newManualClock(time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC))
	results := []struct {
		result CapabilityProbeResult
		err    error
	}{
		{result: CapabilityProbeResult{Capabilities: map[string]bool{"kind:oci": true, "runtime_handler:io.containerd.runc.v2": true}}},
		{result: CapabilityProbeResult{ReasonCode: contract.CapabilityReasonHelperUnreachable}, err: errors.New("private helper socket detail")},
		{result: CapabilityProbeResult{Capabilities: map[string]bool{"kind:oci": true, "runtime_handler:io.containerd.runc.v2": true}}},
	}
	probe := capabilityProbeFunc(func(context.Context) (CapabilityProbeResult, error) {
		result := results[0]
		results = results[1:]
		return result.result, result.err
	})
	state := newCapabilityState(map[string]bool{"kind:process": true, "kind:oci": true}, probe, clock, 0)
	ociSpec := contract.JobSpec{Kind: contract.JobKindOCI, Class: contract.JobClassOneShot, Execution: contract.ExecutionSpec{OCI: &contract.OCIExecutionSpec{Image: contract.OCIImageSpec{Reference: "example/probe"}}}}
	if state.allows(ociSpec) {
		t.Fatal("unprobed static OCI capability allowed a local start")
	}
	if err := state.refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	earned := state.snapshot()
	if !state.allows(ociSpec) || earned.Revision != 2 || !earned.Capabilities["kind:oci"] {
		t.Fatalf("earned capability observation = %#v", earned)
	}

	clock.Advance(time.Second)
	probeErr := state.refresh(context.Background())
	if probeErr == nil {
		t.Fatal("failed probe returned no local diagnostic")
	}
	withdrawn := state.snapshot()
	if state.allows(ociSpec) || withdrawn.Revision != 3 || withdrawn.Capabilities["kind:oci"] ||
		len(withdrawn.MissingCapabilities) != 2 || withdrawn.ReasonCode != contract.CapabilityReasonHelperUnreachable {
		t.Fatalf("failed probe did not synchronously suppress OCI = %#v", withdrawn)
	}
	if request := heartbeatRequest("boot-1", withdrawn); request.CapabilityRevision != 3 || request.Capabilities["kind:oci"] ||
		request.CapabilityReasonCode != contract.CapabilityReasonHelperUnreachable {
		t.Fatalf("next heartbeat did not serialize withdrawn snapshot = %#v", request)
	}

	clock.Advance(time.Second)
	if err := state.refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	recovered := state.snapshot()
	if !state.allows(ociSpec) || recovered.Revision != 4 || !recovered.Capabilities["kind:oci"] ||
		!recovered.ObservedAt.After(withdrawn.ObservedAt) {
		t.Fatalf("recovery did not require a newer successful observation = %#v", recovered)
	}
}

func TestLocalCapabilityAdmissionPreventsRunnerStart(t *testing.T) {
	state := newCapabilityState(map[string]bool{"kind:process": true}, nil, systemClock{}, 0)
	runner := &countingProcessRunner{}
	lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{
		runtimes: testRuntimeSet(runner), clock: systemClock{}, allowsStart: state.allows,
	})
	claim := l1.Claim{Job: l1.Job{Spec: contract.JobSpec{
		Kind: contract.JobKindOCI, Class: contract.JobClassOneShot,
		Execution: contract.ExecutionSpec{OCI: &contract.OCIExecutionSpec{Image: contract.OCIImageSpec{Reference: "example/probe"}}},
	}}}
	result, err := lifecycle.runWorkload(context.Background(), claim)
	if err != nil {
		t.Fatal(err)
	}
	if result.SpawnError == nil || result.SpawnError.Code != contract.SpawnFailureRuntimeUnavailable || runner.calls != 0 {
		t.Fatalf("suppressed local start = result %#v runner calls %d", result, runner.calls)
	}
}

func TestAgentPublishesProbeWithdrawalByNextSuccessfulHeartbeat(t *testing.T) {
	assertAgentPublishesProbeWithdrawalByNextSuccessfulHeartbeat(t)
}

func TestHungCapabilityProbeTimesOutWithoutStoppingHeartbeats(t *testing.T) {
	network := plain.NewNetwork()
	store, stopServer := startFailureServer(t, network, nil, map[string][]string{"node-timeout": nil})
	defer stopServer()
	probeCanceled := make(chan struct{}, 1)
	var calls atomic.Int32
	probe := capabilityProbeFunc(func(ctx context.Context) (CapabilityProbeResult, error) {
		if calls.Add(1) == 1 {
			return CapabilityProbeResult{Capabilities: map[string]bool{"kind:oci": true}}, nil
		}
		<-ctx.Done()
		select {
		case probeCanceled <- struct{}{}:
		default:
		}
		return CapabilityProbeResult{}, ctx.Err()
	})
	nodeAgent := newCapabilityTestAgent(t, network, "node-timeout", probe, 20*time.Millisecond, 30*time.Millisecond)
	defer nodeAgent.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- nodeAgent.Run(ctx) }()
	initial := waitForAgentNode(t, store, "node-timeout", func(node l1.Node) bool {
		return node.CapabilityRevision == 2 && node.Capabilities["kind:oci"]
	})
	withdrawn := waitForAgentNode(t, store, "node-timeout", func(node l1.Node) bool {
		return node.CapabilityRevision == 3 && !node.Capabilities["kind:oci"]
	})
	if !withdrawn.LastHeartbeatAt.After(initial.LastHeartbeatAt) {
		t.Fatalf("timeout heartbeat did not advance liveness: initial %#v withdrawn %#v", initial, withdrawn)
	}
	select {
	case <-probeCanceled:
	case <-time.After(time.Second):
		t.Fatal("timed-out probe adapter was not canceled")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("agent run after probe timeout: %v", err)
	}
}

func TestRestrictiveCapabilityTransitionPausesClaimsUntilPublished(t *testing.T) {
	network := plain.NewNetwork()
	store, stopServer := startFailureServer(t, network, nil, map[string][]string{"node-barrier": nil})
	defer stopServer()
	var calls atomic.Int32
	probe := capabilityProbeFunc(func(context.Context) (CapabilityProbeResult, error) {
		if calls.Add(1) == 1 {
			return CapabilityProbeResult{Capabilities: map[string]bool{"kind:oci": true}}, nil
		}
		return CapabilityProbeResult{ReasonCode: contract.CapabilityReasonProbeFailed}, errors.New("runtime unavailable")
	})
	nodeAgent := newCapabilityTestAgent(t, network, "node-barrier", probe, 250*time.Millisecond, time.Second)
	defer nodeAgent.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- nodeAgent.Run(ctx) }()
	waitForAgentNode(t, store, "node-barrier", func(node l1.Node) bool {
		return node.CapabilityRevision == 2 && node.Capabilities["kind:oci"]
	})
	if err := nodeAgent.ProbeCapabilities(context.Background()); err == nil {
		t.Fatal("restrictive event probe returned no error")
	}
	job, _, err := store.CreateJob(context.Background(), testOCIJobSpec("capability-publication-barrier"))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	attempts, err := store.ListJobAttempts(context.Background(), job.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if len(attempts) != 0 {
		t.Fatalf("unpublished withdrawal burned attempts = %#v", attempts)
	}
	waitForAgentNode(t, store, "node-barrier", func(node l1.Node) bool {
		return node.CapabilityRevision == 3 && !node.Capabilities["kind:oci"]
	})
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("agent run after publication barrier: %v", err)
	}
}

func TestNilProbeCannotAdvertiseOCI(t *testing.T) {
	network := plain.NewNetwork()
	store, stopServer := startFailureServer(t, network, nil, map[string][]string{"node-unprobed": nil})
	defer stopServer()
	nodeAgent := newCapabilityTestAgent(t, network, "node-unprobed", nil, 50*time.Millisecond, time.Second)
	defer nodeAgent.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- nodeAgent.Run(ctx) }()
	node := waitForAgentNode(t, store, "node-unprobed", func(node l1.Node) bool { return node.CapabilityRevision == 1 })
	if node.Capabilities["kind:oci"] || !node.Capabilities["kind:process"] || nodeAgent.capabilities.allows(testOCIJobSpec("nil-probe-local")) {
		t.Fatalf("nil-probe capability state = node %#v local %#v", node, nodeAgent.CapabilitySnapshot())
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("nil-probe agent run: %v", err)
	}
}

func newCapabilityTestAgent(t *testing.T, network *plain.Network, nodeID string, probe CapabilityProbe, heartbeatInterval, probeTimeout time.Duration) *Agent {
	t.Helper()
	agentFabric := network.NewFabric(fabric.Identity{NodeID: "fabric-" + nodeID, Tags: []string{l1.DefaultAgentPrincipalTag}})
	managedRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	nodeAgent, err := New(Config{
		Fabric: agentFabric, ControlPlaneAddress: "wefty://control-plane", NodeID: nodeID, BootSessionID: "boot-" + nodeID,
		Version: "test", OS: "linux", Architecture: "amd64",
		Capabilities: map[string]bool{"kind:process": true, "kind:oci": true}, CapabilityProbe: probe,
		CapabilityProbeTimeout: probeTimeout, HeartbeatInterval: heartbeatInterval, ClaimInterval: 5 * time.Millisecond,
		ManagedRootDirectory: managedRoot, LogSpoolDirectory: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return nodeAgent
}

func waitForAgentNode(t *testing.T, store *l1.Store, nodeID string, predicate func(l1.Node) bool) l1.Node {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		nodes, err := store.ListNodes(context.Background())
		if err == nil {
			for _, node := range nodes {
				if node.NodeID == nodeID && predicate(node) {
					return node
				}
			}
		}
		time.Sleep(time.Millisecond)
	}
	nodes, _ := store.ListNodes(context.Background())
	t.Fatalf("node %q did not reach capability predicate: %#v", nodeID, nodes)
	return l1.Node{}
}

func testOCIJobSpec(dispatchKey string) contract.JobSpec {
	return contract.JobSpec{
		SchemaVersion: contract.SchemaVersionV1, DispatchKey: dispatchKey, Kind: contract.JobKindOCI, Class: contract.JobClassOneShot,
		Execution: contract.ExecutionSpec{OCI: &contract.OCIExecutionSpec{Image: contract.OCIImageSpec{Reference: "example/probe:latest"}}},
	}
}

func assertAgentPublishesProbeWithdrawalByNextSuccessfulHeartbeat(t *testing.T) {
	t.Helper()
	network := plain.NewNetwork()
	store, stopServer := startFailureServer(t, network, nil, map[string][]string{"node-1": nil})
	defer stopServer()
	allowFailure := make(chan struct{})
	failedProbe := make(chan struct{}, 1)
	var calls atomic.Int32
	probe := capabilityProbeFunc(func(ctx context.Context) (CapabilityProbeResult, error) {
		if calls.Add(1) == 1 {
			return CapabilityProbeResult{Capabilities: map[string]bool{"kind:oci": true}}, nil
		}
		select {
		case <-ctx.Done():
			return CapabilityProbeResult{}, ctx.Err()
		case <-allowFailure:
		}
		select {
		case failedProbe <- struct{}{}:
		default:
		}
		return CapabilityProbeResult{ReasonCode: contract.CapabilityReasonProbeFailed}, errors.New("local probe detail")
	})
	agentFabric := network.NewFabric(fabric.Identity{NodeID: "fabric-node", Tags: []string{l1.DefaultAgentPrincipalTag}})
	managedRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	nodeAgent, err := New(Config{
		Fabric: agentFabric, ControlPlaneAddress: "wefty://control-plane", NodeID: "node-1", BootSessionID: "boot-1",
		Version: "test", OS: "linux", Architecture: "amd64", Capabilities: map[string]bool{"kind:process": true},
		CapabilityProbe: probe, HeartbeatInterval: 20 * time.Millisecond, ClaimInterval: time.Second,
		ManagedRootDirectory: managedRoot, LogSpoolDirectory: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer nodeAgent.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- nodeAgent.Run(ctx) }()
	waitForNodeCapability(t, store, true, 2)
	close(allowFailure)
	select {
	case <-failedProbe:
	case <-time.After(5 * time.Second):
		t.Fatal("heartbeat probe did not fail")
	}
	if snapshot := nodeAgent.CapabilitySnapshot(); snapshot.Capabilities["kind:oci"] {
		t.Fatalf("failed probe left local OCI admission enabled: %#v", snapshot)
	}
	waitForNodeCapability(t, store, false, 3)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("agent run after capability withdrawal: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("agent did not stop")
	}
}

func waitForNodeCapability(t *testing.T, store *l1.Store, wantOCI bool, minimumRevision int64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		nodes, err := store.ListNodes(context.Background())
		if err == nil && len(nodes) == 1 && nodes[0].CapabilityRevision >= minimumRevision &&
			nodes[0].Capabilities["kind:oci"] == wantOCI {
			if !wantOCI && (len(nodes[0].MissingCapabilities) != 1 || nodes[0].MissingCapabilities[0] != "kind:oci" ||
				nodes[0].CapabilityReasonCode != contract.CapabilityReasonProbeFailed) {
				t.Fatalf("published withdrawal metadata = %#v", nodes[0])
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	nodes, _ := store.ListNodes(context.Background())
	t.Fatalf("node OCI capability did not become %t at revision >= %d: %#v", wantOCI, minimumRevision, nodes)
}

type countingProcessRunner struct{ calls int }

func (runner *countingProcessRunner) Run(context.Context, processrunner.Request, processrunner.OutputSink) (contract.ProcessResult, error) {
	runner.calls++
	exitCode := 0
	return contract.ProcessResult{ExitCode: &exitCode}, nil
}
