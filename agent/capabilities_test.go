package agent

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
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
		return node.CapabilityRevision >= 3 && node.Capabilities["kind:oci"]
	})
	withdrawn := waitForAgentNode(t, store, "node-timeout", func(node l1.Node) bool {
		return node.CapabilityRevision > initial.CapabilityRevision && !node.Capabilities["kind:oci"]
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
		return node.CapabilityRevision >= 3 && node.Capabilities["kind:oci"]
	})
	if err := nodeAgent.RecoverOCIRuntimeCapabilities(context.Background()); err == nil {
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
		return node.CapabilityRevision >= 4 && !node.Capabilities["kind:oci"]
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
	node := waitForAgentNode(t, store, "node-unprobed", func(node l1.Node) bool { return node.CapabilityRevision >= 2 })
	if node.Capabilities["kind:oci"] || !node.Capabilities["kind:process"] || nodeAgent.capabilities.allows(testOCIJobSpec("nil-probe-local")) {
		t.Fatalf("nil-probe capability state = node %#v local %#v", node, nodeAgent.CapabilitySnapshot())
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("nil-probe agent run: %v", err)
	}
}

func TestAgentRefusesOCIProbeWithoutBootBarrier(t *testing.T) {
	_, err := New(Config{
		NodeID: "node", BootSessionID: "boot", Version: "test",
		CapabilityProbe: capabilityProbeFunc(func(context.Context) (CapabilityProbeResult, error) {
			return CapabilityProbeResult{Capabilities: map[string]bool{"kind:oci": true}}, nil
		}),
	})
	if err == nil || err.Error() != "agent: OCI capability probe requires a boot barrier" {
		t.Fatalf("agent construction error = %v", err)
	}
}

func TestOCIBootBarrierOrdersRemovalProbePublicationAndClaims(t *testing.T) {
	network := plain.NewNetwork()
	store, stopServer := startFailureServer(t, network, nil, map[string][]string{"node-boot-barrier": nil})
	defer stopServer()
	job, _, err := store.CreateJob(context.Background(), testOCIJobSpec("boot-barrier-order"))
	if err != nil {
		t.Fatal(err)
	}

	var eventMu sync.Mutex
	var events []string
	record := func(event string) {
		eventMu.Lock()
		events = append(events, event)
		eventMu.Unlock()
	}
	barrier := &recordingOCIBootBarrier{ensure: func(context.Context) error {
		record("sweep-verify")
		return nil
	}}
	probe := capabilityProbeFunc(func(context.Context) (CapabilityProbeResult, error) {
		record("probe")
		nodes, err := store.ListNodes(context.Background())
		if err != nil {
			t.Errorf("list nodes during probe: %v", err)
			return CapabilityProbeResult{}, err
		}
		if len(nodes) != 1 || nodes[0].Capabilities["kind:oci"] {
			t.Errorf("pre-probe L1 capability publication = %#v", nodes)
			return CapabilityProbeResult{}, errors.New("OCI capability published before probe")
		}
		attempts, err := store.ListJobAttempts(context.Background(), job.JobID)
		if err != nil {
			t.Errorf("list attempts during probe: %v", err)
			return CapabilityProbeResult{}, err
		}
		if len(attempts) != 0 {
			t.Errorf("OCI claim preceded functional probe: %#v", attempts)
			return CapabilityProbeResult{}, errors.New("OCI claim preceded functional probe")
		}
		return CapabilityProbeResult{Capabilities: map[string]bool{"kind:oci": true}}, nil
	})
	nodeAgent := newBootBarrierTestAgent(t, network, "node-boot-barrier", barrier, probe)
	defer nodeAgent.Close()
	nodeAgent.session.removals = &removalController{managed: &recordingResumeResource{resume: func() {
		record("resume-removals")
		nodes, err := store.ListNodes(context.Background())
		if err != nil {
			t.Errorf("list nodes during removal resume: %v", err)
			return
		}
		if len(nodes) != 1 || nodes[0].Capabilities["kind:oci"] {
			t.Errorf("pre-removal L1 capability publication = %#v", nodes)
		}
		attempts, err := store.ListJobAttempts(context.Background(), job.JobID)
		if err != nil {
			t.Errorf("list attempts during removal resume: %v", err)
			return
		}
		if len(attempts) != 0 {
			t.Errorf("OCI claim preceded pending-removal resume: %#v", attempts)
		}
	}}}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- nodeAgent.Run(ctx) }()
	waitForAgentNode(t, store, "node-boot-barrier", func(node l1.Node) bool {
		return node.CapabilityRevision >= 2 && node.Capabilities["kind:oci"]
	})
	eventMu.Lock()
	gotEvents := append([]string(nil), events...)
	eventMu.Unlock()
	if want := []string{"sweep-verify", "resume-removals", "probe"}; !reflect.DeepEqual(gotEvents, want) {
		t.Fatalf("OCI boot ordering = %v, want %v", gotEvents, want)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestProcessOnlyAgentRegistersWithoutCapabilitySupersede(t *testing.T) {
	network := plain.NewNetwork()
	store, stopServer := startFailureServer(t, network, nil, map[string][]string{"node-process-only": nil})
	defer stopServer()
	agentFabric := network.NewFabric(fabric.Identity{NodeID: "fabric-node-process-only", Tags: []string{l1.DefaultAgentPrincipalTag}})
	managedRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	nodeAgent, err := New(Config{
		Fabric: agentFabric, ControlPlaneAddress: "wefty://control-plane", NodeID: "node-process-only", BootSessionID: "boot-process-only",
		Version: "test", OS: "linux", Architecture: "amd64", Capabilities: map[string]bool{"kind:process": true},
		ManagedRootDirectory: managedRoot, LogSpoolDirectory: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer nodeAgent.Close()
	node, err := nodeAgent.Register(t.Context())
	if err != nil {
		t.Fatalf("process-only registration was treated as OCI supersede: %v", err)
	}
	if !node.Capabilities["kind:process"] || node.Capabilities["kind:oci"] || node.CapabilityRevision != 1 || node.CapabilityReasonCode != "" {
		t.Fatalf("process-only node = %+v", node)
	}
	nodes, err := store.ListNodes(t.Context())
	if err != nil || len(nodes) != 1 {
		t.Fatalf("stored process-only node = %+v, %v", nodes, err)
	}
}

func TestSpecializedBarrierReasonPublishesRestrictiveThenReearnsAfterEnsureAndProbe(t *testing.T) {
	network := plain.NewNetwork()
	store, stopServer := startFailureServer(t, network, nil, map[string][]string{"node-lima-broken": nil})
	defer stopServer()
	var events []string
	barrier := &recordingOCIBootBarrier{
		reason:     contract.CapabilityReasonLimaBroken,
		invalidate: func() { events = append(events, "invalidate") },
		ensure: func(context.Context) error {
			events = append(events, "ensure-failed")
			return errors.New("Lima is Broken")
		},
	}
	probe := capabilityProbeFunc(func(context.Context) (CapabilityProbeResult, error) {
		events = append(events, "probe")
		return CapabilityProbeResult{Capabilities: map[string]bool{"kind:oci": true}}, nil
	})
	nodeAgent := newBootBarrierTestAgent(t, network, "node-lima-broken", barrier, probe)
	defer nodeAgent.Close()
	node, err := nodeAgent.Register(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if node.Capabilities["kind:oci"] || node.CapabilityReasonCode != contract.CapabilityReasonLimaBroken || !reflect.DeepEqual(events, []string{"ensure-failed"}) {
		t.Fatalf("restrictive specialized registration = node=%+v events=%v", node, events)
	}
	barrier.mu.Lock()
	barrier.ensure = func(context.Context) error {
		events = append(events, "ensure-succeeded")
		return nil
	}
	barrier.mu.Unlock()
	if err := nodeAgent.RecoverOCIRuntimeCapabilities(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(events, []string{"ensure-failed", "invalidate", "ensure-succeeded", "probe"}) {
		t.Fatalf("republication ordering = %v", events)
	}
	node = waitForAgentNode(t, store, "node-lima-broken", func(node l1.Node) bool {
		return node.Capabilities["kind:oci"] && node.CapabilityReasonCode == ""
	})
	if node.CapabilityRevision < 3 {
		t.Fatalf("positive republication did not follow restrictive observation: %+v", node)
	}
}

func TestReusedBootAtomicallySupersedesHighRevisionOCIBadge(t *testing.T) {
	const nodeID = "node-reused-high-revision"
	network := plain.NewNetwork()
	store, stopServer := startFailureServer(t, network, nil, map[string][]string{nodeID: nil})
	defer stopServer()
	seeded, err := store.RegisterNode(context.Background(), fabric.Identity{NodeID: "fabric-" + nodeID}, contract.NodeRegistration{
		NodeID: nodeID, BootSessionID: "boot-reused", RootInstanceID: "stale-root",
		OS: "linux", Architecture: "amd64", AgentVersion: "stale",
		Capabilities: map[string]bool{"kind:process": true, "kind:oci": true}, CapabilityRevision: 9,
		CapabilityObservedAt: time.Now().UTC(), MissingCapabilities: []string{},
	}, l1.DefaultNodePolicy(), true)
	if err != nil {
		t.Fatal(err)
	}
	job, _, err := store.CreateJob(context.Background(), testOCIJobSpec("reused-high-revision"))
	if err != nil {
		t.Fatal(err)
	}
	barrier := &recordingOCIBootBarrier{ensure: func(context.Context) error {
		nodes, listErr := store.ListNodes(context.Background())
		if listErr != nil {
			return listErr
		}
		if len(nodes) != 1 || nodes[0].CapabilityRevision != 10 || nodes[0].Capabilities["kind:oci"] {
			return fmt.Errorf("stale OCI badge was not atomically superseded before sweep: %#v", nodes)
		}
		attempts, listErr := store.ListJobAttempts(context.Background(), job.JobID)
		if listErr != nil {
			return listErr
		}
		if len(attempts) != 0 {
			return fmt.Errorf("stale capability minted a claim before sweep: %#v", attempts)
		}
		return nil
	}}
	probe := capabilityProbeFunc(func(context.Context) (CapabilityProbeResult, error) {
		return CapabilityProbeResult{Capabilities: map[string]bool{"kind:oci": true}}, nil
	})
	nodeAgent := newBootBarrierTestAgent(t, network, nodeID, barrier, probe)
	defer nodeAgent.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- nodeAgent.Run(ctx) }()
	node := waitForAgentNode(t, store, nodeID, func(node l1.Node) bool {
		return node.CapabilityRevision == 11 && node.Capabilities["kind:oci"]
	})
	if node.AuthorityGeneration != seeded.AuthorityGeneration+1 {
		t.Fatalf("registration authority generation = %d, want one bump from %d", node.AuthorityGeneration, seeded.AuthorityGeneration)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestFailedOCIBootSweepPublishesNoBadgeAndMintsNoClaim(t *testing.T) {
	network := plain.NewNetwork()
	store, stopServer := startFailureServer(t, network, nil, map[string][]string{"node-sweep-failed": nil})
	defer stopServer()
	probeCalled := false
	barrier := &recordingOCIBootBarrier{ensure: func(context.Context) error {
		return errors.New("runtime residue remains")
	}}
	probe := capabilityProbeFunc(func(context.Context) (CapabilityProbeResult, error) {
		probeCalled = true
		return CapabilityProbeResult{Capabilities: map[string]bool{"kind:oci": true}}, nil
	})
	nodeAgent := newBootBarrierTestAgent(t, network, "node-sweep-failed", barrier, probe)
	defer nodeAgent.Close()
	removalResumed := make(chan struct{}, 1)
	nodeAgent.session.removals = &removalController{managed: &recordingResumeResource{resume: func() {
		removalResumed <- struct{}{}
	}}}
	job, _, err := store.CreateJob(context.Background(), testOCIJobSpec("boot-sweep-failed"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- nodeAgent.Run(ctx) }()
	node := waitForAgentNode(t, store, "node-sweep-failed", func(node l1.Node) bool {
		return node.CapabilityRevision >= 2 && node.CapabilityReasonCode == contract.CapabilityReasonBootSweepFailed
	})
	if node.Capabilities["kind:oci"] || probeCalled {
		t.Fatalf("failed sweep capability state = node %#v probe_called=%t", node, probeCalled)
	}
	select {
	case <-removalResumed:
	case <-time.After(time.Second):
		t.Fatal("failed OCI barrier stranded process-service removal resumption")
	}
	time.Sleep(50 * time.Millisecond)
	attempts, err := store.ListJobAttempts(context.Background(), job.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if len(attempts) != 0 {
		t.Fatalf("failed sweep minted OCI attempts: %#v", attempts)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestHelperLossAtBarrierStagesKeepsPoisedOCIClaimUnminted(t *testing.T) {
	stages := []string{"after-ensure", "during-removals", "during-probe", "before-publication"}
	for _, stage := range stages {
		t.Run(stage, func(t *testing.T) {
			nodeID := "node-loss-" + stage
			network := plain.NewNetwork()
			store, stopServer := startFailureServer(t, network, nil, map[string][]string{nodeID: nil})
			defer stopServer()
			barrier := &stageLossBarrier{}
			if stage == "after-ensure" {
				barrier.loseAfterEnsure = true
			}
			if stage == "before-publication" {
				barrier.loseAtGenerationCall = 2
			}
			probe := capabilityProbeFunc(func(context.Context) (CapabilityProbeResult, error) {
				if stage == "during-probe" {
					barrier.lose(errors.New("helper died during probe"))
				}
				return CapabilityProbeResult{Capabilities: map[string]bool{"kind:oci": true}}, nil
			})
			nodeAgent := newBootBarrierTestAgent(t, network, nodeID, barrier, probe)
			defer nodeAgent.Close()
			resumed := make(chan struct{}, 1)
			nodeAgent.session.removals = &removalController{managed: &recordingResumeResource{resume: func() {
				select {
				case resumed <- struct{}{}:
				default:
				}
				if stage == "during-removals" {
					barrier.lose(errors.New("helper died during removal recovery"))
				}
			}}}
			job, _, err := store.CreateJob(context.Background(), testOCIJobSpec("poised-"+stage))
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- nodeAgent.Run(ctx) }()
			node := waitForAgentNode(t, store, nodeID, func(node l1.Node) bool { return node.CapabilityRevision >= 2 })
			select {
			case <-resumed:
			case <-time.After(time.Second):
				t.Fatalf("removals did not resume after %s helper loss: %#v", stage, node)
			}
			deadline := time.Now().Add(time.Second)
			for nodeAgent.CapabilitySnapshot().Capabilities["kind:oci"] && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if nodeAgent.CapabilitySnapshot().Capabilities["kind:oci"] {
				t.Fatalf("helper loss at %s did not synchronously withdraw local OCI", stage)
			}
			node = waitForAgentNode(t, store, nodeID, func(node l1.Node) bool {
				return node.CapabilityRevision >= 2 && !node.Capabilities["kind:oci"] &&
					node.CapabilityReasonCode == contract.CapabilityReasonBootSweepFailed
			})
			time.Sleep(25 * time.Millisecond)
			attempts, err := store.ListJobAttempts(context.Background(), job.JobID)
			if err != nil {
				t.Fatal(err)
			}
			if len(attempts) != 0 {
				t.Fatalf("helper loss at %s minted poised OCI claim: %#v", stage, attempts)
			}
			cancel()
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		})
	}
}

type recordingOCIBootBarrier struct {
	mu         sync.Mutex
	ready      bool
	ensure     func(context.Context) error
	loss       func(ocihelper.HelperSession, error)
	reason     contract.CapabilityReasonCode
	invalidate func()
}

type readyOCIBootBarrier struct{}

type stageLossBarrier struct {
	mu                   sync.Mutex
	ready                bool
	unavailable          bool
	generationCalls      int
	loseAfterEnsure      bool
	loseAtGenerationCall int
	loss                 func(ocihelper.HelperSession, error)
}

func (barrier *stageLossBarrier) Ready() bool {
	_, ok := barrier.Generation()
	return ok
}
func (barrier *stageLossBarrier) Ensure(context.Context) error {
	barrier.mu.Lock()
	if barrier.unavailable {
		barrier.mu.Unlock()
		return errors.New("helper remains unavailable after injected loss")
	}
	barrier.ready = true
	lose := barrier.loseAfterEnsure
	barrier.mu.Unlock()
	if lose {
		barrier.lose(errors.New("helper died after ensure"))
	}
	return nil
}
func (barrier *stageLossBarrier) Generation() (ocihelper.HelperSession, bool) {
	barrier.mu.Lock()
	barrier.generationCalls++
	call := barrier.generationCalls
	ready := barrier.ready
	lose := barrier.loseAtGenerationCall == call
	handler := barrier.loss
	if lose {
		barrier.ready = false
		barrier.unavailable = true
	}
	barrier.mu.Unlock()
	if lose && ready && handler != nil {
		// The helper is already lost before Generation returns its stale read.
		// Deliver the synchronous production callback asynchronously here only
		// because the caller deliberately holds the claim-publication lock.
		go handler(ocihelper.HelperSession{HelperInstanceID: "stage-loss", SessionGeneration: 1}, errors.New("helper died before publication"))
	}
	return ocihelper.HelperSession{HelperInstanceID: "stage-loss", SessionGeneration: 1}, ready
}
func (barrier *stageLossBarrier) SweepReceipt() (ocihelper.VerifiedSweepReceipt, bool) {
	generation, ok := barrier.Generation()
	return ocihelper.VerifiedSweepReceipt{SweepEpoch: "stage-loss", HelperSession: generation}, ok
}
func (barrier *stageLossBarrier) SetLossHandler(handler func(ocihelper.HelperSession, error)) {
	barrier.mu.Lock()
	barrier.loss = handler
	barrier.mu.Unlock()
}
func (barrier *stageLossBarrier) Close() error { return nil }
func (barrier *stageLossBarrier) lose(err error) {
	barrier.mu.Lock()
	wasReady := barrier.ready
	barrier.ready = false
	barrier.unavailable = true
	handler := barrier.loss
	barrier.mu.Unlock()
	if wasReady && handler != nil {
		handler(ocihelper.HelperSession{HelperInstanceID: "stage-loss", SessionGeneration: 1}, err)
	}
}

func (readyOCIBootBarrier) Ready() bool                  { return true }
func (readyOCIBootBarrier) Ensure(context.Context) error { return nil }
func (readyOCIBootBarrier) Generation() (ocihelper.HelperSession, bool) {
	return ocihelper.HelperSession{HelperInstanceID: "ready", SessionGeneration: 1}, true
}
func (readyOCIBootBarrier) SweepReceipt() (ocihelper.VerifiedSweepReceipt, bool) {
	generation, _ := (readyOCIBootBarrier{}).Generation()
	return ocihelper.VerifiedSweepReceipt{SweepEpoch: "ready", HelperSession: generation}, true
}
func (readyOCIBootBarrier) SetLossHandler(func(ocihelper.HelperSession, error)) {}
func (readyOCIBootBarrier) Close() error                                        { return nil }

func (barrier *recordingOCIBootBarrier) Ready() bool {
	barrier.mu.Lock()
	defer barrier.mu.Unlock()
	return barrier.ready
}

func (barrier *recordingOCIBootBarrier) Ensure(ctx context.Context) error {
	barrier.mu.Lock()
	if barrier.ready {
		barrier.mu.Unlock()
		return nil
	}
	ensure := barrier.ensure
	barrier.mu.Unlock()
	err := ensure(ctx)
	if err == nil {
		barrier.mu.Lock()
		barrier.ready = true
		barrier.mu.Unlock()
	}
	return err
}

func (barrier *recordingOCIBootBarrier) CapabilityReasonCode() contract.CapabilityReasonCode {
	barrier.mu.Lock()
	defer barrier.mu.Unlock()
	return barrier.reason
}

func (barrier *recordingOCIBootBarrier) Invalidate() {
	barrier.mu.Lock()
	barrier.ready = false
	invalidate := barrier.invalidate
	barrier.mu.Unlock()
	if invalidate != nil {
		invalidate()
	}
}

func (barrier *recordingOCIBootBarrier) Generation() (ocihelper.HelperSession, bool) {
	barrier.mu.Lock()
	defer barrier.mu.Unlock()
	return ocihelper.HelperSession{HelperInstanceID: "recording", SessionGeneration: 1}, barrier.ready
}

func (barrier *recordingOCIBootBarrier) SweepReceipt() (ocihelper.VerifiedSweepReceipt, bool) {
	generation, ok := barrier.Generation()
	return ocihelper.VerifiedSweepReceipt{SweepEpoch: "recording", HelperSession: generation}, ok
}

func (barrier *recordingOCIBootBarrier) SetLossHandler(handler func(ocihelper.HelperSession, error)) {
	barrier.mu.Lock()
	barrier.loss = handler
	barrier.mu.Unlock()
}

func (barrier *recordingOCIBootBarrier) Close() error { return nil }

type recordingResumeResource struct{ resume func() }

func (*recordingResumeResource) rootInstanceID() string { return "root" }
func (*recordingResumeResource) prepareAttempt(string, string) (managedResourceAttempt, func(), error) {
	return managedResourceAttempt{}, func() {}, nil
}
func (*recordingResumeResource) remove(context.Context, localRemoval) error { return nil }
func (resource *recordingResumeResource) resumeRemovals(context.Context) ([]localRemoval, error) {
	resource.resume()
	return nil, nil
}

func newBootBarrierTestAgent(
	t *testing.T,
	network *plain.Network,
	nodeID string,
	barrier OCIBootBarrier,
	probe CapabilityProbe,
) *Agent {
	t.Helper()
	agentFabric := network.NewFabric(fabric.Identity{NodeID: "fabric-" + nodeID, Tags: []string{l1.DefaultAgentPrincipalTag}})
	managedRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	nodeAgent, err := New(Config{
		Fabric: agentFabric, ControlPlaneAddress: "wefty://control-plane", NodeID: nodeID, BootSessionID: "boot-reused",
		Version: "test", OS: "linux", Architecture: "amd64",
		Capabilities: map[string]bool{"kind:process": true, "kind:oci": true}, CapabilityProbe: probe,
		OCIBootBarrier: barrier, HeartbeatInterval: time.Hour, ClaimInterval: 250 * time.Millisecond,
		ManagedRootDirectory: managedRoot, LogSpoolDirectory: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return nodeAgent
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
		OCIBootBarrier:         readyOCIBootBarrier{},
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
		CapabilityProbe: probe, OCIBootBarrier: readyOCIBootBarrier{},
		HeartbeatInterval: 20 * time.Millisecond, ClaimInterval: time.Second,
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
	deadline := time.Now().Add(5 * time.Second)
	for nodeAgent.CapabilitySnapshot().Capabilities["kind:oci"] && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if snapshot := nodeAgent.CapabilitySnapshot(); snapshot.Capabilities["kind:oci"] {
		t.Fatalf("failed probe did not withdraw local OCI admission: %#v", snapshot)
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
