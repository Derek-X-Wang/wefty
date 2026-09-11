package agent

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

// staleSupervisorBarrier reproduces runner/lima.SupervisedBootBarrier on real
// hardware: while the barrier is not ready it answers with the Lima
// supervisor's last recorded fact, and after an operator stop that fact is
// oci_intent_disabled until the next Ensure re-runs the supervisor. Fixtures
// that report no reason at all hid this defect from the unit lanes.
type staleSupervisorBarrier struct {
	mu sync.Mutex
	// intentEnabled mirrors the durable marker the Lima supervisor reads: a
	// boot sweep under disabled intent fails instead of starting the instance.
	intentEnabled *atomic.Bool
	ready         bool
	reason        contract.CapabilityReasonCode
}

func (barrier *staleSupervisorBarrier) Ready() bool {
	barrier.mu.Lock()
	defer barrier.mu.Unlock()
	return barrier.ready
}

func (barrier *staleSupervisorBarrier) Ensure(context.Context) error {
	barrier.mu.Lock()
	defer barrier.mu.Unlock()
	if barrier.intentEnabled != nil && !barrier.intentEnabled.Load() {
		barrier.ready = false
		barrier.reason = contract.CapabilityReasonOCIIntentDisabled
		return errors.New("OCI intent is disabled")
	}
	barrier.ready = true
	barrier.reason = ""
	return nil
}

func (barrier *staleSupervisorBarrier) Invalidate() {
	barrier.mu.Lock()
	defer barrier.mu.Unlock()
	barrier.ready = false
}

// stop mirrors `wefty node oci stop`: Lima is stopped and the supervisor
// records the disabled intent it enforced.
func (barrier *staleSupervisorBarrier) stop() {
	barrier.mu.Lock()
	defer barrier.mu.Unlock()
	barrier.ready = false
	barrier.reason = contract.CapabilityReasonOCIIntentDisabled
}

func (barrier *staleSupervisorBarrier) CapabilityReasonCode() contract.CapabilityReasonCode {
	barrier.mu.Lock()
	defer barrier.mu.Unlock()
	if barrier.ready {
		return ""
	}
	return barrier.reason
}

func (barrier *staleSupervisorBarrier) Generation() (ocihelper.HelperSession, bool) {
	barrier.mu.Lock()
	defer barrier.mu.Unlock()
	return ocihelper.HelperSession{HelperInstanceID: "stale-supervisor", SessionGeneration: 1}, barrier.ready
}

func (barrier *staleSupervisorBarrier) SweepReceipt() (ocihelper.VerifiedSweepReceipt, bool) {
	generation, ok := barrier.Generation()
	return ocihelper.VerifiedSweepReceipt{SweepEpoch: "stale-supervisor", HelperSession: generation}, ok
}

func (barrier *staleSupervisorBarrier) SetLossHandler(func(ocihelper.HelperSession, error)) {}

func (barrier *staleSupervisorBarrier) Close() error { return nil }

// TestOCIIntentStopThenStartReopensCapabilityWithoutRestart is the #395
// regression: `wefty node oci stop` followed by `wefty node oci start` must
// reopen OCI capability in the running process, at a strictly higher Capability
// revision, while Lima and the helper are healthy.
func TestOCIIntentStopThenStartReopensCapabilityWithoutRestart(t *testing.T) {
	network := plain.NewNetwork()
	_, stopServer := startFailureServer(t, network, nil, map[string][]string{"node-oci-reopen": nil})
	defer stopServer()
	var intentEnabled atomic.Bool
	var intentRevision atomic.Uint64
	intentEnabled.Store(true)
	intentRevision.Store(2)
	var probeCalls atomic.Int32
	barrier := &staleSupervisorBarrier{intentEnabled: &intentEnabled}
	probe := capabilityProbeFunc(func(context.Context) (CapabilityProbeResult, error) {
		probeCalls.Add(1)
		if !intentEnabled.Load() {
			return CapabilityProbeResult{
				MissingCapabilities: []string{"kind:oci"}, ReasonCode: contract.CapabilityReasonOCIIntentDisabled,
			}, errors.New("OCI intent is disabled")
		}
		return CapabilityProbeResult{Capabilities: map[string]bool{"kind:oci": true}}, nil
	})
	agentFabric := network.NewFabric(fabric.Identity{
		NodeID: "fabric-node-oci-reopen", Tags: []string{l1.DefaultAgentPrincipalTag},
	})
	managedRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	nodeAgent, err := New(Config{
		Fabric: agentFabric, ControlPlaneAddress: "wefty://control-plane", NodeID: "node-oci-reopen",
		BootSessionID: "boot-oci-reopen", Version: "test", OS: "linux", Architecture: "amd64",
		Capabilities: map[string]bool{"kind:process": true}, CapabilityProbe: probe,
		OCIIntent: func(context.Context) (OCIIntentObservation, error) {
			return OCIIntentObservation{Enabled: intentEnabled.Load(), Revision: intentRevision.Load()}, nil
		},
		OCIBootBarrier: barrier, HeartbeatInterval: time.Hour, ClaimInterval: time.Hour,
		ManagedRootDirectory: managedRoot, LogSpoolDirectory: t.TempDir(),
		CapabilityRevisionPath: filepath.Join(t.TempDir(), "wefty-capability-revision.json"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer nodeAgent.Close()
	if _, err := nodeAgent.Register(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := nodeAgent.RecoverOCIRuntimeCapabilities(t.Context()); err != nil {
		t.Fatal(err)
	}
	opened := nodeAgent.CapabilitySnapshot()
	if !opened.Capabilities["kind:oci"] || opened.ReasonCode != "" {
		t.Fatalf("initial OCI admission = %+v, want open OCI capability", opened)
	}

	// `wefty node oci stop`: the durable marker is disabled, the agent latches
	// admission closed, and Lima stops with oci_intent_disabled in its facts.
	intentEnabled.Store(false)
	intentRevision.Store(3)
	if err := nodeAgent.StopOCIRuntime(t.Context()); err != nil {
		t.Fatal(err)
	}
	barrier.stop()
	stopped := nodeAgent.CapabilitySnapshot()
	if stopped.Capabilities["kind:oci"] || stopped.ReasonCode != contract.CapabilityReasonOCIIntentDisabled {
		t.Fatalf("stopped observation = %+v, want restrictive oci_intent_disabled", stopped)
	}
	if stopped.Revision <= opened.Revision {
		t.Fatalf("stop revision = %d, want greater than %d", stopped.Revision, opened.Revision)
	}

	// `wefty node oci start`: durable intent is enabled again at a higher
	// revision while the runtime is healthy.
	intentEnabled.Store(true)
	intentRevision.Store(4)
	probesBeforeStart := probeCalls.Load()
	if err := nodeAgent.RecoverOCIRuntimeCapabilities(t.Context()); err != nil {
		t.Fatalf("operator start recovery: %v", err)
	}
	reopened := nodeAgent.CapabilitySnapshot()
	if !reopened.Capabilities["kind:oci"] || reopened.ReasonCode != "" {
		t.Fatalf("reopened observation = %+v, want open OCI capability with no reason", reopened)
	}
	if reopened.Revision <= stopped.Revision {
		t.Fatalf("reopened revision = %d, want greater than %d", reopened.Revision, stopped.Revision)
	}
	if nodeAgent.capabilities.ociIntentDisabled.Load() {
		t.Fatal("operator start left the durable disabled-intent latch armed")
	}
	if probeCalls.Load() <= probesBeforeStart {
		t.Fatalf("operator start ran %d probes, want at least one", probeCalls.Load()-probesBeforeStart)
	}
	if !nodeAgent.OCIRuntimeLive() {
		t.Fatal("operator start did not report a live OCI runtime")
	}
}

// TestCapabilityRevisionNeverRegressesAcrossAgentRestart pins the durability
// decision: the highest published Capability revision is persisted beside the
// durable node-local OCI intent marker, so a restarted agent publishes above it
// instead of replaying a revision an operator has already seen.
func TestCapabilityRevisionNeverRegressesAcrossAgentRestart(t *testing.T) {
	network := plain.NewNetwork()
	managedRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	stateDirectory := t.TempDir()
	revisionPath := filepath.Join(t.TempDir(), "wefty-capability-revision.json")
	logSpool := t.TempDir()
	newProcess := func(bootSessionID string) *Agent {
		t.Helper()
		agentFabric := network.NewFabric(fabric.Identity{
			NodeID: "fabric-node-revision-floor-" + bootSessionID, Tags: []string{l1.DefaultAgentPrincipalTag},
		})
		nodeAgent, err := New(Config{
			Fabric: agentFabric, ControlPlaneAddress: "wefty://control-plane", NodeID: "node-revision-floor",
			BootSessionID: bootSessionID, Version: "test", OS: "linux", Architecture: "amd64",
			Capabilities: map[string]bool{"kind:process": true, "kind:oci": true},
			CapabilityProbe: capabilityProbeFunc(func(context.Context) (CapabilityProbeResult, error) {
				return CapabilityProbeResult{Capabilities: map[string]bool{"kind:oci": true}}, nil
			}),
			OCIIntent:            enabledTestOCIIntent,
			OCIBootBarrier:       readyOCIBootBarrier{},
			HeartbeatInterval:    time.Hour,
			ClaimInterval:        time.Hour,
			ManagedRootDirectory: managedRoot,
			LogSpoolDirectory:    logSpool,
			HandoffRoot:          stateDirectory,
			// The floor deliberately lives beside the durable intent marker.
			CapabilityRevisionPath: revisionPath,
		})
		if err != nil {
			t.Fatal(err)
		}
		return nodeAgent
	}

	first := newProcess("boot-revision-floor-1")
	for range 3 {
		first.SuppressOCIRuntime(contract.CapabilityReasonHelperUnreachable, errors.New("helper unreachable"))
		first.capabilities.allowOCIIntent()
		if err := first.capabilities.refresh(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	published := first.CapabilitySnapshot().Revision
	if published < 2 {
		t.Fatalf("first process revision = %d, want several transitions", published)
	}
	first.Close()

	second := newProcess("boot-revision-floor-2")
	defer second.Close()
	restarted := second.CapabilitySnapshot().Revision
	if restarted <= published {
		t.Fatalf("restarted revision = %d, want greater than the %d already published", restarted, published)
	}
	second.SuppressOCIRuntime(contract.CapabilityReasonHelperUnreachable, errors.New("helper unreachable"))
	if next := second.CapabilitySnapshot().Revision; next <= restarted {
		t.Fatalf("restarted process revision = %d, want monotonic above %d", next, restarted)
	}
}

// TestColdStartUnderDisabledIntentPublishesIntentReason covers the restart and
// reboot half of #395: a node whose operator ran `wefty node oci stop` must
// come back publishing oci_intent_disabled with the latch armed. Nothing else
// re-arms it — Lima is stopped, so the supervisor never asks for background
// recovery — and without the latch the ADR-0003 guard in recordProbeResult is
// inert for the whole process.
func TestColdStartUnderDisabledIntentPublishesIntentReason(t *testing.T) {
	network := plain.NewNetwork()
	_, stopServer := startFailureServer(t, network, nil, map[string][]string{"node-cold-disabled": nil})
	defer stopServer()
	var intentEnabled atomic.Bool
	var intentRevision atomic.Uint64
	intentRevision.Store(3)
	barrier := &staleSupervisorBarrier{
		intentEnabled: &intentEnabled, reason: contract.CapabilityReasonOCIIntentDisabled,
	}
	var probeCalls atomic.Int32
	probe := capabilityProbeFunc(func(context.Context) (CapabilityProbeResult, error) {
		probeCalls.Add(1)
		if !intentEnabled.Load() {
			return CapabilityProbeResult{
				MissingCapabilities: []string{"kind:oci"}, ReasonCode: contract.CapabilityReasonOCIIntentDisabled,
			}, errors.New("OCI intent is disabled")
		}
		return CapabilityProbeResult{Capabilities: map[string]bool{"kind:oci": true}}, nil
	})
	agentFabric := network.NewFabric(fabric.Identity{
		NodeID: "fabric-node-cold-disabled", Tags: []string{l1.DefaultAgentPrincipalTag},
	})
	managedRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	nodeAgent, err := New(Config{
		Fabric: agentFabric, ControlPlaneAddress: "wefty://control-plane", NodeID: "node-cold-disabled",
		BootSessionID: "boot-cold-disabled", Version: "test", OS: "linux", Architecture: "amd64",
		Capabilities: map[string]bool{"kind:process": true}, CapabilityProbe: probe,
		OCIIntent: func(context.Context) (OCIIntentObservation, error) {
			return OCIIntentObservation{Enabled: intentEnabled.Load(), Revision: intentRevision.Load()}, nil
		},
		OCIBootBarrier: barrier, HeartbeatInterval: time.Hour, ClaimInterval: time.Hour,
		ManagedRootDirectory: managedRoot, LogSpoolDirectory: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer nodeAgent.Close()
	node, err := nodeAgent.Register(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if node.CapabilityReasonCode != contract.CapabilityReasonOCIIntentDisabled {
		t.Fatalf("published registration reason = %q, want oci_intent_disabled", node.CapabilityReasonCode)
	}
	cold := nodeAgent.CapabilitySnapshot()
	if cold.Capabilities["kind:oci"] || cold.ReasonCode != contract.CapabilityReasonOCIIntentDisabled {
		t.Fatalf("cold start observation = %+v, want restrictive oci_intent_disabled", cold)
	}
	if !nodeAgent.capabilities.ociIntentDisabled.Load() {
		t.Fatal("cold start under a durable disabled marker left the intent latch unarmed")
	}
	if err := nodeAgent.capabilities.refresh(t.Context()); !capabilityProbeWasSkipped(err) {
		t.Fatalf("routine refresh error = %v, want a typed disabled-intent skip", err)
	}

	// The operator starts OCI again; the same process must reopen.
	intentEnabled.Store(true)
	intentRevision.Store(4)
	if err := nodeAgent.RecoverOCIRuntimeCapabilities(t.Context()); err != nil {
		t.Fatalf("operator start recovery after cold start: %v", err)
	}
	reopened := nodeAgent.CapabilitySnapshot()
	if !reopened.Capabilities["kind:oci"] || reopened.ReasonCode != "" {
		t.Fatalf("reopened observation = %+v, want open OCI capability", reopened)
	}
	if reopened.Revision <= cold.Revision {
		t.Fatalf("reopened revision = %d, want greater than %d", reopened.Revision, cold.Revision)
	}
}

// TestCapabilityRevisionFloorFailsOnlyOnMalformedContent pins the asymmetry: a
// marker the agent cannot parse is a bug the operator must see, while one it
// cannot read is an environment problem that must not brick an unrelated node.
func TestCapabilityRevisionFloorFailsOnlyOnMalformedContent(t *testing.T) {
	directory := t.TempDir()
	malformed := filepath.Join(directory, "malformed.json")
	if err := os.WriteFile(malformed, []byte(`{"version":1,"capability_revision":0}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := loadCapabilityRevisionFloor(malformed, nil); err == nil {
		t.Fatal("malformed capability revision floor loaded without an error")
	}

	if os.Geteuid() == 0 {
		t.Skip("root bypasses the unreadable-file branch")
	}
	unreadable := filepath.Join(directory, "unreadable.json")
	if err := os.WriteFile(unreadable, []byte(`{"version":1,"capability_revision":9}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(unreadable, 0o000); err != nil {
		t.Fatal(err)
	}
	var logged int
	floor, err := loadCapabilityRevisionFloor(unreadable, func(string, ...any) { logged++ })
	if err != nil {
		t.Fatalf("unreadable capability revision floor failed agent start: %v", err)
	}
	if floor != nil {
		t.Fatal("unreadable capability revision floor was not degraded to per-process mode")
	}
	if logged == 0 {
		t.Fatal("unreadable capability revision floor degraded silently")
	}
	if start := floor.start(); start != 1 {
		t.Fatalf("degraded floor start = %d, want 1", start)
	}
}

// TestCapabilityRevisionFloorDefaultsUnderManagedRoot pins that every node with
// a managed root gets the durable floor, not only OCI-configured ones.
func TestCapabilityRevisionFloorDefaultsUnderManagedRoot(t *testing.T) {
	path := defaultCapabilityRevisionPath("/var/lib/wefty", "node-a")
	if path == "" || filepath.Dir(path) != filepath.Join("/var/lib/wefty", "capability") {
		t.Fatalf("default capability revision path = %q, want a node-scoped file under the managed root", path)
	}
	if same := defaultCapabilityRevisionPath("/var/lib/wefty", "node-b"); same == path {
		t.Fatal("default capability revision path is not node-scoped")
	}
	if empty := defaultCapabilityRevisionPath("", "node-a"); empty != "" {
		t.Fatalf("default capability revision path without a managed root = %q, want empty", empty)
	}
}
