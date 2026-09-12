package agent

import (
	"context"
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

// contendedRecoveryBarrier models the #409 race as it happens on real hardware.
// A Lima cold boot takes longer than the agent's heartbeat interval, so by the
// time the operator's recovery finishes, a second recovery -- the heartbeat
// loop's, the background Lima convergence loop's, or an attempt's helper-loss
// path -- is already parked on the session recovery mutex and retires the
// helper generation the instant that mutex is free. The fixture stands in for
// that competitor: whenever the recovery mutex is observed unheld, the
// competitor is deemed to have run, and it invalidates the barrier.
//
// Every read of the generation that belongs to the recovery transaction must
// therefore happen while the mutex is held, or the transaction is racing a
// recovery it cannot see.
type contendedRecoveryBarrier struct {
	mu        sync.Mutex
	ready     bool
	recovery  *sync.Mutex
	armed     atomic.Bool
	preempted atomic.Bool
}

func (barrier *contendedRecoveryBarrier) preemptWhenUnserialized() {
	if !barrier.armed.Load() || barrier.recovery == nil {
		return
	}
	if !barrier.recovery.TryLock() {
		return
	}
	barrier.recovery.Unlock()
	barrier.preempted.Store(true)
	barrier.Invalidate()
}

func (barrier *contendedRecoveryBarrier) Ready() bool {
	barrier.mu.Lock()
	defer barrier.mu.Unlock()
	return barrier.ready
}

func (barrier *contendedRecoveryBarrier) Ensure(context.Context) error {
	barrier.mu.Lock()
	defer barrier.mu.Unlock()
	barrier.ready = true
	return nil
}

func (barrier *contendedRecoveryBarrier) Invalidate() {
	barrier.mu.Lock()
	defer barrier.mu.Unlock()
	barrier.ready = false
}

func (barrier *contendedRecoveryBarrier) CapabilityReasonCode() contract.CapabilityReasonCode {
	barrier.mu.Lock()
	defer barrier.mu.Unlock()
	if barrier.ready {
		return ""
	}
	return contract.CapabilityReasonBootSweepFailed
}

func (barrier *contendedRecoveryBarrier) Generation() (ocihelper.HelperSession, bool) {
	barrier.preemptWhenUnserialized()
	barrier.mu.Lock()
	defer barrier.mu.Unlock()
	return ocihelper.HelperSession{HelperInstanceID: "contended-recovery", SessionGeneration: 1}, barrier.ready
}

func (barrier *contendedRecoveryBarrier) SweepReceipt() (ocihelper.VerifiedSweepReceipt, bool) {
	generation, ok := barrier.Generation()
	return ocihelper.VerifiedSweepReceipt{SweepEpoch: "contended-recovery", HelperSession: generation}, ok
}

func (barrier *contendedRecoveryBarrier) SetLossHandler(func(ocihelper.HelperSession, error)) {}

func (barrier *contendedRecoveryBarrier) Close() error { return nil }

// TestOperatorRecoveryPublishesInsideTheRecoveryTransaction is the #409 gap-1
// regression. `wefty node oci start` printed "OCI runtime recovery failed" and
// exited 1 on a recovery that had succeeded, because the pinned positive
// publication ran after the recovery mutex was released and a queued competing
// recovery invalidated the helper generation underneath it.
func TestOperatorRecoveryPublishesInsideTheRecoveryTransaction(t *testing.T) {
	network := plain.NewNetwork()
	_, stopServer := startFailureServer(t, network, nil, map[string][]string{"node-oci-contended": nil})
	defer stopServer()
	var intentRevision atomic.Uint64
	intentRevision.Store(3)
	barrier := &contendedRecoveryBarrier{}
	agentFabric := network.NewFabric(fabric.Identity{
		NodeID: "fabric-node-oci-contended", Tags: []string{l1.DefaultAgentPrincipalTag},
	})
	managedRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	nodeAgent, err := New(Config{
		Fabric: agentFabric, ControlPlaneAddress: "wefty://control-plane", NodeID: "node-oci-contended",
		BootSessionID: "boot-oci-contended", Version: "test", OS: "linux", Architecture: "amd64",
		Capabilities: map[string]bool{"kind:process": true},
		CapabilityProbe: capabilityProbeFunc(func(context.Context) (CapabilityProbeResult, error) {
			return CapabilityProbeResult{Capabilities: map[string]bool{"kind:oci": true}}, nil
		}),
		OCIIntent: func(context.Context) (OCIIntentObservation, error) {
			return OCIIntentObservation{Enabled: true, Revision: intentRevision.Load()}, nil
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

	barrier.recovery = &nodeAgent.session.ociRecoveryMu
	barrier.armed.Store(true)
	recoverErr := nodeAgent.RecoverOCIRuntimeCapabilities(t.Context())
	barrier.armed.Store(false)
	if recoverErr != nil {
		t.Fatalf("operator recovery = %v, want a truthful success for a recovery that reached publication", recoverErr)
	}
	if barrier.preempted.Load() {
		t.Fatal("a competing recovery ran between the probe and the pinned publication")
	}
	published := nodeAgent.CapabilitySnapshot()
	if !published.Capabilities["kind:oci"] || published.ReasonCode != "" {
		t.Fatalf("published observation = %+v, want open OCI capability with no reason", published)
	}
}
