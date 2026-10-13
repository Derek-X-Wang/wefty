//go:build darwin || linux

package agent

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
)

// The helper's root cannot be read until its boot barrier has completed.
// Serialize the existing fake root because the collector reads it while the
// test observes whether the helper was asked to give anything up.
type bootMeasurementRuntime struct {
	instantWorkloadRuntime
	barrier *recordingOCIBootBarrier
	mu      sync.Mutex
	root    fakeHelperHandoffRoot
}

func (runtime *bootMeasurementRuntime) InventoryRetainedHandoffs(ctx context.Context, after string) (workloadrunner.RetainedHandoffReport, error) {
	if !runtime.barrier.Ready() {
		return workloadrunner.RetainedHandoffReport{}, errors.New("OCI boot barrier has not completed")
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	return runtime.root.InventoryRetainedHandoffs(ctx, after)
}

func (runtime *bootMeasurementRuntime) RetainedHandoffVolumeName(ownerKey string) (string, error) {
	return runtime.root.RetainedHandoffVolumeName(ownerKey)
}

func (runtime *bootMeasurementRuntime) EvictRetainedHandoff(ctx context.Context, ownerKey string) error {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	return runtime.root.EvictRetainedHandoff(ctx, ownerKey)
}

func (runtime *bootMeasurementRuntime) evictionAttempts() int {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	return len(runtime.root.attempted)
}

func TestTheOCIBootBarrierMeasuresRetainedResultsBeforeTheCollectorTick(t *testing.T) {
	for _, failsAtStartup := range []bool{false, true} {
		name := "delayed startup barrier"
		if failsAtStartup {
			name = "failed startup barrier completes during recovery"
		}
		t.Run(name, func(t *testing.T) {
			harness := newRetentionHarness(t, 7*24*time.Hour)
			harness.retainOCI("run_oci", "attempt-1", true)
			harness.manager.nodeBytes = 1 << 20
			harness.manager.logf = t.Logf
			clock := newManualClock(harness.now)
			startedAt := clock.Now()
			barrierStarted := make(chan struct{})
			releaseBarrier := make(chan struct{})
			var ensures atomic.Int32
			barrier := &recordingOCIBootBarrier{ensure: func(ctx context.Context) error {
				if ensures.Add(1) == 1 {
					close(barrierStarted)
					select {
					case <-releaseBarrier:
					case <-ctx.Done():
						return ctx.Err()
					}
					if failsAtStartup {
						return errors.New("helper is still starting")
					}
				}
				return nil
			}}
			runtime := &bootMeasurementRuntime{barrier: barrier, root: fakeHelperHandoffRoot{
				volumes: []workloadrunner.RetainedHandoffVolume{ociVolume(t, "run_oci", 8<<20, 4, harness.now)},
			}}
			network := plain.NewNetwork()
			_, stopServer := startFailureServer(t, network, clock, map[string][]string{"node-1": nil})
			defer stopServer()
			nodeAgent, err := New(Config{
				Fabric:              network.NewFabric(fabric.Identity{NodeID: "boot-measurement-agent", Tags: []string{l1.DefaultAgentPrincipalTag}}),
				ControlPlaneAddress: "wefty://control-plane", NodeID: "node-1", BootSessionID: "boot-1", Version: "test",
				Clock: clock, LogSpoolDirectory: t.TempDir(), HandoffRoot: harness.root,
				OCIIntent: enabledTestOCIIntent, OCIBootBarrier: barrier,
				WorkloadRuntimes: map[string]WorkloadRuntime{contract.JobKindOCI: runtime},
				CapabilityProbe: capabilityProbeFunc(func(context.Context) (CapabilityProbeResult, error) {
					return CapabilityProbeResult{Capabilities: map[string]bool{"kind:oci": true}}, nil
				}),
				Logf: t.Logf,
			})
			if err != nil {
				t.Fatal(err)
			}
			nodeAgent.handoffs = harness.manager
			defer nodeAgent.Close()
			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan error, 1)
			go func() { done <- nodeAgent.Run(ctx) }()
			defer func() {
				cancel()
				if err := <-done; err != nil {
					t.Errorf("agent Run: %v", err)
				}
			}()
			select {
			case <-barrierStarted:
			case <-time.After(hostedFixtureTimeout):
				t.Fatal("agent did not reach the delayed boot barrier")
			}
			startup, measured := nodeAgent.RetainedResults()
			if !measured || !startup.OCIInventoryFailed || startup.OCI != nil {
				t.Fatalf("startup did not report the unread OCI root: %+v measured=%t", startup, measured)
			}
			clock.waitForDeadline(t, startedAt.Add(time.Hour))
			clock.Advance(10 * time.Minute)
			close(releaseBarrier)
			if failsAtStartup {
				waitForAgentLifecycle(t, nodeAgent, LifecycleReady)
				if err := nodeAgent.RecoverOCIRuntimeCapabilities(t.Context()); err != nil {
					t.Fatalf("complete the boot barrier during recovery: %v", err)
				}
			}
			if err := awaitFixtureCondition(t.Context(), "retained OCI figures after boot barrier", func() bool {
				status, measured := nodeAgent.RetainedResults()
				return measured && status.OCI != nil && !status.OCIInventoryFailed
			}); err != nil {
				t.Fatalf("doctor still has no OCI figures before the hourly tick: %v", err)
			}
			status, _ := nodeAgent.RetainedResults()
			if status.OCI.ChargedBytes != 8<<20 || status.OCI.Volumes != 1 || !status.MeasuredAt.Equal(clock.Now()) {
				t.Fatalf("post-barrier measurement lost the helper's figures: %+v", status)
			}
			if projected := nodeAgent.Status().RetainedResults; projected == nil || !reflect.DeepEqual(*projected, status) {
				t.Fatalf("status and the doctor accessor disagree: %+v vs %+v", projected, status)
			}
			if got := runtime.evictionAttempts(); got != 0 {
				t.Fatalf("the post-barrier measure-only pass attempted %d evictions", got)
			}
			// Completing the barrier must not move the original enforcing tick.
			if !clock.hasDeadline(startedAt.Add(time.Hour)) {
				t.Fatal("the post-barrier pass moved the collector's first tick")
			}
			clock.Advance(50*time.Minute - time.Second)
			if got := runtime.evictionAttempts(); got != 0 {
				t.Fatalf("the collector enforced before its first tick: %d evictions", got)
			}
			clock.Advance(time.Second)
			if err := awaitFixtureCondition(t.Context(), "first timer tick enforces the budget", func() bool {
				return runtime.evictionAttempts() == 1
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}
