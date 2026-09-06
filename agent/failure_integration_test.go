//go:build darwin || linux

package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	"github.com/Derek-X-Wang/wefty/runner/lima"
	processrunner "github.com/Derek-X-Wang/wefty/runner/process"
)

func TestOCIIntentStopCancellationCannotCompleteOrRestartService(t *testing.T) {
	network := plain.NewNetwork()
	store, stopServer := startFailureServerWithPoliciesAndLease(t, network, nil, map[string]l1.NodePolicy{
		"intent-node": {Tags: []string{"intent-stop"}, MaxOneshotSlots: 1, MaxServiceSlots: 1},
	}, 500*time.Millisecond)
	defer stopServer()
	digest := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	job, _, err := store.CreateJob(t.Context(), contract.JobSpec{
		SchemaVersion: contract.SchemaVersionV1, DispatchKey: "intent-stop-cancel-first",
		Kind: contract.JobKindOCI, Class: contract.JobClassService, Restart: contract.RestartAlways,
		RoutingTags: []string{"intent-stop"}, RuntimeHandler: "io.containerd.runc.v2",
		Execution: contract.ExecutionSpec{OCI: &contract.OCIExecutionSpec{
			Image: contract.OCIImageSpec{Reference: "example.invalid/intent-stop:v1", Digest: &digest},
			Argv:  []string{"/payload"},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	intentPath := filepath.Join(t.TempDir(), "oci-intent.json")
	if _, err := lima.InitializeOCIIntent(intentPath, time.Now()); err != nil {
		t.Fatal(err)
	}
	intentSource := lima.FileIntentSource{Path: intentPath}
	runtime := newIntentStopRuntime()
	agentFabric := network.NewFabric(fabric.Identity{NodeID: "intent-agent", Tags: []string{l1.DefaultAgentPrincipalTag}})
	managedRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	nodeAgent, err := New(Config{
		Fabric: agentFabric, ControlPlaneAddress: "wefty://control-plane",
		NodeID: "intent-node", BootSessionID: "intent-boot", Version: "test",
		Capabilities: map[string]bool{
			"kind:process": true, "kind:oci": true, "runtime_handler:io.containerd.runc.v2": true,
		},
		CapabilityProbe: capabilityProbeFunc(func(ctx context.Context) (CapabilityProbeResult, error) {
			intent, err := intentSource.ReadIntent(ctx)
			if err != nil || !intent.Enabled {
				if err == nil {
					err = errors.New("OCI intent is disabled")
				}
				return CapabilityProbeResult{MissingCapabilities: []string{"kind:oci"}, ReasonCode: contract.CapabilityReasonOCIIntentDisabled}, err
			}
			return CapabilityProbeResult{Capabilities: map[string]bool{
				"kind:oci": true, "runtime_handler:io.containerd.runc.v2": true,
			}}, nil
		}),
		OCIIntent: func(ctx context.Context) (OCIIntentObservation, error) {
			intent, err := intentSource.ReadIntent(ctx)
			return OCIIntentObservation{Enabled: intent.Enabled, Revision: intent.Revision}, err
		},
		OCIBootBarrier:       readyOCIBootBarrier{},
		WorkloadRuntimes:     map[string]WorkloadRuntime{contract.JobKindOCI: runtime},
		ManagedRootDirectory: managedRoot, LogSpoolDirectory: t.TempDir(), MaxServiceSlots: 1,
		HeartbeatInterval: 50 * time.Millisecond, ClaimInterval: 5 * time.Millisecond, RenewalInterval: 50 * time.Millisecond,
		Logf: t.Logf,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer nodeAgent.Close()
	runContext, cancelRun := context.WithCancel(t.Context())
	defer cancelRun()
	defer runtime.release()
	runDone := make(chan error, 1)
	go func() { runDone <- nodeAgent.Run(runContext) }()
	var attemptID string
	select {
	case attemptID = <-runtime.started:
	case <-time.After(5 * time.Second):
		current, _ := store.GetJob(t.Context(), job.JobID)
		nodes, _ := store.ListNodes(t.Context())
		t.Fatalf("intent-stop OCI service did not start: job=%+v nodes=%+v agent=%+v", current, nodes, nodeAgent.Status())
	}
	stopDone := make(chan error, 1)
	go func() {
		_, stopErr := lima.SetOCIIntent(t.Context(), intentPath, 1, false, time.Now())
		if stopErr == nil {
			stopErr = nodeAgent.StopOCIRuntime(t.Context())
		}
		stopDone <- stopErr
	}()
	runtime.waitCanceled(t)
	// Cancellation is now the only ready lifecycle branch. Delay terminal
	// publication until it has won, reproducing the native payload ordering.
	runtime.release()
	if err := <-stopDone; err != nil {
		cancelRun()
		t.Fatal(err)
	}
	convergenceResults := make(chan error, 4)
	for range 4 {
		go func() { convergenceResults <- nodeAgent.RecoverOCIRuntimeCapabilities(t.Context()) }()
	}
	for range 4 {
		if err := <-convergenceResults; err == nil {
			cancelRun()
			t.Fatal("background OCI convergence recovered across durable disabled intent")
		}
	}
	if snapshot := nodeAgent.CapabilitySnapshot(); snapshot.Capabilities["kind:oci"] || snapshot.ReasonCode != contract.CapabilityReasonOCIIntentDisabled {
		cancelRun()
		t.Fatalf("disabled OCI intent capability snapshot=%+v", snapshot)
	}
	queued, err := waitForFailureJobState(store, job.JobID, contract.JobQueued, 3*time.Second)
	if err != nil {
		cancelRun()
		t.Fatal(err)
	}
	reconciliation, err := store.Reconcile(t.Context())
	// Observing queued above proves the server's periodic reconciler serialized
	// first; a second pass must report no duplicate transition.
	if err != nil || reconciliation.ExpiredAttempts != 0 {
		cancelRun()
		t.Fatalf("post-intent-stop idempotent reconciliation=%+v err=%v", reconciliation, err)
	}
	attempts, err := store.ListJobAttempts(t.Context(), job.JobID)
	if err != nil || len(attempts) != 1 || attempts[0].AttemptID != attemptID || attempts[0].State != contract.AttemptLost || attempts[0].Result != nil {
		cancelRun()
		t.Fatalf("intent-stop expiry evidence=%+v err=%v", attempts, err)
	}
	if queued.State != contract.JobQueued || queued.CurrentAttemptID != attemptID ||
		queued.BoundNodeID != "intent-node" || queued.RestartStreak != 0 || queued.LifetimeRestartCount != 0 ||
		queued.LeaseLossCount != 1 || queued.NextRestartAt == nil || !queued.NextRestartAt.After(queued.UpdatedAt) || len(queued.LastFailure) != 0 ||
		runtime.starts.Load() != 1 {
		cancelRun()
		t.Fatalf("intent-stop service=%+v starts=%d err=%v", queued, runtime.starts.Load(), err)
	}
	started, err := lima.SetOCIIntent(t.Context(), intentPath, 2, true, time.Now())
	if err == nil {
		err = nodeAgent.RecoverOCIRuntimeCapabilities(t.Context())
	}
	if err != nil || !started.Enabled || !nodeAgent.OCIRuntimeLive() {
		cancelRun()
		t.Fatalf("explicit OCI start intent=%+v live=%t err=%v", started, nodeAgent.OCIRuntimeLive(), err)
	}
	select {
	case nextAttemptID := <-runtime.started:
		if nextAttemptID == attemptID || runtime.starts.Load() != 2 {
			cancelRun()
			t.Fatalf("explicit OCI start attempt=%q prior=%q starts=%d", nextAttemptID, attemptID, runtime.starts.Load())
		}
	case <-time.After(5 * time.Second):
		cancelRun()
		current, _ := store.GetJob(t.Context(), job.JobID)
		nodes, _ := store.ListNodes(t.Context())
		t.Fatalf("explicit OCI start did not restore service admission: job=%+v service=%+v nodes=%+v status=%+v", current, current.ServiceJob, nodes, nodeAgent.Status())
	}
	cancelRun()
	if err := <-runDone; err != nil {
		t.Fatalf("agent shutdown: %v", err)
	}
}

func TestPreStartedOCIRuntimeLossRetainsSpawnFailureThroughRecoveryError(t *testing.T) {
	for _, test := range []struct {
		name       string
		intentStop bool
	}{
		{name: "enabled intent delivers phase-valid spawn failure"},
		{name: "disabled intent suppresses phase-valid spawn failure", intentStop: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			network := plain.NewNetwork()
			store, stopServer := startFailureServerWithPoliciesAndLease(t, network, nil, map[string]l1.NodePolicy{
				"prestarted-loss-node": {Tags: []string{"prestarted-loss"}, MaxOneshotSlots: 1, MaxServiceSlots: 1},
			}, 750*time.Millisecond)
			defer stopServer()
			digest := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
			job, _, err := store.CreateJob(t.Context(), contract.JobSpec{
				SchemaVersion: contract.SchemaVersionV1, DispatchKey: "prestarted-loss-service",
				Kind: contract.JobKindOCI, Class: contract.JobClassService, Restart: contract.RestartAlways,
				RoutingTags: []string{"prestarted-loss"}, RuntimeHandler: "io.containerd.runc.v2",
				Execution: contract.ExecutionSpec{OCI: &contract.OCIExecutionSpec{
					Image: contract.OCIImageSpec{Reference: "example.invalid/prestarted-loss:v1", Digest: &digest},
					Argv:  []string{"/payload"},
				}},
			})
			if err != nil {
				t.Fatal(err)
			}
			intentPath := filepath.Join(t.TempDir(), "oci-intent.json")
			if _, err := lima.InitializeOCIIntent(intentPath, time.Now()); err != nil {
				t.Fatal(err)
			}
			intentSource := lima.FileIntentSource{Path: intentPath}
			barrier := &stageLossBarrier{ready: true}
			runtime := newPreStartedRuntimeLossRuntime(barrier)
			agentFabric := network.NewFabric(fabric.Identity{NodeID: "prestarted-loss-agent", Tags: []string{l1.DefaultAgentPrincipalTag}})
			managedRoot, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			nodeAgent, err := New(Config{
				Fabric: agentFabric, ControlPlaneAddress: "wefty://control-plane",
				NodeID: "prestarted-loss-node", BootSessionID: "prestarted-loss-boot", Version: "test",
				Capabilities: map[string]bool{"kind:process": true},
				CapabilityProbe: capabilityProbeFunc(func(ctx context.Context) (CapabilityProbeResult, error) {
					intent, err := intentSource.ReadIntent(ctx)
					if err != nil || !intent.Enabled {
						if err == nil {
							err = errors.New("OCI intent is disabled")
						}
						return CapabilityProbeResult{MissingCapabilities: []string{"kind:oci"}, ReasonCode: contract.CapabilityReasonOCIIntentDisabled}, err
					}
					return CapabilityProbeResult{Capabilities: map[string]bool{
						"kind:oci": true, "runtime_handler:io.containerd.runc.v2": true,
					}}, nil
				}),
				OCIIntent: func(ctx context.Context) (OCIIntentObservation, error) {
					intent, err := intentSource.ReadIntent(ctx)
					return OCIIntentObservation{Enabled: intent.Enabled, Revision: intent.Revision}, err
				},
				OCIBootBarrier: barrier, WorkloadRuntimes: map[string]WorkloadRuntime{contract.JobKindOCI: runtime},
				ManagedRootDirectory: managedRoot, LogSpoolDirectory: t.TempDir(), MaxServiceSlots: 1,
				HeartbeatInterval: 20 * time.Millisecond, ClaimInterval: 5 * time.Millisecond, RenewalInterval: 50 * time.Millisecond,
				Logf: t.Logf,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer nodeAgent.Close()
			runContext, cancelRun := context.WithCancel(t.Context())
			runDone := make(chan error, 1)
			go func() { runDone <- nodeAgent.Run(runContext) }()
			select {
			case <-runtime.entered:
			case <-time.After(5 * time.Second):
				cancelRun()
				t.Fatal("pre-Started OCI runtime did not enter Run")
			}
			if test.intentStop {
				intent, err := lima.SetOCIIntent(t.Context(), intentPath, 1, false, time.Now())
				if err != nil {
					cancelRun()
					t.Fatal(err)
				}
				releaseFence, err := nodeAgent.FenceOCIIntentStop(t.Context(), intent.Revision)
				if err != nil {
					cancelRun()
					t.Fatal(err)
				}
				releaseFence()
			} else if _, err := store.SetNodeClaimsByOperator(t.Context(), "prestarted-loss-node", "operator", l1.NodeIntentRequest{
				ClaimsEnabled: false, IntentRevision: 0, Reason: "hold replacement while checking pre-Started completion",
			}); err != nil {
				cancelRun()
				t.Fatal(err)
			}
			close(runtime.release)
			deadline := time.Now().Add(3 * time.Second)
			if test.intentStop {
				for {
					receipt := nodeAgent.outbox.spool.inspectCompletion(t.Context(), runtime.attemptID)
					if receipt.State == "suppressed" && receipt.Reason == "service_intent_stop" && receipt.IntentRevision == 2 &&
						receipt.Result.SpawnError != nil && receipt.Result.SpawnError.Code == contract.SpawnFailureRuntimeUnavailable &&
						receipt.Result.OutputError == "" {
						break
					}
					if time.Now().After(deadline) {
						cancelRun()
						t.Fatalf("pre-Started intent-stop completion was not suppressed with its spawn failure: receipt=%+v", receipt)
					}
					time.Sleep(5 * time.Millisecond)
				}
				lostDeadline := time.Now().Add(3 * time.Second)
				for {
					if _, err := store.Reconcile(t.Context()); err != nil {
						cancelRun()
						t.Fatal(err)
					}
					attempts, listErr := store.ListJobAttempts(t.Context(), job.JobID)
					if listErr == nil && len(attempts) == 1 && attempts[0].State == contract.AttemptLost && attempts[0].Result == nil {
						break
					}
					if time.Now().After(lostDeadline) {
						cancelRun()
						t.Fatalf("intent-stopped pre-Started attempt did not expire lost with no result: attempts=%+v err=%v", attempts, listErr)
					}
					time.Sleep(5 * time.Millisecond)
				}
			} else {
				for {
					attempts, listErr := store.ListJobAttempts(t.Context(), job.JobID)
					if listErr == nil && len(attempts) == 1 && attempts[0].State == contract.AttemptFailed && attempts[0].Result != nil {
						if attempts[0].Result.SpawnError == nil || attempts[0].Result.SpawnError.Code != contract.SpawnFailureRuntimeUnavailable || attempts[0].Result.OutputError != "" {
							cancelRun()
							t.Fatalf("pre-Started completion=%+v, want sole runtime_unavailable spawn failure", attempts[0].Result)
						}
						break
					}
					if time.Now().After(deadline) {
						cancelRun()
						t.Fatalf("pre-Started attempt remained uncompleted: attempts=%+v err=%v", attempts, listErr)
					}
					time.Sleep(5 * time.Millisecond)
				}
			}
			cancelRun()
			if err := <-runDone; err != nil && !strings.Contains(err.Error(), "helper remains unavailable") {
				t.Fatalf("agent shutdown after pre-Started runtime loss: %v", err)
			}
		})
	}
}

func TestPreStartedOCIRuntimeLossReturnsAllDiagnostics(t *testing.T) {
	generation := workloadrunner.RuntimeGeneration{InstanceID: "stage-loss", Generation: 1}
	runtime := &diagnosticPreStartedRuntimeLossRuntime{intentStopRuntime: newIntentStopRuntime(), generation: generation}
	lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{
		runtimes:            workloadRuntimeSet{contract.JobKindOCI: runtime},
		clock:               systemClock{},
		finalizationTimeout: time.Second,
		currentOCIGeneration: func() (workloadrunner.RuntimeGeneration, bool) {
			return generation, true
		},
		recoverOCIRuntime: func(context.Context, workloadrunner.RuntimeGeneration) error {
			return errors.New("helper remains unavailable after injected loss")
		},
	})
	result, err := lifecycle.runWorkload(t.Context(), l1.Claim{
		Job: l1.Job{JobID: "prestarted-diagnostics", Spec: contract.JobSpec{
			Kind: contract.JobKindOCI, Class: contract.JobClassOneShot,
			Execution: contract.ExecutionSpec{OCI: &contract.OCIExecutionSpec{
				Image: contract.OCIImageSpec{Reference: "example.invalid/prestarted-diagnostics:v1"},
			}},
		}},
		Lease: l1.AttemptLease{AttemptID: "prestarted-diagnostics-attempt"},
	})
	if result.SpawnError == nil || result.SpawnError.Code != contract.SpawnFailureRuntimeUnavailable || result.OutputError != "" {
		t.Fatalf("pre-Started result = %#v, want sole runtime_unavailable spawn failure", result)
	}
	for _, diagnostic := range []string{
		"helper lost during Run before Started",
		"helper remains unavailable after injected loss",
		"helper unavailable while reaping pre-Started attempt",
	} {
		if err == nil || !strings.Contains(err.Error(), diagnostic) {
			t.Fatalf("pre-Started lifecycle error = %v, want diagnostic %q", err, diagnostic)
		}
	}
}

func TestOCIIntentStopOutcomeWinsSuppressesRestartReplay(t *testing.T) {
	network := plain.NewNetwork()
	store, stopServer := startFailureServerWithPoliciesAndLease(t, network, nil, map[string]l1.NodePolicy{
		"intent-outcome-node": {Tags: []string{"intent-outcome"}, MaxOneshotSlots: 1, MaxServiceSlots: 1},
	}, 2*time.Second)
	defer stopServer()
	digest := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	job, _, err := store.CreateJob(t.Context(), contract.JobSpec{
		SchemaVersion: contract.SchemaVersionV1, DispatchKey: "intent-stop-outcome-first",
		Kind: contract.JobKindOCI, Class: contract.JobClassService, Restart: contract.RestartAlways,
		RoutingTags: []string{"intent-outcome"}, RuntimeHandler: "io.containerd.runc.v2",
		Execution: contract.ExecutionSpec{OCI: &contract.OCIExecutionSpec{
			Image: contract.OCIImageSpec{Reference: "example.invalid/intent-stop:v1", Digest: &digest},
			Argv:  []string{"/payload"},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	spoolDirectory := t.TempDir()
	managedRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	runtime := newOutcomeFirstIntentRuntime()
	var intentEnabled atomic.Bool
	intentEnabled.Store(true)
	newAgent := func(bootSessionID string, workload WorkloadRuntime) *Agent {
		agentFabric := network.NewFabric(fabric.Identity{NodeID: "intent-outcome-agent-" + bootSessionID, Tags: []string{l1.DefaultAgentPrincipalTag}})
		nodeAgent, err := New(Config{
			Fabric: agentFabric, ControlPlaneAddress: "wefty://control-plane",
			NodeID: "intent-outcome-node", BootSessionID: bootSessionID, Version: "test",
			Capabilities: map[string]bool{
				"kind:process": true, "kind:oci": true, "runtime_handler:io.containerd.runc.v2": true,
			},
			CapabilityProbe: capabilityProbeFunc(func(context.Context) (CapabilityProbeResult, error) {
				return CapabilityProbeResult{Capabilities: map[string]bool{
					"kind:oci": true, "runtime_handler:io.containerd.runc.v2": true,
				}}, nil
			}),
			OCIIntent: func(context.Context) (OCIIntentObservation, error) {
				enabled := intentEnabled.Load()
				revision := uint64(1)
				if !enabled {
					revision = 2
				}
				return OCIIntentObservation{Enabled: enabled, Revision: revision}, nil
			},
			OCIBootBarrier: readyOCIBootBarrier{}, WorkloadRuntimes: map[string]WorkloadRuntime{contract.JobKindOCI: workload},
			ManagedRootDirectory: managedRoot, LogSpoolDirectory: spoolDirectory, MaxServiceSlots: 1,
			HeartbeatInterval: 50 * time.Millisecond, ClaimInterval: 5 * time.Millisecond, RenewalInterval: 50 * time.Millisecond,
			Logf: t.Logf,
		})
		if err != nil {
			t.Fatal(err)
		}
		return nodeAgent
	}

	nodeAgent := newAgent("intent-outcome-boot", runtime)
	runContext, cancelRun := context.WithCancel(t.Context())
	runDone := make(chan error, 1)
	go func() { runDone <- nodeAgent.Run(runContext) }()
	var attemptID string
	select {
	case attemptID = <-runtime.started:
	case <-time.After(5 * time.Second):
		t.Fatal("outcome-first OCI service did not start")
	}
	stopDone := make(chan error, 1)
	nodeAgent.outbox.completionStored = func() {
		intentEnabled.Store(false)
		go func() {
			releaseFence, err := nodeAgent.FenceOCIIntentStop(t.Context(), 2)
			if err == nil {
				releaseFence()
				err = nodeAgent.StopOCIRuntime(t.Context())
			}
			stopDone <- err
		}()
		deadline := time.Now().Add(time.Second)
		for time.Now().Before(deadline) {
			if nodeAgent.CapabilitySnapshot().ReasonCode == contract.CapabilityReasonOCIIntentDisabled {
				return
			}
			time.Sleep(time.Millisecond)
		}
		t.Error("OCI intent stop did not cancel the outcome-selected resident")
	}
	runtime.complete()
	if err := <-stopDone; err != nil {
		t.Fatal(err)
	}
	receipt := nodeAgent.outbox.spool.inspectCompletion(t.Context(), attemptID)
	if receipt.State != "suppressed" || receipt.Reason != "service_intent_stop" || receipt.IntentRevision != 2 {
		t.Fatalf("intent-stop outcome receipt=%+v", receipt)
	}
	stillRunning, err := store.GetJob(t.Context(), job.JobID)
	if err != nil || stillRunning.State != contract.JobClaimed || stillRunning.CurrentAttemptID != attemptID ||
		stillRunning.BoundNodeID != "intent-outcome-node" || stillRunning.RestartStreak != 0 {
		t.Fatalf("outcome-first intent stop changed service authority: job=%+v err=%v", stillRunning, err)
	}
	cancelRun()
	if err := <-runDone; err != nil {
		t.Fatalf("first agent shutdown: %v", err)
	}
	nodeAgent.Close()

	restartedRuntime := newIntentStopRuntime()
	restarted := newAgent("intent-outcome-boot", restartedRuntime)
	restartContext, cancelRestart := context.WithCancel(t.Context())
	restartDone := make(chan error, 1)
	go func() { restartDone <- restarted.Run(restartContext) }()
	time.Sleep(150 * time.Millisecond)
	afterReplay, err := store.GetJob(t.Context(), job.JobID)
	if err != nil || afterReplay.State != contract.JobClaimed || afterReplay.CurrentAttemptID != attemptID ||
		afterReplay.BoundNodeID != "intent-outcome-node" || afterReplay.RestartStreak != 0 || restartedRuntime.starts.Load() != 0 {
		t.Fatalf("restart replay consumed retained service authority: job=%+v starts=%d err=%v", afterReplay, restartedRuntime.starts.Load(), err)
	}
	cancelRestart()
	if err := <-restartDone; err != nil {
		t.Fatalf("restarted agent shutdown: %v", err)
	}
	restarted.Close()
}

func TestDurableOCIIntentStopFencesFinishedServiceAttempt(t *testing.T) {
	network := plain.NewNetwork()
	clock := newManualClock(time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC))
	store, stopServer := startFailureServerWithPoliciesAndLease(t, network, clock, map[string]l1.NodePolicy{
		"intent-marker-node": {Tags: []string{"intent-marker"}, MaxOneshotSlots: 1, MaxServiceSlots: 1},
	}, 500*time.Millisecond)
	defer stopServer()
	digest := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	job, _, err := store.CreateJob(t.Context(), contract.JobSpec{
		SchemaVersion: contract.SchemaVersionV1, DispatchKey: "intent-marker-finished-service",
		Kind: contract.JobKindOCI, Class: contract.JobClassService, Restart: contract.RestartAlways,
		RoutingTags: []string{"intent-marker"}, RuntimeHandler: "io.containerd.runc.v2",
		Execution: contract.ExecutionSpec{OCI: &contract.OCIExecutionSpec{
			Image: contract.OCIImageSpec{Reference: "example.invalid/intent-marker:v1", Digest: &digest},
			Argv:  []string{"/payload"},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	intentPath := filepath.Join(t.TempDir(), "oci-intent.json")
	if _, err := lima.InitializeOCIIntent(intentPath, time.Now()); err != nil {
		t.Fatal(err)
	}
	intentSource := lima.FileIntentSource{Path: intentPath}
	intentEnabled := func(ctx context.Context) (OCIIntentObservation, error) {
		intent, err := intentSource.ReadIntent(ctx)
		return OCIIntentObservation{Enabled: intent.Enabled, Revision: intent.Revision}, err
	}
	runtime := newIntentMarkerRaceRuntime()
	deadman := newRecordingDeadmanRenewer()
	agentFabric := network.NewFabric(fabric.Identity{NodeID: "intent-marker-agent", Tags: []string{l1.DefaultAgentPrincipalTag}})
	managedRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	nodeAgent, err := New(Config{
		Fabric: agentFabric, ControlPlaneAddress: "wefty://control-plane",
		NodeID: "intent-marker-node", BootSessionID: "intent-marker-boot", Version: "test",
		Capabilities: map[string]bool{
			"kind:process": true, "kind:oci": true, "runtime_handler:io.containerd.runc.v2": true,
		},
		CapabilityProbe: capabilityProbeFunc(func(context.Context) (CapabilityProbeResult, error) {
			return CapabilityProbeResult{Capabilities: map[string]bool{
				"kind:oci": true, "runtime_handler:io.containerd.runc.v2": true,
			}}, nil
		}),
		OCIIntent:      intentEnabled,
		OCIBootBarrier: readyOCIBootBarrier{}, WorkloadRuntimes: map[string]WorkloadRuntime{contract.JobKindOCI: runtime},
		ManagedRootDirectory: managedRoot, LogSpoolDirectory: t.TempDir(), MaxServiceSlots: 1,
		HeartbeatInterval: 50 * time.Millisecond, ClaimInterval: 5 * time.Millisecond, RenewalInterval: 50 * time.Millisecond,
		AttemptDeadman: deadman, ComputerTokenMinter: &recordingComputerTokenMinter{}, Logf: t.Logf,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nodeAgent.Close)
	intentObserved := make(chan struct{})
	releaseCompletion := make(chan struct{})
	var releaseCompletionOnce sync.Once
	release := func() { releaseCompletionOnce.Do(func() { close(releaseCompletion) }) }
	t.Cleanup(release)
	var observeOnce sync.Once
	nodeAgent.ociIntentGate.observed = func(observation OCIIntentObservation) {
		if observation.Enabled {
			observeOnce.Do(func() { close(intentObserved) })
			<-releaseCompletion
		}
	}
	runContext, cancelRun := context.WithCancel(t.Context())
	defer cancelRun()
	runDone := make(chan error, 1)
	go func() { runDone <- nodeAgent.Run(runContext) }()
	var firstAttempt string
	select {
	case firstAttempt = <-runtime.started:
	case startErr := <-runtime.startFailed:
		t.Fatalf("intent-marker OCI service start failed: %v", startErr)
	case runErr := <-runDone:
		t.Fatalf("intent-marker agent exited before OCI service start: %v", runErr)
	case <-time.After(5 * time.Second):
		queued, jobErr := store.GetJob(t.Context(), job.JobID)
		attempts, attemptsErr := store.ListJobAttempts(t.Context(), job.JobID)
		t.Fatalf("intent-marker OCI service did not start: job=%+v job_err=%v attempts=%+v attempts_err=%v status=%+v capability=%+v",
			queued, jobErr, attempts, attemptsErr, nodeAgent.Status(), nodeAgent.CapabilitySnapshot())
	}
	deadman.waitForRenewal(t)
	if calls, _, generation := deadman.snapshot(); calls < 1 || generation != readyOCIHelperGeneration() {
		t.Fatalf("intent-marker deadman renewals=%d generation=%+v, want at least one renewal for %+v", calls, generation, readyOCIHelperGeneration())
	}
	runtime.finishFirst()
	select {
	case <-intentObserved:
	case <-time.After(3 * time.Second):
		t.Fatal("completion did not pause after its final enabled intent read")
	}
	stopDone := make(chan error, 1)
	go func() {
		intent, stopErr := lima.SetOCIIntent(t.Context(), intentPath, 1, false, time.Now())
		if stopErr == nil {
			var releaseFence func()
			releaseFence, stopErr = nodeAgent.FenceOCIIntentStop(t.Context(), intent.Revision)
			if stopErr == nil {
				releaseFence()
				stopErr = nodeAgent.StopOCIRuntime(t.Context())
			}
		}
		stopDone <- stopErr
	}()
	deadline := time.Now().Add(3 * time.Second)
	for {
		intent, readErr := intentSource.ReadIntent(t.Context())
		if readErr == nil && !intent.Enabled && intent.Revision == 2 && nodeAgent.ociIntentGate.disabledRevision.Load() == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("stop did not durably flip intent while completion paused: intent=%+v err=%v", intent, readErr)
		}
		time.Sleep(time.Millisecond)
	}
	clock.Advance(time.Second)
	deadline = time.Now().Add(3 * time.Second)
	for {
		if _, err := store.Reconcile(t.Context()); err != nil {
			t.Fatal(err)
		}
		queued, queueErr := store.GetJob(t.Context(), job.JobID)
		if queueErr != nil {
			t.Fatal(queueErr)
		}
		if queued.State == contract.JobQueued {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("intent-stop job did not reach queued after lease expiry while completion paused: %+v", queued)
		}
		time.Sleep(time.Millisecond)
	}
	deadline = time.Now().Add(3 * time.Second)
	for {
		nodeAgent.capabilities.mu.RLock()
		pendingRevision := nodeAgent.capabilities.pendingPublicationRevision
		nodeAgent.capabilities.mu.RUnlock()
		if pendingRevision == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("intent-stop capability revision %d was not published before replacement check", pendingRevision)
		}
		time.Sleep(time.Millisecond)
	}
	nodes, err := store.ListNodes(t.Context())
	if err != nil || len(nodes) != 1 || nodes[0].Capabilities["kind:oci"] {
		t.Fatalf("replacement OCI admission remained published while intent-stop completion reader was held: nodes=%+v err=%v", nodes, err)
	}
	attemptsBeforeRelease, err := store.ListJobAttempts(t.Context(), job.JobID)
	if err != nil || len(attemptsBeforeRelease) != 1 || attemptsBeforeRelease[0].AttemptID != firstAttempt {
		t.Fatalf("replacement OCI attempt was admitted before the intent-stop completion reader released: attempts=%+v err=%v", attemptsBeforeRelease, err)
	}
	release()
	if err := <-stopDone; err != nil {
		t.Fatal(err)
	}

	var secondAttempt string
	deadline = time.Now().Add(3 * time.Second)
	for secondAttempt == "" {
		receipt := nodeAgent.outbox.spool.inspectCompletion(t.Context(), firstAttempt)
		if receipt.State == "suppressed" && receipt.Reason == "service_intent_stop" && receipt.IntentRevision == 1 &&
			receipt.Result.ExitCode != nil && *receipt.Result.ExitCode == 1 && nodeAgent.LiveOCIAttempts() == 0 {
			break
		}
		select {
		case secondAttempt = <-runtime.started:
		case <-time.After(5 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			t.Fatalf("intent-stop completion neither suppressed nor restarted: receipt=%+v", nodeAgent.outbox.spool.inspectCompletion(t.Context(), firstAttempt))
		}
	}
	var queued l1.Job
	deadline = time.Now().Add(3 * time.Second)
	for {
		if _, err := store.Reconcile(t.Context()); err != nil {
			t.Fatal(err)
		}
		queued, err = store.GetJob(t.Context(), job.JobID)
		if err != nil {
			t.Fatal(err)
		}
		if queued.State == contract.JobQueued {
			break
		}
		if time.Now().After(deadline) {
			attempts, attemptsErr := store.ListJobAttempts(t.Context(), job.JobID)
			t.Fatalf("intent-stop job remained %q after lease expiry: attempts=%+v err=%v status=%+v receipt=%+v", queued.State, attempts, attemptsErr, nodeAgent.Status(), nodeAgent.outbox.spool.inspectCompletion(t.Context(), firstAttempt))
		}
		time.Sleep(5 * time.Millisecond)
	}
	attempts, err := store.ListJobAttempts(t.Context(), job.JobID)
	if err != nil || len(attempts) != 1 || attempts[0].AttemptID != firstAttempt || attempts[0].State != contract.AttemptLost || attempts[0].Result != nil {
		t.Fatalf("intent-stop marker evidence=%+v second_attempt=%q job=%+v err=%v", attempts, secondAttempt, queued, err)
	}
	cancelRun()
	if err := <-runDone; err != nil {
		t.Fatalf("agent shutdown: %v", err)
	}
}

func TestLostLeaseCompletionReconcilesAfterFreshServiceReadmission(t *testing.T) {
	network := plain.NewNetwork()
	store, stopServer := startFailureServerWithPoliciesAndLease(t, network, nil, map[string]l1.NodePolicy{
		"lease-loss-node": {Tags: []string{"lease-loss"}, MaxOneshotSlots: 1, MaxServiceSlots: 1},
	}, time.Second)
	defer stopServer()
	agentFabric := network.NewFabric(fabric.Identity{NodeID: "lease-loss-agent", Tags: []string{l1.DefaultAgentPrincipalTag}})
	client, err := newClient(agentFabric, "wefty://control-plane", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	registration := contract.NodeRegistration{
		NodeID: "lease-loss-node", BootSessionID: "lease-loss-boot", OS: "linux", Architecture: "amd64", AgentVersion: "test",
		Capabilities: map[string]bool{
			"kind:process": true, "kind:oci": true, "runtime_handler:io.containerd.runc.v2": true,
		},
		CapabilityRevision: 1, CapabilityObservedAt: time.Now(), MissingCapabilities: []string{},
	}
	if _, err := client.Register(t.Context(), registration); err != nil {
		t.Fatal(err)
	}
	outbox, err := newEvidenceOutbox(t.TempDir(), registration.NodeID, 1<<20, systemClock{}, 8, time.Hour, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer outbox.Close()
	outbox.ociIntentGate = &ociIntentCompletionGate{observe: func(context.Context) (OCIIntentObservation, error) {
		return OCIIntentObservation{Enabled: true, Revision: 1}, nil
	}}

	// Seed one completion before startup and wait for its delivery. This proves
	// the initial outbox snapshot finished before the lease-loss completion is
	// inserted, reproducing the post-startup timing that used to strand it.
	sentinelDirectory := t.TempDir()
	if _, _, err := store.CreateJob(t.Context(), contract.JobSpec{
		SchemaVersion: contract.SchemaVersionV1, DispatchKey: "lease-loss-sentinel", Kind: contract.JobKindProcess,
		Class: contract.JobClassOneShot, RoutingTags: []string{"lease-loss"},
		Execution: contract.ExecutionSpec{Executable: contract.ExecutableSpec{Path: "/bin/true"}, Argv: []string{"true"},
			WorkingDirectory: sentinelDirectory, HandoffDirectory: sentinelDirectory},
	}); err != nil {
		t.Fatal(err)
	}
	sentinel, err := client.Claim(t.Context(), registration.NodeID, registration.BootSessionID, contract.JobClassOneShot)
	if err != nil || sentinel == nil {
		t.Fatalf("sentinel claim = %+v err=%v", sentinel, err)
	}
	if err := outbox.ensureAttempt(t.Context(), *sentinel); err != nil {
		t.Fatal(err)
	}
	exitCode := 0
	if err := outbox.storeCompletion(t.Context(), sentinel.Lease.AttemptID, l1.ProcessResult{ExitCode: &exitCode}, time.Now(), l1.RuntimeQuiescenceAttempt); err != nil {
		t.Fatal(err)
	}
	outbox.startRecovery(t.Context(), client, func(err error) { t.Errorf("recover durable evidence: %v", err) })
	waitCompletionReceiptState(t, outbox, sentinel.Lease.AttemptID, "delivered", 3*time.Second)

	digest := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	service, _, err := store.CreateJob(t.Context(), contract.JobSpec{
		SchemaVersion: contract.SchemaVersionV1, DispatchKey: "lease-loss-service", Kind: contract.JobKindOCI,
		Class: contract.JobClassService, Restart: contract.RestartAlways, RoutingTags: []string{"lease-loss"},
		RuntimeHandler: "io.containerd.runc.v2", Execution: contract.ExecutionSpec{OCI: &contract.OCIExecutionSpec{
			Image: contract.OCIImageSpec{Reference: "example.invalid/lease-loss:v1", Digest: &digest}, Argv: []string{"/payload"},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	first, err := client.Claim(t.Context(), registration.NodeID, registration.BootSessionID, contract.JobClassService)
	if err != nil || first == nil || first.Job.JobID != service.JobID {
		t.Fatalf("first service claim = %+v err=%v", first, err)
	}
	observe := func(claim *l1.Claim) {
		t.Helper()
		observation := l1.ImageObservationRequest{
			FencingToken: claim.Lease.FencingToken, SubmittedReference: "example.invalid/lease-loss:v1", TopLevelDigest: digest,
			TopLevelMediaType: "application/vnd.oci.image.manifest.v1+json", PlatformManifestDigest: digest,
			Platform: l1.OCIPlatform{OS: "linux", Architecture: "amd64"}, RuntimeHandler: "io.containerd.runc.v2", Snapshotter: "overlayfs",
		}
		if _, err := client.ObserveAttemptImage(t.Context(), claim.Job.JobID, claim.Lease.AttemptID, observation); err != nil {
			t.Fatal(err)
		}
		if _, err := client.StartAttempt(t.Context(), claim.Job.JobID, claim.Lease.AttemptID, l1.StartedRequest{FencingToken: claim.Lease.FencingToken}); err != nil {
			t.Fatal(err)
		}
	}
	observe(first)
	if err := outbox.ensureAttempt(t.Context(), *first); err != nil {
		t.Fatal(err)
	}
	completionFinishedAt := time.Now().UTC()
	result := l1.ProcessResult{RuntimeFailure: &contract.RuntimeFailure{
		Code: contract.RuntimeFailureUnavailable, Message: "helper generation lost under load",
	}}
	if err := outbox.storeCompletion(t.Context(), first.Lease.AttemptID, result, completionFinishedAt, l1.RuntimeQuiescenceOCISweep); err != nil {
		t.Fatal(err)
	}

	var lost l1.Attempt
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, _ = store.Reconcile(t.Context())
		attempts, listErr := store.ListJobAttempts(t.Context(), service.JobID)
		if listErr == nil && len(attempts) > 0 && attempts[0].State == contract.AttemptLost {
			lost = attempts[0]
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("first attempt did not lose its lease: attempts=%+v err=%v", attempts, listErr)
		}
		time.Sleep(10 * time.Millisecond)
	}

	var second *l1.Claim
	deadline = time.Now().Add(5 * time.Second)
	for second == nil && time.Now().Before(deadline) {
		second, err = client.Claim(t.Context(), registration.NodeID, registration.BootSessionID, contract.JobClassService)
		if err != nil {
			t.Fatal(err)
		}
		if second == nil {
			time.Sleep(10 * time.Millisecond)
		}
	}
	if second == nil || second.Lease.AttemptID == first.Lease.AttemptID {
		t.Fatalf("fresh service claim = %+v after lost attempt %s", second, first.Lease.AttemptID)
	}
	observe(second)
	freshRunning, err := store.GetJob(t.Context(), service.JobID)
	if err != nil || freshRunning.State != contract.JobRunning || freshRunning.CurrentAttemptID != second.Lease.AttemptID {
		t.Fatalf("fresh running service = %+v err=%v", freshRunning, err)
	}

	outbox.scheduleRecovery()
	var late l1.Attempt
	deadline = time.Now().Add(5 * time.Second)
	for {
		attempts, listErr := store.ListJobAttempts(t.Context(), service.JobID)
		if listErr == nil && len(attempts) >= 2 && attempts[0].LateResult != nil {
			late = attempts[0]
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("lost attempt did not receive typed evidence: attempts=%+v err=%v", attempts, listErr)
		}
		time.Sleep(time.Millisecond)
	}
	if late.LateResult.Kind != l1.LateResultObservation || !late.LateResult.Late || late.LateResult.Result == nil ||
		late.LateResult.Result.RuntimeFailure == nil || late.LateResult.Result.RuntimeFailure.Code != contract.RuntimeFailureUnavailable {
		t.Fatalf("lost-attempt evidence = %+v", late.LateResult)
	}
	freshElapsed := freshRunning.UpdatedAt.Sub(lost.UpdatedAt)
	evidenceElapsed := late.LateResult.ObservedAt.Sub(late.LateResult.AuthorityLostAt)
	const measuredRecoveryBound = 3 * time.Second
	if freshElapsed < 0 || freshElapsed > measuredRecoveryBound || evidenceElapsed < 0 || evidenceElapsed > measuredRecoveryBound {
		t.Fatalf("lease-loss timeline exceeded measured recovery bound %s: fresh=%s evidence=%s", measuredRecoveryBound, freshElapsed, evidenceElapsed)
	}
	waitCompletionReceiptState(t, outbox, first.Lease.AttemptID, "delivered", 3*time.Second)
	t.Logf("lease-loss receipt: completion_finished=%s lease_lost=%s fresh_running=%s evidence_observed=%s fresh_elapsed=%s evidence_elapsed=%s measured_bound=%s production_lease=%s",
		completionFinishedAt.Format(time.RFC3339Nano), lost.UpdatedAt.Format(time.RFC3339Nano), freshRunning.UpdatedAt.Format(time.RFC3339Nano),
		late.LateResult.ObservedAt.Format(time.RFC3339Nano), freshElapsed, evidenceElapsed, measuredRecoveryBound, l1.DefaultLeaseDuration)
}

func TestDurableOCIIntentStopSuppressesSpooledServiceCompletion(t *testing.T) {
	network := plain.NewNetwork()
	store, stopServer := startFailureServerWithPoliciesAndLease(t, network, nil, map[string]l1.NodePolicy{
		"intent-spool-node": {Tags: []string{"intent-spool"}, MaxOneshotSlots: 1, MaxServiceSlots: 1},
	}, 500*time.Millisecond)
	defer stopServer()
	client, err := newClient(network.NewFabric(fabric.Identity{
		NodeID: "intent-spool-agent", Tags: []string{l1.DefaultAgentPrincipalTag},
	}), "wefty://control-plane", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	registration := contract.NodeRegistration{
		NodeID: "intent-spool-node", BootSessionID: "intent-spool-boot", OS: "linux", Architecture: "amd64", AgentVersion: "test",
		Capabilities: map[string]bool{
			"kind:process": true, "kind:oci": true, "runtime_handler:io.containerd.runc.v2": true,
		},
		CapabilityRevision: 1, CapabilityObservedAt: time.Now(), MissingCapabilities: []string{},
	}
	if _, err := client.Register(t.Context(), registration); err != nil {
		t.Fatal(err)
	}
	digest := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	service, _, err := store.CreateJob(t.Context(), contract.JobSpec{
		SchemaVersion: contract.SchemaVersionV1, DispatchKey: "intent-spool-service",
		Kind: contract.JobKindOCI, Class: contract.JobClassService, Restart: contract.RestartAlways,
		RoutingTags: []string{"intent-spool"}, RuntimeHandler: "io.containerd.runc.v2",
		Execution: contract.ExecutionSpec{OCI: &contract.OCIExecutionSpec{
			Image: contract.OCIImageSpec{Reference: "example.invalid/intent-spool:v1", Digest: &digest}, Argv: []string{"/payload"},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := client.Claim(t.Context(), registration.NodeID, registration.BootSessionID, contract.JobClassService)
	if err != nil || claim == nil || claim.Job.JobID != service.JobID {
		t.Fatalf("intent-spool claim=%+v err=%v", claim, err)
	}
	start := func(claim *l1.Claim) {
		t.Helper()
		observation := l1.ImageObservationRequest{
			FencingToken: claim.Lease.FencingToken, SubmittedReference: "example.invalid/intent-spool:v1", TopLevelDigest: digest,
			TopLevelMediaType: "application/vnd.oci.image.manifest.v1+json", PlatformManifestDigest: digest,
			Platform: l1.OCIPlatform{OS: "linux", Architecture: "amd64"}, RuntimeHandler: "io.containerd.runc.v2", Snapshotter: "overlayfs",
		}
		if _, err := client.ObserveAttemptImage(t.Context(), claim.Job.JobID, claim.Lease.AttemptID, observation); err != nil {
			t.Fatal(err)
		}
		if _, err := client.StartAttempt(t.Context(), claim.Job.JobID, claim.Lease.AttemptID, l1.StartedRequest{FencingToken: claim.Lease.FencingToken}); err != nil {
			t.Fatal(err)
		}
	}
	start(claim)
	outbox, err := newEvidenceOutbox(t.TempDir(), registration.NodeID, 1<<20, systemClock{}, 8, time.Hour, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer outbox.Close()
	if err := outbox.ensureAttempt(t.Context(), *claim); err != nil {
		t.Fatal(err)
	}
	exitCode := 1
	if err := outbox.storeCompletion(t.Context(), claim.Lease.AttemptID, l1.ProcessResult{ExitCode: &exitCode}, time.Now(), l1.RuntimeQuiescenceAttempt); err != nil {
		t.Fatal(err)
	}
	intentPath := filepath.Join(t.TempDir(), "oci-intent.json")
	if _, err := lima.InitializeOCIIntent(intentPath, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := lima.SetOCIIntent(t.Context(), intentPath, 1, false, time.Now()); err != nil {
		t.Fatal(err)
	}
	intentSource := lima.FileIntentSource{Path: intentPath}
	outbox.ociIntentGate = &ociIntentCompletionGate{observe: func(ctx context.Context) (OCIIntentObservation, error) {
		intent, err := intentSource.ReadIntent(ctx)
		return OCIIntentObservation{Enabled: intent.Enabled, Revision: intent.Revision}, err
	}}
	outbox.startRecovery(t.Context(), client, func(err error) { t.Errorf("recover intent-stop evidence: %v", err) })

	var receipt completionInspectionReceipt
	deadline := time.Now().Add(3 * time.Second)
	for receipt.State != "suppressed" {
		receipt = outbox.spool.inspectCompletion(t.Context(), claim.Lease.AttemptID)
		if time.Now().After(deadline) {
			t.Fatalf("intent-spool completion disposition=%+v", receipt)
		}
		time.Sleep(time.Millisecond)
	}
	if receipt.Reason != "service_intent_stop" || receipt.IntentRevision != 2 ||
		receipt.Result.ExitCode == nil || *receipt.Result.ExitCode != 1 {
		t.Fatalf("suppressed late evidence was not retained and typed: %+v", receipt)
	}
	deadline = time.Now().Add(3 * time.Second)
	for {
		_, _ = store.Reconcile(t.Context())
		attempts, listErr := store.ListJobAttempts(t.Context(), service.JobID)
		lastLost := listErr == nil && len(attempts) > 0 && attempts[len(attempts)-1].State == contract.AttemptLost
		if lastLost {
			if len(attempts) != 1 || attempts[0].AttemptID != claim.Lease.AttemptID || attempts[0].Result != nil {
				t.Fatalf("intent-spool expiry evidence=%+v receipt=%+v err=%v", attempts, receipt, listErr)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("intent-spool attempt did not become lost: attempts=%+v receipt=%+v err=%v", attempts, receipt, listErr)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestOCIServiceAuthorityRecoveryPublishesLostAttemptLateEvidence(t *testing.T) {
	network := plain.NewNetwork()
	store, stopServer := startFailureServerWithPoliciesAndLease(t, network, nil, map[string]l1.NodePolicy{
		"authority-recovery-node": {Tags: []string{"authority-recovery"}, MaxOneshotSlots: 1, MaxServiceSlots: 1},
	}, 500*time.Millisecond)
	defer stopServer()
	digest := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	service, _, err := store.CreateJob(t.Context(), contract.JobSpec{
		SchemaVersion: contract.SchemaVersionV1, DispatchKey: "authority-recovery-service",
		Kind: contract.JobKindOCI, Class: contract.JobClassService, Restart: contract.RestartAlways,
		RoutingTags: []string{"authority-recovery"}, RuntimeHandler: "io.containerd.runc.v2",
		Execution: contract.ExecutionSpec{OCI: &contract.OCIExecutionSpec{
			Image: contract.OCIImageSpec{Reference: "example.invalid/authority-recovery:v1", Digest: &digest}, Argv: []string{"/payload"},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	runtime := newLiveIntentAuthorityRuntime()
	var authorityAvailable atomic.Bool
	authorityAvailable.Store(true)
	agentFabric := network.NewFabric(fabric.Identity{NodeID: "authority-recovery-agent", Tags: []string{l1.DefaultAgentPrincipalTag}})
	managedRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	nodeAgent, err := New(Config{
		Fabric: agentFabric, ControlPlaneAddress: "wefty://control-plane",
		NodeID: "authority-recovery-node", BootSessionID: "authority-recovery-boot", Version: "test",
		Capabilities: map[string]bool{"kind:process": true, "kind:oci": true, "runtime_handler:io.containerd.runc.v2": true},
		CapabilityProbe: capabilityProbeFunc(func(context.Context) (CapabilityProbeResult, error) {
			return CapabilityProbeResult{Capabilities: map[string]bool{"kind:oci": true, "runtime_handler:io.containerd.runc.v2": true}}, nil
		}),
		OCIIntent: func(context.Context) (OCIIntentObservation, error) {
			if !authorityAvailable.Load() {
				return OCIIntentObservation{}, errors.New("transient intent authority read failure")
			}
			return OCIIntentObservation{Enabled: true, Revision: 1}, nil
		},
		OCIBootBarrier: readyOCIBootBarrier{}, WorkloadRuntimes: map[string]WorkloadRuntime{contract.JobKindOCI: runtime},
		ManagedRootDirectory: managedRoot, LogSpoolDirectory: t.TempDir(), MaxServiceSlots: 1,
		HeartbeatInterval: 50 * time.Millisecond, ClaimInterval: 5 * time.Millisecond, RenewalInterval: 50 * time.Millisecond,
		LogRetryInterval: 20 * time.Millisecond, Logf: t.Logf,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer nodeAgent.Close()
	runContext, cancelRun := context.WithCancel(t.Context())
	defer cancelRun()
	runDone := make(chan error, 1)
	go func() { runDone <- nodeAgent.Run(runContext) }()
	var attemptID string
	select {
	case attemptID = <-runtime.started:
	case <-time.After(5 * time.Second):
		t.Fatal("OCI service did not start")
	}
	if _, err := store.SetNodeClaimsByOperator(t.Context(), "authority-recovery-node", "operator", l1.NodeIntentRequest{
		ClaimsEnabled: false, IntentRevision: 0, Reason: "hold replacement while observing late evidence",
	}); err != nil {
		t.Fatal(err)
	}
	authorityAvailable.Store(false)
	runtime.complete()
	waitCompletionReceiptState(t, nodeAgent.outbox, attemptID, "withheld", 3*time.Second)

	var lost l1.Attempt
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, _ = store.Reconcile(t.Context())
		attempts, listErr := store.ListJobAttempts(t.Context(), service.JobID)
		if listErr == nil && len(attempts) == 1 && attempts[0].State == contract.AttemptLost {
			lost = attempts[0]
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("withheld live completion did not become lost: attempts=%+v err=%v", attempts, listErr)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if lost.Result != nil || lost.LateResult != nil {
		t.Fatalf("authority-unavailable completion published before recovery: %+v", lost)
	}
	authorityAvailable.Store(true)
	nodeAgent.outbox.scheduleRecovery()
	deadline = time.Now().Add(5 * time.Second)
	for {
		attempts, listErr := store.ListJobAttempts(t.Context(), service.JobID)
		if listErr == nil && len(attempts) == 1 && attempts[0].LateResult != nil {
			late := attempts[0].LateResult
			if late.Kind != l1.LateResultObservation || !late.Late || late.Result == nil || late.Result.ExitCode == nil || *late.Result.ExitCode != 7 {
				t.Fatalf("recovered late result=%+v", late)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("retained completion did not publish after authority recovery: attempts=%+v err=%v", attempts, listErr)
		}
		time.Sleep(5 * time.Millisecond)
	}
	waitCompletionReceiptState(t, nodeAgent.outbox, attemptID, "delivered", 3*time.Second)
	cancelRun()
	var unavailable *OCIIntentAuthorityUnavailableError
	if runErr := <-runDone; runErr != nil && !errors.As(runErr, &unavailable) {
		t.Fatalf("agent shutdown after unavailable completion authority: %v", runErr)
	}
}

func waitCompletionReceiptState(t *testing.T, outbox *evidenceOutbox, attemptID, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		receipt := outbox.spool.inspectCompletion(t.Context(), attemptID)
		if receipt.State == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("completion receipt for %s = %+v, want %s", attemptID, receipt, want)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestOCIIntentStopLetsFinishedOneShotCompleteAndFinalizeHandoff(t *testing.T) {
	network := plain.NewNetwork()
	store, stopServer := startFailureServerWithPoliciesAndLease(t, network, nil, map[string]l1.NodePolicy{
		"intent-oneshot-node": {Tags: []string{"intent-oneshot"}, MaxOneshotSlots: 1, MaxServiceSlots: 1},
	}, 2*time.Second)
	defer stopServer()
	digest := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	job, _, err := store.CreateJob(t.Context(), contract.JobSpec{
		SchemaVersion: contract.SchemaVersionV1, DispatchKey: "intent-stop-oneshot",
		Kind: contract.JobKindOCI, Class: contract.JobClassOneShot,
		RoutingTags: []string{"intent-oneshot"}, RuntimeHandler: "io.containerd.runc.v2",
		Execution: contract.ExecutionSpec{OCI: &contract.OCIExecutionSpec{
			Image: contract.OCIImageSpec{Reference: "example.invalid/intent-stop:v1", Digest: &digest}, Argv: []string{"/payload"},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	runtime := newOneShotIntentRuntime()
	deadman := newRecordingDeadmanRenewer()
	agentFabric := network.NewFabric(fabric.Identity{NodeID: "intent-oneshot-agent", Tags: []string{l1.DefaultAgentPrincipalTag}})
	managedRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	nodeAgent, err := New(Config{
		Fabric: agentFabric, ControlPlaneAddress: "wefty://control-plane",
		NodeID: "intent-oneshot-node", BootSessionID: "intent-oneshot-boot", Version: "test",
		Capabilities: map[string]bool{"kind:process": true, "kind:oci": true, "runtime_handler:io.containerd.runc.v2": true},
		CapabilityProbe: capabilityProbeFunc(func(context.Context) (CapabilityProbeResult, error) {
			return CapabilityProbeResult{Capabilities: map[string]bool{"kind:oci": true, "runtime_handler:io.containerd.runc.v2": true}}, nil
		}),
		OCIIntent: func(context.Context) (OCIIntentObservation, error) {
			return OCIIntentObservation{Enabled: false, Revision: 2}, nil
		},
		OCIBootBarrier: readyOCIBootBarrier{}, WorkloadRuntimes: map[string]WorkloadRuntime{contract.JobKindOCI: runtime},
		ManagedRootDirectory: managedRoot, LogSpoolDirectory: t.TempDir(), MaxOneshotSlots: 1,
		HeartbeatInterval: 50 * time.Millisecond, ClaimInterval: 5 * time.Millisecond, RenewalInterval: 50 * time.Millisecond,
		AttemptDeadman: deadman, Logf: t.Logf,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer nodeAgent.Close()
	completionStored := make(chan struct{})
	var completionStoredOnce sync.Once
	nodeAgent.outbox.completionStored = func() { completionStoredOnce.Do(func() { close(completionStored) }) }
	runContext, cancelRun := context.WithCancel(t.Context())
	runDone := make(chan error, 1)
	go func() { runDone <- nodeAgent.Run(runContext) }()
	select {
	case <-runtime.started:
	case <-time.After(5 * time.Second):
		t.Fatal("intent-stop OCI one-shot did not start")
	}
	deadman.waitForRenewal(t)
	if err := nodeAgent.StopOCIRuntime(t.Context()); err != nil {
		cancelRun()
		t.Fatal(err)
	}
	select {
	case <-completionStored:
	case <-time.After(time.Second):
		t.Fatal("intent-stop one-shot never persisted its genuine completion")
	}
	select {
	case runErr := <-runDone:
		t.Fatalf("intent-stop one-shot agent exited before L1 completion: %v", runErr)
	default:
	}
	completed, err := waitForFailureJobState(store, job.JobID, contract.JobSucceeded, 5*time.Second)
	if err != nil || completed.State != contract.JobSucceeded || runtime.finalized.Load() != 1 {
		cancelRun()
		attempts, attemptsErr := store.ListJobAttempts(t.Context(), job.JobID)
		t.Fatalf("intent-stop one-shot completion=%+v attempts=%+v attempts_err=%v finalized=%d err=%v", completed, attempts, attemptsErr, runtime.finalized.Load(), err)
	}
	if calls, _, generation := deadman.snapshot(); calls < 1 || generation != readyOCIHelperGeneration() {
		t.Fatalf("intent-stop one-shot deadman renewals=%d generation=%+v, want at least one renewal for %+v", calls, generation, readyOCIHelperGeneration())
	}
	cancelRun()
	if err := <-runDone; err != nil {
		t.Fatalf("agent shutdown: %v", err)
	}
}

func TestReapRefusalCannotCompleteExitZeroAttemptSuccessfully(t *testing.T) {
	network := plain.NewNetwork()
	store, stopServer := startFailureServer(t, network, nil, map[string][]string{"node-1": {"linux"}})
	defer stopServer()
	directory := t.TempDir()
	job, _, err := store.CreateJob(context.Background(), contract.JobSpec{
		SchemaVersion: contract.SchemaVersionV1, DispatchKey: "reap-refusal",
		Kind: contract.JobKindProcess, Class: contract.JobClassOneShot, RoutingTags: []string{"linux"},
		Execution: contract.ExecutionSpec{
			Executable: contract.ExecutableSpec{Path: "/bin/true"}, Argv: []string{"true"},
			WorkingDirectory: directory, HandoffDirectory: directory,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	agentFabric := network.NewFabric(fabric.Identity{NodeID: "fabric-node", Tags: []string{l1.DefaultAgentPrincipalTag}})
	nodeAgent, err := New(Config{
		Fabric: agentFabric, ControlPlaneAddress: "wefty://control-plane",
		NodeID: "node-1", BootSessionID: "boot-1", Version: "test",
		Capabilities:      map[string]bool{"kind:process": true},
		WorkloadRuntimes:  map[string]WorkloadRuntime{contract.JobKindProcess: &reapRefusingRuntime{}},
		LogSpoolDirectory: t.TempDir(), ClaimInterval: 5 * time.Millisecond, HeartbeatInterval: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer nodeAgent.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- nodeAgent.Run(ctx) }()
	completed, err := waitForFailureJobState(store, job.JobID, contract.JobFailed, 5*time.Second)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	attempts, err := store.ListJobAttempts(context.Background(), completed.JobID)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	if len(attempts) != 1 || attempts[0].Result == nil || attempts[0].Result.OutputError == "" || attempts[0].Result.ExitCode != nil {
		cancel()
		t.Fatalf("reap-refused completion = %#v attempts=%#v, want failed attempt with only output_error", completed, attempts)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("agent shutdown: %v", err)
	}
}

// DELIBERATE CONTRACT INVERSION (#57 §8.1): `Run` remains pending after attempt-authority loss, the payload is stopped and reaped, and the agent claims a fresh job after rejoining; `Run` returns cleanly only after outer-context cancellation
// Restoring the old assertion reintroduces the cascade where one control-plane blip kills the daemon and the guardian then reaps every payload on the machine.
func TestPartitionedAgentRejoinsWithoutRepeatingExpiredAttempt(t *testing.T) {
	assertPartitionedAgentRejoinsWithoutRepeatingExpiredAttempt(t)
}

func assertPartitionedAgentRejoinsWithoutRepeatingExpiredAttempt(t *testing.T) {
	clock := newManualClock(time.Date(2026, 8, 9, 14, 0, 0, 0, time.UTC))
	network := plain.NewNetwork()
	store, stopServer := startFailureServer(t, network, clock, map[string][]string{
		"node-1": {"linux"}, "node-2": {"linux"},
	})
	defer stopServer()

	expiredJob := createAgentTestJob(t, store, "partitioned-agent-expired")
	runner := newResilienceRunner()
	agentFabric := network.NewFabric(fabric.Identity{NodeID: "fabric-node-1", Tags: []string{l1.DefaultAgentPrincipalTag}})
	nodeAgent, err := New(Config{
		Fabric: agentFabric, ControlPlaneAddress: "wefty://control-plane",
		NodeID: "node-1", BootSessionID: "boot-1", Version: "test", Clock: clock,
		Capabilities:      map[string]bool{"kind:process": true},
		HeartbeatInterval: 5 * time.Minute, ClaimInterval: time.Minute, RenewalInterval: 10 * time.Second,
		WorkloadRuntimes: testRuntimeSet(runner), LogSpoolDirectory: t.TempDir(), Logf: t.Logf,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer nodeAgent.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- nodeAgent.Run(ctx) }()
	expiredAttemptID := runner.waitStarted(t)
	freshJob := createAgentTestJob(t, store, "partitioned-agent-fresh")
	clock.waitForDeadline(t, clock.Now().Add(10*time.Second))

	clock.Advance(2 * time.Minute)
	if _, err := store.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	runner.waitCanceled(t, expiredAttemptID)
	select {
	case err := <-done:
		t.Fatalf("agent exited after attempt-authority loss: %v", err)
	default:
	}
	freshAttemptID := runner.waitStarted(t)
	if freshAttemptID == expiredAttemptID {
		t.Fatalf("fresh job reused expired attempt %q", freshAttemptID)
	}
	completed, err := waitForFailureJobState(store, freshJob.JobID, contract.JobSucceeded, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if completed.State != contract.JobSucceeded {
		t.Fatalf("fresh job state = %q, want succeeded", completed.State)
	}
	nodes, err := store.ListNodes(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 1 || nodes[0].AuthorityGeneration < 2 {
		t.Fatalf("node registration after recovery = %#v, want a successful rejoin generation", nodes)
	}
	if runner.startCount(expiredAttemptID) != 1 {
		t.Fatalf("expired attempt starts = %d, want exactly 1", runner.startCount(expiredAttemptID))
	}
	expired, err := store.GetJob(context.Background(), expiredJob.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if expired.State != contract.JobFailed {
		t.Fatalf("expired job state = %q, want failed", expired.State)
	}

	_, err = store.RegisterNode(context.Background(), fabric.Identity{NodeID: "fabric-node-2"}, contract.NodeRegistration{
		NodeID: "node-2", BootSessionID: "boot-2", OS: "linux", Architecture: "arm64", AgentVersion: "test",
		Capabilities: map[string]bool{"kind:process": true},
	}, l1.DefaultNodePolicy("linux"), true)
	if err != nil {
		t.Fatal(err)
	}
	secondClaim, err := store.ClaimJob(context.Background(), "fabric-node-2", "node-2", "boot-2", contract.JobClassOneShot)
	if err != nil {
		t.Fatal(err)
	}
	if secondClaim != nil {
		t.Fatalf("expired job was executed a second time: %#v", secondClaim)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("agent Run() after outer cancellation = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("agent did not return after outer cancellation")
	}
}

func TestAgentFillsGrantedClassSlotsConcurrently(t *testing.T) {
	assertAgentFillsGrantedClassSlotsConcurrently(t)
}

func assertAgentFillsGrantedClassSlotsConcurrently(t *testing.T) {
	t.Helper()
	network := plain.NewNetwork()
	store, stopServer := startFailureServer(t, network, nil, map[string][]string{"node-1": {"linux"}})
	defer stopServer()
	var oneShots []l1.Job
	for index := range l1.DefaultMaxOneshotSlots + 1 {
		oneShots = append(oneShots, createAgentTestJob(t, store, fmt.Sprintf("concurrent-one-shot-%d", index)))
	}
	var services []l1.Job
	for index := range l1.DefaultMaxServiceSlots + 1 {
		serviceDirectory := t.TempDir()
		service, _, err := store.CreateJob(context.Background(), contract.JobSpec{
			SchemaVersion: contract.SchemaVersionV1,
			DispatchKey:   fmt.Sprintf("concurrent-service-%d", index),
			Kind:          "process",
			Class:         contract.JobClassService,
			Restart:       contract.RestartAlways,
			RoutingTags:   []string{"linux"},
			Execution: contract.ExecutionSpec{
				Executable:       contract.ExecutableSpec{Path: "/bin/true"},
				Argv:             []string{"true"},
				WorkingDirectory: serviceDirectory,
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		services = append(services, service)
	}

	runner := newBlockingRunner()
	agentFabric := network.NewFabric(fabric.Identity{NodeID: "fabric-node-1", Tags: []string{l1.DefaultAgentPrincipalTag}})
	// #83 made a managed resource mandatory for service jobs, and the #64
	// guardrails refuse a symlink anywhere in the managed ancestry — macOS
	// TempDir lives under /var -> /private/var.
	managedRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	nodeAgent, err := New(Config{
		Fabric: agentFabric, ControlPlaneAddress: "wefty://control-plane",
		NodeID: "node-1", BootSessionID: "boot-1", Version: "test",
		Capabilities:      map[string]bool{"kind:process": true},
		HeartbeatInterval: time.Second, ClaimInterval: 10 * time.Millisecond, RenewalInterval: 100 * time.Millisecond,
		WorkloadRuntimes: testRuntimeSet(runner), LogSpoolDirectory: t.TempDir(), ManagedRootDirectory: managedRoot,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer nodeAgent.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- nodeAgent.Run(ctx) }()

	wantStarts := int32(l1.DefaultMaxOneshotSlots + l1.DefaultMaxServiceSlots)
	deadline := time.Now().Add(5 * time.Second)
	for runner.starts.Load() != wantStarts && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if runner.starts.Load() != wantStarts {
		cancel()
		t.Fatalf("concurrent runner starts = %d, want %d class-scoped slots filled", runner.starts.Load(), wantStarts)
	}
	status := nodeAgent.Status()
	if status.OneShot.Occupied != l1.DefaultMaxOneshotSlots || status.OneShot.Limit != l1.DefaultMaxOneshotSlots ||
		status.Services.Occupied != l1.DefaultMaxServiceSlots || status.Services.Limit != l1.DefaultMaxServiceSlots ||
		len(status.Attempts) != int(wantStarts) {
		cancel()
		t.Fatalf("class occupancy = one-shot %#v service %#v attempts=%d", status.OneShot, status.Services, len(status.Attempts))
	}
	time.Sleep(100 * time.Millisecond)
	if runner.starts.Load() != wantStarts {
		cancel()
		t.Fatalf("N+1th or M+1th payload started: starts=%d want=%d", runner.starts.Load(), wantStarts)
	}
	for class, job := range map[string]l1.Job{
		contract.JobClassOneShot: oneShots[len(oneShots)-1],
		contract.JobClassService: services[len(services)-1],
	} {
		queued, err := store.GetJob(context.Background(), job.JobID)
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		if queued.State != contract.JobQueued {
			cancel()
			t.Fatalf("%s N+1th job state = %q, want visibly queued", class, queued.State)
		}
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("agent Run() after cancellation = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("agent did not join both class loops after cancellation")
	}
}

func TestAgentRefillsSlotAfterSiblingFinalization(t *testing.T) {
	assertAgentRefillsSlotAfterSiblingFinalization(t)
}

func assertAgentRefillsSlotAfterSiblingFinalization(t *testing.T) {
	t.Helper()
	network := plain.NewNetwork()
	store, stopServer := startFailureServer(t, network, nil, map[string][]string{"node-1": {"linux"}})
	defer stopServer()
	jobs := []l1.Job{
		createAgentTestJob(t, store, "slot-refill-1"),
		createAgentTestJob(t, store, "slot-refill-2"),
		createAgentTestJob(t, store, "slot-refill-3"),
	}
	runner := newSlotRefillRunner()
	agentFabric := network.NewFabric(fabric.Identity{NodeID: "fabric-node", Tags: []string{l1.DefaultAgentPrincipalTag}})
	nodeAgent, err := New(Config{
		Fabric: agentFabric, ControlPlaneAddress: "wefty://control-plane",
		NodeID: "node-1", BootSessionID: "boot-1", Version: "test",
		Capabilities:      map[string]bool{"kind:process": true},
		HeartbeatInterval: time.Second, ClaimInterval: time.Millisecond, RenewalInterval: 100 * time.Millisecond,
		MaxOneshotSlots: 2, MaxServiceSlots: 1,
		WorkloadRuntimes: testRuntimeSet(runner), LogSpoolDirectory: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer nodeAgent.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- nodeAgent.Run(ctx) }()

	firstAttempt := runner.waitStarted(t)
	secondAttempt := runner.waitStarted(t)
	if firstAttempt == secondAttempt {
		cancel()
		t.Fatalf("two local slots started the same attempt %q", firstAttempt)
	}
	queued, err := store.GetJob(context.Background(), jobs[2].JobID)
	if err != nil || queued.State != contract.JobQueued {
		cancel()
		t.Fatalf("N+1th job before refill = state %q error %v, want queued", queued.State, err)
	}
	runner.release(firstAttempt)
	refilledAttempt := runner.waitStarted(t)
	if refilledAttempt == firstAttempt || refilledAttempt == secondAttempt {
		cancel()
		t.Fatalf("slot refill attempt = %q, want a fresh sibling", refilledAttempt)
	}
	completed, err := store.GetJob(context.Background(), jobs[0].JobID)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	if completed.State != contract.JobSucceeded {
		// Claim order is FIFO but the two initial worker goroutines may report
		// starts in either order, so accept either of the first two jobs as the
		// finalized sibling.
		completed, err = store.GetJob(context.Background(), jobs[1].JobID)
		if err != nil || completed.State != contract.JobSucceeded {
			cancel()
			t.Fatalf("released sibling finalization = state %q error %v", completed.State, err)
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("agent Run() after slot-refill cancellation = %v", err)
	}
}

func TestAgentExcludesARequeuedJobUntilLocalFinalizationReturns(t *testing.T) {
	assertAgentExcludesARequeuedJobUntilLocalFinalizationReturns(t)
}

func assertAgentExcludesARequeuedJobUntilLocalFinalizationReturns(t *testing.T) {
	t.Helper()
	network := plain.NewNetwork()
	store, stopServer := startFailureServer(t, network, nil, map[string][]string{"node-1": {"linux"}})
	defer stopServer()
	workingDirectory := t.TempDir()
	service, _, err := store.CreateJob(context.Background(), contract.JobSpec{
		SchemaVersion: contract.SchemaVersionV1, DispatchKey: "local-quiescence", Kind: "process",
		Class: contract.JobClassService, Restart: contract.RestartAlways, RoutingTags: []string{"linux"},
		Execution: contract.ExecutionSpec{
			Executable: contract.ExecutableSpec{Path: "/bin/true"}, Argv: []string{"true"},
			WorkingDirectory: workingDirectory,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	agentFabric := network.NewFabric(fabric.Identity{NodeID: "fabric-node", Tags: []string{l1.DefaultAgentPrincipalTag}})
	client, err := newClient(agentFabric, "wefty://control-plane", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	registration := contract.NodeRegistration{
		NodeID: "node-1", BootSessionID: "boot-1", OS: "linux", Architecture: "arm64", AgentVersion: "test",
		Capabilities: map[string]bool{"kind:process": true},
	}
	session := newAgentSession(
		client, registration, newCapabilityState(registration.Capabilities, nil, systemClock{}, 0),
		time.Second, 10*time.Millisecond, systemClock{}, newLifecycleObserver(systemClock{}), nil, 0, 2,
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	completionAccepted := make(chan struct{})
	releaseFinalization := make(chan struct{})
	secondAttempt := make(chan struct{})
	var executions atomic.Int32
	go func() {
		done <- session.run(ctx, func(attemptContext context.Context, claim l1.Claim, _ time.Time) (errorDestination, error) {
			ordinal := executions.Add(1)
			if ordinal == 1 {
				exitCode := 1
				_, completeErr := client.Complete(attemptContext, claim.Job.JobID, claim.Lease.AttemptID, l1.CompletionRequest{
					FencingToken: claim.Lease.FencingToken, IdempotencyKey: "local-quiescence-first",
					Result: l1.ProcessResult{ExitCode: &exitCode},
				})
				if completeErr != nil {
					return errorDestinationUnclassified, completeErr
				}
				close(completionAccepted)
				select {
				case <-releaseFinalization:
					return errorDestinationUnclassified, nil
				case <-attemptContext.Done():
					return errorDestinationUnclassified, nil
				}
			}
			close(secondAttempt)
			<-attemptContext.Done()
			return errorDestinationUnclassified, nil
		})
	}()
	select {
	case <-completionAccepted:
	case <-time.After(5 * time.Second):
		t.Fatal("first service completion was not accepted")
	}
	time.Sleep(1500 * time.Millisecond)
	if got := executions.Load(); got != 1 {
		t.Fatalf("locally finalizing service executions = %d, want exactly one", got)
	}
	queued, err := store.GetJob(context.Background(), service.JobID)
	if err != nil || queued.State != contract.JobQueued {
		t.Fatalf("requeued service during local finalization = state %q error %v", queued.State, err)
	}
	close(releaseFinalization)
	select {
	case <-secondAttempt:
	case <-time.After(5 * time.Second):
		t.Fatal("service was not reclaimable after local finalization returned")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("session after local-quiescence cancellation = %v", err)
	}
}

func TestFailedCompletionLeavesSiblingAndSessionRunning(t *testing.T) {
	assertFailedCompletionLeavesSiblingAndSessionRunning(t)
}

func assertFailedCompletionLeavesSiblingAndSessionRunning(t *testing.T) {
	t.Helper()
	network := plain.NewNetwork()
	store, stopServer := startFailureServerWithPolicies(t, network, nil, map[string]l1.NodePolicy{
		"node-1": {Tags: []string{"linux"}, MaxOneshotSlots: 2, MaxServiceSlots: 1},
	})
	defer stopServer()
	createAgentTestJob(t, store, "authority-loss-sibling-a")
	createAgentTestJob(t, store, "authority-loss-sibling-b")
	agentFabric := network.NewFabric(fabric.Identity{NodeID: "fabric-node", Tags: []string{l1.DefaultAgentPrincipalTag}})
	client, err := newClient(agentFabric, "wefty://control-plane", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	clock := systemClock{}
	registration := contract.NodeRegistration{
		NodeID: "node-1", BootSessionID: "boot-1", OS: "linux", Architecture: "arm64", AgentVersion: "test",
		Capabilities: map[string]bool{"kind:process": true},
	}
	session := newAgentSession(
		client,
		registration,
		newCapabilityState(registration.Capabilities, nil, clock, 0),
		time.Second,
		time.Millisecond,
		clock,
		newLifecycleObserver(clock),
		nil,
		2,
		1,
	)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	firstFailed := make(chan struct{})
	siblingStarted := make(chan struct{})
	var executions atomic.Int32
	go func() {
		done <- session.run(ctx, func(attemptContext context.Context, claim l1.Claim, _ time.Time) (errorDestination, error) {
			if executions.Add(1) == 1 {
				exitCode := 0
				_, completionErr := client.Complete(attemptContext, claim.Job.JobID, claim.Lease.AttemptID, l1.CompletionRequest{
					FencingToken: "deliberately-stale", IdempotencyKey: "sibling-scoped-completion-failure",
					Result: l1.ProcessResult{ExitCode: &exitCode},
				})
				if completionErr == nil {
					return errorDestinationUnclassified, errors.New("stale completion unexpectedly succeeded")
				}
				close(firstFailed)
				return classifyAgentProtocolError(completionErr).destination, completionErr
			}
			close(siblingStarted)
			<-attemptContext.Done()
			return errorDestinationUnclassified, nil
		})
	}()
	select {
	case <-firstFailed:
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("attempt completion did not fail")
	}
	select {
	case <-siblingStarted:
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("sibling attempt did not remain independently runnable")
	}
	select {
	case err := <-done:
		cancel()
		t.Fatalf("failed completion terminated the session: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("session after sibling-isolation cancellation = %v", err)
	}
}

func TestDrainJoinsEveryResidentOneShotAttempt(t *testing.T) {
	assertDrainJoinsEveryResidentOneShotAttempt(t)
}

func assertDrainJoinsEveryResidentOneShotAttempt(t *testing.T) {
	t.Helper()
	network := plain.NewNetwork()
	store, stopServer := startFailureServerWithPolicies(t, network, nil, map[string]l1.NodePolicy{
		"node-1": {Tags: []string{"linux"}, MaxOneshotSlots: 3, MaxServiceSlots: 1},
	})
	defer stopServer()
	for index := range 3 {
		createAgentTestJob(t, store, fmt.Sprintf("plural-drain-%d", index))
	}
	runner := newSlotRefillRunner()
	agentFabric := network.NewFabric(fabric.Identity{NodeID: "fabric-node", Tags: []string{l1.DefaultAgentPrincipalTag}})
	nodeAgent, err := New(Config{
		Fabric: agentFabric, ControlPlaneAddress: "wefty://control-plane",
		NodeID: "node-1", BootSessionID: "boot-1", Version: "test",
		Capabilities:      map[string]bool{"kind:process": true},
		HeartbeatInterval: 100 * time.Millisecond, ClaimInterval: time.Millisecond, RenewalInterval: 100 * time.Millisecond,
		MaxOneshotSlots: 3, MaxServiceSlots: 1,
		WorkloadRuntimes: testRuntimeSet(runner), LogSpoolDirectory: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer nodeAgent.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- nodeAgent.Run(ctx) }()
	attempts := []string{runner.waitStarted(t), runner.waitStarted(t), runner.waitStarted(t)}
	drainContext, stopDrain := context.WithTimeout(context.Background(), 5*time.Second)
	_, err = nodeAgent.Drain(drainContext)
	stopDrain()
	if err != nil {
		t.Fatal(err)
	}
	for index, attemptID := range attempts {
		runner.release(attemptID)
		if index < len(attempts)-1 {
			select {
			case err := <-done:
				t.Fatalf("drain returned after only %d/%d attempts finalized: %v", index+1, len(attempts), err)
			case <-time.After(100 * time.Millisecond):
			}
		}
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("plural drain returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("plural drain did not join every resident attempt")
	}
}

func TestSilentRenewalHangCannotOutliveLocalAuthority(t *testing.T) {
	assertSilentRenewalHangCannotOutliveLocalAuthority(t)
}

func assertSilentRenewalHangCannotOutliveLocalAuthority(t *testing.T) {
	network := plain.NewNetwork()
	serverFabric := network.NewFabric(fabric.Identity{NodeID: "control-plane"})
	store, err := l1.OpenStore(filepath.Join(t.TempDir(), "silent-renewal.sqlite"), l1.StoreOptions{LeaseDuration: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	l1Server, err := l1.NewServer(serverFabric, store, l1.ServerConfig{NodePolicies: map[string]l1.NodePolicy{
		"node-1": l1.DefaultNodePolicy("linux"),
	}})
	if err != nil {
		t.Fatal(err)
	}
	renewalAccepted := make(chan struct{}, 1)
	handler := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if strings.HasSuffix(request.URL.Path, "/lease") {
			select {
			case renewalAccepted <- struct{}{}:
			default:
			}
			<-request.Context().Done()
			return
		}
		l1Server.Handler().ServeHTTP(w, request)
	})
	listener, err := serverFabric.Listen("tcp", "wefty://control-plane")
	if err != nil {
		t.Fatal(err)
	}
	httpServer := &http.Server{Handler: handler}
	served := make(chan error, 1)
	go func() { served <- httpServer.Serve(listener) }()
	defer func() {
		_ = httpServer.Close()
		if err := <-served; err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("serve silent-renewal L1: %v", err)
		}
	}()

	job := createAgentTestJob(t, store, "silent-renewal")
	runner := newBlockingRunner()
	agentFabric := network.NewFabric(fabric.Identity{NodeID: "fabric-node-1", Tags: []string{l1.DefaultAgentPrincipalTag}})
	nodeAgent, err := New(Config{
		Fabric: agentFabric, ControlPlaneAddress: "wefty://control-plane",
		NodeID: "node-1", BootSessionID: "boot-1", Version: "test",
		Capabilities:      map[string]bool{"kind:process": true},
		HeartbeatInterval: 5 * time.Minute, ClaimInterval: time.Minute,
		RenewalInterval: 100 * time.Millisecond, OperationTimeout: 5 * time.Second,
		WorkloadRuntimes: testRuntimeSet(runner), LogSpoolDirectory: t.TempDir(), MaxOneshotSlots: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer nodeAgent.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- nodeAgent.Run(ctx) }()
	runner.waitStarted(t)
	runningAttempt := assertAttemptStatus(t, nodeAgent, AttemptRunning)
	if runningAttempt.JobID != job.JobID {
		cancel()
		t.Fatalf("running attempt job = %q, want %q", runningAttempt.JobID, job.JobID)
	}
	if occupancy := nodeAgent.Status().OneShot; occupancy.Occupied != 1 || occupancy.Limit != 1 {
		cancel()
		t.Fatalf("running one-shot occupancy = %#v, want 1/1", occupancy)
	}
	select {
	case <-renewalAccepted:
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("renewal RPC was not accepted")
	}

	deadline := time.Now().Add(5 * time.Second)
	for runner.starts.Load() == 1 && time.Now().Before(deadline) {
		select {
		case err := <-done:
			cancel()
			t.Fatalf("agent exited during silent renewal: %v", err)
		default:
		}
		status := nodeAgent.Status()
		if len(status.Attempts) == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(nodeAgent.Status().Attempts) != 0 {
		cancel()
		t.Fatal("silent renewal let the payload remain resident past local authority")
	}
	// The agent's local authority deadline is derived from the granted TTL
	// measured at its own request start, so it is deliberately EARLIER than the
	// server's lease expiry. The payload is therefore reaped before L1 will
	// agree the lease is gone, and a single Reconcile can legitimately observe
	// the attempt still claimed. Poll until the control plane catches up.
	var expired l1.Job
	reconcileDeadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := store.Reconcile(context.Background()); err != nil {
			cancel()
			t.Fatal(err)
		}
		expired, err = store.GetJob(context.Background(), job.JobID)
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		if expired.State == contract.JobFailed {
			break
		}
		if time.Now().After(reconcileDeadline) {
			cancel()
			t.Fatalf("silent-renewal job = state %q, want failed", expired.State)
		}
		time.Sleep(25 * time.Millisecond)
	}
	select {
	case err := <-done:
		cancel()
		t.Fatalf("agent exited after watchdog reaped payload: %v", err)
	default:
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("agent Run() after cancellation = %v", err)
	}
}

func TestAuthorityWatchdogCountsSuspendWallGap(t *testing.T) {
	assertAuthorityWatchdogCountsSuspendWallGap(t)
}

func assertAuthorityWatchdogCountsSuspendWallGap(t *testing.T) {
	clock := newManualClock(time.Date(2026, 8, 17, 4, 0, 0, 0, time.UTC))
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	attemptContext, cancelAttempt := context.WithCancelCause(ctx)
	watchdog := newAuthorityWatchdog(clock)
	watchdog.checkInterval = time.Second
	watch := watchdog.Start(attemptContext, localAuthority{deadline: clock.Now().Add(30 * time.Second)}, cancelAttempt)
	defer watch.Stop()
	clock.waitForDeadline(t, clock.Now().Add(time.Second))

	// The monotonic clock advances by only one second, as if it paused during
	// laptop suspend, while the wall clock crosses the whole remaining lease.
	clock.AdvanceWall(31 * time.Second)
	clock.Advance(time.Second)
	select {
	case err := <-watch.Failures():
		if !errors.Is(err, errAuthorityDeadlineExceeded) {
			t.Fatalf("watchdog failure = %v, want authority deadline", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("watchdog ignored suspend wall-clock gap")
	}
	if cause := context.Cause(attemptContext); !errors.Is(cause, errAuthorityDeadlineExceeded) {
		t.Fatalf("attempt cancellation cause = %v, want authority deadline", cause)
	}
}

func TestUnreapedPayloadStaysVisibleWithoutKillingDaemon(t *testing.T) {
	assertUnreapedPayloadStaysVisibleWithoutKillingDaemon(t)
}

func assertUnreapedPayloadStaysVisibleWithoutKillingDaemon(t *testing.T) {
	clock := newManualClock(time.Date(2026, 8, 17, 5, 0, 0, 0, time.UTC))
	network := plain.NewNetwork()
	store, stopServer := startFailureServer(t, network, clock, map[string][]string{"node-1": {"linux"}})
	defer stopServer()
	job := createAgentTestJob(t, store, "blocked-reap")
	runner := newStubbornRunner()
	agentFabric := network.NewFabric(fabric.Identity{NodeID: "fabric-node", Tags: []string{l1.DefaultAgentPrincipalTag}})
	nodeAgent, err := New(Config{
		Fabric: agentFabric, ControlPlaneAddress: "wefty://control-plane",
		NodeID: "node-1", BootSessionID: "boot-1", Version: "test", Clock: clock,
		Capabilities:      map[string]bool{"kind:process": true},
		HeartbeatInterval: 5 * time.Minute, ClaimInterval: time.Minute, RenewalInterval: 10 * time.Second,
		WorkloadRuntimes: testRuntimeSet(runner), LogSpoolDirectory: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer nodeAgent.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- nodeAgent.Run(ctx) }()
	runner.waitStarted(t)
	clock.waitForDeadline(t, clock.Now().Add(10*time.Second))
	clock.Advance(30 * time.Second)
	if _, err := store.Reconcile(context.Background()); err != nil {
		cancel()
		t.Fatal(err)
	}
	runner.waitCanceled(t)
	reaping := assertAttemptStatus(t, nodeAgent, AttemptReaping)
	if reaping.JobID != job.JobID || reaping.LastError == "" {
		cancel()
		t.Fatalf("blocked reap status = %#v", reaping)
	}
	select {
	case err := <-done:
		cancel()
		t.Fatalf("daemon exited while payload reap was blocked: %v", err)
	default:
	}
	runner.release()
	deadline := time.Now().Add(5 * time.Second)
	for len(nodeAgent.Status().Attempts) != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(nodeAgent.Status().Attempts) != 0 {
		cancel()
		t.Fatal("reaped attempt remained in local lifecycle status")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("agent Run() after cancellation = %v", err)
	}
}

func waitForFailureJobState(store *l1.Store, jobID string, want contract.JobState, timeout time.Duration) (l1.Job, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		job, err := store.GetJob(context.Background(), jobID)
		if err != nil {
			return l1.Job{}, err
		}
		if job.State == want {
			return job, nil
		}
		time.Sleep(time.Millisecond)
	}
	job, err := store.GetJob(context.Background(), jobID)
	if err != nil {
		return l1.Job{}, err
	}
	return job, fmt.Errorf("job %s state = %q, want %q", jobID, job.State, want)
}

func TestAgentDrainFinishesRunningAttemptAndStopsClaiming(t *testing.T) {
	network := plain.NewNetwork()
	store, stopServer := startFailureServer(t, network, nil, map[string][]string{"node-1": {"linux"}})
	defer stopServer()
	first := createAgentTestJob(t, store, "drain-first")
	second := createAgentTestJob(t, store, "drain-second")
	runner := newBlockingRunner()
	agentFabric := network.NewFabric(fabric.Identity{NodeID: "fabric-node", Tags: []string{l1.DefaultAgentPrincipalTag}})
	nodeAgent, err := New(Config{
		Fabric: agentFabric, ControlPlaneAddress: "wefty://control-plane",
		NodeID: "node-1", BootSessionID: "boot-1", Version: "test",
		Capabilities:      map[string]bool{"kind:process": true},
		HeartbeatInterval: time.Minute, ClaimInterval: time.Millisecond, RenewalInterval: 10 * time.Second,
		WorkloadRuntimes: testRuntimeSet(runner), LogSpoolDirectory: t.TempDir(), MaxOneshotSlots: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer nodeAgent.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- nodeAgent.Run(ctx) }()
	runner.waitStarted(t)

	drainContext, stopDrain := context.WithTimeout(context.Background(), 5*time.Second)
	node, err := nodeAgent.Drain(drainContext)
	stopDrain()
	if err != nil {
		t.Fatal(err)
	}
	if node.State != contract.NodeDraining {
		t.Fatalf("drain state = %q, want draining", node.State)
	}
	if status := nodeAgent.Status(); status.State != LifecycleDraining {
		t.Fatalf("agent lifecycle after drain = %q, want draining", status.State)
	}
	assertAttemptStatus(t, nodeAgent, AttemptRunning)
	select {
	case err := <-done:
		t.Fatalf("agent exited before running attempt finished: %v", err)
	default:
	}
	runner.release()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("agent Run() after drain = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("agent did not finish graceful drain")
	}
	if runner.starts.Load() != 1 {
		t.Fatalf("process starts = %d, want 1", runner.starts.Load())
	}
	firstState, err := store.GetJob(context.Background(), first.JobID)
	if err != nil {
		t.Fatal(err)
	}
	secondState, err := store.GetJob(context.Background(), second.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if firstState.State != contract.JobSucceeded || secondState.State != contract.JobQueued {
		t.Fatalf("drained job states = %q/%q, want succeeded/queued", firstState.State, secondState.State)
	}
}

func TestAgentShutdownFencesAttemptForImmediateSuccessor(t *testing.T) {
	network := plain.NewNetwork()
	store, stopServer := startFailureServer(t, network, nil, map[string][]string{"node-1": {"linux"}})
	defer stopServer()
	first := createAgentTestJob(t, store, "shutdown-fenced-first")
	second := createAgentTestJob(t, store, "shutdown-fenced-successor")
	runner := newBlockingRunner()
	agentFabric := network.NewFabric(fabric.Identity{NodeID: "fabric-node", Tags: []string{l1.DefaultAgentPrincipalTag}})
	firstAgent, err := New(Config{Fabric: agentFabric, ControlPlaneAddress: "wefty://control-plane", NodeID: "node-1", BootSessionID: "boot-1", Version: "test", Capabilities: map[string]bool{"kind:process": true}, WorkloadRuntimes: testRuntimeSet(runner), LogSpoolDirectory: t.TempDir(), MaxOneshotSlots: 1})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- firstAgent.Run(ctx) }()
	runner.waitStarted(t)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("first agent shutdown = %v", err)
	}
	firstAgent.Close()
	state, err := store.GetJob(context.Background(), first.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if state.State != contract.JobFailed || state.CurrentAttemptID == "" {
		t.Fatalf("shutdown completion state = %#v, want terminal attempt", state)
	}

	secondRunner := instantResultRunner{}
	secondAgent, err := New(Config{Fabric: agentFabric, ControlPlaneAddress: "wefty://control-plane", NodeID: "node-1", BootSessionID: "boot-2", Version: "test", Capabilities: map[string]bool{"kind:process": true}, WorkloadRuntimes: testRuntimeSet(secondRunner), LogSpoolDirectory: t.TempDir(), MaxOneshotSlots: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer secondAgent.Close()
	secondCtx, stopSecond := context.WithCancel(context.Background())
	defer stopSecond()
	secondDone := make(chan error, 1)
	go func() { secondDone <- secondAgent.Run(secondCtx) }()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if got, err := store.GetJob(context.Background(), second.JobID); err == nil && got.State == contract.JobSucceeded {
			break
		}
		time.Sleep(time.Millisecond)
	}
	stopSecond()
	if err := <-secondDone; err != nil {
		t.Fatalf("successor shutdown = %v", err)
	}
	if got, err := store.GetJob(context.Background(), second.JobID); err != nil || got.State != contract.JobSucceeded {
		t.Fatalf("successor job = %q, err=%v, want succeeded", got.State, err)
	}
}

func TestAgentShutdownFinalizationUploadsLogs(t *testing.T) {
	network := plain.NewNetwork()
	store, stopServer := startFailureServer(t, network, nil, map[string][]string{"node-1": {"linux"}})
	defer stopServer()
	job := createAgentTestJob(t, store, "shutdown-finalization-logs")
	runner := newLoggingBlockingRunner()
	agentFabric := network.NewFabric(fabric.Identity{NodeID: "fabric-node", Tags: []string{l1.DefaultAgentPrincipalTag}})
	nodeAgent, err := New(Config{Fabric: agentFabric, ControlPlaneAddress: "wefty://control-plane", NodeID: "node-1", BootSessionID: "boot-1", Version: "test", Capabilities: map[string]bool{"kind:process": true}, WorkloadRuntimes: testRuntimeSet(runner), LogSpoolDirectory: t.TempDir(), MaxOneshotSlots: 1})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- nodeAgent.Run(ctx) }()
	attemptID := runner.waitStarted(t)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("agent shutdown = %v", err)
	}
	nodeAgent.Close()
	page, err := store.GetJobLogs(context.Background(), job.JobID, "", l1.MaxLogPageLimit)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != 1 || string(page.Events[0].Bytes) != "shutdown evidence" || page.Events[0].AttemptID != attemptID {
		t.Fatalf("shutdown logs = %#v, want one finalized event for %s", page.Events, attemptID)
	}
}

func TestFinalizationTimeoutStartsAfterServicePayloadStops(t *testing.T) {
	assertFinalizationTimeoutStartsAfterServicePayloadStops(t)
}

func TestServiceCrashWithExpiredLogUploadStillRestarts(t *testing.T) {
	store, err := l1.OpenStore(filepath.Join(t.TempDir(), "restart-after-log-upload-timeout.sqlite"), l1.StoreOptions{
		Jitter: func(delay time.Duration) time.Duration { return delay },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	_, err = store.RegisterNode(t.Context(), fabric.Identity{NodeID: "fabric-log-gap-node"}, contract.NodeRegistration{
		NodeID: "log-gap-node", BootSessionID: "log-gap-boot", OS: "linux", Architecture: "amd64", AgentVersion: "test",
		Capabilities: map[string]bool{"kind:process": true},
	}, l1.DefaultNodePolicy("linux"), true)
	if err != nil {
		t.Fatal(err)
	}
	job, _, err := store.CreateJob(t.Context(), contract.JobSpec{
		SchemaVersion: contract.SchemaVersionV1,
		DispatchKey:   "restart-after-log-upload-timeout",
		Kind:          contract.JobKindProcess,
		Class:         contract.JobClassService,
		Restart:       contract.RestartAlways,
		RoutingTags:   []string{"linux"},
		Execution: contract.ExecutionSpec{
			Executable:       contract.ExecutableSpec{Path: "/bin/false"},
			Argv:             []string{"false"},
			WorkingDirectory: t.TempDir(),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimJob(t.Context(), "fabric-log-gap-node", "log-gap-node", "log-gap-boot", contract.JobClassService)
	if err != nil {
		t.Fatal(err)
	}
	if claim == nil || claim.Job.JobID != job.JobID {
		t.Fatalf("claim = %#v, want service %q", claim, job.JobID)
	}
	managedRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	managedResource, err := initializeManagedResource(managedRoot, "log-gap-node", "log-gap-boot")
	if err != nil {
		t.Fatal(err)
	}

	lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{
		runtimes: workloadRuntimeSet{contract.JobKindProcess: &restartableCrashRuntime{}},
		clock:    systemClock{}, nodeID: "log-gap-node", bootSessionID: "log-gap-boot",
		finalizationTimeout: 25 * time.Millisecond, managedResource: managedResource,
		logSinkFactory: func(context.Context, l1.Claim) (attemptLogSink, error) {
			return blockingFinalizationLogSink{}, nil
		},
	})
	result, runErr := lifecycle.runWorkload(t.Context(), *claim)
	if runErr != nil {
		t.Fatalf("restartable crash finalization = %v, want typed incomplete-log evidence", runErr)
	}
	if result.Signal != "killed" || result.TerminationCause != contract.TerminationCauseSpontaneous ||
		result.OutputError != "" || !result.LogEvidenceIncomplete {
		t.Fatalf("restartable crash result = %#v, want spontaneous signal plus log_evidence_incomplete", result)
	}

	restarted, err := store.CompleteAttempt(t.Context(), "fabric-log-gap-node", job.JobID, claim.Lease.AttemptID, l1.CompletionRequest{
		FencingToken: claim.Lease.FencingToken, IdempotencyKey: "completion:" + claim.Lease.AttemptID,
		Result: toL1Result(result), RuntimeQuiescenceEvidence: l1.RuntimeQuiescenceAttempt,
	})
	if err != nil {
		t.Fatal(err)
	}
	if restarted.State != contract.JobQueued || restarted.RestartStreak != 1 || restarted.NextRestartAt == nil ||
		!restarted.RestartPending(restarted.State, time.Now()) {
		t.Fatalf("service after crash with expired log upload = %#v, want restart-pending", restarted)
	}
	var failure l1.ProcessResult
	if err := json.Unmarshal(restarted.LastFailure, &failure); err != nil {
		t.Fatal(err)
	}
	if failure.Signal != "killed" || failure.OutputError != "" || !failure.LogEvidenceIncomplete {
		t.Fatalf("last_failure = %#v, want payload signal plus typed incomplete-log evidence", failure)
	}
}

func TestTypedNilLogSinkSetupFailureRemainsSpawnFailure(t *testing.T) {
	setupErr := errors.New("log spool unavailable")
	lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{
		runtimes: workloadRuntimeSet{contract.JobKindProcess: &restartableCrashRuntime{}},
		clock:    systemClock{},
		logSinkFactory: func(context.Context, l1.Claim) (attemptLogSink, error) {
			var sink *batchingLogSink
			return sink, setupErr
		},
	})
	result, err := lifecycle.runWorkload(t.Context(), l1.Claim{
		Job: l1.Job{JobID: "typed-nil-log-sink", Spec: contract.JobSpec{
			Kind: contract.JobKindProcess, Class: contract.JobClassOneShot,
		}},
		Lease: l1.AttemptLease{AttemptID: "typed-nil-log-sink-attempt"},
	})
	if !errors.Is(err, setupErr) {
		t.Fatalf("log sink setup error = %v, want %v", err, setupErr)
	}
	if result.SpawnError == nil || result.SpawnError.Code != contract.SpawnFailureLogSinkSetup {
		t.Fatalf("log sink setup result = %#v, want spawn failure", result)
	}
}

func TestOCIPreStartSpawnFailureIgnoresExpiredLogFinalization(t *testing.T) {
	store, err := l1.OpenStore(filepath.Join(t.TempDir(), "pre-start-log-finalization.sqlite"), l1.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	_, err = store.RegisterNode(t.Context(), fabric.Identity{NodeID: "fabric-pre-start-node"}, contract.NodeRegistration{
		NodeID: "pre-start-node", BootSessionID: "pre-start-boot", OS: "linux", Architecture: "amd64", AgentVersion: "test",
		Capabilities: map[string]bool{"kind:oci": true, "runtime_handler:io.containerd.runc.v2": true}, CapabilityRevision: 1,
		CapabilityObservedAt: time.Now(),
	}, l1.DefaultNodePolicy("linux"), true)
	if err != nil {
		t.Fatal(err)
	}
	job, _, err := store.CreateJob(t.Context(), contract.JobSpec{
		SchemaVersion: contract.SchemaVersionV1, DispatchKey: "pre-start-expired-log-finalization",
		Kind: contract.JobKindOCI, Class: contract.JobClassOneShot, RuntimeHandler: "io.containerd.runc.v2",
		Execution: contract.ExecutionSpec{OCI: &contract.OCIExecutionSpec{
			Image: contract.OCIImageSpec{Reference: "example.invalid/pre-start:v1"}, Argv: []string{"/payload"},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimJob(t.Context(), "fabric-pre-start-node", "pre-start-node", "pre-start-boot", contract.JobClassOneShot)
	if err != nil || claim == nil {
		stored, _ := store.GetJob(t.Context(), job.JobID)
		nodes, _ := store.ListNodes(t.Context())
		t.Fatalf("claim pre-start job = %#v err=%v job=%+v nodes=%+v", claim, err, stored, nodes)
	}
	lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{
		runtimes: workloadRuntimeSet{contract.JobKindOCI: &preStartSpawnFailureRuntime{}},
		clock:    systemClock{}, finalizationTimeout: 25 * time.Millisecond,
		logSinkFactory: func(context.Context, l1.Claim) (attemptLogSink, error) {
			return blockingFinalizationLogSink{}, nil
		},
	})
	result, runErr := lifecycle.runWorkload(t.Context(), *claim)
	if runErr != nil || result.SpawnError == nil || result.LogEvidenceIncomplete {
		t.Fatalf("pre-start result = %#v err=%v, want sole spawn_error", result, runErr)
	}
	completed, err := store.CompleteAttempt(t.Context(), "fabric-pre-start-node", job.JobID, claim.Lease.AttemptID, l1.CompletionRequest{
		FencingToken: claim.Lease.FencingToken, IdempotencyKey: "completion:" + claim.Lease.AttemptID,
		Result: toL1Result(result), RuntimeQuiescenceEvidence: l1.RuntimeQuiescenceAttempt,
	})
	if err != nil || completed.State != contract.JobQueued {
		t.Fatalf("pre-start completion = %#v err=%v, want accepted infrastructure retry", completed, err)
	}
}

func TestInnerLogUploadDeadlineRemainsTerminal(t *testing.T) {
	lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{
		runtimes: workloadRuntimeSet{contract.JobKindProcess: &restartableCrashRuntime{}},
		clock:    systemClock{}, finalizationTimeout: time.Second,
		logSinkFactory: func(context.Context, l1.Claim) (attemptLogSink, error) {
			return innerDeadlineLogSink{}, nil
		},
	})
	result, err := lifecycle.runWorkload(t.Context(), l1.Claim{
		Job: l1.Job{JobID: "inner-log-deadline", Spec: contract.JobSpec{
			Kind: contract.JobKindProcess, Class: contract.JobClassOneShot,
		}},
		Lease: l1.AttemptLease{AttemptID: "inner-log-deadline-attempt"},
	})
	if err == nil || result.OutputError == "" || result.Signal != "" || result.LogEvidenceIncomplete {
		t.Fatalf("inner log deadline result = %#v err=%v, want terminal output_error", result, err)
	}
}

func TestOneShotExitZeroSurvivesBoundedLogFinalizationDeadline(t *testing.T) {
	lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{
		runtimes: workloadRuntimeSet{contract.JobKindProcess: instantWorkloadRuntime{}},
		clock:    systemClock{}, finalizationTimeout: 25 * time.Millisecond,
		logSinkFactory: func(context.Context, l1.Claim) (attemptLogSink, error) {
			return blockingFinalizationLogSink{}, nil
		},
	})
	result, err := lifecycle.runWorkload(t.Context(), l1.Claim{
		Job: l1.Job{JobID: "one-shot-log-deadline", Spec: contract.JobSpec{
			Kind: contract.JobKindProcess, Class: contract.JobClassOneShot,
		}},
		Lease: l1.AttemptLease{AttemptID: "one-shot-log-deadline-attempt"},
	})
	if err != nil || result.ExitCode == nil || *result.ExitCode != 0 || result.OutputError != "" || !result.LogEvidenceIncomplete {
		t.Fatalf("one-shot bounded log finalization = %#v err=%v", result, err)
	}
}

func TestConcurrentLogDeadlineAndGenuineOutputFailureStayTerminal(t *testing.T) {
	sink := &compoundFinalizationLogSink{}
	lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{
		runtimes: workloadRuntimeSet{contract.JobKindProcess: instantWorkloadRuntime{}},
		clock:    systemClock{}, finalizationTimeout: 25 * time.Millisecond,
		logSinkFactory: func(context.Context, l1.Claim) (attemptLogSink, error) {
			return sink, nil
		},
	})
	result, err := lifecycle.runWorkload(t.Context(), l1.Claim{
		Job: l1.Job{JobID: "compound-log-finalization", Spec: contract.JobSpec{
			Kind: contract.JobKindProcess, Class: contract.JobClassOneShot,
			Execution: contract.ExecutionSpec{SensitiveEnv: map[string]string{"secret": "long-secret-value"}},
		}},
		Lease: l1.AttemptLease{AttemptID: "compound-log-finalization-attempt"},
	})
	if err == nil || result.OutputError == "" || result.ExitCode != nil || !result.LogEvidenceIncomplete {
		t.Fatalf("compound log finalization = %#v err=%v, want terminal output_error plus incomplete evidence", result, err)
	}
}

func TestConcurrentLogDeadlineAndGenuineOutputFailureStayTerminalThroughRecovery(t *testing.T) {
	store, err := l1.OpenStore(filepath.Join(t.TempDir(), "terminal-output-recovery.sqlite"), l1.StoreOptions{
		Jitter: func(delay time.Duration) time.Duration { return delay },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	identity := fabric.Identity{NodeID: "agent"}
	registration := contract.NodeRegistration{
		NodeID: "terminal-output-node", BootSessionID: "terminal-output-boot",
		OS: "linux", Architecture: "amd64", AgentVersion: "test",
		Capabilities: map[string]bool{"kind:process": true},
	}
	if _, err := store.RegisterNode(t.Context(), identity, registration, l1.DefaultNodePolicy("linux"), true); err != nil {
		t.Fatal(err)
	}
	job, _, err := store.CreateJob(t.Context(), contract.JobSpec{
		SchemaVersion: contract.SchemaVersionV1,
		DispatchKey:   "terminal-output-recovery",
		Kind:          contract.JobKindProcess,
		Class:         contract.JobClassService,
		Restart:       contract.RestartAlways,
		Execution: contract.ExecutionSpec{
			Executable:       contract.ExecutableSpec{Path: "/bin/true"},
			Argv:             []string{"true"},
			WorkingDirectory: t.TempDir(),
			SensitiveEnv:     map[string]string{"secret": "long-secret-value"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimJob(t.Context(), identity.NodeID, registration.NodeID, registration.BootSessionID, contract.JobClassService)
	if err != nil || claim == nil || claim.Job.JobID != job.JobID {
		t.Fatalf("claim terminal-output service = %#v err=%v", claim, err)
	}
	managedRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	managedResource, err := initializeManagedResource(managedRoot, registration.NodeID, registration.BootSessionID)
	if err != nil {
		t.Fatal(err)
	}
	handler := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if !strings.HasSuffix(request.URL.Path, "/complete") {
			http.NotFound(w, request)
			return
		}
		var completion l1.CompletionRequest
		if err := json.NewDecoder(request.Body).Decode(&completion); err != nil {
			t.Error(err)
			return
		}
		completed, completeErr := store.CompleteAttempt(request.Context(), identity.NodeID, job.JobID, claim.Lease.AttemptID, completion)
		if completeErr != nil {
			var protocolErr *l1.Error
			if !errors.As(completeErr, &protocolErr) {
				t.Errorf("complete terminal-output evidence: %v", completeErr)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{
				Code: protocolErr.Code, Message: protocolErr.Error(),
			}})
			return
		}
		_ = json.NewEncoder(w).Encode(completed)
	})
	client, stopServer := startEvidenceReplayServer(t, handler, time.Second)
	defer stopServer()
	defer client.Close()
	outbox, err := newEvidenceOutbox(t.TempDir(), registration.NodeID, 1024*1024, systemClock{}, 8, time.Hour, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer outbox.Close()
	outbox.startRecovery(t.Context(), client, func(err error) { t.Errorf("recover terminal output evidence: %v", err) })
	lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{
		client: client, outbox: outbox,
		runtimes:            workloadRuntimeSet{contract.JobKindProcess: instantWorkloadRuntime{}},
		clock:               systemClock{},
		renewalInterval:     10 * time.Second,
		completionRetry:     time.Millisecond,
		finalizationTimeout: 25 * time.Millisecond,
		managedResource:     managedResource,
		observer:            newLifecycleObserver(systemClock{}),
		logSinkFactory: func(context.Context, l1.Claim) (attemptLogSink, error) {
			return &compoundFinalizationLogSink{}, nil
		},
	})
	if _, err := lifecycle.execute(t.Context(), *claim, time.Now()); err != nil {
		t.Fatal(err)
	}
	waitCompletionReceiptState(t, outbox, claim.Lease.AttemptID, "delivered", 2*time.Second)
	failed, err := store.GetJob(t.Context(), job.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if failed.State != contract.JobFailed || failed.NextRestartAt != nil {
		t.Fatalf("terminal output failure after recovery = %#v, want failed without restart", failed)
	}
	var failure l1.ProcessResult
	if err := json.Unmarshal(failed.LastFailure, &failure); err != nil {
		t.Fatal(err)
	}
	if failure.OutputError == "" || !failure.LogEvidenceIncomplete || failure.ExitCode != nil || failure.Signal != "" {
		t.Fatalf("terminal output failure evidence = %#v raw=%s job=%#v", failure, failed.LastFailure, failed)
	}
	readmitted, err := store.ClaimJob(t.Context(), identity.NodeID, registration.NodeID, registration.BootSessionID, contract.JobClassService)
	if err != nil || readmitted != nil {
		t.Fatalf("terminal output failure readmission = %#v err=%v, want none", readmitted, err)
	}
}

type blockingFinalizationLogSink struct{}

func (blockingFinalizationLogSink) WriteOutput(context.Context, contract.LogEvent) error { return nil }

func (blockingFinalizationLogSink) CloseContext(ctx context.Context) error {
	<-ctx.Done()
	return &logFinalizationDeadlineError{stage: logFinalizationStageUpload, cause: context.Cause(ctx)}
}

type innerDeadlineLogSink struct{}

func (innerDeadlineLogSink) WriteOutput(context.Context, contract.LogEvent) error { return nil }

func (innerDeadlineLogSink) CloseContext(context.Context) error {
	return fmt.Errorf("log destination request: %w", context.DeadlineExceeded)
}

type compoundFinalizationLogSink struct{}

func (*compoundFinalizationLogSink) WriteOutput(ctx context.Context, _ contract.LogEvent) error {
	<-ctx.Done()
	return context.Cause(ctx)
}

func (*compoundFinalizationLogSink) CloseContext(context.Context) error {
	return errors.New("durable uploader corruption")
}

type restartableCrashRuntime struct{}

func (*restartableCrashRuntime) Preflight(_ context.Context, request workloadrunner.Request) (workloadrunner.Admission, workloadrunner.Result, error) {
	return workloadrunner.Admission{Request: request, Release: func() {}}, workloadrunner.Result{}, nil
}

func (*restartableCrashRuntime) Run(ctx context.Context, request workloadrunner.Request, sink workloadrunner.OutputSink) (workloadrunner.Result, error) {
	if err := sink.WriteOutput(ctx, contract.LogEvent{
		AttemptID: request.Authority.AttemptID, Stream: contract.LogStdout, Sequence: 0, Bytes: []byte("before crash"),
	}); err != nil {
		return workloadrunner.Result{}, err
	}
	return workloadrunner.Result{Outcome: contract.ProcessResult{
		Signal: "killed", TerminationCause: contract.TerminationCauseSpontaneous,
	}}, nil
}

func (*restartableCrashRuntime) ReapAndVerify(context.Context, workloadrunner.ReapRequest) (workloadrunner.ReapReceipt, error) {
	return workloadrunner.ReapReceipt{RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidenceAttempt}, nil
}

type preStartSpawnFailureRuntime struct{}

func (*preStartSpawnFailureRuntime) Preflight(_ context.Context, request workloadrunner.Request) (workloadrunner.Admission, workloadrunner.Result, error) {
	return workloadrunner.Admission{Request: request, Release: func() {}}, workloadrunner.Result{}, nil
}

func (*preStartSpawnFailureRuntime) Run(context.Context, workloadrunner.Request, workloadrunner.OutputSink) (workloadrunner.Result, error) {
	return workloadrunner.Result{Outcome: spawnFailure(contract.SpawnFailureRuntimeUnavailable, errors.New("runtime unavailable before start"))}, nil
}

func (*preStartSpawnFailureRuntime) ReapAndVerify(context.Context, workloadrunner.ReapRequest) (workloadrunner.ReapReceipt, error) {
	return workloadrunner.ReapReceipt{RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidenceAttempt}, nil
}

type instantWorkloadRuntime struct{}

func (instantWorkloadRuntime) Preflight(_ context.Context, request workloadrunner.Request) (workloadrunner.Admission, workloadrunner.Result, error) {
	return workloadrunner.Admission{Request: request, Release: func() {}}, workloadrunner.Result{}, nil
}

func (instantWorkloadRuntime) Run(ctx context.Context, request workloadrunner.Request, sink workloadrunner.OutputSink) (workloadrunner.Result, error) {
	if err := sink.WriteOutput(ctx, contract.LogEvent{
		AttemptID: request.Authority.AttemptID, Stream: contract.LogStdout, Bytes: []byte("tail"),
	}); err != nil {
		return workloadrunner.Result{}, err
	}
	exitCode := 0
	return workloadrunner.Result{Outcome: contract.ProcessResult{ExitCode: &exitCode}}, nil
}

func (instantWorkloadRuntime) ReapAndVerify(context.Context, workloadrunner.ReapRequest) (workloadrunner.ReapReceipt, error) {
	return workloadrunner.ReapReceipt{RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidenceAttempt}, nil
}

func assertFinalizationTimeoutStartsAfterServicePayloadStops(t *testing.T) {
	t.Helper()
	network := plain.NewNetwork()
	store, stopServer := startFailureServer(t, network, nil, map[string][]string{"node-1": {"linux"}})
	defer stopServer()
	workingDirectory := t.TempDir()
	service, _, err := store.CreateJob(context.Background(), contract.JobSpec{
		SchemaVersion: contract.SchemaVersionV1,
		DispatchKey:   "finalization-anchor-service",
		Kind:          "process",
		Class:         contract.JobClassService,
		Restart:       contract.RestartAlways,
		RoutingTags:   []string{"linux"},
		Execution: contract.ExecutionSpec{
			Executable:       contract.ExecutableSpec{Path: "/bin/true"},
			Argv:             []string{"true"},
			WorkingDirectory: workingDirectory,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	managedRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	// Race instrumentation can consume tens of milliseconds inside the real
	// SQLite/Fabric finalization path; retain the same compressed-time shape with
	// enough margin to test the anchor instead of scheduler latency.
	finalizationTimeout := 2 * time.Second
	runner := newFinalizationAnchorRunner()
	agentFabric := network.NewFabric(fabric.Identity{NodeID: "fabric-node", Tags: []string{l1.DefaultAgentPrincipalTag}})
	nodeAgent, err := New(Config{
		Fabric: agentFabric, ControlPlaneAddress: "wefty://control-plane",
		NodeID: "node-1", BootSessionID: "boot-1", Version: "test",
		Capabilities:  map[string]bool{"kind:process": true},
		ClaimInterval: 5 * time.Millisecond, RenewalInterval: time.Second,
		FinalizationTimeout: finalizationTimeout, LogBatchSize: 8, LogFlushInterval: time.Hour,
		WorkloadRuntimes: testRuntimeSet(runner), ManagedRootDirectory: managedRoot, LogSpoolDirectory: t.TempDir(), MaxServiceSlots: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer nodeAgent.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- nodeAgent.Run(ctx) }()
	attemptID := runner.waitStarted(t)

	// This is the production failure shape in compressed time: payload uptime
	// exceeds the whole finalization budget before the payload crashes.
	time.Sleep(3 * finalizationTimeout)
	// Keep the restart-pending state observable under race instrumentation. A
	// draining node renews its resident service but cannot reclaim it after the
	// crash and race past the queued assertion.
	if _, err := nodeAgent.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	runner.kill()
	requeued, err := waitForFailureJobState(store, service.JobID, contract.JobQueued, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var failure l1.ProcessResult
	if err := json.Unmarshal(requeued.LastFailure, &failure); err != nil {
		t.Fatalf("decode last_failure: %v", err)
	}
	if failure.OutputError != "" || failure.Signal != "killed" ||
		requeued.RestartStreak != 1 || requeued.NextRestartAt == nil || !requeued.RestartPending(requeued.State, time.Now()) {
		t.Fatalf("service after crash = %#v, want restart-pending without output_error", requeued)
	}
	page, err := store.GetJobLogs(context.Background(), service.JobID, "", l1.MaxLogPageLimit)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != 1 || page.Events[0].AttemptID != attemptID || string(page.Events[0].Bytes) != "final service event" {
		t.Fatalf("finalized service logs = %#v, want final event for %s", page.Events, attemptID)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("agent shutdown = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("agent did not stop after finalization-anchor assertion")
	}
}

func assertPluralServiceDrainJoinsAll(t *testing.T) {
	t.Helper()
	network := plain.NewNetwork()
	store, stopServer := startFailureServerWithPolicies(t, network, nil, map[string]l1.NodePolicy{"node-1": {Tags: []string{"linux"}, MaxOneshotSlots: 2, MaxServiceSlots: 2}})
	defer stopServer()
	for i := 0; i < 2; i++ {
		createAgentTestJob(t, store, fmt.Sprintf("plural-drain-one-shot-%d", i))
	}
	for i := 0; i < 2; i++ {
		if _, _, err := store.CreateJob(context.Background(), contract.JobSpec{SchemaVersion: contract.SchemaVersionV1, DispatchKey: fmt.Sprintf("plural-drain-service-%d", i), Kind: "process", Class: contract.JobClassService, Restart: contract.RestartAlways, RoutingTags: []string{"linux"}, Execution: contract.ExecutionSpec{Executable: contract.ExecutableSpec{Path: "/bin/true"}, Argv: []string{"true"}, WorkingDirectory: t.TempDir()}}); err != nil {
			t.Fatal(err)
		}
	}
	managedRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	runner := newSlotRefillRunner()
	agentFabric := network.NewFabric(fabric.Identity{NodeID: "fabric-node", Tags: []string{l1.DefaultAgentPrincipalTag}})
	nodeAgent, err := New(Config{Fabric: agentFabric, ControlPlaneAddress: "wefty://control-plane", NodeID: "node-1", BootSessionID: "boot-1", Version: "test", Capabilities: map[string]bool{"kind:process": true}, WorkloadRuntimes: testRuntimeSet(runner), ManagedRootDirectory: managedRoot, LogSpoolDirectory: t.TempDir(), ClaimInterval: 5 * time.Millisecond, HeartbeatInterval: 20 * time.Millisecond, RenewalInterval: 50 * time.Millisecond, MaxOneshotSlots: 2, MaxServiceSlots: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer nodeAgent.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- nodeAgent.Run(ctx) }()
	attempts := make([]string, 0, 4)
	for len(attempts) < 4 {
		attempts = append(attempts, runner.waitStarted(t))
	}
	drainCtx, stopDrain := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopDrain()
	if _, err := nodeAgent.Drain(drainCtx); err != nil {
		t.Fatal(err)
	}
	for _, attemptID := range attempts {
		runner.release(attemptID)
	}
	if err := <-done; err != nil {
		t.Fatalf("plural service drain = %v", err)
	}
	if got := len(nodeAgent.Status().Attempts); got != 0 {
		t.Fatalf("resident attempts after plural drain = %d", got)
	}
}

func TestAgentConsumesRenewalDirectivesWithoutDaemonExit(t *testing.T) {
	network := plain.NewNetwork()
	store, stopServer := startFailureServer(t, network, nil, map[string][]string{"node-1": {"linux"}})
	defer stopServer()
	managedRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	service, _, err := store.CreateJob(context.Background(), contract.JobSpec{SchemaVersion: contract.SchemaVersionV1, DispatchKey: "directive-service", Kind: "process", Class: contract.JobClassService, Restart: contract.RestartAlways, RoutingTags: []string{"linux"}, Execution: contract.ExecutionSpec{Executable: contract.ExecutableSpec{Path: "/bin/true"}, Argv: []string{"true"}, WorkingDirectory: t.TempDir()}})
	if err != nil {
		t.Fatal(err)
	}
	runner := newDirectiveRunner()
	agentFabric := network.NewFabric(fabric.Identity{NodeID: "fabric-node", Tags: []string{l1.DefaultAgentPrincipalTag}})
	nodeAgent, err := New(Config{Fabric: agentFabric, ControlPlaneAddress: "wefty://control-plane", NodeID: "node-1", BootSessionID: "boot-1", Version: "test", Capabilities: map[string]bool{"kind:process": true}, WorkloadRuntimes: testRuntimeSet(runner), ManagedRootDirectory: managedRoot, LogSpoolDirectory: t.TempDir(), RenewalInterval: 20 * time.Millisecond, HeartbeatInterval: 20 * time.Millisecond, MaxServiceSlots: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer nodeAgent.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- nodeAgent.Run(ctx) }()
	runner.waitStarted(t)
	if _, _, err := store.RestartService(context.Background(), service.JobID, l1.ServiceRestartRequest{IdempotencyKey: "directive-restart"}); err != nil {
		t.Fatal(err)
	}
	runner.waitCanceled(t)
	// The restart directive must be consumed by this fresh attempt; its
	// completion returns the service to queued/restart-pending without
	// terminating the daemon.
	if _, err := waitForFailureJobState(store, service.JobID, contract.JobQueued, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if status := nodeAgent.Status(); status.State == LifecycleQuarantined {
		t.Fatal("restart directive quarantined daemon")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("directive daemon exit = %v", err)
	}
}

func TestAgentHeartbeatClaimsDisabledStopsNewClaims(t *testing.T) {
	network := plain.NewNetwork()
	store, stopServer := startFailureServer(t, network, nil, map[string][]string{"node-1": {"linux"}})
	defer stopServer()
	first := createAgentTestJob(t, store, "claims-disabled-running")
	second := createAgentTestJob(t, store, "claims-disabled-queued")
	runner := newBlockingRunner()
	agentFabric := network.NewFabric(fabric.Identity{NodeID: "fabric-node", Tags: []string{l1.DefaultAgentPrincipalTag}})
	nodeAgent, err := New(Config{Fabric: agentFabric, ControlPlaneAddress: "wefty://control-plane", NodeID: "node-1", BootSessionID: "boot-1", Version: "test", Capabilities: map[string]bool{"kind:process": true}, WorkloadRuntimes: testRuntimeSet(runner), LogSpoolDirectory: t.TempDir(), HeartbeatInterval: 20 * time.Millisecond, ClaimInterval: 5 * time.Millisecond, MaxOneshotSlots: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer nodeAgent.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- nodeAgent.Run(ctx) }()
	runner.waitStarted(t)
	if _, err := store.SetNodeClaimsByOperator(context.Background(), "node-1", "operator", l1.NodeIntentRequest{ClaimsEnabled: false, IntentRevision: 0, Reason: "maintenance"}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if runner.starts.Load() != 1 {
		t.Fatalf("claims-disabled starts = %d, want one resident attempt", runner.starts.Load())
	}
	queued, err := store.GetJob(context.Background(), second.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if queued.State != contract.JobQueued {
		t.Fatalf("claims-disabled queued job state = %q", queued.State)
	}
	runner.release()
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("claims-disabled daemon exit = %v", err)
	}
	_ = first
}

func startFailureServer(t *testing.T, network *plain.Network, clock l1.Clock, nodeTags map[string][]string) (*l1.Store, func()) {
	t.Helper()
	policies := make(map[string]l1.NodePolicy, len(nodeTags))
	for nodeID, tags := range nodeTags {
		policies[nodeID] = l1.DefaultNodePolicy(tags...)
	}
	return startFailureServerWithPolicies(t, network, clock, policies)
}

func startFailureServerWithPolicies(t *testing.T, network *plain.Network, clock l1.Clock, policies map[string]l1.NodePolicy) (*l1.Store, func()) {
	return startFailureServerWithPoliciesAndLease(t, network, clock, policies, 30*time.Second)
}

func startFailureServerWithPoliciesAndLease(t *testing.T, network *plain.Network, clock l1.Clock, policies map[string]l1.NodePolicy, leaseDuration time.Duration) (*l1.Store, func()) {
	t.Helper()
	serverFabric := network.NewFabric(fabric.Identity{NodeID: "control-plane"})
	options := l1.StoreOptions{LeaseDuration: leaseDuration}
	if clock != nil {
		options.Clock = clock
	}
	store, err := l1.OpenStore(filepath.Join(t.TempDir(), "failure.sqlite"), options)
	if err != nil {
		t.Fatal(err)
	}
	server, err := l1.NewServer(serverFabric, store, l1.ServerConfig{NodePolicies: policies})
	if err != nil {
		store.Close()
		t.Fatal(err)
	}
	listener, err := serverFabric.Listen("tcp", "wefty://control-plane")
	if err != nil {
		store.Close()
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx, listener) }()
	return store, func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("serve L1: %v", err)
		}
		if err := store.Close(); err != nil {
			t.Errorf("close L1: %v", err)
		}
	}
}

func createAgentTestJob(t *testing.T, store *l1.Store, dispatchKey string) l1.Job {
	t.Helper()
	directory := t.TempDir()
	job, _, err := store.CreateJob(context.Background(), contract.JobSpec{
		SchemaVersion: contract.SchemaVersionV1, DispatchKey: dispatchKey, Kind: "process", Class: contract.JobClassOneShot, RoutingTags: []string{"linux"},
		Execution: contract.ExecutionSpec{
			Executable: contract.ExecutableSpec{Path: "/bin/true"}, Argv: []string{"true"},
			WorkingDirectory: directory, HandoffDirectory: directory,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return job
}

type blockingRunner struct {
	started     chan struct{}
	releaseOnce sync.Once
	releaseCh   chan struct{}
	starts      atomic.Int32
}

type slotRefillRunner struct {
	mu       sync.Mutex
	releases map[string]chan struct{}
	started  chan string
}

func newSlotRefillRunner() *slotRefillRunner {
	return &slotRefillRunner{releases: make(map[string]chan struct{}), started: make(chan string, 8)}
}

func (runner *slotRefillRunner) Run(ctx context.Context, request processrunner.Request, _ processrunner.OutputSink) (contract.ProcessResult, error) {
	if request.Started != nil {
		request.Started()
	}
	release := make(chan struct{})
	runner.mu.Lock()
	runner.releases[request.AttemptID] = release
	runner.mu.Unlock()
	runner.started <- request.AttemptID
	select {
	case <-ctx.Done():
		return contract.ProcessResult{Signal: "canceled", TerminationCause: contract.TerminationCauseAgent}, ctx.Err()
	case <-release:
		exitCode := 0
		return contract.ProcessResult{ExitCode: &exitCode}, nil
	}
}

func (runner *slotRefillRunner) waitStarted(t *testing.T) string {
	t.Helper()
	select {
	case attemptID := <-runner.started:
		return attemptID
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for slot-refill attempt")
		return ""
	}
}

func (runner *slotRefillRunner) release(attemptID string) {
	runner.mu.Lock()
	release := runner.releases[attemptID]
	delete(runner.releases, attemptID)
	runner.mu.Unlock()
	close(release)
}

type resilienceRunner struct {
	mu       sync.Mutex
	starts   map[string]int
	started  chan string
	canceled chan string
}

type loggingBlockingRunner struct {
	started  chan string
	canceled chan struct{}
}

type finalizationAnchorRunner struct {
	started chan string
	killed  chan struct{}
	once    sync.Once
}

type intentStopRuntime struct {
	started  chan string
	canceled chan string
	releaseC chan struct{}
	once     sync.Once
	starts   atomic.Int32
}

type outcomeFirstIntentRuntime struct {
	*intentStopRuntime
	finish chan struct{}
}

type oneShotIntentRuntime struct {
	*intentStopRuntime
	finalized atomic.Int32
}

type intentMarkerRaceRuntime struct {
	*intentStopRuntime
	firstFinished chan struct{}
	startFailed   chan error
	finishOnce    sync.Once
}

type liveIntentAuthorityRuntime struct {
	*intentStopRuntime
	finish chan struct{}
}

type preStartedRuntimeLossRuntime struct {
	*intentStopRuntime
	barrier   *stageLossBarrier
	entered   chan struct{}
	release   chan struct{}
	attemptID string
}

type diagnosticPreStartedRuntimeLossRuntime struct {
	*intentStopRuntime
	generation workloadrunner.RuntimeGeneration
}

func (runtime *diagnosticPreStartedRuntimeLossRuntime) Run(context.Context, workloadrunner.Request, workloadrunner.OutputSink) (workloadrunner.Result, error) {
	err := &workloadrunner.RuntimeLossError{
		Generation: runtime.generation,
		Err:        errors.New("helper lost during Run before Started"),
	}
	return workloadrunner.Result{Outcome: spawnFailure(contract.SpawnFailureRuntimeUnavailable, err)}, err
}

func (runtime *diagnosticPreStartedRuntimeLossRuntime) ReapAndVerify(context.Context, workloadrunner.ReapRequest) (workloadrunner.ReapReceipt, error) {
	return workloadrunner.ReapReceipt{}, &workloadrunner.RuntimeLossError{
		Generation: runtime.generation,
		Err:        errors.New("helper unavailable while reaping pre-Started attempt"),
	}
}

func newPreStartedRuntimeLossRuntime(barrier *stageLossBarrier) *preStartedRuntimeLossRuntime {
	return &preStartedRuntimeLossRuntime{
		intentStopRuntime: newIntentStopRuntime(), barrier: barrier,
		entered: make(chan struct{}), release: make(chan struct{}),
	}
}

func (runtime *preStartedRuntimeLossRuntime) Run(ctx context.Context, request workloadrunner.Request, _ workloadrunner.OutputSink) (workloadrunner.Result, error) {
	digest := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	observation := workloadrunner.OCIImageObservation{
		SubmittedReference: request.Execution.OCI.Image.Reference,
		TopLevelDigest:     digest, TopLevelMediaType: "application/vnd.oci.image.manifest.v1+json",
		PlatformManifestDigest: digest, PlatformOS: "linux", PlatformArchitecture: "amd64",
		RuntimeHandler: "io.containerd.runc.v2", Snapshotter: "overlayfs",
	}
	if err := request.OCIImageResolved(ctx, observation); err != nil {
		return workloadrunner.Result{}, err
	}
	runtime.attemptID = request.Authority.AttemptID
	close(runtime.entered)
	<-runtime.release
	runtime.barrier.lose(errors.New("helper lost during Run before Started"))
	if request.OCIRuntimeUnavailable != nil {
		request.OCIRuntimeUnavailable(workloadrunner.RuntimeGeneration{InstanceID: "stage-loss", Generation: 1})
	}
	err := &workloadrunner.RuntimeLossError{
		Generation: workloadrunner.RuntimeGeneration{InstanceID: "stage-loss", Generation: 1},
		Err:        errors.New("helper lost during Run before Started"),
	}
	return workloadrunner.Result{Outcome: spawnFailure(contract.SpawnFailureRuntimeUnavailable, err)}, err
}

func (runtime *preStartedRuntimeLossRuntime) ReapAndVerify(context.Context, workloadrunner.ReapRequest) (workloadrunner.ReapReceipt, error) {
	return workloadrunner.ReapReceipt{}, &workloadrunner.RuntimeLossError{
		Generation: workloadrunner.RuntimeGeneration{InstanceID: "stage-loss", Generation: 1},
		Err:        errors.New("helper unavailable while reaping pre-Started attempt"),
	}
}

func newLiveIntentAuthorityRuntime() *liveIntentAuthorityRuntime {
	return &liveIntentAuthorityRuntime{intentStopRuntime: newIntentStopRuntime(), finish: make(chan struct{})}
}

func (runtime *liveIntentAuthorityRuntime) Run(ctx context.Context, request workloadrunner.Request, _ workloadrunner.OutputSink) (workloadrunner.Result, error) {
	digest := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	observation := workloadrunner.OCIImageObservation{
		SubmittedReference: request.Execution.OCI.Image.Reference,
		TopLevelDigest:     digest, TopLevelMediaType: "application/vnd.oci.image.manifest.v1+json",
		PlatformManifestDigest: digest, PlatformOS: "linux", PlatformArchitecture: "amd64",
		RuntimeHandler: "io.containerd.runc.v2", Snapshotter: "overlayfs",
	}
	if err := request.OCIImageResolved(ctx, observation); err != nil {
		return workloadrunner.Result{}, err
	}
	if err := request.OCIStarted(ctx, observation); err != nil {
		return workloadrunner.Result{}, err
	}
	runtime.starts.Add(1)
	runtime.started <- request.Authority.AttemptID
	<-runtime.finish
	exitCode := 7
	return workloadrunner.Result{Outcome: contract.ProcessResult{ExitCode: &exitCode}}, nil
}

func (runtime *liveIntentAuthorityRuntime) complete() {
	runtime.once.Do(func() { close(runtime.finish) })
}

func newIntentMarkerRaceRuntime() *intentMarkerRaceRuntime {
	return &intentMarkerRaceRuntime{
		intentStopRuntime: newIntentStopRuntime(), firstFinished: make(chan struct{}), startFailed: make(chan error, 1),
	}
}

func (runtime *intentMarkerRaceRuntime) Run(ctx context.Context, request workloadrunner.Request, _ workloadrunner.OutputSink) (workloadrunner.Result, error) {
	digest := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	observation := workloadrunner.OCIImageObservation{
		SubmittedReference: request.Execution.OCI.Image.Reference,
		TopLevelDigest:     digest, TopLevelMediaType: "application/vnd.oci.image.manifest.v1+json",
		PlatformManifestDigest: digest, PlatformOS: "linux", PlatformArchitecture: "amd64",
		RuntimeHandler: "io.containerd.runc.v2", Snapshotter: "overlayfs",
	}
	if err := request.OCIStarted(ctx, observation); err != nil {
		runtime.startFailed <- err
		return workloadrunner.Result{}, err
	}
	if err := admitReadyOCIHelper(request); err != nil {
		runtime.startFailed <- err
		return workloadrunner.Result{}, err
	}
	started := runtime.starts.Add(1)
	runtime.started <- request.Authority.AttemptID
	if started == 1 {
		<-runtime.firstFinished
		exitCode := 1
		return workloadrunner.Result{Outcome: contract.ProcessResult{ExitCode: &exitCode, LogEvidenceIncomplete: true}}, nil
	}
	<-ctx.Done()
	runtime.canceled <- request.Authority.AttemptID
	return workloadrunner.Result{Outcome: contract.ProcessResult{
		Signal: "terminated", TerminationCause: contract.TerminationCauseAgent,
	}}, context.Cause(ctx)
}

func (runtime *intentMarkerRaceRuntime) finishFirst() {
	runtime.finishOnce.Do(func() { close(runtime.firstFinished) })
}

func newOneShotIntentRuntime() *oneShotIntentRuntime {
	return &oneShotIntentRuntime{intentStopRuntime: newIntentStopRuntime()}
}

func (runtime *oneShotIntentRuntime) Run(ctx context.Context, request workloadrunner.Request, _ workloadrunner.OutputSink) (workloadrunner.Result, error) {
	runtime.starts.Add(1)
	digest := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	observation := workloadrunner.OCIImageObservation{
		SubmittedReference: request.Execution.OCI.Image.Reference,
		TopLevelDigest:     digest, TopLevelMediaType: "application/vnd.oci.image.manifest.v1+json",
		PlatformManifestDigest: digest, PlatformOS: "linux", PlatformArchitecture: "amd64",
		RuntimeHandler: "io.containerd.runc.v2", Snapshotter: "overlayfs",
	}
	if err := request.OCIImageResolved(ctx, observation); err != nil {
		return workloadrunner.Result{}, err
	}
	if err := request.OCIStarted(ctx, observation); err != nil {
		return workloadrunner.Result{}, err
	}
	if err := admitReadyOCIHelper(request); err != nil {
		return workloadrunner.Result{}, err
	}
	runtime.started <- request.Authority.AttemptID
	<-ctx.Done()
	exitCode := 0
	return workloadrunner.Result{Outcome: contract.ProcessResult{ExitCode: &exitCode}}, nil
}

func (runtime *oneShotIntentRuntime) FinalizeManagedVolumes(context.Context, workloadrunner.ManagedVolumeFinalizationRequest) error {
	runtime.finalized.Add(1)
	return nil
}

func newOutcomeFirstIntentRuntime() *outcomeFirstIntentRuntime {
	return &outcomeFirstIntentRuntime{intentStopRuntime: newIntentStopRuntime(), finish: make(chan struct{})}
}

func (runtime *outcomeFirstIntentRuntime) Run(_ context.Context, request workloadrunner.Request, _ workloadrunner.OutputSink) (workloadrunner.Result, error) {
	runtime.starts.Add(1)
	runtime.started <- request.Authority.AttemptID
	<-runtime.finish
	exitCode := 0
	return workloadrunner.Result{Outcome: contract.ProcessResult{ExitCode: &exitCode}}, nil
}

func (runtime *outcomeFirstIntentRuntime) complete() {
	runtime.once.Do(func() { close(runtime.finish) })
}

func newIntentStopRuntime() *intentStopRuntime {
	return &intentStopRuntime{started: make(chan string, 1), canceled: make(chan string, 2), releaseC: make(chan struct{})}
}

func (runtime *intentStopRuntime) Preflight(_ context.Context, request workloadrunner.Request) (workloadrunner.Admission, workloadrunner.Result, error) {
	return workloadrunner.Admission{Request: request, Release: func() {}}, workloadrunner.Result{}, nil
}

func (runtime *intentStopRuntime) Run(ctx context.Context, request workloadrunner.Request, _ workloadrunner.OutputSink) (workloadrunner.Result, error) {
	runtime.starts.Add(1)
	runtime.started <- request.Authority.AttemptID
	<-ctx.Done()
	runtime.canceled <- request.Authority.AttemptID
	<-runtime.releaseC
	return workloadrunner.Result{Outcome: contract.ProcessResult{Signal: "terminated", TerminationCause: contract.TerminationCauseAgent}}, context.Cause(ctx)
}

func (*intentStopRuntime) ReapAndVerify(context.Context, workloadrunner.ReapRequest) (workloadrunner.ReapReceipt, error) {
	return workloadrunner.ReapReceipt{RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidenceAttempt}, nil
}

func (*intentStopRuntime) RemovalResourceManifest(request workloadrunner.Request) (workloadrunner.RuntimeResourceManifest, error) {
	suffix := request.Authority.AttemptID
	return workloadrunner.RuntimeResourceManifest{
		Version: 1, RuntimeKind: contract.JobKindOCI,
		NodeID: request.Authority.NodeID, BootSessionID: request.Authority.BootSessionID,
		JobID: request.Authority.JobID, AttemptID: suffix, FencingToken: request.Authority.FencingToken,
		WorkloadClass: request.Authority.WorkloadClass, RemovalGeneration: request.Authority.RemovalGeneration,
		LeaseID: "lease-" + suffix, TaskID: "task-" + suffix, ContainerID: "container-" + suffix,
		SnapshotID: "snapshot-" + suffix, ShimID: "shim-" + suffix, CgroupID: "cgroup-" + suffix,
		LogSegmentDirectory: "logs-" + suffix, ServiceDataVolume: "service-" + request.Authority.JobID,
		ServiceDataOwnerRecord: "service-" + request.Authority.JobID + ".owner",
	}, nil
}

func (runtime *intentStopRuntime) waitCanceled(t *testing.T) {
	t.Helper()
	select {
	case <-runtime.canceled:
	case <-time.After(5 * time.Second):
		t.Fatal("intent-stop OCI service was not canceled")
	}
}

func (runtime *intentStopRuntime) release() {
	runtime.once.Do(func() { close(runtime.releaseC) })
}

func newFinalizationAnchorRunner() *finalizationAnchorRunner {
	return &finalizationAnchorRunner{started: make(chan string, 1), killed: make(chan struct{})}
}

func (runner *finalizationAnchorRunner) Run(ctx context.Context, request processrunner.Request, sink processrunner.OutputSink) (contract.ProcessResult, error) {
	if request.Started != nil {
		request.Started()
	}
	runner.started <- request.AttemptID
	select {
	case <-ctx.Done():
		return contract.ProcessResult{Signal: "terminated", TerminationCause: contract.TerminationCauseAgent}, ctx.Err()
	case <-runner.killed:
	}
	if err := sink.WriteOutput(ctx, contract.LogEvent{
		AttemptID: request.AttemptID, Stream: contract.LogStdout, Sequence: 0, Bytes: []byte("final service event"),
	}); err != nil {
		return contract.ProcessResult{}, err
	}
	return contract.ProcessResult{Signal: "killed", TerminationCause: contract.TerminationCauseSpontaneous}, nil
}

func (runner *finalizationAnchorRunner) waitStarted(t *testing.T) string {
	t.Helper()
	select {
	case attemptID := <-runner.started:
		return attemptID
	case <-time.After(5 * time.Second):
		t.Fatal("finalization-anchor service did not start")
		return ""
	}
}

func (runner *finalizationAnchorRunner) kill() {
	runner.once.Do(func() { close(runner.killed) })
}

func newLoggingBlockingRunner() *loggingBlockingRunner {
	return &loggingBlockingRunner{started: make(chan string, 1), canceled: make(chan struct{})}
}

func (runner *loggingBlockingRunner) Run(ctx context.Context, request processrunner.Request, sink processrunner.OutputSink) (contract.ProcessResult, error) {
	if request.Started != nil {
		request.Started()
	}
	if err := sink.WriteOutput(ctx, contract.LogEvent{AttemptID: request.AttemptID, Stream: contract.LogStdout, Sequence: 0, Bytes: []byte("shutdown evidence")}); err != nil {
		return contract.ProcessResult{}, err
	}
	runner.started <- request.AttemptID
	<-ctx.Done()
	close(runner.canceled)
	return contract.ProcessResult{Signal: "terminated", TerminationCause: contract.TerminationCauseAgent}, ctx.Err()
}

func (runner *loggingBlockingRunner) waitStarted(t *testing.T) string {
	t.Helper()
	select {
	case attemptID := <-runner.started:
		return attemptID
	case <-time.After(5 * time.Second):
		t.Fatal("logging runner did not start")
		return ""
	}
}

type directiveRunner struct {
	started  chan struct{}
	canceled chan struct{}
}

func newDirectiveRunner() *directiveRunner {
	return &directiveRunner{started: make(chan struct{}, 4), canceled: make(chan struct{}, 4)}
}

func (runner *directiveRunner) Run(ctx context.Context, request processrunner.Request, _ processrunner.OutputSink) (contract.ProcessResult, error) {
	if request.Started != nil {
		request.Started()
	}
	runner.started <- struct{}{}
	<-ctx.Done()
	runner.canceled <- struct{}{}
	return contract.ProcessResult{Signal: "terminated", TerminationCause: contract.TerminationCauseAgent}, ctx.Err()
}

func (runner *directiveRunner) waitStarted(t *testing.T) {
	t.Helper()
	select {
	case <-runner.started:
	case <-time.After(5 * time.Second):
		t.Fatal("directive runner did not start")
	}
}

func (runner *directiveRunner) waitCanceled(t *testing.T) {
	t.Helper()
	select {
	case <-runner.canceled:
	case <-time.After(15 * time.Second):
		t.Fatal("directive did not cancel payload")
	}
}

type stubbornRunner struct {
	started  chan struct{}
	canceled chan struct{}
	releaseC chan struct{}
}

func newStubbornRunner() *stubbornRunner {
	return &stubbornRunner{started: make(chan struct{}), canceled: make(chan struct{}), releaseC: make(chan struct{})}
}

func (runner *stubbornRunner) Run(ctx context.Context, request processrunner.Request, _ processrunner.OutputSink) (contract.ProcessResult, error) {
	if request.Started != nil {
		request.Started()
	}
	close(runner.started)
	<-ctx.Done()
	close(runner.canceled)
	<-runner.releaseC
	return contract.ProcessResult{Signal: "canceled", TerminationCause: contract.TerminationCauseAgent}, ctx.Err()
}

func (runner *stubbornRunner) waitStarted(t *testing.T) {
	t.Helper()
	select {
	case <-runner.started:
	case <-time.After(5 * time.Second):
		t.Fatal("stubborn runner did not start")
	}
}

func (runner *stubbornRunner) waitCanceled(t *testing.T) {
	t.Helper()
	select {
	case <-runner.canceled:
	case <-time.After(5 * time.Second):
		t.Fatal("stubborn runner did not observe cancellation")
	}
}

func (runner *stubbornRunner) release() { close(runner.releaseC) }

func newResilienceRunner() *resilienceRunner {
	return &resilienceRunner{
		starts: make(map[string]int), started: make(chan string, 4), canceled: make(chan string, 4),
	}
}

func (runner *resilienceRunner) Run(ctx context.Context, request processrunner.Request, _ processrunner.OutputSink) (contract.ProcessResult, error) {
	if request.Started != nil {
		request.Started()
	}
	runner.mu.Lock()
	runner.starts[request.AttemptID]++
	ordinal := len(runner.starts)
	runner.mu.Unlock()
	runner.started <- request.AttemptID
	if ordinal == 1 {
		<-ctx.Done()
		runner.canceled <- request.AttemptID
		return contract.ProcessResult{Signal: "canceled", TerminationCause: contract.TerminationCauseAgent}, ctx.Err()
	}
	exitCode := 0
	return contract.ProcessResult{ExitCode: &exitCode}, nil
}

func (runner *resilienceRunner) waitStarted(t *testing.T) string {
	t.Helper()
	select {
	case attemptID := <-runner.started:
		return attemptID
	case <-time.After(5 * time.Second):
		t.Fatal("process runner did not start")
		return ""
	}
}

func (runner *resilienceRunner) waitCanceled(t *testing.T, want string) {
	t.Helper()
	select {
	case attemptID := <-runner.canceled:
		if attemptID != want {
			t.Fatalf("canceled attempt = %q, want %q", attemptID, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("authority-lost payload was not canceled and reaped")
	}
}

func (runner *resilienceRunner) startCount(attemptID string) int {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	return runner.starts[attemptID]
}

func newBlockingRunner() *blockingRunner {
	return &blockingRunner{started: make(chan struct{}), releaseCh: make(chan struct{})}
}

func (runner *blockingRunner) Run(ctx context.Context, request processrunner.Request, _ processrunner.OutputSink) (contract.ProcessResult, error) {
	if request.Started != nil {
		request.Started()
	}
	if runner.starts.Add(1) == 1 {
		close(runner.started)
	}
	select {
	case <-ctx.Done():
		return contract.ProcessResult{Signal: "canceled", TerminationCause: contract.TerminationCauseAgent}, ctx.Err()
	case <-runner.releaseCh:
		exitCode := 0
		return contract.ProcessResult{ExitCode: &exitCode}, nil
	}
}

func (runner *blockingRunner) waitStarted(t *testing.T) {
	t.Helper()
	select {
	case <-runner.started:
	case <-time.After(5 * time.Second):
		t.Fatal("process runner did not start")
	}
}

func (runner *blockingRunner) release() {
	runner.releaseOnce.Do(func() { close(runner.releaseCh) })
}

type manualClock struct {
	mu     sync.Mutex
	now    time.Time
	wall   time.Time
	timers []*manualTimer
}

func newManualClock(now time.Time) *manualClock { return &manualClock{now: now, wall: now} }

func (clock *manualClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

func (clock *manualClock) WallNow() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.wall
}

func (clock *manualClock) NewTimer(duration time.Duration) Timer {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	timer := &manualTimer{clock: clock, channel: make(chan time.Time, 1), deadline: clock.now.Add(duration), active: true}
	clock.timers = append(clock.timers, timer)
	return timer
}

func (clock *manualClock) Advance(duration time.Duration) {
	clock.mu.Lock()
	clock.now = clock.now.Add(duration)
	clock.wall = clock.wall.Add(duration)
	now := clock.now
	var ready []*manualTimer
	for _, timer := range clock.timers {
		if timer.active && !timer.deadline.After(now) {
			timer.active = false
			ready = append(ready, timer)
		}
	}
	clock.mu.Unlock()
	for _, timer := range ready {
		timer.channel <- now
	}
}

func (clock *manualClock) AdvanceWall(duration time.Duration) {
	clock.mu.Lock()
	clock.wall = clock.wall.Add(duration)
	clock.mu.Unlock()
}

func (clock *manualClock) waitForDeadline(t *testing.T, deadline time.Time) {
	t.Helper()
	waitUntil := time.Now().Add(5 * time.Second)
	for time.Now().Before(waitUntil) {
		clock.mu.Lock()
		found := false
		for _, timer := range clock.timers {
			if timer.active && timer.deadline.Equal(deadline) {
				found = true
				break
			}
		}
		clock.mu.Unlock()
		if found {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for timer deadline %s", deadline)
}

type manualTimer struct {
	clock    *manualClock
	channel  chan time.Time
	deadline time.Time
	active   bool
}

func (timer *manualTimer) C() <-chan time.Time { return timer.channel }

func (timer *manualTimer) Stop() bool {
	timer.clock.mu.Lock()
	defer timer.clock.mu.Unlock()
	active := timer.active
	timer.active = false
	return active
}

func (timer *manualTimer) Reset(duration time.Duration) bool {
	timer.clock.mu.Lock()
	defer timer.clock.mu.Unlock()
	active := timer.active
	timer.deadline = timer.clock.now.Add(duration)
	timer.active = true
	return active
}
