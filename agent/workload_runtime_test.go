package agent

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/l3"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	processrunner "github.com/Derek-X-Wang/wefty/runner/process"
)

func testProcessRuntime(executor processrunner.Executor) WorkloadRuntime {
	return processrunner.NewAdapter(executor)
}

func testRuntimeSet(executor processrunner.Executor) workloadRuntimeSet {
	return workloadRuntimeSet{"process": testProcessRuntime(executor)}
}

func workloadRequest(attemptID string) workloadrunner.Request {
	return workloadrunner.Request{
		Authority:        workloadrunner.AttemptAuthority{AttemptID: attemptID},
		LifetimeBoundary: workloadrunner.AgentBootLifetime,
	}
}

func TestOCIRuntimeSweepMapsToL1QuiescenceContract(t *testing.T) {
	if got := toL1QuiescenceEvidence(workloadrunner.ReapEvidenceOCIRuntimeSweep); got != l1.RuntimeQuiescenceOCISweep {
		t.Fatalf("runtime sweep evidence mapped to %q, want %q", got, l1.RuntimeQuiescenceOCISweep)
	}
}

func TestWorkloadRuntimeRequiresPositiveReapVerification(t *testing.T) {
	runtime := &reapRefusingRuntime{}
	lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{
		runtimes: workloadRuntimeSet{"future.microvm": runtime},
		clock:    systemClock{}, nodeID: "node-1", bootSessionID: "boot-1",
	})
	claim := l1.Claim{
		Job:   l1.Job{JobID: "future-job", Spec: contract.JobSpec{Kind: "future.microvm", Class: contract.JobClassOneShot}},
		Lease: l1.AttemptLease{AttemptID: "future-attempt", FencingToken: "fence-1"},
	}
	result, err := lifecycle.runWorkload(context.Background(), claim)
	if err == nil || !strings.Contains(err.Error(), "did not verify quiescence") {
		t.Fatalf("unverified runtime result = (%#v, %v), want quiescence failure", result, err)
	}
	if result.OutputError == "" || result.ExitCode != nil || result.SpawnError != nil || result.RuntimeFailure != nil || result.Signal != "" {
		t.Fatalf("unverified runtime result = %#v, want only output_error", result)
	}
	if runtime.runCalls != 1 || runtime.reapCalls != 1 {
		t.Fatalf("runtime calls = run %d reap %d, want one each", runtime.runCalls, runtime.reapCalls)
	}
	if got := runtime.request.Authority; got.NodeID != "node-1" || got.BootSessionID != "boot-1" ||
		got.JobID != "future-job" || got.AttemptID != "future-attempt" || got.FencingToken != "fence-1" {
		t.Fatalf("runtime authority = %#v", got)
	}
	if runtime.request.LifetimeBoundary != workloadrunner.CallerLifetime || runtime.request.IdlePolicy != workloadrunner.MonitorIdle {
		t.Fatalf("one-shot mechanical policy = lifetime %d idle %d", runtime.request.LifetimeBoundary, runtime.request.IdlePolicy)
	}
}

func TestWorkloadRuntimeReapUsesFinalizationContext(t *testing.T) {
	adapter := &finalizationContextRuntime{}
	lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{
		runtimes: workloadRuntimeSet{"future.microvm": adapter}, clock: systemClock{},
		nodeID: "node-1", bootSessionID: "boot-1",
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	claim := l1.Claim{
		Job: l1.Job{JobID: "job-finalization-context", Spec: contract.JobSpec{
			Kind: "future.microvm", Class: contract.JobClassOneShot,
		}},
		Lease: l1.AttemptLease{AttemptID: "attempt-finalization-context", FencingToken: "fence-1"},
	}
	result, err := lifecycle.runWorkload(ctx, claim)
	if err != nil || result.ExitCode == nil || *result.ExitCode != 0 {
		t.Fatalf("workload with canceled execution context = (%#v, %v)", result, err)
	}
	if adapter.reapContextErr != nil {
		t.Fatalf("reap context inherited execution cancellation: %v", adapter.reapContextErr)
	}
}

func TestOCIImageDeliveryUsesLocalPullingStateBeforeStartedCallback(t *testing.T) {
	observer := newLifecycleObserver(systemClock{})
	observer.beginAttempt("oci-attempt", "oci-job", contract.JobClassOneShot)
	adapter := &pullingObservationRuntime{observer: observer}
	lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{
		runtimes: workloadRuntimeSet{contract.JobKindOCI: adapter}, observer: observer,
		clock: systemClock{}, nodeID: "node-1", bootSessionID: "boot-1",
	})
	claim := l1.Claim{
		Job:   l1.Job{JobID: "oci-job", Spec: contract.JobSpec{Kind: contract.JobKindOCI, Class: contract.JobClassOneShot}},
		Lease: l1.AttemptLease{AttemptID: "oci-attempt", FencingToken: "fence-1"},
	}
	deadline := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)
	claim.PrestartDeadline = &deadline
	result, err := lifecycle.runWorkload(t.Context(), claim)
	if err != nil || result.ExitCode == nil || *result.ExitCode != 0 {
		t.Fatalf("OCI pulling observation result=(%+v, %v)", result, err)
	}
	if adapter.observed != AttemptPulling {
		t.Fatalf("local state during image delivery = %q, want pulling", adapter.observed)
	}
	if !adapter.startedCallbackPresent {
		t.Fatal("OCI Started callback was not retained after local pulling observation")
	}
	if !adapter.resolvedCallbackPresent || !adapter.imageDeadline.Equal(deadline) {
		t.Fatalf("OCI resolution authority = callback %t deadline %s, want %s", adapter.resolvedCallbackPresent, adapter.imageDeadline, deadline)
	}
	if adapter.afterPull != AttemptStarting {
		t.Fatalf("local state after image delivery = %q, want starting", adapter.afterPull)
	}
}

func TestOCIImageFailuresSurviveFullLifecycleFinalization(t *testing.T) {
	codes := []contract.SpawnFailureCode{
		contract.SpawnFailureImageUnavailable,
		contract.SpawnFailureImageNotFound,
		contract.SpawnFailureImageManifestInvalid,
		contract.SpawnFailureImagePlatformUnsupported,
		contract.SpawnFailureRuntimeUnavailable,
	}
	for _, code := range codes {
		t.Run(string(code), func(t *testing.T) {
			runtime := &preRunFailureRuntime{code: code}
			lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{
				runtimes: workloadRuntimeSet{contract.JobKindOCI: runtime}, observer: newLifecycleObserver(systemClock{}),
				clock: systemClock{}, nodeID: "node-1", bootSessionID: "boot-1",
			})
			claim := l1.Claim{
				Job:   l1.Job{JobID: "oci-failure", Spec: contract.JobSpec{Kind: contract.JobKindOCI, Class: contract.JobClassOneShot}},
				Lease: l1.AttemptLease{AttemptID: "attempt-failure", FencingToken: "fence-1"},
			}
			result, err := lifecycle.runWorkload(t.Context(), claim)
			if err == nil || result.SpawnError == nil || result.SpawnError.Code != code || result.OutputError != "" {
				t.Fatalf("full lifecycle outcome = (%+v, %v), want %s without output_error", result, err, code)
			}
		})
	}
}

func TestOCIOneshotDelegatesHandoffToRuntime(t *testing.T) {
	runtime := &captureRuntime{}
	handoffRoot := t.TempDir()
	lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{
		runtimes: workloadRuntimeSet{contract.JobKindOCI: runtime},
		handoffs: newHandoffManager(handoffRoot, time.Hour),
		observer: newLifecycleObserver(systemClock{}),
		clock:    systemClock{}, nodeID: "node-1", bootSessionID: "boot-1",
	})
	claim := l1.Claim{
		Job: l1.Job{JobID: "oci-job", Spec: contract.JobSpec{
			Kind: contract.JobKindOCI, Class: contract.JobClassOneShot,
			Execution: contract.ExecutionSpec{
				Env: map[string]string{contract.EnvHandoffDir: contract.OCIContainerHandoffDirectory},
				OCI: &contract.OCIExecutionSpec{Image: contract.OCIImageSpec{Reference: "ghcr.io/example/echo:latest"}},
			},
			Labels: map[string]string{"run_id": "run-oci"},
		}},
		Lease: l1.AttemptLease{AttemptID: "oci-attempt", FencingToken: "fence-1", LeaseTTL: time.Minute},
	}
	result, err := lifecycle.runWorkload(t.Context(), claim)
	if err != nil || result.ExitCode == nil || *result.ExitCode != 0 {
		t.Fatalf("OCI one-shot result = (%+v, %v)", result, err)
	}
	if got := runtime.request.Execution.Env[contract.EnvHandoffDir]; got != contract.OCIContainerHandoffDirectory {
		t.Fatalf("OCI handoff environment = %q, want %q", got, contract.OCIContainerHandoffDirectory)
	}
	entries, err := os.ReadDir(handoffRoot)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("agent created host handoff paths for OCI: %v", entries)
	}
}

func TestOCIRuntimeLossRecoversSweepBeforeReap(t *testing.T) {
	tests := []struct {
		name   string
		result contract.ProcessResult
	}{
		{name: "pre-start helper loss", result: contract.ProcessResult{SpawnError: &contract.SpawnFailure{Code: contract.SpawnFailureRuntimeUnavailable, Message: "helper lost"}}},
		{name: "post-start engine loss", result: contract.ProcessResult{RuntimeFailure: &contract.RuntimeFailure{Code: contract.RuntimeFailureUnavailable, Message: "engine lost"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runtime := &recoverySweepRuntime{result: test.result}
			lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{
				runtimes: workloadRuntimeSet{contract.JobKindOCI: runtime}, observer: newLifecycleObserver(systemClock{}),
				clock: systemClock{}, nodeID: "node-1", bootSessionID: "boot-1",
				recoverOCIRuntime: func(context.Context, workloadrunner.RuntimeGeneration) error {
					runtime.recovered = true
					return nil
				},
			})
			claim := l1.Claim{
				Job:   l1.Job{JobID: "oci-runtime-loss", Spec: contract.JobSpec{Kind: contract.JobKindOCI, Class: contract.JobClassOneShot}},
				Lease: l1.AttemptLease{AttemptID: "attempt-runtime-loss", FencingToken: "fence-1"},
			}
			result, err := lifecycle.runWorkload(t.Context(), claim)
			if err == nil || result.OutputError != "" || !runtime.recovered || !runtime.reapedAfterRecovery {
				t.Fatalf("runtime-loss lifecycle = result %+v err %v recovered %t reaped-after %t", result, err, runtime.recovered, runtime.reapedAfterRecovery)
			}
		})
	}
}

func TestOCIRuntimeLossReusesNewerSiblingSweep(t *testing.T) {
	for _, test := range []struct {
		name     string
		reported workloadrunner.RuntimeGeneration
	}{
		{name: "reported generation", reported: workloadrunner.RuntimeGeneration{InstanceID: "lost-helper", Generation: 7}},
		{name: "empty generation uses hook-arm snapshot"},
	} {
		t.Run(test.name, func(t *testing.T) {
			invalidations := 0
			barrier := &recordingOCIBootBarrier{ready: true, invalidate: func() { invalidations++ }}
			agent := &Agent{session: &agentSession{ociBootBarrier: barrier}}
			latch := ociRuntimeLossLatch{armed: workloadrunner.RuntimeGeneration{InstanceID: "lost-helper", Generation: 7}}
			observed := latch.record(test.reported)
			err := agent.recoverOCIRuntimeAfterLoss(t.Context(), observed)
			if err != nil || invalidations != 0 {
				t.Fatalf("newer sibling sweep recovery = err %v invalidations %d observed %+v", err, invalidations, observed)
			}
		})
	}
}

func TestOCIRuntimeLossEmbargoesSiblingBeforeRunReturns(t *testing.T) {
	state := newCapabilityState(map[string]bool{"kind:process": true}, nil, systemClock{}, 0)
	state.record(CapabilityProbeResult{Capabilities: map[string]bool{"kind:oci": true}}, nil)
	runtime := &pausingRuntimeLossRuntime{reported: make(chan struct{}), release: make(chan struct{})}
	lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{
		runtimes: workloadRuntimeSet{contract.JobKindOCI: runtime}, observer: newLifecycleObserver(systemClock{}),
		clock: systemClock{}, nodeID: "node-1", bootSessionID: "boot-1", allowsStart: state.allows,
		embargoOCIRuntime: func(workloadrunner.RuntimeGeneration) {
			state.suppressOCI(contract.CapabilityReasonBootSweepFailed, errors.New("runtime lost"))
		},
	})
	claim := l1.Claim{
		Job: l1.Job{JobID: "oci-loss-embargo", Spec: contract.JobSpec{
			Kind: contract.JobKindOCI, Class: contract.JobClassOneShot,
			Execution: contract.ExecutionSpec{OCI: &contract.OCIExecutionSpec{Image: contract.OCIImageSpec{Reference: "example.invalid/image"}}},
		}},
		Lease: l1.AttemptLease{AttemptID: "attempt-loss-embargo", FencingToken: "fence-1"},
	}
	done := make(chan struct{})
	go func() {
		_, _ = lifecycle.runWorkload(t.Context(), claim)
		close(done)
	}()
	<-runtime.reported
	if state.allows(claim.Job.Spec) {
		t.Fatal("sibling OCI admission remained open after runtime-loss callback")
	}
	close(runtime.release)
	<-done
}

func TestOCIReapRuntimeLossEmbargoesBeforeRecoveryAndRetries(t *testing.T) {
	state := newCapabilityState(map[string]bool{"kind:process": true}, nil, systemClock{}, 0)
	state.record(CapabilityProbeResult{Capabilities: map[string]bool{"kind:oci": true}}, nil)
	runtime := &reapRuntimeLossRuntime{}
	lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{
		runtimes: workloadRuntimeSet{contract.JobKindOCI: runtime}, observer: newLifecycleObserver(systemClock{}),
		clock: systemClock{}, nodeID: "node-1", bootSessionID: "boot-1", allowsStart: state.allows,
		embargoOCIRuntime: func(workloadrunner.RuntimeGeneration) {
			state.suppressOCI(contract.CapabilityReasonBootSweepFailed, errors.New("runtime lost during reap"))
		},
		recoverOCIRuntime: func(context.Context, workloadrunner.RuntimeGeneration) error {
			if state.allows(contract.JobSpec{Kind: contract.JobKindOCI}) {
				return errors.New("replacement OCI claim remained admitted before recovery")
			}
			runtime.recovered = true
			return nil
		},
	})
	claim := l1.Claim{
		Job: l1.Job{JobID: "oci-reap-loss", Spec: contract.JobSpec{
			Kind: contract.JobKindOCI, Class: contract.JobClassOneShot,
			Execution: contract.ExecutionSpec{OCI: &contract.OCIExecutionSpec{Image: contract.OCIImageSpec{Reference: "example.invalid/image"}}},
		}},
		Lease: l1.AttemptLease{AttemptID: "attempt-reap-loss", FencingToken: "fence-1"},
	}
	result, err := lifecycle.runWorkload(t.Context(), claim)
	if err != nil || result.ExitCode == nil || *result.ExitCode != 0 || runtime.reapCalls != 2 || !runtime.recovered {
		t.Fatalf("reap-loss recovery = result %+v err %v calls %d recovered %t", result, err, runtime.reapCalls, runtime.recovered)
	}
}

type reapRuntimeLossRuntime struct {
	reapCalls int
	recovered bool
}

func (runtime *reapRuntimeLossRuntime) Preflight(_ context.Context, request workloadrunner.Request) (workloadrunner.Admission, workloadrunner.Result, error) {
	return workloadrunner.Admission{Request: request, Release: func() {}}, workloadrunner.Result{}, nil
}

func (*reapRuntimeLossRuntime) Run(context.Context, workloadrunner.Request, workloadrunner.OutputSink) (workloadrunner.Result, error) {
	exit := 0
	return workloadrunner.Result{Outcome: contract.ProcessResult{ExitCode: &exit}}, nil
}

func (runtime *reapRuntimeLossRuntime) ReapAndVerify(context.Context, workloadrunner.ReapRequest) (workloadrunner.ReapReceipt, error) {
	runtime.reapCalls++
	if runtime.reapCalls == 1 {
		return workloadrunner.ReapReceipt{}, &workloadrunner.RuntimeLossError{
			Generation: workloadrunner.RuntimeGeneration{InstanceID: "helper-old", Generation: 7}, Err: errors.New("Delete lost engine"),
		}
	}
	if !runtime.recovered {
		return workloadrunner.ReapReceipt{}, errors.New("reap retried before recovery")
	}
	return workloadrunner.ReapReceipt{RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidenceOCISweep}, nil
}

func TestOCIRuntimeFailureArmSurvivesRecoveryFailure(t *testing.T) {
	runtime := &recoverySweepRuntime{result: contract.ProcessResult{RuntimeFailure: &contract.RuntimeFailure{
		Code: contract.RuntimeFailureUnavailable, Message: "engine lost",
	}}}
	lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{
		runtimes: workloadRuntimeSet{contract.JobKindOCI: runtime}, observer: newLifecycleObserver(systemClock{}),
		clock: systemClock{}, nodeID: "node-1", bootSessionID: "boot-1",
		recoverOCIRuntime: func(context.Context, workloadrunner.RuntimeGeneration) error {
			return errors.New("recovery budget expired")
		},
	})
	claim := l1.Claim{
		Job:   l1.Job{JobID: "oci-runtime-failure-arm", Spec: contract.JobSpec{Kind: contract.JobKindOCI, Class: contract.JobClassOneShot}},
		Lease: l1.AttemptLease{AttemptID: "attempt-runtime-failure-arm", FencingToken: "fence-1"},
	}
	result, err := lifecycle.runWorkload(t.Context(), claim)
	if err == nil || result.RuntimeFailure == nil || result.RuntimeFailure.Code != contract.RuntimeFailureUnavailable || result.OutputError != "" {
		t.Fatalf("recovery failure overwrote runtime arm = result %+v err %v", result, err)
	}
}

func TestOCIRecoveryMutexAcquisitionHonorsContext(t *testing.T) {
	session := &agentSession{}
	session.ociRecoveryMu.Lock()
	defer session.ociRecoveryMu.Unlock()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	if _, err := session.recoverOCIRuntime(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("context-aware OCI recovery lock = %v, want deadline", err)
	}
}

type pausingRuntimeLossRuntime struct {
	reported chan struct{}
	release  chan struct{}
}

func (runtime *pausingRuntimeLossRuntime) Preflight(_ context.Context, request workloadrunner.Request) (workloadrunner.Admission, workloadrunner.Result, error) {
	return workloadrunner.Admission{Request: request, Release: func() {}}, workloadrunner.Result{}, nil
}

func (runtime *pausingRuntimeLossRuntime) Run(_ context.Context, request workloadrunner.Request, _ workloadrunner.OutputSink) (workloadrunner.Result, error) {
	request.OCIRuntimeUnavailable(workloadrunner.RuntimeGeneration{})
	close(runtime.reported)
	<-runtime.release
	err := errors.New("runtime lost")
	return workloadrunner.Result{Outcome: contract.ProcessResult{RuntimeFailure: &contract.RuntimeFailure{Code: contract.RuntimeFailureUnavailable, Message: err.Error()}}}, err
}

func (*pausingRuntimeLossRuntime) ReapAndVerify(context.Context, workloadrunner.ReapRequest) (workloadrunner.ReapReceipt, error) {
	return workloadrunner.ReapReceipt{RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidenceOCISweep}, nil
}

type recoverySweepRuntime struct {
	result              contract.ProcessResult
	recovered           bool
	reapedAfterRecovery bool
}

func (runtime *recoverySweepRuntime) Preflight(_ context.Context, request workloadrunner.Request) (workloadrunner.Admission, workloadrunner.Result, error) {
	return workloadrunner.Admission{Request: request, Release: func() {}}, workloadrunner.Result{}, nil
}

func (runtime *recoverySweepRuntime) Run(_ context.Context, request workloadrunner.Request, _ workloadrunner.OutputSink) (workloadrunner.Result, error) {
	if request.OCIRuntimeUnavailable == nil {
		return workloadrunner.Result{Outcome: runtime.result}, errors.New("OCI runtime recovery callback is absent")
	}
	request.OCIRuntimeUnavailable(workloadrunner.RuntimeGeneration{InstanceID: "helper-old", Generation: 7})
	return workloadrunner.Result{Outcome: runtime.result}, errors.New("OCI runtime unavailable")
}

func (runtime *recoverySweepRuntime) ReapAndVerify(context.Context, workloadrunner.ReapRequest) (workloadrunner.ReapReceipt, error) {
	runtime.reapedAfterRecovery = runtime.recovered
	if !runtime.recovered {
		return workloadrunner.ReapReceipt{}, errors.New("sweep embargo was not completed")
	}
	return workloadrunner.ReapReceipt{RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidenceOCISweep}, nil
}

type preRunFailureRuntime struct{ code contract.SpawnFailureCode }

func (runtime *preRunFailureRuntime) Preflight(_ context.Context, request workloadrunner.Request) (workloadrunner.Admission, workloadrunner.Result, error) {
	return workloadrunner.Admission{Request: request, Release: func() {}}, workloadrunner.Result{}, nil
}

func (runtime *preRunFailureRuntime) Run(context.Context, workloadrunner.Request, workloadrunner.OutputSink) (workloadrunner.Result, error) {
	err := errors.New("delivery failed before runtime creation")
	return workloadrunner.Result{Outcome: contract.ProcessResult{SpawnError: &contract.SpawnFailure{Code: runtime.code, Message: err.Error()}}}, err
}

func (*preRunFailureRuntime) ReapAndVerify(context.Context, workloadrunner.ReapRequest) (workloadrunner.ReapReceipt, error) {
	return workloadrunner.ReapReceipt{RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidenceNoRuntime}, nil
}

func TestProcessPreflightRejectsBeforeAgentResourceAcquisition(t *testing.T) {
	publishedPort := 8080
	tests := []struct {
		name string
		spec contract.JobSpec
		code contract.SpawnFailureCode
	}{
		{
			name: "process runtime handler", code: contract.SpawnFailureUnsupportedRuntimeHandler,
			spec: contract.JobSpec{
				Kind: contract.JobKindProcess, Class: contract.JobClassService,
				RuntimeHandler: "runc", PublishedPort: &publishedPort,
			},
		},
		{
			name: "inline executable materialization", code: contract.SpawnFailureExecutableMaterialization,
			spec: contract.JobSpec{Kind: contract.JobKindProcess, Class: contract.JobClassOneShot, Execution: contract.ExecutionSpec{
				Executable:       contract.ExecutableSpec{InlineBase64: "%%%", SHA256: strings.Repeat("0", 64)},
				HandoffDirectory: filepath.Join(t.TempDir(), "handoff"),
			}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			executor := &preflightExecutor{}
			managed := &preflightManagedResource{}
			var portReservations, bridges, sinks int
			lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{
				runtimes: workloadRuntimeSet{contract.JobKindProcess: processrunner.NewAdapter(executor)},
				clock:    systemClock{}, nodeID: "node-1", bootSessionID: "boot-1",
				managedResource: managed, handoffs: newHandoffManager(t.TempDir(), 0),
				reservePublishedPort: func(l1.Claim) (net.Listener, *contract.SpawnFailure) {
					portReservations++
					return nil, nil
				},
				workflowBridge: func(context.Context, string, contract.ExecutionSpec) (*workflowBridge, error) {
					bridges++
					return nil, errors.New("must not create workflow bridge")
				},
				outputSinkFactory: func(l1.Claim) processrunner.OutputSink {
					sinks++
					return nil
				},
			})
			claim := l1.Claim{
				Job:   l1.Job{JobID: "job-preflight", Spec: test.spec},
				Lease: l1.AttemptLease{AttemptID: "attempt-preflight", FencingToken: "fence-preflight"},
			}
			result, err := lifecycle.runWorkload(context.Background(), claim)
			if err == nil || result.SpawnError == nil || result.SpawnError.Code != test.code {
				t.Fatalf("preflight result = (%#v, %v), want %s", result, err, test.code)
			}
			if executor.calls != 0 || managed.calls != 0 || portReservations != 0 || bridges != 0 || sinks != 0 {
				t.Fatalf("preflight rejection side effects = executor %d managed %d ports %d bridges %d sinks %d",
					executor.calls, managed.calls, portReservations, bridges, sinks)
			}
			if _, statErr := os.Stat(test.spec.Execution.HandoffDirectory); test.spec.Execution.HandoffDirectory != "" && !os.IsNotExist(statErr) {
				t.Fatalf("preflight rejection created handoff path: %v", statErr)
			}
		})
	}
}

func TestOCIPreflightFailureDoesNotPersistRuntimeManifest(t *testing.T) {
	outbox, err := newEvidenceOutbox(t.TempDir(), "preflight-node", 1024, systemClock{}, 8, time.Hour, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer outbox.Close()
	runtime := &manifestPreflightFailureRuntime{}
	lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{
		runtimes: workloadRuntimeSet{contract.JobKindOCI: runtime}, outbox: outbox,
		clock: systemClock{}, nodeID: "preflight-node", bootSessionID: "preflight-boot",
	})
	claim := l1.Claim{
		Job: l1.Job{JobID: "preflight-job", Spec: contract.JobSpec{
			Kind: contract.JobKindOCI, Class: contract.JobClassService,
		}},
		Lease: l1.AttemptLease{AttemptID: "preflight-attempt", FencingToken: "preflight-fence"},
	}
	result, err := lifecycle.runWorkload(t.Context(), claim)
	if err == nil || result.SpawnError == nil || result.SpawnError.Code != contract.SpawnFailureImageNotFound {
		t.Fatalf("OCI preflight failure = (%+v, %v)", result, err)
	}
	if runtime.manifestCalls != 0 {
		t.Fatalf("preflight failure derived %d removal manifests", runtime.manifestCalls)
	}
	var count int
	if err := outbox.spool.db.QueryRow(`SELECT COUNT(*) FROM runtime_attempt_manifests WHERE attempt_id=?`, claim.Lease.AttemptID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("preflight failure persisted %d runtime manifests", count)
	}
}

type manifestPreflightFailureRuntime struct{ manifestCalls int }

func (*manifestPreflightFailureRuntime) Preflight(_ context.Context, request workloadrunner.Request) (workloadrunner.Admission, workloadrunner.Result, error) {
	err := errors.New("image does not exist")
	return workloadrunner.Admission{Request: request, Release: func() {}}, workloadrunner.Result{
		Outcome: contract.ProcessResult{SpawnError: &contract.SpawnFailure{Code: contract.SpawnFailureImageNotFound, Message: err.Error()}},
	}, err
}

func (*manifestPreflightFailureRuntime) Run(context.Context, workloadrunner.Request, workloadrunner.OutputSink) (workloadrunner.Result, error) {
	panic("Run called after failed preflight")
}

func (*manifestPreflightFailureRuntime) ReapAndVerify(context.Context, workloadrunner.ReapRequest) (workloadrunner.ReapReceipt, error) {
	return workloadrunner.ReapReceipt{RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidenceNoRuntime}, nil
}

func (runtime *manifestPreflightFailureRuntime) RemovalResourceManifest(workloadrunner.Request) (workloadrunner.RuntimeResourceManifest, error) {
	runtime.manifestCalls++
	return workloadrunner.RuntimeResourceManifest{}, nil
}

type reapRefusingRuntime struct {
	runCalls  int
	reapCalls int
	request   workloadrunner.Request
}

type captureRuntime struct {
	request workloadrunner.Request
}

func (runtime *captureRuntime) Preflight(_ context.Context, request workloadrunner.Request) (workloadrunner.Admission, workloadrunner.Result, error) {
	return workloadrunner.Admission{Request: request, Release: func() {}}, workloadrunner.Result{}, nil
}

func (runtime *captureRuntime) Run(_ context.Context, request workloadrunner.Request, _ workloadrunner.OutputSink) (workloadrunner.Result, error) {
	runtime.request = request
	exitCode := 0
	return workloadrunner.Result{Outcome: contract.ProcessResult{ExitCode: &exitCode}}, nil
}

func (*captureRuntime) ReapAndVerify(context.Context, workloadrunner.ReapRequest) (workloadrunner.ReapReceipt, error) {
	return workloadrunner.ReapReceipt{RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidenceAttempt}, nil
}

func (adapter *reapRefusingRuntime) Preflight(_ context.Context, request workloadrunner.Request) (workloadrunner.Admission, workloadrunner.Result, error) {
	return workloadrunner.Admission{Request: request, Release: func() {}}, workloadrunner.Result{}, nil
}

func (runtime *reapRefusingRuntime) Run(_ context.Context, request workloadrunner.Request, _ workloadrunner.OutputSink) (workloadrunner.Result, error) {
	runtime.runCalls++
	runtime.request = request
	exitCode := 0
	return workloadrunner.Result{Outcome: contract.ProcessResult{ExitCode: &exitCode}}, nil
}

func (runtime *reapRefusingRuntime) ReapAndVerify(context.Context, workloadrunner.ReapRequest) (workloadrunner.ReapReceipt, error) {
	runtime.reapCalls++
	return workloadrunner.ReapReceipt{}, nil
}

type finalizationContextRuntime struct {
	reapContextErr error
}

type pullingObservationRuntime struct {
	observer                *lifecycleObserver
	observed                AttemptLifecycleState
	afterPull               AttemptLifecycleState
	startedCallbackPresent  bool
	resolvedCallbackPresent bool
	imageDeadline           time.Time
}

func (runtime *pullingObservationRuntime) Preflight(_ context.Context, request workloadrunner.Request) (workloadrunner.Admission, workloadrunner.Result, error) {
	return workloadrunner.Admission{Request: request, Release: func() {}}, workloadrunner.Result{}, nil
}

func (runtime *pullingObservationRuntime) Run(_ context.Context, request workloadrunner.Request, _ workloadrunner.OutputSink) (workloadrunner.Result, error) {
	request.OCIImagePulling()
	runtime.observed = runtime.observer.snapshot(ClassOccupancy{}, ClassOccupancy{}).Attempts[request.Authority.AttemptID].State
	request.OCIImageReady()
	runtime.afterPull = runtime.observer.snapshot(ClassOccupancy{}, ClassOccupancy{}).Attempts[request.Authority.AttemptID].State
	runtime.startedCallbackPresent = request.OCIStarted != nil
	runtime.resolvedCallbackPresent = request.OCIImageResolved != nil
	runtime.imageDeadline = request.OCIImageDeadline
	exitCode := 0
	return workloadrunner.Result{Outcome: contract.ProcessResult{ExitCode: &exitCode}}, nil
}

func (*pullingObservationRuntime) ReapAndVerify(context.Context, workloadrunner.ReapRequest) (workloadrunner.ReapReceipt, error) {
	return workloadrunner.ReapReceipt{RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidenceAttempt}, nil
}

func (adapter *finalizationContextRuntime) Preflight(_ context.Context, request workloadrunner.Request) (workloadrunner.Admission, workloadrunner.Result, error) {
	return workloadrunner.Admission{Request: request, Release: func() {}}, workloadrunner.Result{}, nil
}

func (*finalizationContextRuntime) Run(context.Context, workloadrunner.Request, workloadrunner.OutputSink) (workloadrunner.Result, error) {
	exitCode := 0
	return workloadrunner.Result{Outcome: contract.ProcessResult{ExitCode: &exitCode}}, nil
}

func (adapter *finalizationContextRuntime) ReapAndVerify(ctx context.Context, _ workloadrunner.ReapRequest) (workloadrunner.ReapReceipt, error) {
	adapter.reapContextErr = ctx.Err()
	return workloadrunner.ReapReceipt{RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidenceAttempt}, adapter.reapContextErr
}

type preflightExecutor struct{ calls int }

func TestRuntimeManagedVolumesCompileClassPolicyBeforeOCIAdapter(t *testing.T) {
	oneshoot := runtimeManagedVolumes(l1.Claim{Job: l1.Job{Spec: contract.JobSpec{
		Kind: contract.JobKindOCI, Class: contract.JobClassOneShot,
		Labels: map[string]string{"run_id": "run-1"},
	}}})
	if len(oneshoot) != 1 || oneshoot[0].Kind != workloadrunner.ManagedVolumeHandoff || oneshoot[0].OwnerKey != "run-1" {
		t.Fatalf("one-shot managed volumes = %+v", oneshoot)
	}
	service := runtimeManagedVolumes(l1.Claim{Job: l1.Job{Spec: contract.JobSpec{Kind: contract.JobKindOCI, Class: contract.JobClassService}}})
	if len(service) != 1 || service[0].Kind != workloadrunner.ManagedVolumeServiceData || service[0].OwnerKey != "" {
		t.Fatalf("service managed volumes = %+v", service)
	}
	if finalized := runtimeManagedVolumesForSuccessfulCompletion(contract.JobSpec{Kind: contract.JobKindOCI, Class: contract.JobClassService}); len(finalized) != 0 {
		t.Fatalf("successful service completion finalized durable data = %+v", finalized)
	}
	computer := runtimeManagedVolumes(l1.Claim{
		Job: l1.Job{Spec: contract.JobSpec{Kind: contract.JobKindOCI, Class: contract.JobClassService, Execution: contract.ExecutionSpec{
			OCI: &contract.OCIExecutionSpec{Computer: &contract.OCIComputerSpec{DiskBytes: 8 << 30}},
		}}},
		ComputerStorage: &l1.ComputerStorageClaim{ComputerID: "computer-1", StorageID: "storage-1", StorageGeneration: 3, IntentRevision: 4},
	})
	if len(computer) != 1 || computer[0].Kind != workloadrunner.ManagedVolumeComputerDisk || computer[0].ComputerStorage == nil ||
		computer[0].ComputerStorage.ComputerID != "computer-1" || computer[0].ComputerStorage.StorageID != "storage-1" ||
		computer[0].ComputerStorage.StorageGeneration != 3 || computer[0].ComputerStorage.DiskBytes != 8<<30 {
		t.Fatalf("Computer managed volume = %+v", computer)
	}
	if volumes := runtimeManagedVolumes(l1.Claim{Job: l1.Job{Spec: contract.JobSpec{Kind: contract.JobKindProcess, Class: contract.JobClassService}}}); len(volumes) != 0 {
		t.Fatalf("process managed volumes = %+v", volumes)
	}
}

func TestRuntimeAttemptEndpointsCompileComputerRolesExactly(t *testing.T) {
	port := 8080
	computer := contract.JobSpec{Kind: contract.JobKindOCI, Class: contract.JobClassService, Execution: contract.ExecutionSpec{
		OCI: &contract.OCIExecutionSpec{Computer: &contract.OCIComputerSpec{}},
	}}
	if endpoints := runtimeAttemptEndpoints(computer); !slices.Equal(endpoints, []string{workloadrunner.AttemptEndpointView, workloadrunner.AttemptEndpointControl}) {
		t.Fatalf("Computer attempt endpoints = %v", endpoints)
	}
	service := contract.JobSpec{Kind: contract.JobKindOCI, Class: contract.JobClassService, PublishedPort: &port}
	if endpoints := runtimeAttemptEndpoints(service); !slices.Equal(endpoints, []string{workloadrunner.AttemptEndpointService}) {
		t.Fatalf("ordinary service endpoints = %v", endpoints)
	}
	if endpoints := runtimeAttemptEndpoints(contract.JobSpec{Kind: contract.JobKindOCI, Class: contract.JobClassService}); len(endpoints) != 0 {
		t.Fatalf("portless service endpoints = %v", endpoints)
	}
}

func TestComputerReservedOperatorEnvironmentIsStrippedBeforeAuthoritativeInjection(t *testing.T) {
	public := map[string]string{
		contract.EnvHandoffDir: "attacker", contract.EnvServiceDir: "attacker", contract.EnvServicePort: "1",
		contract.EnvL3Endpoint: "attacker", contract.EnvComputerViewPort: "2", contract.EnvComputerControlPort: "3",
		"WEFTY_CUSTOM": "preserved",
	}
	sensitive := map[string]string{contract.EnvRunToken: "attacker", contract.EnvComputerToken: "attacker", "TENANT_SECRET": "preserved"}
	got := withoutComputerReservedOperatorEnvironment(contract.ExecutionSpec{Env: public, SensitiveEnv: sensitive})
	for name := range got.Env {
		if contract.IsOCIReservedEnvironmentName(name) {
			t.Fatalf("reserved public operator value %q survived", name)
		}
	}
	for name := range got.SensitiveEnv {
		if contract.IsOCIReservedEnvironmentName(name) {
			t.Fatalf("reserved sensitive operator value %q survived", name)
		}
	}
	if got.Env["WEFTY_CUSTOM"] != "preserved" || got.SensitiveEnv["TENANT_SECRET"] != "preserved" {
		t.Fatalf("non-reserved operator environment changed: public=%v sensitive=%v", got.Env, got.SensitiveEnv)
	}
	if public[contract.EnvComputerViewPort] != "2" || sensitive[contract.EnvComputerToken] != "attacker" {
		t.Fatal("reserved-environment stripping mutated the immutable Job projection")
	}
}

type recordingComputerTokenMinter struct {
	grant l3.ComputerTokenGrant
	err   error
	calls int
}

func (minter *recordingComputerTokenMinter) MintComputerToken(_ context.Context, _ l3.ComputerTokenMintRequest) (l3.ComputerTokenGrant, error) {
	minter.calls++
	return minter.grant, minter.err
}

type captureComputerPreflight struct{ request workloadrunner.Request }

func (runtime *captureComputerPreflight) Preflight(_ context.Context, request workloadrunner.Request) (workloadrunner.Admission, workloadrunner.Result, error) {
	runtime.request = request
	err := errors.New("stop after preflight")
	return workloadrunner.Admission{Request: request}, workloadrunner.Result{Outcome: spawnFailure(contract.SpawnFailureProcessRequest, err)}, err
}

func (*captureComputerPreflight) Run(context.Context, workloadrunner.Request, workloadrunner.OutputSink) (workloadrunner.Result, error) {
	panic("Run must not follow failed preflight")
}

func (*captureComputerPreflight) ReapAndVerify(context.Context, workloadrunner.ReapRequest) (workloadrunner.ReapReceipt, error) {
	return workloadrunner.ReapReceipt{RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidenceNoRuntime}, nil
}

func TestComputerTokenMintedIntoSensitiveClosedInputOnlyWhenEnabled(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "default-off", true: "enabled"}[enabled], func(t *testing.T) {
			runtime := &captureComputerPreflight{}
			minter := &recordingComputerTokenMinter{grant: l3.ComputerTokenGrant{Token: "secret-computer-pass",
				ComputerID: "computer-1", ComputerAttemptID: "attempt-1", ComputerStorageGeneration: 5,
				SubmitIntentRevision: 2, HostNodeID: "node-1", SubmitMaxInflight: 20}}
			lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{
				runtimes: workloadRuntimeSet{contract.JobKindOCI: runtime}, clock: systemClock{},
				nodeID: "node-1", bootSessionID: "boot-1", computerTokens: minter,
				observer: newLifecycleObserver(systemClock{}),
			})
			claim := l1.Claim{Job: l1.Job{JobID: "computer-job", Spec: contract.JobSpec{
				Kind: contract.JobKindOCI, Class: contract.JobClassService,
				Execution: contract.ExecutionSpec{SensitiveEnv: map[string]string{contract.EnvComputerToken: "attacker"},
					OCI: &contract.OCIExecutionSpec{Image: contract.OCIImageSpec{Reference: "example.test/computer:latest"},
						Computer: &contract.OCIComputerSpec{DiskBytes: 8 << 30}}},
			}}, Lease: l1.AttemptLease{AttemptID: "attempt-1", FencingToken: "fence-1", LeaseTTL: time.Second},
				ComputerStorage: &l1.ComputerStorageClaim{ComputerID: "computer-1", StorageID: "storage-1", StorageGeneration: 5,
					IntentRevision: 7, SubmitEnabled: enabled, SubmitIntentRevision: 2, SubmitMaxInflight: 20}}
			_, _ = lifecycle.runWorkload(t.Context(), claim)
			if enabled {
				if minter.calls != 1 || runtime.request.Execution.SensitiveEnv[contract.EnvComputerToken] != "secret-computer-pass" {
					t.Fatalf("enabled Computer pass: calls=%d env=%v", minter.calls, runtime.request.Execution.SensitiveEnv)
				}
			} else if minter.calls != 0 {
				t.Fatalf("default-off Computer minted %d pass(es)", minter.calls)
			} else if _, exists := runtime.request.Execution.SensitiveEnv[contract.EnvComputerToken]; exists {
				t.Fatalf("default-off Computer retained caller token: %v", runtime.request.Execution.SensitiveEnv)
			}
		})
	}
}

func TestComputerTokenMintFailureStopsBeforeRuntime(t *testing.T) {
	runtime := &captureComputerPreflight{}
	minter := &recordingComputerTokenMinter{err: errors.New("L3 unavailable")}
	lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{runtimes: workloadRuntimeSet{contract.JobKindOCI: runtime},
		clock: systemClock{}, nodeID: "node-1", bootSessionID: "boot-1", computerTokens: minter,
		observer: newLifecycleObserver(systemClock{})})
	claim := l1.Claim{Job: l1.Job{JobID: "computer-job", Spec: contract.JobSpec{Kind: contract.JobKindOCI,
		Class: contract.JobClassService, Execution: contract.ExecutionSpec{OCI: &contract.OCIExecutionSpec{
			Image: contract.OCIImageSpec{Reference: "example.test/computer:latest"}, Computer: &contract.OCIComputerSpec{DiskBytes: 8 << 30}}}}},
		Lease: l1.AttemptLease{AttemptID: "attempt-1", FencingToken: "fence-1", LeaseTTL: time.Second},
		ComputerStorage: &l1.ComputerStorageClaim{ComputerID: "computer-1", StorageID: "storage-1", StorageGeneration: 5,
			SubmitEnabled: true, SubmitIntentRevision: 2, SubmitMaxInflight: 20}}
	result, err := lifecycle.runWorkload(t.Context(), claim)
	if err == nil || result.SpawnError == nil || result.SpawnError.Code != contract.SpawnFailurePassUnavailable || minter.calls != 1 {
		t.Fatalf("mint failure = result %#v err %v calls %d", result, err, minter.calls)
	}
	if runtime.request.Authority.AttemptID != "" {
		t.Fatal("runtime preflight ran after Computer token mint failure")
	}
}

func (executor *preflightExecutor) Run(context.Context, processrunner.Request, processrunner.OutputSink) (contract.ProcessResult, error) {
	executor.calls++
	return contract.ProcessResult{}, nil
}

type preflightManagedResource struct{ calls int }

func (*preflightManagedResource) rootInstanceID() string { return "root-preflight" }

func (resource *preflightManagedResource) prepareAttempt(string, string) (managedResourceAttempt, func(), error) {
	resource.calls++
	return managedResourceAttempt{}, func() {}, nil
}

func (*preflightManagedResource) remove(context.Context, localRemoval) error { return nil }

func (*preflightManagedResource) resumeRemovals(context.Context) ([]localRemoval, error) {
	return nil, nil
}
