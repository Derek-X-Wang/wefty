package agent

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
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

type reapRefusingRuntime struct {
	runCalls  int
	reapCalls int
	request   workloadrunner.Request
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
