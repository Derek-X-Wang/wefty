package main

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/agent"
	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	limarunner "github.com/Derek-X-Wang/wefty/runner/lima"
	"github.com/Derek-X-Wang/wefty/runner/ocicontrol"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

type capabilityProbeAdapterStub struct {
	err   error
	calls *int
}

func TestSecondShutdownSignalCancelsWhileGracefulDrainIsStillJoining(t *testing.T) {
	ctx, cancelContext := context.WithCancel(t.Context())
	defer cancelContext()
	signals := make(chan os.Signal, 2)
	drainEntered := make(chan struct{})
	releaseDrain := make(chan struct{})
	canceled := make(chan struct{})
	logs := make(chan string, 1)
	done := make(chan struct{})
	go func() {
		handleShutdownSignals(ctx, signals, func(context.Context) error {
			close(drainEntered)
			<-releaseDrain
			return nil
		}, func() { close(canceled) }, func(format string, _ ...any) { logs <- format })
		close(done)
	}()
	signals <- os.Interrupt
	<-drainEntered
	signals <- os.Interrupt
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("second shutdown signal remained blocked behind graceful drain")
	}
	select {
	case line := <-logs:
		if !strings.Contains(line, "forced_shutdown") || !strings.Contains(line, "second_signal") {
			t.Fatalf("forced shutdown evidence = %q", line)
		}
	case <-time.After(time.Second):
		t.Fatal("second shutdown signal emitted no typed forced-shutdown evidence")
	}
	close(releaseDrain)
	<-done
}

func TestSingleShutdownSignalDrainsToCompletionWithoutForcingCancellation(t *testing.T) {
	ctx, cancelContext := context.WithCancel(t.Context())
	signals := make(chan os.Signal, 1)
	drained := make(chan struct{})
	canceled := make(chan struct{}, 1)
	done := make(chan struct{})
	go func() {
		handleShutdownSignals(ctx, signals, func(context.Context) error {
			close(drained)
			return nil
		}, func() { canceled <- struct{}{} }, func(string, ...any) {})
		close(done)
	}()
	signals <- os.Interrupt
	select {
	case <-drained:
	case <-time.After(time.Second):
		t.Fatal("single shutdown signal did not complete graceful drain")
	}
	select {
	case <-canceled:
		t.Fatal("single shutdown signal forced cancellation after successful drain")
	case <-time.After(20 * time.Millisecond):
	}
	cancelContext()
	<-done
}

func TestSecondShutdownSignalAfterDrainStillEmitsForcedShutdownEvidence(t *testing.T) {
	ctx, cancelContext := context.WithCancel(t.Context())
	defer cancelContext()
	signals := make(chan os.Signal, 2)
	drained := make(chan struct{})
	canceled := make(chan struct{})
	logs := make(chan string, 1)
	go handleShutdownSignals(ctx, signals, func(context.Context) error {
		close(drained)
		return nil
	}, func() { close(canceled) }, func(format string, _ ...any) { logs <- format })
	signals <- os.Interrupt
	<-drained
	signals <- os.Interrupt
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("post-drain second signal did not cancel")
	}
	select {
	case line := <-logs:
		if !strings.Contains(line, "forced_shutdown") || !strings.Contains(line, "second_signal") {
			t.Fatalf("forced shutdown evidence = %q", line)
		}
	case <-time.After(time.Second):
		t.Fatal("post-drain second signal emitted no typed evidence")
	}
}

type readyMainTestOCIBootBarrier struct{}

func (readyMainTestOCIBootBarrier) Ready() bool                  { return true }
func (readyMainTestOCIBootBarrier) Ensure(context.Context) error { return nil }
func (readyMainTestOCIBootBarrier) Generation() (ocihelper.HelperSession, bool) {
	return ocihelper.HelperSession{HelperInstanceID: "main-test", SessionGeneration: 1}, true
}
func (readyMainTestOCIBootBarrier) SweepReceipt() (ocihelper.VerifiedSweepReceipt, bool) {
	generation, _ := (readyMainTestOCIBootBarrier{}).Generation()
	return ocihelper.VerifiedSweepReceipt{SweepEpoch: "main-test", HelperSession: generation}, true
}
func (readyMainTestOCIBootBarrier) SetLossHandler(func(ocihelper.HelperSession, error)) {}
func (readyMainTestOCIBootBarrier) Close() error                                        { return nil }

type atomicCapabilityProbeAdapterStub struct{ calls atomic.Int32 }

func (stub *atomicCapabilityProbeAdapterStub) Probe(context.Context, string, string, string, string, time.Duration) error {
	stub.calls.Add(1)
	return nil
}

func (stub capabilityProbeAdapterStub) Probe(context.Context, string, string, string, string, time.Duration) error {
	if stub.calls != nil {
		*stub.calls++
	}
	return stub.err
}

func TestOCIProbePublishesComputerOnlyAfterExactHelperProbe(t *testing.T) {
	for _, test := range []struct {
		name         string
		probeErr     error
		wantComputer bool
		wantErr      bool
	}{
		{name: "supporting helper", wantComputer: true},
		{name: "lost helper", probeErr: errors.New("helper session lost"), wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			probe := ociCapabilityProbe{adapter: capabilityProbeAdapterStub{err: test.probeErr}}
			result, err := probe.Probe(t.Context())
			if (err != nil) != test.wantErr {
				t.Fatalf("probe error = %v, want error %t", err, test.wantErr)
			}
			if result.Capabilities["computer"] != test.wantComputer {
				t.Fatalf("computer capability = %t, want %t in %+v", result.Capabilities["computer"], test.wantComputer, result)
			}
			if test.wantErr && len(result.MissingCapabilities) != 1 {
				t.Fatalf("lost helper result = %+v", result)
			}
		})
	}
}

func TestOCIProbeFailsClosedBeforeAdapterWhenIntentIsUnavailable(t *testing.T) {
	for _, test := range []struct {
		name   string
		intent limarunner.IntentSource
	}{
		{name: "missing", intent: limarunner.FileIntentSource{Path: filepath.Join(t.TempDir(), "missing.json")}},
		{name: "disabled", intent: limarunner.IntentSourceFunc(func(context.Context) (limarunner.OCIIntent, error) {
			return limarunner.OCIIntent{Version: limarunner.OCIIntentVersion, Revision: 2, Enabled: false, UpdatedAt: time.Now()}, nil
		})},
		{name: "malformed", intent: limarunner.IntentSourceFunc(func(context.Context) (limarunner.OCIIntent, error) {
			return limarunner.OCIIntent{}, errors.New("invalid marker")
		})},
	} {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			probe := ociCapabilityProbe{adapter: capabilityProbeAdapterStub{calls: &calls}, intent: test.intent}
			result, err := probe.Probe(t.Context())
			if err == nil || calls != 0 || result.ReasonCode != contract.CapabilityReasonOCIIntentDisabled {
				t.Fatalf("result=%+v calls=%d err=%v", result, calls, err)
			}
		})
	}
}

func TestDurableOCIIntentGatesBackgroundRecoveryUntilControllerStart(t *testing.T) {
	intentPath := filepath.Join(t.TempDir(), "oci-intent.json")
	if _, err := limarunner.InitializeOCIIntent(intentPath, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := limarunner.SetOCIIntent(t.Context(), intentPath, 1, false, time.Now()); err != nil {
		t.Fatal(err)
	}

	network := plain.NewNetwork()
	store, err := l1.OpenStore(filepath.Join(t.TempDir(), "l1.sqlite"), l1.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	serverFabric := network.NewFabric(fabric.Identity{NodeID: "control-plane"})
	server, err := l1.NewServer(serverFabric, store, l1.ServerConfig{NodePolicies: map[string]l1.NodePolicy{
		"intent-node": {MaxOneshotSlots: 1, MaxServiceSlots: 1},
	}})
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	listener, err := serverFabric.Listen("tcp", "wefty://control-plane")
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	serverContext, cancelServer := context.WithCancel(t.Context())
	serverDone := make(chan error, 1)
	go func() { serverDone <- server.Serve(serverContext, listener) }()
	defer func() {
		cancelServer()
		if err := <-serverDone; err != nil {
			t.Errorf("serve L1: %v", err)
		}
		if err := store.Close(); err != nil {
			t.Errorf("close L1: %v", err)
		}
	}()

	probeAdapter := &atomicCapabilityProbeAdapterStub{}
	probe := ociCapabilityProbe{
		adapter: probeAdapter, nodeID: "intent-node", bootSessionID: "intent-boot",
		reference: "example.invalid/probe:v1", digest: "sha256:" + strings.Repeat("a", 64),
		intent: limarunner.FileIntentSource{Path: intentPath},
	}
	agentFabric := network.NewFabric(fabric.Identity{NodeID: "intent-agent", Tags: []string{l1.DefaultAgentPrincipalTag}})
	managedRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	nodeAgent, err := agent.New(agent.Config{
		Fabric: agentFabric, ControlPlaneAddress: "wefty://control-plane",
		NodeID: "intent-node", BootSessionID: "intent-boot", Version: "test", OS: "linux", Architecture: "amd64",
		Capabilities: map[string]bool{"kind:process": true, "kind:oci": true}, CapabilityProbe: probe, OCIIntent: probe.intentObservation,
		OCIBootBarrier: readyMainTestOCIBootBarrier{}, HeartbeatInterval: 10 * time.Millisecond, ClaimInterval: time.Second,
		ManagedRootDirectory: managedRoot, LogSpoolDirectory: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer nodeAgent.Close()
	agentContext, cancelAgent := context.WithCancel(t.Context())
	agentDone := make(chan error, 1)
	go func() { agentDone <- nodeAgent.Run(agentContext) }()
	waitForMainTestNode(t, store, "intent-node", func(node l1.Node) bool {
		return !node.Capabilities["kind:oci"] && node.CapabilityReasonCode == contract.CapabilityReasonOCIIntentDisabled
	})

	convergence := make(chan error, 4)
	for range 4 {
		go func() { convergence <- nodeAgent.RecoverOCIRuntimeCapabilities(t.Context()) }()
	}
	for range 4 {
		if err := <-convergence; err == nil {
			cancelAgent()
			t.Fatal("background convergence recovered across durable disabled OCI intent")
		}
	}
	time.Sleep(50 * time.Millisecond)
	disabled := waitForMainTestNode(t, store, "intent-node", func(node l1.Node) bool {
		return !node.Capabilities["kind:oci"] && node.CapabilityReasonCode == contract.CapabilityReasonOCIIntentDisabled
	})
	if probeAdapter.calls.Load() != 0 {
		cancelAgent()
		t.Fatalf("disabled OCI intent reached adapter probe: node=%+v calls=%d", disabled, probeAdapter.calls.Load())
	}

	controller, err := ocicontrol.NewController(ocicontrol.ControllerConfig{IntentPath: intentPath, Runtime: nodeAgent})
	if err != nil {
		cancelAgent()
		t.Fatal(err)
	}
	started, err := controller.Start(t.Context(), ocicontrol.IntentMutationRequest{ExpectedRevision: 2})
	if err != nil || !started.Intent.Enabled || !started.CapabilityPublished {
		cancelAgent()
		t.Fatalf("explicit OCI start=%+v err=%v cause=%v agent_status=%+v capability=%+v probe_calls=%d",
			started, err, errors.Unwrap(err), nodeAgent.Status(), nodeAgent.CapabilitySnapshot(), probeAdapter.calls.Load())
	}
	waitForMainTestNode(t, store, "intent-node", func(node l1.Node) bool { return node.Capabilities["kind:oci"] })
	if probeAdapter.calls.Load() == 0 {
		cancelAgent()
		t.Fatal("explicit OCI start did not reach adapter probe")
	}
	cancelAgent()
	if err := <-agentDone; err != nil {
		t.Fatalf("agent shutdown: %v", err)
	}
}

func TestControllerStopWaitsForRealInFlightCompletionRequest(t *testing.T) {
	intentPath := filepath.Join(t.TempDir(), "oci-intent.json")
	if _, err := limarunner.InitializeOCIIntent(intentPath, time.Now()); err != nil {
		t.Fatal(err)
	}
	intentSource := limarunner.FileIntentSource{Path: intentPath}
	network := plain.NewNetwork()
	store, err := l1.OpenStore(filepath.Join(t.TempDir(), "l1.sqlite"), l1.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	serverFabric := network.NewFabric(fabric.Identity{NodeID: "control-plane"})
	l1Server, err := l1.NewServer(serverFabric, store, l1.ServerConfig{NodePolicies: map[string]l1.NodePolicy{
		"controller-fence-node": {MaxServiceSlots: 1},
	}})
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	requestEntered := make(chan struct{})
	releaseRequest := make(chan struct{})
	var enteredOnce, releaseOnce sync.Once
	handler := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if strings.HasSuffix(request.URL.Path, "/complete") {
			enteredOnce.Do(func() { close(requestEntered) })
			<-releaseRequest
		}
		l1Server.Handler().ServeHTTP(w, request)
	})
	listener, err := serverFabric.Listen("tcp", "wefty://control-plane")
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	httpServer := &http.Server{Handler: handler}
	serverDone := make(chan error, 1)
	go func() { serverDone <- httpServer.Serve(listener) }()
	defer func() {
		_ = httpServer.Close()
		if serveErr := <-serverDone; serveErr != nil && serveErr != http.ErrServerClosed {
			t.Errorf("serve L1: %v", serveErr)
		}
		if closeErr := store.Close(); closeErr != nil {
			t.Errorf("close L1: %v", closeErr)
		}
	}()
	digest := "sha256:" + strings.Repeat("a", 64)
	if _, _, err := store.CreateJob(t.Context(), contract.JobSpec{
		SchemaVersion: contract.SchemaVersionV1, DispatchKey: "controller-fence-service",
		Kind: contract.JobKindOCI, Class: contract.JobClassService, Restart: contract.RestartAlways,
		Execution: contract.ExecutionSpec{OCI: &contract.OCIExecutionSpec{
			Image: contract.OCIImageSpec{Reference: "example.invalid/controller-fence:v1", Digest: &digest},
			Argv:  []string{"/payload"},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	agentFabric := network.NewFabric(fabric.Identity{NodeID: "controller-fence-agent", Tags: []string{l1.DefaultAgentPrincipalTag}})
	managedRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	nodeAgent, err := agent.New(agent.Config{
		Fabric: agentFabric, ControlPlaneAddress: "wefty://control-plane",
		NodeID: "controller-fence-node", BootSessionID: "controller-fence-boot", Version: "test", OS: "linux", Architecture: "amd64",
		Capabilities: map[string]bool{"kind:process": true}, CapabilityProbe: staticMainOCIProbe{},
		OCIIntent: func(ctx context.Context) (agent.OCIIntentObservation, error) {
			intent, readErr := intentSource.ReadIntent(ctx)
			return agent.OCIIntentObservation{Enabled: intent.Enabled, Revision: intent.Revision}, readErr
		},
		OCIBootBarrier: readyMainTestOCIBootBarrier{},
		WorkloadRuntimes: map[string]agent.WorkloadRuntime{
			contract.JobKindOCI: immediateMainOCIRuntime{},
		},
		HeartbeatInterval: 20 * time.Millisecond, ClaimInterval: 5 * time.Millisecond, RenewalInterval: 50 * time.Millisecond,
		ManagedRootDirectory: managedRoot, LogSpoolDirectory: t.TempDir(), MaxServiceSlots: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	runContext, cancelRun := context.WithCancel(t.Context())
	agentDone := make(chan error, 1)
	go func() { agentDone <- nodeAgent.Run(runContext) }()
	defer func() {
		releaseOnce.Do(func() { close(releaseRequest) })
		cancelRun()
		if runErr := <-agentDone; runErr != nil {
			t.Errorf("agent run: %v", runErr)
		}
		nodeAgent.Close()
	}()
	select {
	case <-requestEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("completion request did not enter L1")
	}
	observedRuntime := &stopObservingAgentRuntime{Agent: nodeAgent, stopEntered: make(chan struct{})}
	controller, err := ocicontrol.NewController(ocicontrol.ControllerConfig{IntentPath: intentPath, Runtime: observedRuntime})
	if err != nil {
		t.Fatal(err)
	}
	type stopResult struct {
		response ocicontrol.IntentResponse
		err      error
	}
	stopDone := make(chan stopResult, 1)
	go func() {
		response, stopErr := controller.Stop(t.Context(), ocicontrol.IntentMutationRequest{ExpectedRevision: 1})
		stopDone <- stopResult{response: response, err: stopErr}
	}()
	deadline := time.Now().Add(time.Second)
	for {
		intent, readErr := intentSource.ReadIntent(t.Context())
		if readErr != nil {
			t.Fatal(readErr)
		}
		if !intent.Enabled && intent.Revision == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("controller did not durably disable intent: %+v", intent)
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case <-observedRuntime.stopEntered:
		t.Fatal("StopOCIRuntime overtook the in-flight completion")
	case result := <-stopDone:
		t.Fatalf("controller stop returned before completion: %+v err=%v", result.response, result.err)
	case <-time.After(50 * time.Millisecond):
	}
	releaseOnce.Do(func() { close(releaseRequest) })
	select {
	case result := <-stopDone:
		if result.err != nil || !result.response.RuntimeQuiesced || result.response.Intent.Revision != 2 {
			t.Fatalf("controller stop=%+v err=%v", result.response, result.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("controller stop did not finish after completion returned")
	}
	select {
	case <-observedRuntime.stopEntered:
	default:
		t.Fatal("StopOCIRuntime was not reached after completion returned")
	}
}

type staticMainOCIProbe struct{}

func (staticMainOCIProbe) Probe(context.Context) (agent.CapabilityProbeResult, error) {
	return agent.CapabilityProbeResult{Capabilities: map[string]bool{"kind:oci": true}}, nil
}

type immediateMainOCIRuntime struct{}

func (immediateMainOCIRuntime) Preflight(_ context.Context, request workloadrunner.Request) (workloadrunner.Admission, workloadrunner.Result, error) {
	return workloadrunner.Admission{Request: request, Release: func() {}}, workloadrunner.Result{}, nil
}

func (immediateMainOCIRuntime) Run(context.Context, workloadrunner.Request, workloadrunner.OutputSink) (workloadrunner.Result, error) {
	exitCode := 7
	return workloadrunner.Result{Outcome: contract.ProcessResult{ExitCode: &exitCode}}, nil
}

func (immediateMainOCIRuntime) ReapAndVerify(context.Context, workloadrunner.ReapRequest) (workloadrunner.ReapReceipt, error) {
	return workloadrunner.ReapReceipt{RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidenceAttempt}, nil
}

type stopObservingAgentRuntime struct {
	*agent.Agent
	stopEntered chan struct{}
	stopOnce    sync.Once
}

func (runtime *stopObservingAgentRuntime) StopOCIRuntime(ctx context.Context) error {
	runtime.stopOnce.Do(func() { close(runtime.stopEntered) })
	return runtime.Agent.StopOCIRuntime(ctx)
}

func waitForMainTestNode(t *testing.T, store *l1.Store, nodeID string, predicate func(l1.Node) bool) l1.Node {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		nodes, err := store.ListNodes(t.Context())
		if err == nil {
			for _, node := range nodes {
				if node.NodeID == nodeID && predicate(node) {
					return node
				}
			}
		}
		time.Sleep(time.Millisecond)
	}
	nodes, _ := store.ListNodes(t.Context())
	t.Fatalf("node %q did not reach capability predicate: %+v", nodeID, nodes)
	return l1.Node{}
}

func TestConsoleOutputIsAttributedAndSerialized(t *testing.T) {
	assertConsoleOutputIsAttributedAndSerialized(t)
}

func assertConsoleOutputIsAttributedAndSerialized(t *testing.T) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	claimOne := l1.Claim{Job: l1.Job{JobID: "job-one"}, Lease: l1.AttemptLease{AttemptID: "attempt-one"}}
	claimTwo := l1.Claim{Job: l1.Job{JobID: "job-two"}, Lease: l1.AttemptLease{AttemptID: "attempt-two"}}
	sinkOne := newConsoleOutputSink(&stdout, &stderr, claimOne)
	sinkTwo := newConsoleOutputSink(&stdout, &stderr, claimTwo)
	done := make(chan error, 2)
	go func() {
		done <- sinkOne.WriteOutput(context.Background(), contract.LogEvent{Stream: contract.LogStdout, Bytes: []byte("alpha\n")})
	}()
	go func() {
		done <- sinkTwo.WriteOutput(context.Background(), contract.LogEvent{Stream: contract.LogStdout, Bytes: []byte("beta\n")})
	}()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	one := "[job=job-one attempt=attempt-one stream=stdout] alpha\n"
	two := "[job=job-two attempt=attempt-two stream=stdout] beta\n"
	if got := stdout.String(); got != one+two && got != two+one {
		t.Fatalf("concurrent console output = %q, want two intact attributed records", got)
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}
