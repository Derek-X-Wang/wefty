package agent

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	processrunner "github.com/Derek-X-Wang/wefty/runner/process"
)

func TestPublishedPortFailureComesFromFabricNamespace(t *testing.T) {
	assertPublishedPortFailureComesFromFabricNamespace(t)
}

func TestServiceCancellationClosesPublicationBeforeSignalingGuardian(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	runner := &cancellationOrderRunner{started: make(chan struct{}), listenerAddress: listener.Addr().String()}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, runErr := runPortfulService(
			ctx,
			testProcessRuntime(runner),
			workloadRequest("cancellation-order"),
			nil,
			listener,
			serviceRuntimeEndpoint{dial: func(context.Context) (net.Conn, error) {
				return nil, errors.New("backend should not be dialed")
			}},
			serviceSupervisorConfig{},
		)
		done <- runErr
	}()
	<-runner.started
	cancel()
	if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if runner.listenerWasOpen {
		t.Fatal("guardian cancellation became visible before the Fabric listener closed")
	}
}

func TestPublishedListenerFailureHasDedicatedCode(t *testing.T) {
	listener := &failedAcceptListener{err: errors.New("listener failed")}
	runner := &publicationAuthorityRunner{canceled: make(chan struct{})}
	result, err := runPortfulService(
		context.Background(),
		testProcessRuntime(runner),
		workloadRequest("published-listener-failure"),
		nil,
		listener,
		serviceRuntimeEndpoint{
			address: "127.0.0.1:1",
			dial: func(context.Context) (net.Conn, error) {
				return nil, errors.New("not reached")
			},
		},
		serviceSupervisorConfig{},
	)
	if err == nil {
		t.Fatal("published listener failure returned nil error")
	}
	if result.SpawnError == nil || result.SpawnError.Code != contract.SpawnFailurePublishedListener {
		t.Fatalf("published listener result = %#v, want dedicated failure code", result)
	}
	select {
	case <-runner.canceled:
	default:
		t.Fatal("published listener failure did not stop the guardian-owned payload")
	}
}

type failedAcceptListener struct {
	err error
}

func (listener *failedAcceptListener) Accept() (net.Conn, error) { return nil, listener.err }
func (*failedAcceptListener) Close() error                       { return nil }
func (*failedAcceptListener) Addr() net.Addr                     { return failedAcceptAddr("failed") }

type failedAcceptAddr string

func (address failedAcceptAddr) Network() string { return "test" }
func (address failedAcceptAddr) String() string  { return string(address) }

type cancellationOrderRunner struct {
	started         chan struct{}
	listenerAddress string
	listenerWasOpen bool
}

func (runner *cancellationOrderRunner) Run(
	ctx context.Context,
	_ processrunner.Request,
	_ processrunner.OutputSink,
) (contract.ProcessResult, error) {
	close(runner.started)
	<-ctx.Done()
	connection, err := net.DialTimeout("tcp4", runner.listenerAddress, 100*time.Millisecond)
	if err == nil {
		runner.listenerWasOpen = true
		_ = connection.Close()
	}
	return contract.ProcessResult{Signal: "terminated", TerminationCause: contract.TerminationCauseGuardian}, ctx.Err()
}

func assertPublishedPortFailureComesFromFabricNamespace(t *testing.T) {
	t.Helper()
	port := unusedHostPort(t)
	participant := &rejectPublishedListenerFabric{err: errors.New("Fabric namespace already reserved")}
	nodeAgent := &Agent{
		fabric:       participant,
		registration: contract.NodeRegistration{NodeID: "node-fabric-reservation"},
	}
	claim := l1.Claim{Job: l1.Job{Spec: contract.JobSpec{
		Class: contract.JobClassService, PublishedPort: &port,
	}}}
	listener, failure := nodeAgent.reservePublishedPort(claim)
	if listener != nil || failure == nil || failure.Code != contract.SpawnFailurePublishedPortOccupied {
		t.Fatalf("reservePublishedPort() = (%v, %#v), want Fabric collision", listener, failure)
	}
	if participant.listenCalls.Load() != 1 {
		t.Fatalf("Fabric Listen calls = %d, want 1", participant.listenCalls.Load())
	}
	if strings.Contains(failure.Message, participant.err.Error()) {
		t.Fatalf("public failure leaked Fabric implementation error %q", failure.Message)
	}

	// The ordinary host socket is free. A host-port preflight would have
	// passed and therefore cannot be the source of this terminal result.
	hostListener, err := net.Listen("tcp4", net.JoinHostPort("127.0.0.1", portString(port)))
	if err != nil {
		t.Fatalf("host port %d was unexpectedly occupied: %v", port, err)
	}
	_ = hostListener.Close()
}

func TestPortlessServiceSkipsFabricProxyProbeAndDeadline(t *testing.T) {
	assertPortlessServiceSkipsFabricProxyProbeAndDeadline(t)
}

func TestPortlessOCIServiceReceivesOnlyReservedContainerDirectory(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	resource, err := initializeManagedResource(root, "portless-oci-node", "portless-oci-boot")
	if err != nil {
		t.Fatal(err)
	}
	runtime := &opaqueEndpointRuntime{release: make(chan struct{})}
	close(runtime.release)
	lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{
		runtimes: workloadRuntimeSet{contract.JobKindOCI: runtime}, managedResource: resource,
		clock: systemClock{}, observer: newLifecycleObserver(systemClock{}),
		nodeID: "portless-oci-node", bootSessionID: "portless-oci-boot",
	})
	digest := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	claim := l1.Claim{
		Job: l1.Job{JobID: "portless-oci-job", Spec: contract.JobSpec{
			Kind: contract.JobKindOCI, Class: contract.JobClassService,
			Execution: contract.ExecutionSpec{OCI: &contract.OCIExecutionSpec{
				Image: contract.OCIImageSpec{Reference: "example.invalid/portless:v1", Digest: &digest},
				Argv:  []string{"/payload", "--portless"},
			}},
		}},
		Lease: l1.AttemptLease{AttemptID: "portless-oci-attempt", FencingToken: "portless-oci-fence", LeaseTTL: time.Minute},
	}
	result, err := lifecycle.runWorkload(context.Background(), claim)
	if err != nil || result.ExitCode == nil || *result.ExitCode != 0 {
		t.Fatalf("portless OCI runWorkload() = (%#v, %v)", result, err)
	}
	if got := runtime.request.Execution.Env[contract.EnvServiceDir]; got != contract.OCIContainerServiceDirectory {
		t.Fatalf("%s = %q, want reserved container path %q", contract.EnvServiceDir, got, contract.OCIContainerServiceDirectory)
	}
	if _, exists := runtime.request.Execution.Env[contract.EnvServicePort]; exists {
		t.Fatalf("portless OCI request received %s", contract.EnvServicePort)
	}
}

func assertPortlessServiceSkipsFabricProxyProbeAndDeadline(t *testing.T) {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	resource, err := initializeManagedResource(root, "portless-node", "portless-boot")
	if err != nil {
		t.Fatal(err)
	}
	runner := &capturingStartedRunner{}
	lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{
		runtimes: testRuntimeSet(runner), managedResource: resource, clock: systemClock{}, observer: newLifecycleObserver(systemClock{}),
		reservePublishedPort: func(l1.Claim) (net.Listener, *contract.SpawnFailure) {
			t.Fatal("portless service called Fabric.Listen")
			return nil, nil
		},
		prepareServiceEndpoint: func(context.Context) (serviceRuntimeEndpoint, error) {
			t.Fatal("portless service prepared a runtime-local endpoint")
			return serviceRuntimeEndpoint{}, nil
		},
	})
	claim := l1.Claim{
		Job: l1.Job{JobID: "portless-job", Spec: contract.JobSpec{
			Kind: "process", Class: contract.JobClassService,
			Execution: contract.ExecutionSpec{
				Executable: contract.ExecutableSpec{Path: "/bin/true"}, Argv: []string{"true"},
				WorkingDirectory: t.TempDir(),
			},
		}},
		Lease: l1.AttemptLease{AttemptID: "portless-attempt"},
	}
	result, err := lifecycle.runWorkload(context.Background(), claim)
	if err != nil || result.ExitCode == nil || *result.ExitCode != 0 {
		t.Fatalf("portless runWorkload() = (%#v, %v)", result, err)
	}
	if _, exists := runner.request.Execution.Env[contract.EnvServicePort]; exists {
		t.Fatalf("portless request received %s", contract.EnvServicePort)
	}
	if !runner.request.Guarded {
		t.Fatal("service lifecycle policy did not request the process Guardian boundary")
	}
}

func TestPostStartupProbeLossWithdrawsAndRecoversWithoutKilling(t *testing.T) {
	assertPostStartupProbeLossWithdrawsAndRecoversWithoutKilling(t)
}

func TestOpaqueRuntimeEndpointDrivesReadinessAndFabricForwarding(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	backend, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	go serveTestEcho(backend)

	runtime := &opaqueEndpointRuntime{release: make(chan struct{})}
	dialer := &net.Dialer{}
	finished := make(chan serviceRunOutcome, 1)
	go func() {
		result, runErr := runPortfulService(
			context.Background(), runtime, workloadRequest("opaque-endpoint"), nil, listener,
			serviceRuntimeEndpoint{dial: func(ctx context.Context) (net.Conn, error) {
				return dialer.DialContext(ctx, "tcp4", backend.Addr().String())
			}},
			serviceSupervisorConfig{},
		)
		finished <- serviceRunOutcome{result: result, err: runErr}
	}()

	waitForPublishedEcho(t, listener.Addr().String(), true)
	close(runtime.release)
	outcome := waitServiceOutcome(t, finished)
	if outcome.err != nil || outcome.result.ExitCode == nil || *outcome.result.ExitCode != 0 {
		t.Fatalf("opaque endpoint outcome = (%#v, %v)", outcome.result, outcome.err)
	}
}

func TestOpaqueRuntimeEndpointStartupTimeoutStopsUnpublishedPayload(t *testing.T) {
	clock := newManualClock(time.Unix(1_700_000_000, 0))
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	runtime := &opaqueEndpointRuntime{release: make(chan struct{})}
	finished := make(chan serviceRunOutcome, 1)
	go func() {
		result, runErr := runPortfulService(
			context.Background(), runtime, workloadRequest("opaque-timeout"), nil, listener,
			serviceRuntimeEndpoint{dial: func(context.Context) (net.Conn, error) {
				return nil, errors.New("backend is not ready")
			}},
			serviceSupervisorConfig{
				clock: clock, startupReadinessDeadline: 10 * time.Second,
				readinessProbeInterval: time.Second, readinessConnectTimeout: time.Second,
			},
		)
		finished <- serviceRunOutcome{result: result, err: runErr}
	}()

	clock.waitForDeadline(t, clock.Now().Add(10*time.Second))
	clock.Advance(10 * time.Second)
	outcome := waitServiceOutcome(t, finished)
	if outcome.err == nil || outcome.result.SpawnError == nil || outcome.result.SpawnError.Code != contract.SpawnFailureStartupReadinessTimeout {
		t.Fatalf("opaque startup timeout outcome = (%#v, %v)", outcome.result, outcome.err)
	}
	assertPublishedEcho(t, listener.Addr().String(), false)
}

func TestOpaqueRuntimeTunnelLossWithdrawsAndRepublishesWithoutKilling(t *testing.T) {
	clock := newManualClock(time.Unix(1_700_000_000, 0))
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	backend, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	go serveTestEcho(backend)

	var available atomic.Bool
	available.Store(true)
	runtime := &opaqueEndpointRuntime{release: make(chan struct{})}
	dialer := &net.Dialer{}
	finished := make(chan serviceRunOutcome, 1)
	go func() {
		result, runErr := runPortfulService(
			context.Background(), runtime, workloadRequest("opaque-republication"), nil, listener,
			serviceRuntimeEndpoint{dial: func(ctx context.Context) (net.Conn, error) {
				if !available.Load() {
					return nil, errors.New("helper tunnel unavailable")
				}
				return dialer.DialContext(ctx, "tcp4", backend.Addr().String())
			}},
			serviceSupervisorConfig{
				clock: clock, startupReadinessDeadline: 10 * time.Second,
				readinessProbeInterval: time.Second, readinessConnectTimeout: time.Second,
				publicationRecoveryWindow: 10 * time.Second,
			},
		)
		finished <- serviceRunOutcome{result: result, err: runErr}
	}()

	waitForPublishedEcho(t, listener.Addr().String(), true)
	available.Store(false)
	clock.waitForDeadline(t, clock.Now().Add(time.Second))
	clock.Advance(time.Second)
	waitForPublishedEcho(t, listener.Addr().String(), false)
	select {
	case outcome := <-finished:
		t.Fatalf("tunnel loss killed payload: (%#v, %v)", outcome.result, outcome.err)
	default:
	}

	available.Store(true)
	clock.waitForDeadline(t, clock.Now().Add(time.Second))
	clock.Advance(time.Second)
	clock.waitForDeadline(t, clock.Now().Add(10*time.Second))
	clock.Advance(10 * time.Second)
	waitForPublishedEcho(t, listener.Addr().String(), true)
	close(runtime.release)
	outcome := waitServiceOutcome(t, finished)
	if outcome.err != nil || outcome.result.ExitCode == nil || *outcome.result.ExitCode != 0 {
		t.Fatalf("republished opaque endpoint outcome = (%#v, %v)", outcome.result, outcome.err)
	}
}

func TestPublicationAuthorityLossStopsAttemptThroughGuardian(t *testing.T) {
	assertPublicationAuthorityLossStopsAttemptThroughGuardian(t)
}

func assertPublicationAuthorityLossStopsAttemptThroughGuardian(t *testing.T) {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	runner := &publicationAuthorityRunner{canceled: make(chan struct{})}
	result, err := runPortfulService(
		context.Background(),
		testProcessRuntime(runner),
		workloadRequest("authority-loss"),
		nil,
		listener,
		serviceRuntimeEndpoint{
			address: "127.0.0.1:1",
			dial: func(context.Context) (net.Conn, error) {
				return nil, errors.New("not reached")
			},
		},
		serviceSupervisorConfig{
			clock: systemClock{},
			publish: func(context.Context, bool) error {
				return &ProtocolError{
					StatusCode: http.StatusConflict,
					APIError:   contract.APIError{Code: contract.ErrorAttemptMismatch},
				}
			},
		},
	)
	var routed *routedDestinationError
	if !errors.As(err, &routed) || routed.destination != errorDestinationAttemptAuthority {
		t.Fatalf("publication authority error = %v, want attempt-authority", err)
	}
	if result.TerminationCause != contract.TerminationCauseGuardian {
		t.Fatalf("termination cause = %q, want guardian", result.TerminationCause)
	}
	select {
	case <-runner.canceled:
	default:
		t.Fatal("publication authority loss did not cancel the guardian-owned attempt")
	}
}

func assertPostStartupProbeLossWithdrawsAndRecoversWithoutKilling(t *testing.T) {
	t.Helper()
	clock := newManualClock(time.Unix(1_700_000_000, 0))
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	backend, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	go serveTestEcho(backend)
	dialer := &net.Dialer{}
	runner := newReadinessRunner()
	type readinessState struct{ startupSatisfied, ready bool }
	states := make(chan readinessState, 4)
	finished := make(chan serviceRunOutcome, 1)
	go func() {
		result, err := runPortfulService(
			context.Background(), testProcessRuntime(runner),
			workloadRequest("readiness-recovery"), nil,
			listener, serviceRuntimeEndpoint{
				address: backend.Addr().String(), dial: func(ctx context.Context) (net.Conn, error) {
					return dialer.DialContext(ctx, "tcp4", backend.Addr().String())
				},
			},
			serviceSupervisorConfig{
				clock: clock,
				onReadiness: func(startupSatisfied, ready bool) {
					states <- readinessState{startupSatisfied: startupSatisfied, ready: ready}
				},
			},
		)
		finished <- serviceRunOutcome{result: result, err: err}
	}()
	runner.waitStarted(t)
	runner.publishReadiness(t, true)
	first := waitReadinessState(t, states)
	if !first.startupSatisfied || !first.ready {
		t.Fatalf("first local probe state = %#v, want startup satisfied and ready", first)
	}
	waitForPublishedEcho(t, listener.Addr().String(), true)

	runner.publishReadiness(t, false)
	lost := waitReadinessState(t, states)
	if !lost.startupSatisfied || lost.ready {
		t.Fatalf("lost readiness state = %#v, want monotonic startup and ready=false", lost)
	}
	assertPublishedEcho(t, listener.Addr().String(), false)
	select {
	case <-runner.canceled:
		t.Fatal("readiness probe killed the payload after startup")
	default:
	}

	runner.publishReadiness(t, true)
	recovered := waitReadinessState(t, states)
	if !recovered.startupSatisfied || !recovered.ready {
		t.Fatalf("recovered readiness state = %#v, want republished local forwarding", recovered)
	}
	assertPublishedEcho(t, listener.Addr().String(), false)
	clock.waitForDeadline(t, clock.Now().Add(DefaultPublicationRecoveryWindow))
	clock.Advance(DefaultPublicationRecoveryWindow)
	waitForPublishedEcho(t, listener.Addr().String(), true)
	close(runner.release)
	outcome := waitServiceOutcome(t, finished)
	if outcome.err != nil || outcome.result.ExitCode == nil || *outcome.result.ExitCode != 0 {
		t.Fatalf("recovered service outcome = (%#v, %v)", outcome.result, outcome.err)
	}
}

func unusedHostPort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return port
}

func portString(port int) string {
	return fmt.Sprintf("%d", port)
}

type rejectPublishedListenerFabric struct {
	err         error
	listenCalls atomic.Int32
}

func (participant *rejectPublishedListenerFabric) Listen(string, string) (net.Listener, error) {
	participant.listenCalls.Add(1)
	return nil, participant.err
}

func (*rejectPublishedListenerFabric) Dial(context.Context, string, string) (net.Conn, error) {
	return nil, errors.New("not implemented")
}

func (*rejectPublishedListenerFabric) WhoIs(context.Context, string) (fabric.Identity, error) {
	return fabric.Identity{}, errors.New("not implemented")
}

func (*rejectPublishedListenerFabric) ConnectHost() string { return "127.0.0.1" }

type capturingStartedRunner struct{ request processrunner.Request }

type opaqueEndpointRuntime struct {
	release chan struct{}
	request workloadrunner.Request
}

func (runtime *opaqueEndpointRuntime) Preflight(_ context.Context, request workloadrunner.Request) (workloadrunner.Admission, workloadrunner.Result, error) {
	return workloadrunner.Admission{Request: request, Release: func() {}}, workloadrunner.Result{}, nil
}

func (runtime *opaqueEndpointRuntime) Run(ctx context.Context, request workloadrunner.Request, _ workloadrunner.OutputSink) (workloadrunner.Result, error) {
	runtime.request = request
	if request.Started != nil {
		request.Started()
	}
	select {
	case <-ctx.Done():
		return workloadrunner.Result{}, ctx.Err()
	case <-runtime.release:
		exitCode := 0
		return workloadrunner.Result{Outcome: contract.ProcessResult{ExitCode: &exitCode}}, nil
	}
}

func (*opaqueEndpointRuntime) ReapAndVerify(context.Context, workloadrunner.ReapRequest) (workloadrunner.ReapReceipt, error) {
	return workloadrunner.ReapReceipt{RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidenceAttempt}, nil
}

func (runner *capturingStartedRunner) Run(_ context.Context, request processrunner.Request, _ processrunner.OutputSink) (contract.ProcessResult, error) {
	runner.request = request
	if request.Started != nil {
		request.Started()
	}
	exitCode := 0
	return contract.ProcessResult{ExitCode: &exitCode}, nil
}

type readinessRunner struct {
	started   chan struct{}
	canceled  chan struct{}
	readiness chan bool
	release   chan struct{}
	startOnce sync.Once
}

type publicationAuthorityRunner struct {
	canceled chan struct{}
}

func (runner *publicationAuthorityRunner) Run(
	ctx context.Context,
	request processrunner.Request,
	_ processrunner.OutputSink,
) (contract.ProcessResult, error) {
	if request.Started != nil {
		request.Started()
	}
	request.ReadinessChanged(true, true)
	<-ctx.Done()
	close(runner.canceled)
	return contract.ProcessResult{
		Signal: "terminated", TerminationCause: contract.TerminationCauseGuardian,
	}, ctx.Err()
}

func newReadinessRunner() *readinessRunner {
	return &readinessRunner{
		started: make(chan struct{}), canceled: make(chan struct{}), readiness: make(chan bool), release: make(chan struct{}),
	}
}

func (runner *readinessRunner) Run(ctx context.Context, request processrunner.Request, _ processrunner.OutputSink) (contract.ProcessResult, error) {
	if request.Started != nil {
		request.Started()
	}
	runner.startOnce.Do(func() { close(runner.started) })
	for range 3 {
		select {
		case <-ctx.Done():
			close(runner.canceled)
			return contract.ProcessResult{Signal: "terminated", TerminationCause: contract.TerminationCauseGuardian}, ctx.Err()
		case ready := <-runner.readiness:
			request.ReadinessChanged(true, ready)
		}
	}
	select {
	case <-ctx.Done():
		close(runner.canceled)
		return contract.ProcessResult{Signal: "terminated", TerminationCause: contract.TerminationCauseGuardian}, ctx.Err()
	case <-runner.release:
	}
	exitCode := 0
	return contract.ProcessResult{ExitCode: &exitCode}, nil
}

func serveTestEcho(listener net.Listener) {
	for {
		connection, err := listener.Accept()
		if err != nil {
			return
		}
		go func() {
			defer connection.Close()
			buffer := make([]byte, 1)
			if _, err := connection.Read(buffer); err == nil {
				_, _ = connection.Write(buffer)
			}
		}()
	}
}

func assertPublishedEcho(t *testing.T, address string, wantForwarded bool) {
	t.Helper()
	connection, err := net.DialTimeout("tcp4", address, time.Second)
	if err != nil {
		if wantForwarded {
			t.Fatalf("dial published listener: %v", err)
		}
		return
	}
	defer connection.Close()
	_ = connection.SetDeadline(time.Now().Add(time.Second))
	if _, err := connection.Write([]byte{'x'}); err != nil {
		if wantForwarded {
			t.Fatalf("write published listener: %v", err)
		}
		return
	}
	buffer := make([]byte, 1)
	_, err = connection.Read(buffer)
	forwarded := err == nil && buffer[0] == 'x'
	if forwarded != wantForwarded {
		t.Fatalf("published forwarding = %v (read error %v), want %v", forwarded, err, wantForwarded)
	}
}

func waitForPublishedEcho(t *testing.T, address string, wantForwarded bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if publishedEchoForwarded(address) == wantForwarded {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("published forwarding did not become %v", wantForwarded)
}

func publishedEchoForwarded(address string) bool {
	connection, err := net.DialTimeout("tcp4", address, 50*time.Millisecond)
	if err != nil {
		return false
	}
	defer connection.Close()
	_ = connection.SetDeadline(time.Now().Add(50 * time.Millisecond))
	if _, err := connection.Write([]byte{'x'}); err != nil {
		return false
	}
	buffer := make([]byte, 1)
	_, err = connection.Read(buffer)
	return err == nil && buffer[0] == 'x'
}

func (runner *readinessRunner) waitStarted(t *testing.T) {
	t.Helper()
	select {
	case <-runner.started:
	case <-time.After(5 * time.Second):
		t.Fatal("runtime did not report successful spawn")
	}
}

func (runner *readinessRunner) publishReadiness(t *testing.T, ready bool) {
	t.Helper()
	select {
	case runner.readiness <- ready:
	case <-time.After(5 * time.Second):
		t.Fatal("runtime did not accept a readiness transition")
	}
}

func waitServiceOutcome(t *testing.T, outcomes <-chan serviceRunOutcome) serviceRunOutcome {
	t.Helper()
	select {
	case outcome := <-outcomes:
		return outcome
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for supervised service outcome")
		return serviceRunOutcome{}
	}
}

func waitReadinessState[T any](t *testing.T, states <-chan T) T {
	t.Helper()
	select {
	case state := <-states:
		return state
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for local readiness transition")
		var zero T
		return zero
	}
}
