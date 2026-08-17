//go:build darwin || linux

package agent

import (
	"context"
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
	processrunner "github.com/Derek-X-Wang/wefty/runner/process"
)

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
		HeartbeatInterval: 5 * time.Minute, ClaimInterval: time.Minute, RenewalInterval: 10 * time.Second,
		Runner: runner, LogSpoolDirectory: t.TempDir(), Logf: t.Logf,
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

func TestAgentClaimsOneAttemptPerClassConcurrently(t *testing.T) {
	network := plain.NewNetwork()
	store, stopServer := startFailureServer(t, network, nil, map[string][]string{"node-1": {"linux"}})
	defer stopServer()
	createAgentTestJob(t, store, "concurrent-one-shot")
	serviceDirectory := t.TempDir()
	if _, _, err := store.CreateJob(context.Background(), contract.JobSpec{
		SchemaVersion: contract.SchemaVersionV1,
		DispatchKey:   "concurrent-service",
		Kind:          "process",
		Class:         contract.JobClassService,
		Restart:       contract.RestartAlways,
		RoutingTags:   []string{"linux"},
		Execution: contract.ExecutionSpec{
			Executable:       contract.ExecutableSpec{Path: "/bin/true"},
			Argv:             []string{"true"},
			WorkingDirectory: serviceDirectory,
		},
	}); err != nil {
		t.Fatal(err)
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
		HeartbeatInterval: time.Second, ClaimInterval: 10 * time.Millisecond, RenewalInterval: 100 * time.Millisecond,
		Runner: runner, LogSpoolDirectory: t.TempDir(), ManagedRootDirectory: managedRoot,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer nodeAgent.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- nodeAgent.Run(ctx) }()

	deadline := time.Now().Add(5 * time.Second)
	for runner.starts.Load() != 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if runner.starts.Load() != 2 {
		cancel()
		t.Fatalf("concurrent runner starts = %d, want one one-shot and one service", runner.starts.Load())
	}
	status := nodeAgent.Status()
	if status.OneShot.Occupied != 1 || status.OneShot.Limit != prefactorClassLimit ||
		status.Services.Occupied != 1 || status.Services.Limit != prefactorClassLimit {
		cancel()
		t.Fatalf("class occupancy = one-shot %#v service %#v, want 1/1 each", status.OneShot, status.Services)
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
		HeartbeatInterval: 5 * time.Minute, ClaimInterval: time.Minute,
		RenewalInterval: 100 * time.Millisecond, OperationTimeout: 5 * time.Second,
		Runner: runner, LogSpoolDirectory: t.TempDir(),
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
	if occupancy := nodeAgent.Status().OneShot; occupancy.Occupied != 1 || occupancy.Limit != prefactorClassLimit {
		cancel()
		t.Fatalf("running one-shot occupancy = %#v, want 1/%d", occupancy, prefactorClassLimit)
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
		HeartbeatInterval: 5 * time.Minute, ClaimInterval: time.Minute, RenewalInterval: 10 * time.Second,
		Runner: runner, LogSpoolDirectory: t.TempDir(),
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
		HeartbeatInterval: time.Minute, ClaimInterval: time.Millisecond, RenewalInterval: 10 * time.Second,
		Runner: runner, LogSpoolDirectory: t.TempDir(),
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

func startFailureServer(t *testing.T, network *plain.Network, clock l1.Clock, nodeTags map[string][]string) (*l1.Store, func()) {
	t.Helper()
	serverFabric := network.NewFabric(fabric.Identity{NodeID: "control-plane"})
	options := l1.StoreOptions{LeaseDuration: 30 * time.Second}
	if clock != nil {
		options.Clock = clock
	}
	store, err := l1.OpenStore(filepath.Join(t.TempDir(), "failure.sqlite"), options)
	if err != nil {
		t.Fatal(err)
	}
	policies := make(map[string]l1.NodePolicy, len(nodeTags))
	for nodeID, tags := range nodeTags {
		policies[nodeID] = l1.DefaultNodePolicy(tags...)
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

type resilienceRunner struct {
	mu       sync.Mutex
	starts   map[string]int
	started  chan string
	canceled chan string
}

type stubbornRunner struct {
	started  chan struct{}
	canceled chan struct{}
	releaseC chan struct{}
}

func newStubbornRunner() *stubbornRunner {
	return &stubbornRunner{started: make(chan struct{}), canceled: make(chan struct{}), releaseC: make(chan struct{})}
}

func (runner *stubbornRunner) Run(ctx context.Context, _ processrunner.Request, _ processrunner.OutputSink) (contract.ProcessResult, error) {
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

func (runner *blockingRunner) Run(ctx context.Context, _ processrunner.Request, _ processrunner.OutputSink) (contract.ProcessResult, error) {
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
