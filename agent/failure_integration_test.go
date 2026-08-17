//go:build darwin || linux

package agent

import (
	"context"
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

func TestPartitionedAgentLosesAuthorityWithoutSecondExecution(t *testing.T) {
	clock := newManualClock(time.Date(2026, 8, 9, 14, 0, 0, 0, time.UTC))
	network := plain.NewNetwork()
	store, stopServer := startFailureServer(t, network, clock, map[string][]string{
		"node-1": {"linux"}, "node-2": {"linux"},
	})
	defer stopServer()

	job := createAgentTestJob(t, store, "partitioned-agent")
	runner := newBlockingRunner()
	agentFabric := network.NewFabric(fabric.Identity{NodeID: "fabric-node-1", Tags: []string{l1.DefaultAgentPrincipalTag}})
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
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- nodeAgent.Run(ctx) }()
	runner.waitStarted(t)
	clock.waitForDeadline(t, clock.Now().Add(10*time.Second))

	clock.Advance(30 * time.Second)
	if _, err := store.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "renew lease") {
			t.Fatalf("agent Run() = %v, want lease-authority failure", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("partitioned agent did not stop after losing lease authority")
	}
	if runner.starts.Load() != 1 {
		t.Fatalf("process starts = %d, want 1", runner.starts.Load())
	}
	completed, err := store.GetJob(context.Background(), job.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if completed.State != contract.JobFailed {
		t.Fatalf("expired job state = %q, want failed", completed.State)
	}

	_, err = store.RegisterNode(context.Background(), fabric.Identity{NodeID: "fabric-node-2"}, contract.NodeRegistration{
		NodeID: "node-2", BootSessionID: "boot-2", OS: "linux", Architecture: "arm64", AgentVersion: "test",
	}, l1.DefaultNodePolicy("linux"))
	if err != nil {
		t.Fatal(err)
	}
	secondClaim, err := store.ClaimJob(context.Background(), "fabric-node-2", "node-2", "boot-2")
	if err != nil {
		t.Fatal(err)
	}
	if secondClaim != nil {
		t.Fatalf("expired job was executed a second time: %#v", secondClaim)
	}
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
	timers []*manualTimer
}

func newManualClock(now time.Time) *manualClock { return &manualClock{now: now} }

func (clock *manualClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
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
