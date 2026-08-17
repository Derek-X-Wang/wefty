package agent

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
	processrunner "github.com/Derek-X-Wang/wefty/runner/process"
)

func TestDurableLogSinkReconnectsAcrossControlPlaneRestartWithoutDuplicates(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clock := newManualClock(time.Date(2026, 8, 9, 15, 0, 0, 0, time.UTC))
	network := plain.NewNetwork()
	serverFabric := network.NewFabric(fabric.Identity{NodeID: "control-plane"})
	databasePath := filepath.Join(t.TempDir(), "l1.sqlite")
	store, stopServer := startRestartableLogServer(t, ctx, serverFabric, databasePath, clock)

	claim := createClaimForDurableLogs(t, store, clock)
	participant := network.NewFabric(fabric.Identity{NodeID: "fabric-node", Tags: []string{l1.DefaultAgentPrincipalTag}})
	client, err := NewClient(participant, "wefty://control-plane")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	spool := openTestLogSpool(t, t.TempDir(), "stable-node", 1024)
	defer spool.Close()
	sink, err := newBatchingLogSink(ctx, client, claim, spool, clock, 1, time.Hour, 5*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.WriteOutput(ctx, spoolTestEvent(claim.Lease.AttemptID, contract.LogStdout, 0, "first")); err != nil {
		t.Fatal(err)
	}
	waitForSpoolHighWater(t, spool, claim.Lease.AttemptID, contract.LogStdout, 0)
	firstPage, err := store.GetJobLogs(ctx, claim.Job.JobID, "", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(firstPage.Events) != 1 || firstPage.NextCursor == "" {
		t.Fatalf("first reader page = %#v", firstPage)
	}
	// Reading advances only the opaque reader cursor. It cannot alter the
	// persisted upload acknowledgement maintained by the agent spool.
	waitForSpoolHighWater(t, spool, claim.Lease.AttemptID, contract.LogStdout, 0)

	stopServer()
	if err := sink.WriteOutput(ctx, spoolTestEvent(claim.Lease.AttemptID, contract.LogStdout, 1, "second")); err != nil {
		t.Fatal(err)
	}
	clock.waitForDeadline(t, clock.Now().Add(5*time.Millisecond))
	store, stopServer = startRestartableLogServer(t, ctx, serverFabric, databasePath, clock)
	defer stopServer()
	clock.Advance(5 * time.Millisecond)
	if err := sink.Close(); err != nil {
		t.Fatal(err)
	}

	all, err := store.GetJobLogs(ctx, claim.Job.JobID, "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(all.Events) != 2 || all.Events[0].Sequence != 0 || all.Events[1].Sequence != 1 {
		t.Fatalf("logs after reconnect = %#v, want two unique ordered events", all.Events)
	}
	rest, err := store.GetJobLogs(ctx, claim.Job.JobID, firstPage.NextCursor, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rest.Events) != 1 || rest.Events[0].Sequence != 1 {
		t.Fatalf("reader resume after control-plane restart = %#v", rest.Events)
	}
	waitForSpoolHighWater(t, spool, claim.Lease.AttemptID, contract.LogStdout, 1)
}

func TestAgentCompletesWithoutDuplicateLogsAfterNetworkLossMidJob(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clock := newManualClock(time.Date(2026, 8, 9, 15, 30, 0, 0, time.UTC))
	network := plain.NewNetwork()
	serverFabric := network.NewFabric(fabric.Identity{NodeID: "control-plane"})
	databasePath := filepath.Join(t.TempDir(), "l1.sqlite")
	store, stopServer := startRestartableLogServer(t, ctx, serverFabric, databasePath, clock)
	directory := t.TempDir()
	job, _, err := store.CreateJob(ctx, contract.JobSpec{
		SchemaVersion: contract.SchemaVersionV1, DispatchKey: "network-mid-job", Kind: "process", Class: contract.JobClassOneShot, RoutingTags: []string{"linux"},
		Execution: contract.ExecutionSpec{
			Executable: contract.ExecutableSpec{Path: agentHelperPath},
			Argv:       []string{"processhelper", "paced-output", "200"}, WorkingDirectory: directory, HandoffDirectory: directory,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	firstOutput := make(chan struct{}, 1)
	participant := network.NewFabric(fabric.Identity{NodeID: "fabric-node", Tags: []string{l1.DefaultAgentPrincipalTag}})
	nodeAgent, err := New(Config{
		Fabric: participant, ControlPlaneAddress: "wefty://control-plane",
		NodeID: "stable-node", BootSessionID: "boot-network", Version: "test", Clock: clock,
		ClaimInterval: time.Second, LogBatchSize: 1, LogRetryInterval: 5 * time.Millisecond,
		LogSpoolDirectory: t.TempDir(), LogSpoolMaxBytes: 1024,
		OutputSinkFactory: func(l1.Claim) processrunner.OutputSink {
			return processrunner.OutputSinkFunc(func(_ context.Context, event contract.LogEvent) error {
				if event.Stream == contract.LogStdout && event.Sequence == 0 {
					select {
					case firstOutput <- struct{}{}:
					default:
					}
				}
				return nil
			})
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer nodeAgent.Close()
	agentDone := make(chan error, 1)
	go func() { agentDone <- nodeAgent.Run(ctx) }()
	select {
	case <-firstOutput:
	case <-time.After(5 * time.Second):
		t.Fatal("compiled helper did not produce its first event")
	}
	claimed := waitForJobAttempt(t, store, job.JobID)
	waitForSpoolHighWater(t, nodeAgent.logSpool, claimed, contract.LogStdout, 0)
	stopServer()
	clock.waitForDeadline(t, clock.Now().Add(5*time.Millisecond))
	store, stopServer = startRestartableLogServer(t, ctx, serverFabric, databasePath, clock)
	defer stopServer()
	clock.Advance(5 * time.Millisecond)
	waitForJobState(t, store, job.JobID, contract.JobSucceeded, 5*time.Second)
	page, err := store.GetJobLogs(ctx, job.JobID, "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != 2 || page.Events[0].Sequence != 0 || page.Events[1].Sequence != 1 {
		t.Fatalf("logs after mid-job reconnect = %#v", page.Events)
	}
	cancel()
	select {
	case err := <-agentDone:
		if err != nil {
			t.Fatalf("agent Run() = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("agent did not stop")
	}
}

func TestAgentRestartRecoversOriginalAttemptSpool(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clock := newManualClock(time.Date(2026, 8, 9, 16, 0, 0, 0, time.UTC))
	network := plain.NewNetwork()
	serverFabric := network.NewFabric(fabric.Identity{NodeID: "control-plane"})
	databasePath := filepath.Join(t.TempDir(), "l1.sqlite")
	store, stopServer := startRestartableLogServer(t, ctx, serverFabric, databasePath, clock)
	defer stopServer()
	claim := createClaimForDurableLogs(t, store, clock)

	spoolDirectory := t.TempDir()
	spool := openTestLogSpool(t, spoolDirectory, "stable-node", 1024)
	if err := spool.ensureAttempt(ctx, claim); err != nil {
		t.Fatal(err)
	}
	if err := spool.append(ctx, spoolTestEvent(claim.Lease.AttemptID, contract.LogStdout, 0, "before-crash")); err != nil {
		t.Fatal(err)
	}
	if err := spool.Close(); err != nil {
		t.Fatal(err)
	}

	participant := network.NewFabric(fabric.Identity{NodeID: "fabric-node", Tags: []string{l1.DefaultAgentPrincipalTag}})
	restarted, err := New(Config{
		Fabric: participant, ControlPlaneAddress: "wefty://control-plane",
		NodeID: "stable-node", BootSessionID: "boot-2", Version: "test",
		Clock: clock, LogSpoolDirectory: spoolDirectory, LogSpoolMaxBytes: 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	if err := restarted.recoverPendingLogs(ctx); err != nil {
		t.Fatal(err)
	}
	node, err := restarted.Register(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if node.BootSessionID != "boot-2" {
		t.Fatalf("restarted boot session = %q", node.BootSessionID)
	}
	page, err := store.GetJobLogs(ctx, claim.Job.JobID, "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != 1 || page.Events[0].AttemptID != claim.Lease.AttemptID || string(page.Events[0].Bytes) != "before-crash" {
		t.Fatalf("recovered events = %#v", page.Events)
	}
	if err := restarted.recoverPendingLogs(ctx); err != nil {
		t.Fatal(err)
	}
	replayed, err := store.GetJobLogs(ctx, claim.Job.JobID, "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(replayed.Events) != 1 {
		t.Fatalf("restart replay duplicated logs: %#v", replayed.Events)
	}
}

func startRestartableLogServer(t *testing.T, parent context.Context, serverFabric fabric.Fabric, databasePath string, clock l1.Clock) (*l1.Store, func()) {
	t.Helper()
	store, err := l1.OpenStore(databasePath, l1.StoreOptions{Clock: clock, LeaseDuration: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	server, err := l1.NewServer(serverFabric, store, l1.ServerConfig{NodePolicies: map[string]l1.NodePolicy{"stable-node": l1.DefaultNodePolicy("linux")}})
	if err != nil {
		store.Close()
		t.Fatal(err)
	}
	listener, err := serverFabric.Listen("tcp", "wefty://control-plane")
	if err != nil {
		store.Close()
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(parent)
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx, listener) }()
	return store, func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("serve restartable L1: %v", err)
		}
		if err := store.Close(); err != nil {
			t.Errorf("close restartable L1: %v", err)
		}
	}
}

func createClaimForDurableLogs(t *testing.T, store *l1.Store, clock Clock) l1.Claim {
	t.Helper()
	_, err := store.RegisterNode(context.Background(), fabric.Identity{NodeID: "fabric-node"}, contract.NodeRegistration{
		NodeID: "stable-node", BootSessionID: "boot-1", OS: "linux", Architecture: "arm64", AgentVersion: "test",
	}, l1.DefaultNodePolicy("linux"))
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	job, _, err := store.CreateJob(context.Background(), contract.JobSpec{
		SchemaVersion: contract.SchemaVersionV1, DispatchKey: "durable-" + directory, Kind: "process", Class: contract.JobClassOneShot, RoutingTags: []string{"linux"},
		Execution: contract.ExecutionSpec{
			Executable: contract.ExecutableSpec{Path: "/bin/true"}, Argv: []string{"true"},
			WorkingDirectory: directory, HandoffDirectory: directory,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimJob(context.Background(), "fabric-node", "stable-node", "boot-1")
	if err != nil {
		t.Fatal(err)
	}
	if claim == nil || claim.Job.JobID != job.JobID {
		t.Fatalf("claim = %#v, want job %q", claim, job.JobID)
	}
	return *claim
}

func waitForSpoolHighWater(t *testing.T, spool *logSpool, attemptID string, stream contract.LogStream, want uint64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		got, present, err := spool.highWater(context.Background(), attemptID, stream)
		if err != nil {
			t.Fatal(err)
		}
		if present && got == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s acknowledgement %d", stream, want)
}

func waitForJobAttempt(t *testing.T, store *l1.Store, jobID string) string {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		job, err := store.GetJob(context.Background(), jobID)
		if err != nil {
			t.Fatal(err)
		}
		if job.CurrentAttemptID != "" {
			return job.CurrentAttemptID
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("timed out waiting for job attempt")
	return ""
}
