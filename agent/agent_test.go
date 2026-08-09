//go:build darwin || linux

package agent

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
	processrunner "github.com/Derek-X-Wang/wefty/runner/process"
)

var (
	agentHelperPath  string
	agentBinaryPath  string
	controlPlanePath string
)

func TestMain(main *testing.M) {
	directory, err := os.MkdirTemp("", "wefty-agent-test-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	agentHelperPath = filepath.Join(directory, "processhelper")
	agentBinaryPath = filepath.Join(directory, "wefty-agent")
	controlPlanePath = filepath.Join(directory, "wefty-l1")
	for _, build := range []struct {
		name, output, pkg string
	}{
		{name: "process helper", output: agentHelperPath, pkg: "github.com/Derek-X-Wang/wefty/runner/process/testdata/processhelper"},
		{name: "agent", output: agentBinaryPath, pkg: "github.com/Derek-X-Wang/wefty/cmd/wefty-agent"},
		{name: "control plane", output: controlPlanePath, pkg: "github.com/Derek-X-Wang/wefty/cmd/wefty-l1"},
	} {
		command := exec.Command("go", "build", "-o", build.output, build.pkg)
		if output, buildErr := command.CombinedOutput(); buildErr != nil {
			fmt.Fprintf(os.Stderr, "build %s: %v\n%s", build.name, buildErr, output)
			os.Exit(1)
		}
	}
	code := main.Run()
	if err := os.RemoveAll(directory); err != nil && code == 0 {
		fmt.Fprintln(os.Stderr, err)
		code = 1
	}
	os.Exit(code)
}

func TestRunProcessRejectsUnknownKind(t *testing.T) {
	a := &Agent{runner: panicRunner{}}
	result, err := a.runProcess(context.Background(), l1.Claim{Job: l1.Job{Spec: contract.JobSpec{Kind: "oci"}}})
	var executionError *contract.ExecutionError
	if !errors.As(err, &executionError) {
		t.Fatalf("runProcess() error = %v, want ExecutionError", err)
	}
	if executionError.Code() != contract.ErrorUnsupportedKind || result.SpawnError == "" {
		t.Fatalf("error/result = %q/%#v", executionError.Code(), result)
	}
}

func TestNewBuildsRegistrationFromStableAndBootMetadata(t *testing.T) {
	participant := plain.NewNetwork().NewFabric(fabric.Identity{NodeID: "fabric-node", Tags: []string{l1.DefaultAgentPrincipalTag}})
	nodeAgent, err := New(Config{
		Fabric: participant, ControlPlaneAddress: "127.0.0.1:1",
		NodeID: "stable-node", BootSessionID: "boot-session", Version: "v1.2.3",
		OS: "test-os", Architecture: "test-arch",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer nodeAgent.Close()
	got := nodeAgent.registration
	if got.NodeID != "stable-node" || got.BootSessionID != "boot-session" || got.OS != "test-os" || got.Architecture != "test-arch" || got.AgentVersion != "v1.2.3" {
		t.Fatalf("registration = %#v", got)
	}
	if len(got.Capabilities) != 1 || !got.Capabilities["process"] {
		t.Fatalf("capabilities = %#v", got.Capabilities)
	}
}

func TestShortLeaseRenewalKeepsLongProcessAlive(t *testing.T) {
	network := plain.NewNetwork()
	serverFabric := network.NewFabric(fabric.Identity{NodeID: "control-plane"})
	store, err := l1.OpenStore(filepath.Join(t.TempDir(), "l1.sqlite"), l1.StoreOptions{LeaseDuration: 200 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	server, err := l1.NewServer(serverFabric, store, l1.ServerConfig{AuthoritativeNodeTags: map[string][]string{"stable-node": {"linux"}}})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := serverFabric.Listen("tcp", "wefty://control-plane")
	if err != nil {
		t.Fatal(err)
	}
	serverContext, stopServer := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- server.Serve(serverContext, listener) }()
	defer func() {
		stopServer()
		if err := <-served; err != nil {
			t.Errorf("serve: %v", err)
		}
	}()

	agentFabric := network.NewFabric(fabric.Identity{NodeID: "fabric-node", Tags: []string{l1.DefaultAgentPrincipalTag}})
	nodeAgent, err := New(Config{
		Fabric:              agentFabric,
		ControlPlaneAddress: "wefty://control-plane",
		NodeID:              "stable-node",
		BootSessionID:       "boot-renewal",
		Version:             "test",
		HeartbeatInterval:   5 * time.Second,
		ClaimInterval:       10 * time.Millisecond,
		RenewalInterval:     40 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer nodeAgent.Close()
	agentContext, stopAgent := context.WithCancel(context.Background())
	agentDone := make(chan error, 1)
	go func() { agentDone <- nodeAgent.Run(agentContext) }()

	workingDirectory := t.TempDir()
	job, _, err := store.CreateJob(context.Background(), contract.JobSpec{
		SchemaVersion: contract.SchemaVersionV1,
		DispatchKey:   "short-lease",
		Kind:          "process",
		RoutingTags:   []string{"linux"},
		Execution: contract.ExecutionSpec{
			Executable:       contract.ExecutableSpec{Path: agentHelperPath},
			Argv:             []string{"processhelper", "sleep", "800"},
			WorkingDirectory: workingDirectory,
			HandoffDirectory: workingDirectory,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	completed := waitForJobState(t, store, job.JobID, contract.JobSucceeded, 5*time.Second)
	if elapsed := time.Since(started); elapsed < 600*time.Millisecond {
		t.Fatalf("job completed in %s; helper did not outlive the initial lease", elapsed)
	}
	if completed.State != contract.JobSucceeded {
		t.Fatalf("job state = %q", completed.State)
	}
	stopAgent()
	if err := <-agentDone; err != nil {
		t.Fatalf("agent Run() = %v", err)
	}
}

type panicRunner struct{}

func (panicRunner) Run(context.Context, processrunner.Request, processrunner.OutputSink) (contract.ProcessResult, error) {
	panic("unsupported kind reached process runner")
}

func waitForJobState(t *testing.T, store *l1.Store, jobID string, state contract.JobState, timeout time.Duration) l1.Job {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		job, err := store.GetJob(context.Background(), jobID)
		if err != nil {
			t.Fatal(err)
		}
		if job.State == state {
			return job
		}
		if job.State == contract.JobFailed {
			t.Fatalf("job failed while waiting for %q", state)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for job %q state %q", jobID, state)
	return l1.Job{}
}

func newHTTPClient(participant fabric.Fabric, address string) *http.Client {
	return &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
		return participant.Dial(ctx, network, address)
	}}}
}
