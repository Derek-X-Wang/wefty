//go:build darwin || linux

package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
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

func TestRunProcessRedactsSensitiveEnvironmentFromLogEvents(t *testing.T) {
	var payload []byte
	a := &Agent{
		runner: emittingRunner{},
		outputSinkFactory: func(l1.Claim) processrunner.OutputSink {
			return processrunner.OutputSinkFunc(func(_ context.Context, event contract.LogEvent) error {
				payload = append(payload, event.Bytes...)
				return nil
			})
		},
	}
	claim := l1.Claim{
		Job: l1.Job{Spec: contract.JobSpec{
			Kind:      "process",
			Execution: contract.ExecutionSpec{SensitiveEnv: map[string]string{contract.EnvRunToken: "wrun_log_secret"}},
		}},
		Lease: l1.AttemptLease{AttemptID: "attempt-redaction"},
	}
	result, err := a.runProcess(context.Background(), claim)
	if err != nil || result.ExitCode == nil || *result.ExitCode != 0 {
		t.Fatalf("runProcess() = (%#v, %v)", result, err)
	}
	if got, want := string(payload), "before [REDACTED] after"; got != want {
		t.Fatalf("redacted log payload = %q, want %q", got, want)
	}
}

func TestRunProcessReportsOutputFinalizationFailureInsteadOfExitZero(t *testing.T) {
	a := &Agent{
		runner: bufferedOutputRunner{},
		outputSinkFactory: func(l1.Claim) processrunner.OutputSink {
			return processrunner.OutputSinkFunc(func(context.Context, contract.LogEvent) error {
				return errors.New("durable output unavailable")
			})
		},
	}
	claim := l1.Claim{
		Job: l1.Job{Spec: contract.JobSpec{
			Kind:      "process",
			Execution: contract.ExecutionSpec{SensitiveEnv: map[string]string{contract.EnvRunToken: "long-secret-value"}},
		}},
		Lease: l1.AttemptLease{AttemptID: "attempt-output-finalization"},
	}
	result, err := a.runProcess(context.Background(), claim)
	if err == nil || !strings.Contains(err.Error(), "flush redacted output") {
		t.Fatalf("runProcess error = %v, want redaction flush failure", err)
	}
	if result.OutputError == "" || result.ExitCode != nil {
		t.Fatalf("runProcess result = %#v, want only output_error", result)
	}
	if completed := toL1Result(result); completed.OutputError == "" || completed.ExitCode != nil {
		t.Fatalf("L1 completion result = %#v, want output failure", completed)
	}
}

func TestNewBuildsRegistrationFromStableAndBootMetadata(t *testing.T) {
	participant := plain.NewNetwork().NewFabric(fabric.Identity{NodeID: "fabric-node", Tags: []string{l1.DefaultAgentPrincipalTag}})
	nodeAgent, err := New(Config{
		Fabric: participant, ControlPlaneAddress: "127.0.0.1:1",
		NodeID: "stable-node", BootSessionID: "boot-session", Version: "v1.2.3",
		OS: "test-os", Architecture: "test-arch",
		LogSpoolDirectory: t.TempDir(),
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
		LogSpoolDirectory:   t.TempDir(),
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

func TestLeaseRenewalContinuesWhileCompletionRetriesPastOriginalExpiry(t *testing.T) {
	network := plain.NewNetwork()
	serverFabric := network.NewFabric(fabric.Identity{NodeID: "control-plane"})
	store, err := l1.OpenStore(filepath.Join(t.TempDir(), "completion-retry.sqlite"), l1.StoreOptions{LeaseDuration: 200 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	l1Server, err := l1.NewServer(serverFabric, store, l1.ServerConfig{AuthoritativeNodeTags: map[string][]string{"stable-node": {"linux"}}})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := serverFabric.Listen("tcp", "wefty://control-plane")
	if err != nil {
		t.Fatal(err)
	}
	var completionAvailable atomic.Bool
	var completionRequests atomic.Int32
	var renewalRequests atomic.Int32
	completionBlocked := make(chan struct{}, 1)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/lease") {
			renewalRequests.Add(1)
		}
		if strings.HasSuffix(r.URL.Path, "/complete") {
			completionRequests.Add(1)
		}
		if strings.HasSuffix(r.URL.Path, "/complete") && !completionAvailable.Load() {
			select {
			case completionBlocked <- struct{}{}:
			default:
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{
				Code: contract.ErrorInternal, Message: "completion temporarily unavailable", Retryable: true,
			}})
			return
		}
		l1Server.Handler().ServeHTTP(w, r)
	})
	httpServer := &http.Server{Handler: handler}
	served := make(chan error, 1)
	go func() { served <- httpServer.Serve(listener) }()
	defer func() {
		_ = httpServer.Close()
		if err := <-served; err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("serve completion retry L1: %v", err)
		}
	}()

	agentFabric := network.NewFabric(fabric.Identity{NodeID: "fabric-node", Tags: []string{l1.DefaultAgentPrincipalTag}})
	nodeAgent, err := New(Config{
		Fabric: agentFabric, ControlPlaneAddress: "wefty://control-plane",
		NodeID: "stable-node", BootSessionID: "boot-completion-retry", Version: "test",
		HeartbeatInterval: time.Second, ClaimInterval: 5 * time.Millisecond,
		RenewalInterval: 40 * time.Millisecond, LogRetryInterval: 10 * time.Millisecond,
		LogSpoolDirectory: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer nodeAgent.Close()
	ctx, cancel := context.WithCancel(context.Background())
	agentDone := make(chan error, 1)
	go func() { agentDone <- nodeAgent.Run(ctx) }()

	directory := t.TempDir()
	truePath, err := exec.LookPath("true")
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	job, _, err := store.CreateJob(context.Background(), contract.JobSpec{
		SchemaVersion: contract.SchemaVersionV1, DispatchKey: "completion-retry", Kind: "process", RoutingTags: []string{"linux"},
		Execution: contract.ExecutionSpec{
			Executable: contract.ExecutableSpec{Path: truePath}, Argv: []string{"true"},
			WorkingDirectory: directory, HandoffDirectory: directory,
		},
	})
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	select {
	case <-completionBlocked:
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("agent did not reach blocked completion")
	}
	time.Sleep(350 * time.Millisecond)
	if _, err := store.Reconcile(context.Background()); err != nil {
		cancel()
		t.Fatal(err)
	}
	stillActive, err := store.GetJob(context.Background(), job.JobID)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	if stillActive.State == contract.JobFailed {
		cancel()
		t.Fatal("attempt expired while completion was retrying")
	}
	// Let the renewal loop establish a fresh lease margin after the explicit
	// reconciliation boundary, rather than releasing completion at the edge of
	// the lease that happened to be current during the check above.
	time.Sleep(80 * time.Millisecond)
	if _, err := store.Reconcile(context.Background()); err != nil {
		cancel()
		t.Fatal(err)
	}
	stillActive, err = store.GetJob(context.Background(), job.JobID)
	if err != nil || stillActive.State == contract.JobFailed {
		cancel()
		t.Fatalf("attempt did not survive the renewed completion-retry window: state %q error %v", stillActive.State, err)
	}
	completionAvailable.Store(true)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		completed, err := store.GetJob(context.Background(), job.JobID)
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		if completed.State == contract.JobSucceeded {
			break
		}
		if completed.State == contract.JobFailed {
			cancel()
			t.Fatalf("job failed after completion became available: current_attempt=%s now=%s renewals=%d completions=%d", completed.CurrentAttemptID, time.Now(), renewalRequests.Load(), completionRequests.Load())
		}
		time.Sleep(10 * time.Millisecond)
	}
	completed, err := store.GetJob(context.Background(), job.JobID)
	if err != nil || completed.State != contract.JobSucceeded {
		cancel()
		t.Fatalf("completion retry result = state %q error %v", completed.State, err)
	}
	cancel()
	if err := <-agentDone; err != nil {
		t.Fatalf("agent Run() = %v", err)
	}
}

type panicRunner struct{}

func (panicRunner) Run(context.Context, processrunner.Request, processrunner.OutputSink) (contract.ProcessResult, error) {
	panic("unsupported kind reached process runner")
}

type emittingRunner struct{}

func (emittingRunner) Run(ctx context.Context, request processrunner.Request, sink processrunner.OutputSink) (contract.ProcessResult, error) {
	if err := sink.WriteOutput(ctx, contract.LogEvent{
		AttemptID: request.AttemptID,
		Stream:    contract.LogStdout,
		Sequence:  0,
		Bytes:     []byte("before wrun_log_"),
	}); err != nil {
		return contract.ProcessResult{}, err
	}
	if err := sink.WriteOutput(ctx, contract.LogEvent{
		AttemptID: request.AttemptID,
		Stream:    contract.LogStdout,
		Sequence:  1,
		Bytes:     []byte("secret after"),
	}); err != nil {
		return contract.ProcessResult{}, err
	}
	exitCode := 0
	return contract.ProcessResult{ExitCode: &exitCode}, nil
}

type bufferedOutputRunner struct{}

func (bufferedOutputRunner) Run(ctx context.Context, request processrunner.Request, sink processrunner.OutputSink) (contract.ProcessResult, error) {
	if err := sink.WriteOutput(ctx, contract.LogEvent{
		AttemptID: request.AttemptID, Stream: contract.LogStdout, Sequence: 0, Bytes: []byte("tail"),
	}); err != nil {
		return contract.ProcessResult{}, err
	}
	exitCode := 0
	return contract.ProcessResult{ExitCode: &exitCode}, nil
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
