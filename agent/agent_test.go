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

func TestAgentTakesStableNodeLockBeforeOpeningSpool(t *testing.T) {
	assertAgentTakesStableNodeLockBeforeOpeningSpool(t)
}

func assertAgentTakesStableNodeLockBeforeOpeningSpool(t *testing.T) {
	t.Helper()
	directory := t.TempDir()
	network := plain.NewNetwork()
	newAgent := func(bootSessionID string) (*Agent, error) {
		return New(Config{
			Fabric:              network.NewFabric(fabric.Identity{NodeID: "fabric-" + bootSessionID}),
			ControlPlaneAddress: "wefty://control-plane",
			NodeID:              "stable-node", BootSessionID: bootSessionID, Version: "test",
			LogSpoolDirectory: directory,
		})
	}

	first, err := newAgent("boot-1")
	if err != nil {
		t.Fatal(err)
	}
	second, err := newAgent("boot-2")
	if second != nil {
		second.Close()
		t.Fatal("second agent acquired the same stable-node lock")
	}
	if err == nil || !strings.Contains(err.Error(), "already active") {
		t.Fatalf("second New error = %v, want stable-node lock refusal", err)
	}
	claim := spoolTestClaim("attempt-takeover")
	if err := first.logSpool.ensureAttempt(context.Background(), claim); err != nil {
		t.Fatal(err)
	}
	if err := first.logSpool.append(context.Background(), spoolTestEvent(claim.Lease.AttemptID, contract.LogStdout, 0, "pending")); err != nil {
		t.Fatal(err)
	}
	first.Close()

	restarted, err := newAgent("boot-3")
	if err != nil {
		t.Fatalf("lock did not release with the first agent: %v", err)
	}
	pending, err := restarted.logSpool.pending(context.Background(), claim.Lease.AttemptID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || string(pending[0].Bytes) != "pending" {
		t.Fatalf("takeover changed pending evidence: %#v", pending)
	}
	if _, acknowledged, err := restarted.logSpool.highWater(context.Background(), claim.Lease.AttemptID, contract.LogStdout); err != nil || acknowledged {
		t.Fatalf("takeover acknowledgement = (present=%t, err=%v), want absent", acknowledged, err)
	}
	restarted.Close()
}

func TestRunProcessRejectsUnknownKind(t *testing.T) {
	a := &Agent{runner: panicRunner{}}
	result, err := a.runProcess(context.Background(), l1.Claim{Job: l1.Job{Spec: contract.JobSpec{Kind: "oci", Class: contract.JobClassOneShot}}})
	var executionError *contract.ExecutionError
	if !errors.As(err, &executionError) {
		t.Fatalf("runProcess() error = %v, want ExecutionError", err)
	}
	if executionError.Code() != contract.ErrorUnsupportedKind || result.SpawnError == nil || result.SpawnError.Code != contract.SpawnFailureUnsupportedKind {
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
			Class:     contract.JobClassOneShot,
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
			Class:     contract.JobClassOneShot,
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

func TestConsoleMirrorFailurePolicyDependsOnWorkloadClass(t *testing.T) {
	assertConsoleMirrorFailurePolicyByClass(t)
}

func assertConsoleMirrorFailurePolicyByClass(t *testing.T) {
	t.Helper()
	mirrorErr := errors.New("console unavailable")
	// #83 made a managed resource mandatory for service jobs, so this test
	// needs a real managed root to reach the console-mirror policy it is
	// actually about.
	// EvalSymlinks because the guardrails refuse a symlink anywhere in the
	// managed ancestry, and macOS TempDir sits under /var -> /private/var.
	resolvedRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	managedResource, err := initializeManagedResource(resolvedRoot, "console-mirror-node", "boot-console-mirror")
	if err != nil {
		t.Fatal(err)
	}
	newAgent := func(logs *[]string) *Agent {
		return &Agent{
			runner:          emittingRunner{},
			managedResource: managedResource,
			outputSinkFactory: func(l1.Claim) processrunner.OutputSink {
				return processrunner.OutputSinkFunc(func(context.Context, contract.LogEvent) error { return mirrorErr })
			},
			logf: func(format string, values ...any) {
				*logs = append(*logs, fmt.Sprintf(format, values...))
			},
		}
	}
	claim := l1.Claim{
		Job: l1.Job{
			JobID: "job-console-mirror",
			Spec:  contract.JobSpec{Kind: "process", Class: contract.JobClassService},
		},
		Lease: l1.AttemptLease{AttemptID: "service-console-best-effort"},
	}
	var logs []string
	result, err := newAgent(&logs).runProcess(context.Background(), claim)
	if err != nil || result.ExitCode == nil || *result.ExitCode != 0 {
		t.Fatalf("service console failure runProcess() = (%#v, %v)", result, err)
	}
	if len(logs) == 0 || !strings.Contains(logs[0], mirrorErr.Error()) {
		t.Fatalf("service console failure logs = %#v", logs)
	}

	claim.Job.Spec.Class = contract.JobClassOneShot
	logs = nil
	result, err = newAgent(&logs).runProcess(context.Background(), claim)
	if !errors.Is(err, mirrorErr) {
		t.Fatalf("one-shot console failure runProcess() error = %v, want %v", err, mirrorErr)
	}
	if result.ExitCode != nil {
		t.Fatalf("one-shot console failure result = %#v, want producer stopped", result)
	}
	if len(logs) != 0 {
		t.Fatalf("one-shot console failure was downgraded: logs=%#v", logs)
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
	store, err := l1.OpenStore(filepath.Join(t.TempDir(), "l1.sqlite"), l1.StoreOptions{LeaseDuration: 1 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	server, err := l1.NewServer(serverFabric, store, l1.ServerConfig{NodePolicies: map[string]l1.NodePolicy{"stable-node": l1.DefaultNodePolicy("linux")}})
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
		RenewalInterval:     100 * time.Millisecond,
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
		Class:         contract.JobClassOneShot,
		RoutingTags:   []string{"linux"},
		Execution: contract.ExecutionSpec{
			Executable:       contract.ExecutableSpec{Path: agentHelperPath},
			Argv:             []string{"processhelper", "sleep", "3000"},
			WorkingDirectory: workingDirectory,
			HandoffDirectory: workingDirectory,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	completed := waitForJobState(t, store, job.JobID, contract.JobSucceeded, 15*time.Second)
	if elapsed := time.Since(started); elapsed < 2500*time.Millisecond {
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
	store, err := l1.OpenStore(filepath.Join(t.TempDir(), "completion-retry.sqlite"), l1.StoreOptions{LeaseDuration: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	l1Server, err := l1.NewServer(serverFabric, store, l1.ServerConfig{NodePolicies: map[string]l1.NodePolicy{"stable-node": l1.DefaultNodePolicy("linux")}})
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
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{
				Code: contract.ErrorInternal, Message: "completion temporarily unavailable", Retryable: false,
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
		RenewalInterval: 200 * time.Millisecond, LogRetryInterval: 50 * time.Millisecond,
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
		SchemaVersion: contract.SchemaVersionV1, DispatchKey: "completion-retry", Kind: "process", Class: contract.JobClassOneShot, RoutingTags: []string{"linux"},
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
	time.Sleep(1750 * time.Millisecond)
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
	time.Sleep(400 * time.Millisecond)
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
