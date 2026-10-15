//go:build darwin || linux

package agent

import (
	"context"
	"encoding/json"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
	processrunner "github.com/Derek-X-Wang/wefty/runner/process"
)

// A process one-shot that prints nothing reads running once its payload
// exists, not at its first output or first lease renewal (#945). Renewal here
// is minutes away and the payload is silent, so only the agent's start
// acknowledgement can move the job out of claimed.
func TestSilentProcessOneShotRunsBeforeFirstRenewal(t *testing.T) {
	network := plain.NewNetwork()
	store, stopServer := startFailureServerWithPoliciesAndLease(t, network, nil, map[string]l1.NodePolicy{"silent-node": l1.DefaultNodePolicy()}, 10*time.Minute)
	defer stopServer()
	job, _, err := store.CreateJob(t.Context(), contract.JobSpec{SchemaVersion: 1, DispatchKey: "silent-one-shot", Kind: contract.JobKindProcess, Class: contract.JobClassOneShot,
		Execution: contract.ExecutionSpec{Executable: contract.ExecutableSpec{Path: "/bin/sleep"}, Argv: []string{"sleep", "60"}, WorkingDirectory: t.TempDir()}})
	if err != nil {
		t.Fatal(err)
	}
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	nodeAgent, err := New(Config{Fabric: network.NewFabric(fabric.Identity{NodeID: "silent-agent", Tags: []string{l1.DefaultAgentPrincipalTag}}), ControlPlaneAddress: "wefty://control-plane",
		NodeID: "silent-node", BootSessionID: "boot-silent", Version: "test", Capabilities: map[string]bool{"kind:process": true}, ManagedRootDirectory: root, LogSpoolDirectory: t.TempDir(),
		HeartbeatInterval: 20 * time.Millisecond, ClaimInterval: 5 * time.Millisecond, RenewalInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer nodeAgent.Close()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- nodeAgent.Run(ctx) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("agent shutdown: %v", err)
		}
	}()
	deadline := time.Now().Add(20 * time.Second)
	for {
		current, err := store.GetJob(t.Context(), job.JobID)
		if err != nil {
			t.Fatal(err)
		}
		if current.State == contract.JobRunning {
			return
		}
		if current.State != contract.JobQueued && current.State != contract.JobClaimed {
			t.Fatalf("silent one-shot left claim as %s: %+v", current.State, current)
		}
		if time.Now().After(deadline) {
			t.Fatalf("silent one-shot is still %s with its payload started; only a renewal or output would promote it", current.State)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// For a one-shot the start acknowledgement only reports the start. Renewal
// still decides authority, so a refusal, including an older L1 that accepts
// Started only for OCI, never stops the payload. The payload ends only after
// the agent has handled the refusal, so a refusal that stopped it would win.
func TestProcessOneShotStartRefusalLeavesThePayloadRunning(t *testing.T) {
	client, closeServer := startEvidenceReplayServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/agent/jobs/job/attempts/attempt/started" {
			http.NotFound(w, r)
			return
		}
		var request l1.StartedRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil || request.FencingToken != "fence" {
			t.Errorf("start request = %+v %v", request, err)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": contract.APIError{Code: contract.ErrorConflict, Message: "Started is only valid for OCI attempts"}})
	}), time.Second)
	defer closeServer()
	defer client.Close()
	// The agent logs a refusal it keeps the payload through, once it has
	// read and classified L1's answer.
	handled := make(chan struct{})
	var handledOnce sync.Once
	logf := func(format string, _ ...any) {
		if strings.Contains(format, "start acknowledgement was not recorded") {
			handledOnce.Do(func() { close(handled) })
		}
	}
	executor := directiveContinuationRunner(func(ctx context.Context, request processrunner.Request, _ processrunner.OutputSink) (contract.ProcessResult, error) {
		if request.Started == nil {
			t.Fatal("process one-shot runner has no start acknowledgement hook")
		}
		request.Started()
		select {
		case <-handled:
		case <-ctx.Done():
		case <-time.After(5 * time.Second):
			t.Error("the agent never handled the start refusal")
		}
		if ctx.Err() != nil {
			return contract.ProcessResult{Signal: "terminated", TerminationCause: contract.TerminationCauseAgent}, context.Cause(ctx)
		}
		zero := 0
		return contract.ProcessResult{ExitCode: &zero}, nil
	})
	observer := newLifecycleObserver(systemClock{})
	observer.beginAttempt("attempt", "job", contract.JobClassOneShot)
	lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{nodeID: "node", bootSessionID: "boot", client: client, runtimes: testRuntimeSet(executor),
		clock: systemClock{}, completionRetry: time.Millisecond, finalizationTimeout: time.Second, observer: observer, logf: logf})
	claim := l1.Claim{Job: l1.Job{JobID: "job", Spec: contract.JobSpec{Kind: contract.JobKindProcess, Class: contract.JobClassOneShot,
		Execution: contract.ExecutionSpec{Executable: contract.ExecutableSpec{Path: "/bin/true"}, Argv: []string{"true"}, WorkingDirectory: t.TempDir()}}},
		Lease: l1.AttemptLease{AttemptID: "attempt", FencingToken: "fence", LeaseTTL: time.Minute}}
	result, err := lifecycle.runWorkload(t.Context(), claim)
	if err != nil || result.ExitCode == nil || *result.ExitCode != 0 {
		t.Fatalf("refused start stopped the one-shot: %+v %v", result, err)
	}
}
