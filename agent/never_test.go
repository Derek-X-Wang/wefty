package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
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

func TestNeverProcessAcknowledgesRunnerStart(t *testing.T) {
	for _, refuse := range []bool{false, true} {
		t.Run(map[bool]string{false: "accepted", true: "refused"}[refuse], func(t *testing.T) {
			var acknowledged atomic.Bool
			client, closeServer := startEvidenceReplayServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/v1/agent/jobs/job/attempts/attempt/started" {
					t.Errorf("unexpected path %s", r.URL.Path)
				}
				var req l1.StartedRequest
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.FencingToken != "fence" {
					t.Errorf("start request = %+v %v", req, err)
				}
				acknowledged.Store(true)
				w.Header().Set("Content-Type", "application/json")
				if refuse {
					w.WriteHeader(http.StatusConflict)
					_ = json.NewEncoder(w).Encode(map[string]any{"error": contract.APIError{Code: contract.ErrorStaleFence, Message: "lost fence"}})
				} else {
					_ = json.NewEncoder(w).Encode(l1.Job{JobID: "job", State: contract.JobRunning})
				}
			}), time.Second)
			defer closeServer()
			defer client.Close()
			root, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			resource, err := initializeManagedResource(root, "node", "boot")
			if err != nil {
				t.Fatal(err)
			}
			executor := directiveContinuationRunner(func(ctx context.Context, req processrunner.Request, _ processrunner.OutputSink) (contract.ProcessResult, error) {
				if req.Started == nil {
					t.Fatal("process runner has no start acknowledgement hook")
				}
				if acknowledged.Load() {
					t.Fatal("acknowledged before runner start")
				}
				req.Started()
				if !acknowledged.Load() {
					t.Fatal("runner start did not reach L1")
				}
				if refuse {
					if ctx.Err() == nil {
						t.Fatal("refused acknowledgement left payload running")
					}
					return contract.ProcessResult{Signal: "terminated", TerminationCause: contract.TerminationCauseAgent}, ctx.Err()
				}
				zero := 0
				return contract.ProcessResult{ExitCode: &zero}, nil
			})
			lifecycle := newAttemptLifecycle(attemptLifecycleDependencies{nodeID: "node", bootSessionID: "boot", managedResource: resource, client: client, runtimes: testRuntimeSet(executor), clock: systemClock{}, finalizationTimeout: time.Second})
			claim := l1.Claim{Job: l1.Job{JobID: "job", Spec: contract.JobSpec{Kind: "process", Class: "service", Restart: "never", Execution: contract.ExecutionSpec{Executable: contract.ExecutableSpec{Path: "/bin/true"}, Argv: []string{"true"}, WorkingDirectory: t.TempDir()}}}, Lease: l1.AttemptLease{AttemptID: "attempt", FencingToken: "fence"}}
			result, err := lifecycle.runWorkload(t.Context(), claim)
			if refuse {
				if err == nil || !strings.Contains(err.Error(), "acknowledge process start") || result.Signal == "" {
					t.Fatalf("refused start = %+v %v", result, err)
				}
			} else if err != nil || result.ExitCode == nil || *result.ExitCode != 0 {
				t.Fatalf("accepted start = %+v %v", result, err)
			}
		})
	}
}

// An app drives a real L1 and node agent entirely through the client contract.
// The real process runner and guardian must acknowledge start and publish the
// chosen exit, and a new execution requires explicit start/restart.
func TestNeverProcessPublicContractWithRealAgent(t *testing.T) {
	for _, code := range []int{0, 7} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			network := plain.NewNetwork()
			_, stopServer := startFailureServerWithPoliciesAndLease(t, network, nil, map[string]l1.NodePolicy{"never-node": {MaxServiceSlots: 1}}, time.Second)
			defer stopServer()
			client := newHTTPClient(network.NewFabric(fabric.Identity{NodeID: "app", Tags: []string{l1.DefaultClientPrincipalTag}}), "wefty://control-plane")
			defer client.CloseIdleConnections()
			call := func(method, path string, value any, want int) l1.Job {
				t.Helper()
				var payload []byte
				if value != nil {
					var err error
					payload, err = json.Marshal(value)
					if err != nil {
						t.Fatal(err)
					}
				}
				req, err := http.NewRequestWithContext(t.Context(), method, "http://l1"+path, bytes.NewReader(payload))
				if err != nil {
					t.Fatal(err)
				}
				req.Header.Set("Content-Type", "application/json")
				resp, err := client.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				defer resp.Body.Close()
				body, err := io.ReadAll(resp.Body)
				if err != nil {
					t.Fatal(err)
				}
				if resp.StatusCode != want {
					t.Fatalf("%s %s = %d %s", method, path, resp.StatusCode, body)
				}
				var job l1.Job
				if err := json.Unmarshal(body, &job); err != nil {
					t.Fatal(err)
				}
				return job
			}
			root, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			nodeAgent, err := New(Config{Fabric: network.NewFabric(fabric.Identity{NodeID: "never-agent", Tags: []string{l1.DefaultAgentPrincipalTag}}), ControlPlaneAddress: "wefty://control-plane", NodeID: "never-node", BootSessionID: "boot-never", Version: "test", Capabilities: map[string]bool{"kind:process": true}, MaxServiceSlots: 1, ManagedRootDirectory: root, LogSpoolDirectory: t.TempDir(), GuardianExecutable: agentBinaryPath, HeartbeatInterval: 20 * time.Millisecond, ClaimInterval: 5 * time.Millisecond, RenewalInterval: 20 * time.Millisecond})
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
			job := call(http.MethodPost, "/v1/jobs", contract.JobSpec{SchemaVersion: 1, DispatchKey: "never-real", Kind: "process", Class: "service", Restart: "never", Execution: contract.ExecutionSpec{Executable: contract.ExecutableSpec{Path: "/bin/sh"}, Argv: []string{"sh", "-c", fmt.Sprintf("exit %d", code)}, WorkingDirectory: t.TempDir()}}, http.StatusCreated)
			wait := func() l1.Job {
				t.Helper()
				deadline := time.Now().Add(5 * time.Second)
				for {
					got := call(http.MethodGet, "/v1/jobs/"+job.JobID+"?class=service", nil, http.StatusOK)
					if got.PolicyStop != nil {
						return got
					}
					if time.Now().After(deadline) {
						t.Fatalf("no policy stop: %+v", got)
					}
					time.Sleep(5 * time.Millisecond)
				}
			}
			stopped := wait()
			want := contract.JobStopped
			if code != 0 {
				want = contract.JobFailed
			}
			if stopped.State != want || stopped.DesiredState != contract.ServiceDesiredRunning || stopped.PolicyStop.ExitCode == nil || *stopped.PolicyStop.ExitCode != code || stopped.HoldsSlot(stopped.State) {
				t.Fatalf("payload verdict = %+v", stopped)
			}
			time.Sleep(50 * time.Millisecond)
			retained := call(http.MethodGet, "/v1/jobs/"+job.JobID+"?class=service", nil, http.StatusOK)
			if retained.State != want || retained.CurrentAttemptID != stopped.CurrentAttemptID {
				t.Fatalf("automatic requeue = %+v", retained)
			}
			if code == 0 {
				call(http.MethodPut, "/v1/jobs/"+job.JobID+"/desired-state?class=service", l1.ServiceDesiredStateRequest{DesiredState: contract.ServiceDesiredRunning}, http.StatusAccepted)
			} else {
				call(http.MethodPost, "/v1/jobs/"+job.JobID+"/restart?class=service", l1.ServiceRestartRequest{IdempotencyKey: "resume"}, http.StatusAccepted)
			}
			resumed := wait()
			if resumed.State != want || resumed.CurrentAttemptID == stopped.CurrentAttemptID {
				t.Fatalf("explicit resume = %+v", resumed)
			}
		})
	}
}
