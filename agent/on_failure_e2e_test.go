//go:build darwin || linux

package agent

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestAgentOnFailurePolicyEndToEnd(t *testing.T) {
	directory, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	readyFile := filepath.Join(directory, "ready.json")
	server := startManagedProcess(t, controlPlanePath, "--fabric=plain", "--listen=127.0.0.1:0", "--run-ledger=", "--db="+filepath.Join(directory, "l1.sqlite"), "--node-tags=stable-node=linux", "--ready-file="+readyFile)
	server.command.Stdout = &lockedBuffer{}
	server.command.Stderr = server.command.Stdout
	server.start(t)
	address := waitForReadyAddress(t, readyFile, server, 10*time.Second)
	node := startManagedProcess(t, agentBinaryPath, "--fabric=plain", "--control-plane="+address, "--node-id=stable-node", "--log-spool-dir="+directory, "--managed-root="+filepath.Join(directory, "managed"), "--handoff-root="+filepath.Join(directory, "handoffs"), "--plain-identity=fabric-node", "--heartbeat-interval=1s", "--claim-interval=10ms", "--renewal-interval=100ms", "--max-service-slots=1")
	nodeLogs := &lockedBuffer{}
	node.command.Stdout = nodeLogs
	node.command.Stderr = node.command.Stdout
	node.start(t)
	client := newHTTPClient(plain.NewNetwork().NewFabric(fabric.Identity{NodeID: "e2e-client", Tags: []string{l1.DefaultClientPrincipalTag}}), address)
	defer client.CloseIdleConnections()
	starts, release := filepath.Join(directory, "starts"), filepath.Join(directory, "release")
	registerE2EWorkloadRelease(t, release)
	script := filepath.Join(directory, "payload.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\ntrap 'exit 0' TERM\nprintf 'started\\n' >> \"$1\"\ni=0\nwhile [ ! -e \"$2\" ] && [ $i -lt 300 ]; do sleep 0.05; i=$((i+1)); done\nexit 0\n"), 0700); err != nil {
		t.Fatal(err)
	}
	job := submitE2EJob(t, client, contract.JobSpec{SchemaVersion: 1, DispatchKey: "on-failure-e2e", Kind: "process", Class: "service", Restart: "on-failure", RoutingTags: []string{"linux"}, Execution: contract.ExecutionSpec{Executable: contract.ExecutableSpec{Path: "/bin/sh"}, Argv: []string{"sh", script, starts, release}, WorkingDirectory: directory}})
	waitStarts := func(want int) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			raw, _ := os.ReadFile(starts)
			if bytes.Count(raw, []byte("started\n")) >= want {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatalf("payload did not start %d times: %+v\nagent: %s", want, getOnFailureE2EService(t, client, job.JobID), nodeLogs.Bytes())
	}
	mutate := func(method, operation string, payload any) {
		t.Helper()
		raw, _ := json.Marshal(payload)
		req, err := http.NewRequestWithContext(t.Context(), method, "http://control-plane.invalid/v1/jobs/"+job.JobID+"/"+operation+"?class=service", bytes.NewReader(raw))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		response, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		body, _ := io.ReadAll(response.Body)
		if response.StatusCode != http.StatusAccepted {
			t.Fatalf("%s = %d %s", operation, response.StatusCode, body)
		}
	}
	waitStarts(1)
	firstAttemptID := getOnFailureE2EService(t, client, job.JobID).CurrentAttemptID
	mutate(http.MethodPost, "restart", l1.ServiceRestartRequest{IdempotencyKey: "term-handler-zero"})
	waitStarts(2)
	if err := os.WriteFile(release, []byte("release"), 0600); err != nil {
		t.Fatal(err)
	}
	waitOnFailureE2EStopped(t, client, job.JobID)
	stopped := getOnFailureE2EService(t, client, job.JobID)
	// Decode through JSON so this test also compiles against the original code
	// for the red check.
	raw, _ := json.Marshal(stopped)
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(raw, &fields)
	if stopped.DesiredState != contract.ServiceDesiredRunning || stopped.SlotHeld || len(fields["policy_stop"]) == 0 || len(stopped.Attempts) != 2 {
		t.Fatalf("policy stop = %s", raw)
	}
	var termHandled bool
	for _, attempt := range stopped.Attempts {
		if attempt.AttemptID == firstAttemptID && attempt.Result != nil && attempt.Result.ExitCode != nil && *attempt.Result.ExitCode == 0 {
			termHandled = true
		}
	}
	if !termHandled {
		t.Fatalf("TERM handler did not exit zero: %+v", stopped.Attempts)
	}
	time.Sleep(250 * time.Millisecond)
	if got := getOnFailureE2EService(t, client, job.JobID); got.State != contract.JobStopped || len(got.Attempts) != 2 {
		t.Fatalf("automatic restart = %+v", got)
	}
	if err := os.Remove(release); err != nil {
		t.Fatal(err)
	}
	mutate(http.MethodPut, "desired-state", l1.ServiceDesiredStateRequest{DesiredState: contract.ServiceDesiredRunning})
	waitStarts(3)
	if err := os.WriteFile(release, []byte("release"), 0600); err != nil {
		t.Fatal(err)
	}
	waitOnFailureE2EStopped(t, client, job.JobID)
	final := getOnFailureE2EService(t, client, job.JobID)
	if len(final.Attempts) != 3 {
		t.Fatalf("start did not create fresh attempt: %+v", final)
	}
	node.stop(t)
	server.stop(t)
}

// A forced agent shutdown ends a TERM-handling on-failure service through its
// guardian. The payload's exit zero answers that request, so the completion
// requeues the service and the next agent runs it again; only a clean exit
// nobody asked for is a policy stop.
func TestAgentOnFailureShutdownRequeuesTermHandlingService(t *testing.T) {
	directory, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	readyFile := filepath.Join(directory, "ready.json")
	server := startManagedProcess(t, controlPlanePath, "--fabric=plain", "--listen=127.0.0.1:0", "--run-ledger=", "--db="+filepath.Join(directory, "l1.sqlite"), "--node-tags=stable-node=linux", "--ready-file="+readyFile)
	server.command.Stdout = &lockedBuffer{}
	server.command.Stderr = server.command.Stdout
	server.start(t)
	address := waitForReadyAddress(t, readyFile, server, 10*time.Second)
	nodeLogs := &lockedBuffer{}
	startNode := func() *managedProcess {
		node := startManagedProcess(t, agentBinaryPath, "--fabric=plain", "--control-plane="+address, "--node-id=stable-node", "--log-spool-dir="+directory, "--managed-root="+filepath.Join(directory, "managed"), "--handoff-root="+filepath.Join(directory, "handoffs"), "--plain-identity=fabric-node", "--heartbeat-interval=1s", "--claim-interval=10ms", "--renewal-interval=100ms", "--max-service-slots=1")
		node.command.Stdout = nodeLogs
		node.command.Stderr = node.command.Stdout
		node.start(t)
		return node
	}
	node := startNode()
	client := newHTTPClient(plain.NewNetwork().NewFabric(fabric.Identity{NodeID: "e2e-client", Tags: []string{l1.DefaultClientPrincipalTag}}), address)
	defer client.CloseIdleConnections()
	starts, release := filepath.Join(directory, "starts"), filepath.Join(directory, "release")
	registerE2EWorkloadRelease(t, release)
	script := filepath.Join(directory, "payload.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\ntrap 'exit 0' TERM\nprintf 'started\\n' >> \"$1\"\ni=0\nwhile [ ! -e \"$2\" ] && [ $i -lt 300 ]; do sleep 0.05; i=$((i+1)); done\nexit 0\n"), 0700); err != nil {
		t.Fatal(err)
	}
	job := submitE2EJob(t, client, contract.JobSpec{SchemaVersion: 1, DispatchKey: "on-failure-shutdown-e2e", Kind: "process", Class: "service", Restart: "on-failure", RoutingTags: []string{"linux"}, Execution: contract.ExecutionSpec{Executable: contract.ExecutableSpec{Path: "/bin/sh"}, Argv: []string{"sh", script, starts, release}, WorkingDirectory: directory}})
	waitStarts := func(want int) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			raw, _ := os.ReadFile(starts)
			if bytes.Count(raw, []byte("started\n")) >= want {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatalf("payload did not start %d times: %+v\nagent: %s", want, getOnFailureE2EService(t, client, job.JobID), nodeLogs.Bytes())
	}
	waitStarts(1)
	firstAttemptID := getOnFailureE2EService(t, client, job.JobID).CurrentAttemptID
	// The first signal drains, which never ends a resident service; the second
	// forces the shutdown that stops it.
	for range 2 {
		if err := node.command.Process.Signal(os.Interrupt); err != nil {
			t.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	select {
	case <-node.done:
	case <-time.After(15 * time.Second):
		t.Fatalf("agent did not shut down\nagent: %s", nodeLogs.Bytes())
	}
	interrupted := getOnFailureE2EService(t, client, job.JobID)
	raw, _ := json.Marshal(interrupted)
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(raw, &fields)
	var answered bool
	for _, attempt := range interrupted.Attempts {
		if attempt.AttemptID == firstAttemptID && attempt.Result != nil && attempt.Result.ExitCode != nil && *attempt.Result.ExitCode == 0 {
			answered = true
		}
	}
	// The shutdown completion was accepted, not lost to lease expiry: the
	// attempt carries the TERM handler's exit zero.
	if !answered {
		t.Fatalf("shutdown completion was not recorded: %s\nagent: %s", raw, nodeLogs.Bytes())
	}
	if interrupted.State != contract.JobQueued || interrupted.DesiredState != contract.ServiceDesiredRunning || len(fields["policy_stop"]) != 0 || interrupted.RestartStreak != 0 {
		t.Fatalf("agent shutdown was read as the payload stopping itself: %s", raw)
	}

	startNode()
	waitStarts(2)
	if err := os.WriteFile(release, []byte("release"), 0600); err != nil {
		t.Fatal(err)
	}
	waitOnFailureE2EStopped(t, client, job.JobID)
	stopped := getOnFailureE2EService(t, client, job.JobID)
	raw, _ = json.Marshal(stopped)
	fields = nil
	_ = json.Unmarshal(raw, &fields)
	if stopped.DesiredState != contract.ServiceDesiredRunning || len(fields["policy_stop"]) == 0 {
		t.Fatalf("self-exit did not record a policy stop: %s", raw)
	}
}

func getOnFailureE2EService(t *testing.T, client *http.Client, jobID string) l1.Job {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://control-plane.invalid/v1/jobs/"+jobID+"?class=service", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("service get = %d %s", response.StatusCode, raw)
	}
	var job l1.Job
	if err := json.Unmarshal(raw, &job); err != nil {
		t.Fatal(err)
	}
	return job
}
func waitOnFailureE2EStopped(t *testing.T, client *http.Client, jobID string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		job := getOnFailureE2EService(t, client, jobID)
		if job.State == contract.JobStopped {
			return
		}
		if job.State == contract.JobFailed {
			t.Fatalf("service failed: %+v", job)
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("service did not stop: %+v", getOnFailureE2EService(t, client, jobID))
}
