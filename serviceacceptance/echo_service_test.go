//go:build (service_acceptance || service_acceptance_realtiming) && (darwin || linux)

package serviceacceptance

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/agent/managedroot"
	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestServiceRestartsAfterSuccessfulProcessExit(t *testing.T) {
	harness := newAcceptanceHarness(t)
	port := reservePort(t)
	job := harness.submitEchoService(t, port)

	baseURL := "http://published-service.invalid"
	client := harness.publishedHTTPClient(t, port)
	health := waitForHealth(t, client, baseURL, harness.agent)
	running := harness.waitForJobState(t, job.JobID, contract.JobClassService, contract.JobRunning, 5*time.Second)
	if running.CurrentAttemptID == "" {
		t.Fatal("running service has no attempt ID")
	}
	if running.Ready == nil || !*running.Ready {
		t.Fatalf("reachable service publication = %v, want ready=true", running.Ready)
	}
	if health.PID == harness.agent.command.Process.Pid {
		t.Fatalf("health PID = agent PID %d; payload was not a distinct child process", health.PID)
	}
	if health.ListeningPort <= 0 || health.ListeningPort == port {
		t.Fatalf("payload listening port = %d, want injected backend distinct from published port %d", health.ListeningPort, port)
	}
	assertManagedServiceLayout(t, harness, running, health.ServiceDirectory)

	assertEcho(t, client, baseURL, []byte("echo acceptance"))
	assertGracefulShutdown(t, harness, job.JobID, port, health.PID, harness.agent)
	restarted := waitForFreshRunningAttempt(t, harness, job.JobID, running.CurrentAttemptID, 5*time.Second)
	restartedHealth := waitForHealth(t, client, baseURL, harness.agent)
	if restartedHealth.PID == health.PID {
		t.Fatalf("service restart reused payload PID %d", health.PID)
	}
	if restarted.JobID != job.JobID {
		t.Fatalf("service restart job ID = %q, want stable %q", restarted.JobID, job.JobID)
	}
	assertEcho(t, client, baseURL, []byte("echo after restart"))
	// Residue assertions from #83, taken after the restart rather than after a
	// terminal success: under the restart engine a service that exits zero is
	// retried, so the first attempt scratch must be gone while the job lives on.
	assertAttemptScratchCleared(t, harness, running)
	assertHandoffRootEmpty(t, harness.handoffRoot)
	assertWorkingDirectoryUntouched(t, harness.workingDirectories[job.JobID])
}

func TestServiceCrashAfterFinalizationBudgetRestarts(t *testing.T) {
	const finalizationTimeout = 100 * time.Millisecond
	harness := newAcceptanceHarnessWithAgentArguments(t, "--finalization-timeout="+finalizationTimeout.String())
	port := reservePort(t)
	job := harness.submitEchoService(t, port)
	health := waitForHealth(t, harness.publishedHTTPClient(t, port), "http://published-service.invalid", harness.agent)
	running := harness.waitForJobState(t, job.JobID, contract.JobClassService, contract.JobRunning, 5*time.Second)

	// Hold the real payload beyond the complete finalization allowance. The
	// allowance must still be fresh when SIGKILL starts finalization.
	time.Sleep(3 * finalizationTimeout)
	if err := syscall.Kill(health.PID, syscall.SIGKILL); err != nil {
		t.Fatalf("SIGKILL payload: %v", err)
	}
	waitForProcessAbsent(t, health.PID, 5*time.Second)
	restartPending := waitForServiceStatus(t, harness, job.JobID, "restart-pending", 5*time.Second)
	if restartPending.State != contract.JobQueued || restartPending.RestartStreak != 1 || restartPending.NextRestartAt == nil {
		t.Fatalf("service after payload SIGKILL = %#v, want restart-pending", restartPending)
	}
	var failure l1.ProcessResult
	if err := json.Unmarshal(restartPending.LastFailure, &failure); err != nil {
		t.Fatalf("decode last_failure: %v", err)
	}
	if failure.OutputError != "" || failure.Signal == "" || failure.TerminationCause != contract.TerminationCauseSpontaneous {
		t.Fatalf("service failure = %#v, want spontaneous signal without output_error", failure)
	}

	var logs l1.LogPage
	status, body := harness.doJSON(t, http.MethodGet, "/v1/jobs/"+job.JobID+"/logs?class=service&limit=100", nil, &logs)
	if status != http.StatusOK {
		t.Fatalf("get finalized service logs status = %d body=%s", status, body)
	}
	foundAttemptEvent := false
	for _, event := range logs.Events {
		if event.AttemptID == running.CurrentAttemptID {
			foundAttemptEvent = true
			break
		}
	}
	if !foundAttemptEvent && !failure.LogEvidenceIncomplete {
		t.Fatalf("service logs omitted durable events for attempt %q without log_evidence_incomplete: %#v", running.CurrentAttemptID, logs.Events)
	}
	if restarted := waitForFreshRunningAttempt(t, harness, job.JobID, running.CurrentAttemptID, 5*time.Second); restarted.JobID != job.JobID {
		t.Fatalf("restarted service job ID = %q, want %q", restarted.JobID, job.JobID)
	}
}

func waitForServiceStatus(t *testing.T, harness *acceptanceHarness, jobID, wantStatus string, timeout time.Duration) l1.Job {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if harness.agent.exited() {
			t.Fatalf("agent exited while waiting for service status %q: %v\n%s", wantStatus, harness.agent.waitError(), harness.agent.outputString())
		}
		var job l1.Job
		status, body := harness.doJSON(t, http.MethodGet, "/v1/jobs/"+jobID+"?class=service", nil, &job)
		if status != http.StatusOK {
			t.Fatalf("get service status = %d body=%s", status, body)
		}
		if job.State == contract.JobFailed {
			t.Fatalf("service latched failed while waiting for %q: %s\n%s", wantStatus, body, harness.agent.outputString())
		}
		if job.Status == wantStatus {
			return job
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for service %q status %q\n%s", jobID, wantStatus, harness.agent.outputString())
	return l1.Job{}
}

func waitForPublicationCleared(t *testing.T, harness *acceptanceHarness, jobID string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var job l1.Job
		status, body := harness.doJSON(t, http.MethodGet, "/v1/jobs/"+jobID+"?class=service", nil, &job)
		if status != http.StatusOK {
			t.Fatalf("get withdrawn service status = %d body=%s", status, body)
		}
		if job.Ready != nil && !*job.Ready {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for payload death to clear publication for %q", jobID)
}

func waitForFreshRunningAttempt(t *testing.T, harness *acceptanceHarness, jobID, previousAttemptID string, timeout time.Duration) l1.Job {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if harness.agent.exited() {
			t.Fatalf("agent exited while waiting for service restart: %v\n%s", harness.agent.waitError(), harness.agent.outputString())
		}
		var job l1.Job
		status, body := harness.doJSON(t, http.MethodGet, "/v1/jobs/"+jobID+"?class=service", nil, &job)
		if status != http.StatusOK {
			t.Fatalf("get restarted service status = %d body=%s", status, body)
		}
		if job.State == contract.JobFailed {
			t.Fatalf("service latched failed while waiting for restart: %s\n%s", body, harness.agent.outputString())
		}
		if job.State == contract.JobRunning && job.CurrentAttemptID != "" && job.CurrentAttemptID != previousAttemptID {
			return job
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for a fresh service attempt after %q\n%s", previousAttemptID, harness.agent.outputString())
	return l1.Job{}
}

func TestGuardianReapsPayloadWhenAgentIsSIGKILLed(t *testing.T) {
	harness := newAcceptanceHarness(t)
	port := reservePort(t)
	job := harness.submitEchoService(t, port)

	baseURL := "http://published-service.invalid"
	health := waitForHealth(t, harness.publishedHTTPClient(t, port), baseURL, harness.agent)
	harness.waitForJobState(t, job.JobID, contract.JobClassService, contract.JobRunning, 5*time.Second)
	started := time.Now()
	harness.agent.kill(t)
	waitForProcessAbsent(t, health.PID, 5*time.Second)
	if elapsed := time.Since(started); elapsed >= 1*time.Second {
		t.Fatalf("guardian reap took %s, want before the one-second lease expires", elapsed)
	}

	listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("payload port remained occupied after agent SIGKILL: %v", err)
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPortlessServiceSkipsReadinessAndKeepsRunning(t *testing.T) {
	harness := newAcceptanceHarness(t)
	job := harness.submitPortlessService(t)
	running := harness.waitForJobState(t, job.JobID, contract.JobClassService, contract.JobRunning, 5*time.Second)
	if running.PublishedPort != nil || running.Ready != nil || running.CurrentAttemptID == "" {
		t.Fatalf("portless projection = port %v ready %v attempt %q", running.PublishedPort, running.Ready, running.CurrentAttemptID)
	}
	time.Sleep(750 * time.Millisecond)
	stillRunning := harness.waitForJobState(t, job.JobID, contract.JobClassService, contract.JobRunning, time.Second)
	if stillRunning.CurrentAttemptID != running.CurrentAttemptID {
		t.Fatalf("portless service restarted without process death: before %q after %q", running.CurrentAttemptID, stillRunning.CurrentAttemptID)
	}
	assertHandoffRootEmpty(t, harness.handoffRoot)
	assertWorkingDirectoryUntouched(t, harness.workingDirectories[job.JobID])
}

func reservePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve backend port: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatalf("release backend port reservation: %v", err)
	}
	return port
}

func waitForProcessAbsent(t *testing.T, pid int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if err := syscall.Kill(pid, 0); err == syscall.ESRCH {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("payload PID %d survived agent SIGKILL", pid)
}

func waitForHealth(
	t *testing.T,
	client *http.Client,
	baseURL string,
	agent *managedProcess,
) serviceHealth {
	t.Helper()
	// A failure budget, not a success-path delay. The capacity tests start
	// several real services and one-shots at once, and 5s was tight enough to
	// lose to scheduler noise on a loaded runner.
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if agent.exited() {
			t.Fatalf("agent exited before service became healthy: %v\n%s", agent.waitError(), agent.outputString())
		}
		response, err := client.Get(baseURL + "/healthz")
		if err == nil {
			var health serviceHealth
			decodeErr := json.NewDecoder(response.Body).Decode(&health)
			closeErr := response.Body.Close()
			if response.StatusCode == http.StatusOK && decodeErr == nil && closeErr == nil {
				return health
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("echo service did not become healthy on injected port\n%s", agent.outputString())
	return serviceHealth{}
}

type serviceHealth struct {
	PID              int    `json:"pid"`
	ServiceDirectory string `json:"service_directory"`
	ListeningPort    int    `json:"listening_port"`
}

func assertManagedServiceLayout(t *testing.T, harness *acceptanceHarness, job l1.Job, serviceDirectory string) {
	t.Helper()
	nodeRoot := filepath.Join(harness.managedRoot, "agent", "nodes", managedroot.EncodeID("acceptance-node"))
	serviceRoot := filepath.Join(nodeRoot, "services", managedroot.EncodeID(job.JobID))
	if serviceDirectory != filepath.Join(serviceRoot, "data") {
		t.Fatalf("%s = %q, want managed data directory %q", contract.EnvServiceDir, serviceDirectory, filepath.Join(serviceRoot, "data"))
	}
	var rootManifest managedroot.RootManifest
	readJSONFile(t, filepath.Join(nodeRoot, managedroot.RootManifestName), &rootManifest)
	if rootManifest.NodeID != "acceptance-node" || rootManifest.RootInstanceID == "" {
		t.Fatalf("root manifest = %#v", rootManifest)
	}
	var ownership managedroot.OwnershipManifest
	readJSONFile(t, filepath.Join(serviceRoot, managedroot.OwnershipManifestName), &ownership)
	if ownership.JobID != job.JobID || ownership.RemovalGeneration != 1 {
		t.Fatalf("ownership manifest = %#v", ownership)
	}
	for _, path := range []string{
		filepath.Join(serviceRoot, "data"), filepath.Join(serviceRoot, "attempts"), filepath.Join(serviceRoot, "runtime"),
		filepath.Join(serviceRoot, "attempts", managedroot.EncodeID(job.CurrentAttemptID)),
	} {
		if info, err := os.Stat(path); err != nil || !info.IsDir() {
			t.Fatalf("managed service directory %q = (%v, %v), want directory", path, info, err)
		}
	}
}

func assertAttemptScratchCleared(t *testing.T, harness *acceptanceHarness, job l1.Job) {
	t.Helper()
	path := filepath.Join(
		harness.managedRoot, "agent", "nodes", managedroot.EncodeID("acceptance-node"),
		"services", managedroot.EncodeID(job.JobID), "attempts", managedroot.EncodeID(job.CurrentAttemptID),
	)
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Fatalf("attempt scratch %q survived completion: %v", path, err)
	}
}

func assertHandoffRootEmpty(t *testing.T, root string) {
	t.Helper()
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("service created handoff entries: %#v", entries)
	}
}

func assertWorkingDirectoryUntouched(t *testing.T, directory string) {
	t.Helper()
	payload, err := os.ReadFile(filepath.Join(directory, "operator-owned"))
	if err != nil || string(payload) != "untouched" {
		t.Fatalf("operator working directory changed: payload=%q err=%v", payload, err)
	}
}

func readJSONFile(t *testing.T, path string, target any) {
	t.Helper()
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(payload, target); err != nil {
		t.Fatal(err)
	}
}

func assertEcho(t *testing.T, client *http.Client, baseURL string, payload []byte) {
	t.Helper()
	response, err := client.Post(baseURL+"/echo", "application/octet-stream", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("POST /echo: %v", err)
	}
	defer response.Body.Close()
	actual, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read /echo response: %v", err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("POST /echo status = %d, want %d", response.StatusCode, http.StatusOK)
	}
	if !bytes.Equal(actual, payload) {
		t.Fatalf("POST /echo body = %q, want %q", actual, payload)
	}
}

func assertGracefulShutdown(
	t *testing.T,
	harness *acceptanceHarness,
	jobID string,
	port int,
	payloadPID int,
	agent *managedProcess,
) {
	t.Helper()
	dialContext, cancelDial := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelDial()
	connection, err := harness.dialPublished(dialContext, port)
	if err != nil {
		t.Fatalf("connect streaming echo request: %v", err)
	}
	defer connection.Close()
	if err := connection.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatalf("set streaming echo deadline: %v", err)
	}

	first := []byte("before-term|")
	second := []byte("after-term")
	if _, err := fmt.Fprintf(
		connection,
		"POST /echo HTTP/1.1\r\nHost: acceptance\r\nContent-Length: %d\r\nConnection: close\r\n\r\n",
		len(first)+len(second),
	); err != nil {
		t.Fatalf("write streaming echo headers: %v", err)
	}
	if _, err := connection.Write(first); err != nil {
		t.Fatalf("write first streaming echo segment: %v", err)
	}
	response, err := http.ReadResponse(bufio.NewReader(connection), &http.Request{Method: http.MethodPost})
	if err != nil {
		t.Fatalf("read streaming echo response headers: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("streaming POST /echo status = %d, want %d", response.StatusCode, http.StatusOK)
	}
	actualFirst := make([]byte, len(first))
	if _, err := io.ReadFull(response.Body, actualFirst); err != nil {
		t.Fatalf("read first streaming echo segment: %v", err)
	}
	if !bytes.Equal(actualFirst, first) {
		t.Fatalf("first streaming echo segment = %q, want %q", actualFirst, first)
	}

	if err := syscall.Kill(payloadPID, syscall.SIGTERM); err != nil {
		t.Fatalf("send SIGTERM: %v", err)
	}
	waitForPublicationCleared(t, harness, jobID, 5*time.Second)
	if agent.exited() {
		t.Fatalf("agent exited while the service handled SIGTERM: %v\n%s", agent.waitError(), agent.outputString())
	}

	// Once the payload closes its listener, readiness withdrawal must sever
	// established front-door tunnels before the clear publication completes.
	// The backend still receives the close and can finish graceful shutdown;
	// callers must reconnect to the replacement attempt instead of draining a
	// connection whose publication authority has been withdrawn.
	_, writeErr := connection.Write(second)
	_, readErr := io.ReadAll(response.Body)
	if writeErr == nil && readErr == nil {
		t.Fatal("streaming tunnel survived service readiness withdrawal")
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		err := syscall.Kill(payloadPID, 0)
		if err != nil {
			if err == syscall.ESRCH {
				return
			}
			t.Fatalf("check echo service PID %d: %v", payloadPID, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("echo service PID %d did not exit after graceful shutdown\n%s", payloadPID, agent.outputString())
}
