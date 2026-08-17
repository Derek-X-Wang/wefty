//go:build service_acceptance && (darwin || linux)

package serviceacceptance

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

const servicePortEnvironment = "WEFTY_SERVICE_PORT"

func TestServiceRestartsAfterSuccessfulProcessExit(t *testing.T) {
	harness := newAcceptanceHarness(t)
	port := reservePort(t)
	job := harness.submitEchoService(t, port)

	baseURL := fmt.Sprintf("http://127.0.0.1:%d", port)
	client := &http.Client{Timeout: 5 * time.Second}
	health := waitForHealth(t, client, baseURL, harness.agent)
	running := harness.waitForJobState(t, job.JobID, contract.JobRunning, 5*time.Second)
	if running.CurrentAttemptID == "" {
		t.Fatal("running service has no attempt ID")
	}
	if health.PID == harness.agent.command.Process.Pid {
		t.Fatalf("health PID = agent PID %d; payload was not a distinct child process", health.PID)
	}

	assertEcho(t, client, baseURL, []byte("echo acceptance"))
	assertGracefulShutdown(t, baseURL, health.PID, harness.agent)
	restarted := waitForFreshRunningAttempt(t, harness, job.JobID, running.CurrentAttemptID, 5*time.Second)
	restartedHealth := waitForHealth(t, client, baseURL, harness.agent)
	if restartedHealth.PID == health.PID {
		t.Fatalf("service restart reused payload PID %d", health.PID)
	}
	if restarted.JobID != job.JobID {
		t.Fatalf("service restart job ID = %q, want stable %q", restarted.JobID, job.JobID)
	}
	assertEcho(t, client, baseURL, []byte("echo after restart"))
}

func waitForFreshRunningAttempt(t *testing.T, harness *acceptanceHarness, jobID, previousAttemptID string, timeout time.Duration) l1.Job {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if harness.agent.exited() {
			t.Fatalf("agent exited while waiting for service restart: %v\n%s", harness.agent.waitError(), harness.agent.outputString())
		}
		var job l1.Job
		status, body := harness.doJSON(t, http.MethodGet, "/v1/jobs/"+jobID, nil, &job)
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

	baseURL := fmt.Sprintf("http://127.0.0.1:%d", port)
	health := waitForHealth(t, &http.Client{Timeout: time.Second}, baseURL, harness.agent)
	harness.waitForJobState(t, job.JobID, contract.JobRunning, 5*time.Second)
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

func TestPublishedPortPreflightLatchesFailureWithoutKillingOwner(t *testing.T) {
	harness := newAcceptanceHarness(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	port := listener.Addr().(*net.TCPAddr).Port
	job := harness.submitEchoService(t, port)
	failed := harness.waitForJobState(t, job.JobID, contract.JobFailed, 5*time.Second)

	var failure struct {
		Code          contract.SpawnFailureCode `json:"code"`
		NodeID        string                    `json:"node_id"`
		PublishedPort int                       `json:"published_port"`
	}
	if err := json.Unmarshal(failed.LastFailure, &failure); err != nil {
		t.Fatalf("decode last_failure %s: %v", failed.LastFailure, err)
	}
	if failure.Code != contract.SpawnFailurePublishedPortOccupied ||
		failure.NodeID != "acceptance-node" || failure.PublishedPort != port {
		t.Fatalf("last_failure = %#v, want occupied port with node and port", failure)
	}
	var failureFields map[string]json.RawMessage
	if err := json.Unmarshal(failed.LastFailure, &failureFields); err != nil {
		t.Fatal(err)
	}
	for field := range failureFields {
		if strings.Contains(field, "owner") {
			t.Fatalf("last_failure guessed an owner in field %q: %s", field, failed.LastFailure)
		}
	}
	if failed.DesiredState != contract.ServiceDesiredRunning || failed.RestartStreak != 0 ||
		failed.LifetimeRestartCount != 0 || failed.RestartPending(contract.JobFailed, time.Now()) {
		t.Fatalf("failed service consumed restart budget or changed intent: %#v", failed.ServiceJob)
	}

	connection, err := net.DialTimeout("tcp", listener.Addr().String(), time.Second)
	if err != nil {
		t.Fatalf("published-port owner was disturbed: %v", err)
	}
	_ = connection.Close()
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
) struct{ PID int } {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if agent.exited() {
			t.Fatalf("agent exited before service became healthy: %v\n%s", agent.waitError(), agent.outputString())
		}
		response, err := client.Get(baseURL + "/healthz")
		if err == nil {
			var health struct{ PID int }
			decodeErr := json.NewDecoder(response.Body).Decode(&health)
			closeErr := response.Body.Close()
			if response.StatusCode == http.StatusOK && decodeErr == nil && closeErr == nil {
				return health
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("echo service did not become healthy on injected port\n%s", agent.outputString())
	return struct{ PID int }{}
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
	baseURL string,
	payloadPID int,
	agent *managedProcess,
) {
	t.Helper()
	connection, err := net.DialTimeout("tcp", strings.TrimPrefix(baseURL, "http://"), 5*time.Second)
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
	time.Sleep(250 * time.Millisecond)
	if err := syscall.Kill(payloadPID, 0); err != nil {
		t.Fatalf("echo service exited before its in-flight request completed: %v\n%s", err, agent.outputString())
	}
	if agent.exited() {
		t.Fatalf("agent exited while the service handled SIGTERM: %v\n%s", agent.waitError(), agent.outputString())
	}

	if _, err := connection.Write(second); err != nil {
		t.Fatalf("write second streaming echo segment after SIGTERM: %v", err)
	}
	actualSecond, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read streaming echo response after SIGTERM: %v", err)
	}
	if !bytes.Equal(actualSecond, second) {
		t.Fatalf("second streaming echo segment = %q, want %q", actualSecond, second)
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
