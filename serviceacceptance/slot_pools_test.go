//go:build service_acceptance && (darwin || linux)

package serviceacceptance

import (
	"fmt"
	"net"
	"net/http"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestClassPoolsRunAtCapacityAndIsolateSiblings(t *testing.T) {
	harness := newAcceptanceHarness(t)
	ports := reserveDistinctPorts(t, 3)
	services := []l1.Job{
		harness.submitEchoService(t, ports[0]),
		harness.submitEchoService(t, ports[1]),
		harness.submitEchoService(t, ports[2]),
	}

	serviceClients := []*http.Client{
		harness.publishedHTTPClient(t, ports[0]),
		harness.publishedHTTPClient(t, ports[1]),
	}
	serviceURLs := []string{"http://service-a.invalid", "http://service-b.invalid"}
	healthA := waitForHealth(t, serviceClients[0], serviceURLs[0], harness.agent)
	healthB := waitForHealth(t, serviceClients[1], serviceURLs[1], harness.agent)
	runningA := harness.waitForJobState(t, services[0].JobID, contract.JobRunning, 5*time.Second)
	harness.waitForJobState(t, services[1].JobID, contract.JobRunning, 5*time.Second)
	assertJobRemainsQueued(t, harness, services[2].JobID, 300*time.Millisecond)

	oneshots := make([]l1.Job, 0, 5)
	for index := range 5 {
		oneshots = append(oneshots, harness.submitSleepingOneShot(t, index))
	}
	waitForOneShotSaturation(t, harness, oneshots, 5*time.Second)

	if err := syscall.Kill(healthA.PID, syscall.SIGKILL); err != nil {
		t.Fatalf("kill first service payload: %v", err)
	}
	assertEcho(t, serviceClients[1], serviceURLs[1], []byte("sibling survived"))
	if harness.agent.exited() {
		t.Fatalf("agent exited after one service payload was killed: %v\n%s", harness.agent.waitError(), harness.agent.outputString())
	}
	if current := waitForHealth(t, serviceClients[1], serviceURLs[1], harness.agent); current.PID != healthB.PID {
		t.Fatalf("unaffected service PID = %d, want original sibling PID %d", current.PID, healthB.PID)
	}

	restarted := waitForFreshRunningAttempt(t, harness, services[0].JobID, runningA.CurrentAttemptID, 8*time.Second)
	restartedHealth := waitForHealth(t, serviceClients[0], serviceURLs[0], harness.agent)
	if restarted.CurrentAttemptID == runningA.CurrentAttemptID || restartedHealth.PID == healthA.PID {
		t.Fatalf("killed service did not restart under a fresh attempt and payload: attempt=%q pid=%d", restarted.CurrentAttemptID, restartedHealth.PID)
	}
	assertJobRemainsQueued(t, harness, services[2].JobID, 300*time.Millisecond)

	for index, job := range oneshots {
		harness.waitForJobState(t, job.JobID, contract.JobSucceeded, 8*time.Second)
		output := harness.agent.outputString()
		if !attributedOutputContains(output, job.JobID, fmt.Sprintf("oneshot-%d", index)) {
			t.Fatalf("one-shot %s output was not visibly attributed\n%s", job.JobID, output)
		}
	}
}

func attributedOutputContains(output, jobID, payload string) bool {
	for _, line := range strings.Split(output, "\n") {
		if strings.Contains(line, "[job="+jobID+" attempt=") && strings.HasSuffix(line, payload) {
			return true
		}
	}
	return false
}

func (h *acceptanceHarness) submitSleepingOneShot(t *testing.T, index int) l1.Job {
	t.Helper()
	workingDirectory := t.TempDir()
	handoffDirectory := t.TempDir()
	command := fmt.Sprintf("sleep 1.5; printf 'oneshot-%d\\n'", index)
	spec := contract.JobSpec{
		SchemaVersion: contract.SchemaVersionV1,
		DispatchKey:   fmt.Sprintf("slot-pool-oneshot-%d-%d", index, time.Now().UnixNano()),
		Kind:          "process",
		Class:         contract.JobClassOneShot,
		RoutingTags:   []string{"service-acceptance"},
		Execution: contract.ExecutionSpec{
			Executable:       contract.ExecutableSpec{Path: "/bin/sh"},
			Argv:             []string{"sh", "-c", command},
			WorkingDirectory: workingDirectory,
			HandoffDirectory: handoffDirectory,
		},
		Limits: &contract.JobLimits{MaxRuntimeSeconds: 10, IdleTimeoutSeconds: 10},
	}
	var job l1.Job
	status, body := h.doJSON(t, http.MethodPost, "/v1/jobs", spec, &job)
	if status != http.StatusCreated {
		t.Fatalf("submit one-shot %d status = %d body=%s", index, status, body)
	}
	return job
}

func waitForOneShotSaturation(t *testing.T, harness *acceptanceHarness, jobs []l1.Job, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		active := 0
		fifthQueued := false
		for index, submitted := range jobs {
			var job l1.Job
			status, body := harness.doJSON(t, http.MethodGet, "/v1/jobs/"+submitted.JobID, nil, &job)
			if status != http.StatusOK {
				t.Fatalf("get one-shot %s status = %d body=%s", submitted.JobID, status, body)
			}
			if index < 4 && (job.State == contract.JobClaimed || job.State == contract.JobRunning) {
				active++
			}
			if index == 4 && job.State == contract.JobQueued {
				fifthQueued = true
			}
		}
		if active == 4 && fifthQueued {
			return
		}
		if harness.agent.exited() {
			t.Fatalf("agent exited while filling one-shot slots: %v\n%s", harness.agent.waitError(), harness.agent.outputString())
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("one-shot pool never showed four active and the fifth queued\n%s", harness.agent.outputString())
}

func assertJobRemainsQueued(t *testing.T, harness *acceptanceHarness, jobID string, duration time.Duration) {
	t.Helper()
	deadline := time.Now().Add(duration)
	for time.Now().Before(deadline) {
		var job l1.Job
		status, body := harness.doJSON(t, http.MethodGet, "/v1/jobs/"+jobID, nil, &job)
		if status != http.StatusOK {
			t.Fatalf("get queued job %s status = %d body=%s", jobID, status, body)
		}
		if job.State != contract.JobQueued {
			t.Fatalf("capacity-blocked job %s state = %q, want visibly queued", jobID, job.State)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func reserveDistinctPorts(t *testing.T, count int) []int {
	t.Helper()
	listeners := make([]net.Listener, 0, count)
	defer func() {
		for _, listener := range listeners {
			_ = listener.Close()
		}
	}()
	ports := make([]int, 0, count)
	for range count {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("reserve distinct service port: %v", err)
		}
		listeners = append(listeners, listener)
		ports = append(ports, listener.Addr().(*net.TCPAddr).Port)
	}
	return ports
}
