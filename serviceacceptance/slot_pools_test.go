//go:build (service_acceptance || service_acceptance_realtiming) && (darwin || linux)

package serviceacceptance

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/agent"
	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestClassPoolsRunAtCapacityAndIsolateSiblings(t *testing.T) {
	evidence := newSlotFailureEvidence()
	defer func() {
		failure := recover()
		if t.Failed() || failure != nil {
			evidence.log(t)
		}
		if failure != nil {
			panic(failure)
		}
	}()
	evidence.stage = "harness-start"
	// Real process/SQLite delivery uses the production lease, never a
	// compressed renewal deadline while four payloads compete for the CPU.
	harness := newAcceptanceHarnessWithOptions(t, acceptanceHarnessOptions{leaseDuration: l1.DefaultLeaseDuration})
	evidence.harness = harness
	evidence.stage = "reserve-ports"
	ports := reserveDistinctPorts(t, 3)
	services := make([]l1.Job, 3)
	for index := range services {
		evidence.stage = fmt.Sprintf("submit-service-%d", index)
		services[index] = harness.submitEchoService(t, ports[index])
		evidence.jobs[index].JobID = services[index].JobID
	}

	evidence.stage = "published-clients"
	serviceClients := []*http.Client{
		harness.publishedHTTPClient(t, ports[0]),
		harness.publishedHTTPClient(t, ports[1]),
	}
	serviceURLs := []string{"http://service-a.invalid", "http://service-b.invalid"}
	evidence.stage = "initial-health-a"
	healthA := waitForHealth(t, serviceClients[0], serviceURLs[0], harness.agent)
	evidence.stage = "initial-health-b"
	healthB := waitForHealth(t, serviceClients[1], serviceURLs[1], harness.agent)
	evidence.stage = "initial-running-a"
	runningA := harness.waitForJobState(t, services[0].JobID, contract.JobClassService, contract.JobRunning, poolObservationBudget)
	evidence.stage = "initial-running-b"
	harness.waitForJobState(t, services[1].JobID, contract.JobClassService, contract.JobRunning, poolObservationBudget)
	evidence.stage = "initial-c-queued"
	assertJobRemainsQueued(t, harness, services[2].JobID, 300*time.Millisecond)

	oneshots := make([]l1.Job, 0, 5)
	releases := make([]func(), 0, 5)
	for index := range 5 {
		evidence.stage = fmt.Sprintf("submit-oneshot-%d", index)
		job, release := harness.submitHeldOneShot(t, index)
		oneshots = append(oneshots, job)
		releases = append(releases, release)
		evidence.jobs[3+index].JobID = oneshots[index].JobID
	}
	evidence.stage = "oneshot-saturation"
	waitForOneShotSaturation(t, harness, oneshots, poolObservationBudget)

	evidence.stage = "kill-a-and-check-b"
	if err := syscall.Kill(healthA.PID, syscall.SIGKILL); err != nil {
		t.Fatalf("kill first service payload: %v", err)
	}
	evidence.stage = "post-kill-b-echo"
	assertEcho(t, serviceClients[1], serviceURLs[1], []byte("sibling survived"))
	if harness.agent.exited() {
		t.Fatalf("agent exited after one service payload was killed: %v\n%s", harness.agent.waitError(), harness.agent.outputString())
	}
	evidence.stage = "post-kill-b-pid"
	if current := waitForHealth(t, serviceClients[1], serviceURLs[1], harness.agent); current.PID != healthB.PID {
		t.Fatalf("unaffected service PID = %d, want original sibling PID %d", current.PID, healthB.PID)
	}

	evidence.stage = "a-restart"
	restarted := waitForFreshRunningAttempt(t, harness, services[0].JobID, runningA.CurrentAttemptID, poolObservationBudget)
	evidence.stage = "a-restart-health"
	restartedHealth := waitForHealth(t, serviceClients[0], serviceURLs[0], harness.agent)
	if restarted.CurrentAttemptID == runningA.CurrentAttemptID || restartedHealth.PID == healthA.PID {
		t.Fatalf("killed service did not restart under a fresh attempt and payload: attempt=%q pid=%d", restarted.CurrentAttemptID, restartedHealth.PID)
	}
	evidence.stage = "post-restart-c-queued"
	assertJobRemainsQueued(t, harness, services[2].JobID, 300*time.Millisecond)

	// The first four remain occupied until all capacity/sibling assertions
	// have completed, regardless of how slowly the observer is scheduled.
	for _, release := range releases {
		release()
	}
	for index, job := range oneshots {
		evidence.stage = fmt.Sprintf("await-oneshot-%d-succeeded", index)
		harness.waitForJobState(t, job.JobID, contract.JobClassOneShot, contract.JobSucceeded, poolObservationBudget)
		evidence.stage = fmt.Sprintf("oneshot-%d-output", index)
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

// Use the production startup deadline plus reap time as real-process delivery
// headroom. The condition being waited on, rather than elapsed time, wins.
const poolObservationBudget = agent.DefaultFinalizationTimeout + 5*time.Second

func (h *acceptanceHarness) submitHeldOneShot(t *testing.T, index int) (l1.Job, func()) {
	t.Helper()
	workingDirectory := t.TempDir()
	handoffDirectory := t.TempDir()
	releasePath := filepath.Join(workingDirectory, "release")
	release := func() {
		if err := os.WriteFile(releasePath, nil, 0o600); err != nil {
			t.Error(err)
		}
	}
	t.Cleanup(release)
	command := fmt.Sprintf("while [ ! -f \"$1\" ]; do sleep 0.01; done; printf 'oneshot-%d\\n'", index)
	spec := contract.JobSpec{
		SchemaVersion: contract.SchemaVersionV1,
		DispatchKey:   fmt.Sprintf("slot-pool-oneshot-%d-%d", index, time.Now().UnixNano()),
		Kind:          "process",
		Class:         contract.JobClassOneShot,
		RoutingTags:   []string{"service-acceptance"},
		Execution: contract.ExecutionSpec{
			Executable:       contract.ExecutableSpec{Path: "/bin/sh"},
			Argv:             []string{"sh", "-c", command, "held-oneshot", releasePath},
			WorkingDirectory: workingDirectory,
			HandoffDirectory: handoffDirectory,
		},
		Limits: &contract.JobLimits{MaxRuntimeSeconds: int(4 * poolObservationBudget / time.Second), IdleTimeoutSeconds: int(4 * poolObservationBudget / time.Second)},
	}
	var job l1.Job
	status, body := h.doJSON(t, http.MethodPost, "/v1/jobs", spec, &job)
	if status != http.StatusCreated {
		t.Fatalf("submit one-shot %d status = %d body=%s", index, status, body)
	}
	return job, release
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
		status, body := harness.doJSON(t, http.MethodGet, "/v1/jobs/"+jobID+"?class=service", nil, &job)
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
