//go:build service_acceptance_realtiming && (darwin || linux)

package serviceacceptance

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/agent"
	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
	processrunner "github.com/Derek-X-Wang/wefty/runner/process"
)

const realTimingEvidenceEnvironment = "WEFTY_REALTIME_EVIDENCE_DIR"

func TestServiceLifecycleAndRemovalAtProductionTimings(t *testing.T) {
	assertProductionTimingDefaults(t)
	evidence := newRealTimingEvidence(t)
	harness := newAcceptanceHarnessWithOptions(t, acceptanceHarnessOptions{
		leaseDuration:     l1.DefaultLeaseDuration,
		productionTimings: true,
	})
	t.Cleanup(func() {
		evidence.recordProcessOutput("control-plane.log", harness.controlPlane)
		for index, process := range harness.agents {
			evidence.recordProcessOutput(fmt.Sprintf("agent-%02d.log", index+1), process)
		}
	})
	evidence.recordMetadata(t)

	ports := reserveDistinctPorts(t, 4)
	primary, primaryCreate := harness.submitEchoServiceWithDispatchKey(t, ports[0], "realtiming-primary")
	sibling, siblingCreate := harness.submitEchoServiceWithDispatchKey(t, ports[1], "realtiming-sibling")
	assertCreateHasNoRunID(t, primaryCreate)
	assertCreateHasNoRunID(t, siblingCreate)
	evidence.write("create-primary.json", primaryCreate)
	evidence.write("create-sibling.json", siblingCreate)

	primaryClient := harness.publishedHTTPClient(t, ports[0])
	siblingClient := harness.publishedHTTPClient(t, ports[1])
	primaryHealth := waitForHealth(t, primaryClient, "http://primary.invalid", harness.agent)
	siblingHealth := waitForHealth(t, siblingClient, "http://sibling.invalid", harness.agent)
	primaryRunning := harness.waitForJobState(t, primary.JobID, contract.JobClassService, contract.JobRunning, 45*time.Second)
	siblingRunning := harness.waitForJobState(t, sibling.JobID, contract.JobClassService, contract.JobRunning, 45*time.Second)
	assertEcho(t, primaryClient, "http://primary.invalid", []byte("echo-before-disruption"))
	assertEcho(t, siblingClient, "http://sibling.invalid", []byte("sibling-before-disruption"))
	assertManagedServiceLayout(t, harness, primaryRunning, primaryHealth.ServiceDirectory)
	assertManagedServiceLayout(t, harness, siblingRunning, siblingHealth.ServiceDirectory)
	evidence.recordJob(t, harness, "status-primary-ready.json", primary.JobID)
	evidence.recordJob(t, harness, "status-sibling-ready.json", sibling.JobID)
	assertServiceListParity(t, harness, evidence, primaryRunning, siblingRunning)

	attemptIDs := []string{primaryRunning.CurrentAttemptID}
	payloadPIDs := []int{primaryHealth.PID}
	evidence.recordJob(t, harness, "status-before-payload-sigkill.json", primary.JobID)
	if err := syscall.Kill(primaryHealth.PID, syscall.SIGKILL); err != nil {
		t.Fatalf("SIGKILL primary payload: %v", err)
	}
	restartPending := waitForRestartPending(t, harness, primary.JobID, 15*time.Second)
	evidence.recordJSON("status-restart-pending.json", restartPending)
	assertPublishedUnavailable(t, harness, ports[0])
	primaryAfterKill := waitForFreshRunningAttempt(
		t, harness, primary.JobID, primaryRunning.CurrentAttemptID, 45*time.Second,
	)
	primaryHealth = waitForHealth(t, primaryClient, "http://primary.invalid", harness.agent)
	if primaryHealth.PID == payloadPIDs[0] {
		t.Fatalf("payload SIGKILL restart reused PID %d", primaryHealth.PID)
	}
	attemptIDs = append(attemptIDs, primaryAfterKill.CurrentAttemptID)
	payloadPIDs = append(payloadPIDs, primaryHealth.PID)
	evidence.recordJob(t, harness, "status-after-payload-sigkill.json", primary.JobID)
	assertSiblingUnchanged(
		t, harness, siblingClient, sibling.JobID, siblingRunning.CurrentAttemptID, siblingHealth.PID,
	)

	evidence.recordJob(t, harness, "status-before-cli-restart.json", primary.JobID)
	restartResponse := runServiceCLI(t, harness, "services", "restart", primary.JobID,
		"--idempotency-key=realtiming-healthy-restart")
	evidence.write("cli-restart-response.json", restartResponse)
	primaryAfterCLIRestart := waitForFreshRunningAttempt(
		t, harness, primary.JobID, primaryAfterKill.CurrentAttemptID, 45*time.Second,
	)
	previousPID := primaryHealth.PID
	primaryHealth = waitForHealth(t, primaryClient, "http://primary.invalid", harness.agent)
	waitForProcessAbsent(t, previousPID, processrunner.DefaultTerminationGraceTime+5*time.Second)
	attemptIDs = append(attemptIDs, primaryAfterCLIRestart.CurrentAttemptID)
	payloadPIDs = append(payloadPIDs, primaryHealth.PID)
	evidence.recordJob(t, harness, "status-after-cli-restart.json", primary.JobID)
	assertSiblingUnchanged(
		t, harness, siblingClient, sibling.JobID, siblingRunning.CurrentAttemptID, siblingHealth.PID,
	)

	evidence.recordJob(t, harness, "status-before-agent-sigkill.json", primary.JobID)
	guardianStarted := time.Now()
	harness.agent.kill(t)
	waitForProcessAbsent(t, primaryHealth.PID, 10*time.Second)
	waitForProcessAbsent(t, siblingHealth.PID, 10*time.Second)
	guardianElapsed := time.Since(guardianStarted)
	if guardianElapsed >= l1.DefaultLeaseDuration {
		t.Fatalf("guardian reaping took %s, want before %s lease expiry", guardianElapsed, l1.DefaultLeaseDuration)
	}
	evidence.recordJSON("guardian-timing.json", map[string]any{
		"elapsed": guardianElapsed.String(), "effective_lease": l1.DefaultLeaseDuration.String(),
	})
	evidence.recordProcessOutput("agent-before-restart.log", harness.agent)
	assertPublishedUnavailable(t, harness, ports[0])
	assertPublishedUnavailable(t, harness, ports[1])
	harness.restartAgent(t)
	primaryAfterAgentRestart := waitForFreshRunningAttempt(
		t, harness, primary.JobID, primaryAfterCLIRestart.CurrentAttemptID, 75*time.Second,
	)
	siblingAfterAgentRestart := waitForFreshRunningAttempt(
		t, harness, sibling.JobID, siblingRunning.CurrentAttemptID, 75*time.Second,
	)
	primaryHealth = waitForHealth(t, primaryClient, "http://primary.invalid", harness.agent)
	siblingHealth = waitForHealth(t, siblingClient, "http://sibling.invalid", harness.agent)
	attemptIDs = append(attemptIDs, primaryAfterAgentRestart.CurrentAttemptID)
	payloadPIDs = append(payloadPIDs, primaryHealth.PID)
	evidence.recordJob(t, harness, "status-after-agent-restart.json", primary.JobID)
	evidence.recordJob(t, harness, "status-sibling-after-agent-restart.json", sibling.JobID)
	if siblingAfterAgentRestart.CurrentAttemptID == siblingRunning.CurrentAttemptID {
		t.Fatal("sibling did not receive a fresh attempt after agent SIGKILL")
	}

	logs := waitForAttemptLogs(t, harness, primary.JobID, attemptIDs, 30*time.Second)
	evidence.recordJSON("logs-all-attempts.json", logs)
	evidence.recordJSON("attempt-identities.json", map[string]any{
		"job_id": primary.JobID, "attempt_ids": attemptIDs, "payload_pids": payloadPIDs,
	})

	groupID, err := syscall.Getpgid(primaryHealth.PID)
	if err != nil {
		t.Fatalf("read payload process group: %v", err)
	}
	evidence.recordJob(t, harness, "status-before-cli-stop.json", primary.JobID)
	stopResponse := runServiceCLI(t, harness, "services", "stop", primary.JobID, "--wait=45s")
	evidence.write("cli-stop-response.json", stopResponse)
	stopped := harness.waitForJobState(t, primary.JobID, contract.JobClassService, contract.JobStopped, 10*time.Second)
	assertStoppedService(t, harness, stopped, primaryHealth.PID, groupID, ports[0])
	attemptCount := len(stopped.Attempts)
	restartCount := stopped.LifetimeRestartCount
	time.Sleep(l1.DefaultLeaseDuration + 2*processrunner.DefaultReadinessProbeInterval + 500*time.Millisecond)
	stillStopped := evidence.recordJob(t, harness, "status-after-stop-authority-window.json", primary.JobID)
	if stillStopped.State != contract.JobStopped || len(stillStopped.Attempts) != attemptCount ||
		stillStopped.LifetimeRestartCount != restartCount {
		t.Fatalf("stopped service restarted after authority window: %#v", stillStopped)
	}

	startResponse := runServiceCLI(t, harness, "services", "start", primary.JobID, "--wait=75s")
	evidence.write("cli-start-response.json", startResponse)
	primaryAfterStart := harness.waitForJobState(t, primary.JobID, contract.JobClassService, contract.JobRunning, 10*time.Second)
	if primaryAfterStart.CurrentAttemptID == primaryAfterAgentRestart.CurrentAttemptID {
		t.Fatal("CLI start did not create a fresh attempt")
	}
	primaryHealth = waitForHealth(t, primaryClient, "http://primary.invalid", harness.agent)
	assertEcho(t, primaryClient, "http://primary.invalid", []byte("echo-after-cli-start"))
	evidence.recordJob(t, harness, "status-after-cli-start.json", primary.JobID)
	attemptIDs = append(attemptIDs, primaryAfterStart.CurrentAttemptID)
	payloadPIDs = append(payloadPIDs, primaryHealth.PID)
	logs = waitForAttemptLogs(t, harness, primary.JobID, attemptIDs, 30*time.Second)
	evidence.recordJSON("logs-all-attempts-final.json", logs)
	evidence.recordJSON("attempt-identities-final.json", map[string]any{
		"job_id": primary.JobID, "attempt_ids": attemptIDs, "payload_pids": payloadPIDs,
	})

	runningRemoval := runServiceCLI(t, harness, "services", "remove", sibling.JobID, "--wait=90s")
	evidence.write("remove-running.json", runningRemoval)
	harness.waitForJobState(t, sibling.JobID, contract.JobClassService, contract.JobRemovedVerified, 10*time.Second)
	waitForProcessAbsent(t, siblingHealth.PID, 10*time.Second)
	assertPublishedUnavailable(t, harness, ports[1])
	assertRemovalPersistence(
		t, harness, sibling.JobID, harness.specs[sibling.JobID],
		harness.specs[sibling.JobID].Execution.SensitiveEnv["SERVICE_ACCEPTANCE_SECRET"],
	)
	assertNoServiceResidue(t, harness, sibling.JobID)

	primaryHealth = waitForHealth(t, primaryClient, "http://primary.invalid", harness.agent)
	stopResponse = runServiceCLI(t, harness, "services", "stop", primary.JobID, "--wait=45s")
	evidence.write("cli-stop-before-removal.json", stopResponse)
	harness.waitForJobState(t, primary.JobID, contract.JobClassService, contract.JobStopped, 10*time.Second)
	stoppedRemoval := runServiceCLI(t, harness, "services", "remove", primary.JobID, "--wait=90s")
	evidence.write("remove-stopped.json", stoppedRemoval)
	harness.waitForJobState(t, primary.JobID, contract.JobClassService, contract.JobRemovedVerified, 10*time.Second)
	assertRemovalPersistence(
		t, harness, primary.JobID, harness.specs[primary.JobID],
		harness.specs[primary.JobID].Execution.SensitiveEnv["SERVICE_ACCEPTANCE_SECRET"],
	)
	var primaryReplay l1.Job
	replayStatus, replayBody := harness.doJSON(t, http.MethodPost, "/v1/jobs", harness.specs[primary.JobID], &primaryReplay)
	if replayStatus != http.StatusOK || primaryReplay.JobID != primary.JobID {
		t.Fatalf("stopped removal create replay = status %d body=%s", replayStatus, replayBody)
	}
	evidence.write("create-stopped-removal-replay.json", replayBody)
	assertNoServiceResidue(t, harness, primary.JobID)

	failed := harness.submitFailedService(t)
	failedState := harness.waitForJobState(t, failed.JobID, contract.JobClassService, contract.JobFailed, 45*time.Second)
	evidence.recordJSON("status-latched-failed.json", failedState)
	failedRemoval := runServiceCLI(t, harness, "services", "remove", failed.JobID, "--wait=90s")
	evidence.write("remove-latched-failed.json", failedRemoval)
	harness.waitForJobState(t, failed.JobID, contract.JobClassService, contract.JobRemovedVerified, 10*time.Second)
	assertNoServiceResidue(t, harness, failed.JobID)

	offline, offlineCreate := harness.submitEchoServiceWithDispatchKey(t, ports[3], "realtiming-offline")
	evidence.write("create-offline.json", offlineCreate)
	offlineClient := harness.publishedHTTPClient(t, ports[3])
	offlineHealth := waitForHealth(t, offlineClient, "http://offline.invalid", harness.agent)
	harness.waitForJobState(t, offline.JobID, contract.JobClassService, contract.JobRunning, 45*time.Second)
	offlineRoot := managedServiceRoot(harness, offline.JobID)
	offlineKillStarted := time.Now()
	harness.agent.kill(t)
	waitForProcessAbsent(t, offlineHealth.PID, 10*time.Second)
	offlineGuardianElapsed := time.Since(offlineKillStarted)
	if offlineGuardianElapsed >= l1.DefaultLeaseDuration {
		t.Fatalf("offline-removal guardian reap took %s", offlineGuardianElapsed)
	}
	evidence.recordJSON("guardian-offline-removal-timing.json", map[string]any{
		"elapsed": offlineGuardianElapsed.String(), "effective_lease": l1.DefaultLeaseDuration.String(),
	})
	pendingRemoval := runServiceCLI(t, harness, "services", "remove", offline.JobID)
	evidence.write("remove-offline-pending.json", pendingRemoval)
	time.Sleep(l1.DefaultLeaseDuration + time.Second)
	pending := evidence.recordJob(t, harness, "status-offline-still-pending.json", offline.JobID)
	if pending.State != contract.JobRemovalPending || strings.Contains(pending.Status, "clean") {
		t.Fatalf("offline removal projection is not truthfully pending: %#v", pending)
	}
	if _, err := os.Stat(offlineRoot); err != nil {
		t.Fatalf("offline removal deleted managed root before node returned: %v", err)
	}
	forgottenResponse := runServiceCLI(t, harness, "services", "forget", offline.JobID, "--force")
	evidence.write("force-forget-offline.json", forgottenResponse)
	harness.restartAgent(t)
	forgotten := waitForForgottenCleanup(t, harness, offline.JobID, offlineRoot, 45*time.Second)
	evidence.recordJSON("force-forgotten-after-return.json", forgotten)
	tombstoneBefore := readRemovalTombstone(t, harness.l1Database, offline.JobID)
	evidence.recordJSON("removal-tombstone-before-ack-replay.json", tombstoneBefore)
	replayed := replayFinalizedAcknowledgement(t, harness, offline.JobID, tombstoneBefore)
	evidence.recordJSON("removal-ack-replay-response.json", replayed)
	tombstoneAfter := readRemovalTombstone(t, harness.l1Database, offline.JobID)
	evidence.recordJSON("removal-tombstone-after-ack-replay.json", tombstoneAfter)
	if tombstoneAfter != tombstoneBefore {
		t.Fatalf("finalized acknowledgement replay changed tombstone: before %#v after %#v",
			tombstoneBefore, tombstoneAfter)
	}
	assertNoServiceResidue(t, harness, offline.JobID)

	allJobIDs := []string{primary.JobID, sibling.JobID, failed.JobID, offline.JobID}
	for _, jobID := range allJobIDs {
		assertWorkingDirectoryUntouched(t, harness.workingDirectories[jobID])
		assertManagedServiceAbsent(t, harness, jobID)
	}
	assertHandoffRootEmpty(t, harness.handoffRoot)
	for _, port := range ports {
		assertPublishedUnavailable(t, harness, port)
	}
	assertSpoolRowsAbsent(t, harness.spoolDirectory, allJobIDs)
	evidence.recordResidue(t, harness)
}

func assertProductionTimingDefaults(t *testing.T) {
	t.Helper()
	checks := []struct {
		name     string
		actual   time.Duration
		expected time.Duration
	}{
		{name: "effective lease", actual: l1.DefaultLeaseDuration, expected: 30 * time.Second},
		{name: "heartbeat", actual: agent.DefaultHeartbeatInterval, expected: 15 * time.Second},
		{name: "renewal", actual: agent.DefaultRenewalInterval, expected: 10 * time.Second},
		{name: "startup readiness", actual: processrunner.DefaultStartupReadinessDeadline, expected: 30 * time.Second},
		{name: "readiness probe", actual: processrunner.DefaultReadinessProbeInterval, expected: 250 * time.Millisecond},
		{name: "termination grace", actual: processrunner.DefaultTerminationGraceTime, expected: 5 * time.Second},
	}
	for _, check := range checks {
		if check.actual != check.expected {
			t.Fatalf("production %s default = %s, want %s", check.name, check.actual, check.expected)
		}
	}
}

func assertCreateHasNoRunID(t *testing.T, body []byte) {
	t.Helper()
	var response map[string]json.RawMessage
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatal(err)
	}
	if _, exists := response["run_id"]; exists {
		t.Fatalf("service create response carried forbidden run_id: %s", body)
	}
}

func assertServiceListParity(
	t *testing.T,
	harness *acceptanceHarness,
	evidence *realTimingEvidence,
	want ...l1.Job,
) {
	t.Helper()
	var page l1.JobList
	status, body := harness.doJSON(t, http.MethodGet, "/v1/jobs?class=service&limit=100", nil, &page)
	if status != http.StatusOK {
		t.Fatalf("list services status = %d body=%s", status, body)
	}
	evidence.write("services-list-before-disruption.json", body)
	byID := make(map[string]l1.Job, len(page.Jobs))
	for _, job := range page.Jobs {
		byID[job.JobID] = job
	}
	for _, expected := range want {
		actual, exists := byID[expected.JobID]
		if !exists || actual.CurrentAttemptID != expected.CurrentAttemptID || actual.State != contract.JobRunning ||
			actual.Ready == nil || !*actual.Ready {
			t.Fatalf("service list parity for %q = %#v", expected.JobID, actual)
		}
	}
}

func waitForRestartPending(t *testing.T, harness *acceptanceHarness, jobID string, timeout time.Duration) l1.Job {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var job l1.Job
		status, body := harness.doJSON(t, http.MethodGet, "/v1/jobs/"+jobID+"?class=service", nil, &job)
		if status != http.StatusOK {
			t.Fatalf("read restart-pending service = %d body=%s", status, body)
		}
		if job.State == contract.JobQueued && job.Status == "restart-pending" && job.NextRestartAt != nil &&
			job.Ready != nil && !*job.Ready {
			return job
		}
		if job.State == contract.JobFailed {
			t.Fatalf("payload SIGKILL latched service instead of requeueing: %s", body)
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("service %q never exposed restart-pending", jobID)
	return l1.Job{}
}

func assertSiblingUnchanged(
	t *testing.T,
	harness *acceptanceHarness,
	client *http.Client,
	jobID string,
	attemptID string,
	payloadPID int,
) {
	t.Helper()
	current := waitForHealth(t, client, "http://sibling.invalid", harness.agent)
	if current.PID != payloadPID {
		t.Fatalf("sibling payload changed from %d to %d", payloadPID, current.PID)
	}
	var sibling l1.Job
	status, body := harness.doJSON(t, http.MethodGet, "/v1/jobs/"+jobID+"?class=service", nil, &sibling)
	if status != http.StatusOK {
		t.Fatalf("check sibling status = %d body=%s", status, body)
	}
	if sibling.CurrentAttemptID != attemptID || sibling.State != contract.JobRunning {
		t.Fatalf("sibling attempt changed from %q: %#v", attemptID, sibling)
	}
}

func assertPublishedUnavailable(t *testing.T, harness *acceptanceHarness, port int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	connection, err := harness.dialPublished(ctx, port)
	if err == nil {
		_ = connection.Close()
		t.Fatalf("published port %d still accepted an identity-bearing connection", port)
	}
}

func waitForAttemptLogs(
	t *testing.T,
	harness *acceptanceHarness,
	jobID string,
	attemptIDs []string,
	timeout time.Duration,
) l1.LogPage {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var page l1.LogPage
		status, body := harness.doJSON(t, http.MethodGet,
			"/v1/jobs/"+jobID+"/logs?class=service&limit=1000", nil, &page)
		if status != http.StatusOK {
			t.Fatalf("read service logs status = %d body=%s", status, body)
		}
		seen := make(map[string]bool, len(attemptIDs))
		for _, event := range page.Events {
			seen[event.AttemptID] = true
		}
		complete := true
		for _, attemptID := range attemptIDs {
			complete = complete && seen[attemptID]
		}
		if complete {
			return page
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("logs did not contain every attempt %v", attemptIDs)
	return l1.LogPage{}
}

func assertStoppedService(
	t *testing.T,
	harness *acceptanceHarness,
	job l1.Job,
	payloadPID int,
	processGroupID int,
	port int,
) {
	t.Helper()
	if job.State != contract.JobStopped || job.DesiredState != contract.ServiceDesiredStopped || job.SlotHeld {
		t.Fatalf("stopped service projection = %#v", job)
	}
	waitForProcessAbsent(t, payloadPID, 10*time.Second)
	if err := syscall.Kill(-processGroupID, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("payload process group %d survived stop: %v", processGroupID, err)
	}
	assertPublishedUnavailable(t, harness, port)
}

func runServiceCLI(t *testing.T, harness *acceptanceHarness, arguments ...string) []byte {
	t.Helper()
	args := []string{
		"--fabric=plain",
		"--l1=" + harness.controlPlaneAddress,
		"--plain-identity=realtiming-cli",
		"--json",
	}
	args = append(args, arguments...)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	command := exec.CommandContext(ctx, weftyBinaryPath, args...)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("run wefty %v: %v\n%s", arguments, err, output)
	}
	return output
}

func waitForForgottenCleanup(
	t *testing.T,
	harness *acceptanceHarness,
	jobID, serviceRoot string,
	timeout time.Duration,
) l1.Job {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var job l1.Job
		status, body := harness.doJSON(t, http.MethodGet, "/v1/jobs/"+jobID+"?class=service", nil, &job)
		if status != http.StatusOK {
			t.Fatalf("read force-forgotten cleanup = %d body=%s", status, body)
		}
		if job.State == contract.JobForgottenCleanupUnverified && job.Removal != nil &&
			job.Removal.CleanupAcknowledgedAt != nil {
			if _, err := os.Lstat(serviceRoot); !os.IsNotExist(err) {
				t.Fatalf("force-forgotten cleanup acknowledged before root absence: %v", err)
			}
			return job
		}
		if harness.agent.exited() {
			t.Fatalf("returning agent exited during force-forgotten cleanup: %v\n%s",
				harness.agent.waitError(), harness.agent.outputString())
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("force-forgotten cleanup did not complete for %q", jobID)
	return l1.Job{}
}

type removalTombstone struct {
	DispatchKeyHash       string `json:"dispatch_key_hash"`
	RequestHash           string `json:"request_hash"`
	CreatedNS             int64  `json:"created_ns"`
	RemovalRequestedNS    int64  `json:"removal_requested_ns"`
	RemovedNS             int64  `json:"removed_ns"`
	Outcome               string `json:"outcome"`
	LastBoundNodeID       string `json:"last_bound_node_id"`
	RemovalGeneration     uint64 `json:"removal_generation"`
	RootInstanceID        string `json:"root_instance_id"`
	CleanupAcknowledgedNS int64  `json:"cleanup_acknowledged_ns"`
}

func readRemovalTombstone(t *testing.T, databasePath, jobID string) removalTombstone {
	t.Helper()
	database, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	var tombstone removalTombstone
	if err := database.QueryRow(`SELECT dispatch_key_hash, request_hash, created_ns, removal_requested_ns,
		removed_ns, outcome, last_bound_node_id, removal_generation, root_instance_id,
		cleanup_acknowledged_ns FROM service_tombstones WHERE job_id=?`, jobID).Scan(
		&tombstone.DispatchKeyHash, &tombstone.RequestHash, &tombstone.CreatedNS,
		&tombstone.RemovalRequestedNS, &tombstone.RemovedNS, &tombstone.Outcome,
		&tombstone.LastBoundNodeID, &tombstone.RemovalGeneration, &tombstone.RootInstanceID,
		&tombstone.CleanupAcknowledgedNS,
	); err != nil {
		t.Fatal(err)
	}
	return tombstone
}

func replayFinalizedAcknowledgement(
	t *testing.T,
	harness *acceptanceHarness,
	jobID string,
	tombstone removalTombstone,
) l1.Job {
	t.Helper()
	database, err := sql.Open("sqlite", harness.l1Database)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	var bootSessionID string
	if err := database.QueryRow(`SELECT boot_session_id FROM nodes WHERE node_id=?`,
		tombstone.LastBoundNodeID).Scan(&bootSessionID); err != nil {
		t.Fatal(err)
	}
	requestBody := l1.RemovalAcknowledgementRequest{
		NodeID: tombstone.LastBoundNodeID, BootSessionID: bootSessionID,
		RemovalGeneration: tombstone.RemovalGeneration,
		CleanupFence:      "not-retained-after-finalization", RootInstanceID: tombstone.RootInstanceID,
		IdempotencyKey: "replayed-after-finalization",
	}
	payload, err := json.Marshal(requestBody)
	if err != nil {
		t.Fatal(err)
	}
	participant := plain.NewNetwork().NewFabric(fabric.Identity{
		NodeID: "acceptance-agent", Tags: []string{l1.DefaultAgentPrincipalTag},
	})
	client := &http.Client{Transport: &http.Transport{DialContext: participant.Dial}}
	defer client.CloseIdleConnections()
	httpRequest, err := http.NewRequest(http.MethodPost,
		"http://"+harness.controlPlaneAddress+"/v1/agent/jobs/"+jobID+"/removal-acknowledgement",
		strings.NewReader(string(payload)))
	if err != nil {
		t.Fatal(err)
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	response, err := client.Do(httpRequest)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var replayed l1.Job
	if response.StatusCode != http.StatusOK {
		t.Fatalf("post-finalization acknowledgement replay status = %d", response.StatusCode)
	}
	if err := json.NewDecoder(response.Body).Decode(&replayed); err != nil {
		t.Fatal(err)
	}
	if replayed.State != contract.JobForgottenCleanupUnverified || replayed.Removal == nil ||
		replayed.Removal.CleanupAcknowledgedAt == nil {
		t.Fatalf("post-finalization acknowledgement replay = %#v", replayed)
	}
	return replayed
}

func assertSpoolRowsAbsent(t *testing.T, spoolDirectory string, jobIDs []string) {
	t.Helper()
	spoolPath := findSpoolDatabase(t, spoolDirectory)
	database, err := sql.Open("sqlite", spoolPath)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	for _, jobID := range jobIDs {
		var attempts int
		if err := database.QueryRow(`SELECT COUNT(*) FROM spool_attempts WHERE job_id=?`, jobID).Scan(&attempts); err != nil {
			t.Fatal(err)
		}
		if attempts != 0 {
			t.Fatalf("service %q retained %d spool attempts", jobID, attempts)
		}
	}
}

func findSpoolDatabase(t *testing.T, directory string) string {
	t.Helper()
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".sqlite" {
			return filepath.Join(directory, entry.Name())
		}
	}
	t.Fatal("agent spool database was not created")
	return ""
}

type realTimingEvidence struct {
	t         *testing.T
	directory string
}

func newRealTimingEvidence(t *testing.T) *realTimingEvidence {
	t.Helper()
	directory := os.Getenv(realTimingEvidenceEnvironment)
	if directory == "" {
		directory = t.TempDir()
	}
	if err := os.MkdirAll(directory, 0o755); err != nil {
		t.Fatal(err)
	}
	evidence := &realTimingEvidence{t: t, directory: directory}
	evidence.write("deviations.txt", []byte(
		"fabric=plain on loopback; real tailnet DNS/ACL and second-physical-peer reachability are not covered\n"+
			"GitHub-hosted runner; native-hardware evidence is not covered\n",
	))
	return evidence
}

func (evidence *realTimingEvidence) recordMetadata(t *testing.T) {
	t.Helper()
	commit, err := exec.Command("git", "rev-parse", "HEAD").Output()
	if err != nil {
		t.Fatal(err)
	}
	platform, err := exec.Command("uname", "-a").Output()
	if err != nil {
		t.Fatal(err)
	}
	evidence.recordJSON("metadata.json", map[string]any{
		"commit":   strings.TrimSpace(string(commit)),
		"platform": strings.TrimSpace(string(platform)),
		"goos":     runtime.GOOS,
		"goarch":   runtime.GOARCH,
		"timings": map[string]string{
			"lease":             l1.DefaultLeaseDuration.String(),
			"heartbeat":         agent.DefaultHeartbeatInterval.String(),
			"renewal":           agent.DefaultRenewalInterval.String(),
			"startup_readiness": processrunner.DefaultStartupReadinessDeadline.String(),
			"probe":             processrunner.DefaultReadinessProbeInterval.String(),
			"termination_grace": processrunner.DefaultTerminationGraceTime.String(),
		},
	})
}

func (evidence *realTimingEvidence) recordJob(
	t *testing.T,
	harness *acceptanceHarness,
	name, jobID string,
) l1.Job {
	t.Helper()
	var job l1.Job
	status, body := harness.doJSON(t, http.MethodGet, "/v1/jobs/"+jobID+"?class=service", nil, &job)
	if status != http.StatusOK {
		t.Fatalf("record service status = %d body=%s", status, body)
	}
	evidence.write(name, body)
	return job
}

func (evidence *realTimingEvidence) recordResidue(t *testing.T, harness *acceptanceHarness) {
	t.Helper()
	database, err := sql.Open("sqlite", harness.l1Database)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	counts := map[string]int{}
	for _, table := range []string{"jobs", "attempts", "log_events", "service_tombstones", "service_removals"} {
		var count int
		if err := database.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil {
			t.Fatal(err)
		}
		counts[table] = count
	}
	info, err := os.Stat(harness.l1Database)
	if err != nil {
		t.Fatal(err)
	}
	evidence.recordJSON("residue-after-removal.json", map[string]any{
		"l1_database_bytes": info.Size(), "row_counts": counts,
	})
}

func (evidence *realTimingEvidence) recordProcessOutput(name string, process *managedProcess) {
	if process == nil {
		return
	}
	evidence.write(name, []byte(process.outputString()))
}

func (evidence *realTimingEvidence) recordJSON(name string, value any) {
	payload, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		evidence.t.Errorf("encode evidence %s: %v", name, err)
		return
	}
	payload = append(payload, '\n')
	evidence.write(name, payload)
}

func (evidence *realTimingEvidence) write(name string, payload []byte) {
	if err := os.WriteFile(filepath.Join(evidence.directory, name), payload, 0o600); err != nil {
		evidence.t.Errorf("write evidence %s: %v", name, err)
	}
}
