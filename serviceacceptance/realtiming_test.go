//go:build service_acceptance_realtiming && (darwin || linux)

package serviceacceptance

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	"github.com/Derek-X-Wang/wefty/runner/lima"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
	processrunner "github.com/Derek-X-Wang/wefty/runner/process"
)

const realTimingEvidenceEnvironment = "WEFTY_REALTIME_EVIDENCE_DIR"

func TestOCIPrestartRuntimeUnavailablePersistsWallClockBackoffAtProductionTimings(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "oci-realtime.sqlite")
	store, err := l1.OpenStore(databasePath, l1.StoreOptions{
		LeaseDuration: l1.DefaultLeaseDuration,
		Jitter:        func(delay time.Duration) time.Duration { return delay },
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close realtime OCI store: %v", err)
		}
	})
	ociSpec := contract.JobSpec{
		SchemaVersion: contract.SchemaVersionV1, DispatchKey: "realtime-oci-prestart", Kind: contract.JobKindOCI, Class: contract.JobClassOneShot,
		RuntimeHandler: "io.containerd.runc.v2",
		Execution:      contract.ExecutionSpec{OCI: &contract.OCIExecutionSpec{Image: contract.OCIImageSpec{Reference: "ghcr.io/example/tool:latest"}}},
	}
	job, _, err := store.CreateJob(context.Background(), ociSpec)
	if err != nil {
		t.Fatal(err)
	}
	database, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	registration := contract.NodeRegistration{
		NodeID: "oci-realtime-node", BootSessionID: "oci-realtime-boot", OS: "linux", Architecture: runtime.GOARCH, AgentVersion: "acceptance",
		Capabilities: map[string]bool{
			"kind:oci":                              true,
			"runtime_handler:io.containerd.runc.v2": true,
		},
		CapabilityRevision: 1, CapabilityObservedAt: time.Now().UTC(), MissingCapabilities: []string{},
	}
	if _, err := store.RegisterNode(context.Background(), fabric.Identity{NodeID: "oci-realtime-agent"}, registration, l1.DefaultNodePolicy(), true); err != nil {
		t.Fatal(err)
	}
	first, err := store.ClaimJob(context.Background(), "oci-realtime-agent", registration.NodeID, registration.BootSessionID, contract.JobClassOneShot)
	if err != nil || first == nil {
		t.Fatalf("first OCI claim = %#v err %v", first, err)
	}
	if first.Lease.LeaseTTL != l1.DefaultLeaseDuration {
		t.Fatalf("lease TTL = %s, want production %s", first.Lease.LeaseTTL, l1.DefaultLeaseDuration)
	}
	requeued, err := store.CompleteAttempt(context.Background(), "oci-realtime-agent", job.JobID, first.Lease.AttemptID, l1.CompletionRequest{
		FencingToken: first.Lease.FencingToken, IdempotencyKey: "realtime-runtime-unavailable",
		Result: l1.ProcessResult{SpawnError: &contract.SpawnFailure{Code: contract.SpawnFailureRuntimeUnavailable, Message: "engine unavailable"}},
	})
	if err != nil || requeued.State != contract.JobQueued || requeued.CurrentAttemptID != "" || requeued.NodeID != "" {
		t.Fatalf("pre-start requeue = %#v err %v", requeued, err)
	}
	var nextRetryNS int64
	if err := database.QueryRow(`SELECT prestart_next_retry_at_ns FROM jobs WHERE job_id=?`, job.JobID).Scan(&nextRetryNS); err != nil {
		t.Fatal(err)
	}
	if immediate, err := store.ClaimJob(context.Background(), "oci-realtime-agent", registration.NodeID, registration.BootSessionID, contract.JobClassOneShot); err != nil || immediate != nil {
		t.Fatalf("claim before persisted backoff = %#v err %v", immediate, err)
	}
	waitStarted := time.Now()
	deadline := time.Now().Add(2 * time.Second)
	var second *l1.Claim
	for time.Now().Before(deadline) {
		second, err = store.ClaimJob(context.Background(), "oci-realtime-agent", registration.NodeID, registration.BootSessionID, contract.JobClassOneShot)
		if err != nil {
			t.Fatal(err)
		}
		if second != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if second == nil {
		t.Fatal("OCI job was not claimable after persisted backoff")
	}
	if nowNS := time.Now().UnixNano(); nowNS < nextRetryNS {
		t.Fatalf("second claim at %d preceded persisted due time %d", nowNS, nextRetryNS)
	}
	if elapsed := time.Since(waitStarted); elapsed < 900*time.Millisecond {
		t.Fatalf("wall-clock backoff = %s, want approximately one second", elapsed)
	}
}

func TestServiceLifecycleAndRemovalAtProductionTimings(t *testing.T) {
	assertProductionTimingDefaults(t)
	evidence := newRealTimingEvidence(t)
	var agentArguments []string
	if runtime.GOOS == "linux" {
		intentPath := filepath.Join(t.TempDir(), "oci-intent.json")
		if _, err := lima.InitializeOCIIntent(intentPath, time.Now()); err != nil {
			t.Fatal(err)
		}
		for _, value := range []struct{ name, flag string }{
			{"WEFTY_OCI_HELPER_SOCKET", "--oci-helper-socket="},
			{"WEFTY_OCI_HELPER_CHECKSUM", "--oci-helper-checksum="},
			{"WEFTY_OCI_PROBE_REFERENCE", "--oci-probe-image="},
			{"WEFTY_OCI_PROBE_DIGEST", "--oci-probe-digest="},
		} {
			setting := os.Getenv(value.name)
			if setting == "" {
				t.Fatalf("Linux OCI removal acceptance requires %s", value.name)
			}
			agentArguments = append(agentArguments, value.flag+setting)
		}
		archivePath := os.Getenv("WEFTY_OCI_PROBE_ARCHIVE")
		if archivePath == "" {
			t.Fatal("Linux OCI removal acceptance requires WEFTY_OCI_PROBE_ARCHIVE")
		}
		importRealtimeProbeImage(t, archivePath,
			os.Getenv("WEFTY_OCI_HELPER_SOCKET"), os.Getenv("WEFTY_OCI_HELPER_CHECKSUM"),
			os.Getenv("WEFTY_OCI_PROBE_REFERENCE"), os.Getenv("WEFTY_OCI_PROBE_DIGEST"), nil)
		agentArguments = append(agentArguments, "--oci-intent-file="+intentPath)
	}
	harness := newAcceptanceHarnessWithOptions(t, acceptanceHarnessOptions{
		leaseDuration:     l1.DefaultLeaseDuration,
		productionTimings: true,
		agentArguments:    agentArguments,
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
	leaseLossCount := stopped.LeaseLossCount
	time.Sleep(l1.DefaultLeaseDuration + 2*processrunner.DefaultReadinessProbeInterval + 500*time.Millisecond)
	stillStopped := evidence.recordJob(t, harness, "status-after-stop-authority-window.json", primary.JobID)
	if stillStopped.State != contract.JobStopped || len(stillStopped.Attempts) != attemptCount ||
		stillStopped.LifetimeRestartCount != restartCount || stillStopped.LeaseLossCount != leaseLossCount {
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

	runningRemovalCtx, cancelRunningRemoval := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancelRunningRemoval()
	runningRemoval := runServiceCLIContext(t, runningRemovalCtx, harness, "services", "remove", sibling.JobID, "--wait=90s")
	evidence.write("remove-running.json", runningRemoval)
	assertRemovalVerified(t, runningRemovalCtx, harness, sibling.JobID)
	assertRemovalPersistence(
		t, runningRemovalCtx, harness, sibling.JobID, harness.specs[sibling.JobID],
		harness.specs[sibling.JobID].Execution.SensitiveEnv["SERVICE_ACCEPTANCE_SECRET"],
	)
	waitForProcessAbsent(t, siblingHealth.PID, 10*time.Second)
	assertPublishedUnavailable(t, harness, ports[1])
	assertNoServiceResidue(t, harness, sibling.JobID)

	primaryHealth = waitForHealth(t, primaryClient, "http://primary.invalid", harness.agent)
	stopResponse = runServiceCLI(t, harness, "services", "stop", primary.JobID, "--wait=45s")
	evidence.write("cli-stop-before-removal.json", stopResponse)
	harness.waitForJobState(t, primary.JobID, contract.JobClassService, contract.JobStopped, 10*time.Second)
	stoppedRemovalCtx, cancelStoppedRemoval := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancelStoppedRemoval()
	stoppedRemoval := runServiceCLIContext(t, stoppedRemovalCtx, harness, "services", "remove", primary.JobID, "--wait=90s")
	evidence.write("remove-stopped.json", stoppedRemoval)
	assertRemovalVerified(t, stoppedRemovalCtx, harness, primary.JobID)
	assertRemovalPersistence(
		t, stoppedRemovalCtx, harness, primary.JobID, harness.specs[primary.JobID],
		harness.specs[primary.JobID].Execution.SensitiveEnv["SERVICE_ACCEPTANCE_SECRET"],
	)
	var primaryReplay l1.Job
	replayStatus, replayBody := harness.doJSON(t, http.MethodPost, "/v1/jobs", harness.specs[primary.JobID], &primaryReplay)
	if replayStatus != http.StatusOK || primaryReplay.JobID != primary.JobID {
		t.Fatalf("stopped removal create replay = status %d body=%s", replayStatus, replayBody)
	}
	evidence.write("create-stopped-removal-replay.json", replayBody)
	assertNoServiceResidue(t, harness, primary.JobID)

	var failed l1.Job
	if runtime.GOOS == "linux" {
		failed = harness.submitFailedOCIService(t)
	} else {
		failed = harness.submitFailedService(t)
	}
	failedState := harness.waitForJobState(t, failed.JobID, contract.JobClassService, contract.JobFailed, 45*time.Second)
	evidence.recordJSON("status-latched-failed.json", failedState)
	if runtime.GOOS == "linux" {
		assertCurrentRuntimeServiceManifest(t, harness.spoolDirectory, failed.JobID)
	}
	failedRemoval := runServiceCLI(t, harness, "services", "remove", failed.JobID, "--wait=90s")
	evidence.write("remove-latched-failed.json", failedRemoval)
	harness.waitForJobState(t, failed.JobID, contract.JobClassService, contract.JobRemovedVerified, 10*time.Second)
	assertNoServiceResidue(t, harness, failed.JobID)

	var backoff l1.Job
	if runtime.GOOS == "linux" {
		backoff = harness.submitBackoffService(t)
		backoffState := waitForRestartPending(t, harness, backoff.JobID, 45*time.Second)
		evidence.recordJSON("status-removal-from-backoff.json", backoffState)
		assertCurrentRuntimeServiceManifest(t, harness.spoolDirectory, backoff.JobID)
		backoffRemoval := runServiceCLI(t, harness, "services", "remove", backoff.JobID, "--wait=90s")
		evidence.write("remove-backoff.json", backoffRemoval)
		harness.waitForJobState(t, backoff.JobID, contract.JobClassService, contract.JobRemovedVerified, 10*time.Second)
		assertNoServiceResidue(t, harness, backoff.JobID)
	}

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
	forgotten, tombstoneBefore := waitForForgottenCleanup(t, harness, offline.JobID, offlineRoot, 45*time.Second)
	evidence.recordJSON("force-forgotten-after-return.json", forgotten)
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
	if backoff.JobID != "" {
		allJobIDs = append(allJobIDs, backoff.JobID)
	}
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

func importRealtimeProbeImage(t *testing.T, archivePath, helperSocket, helperChecksum, reference, digest string, recordResidue func(*ocihelper.NamespaceResidueError)) {
	t.Helper()
	client := ocihelper.NewUnixClient(helperSocket, helperChecksum)
	barrier, err := ocihelper.NewBootBarrier(client, ocihelper.AcquireSessionRequest{
		NodeID: "service-acceptance-provisioner", BootSessionID: "service-acceptance-provisioner",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer barrier.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	if err := barrier.Ensure(ctx); err != nil {
		var residue *ocihelper.NamespaceResidueError
		if recordResidue != nil && errors.As(err, &residue) {
			recordResidue(residue)
		}
		t.Fatal(err)
	}
	session, err := barrier.Session()
	if err != nil {
		t.Fatal(err)
	}
	archive, err := os.Open(archivePath)
	if err != nil {
		t.Fatal(err)
	}
	var imported ocihelper.EnsureImageResponse
	importErr := session.ImportImage(ctx, ocihelper.EnsureImageRequest{
		Reference: reference, Digest: digest,
		Platform:         ocihelper.OCIPlatform{OS: "linux", Architecture: runtime.GOARCH},
		OperationTimeout: 2 * time.Minute,
	}, archive, func(event ocihelper.EnsureImageEvent) error {
		if event.Result != nil {
			imported = *event.Result
		}
		return nil
	})
	closeErr := archive.Close()
	if importErr != nil || closeErr != nil {
		t.Fatal(errors.Join(importErr, closeErr))
	}
	if imported.TopLevelDigest != digest || imported.PlatformDigest == "" {
		t.Fatalf("realtiming probe import = %+v, want top-level digest %s and a platform digest", imported, digest)
	}
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
		if restartPendingObserved(job) {
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
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	return runServiceCLIContext(t, ctx, harness, arguments...)
}

func runServiceCLIContext(t *testing.T, ctx context.Context, harness *acceptanceHarness, arguments ...string) []byte {
	t.Helper()
	args := []string{
		"--fabric=plain",
		"--l1=" + harness.controlPlaneAddress,
		"--plain-identity=realtiming-cli",
		"--json",
	}
	args = append(args, arguments...)
	command := exec.CommandContext(ctx, weftyBinaryPath, args...)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("run wefty %v: %v\n%s", arguments, err, output)
	}
	if err := ctx.Err(); err != nil {
		t.Fatal(err)
	}
	return output
}

func waitForForgottenCleanup(
	t *testing.T,
	harness *acceptanceHarness,
	jobID, serviceRoot string,
	timeout time.Duration,
) (l1.Job, removalTombstone) {
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

			// GET can observe the acknowledgement commit before the separate
			// finalization commit. Observe both within this same cleanup deadline.
			tombstone, finalized := readRemovalTombstoneState(t, harness.l1Database, jobID)
			if finalized {
				if job.Removal.RemovalOutcome != l1.ServiceRemovalForgotten ||
					tombstone.Outcome != string(l1.ServiceRemovalForgotten) ||
					tombstone.LastBoundNodeID != job.Removal.RemovalBoundNodeID ||
					tombstone.RemovalGeneration != job.Removal.RemovalGeneration ||
					tombstone.CleanupAcknowledgedNS != job.Removal.CleanupAcknowledgedAt.UnixNano() {
					t.Fatalf("finalized forgotten tombstone differs from acknowledged job: job=%#v tombstone=%#v", job, tombstone)
				}
				return job, tombstone
			}
		}
		if harness.agent.exited() {
			t.Fatalf("returning agent exited during force-forgotten cleanup: %v\n%s",
				harness.agent.waitError(), harness.agent.outputString())
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("force-forgotten cleanup did not complete for %q", jobID)
	return l1.Job{}, removalTombstone{}
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
	tombstone, finalized := readRemovalTombstoneState(t, databasePath, jobID)
	if !finalized {
		t.Fatalf("removal tombstone for %q has not finalized cleanup acknowledgement", jobID)
	}
	return tombstone
}

// A force-forgotten tombstone exists before late cleanup finalizes. NULL is
// pending evidence, never a zero acknowledgement or permission to replay.
func readRemovalTombstoneState(t *testing.T, databasePath, jobID string) (removalTombstone, bool) {
	t.Helper()
	database, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	var tombstone removalTombstone
	var acknowledged sql.NullInt64
	if err := database.QueryRow(`SELECT dispatch_key_hash, request_hash, created_ns, removal_requested_ns,
		removed_ns, outcome, last_bound_node_id, removal_generation, root_instance_id,
		cleanup_acknowledged_ns FROM service_tombstones WHERE job_id=?`, jobID).Scan(
		&tombstone.DispatchKeyHash, &tombstone.RequestHash, &tombstone.CreatedNS,
		&tombstone.RemovalRequestedNS, &tombstone.RemovedNS, &tombstone.Outcome,
		&tombstone.LastBoundNodeID, &tombstone.RemovalGeneration, &tombstone.RootInstanceID,
		&acknowledged,
	); err != nil {
		t.Fatal(err)
	}
	if !acknowledged.Valid {
		return removalTombstone{}, false
	}
	tombstone.CleanupAcknowledgedNS = acknowledged.Int64
	return tombstone, true
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
		var attempts, runtimeAttempts, runtimeServices, runtimeRemovals int
		if err := database.QueryRow(`SELECT COUNT(*) FROM spool_attempts WHERE job_id=?`, jobID).Scan(&attempts); err != nil {
			t.Fatal(err)
		}
		if attempts != 0 {
			t.Fatalf("service %q retained %d spool attempts", jobID, attempts)
		}
		for query, destination := range map[string]*int{
			`SELECT COUNT(*) FROM runtime_attempt_manifests WHERE job_id=?`: &runtimeAttempts,
			`SELECT COUNT(*) FROM runtime_service_manifests WHERE job_id=?`: &runtimeServices,
			`SELECT COUNT(*) FROM runtime_removal_manifests WHERE job_id=?`: &runtimeRemovals,
		} {
			if err := database.QueryRow(query, jobID).Scan(destination); err != nil {
				t.Fatal(err)
			}
		}
		if runtimeAttempts != 0 || runtimeServices != 0 || runtimeRemovals != 0 {
			t.Fatalf("service %q retained runtime rows attempts=%d current=%d removals=%d", jobID, runtimeAttempts, runtimeServices, runtimeRemovals)
		}
	}
}

func assertCurrentRuntimeServiceManifest(t *testing.T, spoolDirectory, jobID string) {
	t.Helper()
	database, err := sql.Open("sqlite", findSpoolDatabase(t, spoolDirectory))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	var payload []byte
	if err := database.QueryRow(`SELECT manifest_json FROM runtime_service_manifests WHERE job_id=?`, jobID).Scan(&payload); err != nil {
		t.Fatalf("service %q has no bounded current runtime manifest: %v", jobID, err)
	}
	var manifest struct {
		JobID                  string `json:"job_id"`
		AttemptID              string `json:"attempt_id"`
		ServiceDataVolume      string `json:"service_data_volume"`
		ServiceDataOwnerRecord string `json:"service_data_owner_record"`
	}
	if err := json.Unmarshal(payload, &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.JobID != jobID || manifest.AttemptID == "" || manifest.ServiceDataVolume == "" || manifest.ServiceDataOwnerRecord == "" {
		t.Fatalf("service %q current runtime manifest is incomplete: %+v", jobID, manifest)
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

// This exercises the acceptance reader against real L1 transactions, without
// an agent or background reconciler; each durable phase is explicitly driven.
func TestForgottenTombstoneObservationRequiresFinalization(t *testing.T) {
	path := filepath.Join(t.TempDir(), "forgotten.sqlite")
	store, err := l1.OpenStore(path, l1.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	registration := contract.NodeRegistration{NodeID: "node", BootSessionID: "boot", RootInstanceID: "root",
		OS: "linux", Architecture: runtime.GOARCH, AgentVersion: "test",
		Capabilities: map[string]bool{"kind:process": true}, CapabilityRevision: 1, CapabilityObservedAt: time.Now().UTC()}
	if _, err := store.RegisterNode(t.Context(), fabric.Identity{NodeID: "agent"}, registration, l1.DefaultNodePolicy(), true); err != nil {
		t.Fatal(err)
	}
	job, _, err := store.CreateJob(t.Context(), contract.JobSpec{SchemaVersion: contract.SchemaVersionV1,
		DispatchKey: "forgotten-reader", Kind: contract.JobKindProcess, Class: contract.JobClassService,
		Restart: contract.RestartAlways, Execution: contract.ExecutionSpec{Executable: contract.ExecutableSpec{Path: "/bin/echo"}, Argv: []string{"echo", "hello"}, WorkingDirectory: "/tmp"}})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimJob(t.Context(), "agent", registration.NodeID, registration.BootSessionID, contract.JobClassService)
	if err != nil || claim == nil {
		t.Fatalf("claim=%#v err=%v", claim, err)
	}
	if _, err := store.ForceForgetService(t.Context(), job.JobID); err != nil {
		t.Fatal(err)
	}
	directives, err := store.ListNodeRemovalDirectives(t.Context(), "agent", registration.NodeID, registration.BootSessionID)
	if err != nil || len(directives) != 1 {
		t.Fatalf("directives=%#v err=%v", directives, err)
	}
	directive := directives[0]
	ack := l1.RemovalAcknowledgementRequest{NodeID: registration.NodeID, BootSessionID: registration.BootSessionID,
		RemovalGeneration: directive.RemovalGeneration, RootInstanceID: directive.RootInstanceID,
		CleanupFence: directive.CleanupFence, IdempotencyKey: "ack"}
	if _, finalized := readRemovalTombstoneState(t, path, job.JobID); finalized {
		t.Fatal("force-forgotten tombstone accepted before acknowledgement")
	}
	acknowledged, err := store.AcknowledgeServiceRemoval(t.Context(), "agent", job.JobID, ack)
	if err != nil {
		t.Fatal(err)
	}
	if acknowledged.State != contract.JobForgottenCleanupUnverified || acknowledged.Removal == nil || acknowledged.Removal.CleanupAcknowledgedAt == nil {
		t.Fatalf("acknowledged=%#v", acknowledged)
	}
	if _, finalized := readRemovalTombstoneState(t, path, job.JobID); finalized {
		t.Fatal("acknowledgement accepted as finalized tombstone")
	}

	// Finalize only when the waiter requests a second observation. Returning
	// after the first API acknowledgement would leave the tombstone NULL.
	observations := 0
	transport := forgottenCleanupTransport(func(request *http.Request) (*http.Response, error) {
		if request.Method != http.MethodGet || request.URL.Path != "/v1/jobs/"+job.JobID {
			return nil, fmt.Errorf("unexpected request %s %s", request.Method, request.URL.Path)
		}
		observations++
		if observations == 2 {
			if _, changed, err := store.FinalizeServiceRemoval(request.Context(), job.JobID); err != nil || !changed {
				return nil, fmt.Errorf("controlled finalization changed=%v err=%v", changed, err)
			}
		}
		projected, err := store.GetJob(request.Context(), job.JobID)
		if err != nil {
			return nil, err
		}
		body, err := json.Marshal(projected)
		if err != nil {
			return nil, err
		}
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(string(body))), Header: make(http.Header)}, nil
	})
	harness := &acceptanceHarness{client: &http.Client{Transport: transport}, l1Database: path, agent: &managedProcess{done: make(chan struct{})}}
	observed, before := waitForForgottenCleanup(t, harness, job.JobID, filepath.Join(t.TempDir(), "absent-service-root"), 45*time.Second)
	if observations != 2 || observed.State != contract.JobForgottenCleanupUnverified {
		t.Fatalf("waiter observations=%d job=%#v", observations, observed)
	}
	if before.Outcome != string(l1.ServiceRemovalForgotten) || before.RemovalGeneration != directive.RemovalGeneration ||
		before.RootInstanceID != directive.RootInstanceID || before.LastBoundNodeID != directive.BoundNodeID ||
		before.CleanupAcknowledgedNS != acknowledged.Removal.CleanupAcknowledgedAt.UnixNano() {
		t.Fatalf("finalized tombstone=%#v", before)
	}
	ack.CleanupFence = "not-retained-after-finalization"
	ack.IdempotencyKey = "replayed-after-finalization"
	replayed, err := store.AcknowledgeServiceRemoval(t.Context(), "agent", job.JobID, ack)
	if err != nil || replayed.State != contract.JobForgottenCleanupUnverified || replayed.Removal.RemovalOutcome != l1.ServiceRemovalForgotten {
		t.Fatalf("replayed=%#v err=%v", replayed, err)
	}
	if after := readRemovalTombstone(t, path, job.JobID); after != before {
		t.Fatalf("replay changed tombstone: before=%#v after=%#v", before, after)
	}
}

// Only the HTTP boundary is controlled; responses come from the real L1 store.
type forgottenCleanupTransport func(*http.Request) (*http.Response, error)

func (transport forgottenCleanupTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return transport(request)
}
