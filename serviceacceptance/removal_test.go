//go:build (service_acceptance || service_acceptance_realtiming) && (darwin || linux)

package serviceacceptance

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/agent/managedroot"
	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestServiceRemovalCleansRunningService(t *testing.T) {
	harness := newAcceptanceHarness(t)
	port := reservePort(t)
	created := harness.submitEchoService(t, port)
	health := waitForHealth(t, harness.publishedHTTPClient(t, port), "http://published-service.invalid", harness.agent)
	running := harness.waitForJobState(t, created.JobID, contract.JobClassService, contract.JobRunning, 5*time.Second)
	spec := harness.specs[created.JobID]
	secret := spec.Execution.SensitiveEnv["SERVICE_ACCEPTANCE_SECRET"]

	serviceRoot := filepath.Join(
		harness.managedRoot,
		"agent", "nodes", managedroot.EncodeID("acceptance-node"),
		"services", managedroot.EncodeID(created.JobID),
	)
	if _, err := os.Stat(serviceRoot); err != nil {
		t.Fatalf("managed service root before removal: %v", err)
	}

	removalCtx, cancelRemoval := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancelRemoval()
	var pending l1.Job
	status, body := removalJSON(t, removalCtx, harness, http.MethodPost, "/v1/jobs/"+created.JobID+"/remove?class=service", nil, &pending)
	if status != http.StatusAccepted || pending.State != contract.JobRemovalPending {
		t.Fatalf("remove running service = status %d job %#v body=%s", status, pending, body)
	}

	waitRemovalVerified(t, removalCtx, harness, created.JobID)
	assertRemovalPersistence(t, removalCtx, harness, created.JobID, spec, secret)
	assertAgentConfirmedStatus(t, harness, created.JobID)
	waitForProcessAbsent(t, health.PID, 5*time.Second)
	if _, err := os.Lstat(serviceRoot); !os.IsNotExist(err) {
		t.Fatalf("managed service root survived removal: %v", err)
	}
	assertWorkingDirectoryUntouched(t, harness.workingDirectories[running.JobID])
	assertNoServiceResidue(t, harness, created.JobID)
}

func assertAgentConfirmedStatus(t *testing.T, harness *acceptanceHarness, jobID string) {
	t.Helper()
	command := exec.Command(
		weftyBinaryPath,
		"--fabric=plain",
		"--l1="+harness.controlPlaneAddress,
		"--plain-identity=service-acceptance-cli",
		"--json",
		"services", "status", jobID,
	)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("read removed service through CLI: %v\n%s", err, output)
	}
	var status struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(output, &status); err != nil {
		t.Fatalf("decode removed service CLI status: %v\n%s", err, output)
	}
	if status.Status != "removed (agent-confirmed)" {
		t.Fatalf("removed service CLI status = %q, want agent-confirmed", status.Status)
	}
}

func TestServiceRemovalAcceptsStoppedAndLatchedFailed(t *testing.T) {
	t.Run("stopped", func(t *testing.T) {
		harness := newAcceptanceHarness(t)
		created := harness.submitEchoService(t, reservePort(t))
		harness.waitForJobState(t, created.JobID, contract.JobClassService, contract.JobRunning, 5*time.Second)
		var stopping l1.Job
		status, body := harness.doJSON(t, http.MethodPut, "/v1/jobs/"+created.JobID+"/desired-state?class=service",
			l1.ServiceDesiredStateRequest{DesiredState: contract.ServiceDesiredStopped}, &stopping)
		if status != http.StatusAccepted {
			t.Fatalf("stop service status = %d body=%s", status, body)
		}
		harness.waitForJobState(t, created.JobID, contract.JobClassService, contract.JobStopped, 10*time.Second)
		removeAndWait(t, harness, created.JobID)
		assertWorkingDirectoryUntouched(t, harness.workingDirectories[created.JobID])
		assertManagedServiceAbsent(t, harness, created.JobID)
	})

	t.Run("latched failed", func(t *testing.T) {
		harness := newAcceptanceHarness(t)
		created := harness.submitFailedService(t)
		harness.waitForJobState(t, created.JobID, contract.JobClassService, contract.JobFailed, 5*time.Second)
		removeAndWait(t, harness, created.JobID)
		assertWorkingDirectoryUntouched(t, harness.workingDirectories[created.JobID])
		assertManagedServiceAbsent(t, harness, created.JobID)
	})
}

func TestOfflineServiceRemovalStaysPending(t *testing.T) {
	harness := newAcceptanceHarness(t)
	created := harness.submitEchoService(t, reservePort(t))
	health := waitForHealth(t, harness.publishedHTTPClient(t, *harness.specs[created.JobID].PublishedPort),
		"http://published-service.invalid", harness.agent)
	harness.waitForJobState(t, created.JobID, contract.JobClassService, contract.JobRunning, 5*time.Second)
	harness.agent.kill(t)
	waitForProcessAbsent(t, health.PID, 5*time.Second)

	var pending l1.Job
	status, body := harness.doJSON(t, http.MethodPost, "/v1/jobs/"+created.JobID+"/remove?class=service", nil, &pending)
	if status != http.StatusAccepted || pending.State != contract.JobRemovalPending {
		t.Fatalf("offline remove = status %d job %#v body=%s", status, pending, body)
	}
	time.Sleep(750 * time.Millisecond)
	status, body = harness.doJSON(t, http.MethodGet, "/v1/jobs/"+created.JobID+"?class=service", nil, &pending)
	if status != http.StatusOK || pending.State != contract.JobRemovalPending || strings.Contains(pending.Status, "clean") {
		t.Fatalf("offline removal projection = status %d job %#v body=%s", status, pending, body)
	}
	assertWorkingDirectoryUntouched(t, harness.workingDirectories[created.JobID])
}

func TestForceForgetLeavesCleanupStandingForReturningNode(t *testing.T) {
	harness := newAcceptanceHarness(t)
	created := harness.submitEchoService(t, reservePort(t))
	harness.waitForJobState(t, created.JobID, contract.JobClassService, contract.JobRunning, 5*time.Second)
	harness.agent.kill(t)

	var pending l1.Job
	status, body := harness.doJSON(t, http.MethodPost, "/v1/jobs/"+created.JobID+"/remove?class=service", nil, &pending)
	if status != http.StatusAccepted || pending.State != contract.JobRemovalPending {
		t.Fatalf("offline remove before forget = status %d job %#v body=%s", status, pending, body)
	}
	var forgotten l1.Job
	status, body = harness.doJSON(t, http.MethodPost, "/v1/jobs/"+created.JobID+"/forget?class=service",
		l1.ForceForgetRequest{Force: true}, &forgotten)
	if status != http.StatusOK || forgotten.State != contract.JobForgottenCleanupUnverified {
		t.Fatalf("force forget = status %d job %#v body=%s", status, forgotten, body)
	}
	serviceRoot := managedServiceRoot(harness, created.JobID)
	if _, err := os.Stat(serviceRoot); err != nil {
		t.Fatalf("force forget invoked local deletion while node was offline: %v", err)
	}

	harness.restartAgent(t)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Lstat(serviceRoot); os.IsNotExist(err) {
			break
		}
		if harness.agent.exited() {
			t.Fatalf("returning agent exited before cleanup: %v\n%s", harness.agent.waitError(), harness.agent.outputString())
		}
		time.Sleep(20 * time.Millisecond)
	}
	if _, err := os.Lstat(serviceRoot); !os.IsNotExist(err) {
		t.Fatalf("returning node left force-forgotten managed root: %v", err)
	}
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		status, body = harness.doJSON(t, http.MethodGet, "/v1/jobs/"+created.JobID+"?class=service", nil, &forgotten)
		if status != http.StatusOK {
			t.Fatalf("read force-forgotten cleanup = status %d body=%s", status, body)
		}
		if forgotten.Removal != nil && forgotten.Removal.CleanupAcknowledgedAt != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if forgotten.State != contract.JobForgottenCleanupUnverified || forgotten.Removal == nil || forgotten.Removal.CleanupAcknowledgedAt == nil {
		t.Fatalf("returning node did not attest force-forgotten cleanup: %#v", forgotten)
	}
	assertWorkingDirectoryUntouched(t, harness.workingDirectories[created.JobID])
}

func removeAndWait(t *testing.T, harness *acceptanceHarness, jobID string) {
	t.Helper()
	var pending l1.Job
	status, body := harness.doJSON(t, http.MethodPost, "/v1/jobs/"+jobID+"/remove?class=service", nil, &pending)
	if status != http.StatusAccepted || pending.State != contract.JobRemovalPending {
		t.Fatalf("remove service = status %d job %#v body=%s", status, pending, body)
	}
	harness.waitForJobState(t, jobID, contract.JobClassService, contract.JobRemovedVerified, 10*time.Second)
}

func assertRemovalPersistence(
	t *testing.T,
	ctx context.Context,
	harness *acceptanceHarness,
	jobID string,
	spec contract.JobSpec,
	secret string,
) {
	t.Helper()
	database, err := sql.Open("sqlite", harness.l1Database)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	var secretRows int
	if err := database.QueryRowContext(ctx, `SELECT COUNT(*) FROM jobs WHERE CAST(spec_json AS TEXT) LIKE ?`, "%"+secret+"%").Scan(&secretRows); err != nil {
		t.Fatal(err)
	}
	if secretRows != 0 {
		t.Fatalf("SensitiveEnv value remains in jobs.spec_json for removed service")
	}
	dispatchDigest := sha256.Sum256([]byte(spec.DispatchKey))
	var dispatchKeyHash string
	if err := database.QueryRowContext(ctx, `SELECT dispatch_key_hash FROM service_tombstones WHERE job_id=?`, jobID).Scan(&dispatchKeyHash); err != nil {
		t.Fatal(err)
	}
	if dispatchKeyHash != hex.EncodeToString(dispatchDigest[:]) {
		t.Fatalf("tombstone dispatch-key hash = %q", dispatchKeyHash)
	}

	var replay l1.Job
	status, body := removalJSON(t, ctx, harness, http.MethodPost, "/v1/jobs", spec, &replay)
	if status != http.StatusOK || replay.JobID != jobID || replay.State != contract.JobRemovedVerified {
		t.Fatalf("identical service create replay = status %d job %#v body=%s", status, replay, body)
	}

	entries, err := os.ReadDir(harness.spoolDirectory)
	if err != nil {
		t.Fatal(err)
	}
	var spoolPath string
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".sqlite" {
			spoolPath = filepath.Join(harness.spoolDirectory, entry.Name())
			break
		}
	}
	if spoolPath == "" {
		t.Fatal("agent spool database was not created")
	}
	spool, err := sql.Open("sqlite", spoolPath)
	if err != nil {
		t.Fatal(err)
	}
	defer spool.Close()
	if err := waitRemovalSpool(ctx, spool, jobID, func() error { return removalAgentError(harness) }, nil); err != nil {
		t.Fatal(err)
	}
}

func assertManagedServiceAbsent(t *testing.T, harness *acceptanceHarness, jobID string) {
	t.Helper()
	serviceRoot := managedServiceRoot(harness, jobID)
	if _, err := os.Lstat(serviceRoot); !os.IsNotExist(err) {
		t.Fatalf("managed service root survived removal: %v", err)
	}
}

func managedServiceRoot(harness *acceptanceHarness, jobID string) string {
	return filepath.Join(
		harness.managedRoot, "agent", "nodes", managedroot.EncodeID("acceptance-node"),
		"services", managedroot.EncodeID(jobID),
	)
}

func assertNoServiceResidue(t *testing.T, harness *acceptanceHarness, jobID string) {
	t.Helper()
	assertManagedServiceAbsent(t, harness, jobID)
	assertHandoffRootEmpty(t, harness.handoffRoot)
	encodedJobID := managedroot.EncodeID(jobID)
	roots := []string{filepath.Join(os.TempDir(), "wefty"), harness.managedRoot}
	if cache, err := os.UserCacheDir(); err == nil {
		roots = append(roots, filepath.Join(cache, "wefty"))
	}
	for _, root := range roots {
		err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
			if os.IsNotExist(walkErr) {
				return filepath.SkipDir
			}
			if walkErr != nil {
				return walkErr
			}
			if entry.Name() == encodedJobID {
				t.Fatalf("service residue remains at %q", path)
			}
			return nil
		})
		if err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
	}
}

// Local intent release follows receipt of the remote ACK. Each sample is a
// short transaction; never hold a read snapshot while waiting for the writer.
type removalSpoolSnapshot struct {
	attempts, removals int
	tuples             []string
}

func waitRemovalSpool(ctx context.Context, db *sql.DB, jobID string, agentError func() error, observed func(removalSpoolSnapshot)) error {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	var last removalSpoolSnapshot
	fail := func(err error) error {
		return fmt.Errorf("observe removal %s: %w; attempts=%d removals=%d tuples=%v", jobID, err, last.attempts, last.removals, last.tuples)
	}
	for {
		if err := ctx.Err(); err != nil {
			return fail(err)
		}
		if err := agentError(); err != nil {
			return fail(err)
		}
		snapshot, err := readRemovalSpool(ctx, db, jobID)
		if err != nil {
			return fail(err)
		}
		last = snapshot
		if observed != nil {
			observed(snapshot)
		}
		if err := ctx.Err(); err != nil {
			return fail(err)
		}
		if err := agentError(); err != nil {
			return fail(err)
		}
		if last.attempts == 0 && last.removals == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return fail(ctx.Err())
		case <-ticker.C:
		}
	}
}

func readRemovalSpool(ctx context.Context, db *sql.DB, jobID string) (removalSpoolSnapshot, error) {
	var result removalSpoolSnapshot
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return result, err
	}
	defer tx.Rollback()
	if err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM spool_attempts WHERE job_id=?", jobID).Scan(&result.attempts); err != nil {
		return result, err
	}
	rows, err := tx.QueryContext(ctx, "SELECT removal_generation, cleanup_fence, root_instance_id FROM spool_removals WHERE job_id=?", jobID)
	if err != nil {
		return result, err
	}
	for rows.Next() {
		var generation uint64
		var fence, root string
		if err = rows.Scan(&generation, &fence, &root); err != nil {
			rows.Close()
			return result, err
		}
		result.removals++
		result.tuples = append(result.tuples, fmt.Sprintf("generation=%d fence=%s root=%s", generation, fence, root))
	}
	err = rows.Err()
	closeErr := rows.Close()
	if err != nil {
		return result, err
	}
	if closeErr != nil {
		return result, closeErr
	}
	return result, tx.Commit()
}

func removalAgentError(h *acceptanceHarness) error {
	if h.agent.exited() {
		return fmt.Errorf("agent exited: %v\n%s", h.agent.waitError(), h.agent.outputString())
	}
	return nil
}

func assertRemovalVerified(t *testing.T, ctx context.Context, h *acceptanceHarness, jobID string) {
	t.Helper()
	var job l1.Job
	status, body := removalJSON(t, ctx, h, http.MethodGet, "/v1/jobs/"+jobID+"?class=service", nil, &job)
	if status != http.StatusOK || job.JobID != jobID || job.State != contract.JobRemovedVerified {
		t.Fatalf("verified removal: status=%d job=%+v body=%s", status, job, body)
	}
}

func waitRemovalVerified(t *testing.T, ctx context.Context, h *acceptanceHarness, jobID string) {
	t.Helper()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := removalAgentError(h); err != nil {
			t.Fatal(err)
		}
		var job l1.Job
		status, body := removalJSON(t, ctx, h, http.MethodGet, "/v1/jobs/"+jobID+"?class=service", nil, &job)
		if status != http.StatusOK || job.JobID != jobID {
			t.Fatalf("removal status=%d body=%s", status, body)
		}
		if job.State == contract.JobRemovedVerified {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-ticker.C:
		}
	}
}

func removalJSON(t *testing.T, ctx context.Context, h *acceptanceHarness, method, path string, input, output any) (int, []byte) {
	t.Helper()
	var reader io.Reader
	if input != nil {
		data, err := json.Marshal(input)
		if err != nil {
			t.Fatal(err)
		}
		reader = bytes.NewReader(data)
	}
	request, err := http.NewRequestWithContext(ctx, method, "http://control-plane.invalid"+path, reader)
	if err != nil {
		t.Fatal(err)
	}
	if input != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := h.client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if output != nil && response.StatusCode >= 200 && response.StatusCode < 300 {
		if err := json.Unmarshal(body, output); err != nil {
			t.Fatal(err)
		}
	}
	if err := ctx.Err(); err != nil {
		t.Fatal(err)
	}
	return response.StatusCode, body
}
