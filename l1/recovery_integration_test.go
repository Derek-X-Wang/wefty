package l1

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
)

func TestLeaseExpiryReaperFencesAttemptWithoutRequeue(t *testing.T) {
	h := newIntegrationHarness(t, map[string][]string{"node-1": {"linux"}, "node-2": {"linux"}})
	client := h.client(fabric.Identity{NodeID: "caller", Tags: []string{DefaultClientPrincipalTag}})
	agent1 := h.client(fabric.Identity{NodeID: "fabric-node-1", Tags: []string{DefaultAgentPrincipalTag}})
	agent2 := h.client(fabric.Identity{NodeID: "fabric-node-2", Tags: []string{DefaultAgentPrincipalTag}})
	h.register(agent1, "node-1")
	h.register(agent2, "node-2")
	job := h.submit(client, "expiry-no-requeue", []string{"linux"})
	claim := claimJob(t, h, agent1, "node-1")
	logPath := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/logs", job.JobID, claim.Lease.AttemptID)
	acceptedLog := AppendLogsRequest{FencingToken: claim.Lease.FencingToken, Events: []contract.LogEvent{{
		AttemptID: claim.Lease.AttemptID, Stream: contract.LogStdout, Sequence: 0,
		Timestamp: h.clock.Now(), Bytes: []byte("accepted-before-expiry\n"),
	}}}
	status, _, body := h.do(agent1, http.MethodPost, logPath, acceptedLog)
	if status != http.StatusOK {
		t.Fatalf("initial log status = %d body=%s", status, body)
	}

	h.clock.Advance(30 * time.Second)
	result, err := h.store.Reconcile(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.ExpiredAttempts != 1 {
		t.Fatalf("expired attempts = %d, want 1", result.ExpiredAttempts)
	}
	assertJobAndAttemptState(t, h.store, job.JobID, claim.Lease.AttemptID, contract.JobFailed, contract.AttemptLost)
	status, _, body = h.do(agent1, http.MethodPost, logPath, acceptedLog)
	if status != http.StatusOK {
		t.Fatalf("identical expired log replay status = %d body=%s", status, body)
	}
	lateLog := AppendLogsRequest{FencingToken: claim.Lease.FencingToken, Events: []contract.LogEvent{{
		AttemptID: claim.Lease.AttemptID, Stream: contract.LogStdout, Sequence: 1,
		Timestamp: h.clock.Now(), Bytes: []byte("must-not-append\n"),
	}}}
	status, _, body = h.do(agent1, http.MethodPost, logPath, lateLog)
	assertAPIError(t, status, body, http.StatusConflict, contract.ErrorLeaseExpired)
	var logRows int
	if err := h.store.db.QueryRow("SELECT count(*) FROM log_events WHERE job_id=?", job.JobID).Scan(&logRows); err != nil {
		t.Fatal(err)
	}
	if logRows != 1 {
		t.Fatalf("log rows after expired append = %d, want 1", logRows)
	}

	completionPath := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/complete", job.JobID, claim.Lease.AttemptID)
	exitCode := 0
	status, _, body = h.do(agent1, http.MethodPost, completionPath, CompletionRequest{
		FencingToken: claim.Lease.FencingToken, IdempotencyKey: "late-completion", Result: ProcessResult{ExitCode: &exitCode},
	})
	assertAPIError(t, status, body, http.StatusConflict, contract.ErrorLeaseExpired)

	status, _, body = h.do(agent2, http.MethodPost, "/v1/agent/jobs/claim", ClaimRequest{NodeID: "node-2", BootSessionID: "boot-node-2"})
	if status != http.StatusNoContent {
		t.Fatalf("second execution claim status = %d, want 204 body=%s", status, body)
	}
	var attempts int
	if err := h.store.db.QueryRow("SELECT count(*) FROM attempts WHERE job_id=?", job.JobID).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 {
		t.Fatalf("attempt rows = %d, want exactly one", attempts)
	}

	replayed, err := h.store.Reconcile(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if replayed.ExpiredAttempts != 0 {
		t.Fatalf("second reap expired attempts = %d, want 0", replayed.ExpiredAttempts)
	}
}

func TestOutputFailureCompletionFailsJob(t *testing.T) {
	h := newIntegrationHarness(t, map[string][]string{"node-1": {"linux"}})
	client := h.client(fabric.Identity{NodeID: "caller", Tags: []string{DefaultClientPrincipalTag}})
	agent := h.client(fabric.Identity{NodeID: "fabric-node", Tags: []string{DefaultAgentPrincipalTag}})
	h.register(agent, "node-1")
	job := h.submit(client, "output-failure", []string{"linux"})
	claim := claimJob(t, h, agent, "node-1")
	path := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/complete", job.JobID, claim.Lease.AttemptID)
	status, _, body := h.do(agent, http.MethodPost, path, CompletionRequest{
		FencingToken: claim.Lease.FencingToken, IdempotencyKey: "output-failure",
		Result: ProcessResult{OutputError: "durable log finalization failed"},
	})
	if status != http.StatusOK {
		t.Fatalf("output-failure completion status = %d body=%s", status, body)
	}
	assertJobAndAttemptState(t, h.store, job.JobID, claim.Lease.AttemptID, contract.JobFailed, contract.AttemptFailed)
}

func TestReservedAwaitingInputLeaseExpiryFailsJobConsistently(t *testing.T) {
	h := newIntegrationHarness(t, map[string][]string{"node-1": {"linux"}})
	client := h.client(fabric.Identity{NodeID: "caller", Tags: []string{DefaultClientPrincipalTag}})
	agent := h.client(fabric.Identity{NodeID: "fabric-node", Tags: []string{DefaultAgentPrincipalTag}})
	h.register(agent, "node-1")
	job := h.submit(client, "awaiting-input-expiry", []string{"linux"})
	claim := claimJob(t, h, agent, "node-1")
	if _, err := h.store.db.Exec(`UPDATE attempts SET state=? WHERE attempt_id=?`, contract.AttemptAwaitingInput, claim.Lease.AttemptID); err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.db.Exec(`UPDATE jobs SET state=? WHERE job_id=?`, contract.JobAwaitingInput, job.JobID); err != nil {
		t.Fatal(err)
	}
	h.clock.Advance(DefaultLeaseDuration)
	result, err := h.store.Reconcile(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.ExpiredAttempts != 1 {
		t.Fatalf("expired awaiting-input attempts = %d, want 1", result.ExpiredAttempts)
	}
	assertJobAndAttemptState(t, h.store, job.JobID, claim.Lease.AttemptID, contract.JobFailed, contract.AttemptLost)
}

func TestExpiryBoundarySerializesRenewalAndReaper(t *testing.T) {
	h := newIntegrationHarness(t, map[string][]string{"node-1": {"linux"}})
	client := h.client(fabric.Identity{NodeID: "caller", Tags: []string{DefaultClientPrincipalTag}})
	agent := h.client(fabric.Identity{NodeID: "fabric-node", Tags: []string{DefaultAgentPrincipalTag}})
	h.register(agent, "node-1")
	job := h.submit(client, "expiry-race", []string{"linux"})
	claim := claimJob(t, h, agent, "node-1")
	h.clock.Advance(30 * time.Second)

	start := make(chan struct{})
	type renewalResult struct {
		status int
		body   []byte
		err    error
	}
	renewed := make(chan renewalResult, 1)
	reaped := make(chan struct {
		result ReconcileResult
		err    error
	}, 1)
	renewPath := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/lease", job.JobID, claim.Lease.AttemptID)
	go func() {
		<-start
		status, _, body, err := doRequest(agent, http.MethodPost, renewPath, RenewalRequest{FencingToken: claim.Lease.FencingToken})
		renewed <- renewalResult{status: status, body: body, err: err}
	}()
	go func() {
		<-start
		result, err := h.store.Reconcile(context.Background())
		reaped <- struct {
			result ReconcileResult
			err    error
		}{result: result, err: err}
	}()
	close(start)

	renewal := <-renewed
	if renewal.err != nil {
		t.Fatal(renewal.err)
	}
	assertAPIError(t, renewal.status, renewal.body, http.StatusConflict, contract.ErrorLeaseExpired)
	reap := <-reaped
	if reap.err != nil {
		t.Fatal(reap.err)
	}
	if reap.result.ExpiredAttempts != 0 && reap.result.ExpiredAttempts != 1 {
		t.Fatalf("reaper transitions = %d, want 0 or 1 depending on serialized winner", reap.result.ExpiredAttempts)
	}
	assertJobAndAttemptState(t, h.store, job.JobID, claim.Lease.AttemptID, contract.JobFailed, contract.AttemptLost)
}

func TestStaleAndDeadNodesCannotClaim(t *testing.T) {
	t.Run("stale heartbeat revives node", func(t *testing.T) {
		h := newIntegrationHarness(t, map[string][]string{"node-1": {"linux"}})
		client := h.client(fabric.Identity{NodeID: "caller", Tags: []string{DefaultClientPrincipalTag}})
		agent := h.client(fabric.Identity{NodeID: "fabric-node", Tags: []string{DefaultAgentPrincipalTag}})
		h.register(agent, "node-1")
		h.submit(client, "stale-claim", []string{"linux"})
		h.clock.Advance(DefaultNodeStaleAfter)

		status, _, body := h.do(agent, http.MethodPost, "/v1/agent/jobs/claim", ClaimRequest{NodeID: "node-1", BootSessionID: "boot-node-1"})
		assertAPIError(t, status, body, http.StatusConflict, contract.ErrorConflict)
		node, err := getNode(context.Background(), h.store.db, "node-1")
		if err != nil {
			t.Fatal(err)
		}
		if node.State != contract.NodeStale {
			t.Fatalf("node state = %q, want stale", node.State)
		}
		status, _, body = h.do(agent, http.MethodPost, "/v1/agent/nodes/node-1/heartbeat", HeartbeatRequest{BootSessionID: "boot-node-1"})
		if status != http.StatusOK {
			t.Fatalf("heartbeat status = %d body=%s", status, body)
		}
		if claim := claimJob(t, h, agent, "node-1"); claim.Job.State != contract.JobClaimed {
			t.Fatalf("revived node claim = %#v", claim)
		}
	})

	t.Run("dead requires registration", func(t *testing.T) {
		h := newIntegrationHarness(t, map[string][]string{"node-1": {"linux"}})
		client := h.client(fabric.Identity{NodeID: "caller", Tags: []string{DefaultClientPrincipalTag}})
		agent := h.client(fabric.Identity{NodeID: "fabric-node", Tags: []string{DefaultAgentPrincipalTag}})
		h.register(agent, "node-1")
		h.submit(client, "dead-claim", []string{"linux"})
		h.clock.Advance(DefaultNodeDeadAfter)
		status, _, body := h.do(agent, http.MethodPost, "/v1/agent/jobs/claim", ClaimRequest{NodeID: "node-1", BootSessionID: "boot-node-1"})
		assertAPIError(t, status, body, http.StatusConflict, contract.ErrorNodeDead)
		status, _, body = h.do(agent, http.MethodPost, "/v1/agent/nodes/node-1/heartbeat", HeartbeatRequest{BootSessionID: "boot-node-1"})
		assertAPIError(t, status, body, http.StatusConflict, contract.ErrorNodeDead)
		h.register(agent, "node-1")
		claimJob(t, h, agent, "node-1")
	})
}

func TestDrainStopsClaimsAndAllowsRunningAttemptToFinish(t *testing.T) {
	h := newIntegrationHarness(t, map[string][]string{"node-1": {"linux"}})
	client := h.client(fabric.Identity{NodeID: "caller", Tags: []string{DefaultClientPrincipalTag}})
	agent := h.client(fabric.Identity{NodeID: "fabric-node", Tags: []string{DefaultAgentPrincipalTag}})
	h.register(agent, "node-1")
	first := h.submit(client, "drain-running", []string{"linux"})
	second := h.submit(client, "drain-queued", []string{"linux"})
	claim := claimJob(t, h, agent, "node-1")
	runningJobID := claim.Job.JobID
	queuedJobID := first.JobID
	if runningJobID == first.JobID {
		queuedJobID = second.JobID
	} else if runningJobID != second.JobID {
		t.Fatalf("claimed unexpected job %q", runningJobID)
	}
	renewPath := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/lease", runningJobID, claim.Lease.AttemptID)
	status, _, body := h.do(agent, http.MethodPost, renewPath, RenewalRequest{FencingToken: claim.Lease.FencingToken})
	if status != http.StatusOK {
		t.Fatalf("renew status = %d body=%s", status, body)
	}

	status, _, body = h.do(agent, http.MethodPost, "/v1/agent/nodes/node-1/drain", DrainRequest{BootSessionID: "boot-node-1"})
	if status != http.StatusOK {
		t.Fatalf("drain status = %d body=%s", status, body)
	}
	var draining Node
	if err := json.Unmarshal(body, &draining); err != nil {
		t.Fatal(err)
	}
	if draining.State != contract.NodeDraining {
		t.Fatalf("drain state = %q, want draining", draining.State)
	}
	status, _, body = h.do(agent, http.MethodPost, "/v1/agent/nodes/node-1/heartbeat", HeartbeatRequest{BootSessionID: "boot-node-1"})
	if status != http.StatusOK {
		t.Fatalf("draining heartbeat status = %d body=%s", status, body)
	}
	if err := json.Unmarshal(body, &draining); err != nil {
		t.Fatal(err)
	}
	if draining.State != contract.NodeDraining {
		t.Fatalf("heartbeat revived draining node to %q", draining.State)
	}

	exitCode := 0
	completionPath := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/complete", runningJobID, claim.Lease.AttemptID)
	status, _, body = h.do(agent, http.MethodPost, completionPath, CompletionRequest{
		FencingToken: claim.Lease.FencingToken, IdempotencyKey: "drain-completion", Result: ProcessResult{ExitCode: &exitCode},
	})
	if status != http.StatusOK {
		t.Fatalf("complete while draining status = %d body=%s", status, body)
	}
	status, _, body = h.do(agent, http.MethodPost, "/v1/agent/jobs/claim", ClaimRequest{NodeID: "node-1", BootSessionID: "boot-node-1"})
	assertAPIError(t, status, body, http.StatusConflict, contract.ErrorNodeDraining)
	queued, err := h.store.GetJob(context.Background(), queuedJobID)
	if err != nil {
		t.Fatal(err)
	}
	if queued.State != contract.JobQueued {
		t.Fatalf("unclaimed job state = %q, want queued", queued.State)
	}
}

func TestCompletionReplayDoesNotDoubleTransition(t *testing.T) {
	h := newIntegrationHarness(t, map[string][]string{"node-1": {"linux"}})
	client := h.client(fabric.Identity{NodeID: "caller", Tags: []string{DefaultClientPrincipalTag}})
	agent := h.client(fabric.Identity{NodeID: "fabric-node", Tags: []string{DefaultAgentPrincipalTag}})
	h.register(agent, "node-1")
	job := h.submit(client, "completion-replay", []string{"linux"})
	claim := claimJob(t, h, agent, "node-1")
	exitCode := 0
	request := CompletionRequest{FencingToken: claim.Lease.FencingToken, IdempotencyKey: "same-completion", Result: ProcessResult{ExitCode: &exitCode}}
	path := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/complete", job.JobID, claim.Lease.AttemptID)
	status, _, body := h.do(agent, http.MethodPost, path, request)
	if status != http.StatusOK {
		t.Fatalf("first completion status = %d body=%s", status, body)
	}
	var jobUpdated, attemptUpdated int64
	if err := h.store.db.QueryRow(`SELECT j.updated_ns, a.updated_ns FROM jobs j JOIN attempts a ON a.attempt_id=j.current_attempt_id WHERE j.job_id=?`, job.JobID).Scan(&jobUpdated, &attemptUpdated); err != nil {
		t.Fatal(err)
	}
	h.clock.Advance(time.Hour)
	status, _, body = h.do(agent, http.MethodPost, path, request)
	if status != http.StatusOK {
		t.Fatalf("replayed completion status = %d body=%s", status, body)
	}
	var replayedJobUpdated, replayedAttemptUpdated int64
	if err := h.store.db.QueryRow(`SELECT j.updated_ns, a.updated_ns FROM jobs j JOIN attempts a ON a.attempt_id=j.current_attempt_id WHERE j.job_id=?`, job.JobID).Scan(&replayedJobUpdated, &replayedAttemptUpdated); err != nil {
		t.Fatal(err)
	}
	if replayedJobUpdated != jobUpdated || replayedAttemptUpdated != attemptUpdated {
		t.Fatalf("replay changed timestamps job %d->%d attempt %d->%d", jobUpdated, replayedJobUpdated, attemptUpdated, replayedAttemptUpdated)
	}
	assertJobAndAttemptState(t, h.store, job.JobID, claim.Lease.AttemptID, contract.JobSucceeded, contract.AttemptSucceeded)
}

func TestControlPlaneRestartRecoversQueuedAndRunningJobs(t *testing.T) {
	directory := t.TempDir()
	databasePath := filepath.Join(directory, "restart.sqlite")
	clock := &fakeClock{now: time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)}
	network := plain.NewNetwork()
	serverFabric := network.NewFabric(fabric.Identity{NodeID: "control-plane"})
	config := ServerConfig{NodePolicies: map[string]NodePolicy{"node-1": DefaultNodePolicy("linux")}}

	start := func(store *Store) (context.CancelFunc, <-chan error) {
		t.Helper()
		server, err := NewServer(serverFabric, store, config)
		if err != nil {
			t.Fatal(err)
		}
		listener, err := serverFabric.Listen("tcp", "wefty://control-plane")
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- server.Serve(ctx, listener) }()
		return cancel, done
	}
	stop := func(cancel context.CancelFunc, done <-chan error) {
		t.Helper()
		cancel()
		if err := <-done; err != nil {
			t.Fatalf("serve L1: %v", err)
		}
	}
	newClient := func(identity fabric.Identity) *http.Client {
		participant := network.NewFabric(identity)
		return &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			return participant.Dial(ctx, network, "wefty://control-plane")
		}}}
	}
	client := newClient(fabric.Identity{NodeID: "caller", Tags: []string{DefaultClientPrincipalTag}})
	agent := newClient(fabric.Identity{NodeID: "fabric-node", Tags: []string{DefaultAgentPrincipalTag}})
	defer client.CloseIdleConnections()
	defer agent.CloseIdleConnections()

	store, err := OpenStore(databasePath, StoreOptions{Clock: clock, LeaseDuration: 30 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	cancel, done := start(store)
	registerOverHTTP(t, agent, "node-1", "boot-1")
	running := submitOverHTTP(t, client, "restart-running")
	claim := claimOverHTTP(t, agent, "node-1", "boot-1")
	renewOverHTTP(t, agent, running.JobID, claim)
	queued := submitOverHTTP(t, client, "restart-queued")
	stop(cancel, done)
	client.CloseIdleConnections()
	agent.CloseIdleConnections()
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = OpenStore(databasePath, StoreOptions{Clock: clock, LeaseDuration: 30 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	cancel, done = start(store)
	defer func() {
		stop(cancel, done)
		if err := store.Close(); err != nil {
			t.Errorf("close restarted store: %v", err)
		}
	}()
	assertHTTPJobState(t, client, running.JobID, contract.JobRunning)
	assertHTTPJobState(t, client, queued.JobID, contract.JobQueued)

	exitCode := 0
	completionPath := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/complete", running.JobID, claim.Lease.AttemptID)
	status, _, body, err := doRequest(agent, http.MethodPost, completionPath, CompletionRequest{
		FencingToken: claim.Lease.FencingToken, IdempotencyKey: "restart-completion", Result: ProcessResult{ExitCode: &exitCode},
	})
	if err != nil {
		t.Fatal(err)
	}
	if status != http.StatusOK {
		t.Fatalf("post-restart completion status = %d body=%s", status, body)
	}
	queuedClaim := claimOverHTTP(t, agent, "node-1", "boot-1")
	if queuedClaim.Job.JobID != queued.JobID {
		t.Fatalf("post-restart claim = %q, want queued job %q", queuedClaim.Job.JobID, queued.JobID)
	}
}

func claimJob(t *testing.T, h *integrationHarness, client *http.Client, nodeID string) Claim {
	t.Helper()
	status, _, body := h.do(client, http.MethodPost, "/v1/agent/jobs/claim", ClaimRequest{NodeID: nodeID, BootSessionID: "boot-" + nodeID})
	if status != http.StatusOK {
		t.Fatalf("claim status = %d body=%s", status, body)
	}
	var claim Claim
	if err := json.Unmarshal(body, &claim); err != nil {
		t.Fatal(err)
	}
	return claim
}

func assertJobAndAttemptState(t *testing.T, store *Store, jobID, attemptID string, jobState contract.JobState, attemptState contract.AttemptState) {
	t.Helper()
	job, err := store.GetJob(context.Background(), jobID)
	if err != nil {
		t.Fatal(err)
	}
	if job.State != jobState {
		t.Fatalf("job state = %q, want %q", job.State, jobState)
	}
	var gotAttempt contract.AttemptState
	if err := store.db.QueryRow("SELECT state FROM attempts WHERE attempt_id=?", attemptID).Scan(&gotAttempt); err != nil {
		t.Fatal(err)
	}
	if gotAttempt != attemptState {
		t.Fatalf("attempt state = %q, want %q", gotAttempt, attemptState)
	}
}

func registerOverHTTP(t *testing.T, client *http.Client, nodeID, bootID string) {
	t.Helper()
	status, _, body, err := doRequest(client, http.MethodPost, "/v1/agent/nodes/register", contract.NodeRegistration{
		NodeID: nodeID, BootSessionID: bootID, OS: "linux", Architecture: "arm64", AgentVersion: "test",
	})
	if err != nil {
		t.Fatal(err)
	}
	if status != http.StatusOK {
		t.Fatalf("register status = %d body=%s", status, body)
	}
}

func submitOverHTTP(t *testing.T, client *http.Client, dispatchKey string) Job {
	t.Helper()
	status, _, body, err := doRequest(client, http.MethodPost, "/v1/jobs", validJobSpec(dispatchKey, []string{"linux"}))
	if err != nil {
		t.Fatal(err)
	}
	if status != http.StatusCreated {
		t.Fatalf("submit status = %d body=%s", status, body)
	}
	var job Job
	if err := json.Unmarshal(body, &job); err != nil {
		t.Fatal(err)
	}
	return job
}

func claimOverHTTP(t *testing.T, client *http.Client, nodeID, bootID string) Claim {
	t.Helper()
	status, _, body, err := doRequest(client, http.MethodPost, "/v1/agent/jobs/claim", ClaimRequest{NodeID: nodeID, BootSessionID: bootID})
	if err != nil {
		t.Fatal(err)
	}
	if status != http.StatusOK {
		t.Fatalf("claim status = %d body=%s", status, body)
	}
	var claim Claim
	if err := json.Unmarshal(body, &claim); err != nil {
		t.Fatal(err)
	}
	return claim
}

func renewOverHTTP(t *testing.T, client *http.Client, jobID string, claim Claim) {
	t.Helper()
	path := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/lease", jobID, claim.Lease.AttemptID)
	status, _, body, err := doRequest(client, http.MethodPost, path, RenewalRequest{FencingToken: claim.Lease.FencingToken})
	if err != nil {
		t.Fatal(err)
	}
	if status != http.StatusOK {
		t.Fatalf("renew status = %d body=%s", status, body)
	}
}

func assertHTTPJobState(t *testing.T, client *http.Client, jobID string, want contract.JobState) {
	t.Helper()
	status, _, body, err := doRequest(client, http.MethodGet, "/v1/jobs/"+jobID, nil)
	if err != nil {
		t.Fatal(err)
	}
	if status != http.StatusOK {
		t.Fatalf("get job status = %d body=%s", status, body)
	}
	var job Job
	if err := json.Unmarshal(body, &job); err != nil {
		t.Fatal(err)
	}
	if job.State != want {
		t.Fatalf("job %s state = %q, want %q", jobID, job.State, want)
	}
}
