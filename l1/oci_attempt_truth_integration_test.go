package l1

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

func TestOCIAttemptEvidenceAgentRoutes(t *testing.T) {
	h := newIntegrationHarness(t, map[string][]string{"node-1": {}})
	registerOCIFixtureNode(t, h)
	job := createOCIFixtureJob(t, h, "oci-agent-routes", contract.JobClassOneShot)
	claim := claimOCIFixture(t, h, contract.JobClassOneShot)
	agent := h.client(fabric.Identity{NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag}})
	path := "/v1/agent/jobs/" + job.JobID + "/attempts/" + claim.Lease.AttemptID
	status, _, body := h.do(agent, http.MethodPut, path+"/image", testImageObservation(claim.Lease.FencingToken))
	if status != http.StatusOK {
		t.Fatalf("image observation status = %d body=%s", status, body)
	}
	status, _, body = h.do(agent, http.MethodPost, path+"/started", StartedRequest{FencingToken: claim.Lease.FencingToken})
	if status != http.StatusOK {
		t.Fatalf("Started status = %d body=%s", status, body)
	}
	var started Job
	if err := json.Unmarshal(body, &started); err != nil {
		t.Fatal(err)
	}
	if started.State != contract.JobRunning {
		t.Fatalf("Started route job state = %s, want running", started.State)
	}
}

const (
	testTopDigest       = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	testPlatformDigest  = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	testRefloatedDigest = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
)

func TestOCICompletionResultMustMatchDurableStartedPhase(t *testing.T) {
	t.Run("pre-start output error", func(t *testing.T) {
		h := newIntegrationHarness(t, map[string][]string{"node-1": {}})
		registerOCIFixtureNode(t, h)
		job := createOCIFixtureJob(t, h, "oci-prestart-output", contract.JobClassOneShot)
		claim := claimOCIFixture(t, h, contract.JobClassOneShot)
		_, err := h.store.CompleteAttempt(context.Background(), "agent", job.JobID, claim.Lease.AttemptID, CompletionRequest{
			FencingToken: claim.Lease.FencingToken, IdempotencyKey: "prestart-output",
			Result: ProcessResult{OutputError: "spool failed"},
		})
		if errorCode(err) != contract.ErrorConflict {
			t.Fatalf("pre-start output_error = %v, want conflict", err)
		}
		assertNoOCICompletionEvidence(t, h, claim.Lease.AttemptID)
	})

	t.Run("late pre-start runtime failure", func(t *testing.T) {
		h := newIntegrationHarness(t, map[string][]string{"node-1": {}})
		registerOCIFixtureNode(t, h)
		job := createOCIFixtureJob(t, h, "oci-late-prestart-runtime", contract.JobClassOneShot)
		claim := claimOCIFixture(t, h, contract.JobClassOneShot)
		h.clock.Advance(DefaultLeaseDuration + time.Second)
		if _, err := h.store.Reconcile(context.Background()); err != nil {
			t.Fatal(err)
		}
		_, err := h.store.CompleteAttempt(context.Background(), "agent", job.JobID, claim.Lease.AttemptID, CompletionRequest{
			FencingToken: claim.Lease.FencingToken, IdempotencyKey: "late-prestart-runtime",
			Result: ProcessResult{RuntimeFailure: &contract.RuntimeFailure{Code: contract.RuntimeFailureUnavailable, Message: "engine lost"}},
		})
		if errorCode(err) != contract.ErrorConflict {
			t.Fatalf("late pre-start runtime_failure = %v, want conflict", err)
		}
		assertNoOCICompletionEvidence(t, h, claim.Lease.AttemptID)
	})

	t.Run("post-start spawn error", func(t *testing.T) {
		h := newIntegrationHarness(t, map[string][]string{"node-1": {}})
		registerOCIFixtureNode(t, h)
		job := createOCIFixtureJob(t, h, "oci-poststart-spawn", contract.JobClassOneShot)
		claim := claimOCIFixture(t, h, contract.JobClassOneShot)
		if _, err := h.store.ObserveAttemptImage(context.Background(), "agent", job.JobID, claim.Lease.AttemptID, testImageObservation(claim.Lease.FencingToken)); err != nil {
			t.Fatal(err)
		}
		if _, err := h.store.StartAttempt(context.Background(), "agent", job.JobID, claim.Lease.AttemptID, StartedRequest{FencingToken: claim.Lease.FencingToken}); err != nil {
			t.Fatal(err)
		}
		_, err := h.store.CompleteAttempt(context.Background(), "agent", job.JobID, claim.Lease.AttemptID, CompletionRequest{
			FencingToken: claim.Lease.FencingToken, IdempotencyKey: "poststart-spawn",
			Result: ProcessResult{SpawnError: &contract.SpawnFailure{Code: contract.SpawnFailureRuntimeUnavailable, Message: "too late"}},
		})
		if errorCode(err) != contract.ErrorConflict {
			t.Fatalf("post-start spawn_error = %v, want conflict", err)
		}
		assertNoOCICompletionEvidence(t, h, claim.Lease.AttemptID)
	})

	t.Run("pre-start OOM is not spawn evidence", func(t *testing.T) {
		h := newIntegrationHarness(t, map[string][]string{"node-1": {}})
		registerOCIFixtureNode(t, h)
		job := createOCIFixtureJob(t, h, "oci-prestart-oom", contract.JobClassOneShot)
		claim := claimOCIFixture(t, h, contract.JobClassOneShot)
		_, err := h.store.CompleteAttempt(context.Background(), "agent", job.JobID, claim.Lease.AttemptID, CompletionRequest{
			FencingToken: claim.Lease.FencingToken, IdempotencyKey: "prestart-oom",
			Result: ProcessResult{SpawnError: &contract.SpawnFailure{Code: contract.SpawnFailureRuntimeUnavailable, Message: "engine lost"}, OOM: true},
		})
		if errorCode(err) != contract.ErrorConflict {
			t.Fatalf("pre-start spawn_error with OOM = %v, want conflict", err)
		}
	})
}

func TestOCIAttemptRequiresStartedForRunning(t *testing.T) {
	h := newIntegrationHarness(t, map[string][]string{"node-1": {}})
	registerOCIFixtureNode(t, h)
	job := createOCIFixtureJob(t, h, "oci-start-truth", contract.JobClassOneShot)
	claim := claimOCIFixture(t, h, contract.JobClassOneShot)

	if _, err := h.store.RenewLease(context.Background(), "agent", job.JobID, claim.Lease.AttemptID, claim.Lease.FencingToken); err != nil {
		t.Fatal(err)
	}
	assertOCIFixtureState(t, h, job.JobID, contract.JobClaimed, contract.AttemptClaimed)

	_, err := h.store.AppendLogs(context.Background(), "agent", job.JobID, claim.Lease.AttemptID, AppendLogsRequest{
		FencingToken: claim.Lease.FencingToken,
		Events:       []contract.LogEvent{{AttemptID: claim.Lease.AttemptID, Stream: contract.LogStdout, Sequence: 0, Timestamp: h.clock.Now(), Bytes: []byte("pre-start")}},
	})
	if err != nil {
		t.Fatal(err)
	}
	assertOCIFixtureState(t, h, job.JobID, contract.JobClaimed, contract.AttemptClaimed)

	exitZero := 0
	_, err = h.store.CompleteAttempt(context.Background(), "agent", job.JobID, claim.Lease.AttemptID, CompletionRequest{
		FencingToken: claim.Lease.FencingToken, IdempotencyKey: "completion-before-started", Result: ProcessResult{ExitCode: &exitZero},
	})
	if errorCode(err) != contract.ErrorConflict {
		t.Fatalf("completion before Started error = %v, want conflict", err)
	}
	assertOCIFixtureState(t, h, job.JobID, contract.JobClaimed, contract.AttemptClaimed)

	observation := testImageObservation(claim.Lease.FencingToken)
	staleObservation := observation
	staleObservation.FencingToken += "-stale"
	if _, err := h.store.ObserveAttemptImage(context.Background(), "agent", job.JobID, claim.Lease.AttemptID, staleObservation); errorCode(err) != contract.ErrorStaleFence {
		t.Fatalf("stale image observation error = %v, want stale fence", err)
	}
	if _, err := h.store.ObserveAttemptImage(context.Background(), "agent", job.JobID, claim.Lease.AttemptID, observation); err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.ObserveAttemptImage(context.Background(), "agent", job.JobID, claim.Lease.AttemptID, observation); err != nil {
		t.Fatalf("identical image replay: %v", err)
	}
	conflict := observation
	conflict.Snapshotter = "future-snapshotter"
	if _, err := h.store.ObserveAttemptImage(context.Background(), "agent", job.JobID, claim.Lease.AttemptID, conflict); errorCode(err) != contract.ErrorIdempotencyConflict {
		t.Fatalf("conflicting image replay error = %v, want idempotency conflict", err)
	}
	if _, err := h.store.StartAttempt(context.Background(), "agent", job.JobID, claim.Lease.AttemptID, StartedRequest{FencingToken: claim.Lease.FencingToken + "-stale"}); errorCode(err) != contract.ErrorStaleFence {
		t.Fatalf("stale Started error = %v, want stale fence", err)
	}
	started := StartedRequest{FencingToken: claim.Lease.FencingToken}
	if _, err := h.store.StartAttempt(context.Background(), "agent", job.JobID, claim.Lease.AttemptID, started); err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.StartAttempt(context.Background(), "agent", job.JobID, claim.Lease.AttemptID, started); err != nil {
		t.Fatalf("Started replay: %v", err)
	}
	assertOCIFixtureState(t, h, job.JobID, contract.JobRunning, contract.AttemptRunning)
	attempts, err := h.store.ListJobAttempts(context.Background(), job.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if attempts[0].Image == nil || attempts[0].Image.ResolvedAt.IsZero() || attempts[0].Image.StartedAt == nil ||
		attempts[0].Image.Platform.OS != "linux" || attempts[0].Image.TopLevelDigest != testTopDigest {
		t.Fatalf("image/start evidence = %#v", attempts[0].Image)
	}
	completed, err := h.store.CompleteAttempt(context.Background(), "agent", job.JobID, claim.Lease.AttemptID, CompletionRequest{
		FencingToken: claim.Lease.FencingToken, IdempotencyKey: "completion-after-started", Result: ProcessResult{ExitCode: &exitZero},
	})
	if err != nil || completed.State != contract.JobSucceeded {
		t.Fatalf("completion after Started = %#v err %v", completed, err)
	}
	replayedStarted, err := h.store.StartAttempt(context.Background(), "agent", job.JobID, claim.Lease.AttemptID, started)
	if err != nil || replayedStarted.State != contract.JobSucceeded {
		t.Fatalf("Started replay after completion = %#v err %v", replayedStarted, err)
	}
}

func TestOCIPrestartRuntimeLossRequeuesOnceAndExhaustsBudget(t *testing.T) {
	h := newIntegrationHarnessWithOptions(t, StoreOptions{
		PrestartInfrastructureBudget: 1500 * time.Millisecond,
		Jitter:                       func(delay time.Duration) time.Duration { return delay },
	}, map[string]NodePolicy{
		"node-1": DefaultNodePolicy(),
	})
	registerOCIFixtureNode(t, h)
	job := createOCIFixtureJob(t, h, "oci-prestart-budget", contract.JobClassOneShot)
	first := claimOCIFixture(t, h, contract.JobClassOneShot)
	if _, err := h.store.ObserveAttemptImage(context.Background(), "agent", job.JobID, first.Lease.AttemptID, testImageObservation(first.Lease.FencingToken)); err != nil {
		t.Fatal(err)
	}
	request := CompletionRequest{
		FencingToken: first.Lease.FencingToken, IdempotencyKey: "runtime-loss-1",
		Result: ProcessResult{SpawnError: &contract.SpawnFailure{Code: contract.SpawnFailureRuntimeUnavailable, Message: "engine unavailable"}},
	}
	requeued, err := h.store.CompleteAttempt(context.Background(), "agent", job.JobID, first.Lease.AttemptID, request)
	if err != nil || requeued.State != contract.JobQueued {
		t.Fatalf("first runtime loss = job %#v err %v", requeued, err)
	}
	if requeued.CurrentAttemptID != "" || requeued.NodeID != "" {
		t.Fatalf("requeued job retained terminal attempt authority = %#v", requeued)
	}
	replay, err := h.store.CompleteAttempt(context.Background(), "agent", job.JobID, first.Lease.AttemptID, request)
	if err != nil || replay.State != contract.JobQueued {
		t.Fatalf("runtime loss replay = job %#v err %v", replay, err)
	}
	if claim, err := h.store.ClaimJob(context.Background(), "agent", "node-1", "boot-node-1", contract.JobClassOneShot); err != nil || claim != nil {
		t.Fatalf("claim before backoff = %#v err %v", claim, err)
	}
	var retryCount int
	var deadlineNS, nextRetryNS int64
	if err := h.store.db.QueryRow(`SELECT prestart_retry_count, prestart_budget_deadline_ns, prestart_next_retry_at_ns FROM jobs WHERE job_id=?`, job.JobID).
		Scan(&retryCount, &deadlineNS, &nextRetryNS); err != nil {
		t.Fatal(err)
	}
	if retryCount != 1 || nextRetryNS <= h.clock.Now().UnixNano() {
		t.Fatalf("persisted retry = count %d deadline %d next %d", retryCount, deadlineNS, nextRetryNS)
	}

	h.clock.Advance(time.Second)
	second := claimOCIFixture(t, h, contract.JobClassOneShot)
	refloated := testImageObservation(second.Lease.FencingToken)
	refloated.TopLevelDigest = testRefloatedDigest
	if _, err := h.store.ObserveAttemptImage(context.Background(), "agent", job.JobID, second.Lease.AttemptID, refloated); errorCode(err) != contract.ErrorIdempotencyConflict {
		t.Fatalf("re-floated job image observation = %v, want idempotency conflict", err)
	}
	secondRequest := CompletionRequest{
		FencingToken: second.Lease.FencingToken, IdempotencyKey: "runtime-loss-2",
		Result: ProcessResult{SpawnError: &contract.SpawnFailure{Code: contract.SpawnFailureRuntimeUnavailable, Message: "engine still unavailable"}},
	}
	exhausted, err := h.store.CompleteAttempt(context.Background(), "agent", job.JobID, second.Lease.AttemptID, secondRequest)
	if err != nil || exhausted.State != contract.JobFailed {
		t.Fatalf("exhausted runtime loss = job %#v err %v", exhausted, err)
	}
	if _, err := h.store.CompleteAttempt(context.Background(), "agent", job.JobID, first.Lease.AttemptID, request); errorCode(err) != contract.ErrorAttemptMismatch {
		t.Fatalf("superseded retry error = %v, want attempt mismatch", err)
	}
	attempts, err := h.store.ListJobAttempts(context.Background(), job.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if len(attempts) != 2 || attempts[0].State != contract.AttemptFailed || attempts[1].State != contract.AttemptFailed ||
		attempts[0].Result == nil || attempts[0].Result.SpawnError.Code != contract.SpawnFailureRuntimeUnavailable {
		t.Fatalf("terminal attempt evidence = %#v", attempts)
	}
}

func TestOCIServiceRuntimeFailureClassifierDefaultsTerminal(t *testing.T) {
	for _, test := range []struct {
		name        string
		code        contract.RuntimeFailureCode
		want        contract.JobState
		wantBackoff bool
	}{
		{name: "known infrastructure", code: contract.RuntimeFailureUnavailable, want: contract.JobQueued, wantBackoff: true},
		{name: "unknown terminal", code: "future_runtime_failure", want: contract.JobFailed},
	} {
		t.Run(test.name, func(t *testing.T) {
			h := newIntegrationHarness(t, map[string][]string{"node-1": {}})
			registerOCIFixtureNode(t, h)
			job := createOCIFixtureJob(t, h, "oci-service-"+test.name, contract.JobClassService)
			claim := claimOCIFixture(t, h, contract.JobClassService)
			if _, err := h.store.ObserveAttemptImage(context.Background(), "agent", job.JobID, claim.Lease.AttemptID, testImageObservation(claim.Lease.FencingToken)); err != nil {
				t.Fatal(err)
			}
			if _, err := h.store.StartAttempt(context.Background(), "agent", job.JobID, claim.Lease.AttemptID, StartedRequest{FencingToken: claim.Lease.FencingToken}); err != nil {
				t.Fatal(err)
			}
			completed, err := h.store.CompleteAttempt(context.Background(), "agent", job.JobID, claim.Lease.AttemptID, CompletionRequest{
				FencingToken: claim.Lease.FencingToken, IdempotencyKey: "runtime-result",
				Result: ProcessResult{RuntimeFailure: &contract.RuntimeFailure{Code: test.code, Message: "runtime disappeared"}},
			})
			if err != nil || completed.State != test.want || completed.RestartStreak != 0 || (completed.NextRestartAt != nil) != test.wantBackoff {
				t.Fatalf("completion = %#v err %v, want %s with unchanged streak", completed, err, test.want)
			}
		})
	}
}

func TestOCIResultArmsAndTransitions(t *testing.T) {
	exit := 137
	for _, result := range []ProcessResult{
		{SpawnError: &contract.SpawnFailure{Code: contract.SpawnFailureOCISpecRejected, Message: "invalid mount"}},
		{RuntimeFailure: &contract.RuntimeFailure{Code: contract.RuntimeFailureUnavailable, Message: "helper lost"}},
		{OutputError: "log finalization failed"},
		{ExitCode: &exit, OOM: true},
		{Signal: "killed", TerminationCause: contract.TerminationCauseGuardian, OOM: true},
	} {
		if err := validateProcessResult(result); err != nil {
			t.Fatalf("valid result %#v rejected: %v", result, err)
		}
	}
	if err := validateProcessResult(ProcessResult{ExitCode: &exit, RuntimeFailure: &contract.RuntimeFailure{Code: contract.RuntimeFailureUnavailable, Message: "lost"}}); err == nil {
		t.Fatal("two primary result arms accepted")
	}
	if !contract.CanTransition(contract.JobTransitions, contract.JobClaimed, contract.JobQueued) ||
		!contract.CanTransition(contract.AttemptTransitions, contract.AttemptClaimed, contract.AttemptFailed) {
		t.Fatal("pre-start requeue transition table is incomplete")
	}
}

func registerOCIFixtureNode(t *testing.T, h *integrationHarness) {
	t.Helper()
	agent := h.client(fabric.Identity{NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag}})
	h.register(agent, "node-1")
}

func createOCIFixtureJob(t *testing.T, h *integrationHarness, dispatchKey, class string) Job {
	t.Helper()
	processSpec := validJobSpec(dispatchKey, nil)
	processSpec.Class = class
	if class == contract.JobClassService {
		processSpec.Execution.HandoffDirectory = ""
		processSpec.Restart = contract.RestartAlways
	}
	job, _, err := h.store.CreateJob(context.Background(), processSpec)
	if err != nil {
		t.Fatal(err)
	}
	digest := testTopDigest
	image := contract.OCIImageSpec{Reference: "ghcr.io/example/tool:latest"}
	if class == contract.JobClassService {
		image.Digest = &digest
	}
	ociSpec := contract.JobSpec{
		SchemaVersion: contract.SchemaVersionV1, DispatchKey: dispatchKey, Kind: contract.JobKindOCI, Class: class,
		Restart: processSpec.Restart, RuntimeHandler: "io.containerd.runc.v2",
		Execution: contract.ExecutionSpec{OCI: &contract.OCIExecutionSpec{Image: image}},
	}
	if err := contract.ValidateJobSpec(ociSpec); err != nil {
		t.Fatalf("OCI fixture does not satisfy the public contract: %v", err)
	}
	raw, err := json.Marshal(ociSpec)
	if err != nil {
		t.Fatal(err)
	}
	// TODO(#135): CreateJob deliberately rejects OCI until capability-aware
	// claiming lands, so this fixture patches only the already-validated spec.
	if _, err := h.store.db.Exec(`UPDATE jobs SET spec_json=? WHERE job_id=?`, raw, job.JobID); err != nil {
		t.Fatal(err)
	}
	job, err = h.store.GetJob(context.Background(), job.JobID)
	if err != nil {
		t.Fatal(err)
	}
	return job
}

func assertNoOCICompletionEvidence(t *testing.T, h *integrationHarness, attemptID string) {
	t.Helper()
	var completionKey, resultJSON, lateResultJSON []byte
	if err := h.store.db.QueryRow(`SELECT completion_key, result_json, late_result_json FROM attempts WHERE attempt_id=?`, attemptID).
		Scan(&completionKey, &resultJSON, &lateResultJSON); err != nil {
		t.Fatal(err)
	}
	if len(completionKey) != 0 || len(resultJSON) != 0 || len(lateResultJSON) != 0 {
		t.Fatalf("rejected completion stored evidence key=%q result=%s late=%s", completionKey, resultJSON, lateResultJSON)
	}
}

func claimOCIFixture(t *testing.T, h *integrationHarness, class string) *Claim {
	t.Helper()
	claim, err := h.store.ClaimJob(context.Background(), "agent", "node-1", "boot-node-1", class)
	if err != nil || claim == nil {
		t.Fatalf("claim = %#v err %v", claim, err)
	}
	return claim
}

func testImageObservation(fence string) ImageObservationRequest {
	return ImageObservationRequest{
		FencingToken: fence, SubmittedReference: "ghcr.io/example/tool:latest", TopLevelDigest: testTopDigest,
		TopLevelMediaType: "application/vnd.oci.image.index.v1+json", PlatformManifestDigest: testPlatformDigest,
		Platform:       OCIPlatform{OS: "linux", Architecture: "arm64", Variant: "v8"},
		RuntimeHandler: "io.containerd.runc.v2", Snapshotter: "overlayfs",
	}
}

func assertOCIFixtureState(t *testing.T, h *integrationHarness, jobID string, jobState contract.JobState, attemptState contract.AttemptState) {
	t.Helper()
	job, err := h.store.GetJob(context.Background(), jobID)
	if err != nil {
		t.Fatal(err)
	}
	attempts, err := h.store.ListJobAttempts(context.Background(), jobID)
	if err != nil {
		t.Fatal(err)
	}
	if job.State != jobState || len(attempts) != 1 || attempts[0].State != attemptState {
		t.Fatalf("job/attempt state = %s/%#v, want %s/%s", job.State, attempts, jobState, attemptState)
	}
}
