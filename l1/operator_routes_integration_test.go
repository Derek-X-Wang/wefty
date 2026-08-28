package l1

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

func TestServiceOperatorRouteOwnershipListAndProjection(t *testing.T) {
	assertServiceOperatorRouteOwnershipListAndProjection(t)
}

func assertServiceOperatorRouteOwnershipListAndProjection(t *testing.T) {
	t.Helper()
	h := newIntegrationHarness(t, nil)
	client := h.client(fabric.Identity{NodeID: "operator", Tags: []string{DefaultClientPrincipalTag}})

	created := make([]Job, 0, 3)
	for _, key := range []string{"operator-list-c", "operator-list-a", "operator-list-b"} {
		spec := operatorServiceSpec(key, []string{"missing-node"})
		spec.Execution.SensitiveEnv = map[string]string{"OPERATOR_SECRET": "must-not-leak"}
		status, _, body := h.do(client, http.MethodPost, "/v1/jobs", spec)
		if status != http.StatusCreated {
			t.Fatalf("create service status = %d body=%s", status, body)
		}
		if bytes.Contains(body, []byte("must-not-leak")) || bytes.Contains(body, []byte("OPERATOR_SECRET")) {
			t.Fatalf("create response leaked SensitiveEnv: %s", body)
		}
		var job Job
		if err := json.Unmarshal(body, &job); err != nil {
			t.Fatal(err)
		}
		if job.Status != "unschedulable" || !strings.Contains(job.UnschedulableReason, "routing tags") || job.SlotHeld {
			t.Fatalf("unbound status projection = %#v", job)
		}
		created = append(created, job)
	}
	oneshoot := h.submit(client, "operator-list-one-shot", nil)

	status, _, body := h.do(client, http.MethodGet, "/v1/jobs", nil)
	assertAPIError(t, status, body, http.StatusBadRequest, contract.ErrorInvalidRequest)
	status, _, body = h.do(client, http.MethodGet, "/v1/jobs/", nil)
	assertAPIError(t, status, body, http.StatusBadRequest, contract.ErrorInvalidRequest)
	status, _, body = h.do(client, http.MethodGet, "/v1/jobs?class=one-shot", nil)
	assertAPIError(t, status, body, http.StatusBadRequest, contract.ErrorInvalidRequest)
	status, _, body = h.do(client, http.MethodGet, "/v1/jobs/?class=service&limit=1", nil)
	if status != http.StatusOK {
		t.Fatalf("trailing-slash service list = %d body=%s", status, body)
	}

	expectedIDs := []string{created[0].JobID, created[1].JobID, created[2].JobID}
	sort.Strings(expectedIDs)
	listedIDs := []string{}
	cursor := ""
	for {
		path := "/v1/jobs?class=service&limit=2"
		if cursor != "" {
			path += "&cursor=" + cursor
		}
		status, _, body = h.do(client, http.MethodGet, path, nil)
		if status != http.StatusOK {
			t.Fatalf("list services status = %d body=%s", status, body)
		}
		if bytes.Contains(body, []byte("must-not-leak")) || bytes.Contains(body, []byte("OPERATOR_SECRET")) {
			t.Fatalf("list response leaked SensitiveEnv: %s", body)
		}
		var page JobList
		if err := json.Unmarshal(body, &page); err != nil {
			t.Fatal(err)
		}
		for _, job := range page.Jobs {
			if job.Spec.Class != contract.JobClassService || job.Status != "unschedulable" || job.SlotHeld {
				t.Fatalf("listed service projection = %#v", job)
			}
			listedIDs = append(listedIDs, job.JobID)
		}
		cursor = page.NextCursor
		if cursor == "" {
			break
		}
	}
	if len(listedIDs) != len(expectedIDs) {
		t.Fatalf("listed IDs = %v, want %v", listedIDs, expectedIDs)
	}
	for index := range expectedIDs {
		if listedIDs[index] != expectedIDs[index] {
			t.Fatalf("stable list order = %v, want %v", listedIDs, expectedIDs)
		}
	}
	status, _, body = h.do(client, http.MethodGet, "/v1/jobs?class=service&cursor=not-a-cursor", nil)
	assertAPIError(t, status, body, http.StatusBadRequest, contract.ErrorInvalidRequest)

	serviceID := created[0].JobID
	status, _, body = h.do(client, http.MethodGet, "/v1/jobs/"+serviceID, nil)
	assertAPIError(t, status, body, http.StatusBadRequest, contract.ErrorInvalidRequest)
	status, _, body = h.do(client, http.MethodGet, "/v1/jobs/"+serviceID+"?class=service", nil)
	if status != http.StatusOK || bytes.Contains(body, []byte("must-not-leak")) {
		t.Fatalf("scoped status = %d body=%s", status, body)
	}
	nextRestart := h.clock.Now().Add(DefaultServiceStabilityWindow)
	if _, err := h.store.db.Exec("UPDATE service_jobs SET next_restart_at=? WHERE job_id=?", nextRestart.UnixNano(), serviceID); err != nil {
		t.Fatal(err)
	}
	restartPending := getOperatorService(t, h, client, serviceID)
	if restartPending.State != contract.JobQueued || restartPending.Status != "restart-pending" || restartPending.UnschedulableReason != "" {
		t.Fatalf("restart-pending projection = %#v", restartPending)
	}
	status, _, body = h.do(client, http.MethodGet, "/v1/jobs/"+oneshoot.JobID+"?class=service", nil)
	assertAPIError(t, status, body, http.StatusNotFound, contract.ErrorNotFound)
	status, _, body = h.do(client, http.MethodGet, "/v1/jobs/"+oneshoot.JobID, nil)
	if status != http.StatusOK {
		t.Fatalf("internal one-shot status compatibility = %d body=%s", status, body)
	}

	status, _, body = h.do(client, http.MethodGet, "/v1/jobs/"+serviceID+"/logs", nil)
	assertAPIError(t, status, body, http.StatusBadRequest, contract.ErrorInvalidRequest)
	status, _, body = h.do(client, http.MethodGet, "/v1/jobs/"+serviceID+"/logs?class=service", nil)
	if status != http.StatusOK {
		t.Fatalf("scoped service logs = %d body=%s", status, body)
	}
	status, _, body = h.do(client, http.MethodPost, "/v1/jobs/"+serviceID+"/remove", nil)
	assertAPIError(t, status, body, http.StatusBadRequest, contract.ErrorInvalidRequest)
	if _, err := h.store.GetJob(context.Background(), serviceID); err != nil {
		t.Fatalf("unscoped removal mutated the service: %v", err)
	}
}

func TestServiceOperatorDesiredStateRestartCapacityAndLogs(t *testing.T) {
	assertServiceOperatorDesiredStateRestartCapacityAndLogs(t)
}

func assertServiceOperatorDesiredStateRestartCapacityAndLogs(t *testing.T) {
	t.Helper()
	h := newIntegrationHarnessWithPolicies(t, map[string]NodePolicy{
		"service-node": {Tags: []string{"service"}, MaxOneshotSlots: 4, MaxServiceSlots: 1},
	})
	client := h.client(fabric.Identity{NodeID: "operator", Tags: []string{DefaultClientPrincipalTag}})
	agent := h.client(fabric.Identity{NodeID: "fabric-service-node", Tags: []string{DefaultAgentPrincipalTag}})
	node := h.register(agent, "service-node")
	first := submitOperatorService(t, h, client, "operator-first")
	firstAttempt := claimOperatorService(t, h, node, first.JobID)
	appendOperatorLog(t, h, first.JobID, firstAttempt, 0, "first attempt")
	second := submitOperatorService(t, h, client, "operator-second")
	statusJob := getOperatorService(t, h, client, first.JobID)
	if statusJob.Status != string(contract.JobRunning) || !statusJob.SlotHeld || statusJob.NodeState != contract.NodeAlive {
		t.Fatalf("bound status projection = %#v", statusJob)
	}
	blocked := getOperatorService(t, h, client, second.JobID)
	if blocked.State != contract.JobQueued || blocked.Status != "unschedulable" ||
		!strings.Contains(blocked.UnschedulableReason, "service-node occupancy 1/1") {
		t.Fatalf("capacity-blocked projection = %#v", blocked)
	}

	status, _, body := h.do(client, http.MethodPut, "/v1/jobs/"+first.JobID+"/desired-state", ServiceDesiredStateRequest{DesiredState: contract.ServiceDesiredStopped})
	assertAPIError(t, status, body, http.StatusBadRequest, contract.ErrorInvalidRequest)
	status, _, body = h.do(client, http.MethodPut, serviceMutationPath(first.JobID, "desired-state"), ServiceDesiredStateRequest{DesiredState: contract.ServiceDesiredStopped})
	if status != http.StatusAccepted {
		t.Fatalf("stop service = %d body=%s", status, body)
	}
	var stopping Job
	if err := json.Unmarshal(body, &stopping); err != nil {
		t.Fatal(err)
	}
	if stopping.State != contract.JobStopping || stopping.DesiredState != contract.ServiceDesiredStopped || !stopping.SlotHeld {
		t.Fatalf("stopping projection = %#v", stopping)
	}
	completeOperatorAttempt(t, h, first.JobID, firstAttempt, ProcessResult{
		Signal: "terminated", TerminationCause: contract.TerminationCauseAgent,
	}, "stop-first")
	stopped := getOperatorService(t, h, client, first.JobID)
	if stopped.State != contract.JobStopped || stopped.SlotHeld || stopped.RestartSuppressed == "" {
		t.Fatalf("stopped projection = %#v", stopped)
	}

	secondAttempt := claimOperatorService(t, h, node, second.JobID)
	status, _, body = h.do(client, http.MethodPut, serviceMutationPath(first.JobID, "desired-state"), ServiceDesiredStateRequest{DesiredState: contract.ServiceDesiredRunning})
	assertCapacityError(t, status, body, "service-node", 1, 1)
	status, _, body = h.do(client, http.MethodPost, serviceMutationPath(first.JobID, "restart"), ServiceRestartRequest{IdempotencyKey: "capacity-restart"})
	assertCapacityError(t, status, body, "service-node", 1, 1)
	status, _, body = h.do(client, http.MethodPost, "/v1/jobs/"+first.JobID+"/restart", ServiceRestartRequest{IdempotencyKey: "unscoped-restart"})
	assertAPIError(t, status, body, http.StatusBadRequest, contract.ErrorInvalidRequest)
	var failedCapacityRestartRows int
	if err := h.store.db.QueryRow("SELECT COUNT(*) FROM service_restart_requests WHERE job_id=?", first.JobID).Scan(&failedCapacityRestartRows); err != nil {
		t.Fatal(err)
	}
	if failedCapacityRestartRows != 0 {
		t.Fatalf("capacity-refused restart rows = %d, want 0", failedCapacityRestartRows)
	}

	status, _, body = h.do(client, http.MethodPut, serviceMutationPath(second.JobID, "desired-state"), ServiceDesiredStateRequest{DesiredState: contract.ServiceDesiredStopped})
	if status != http.StatusAccepted {
		t.Fatalf("stop capacity holder = %d body=%s", status, body)
	}
	completeOperatorAttempt(t, h, second.JobID, secondAttempt, ProcessResult{
		Signal: "terminated", TerminationCause: contract.TerminationCauseAgent,
	}, "stop-second")

	status, _, body = h.do(client, http.MethodPut, serviceMutationPath(first.JobID, "desired-state"), ServiceDesiredStateRequest{DesiredState: contract.ServiceDesiredRunning})
	if status != http.StatusAccepted {
		t.Fatalf("start after capacity release = %d body=%s", status, body)
	}
	resumedAttempt := claimOperatorService(t, h, node, first.JobID)
	appendOperatorLog(t, h, first.JobID, resumedAttempt, 0, "resumed attempt")
	status, _, body = h.do(client, http.MethodPut, serviceMutationPath(first.JobID, "desired-state"), ServiceDesiredStateRequest{DesiredState: contract.ServiceDesiredRunning})
	if status != http.StatusAccepted {
		t.Fatalf("idempotent healthy start = %d body=%s", status, body)
	}
	var unchanged Job
	if err := json.Unmarshal(body, &unchanged); err != nil {
		t.Fatal(err)
	}
	if unchanged.CurrentAttemptID != resumedAttempt.AttemptID {
		t.Fatalf("healthy start disturbed attempt: got %q want %q", unchanged.CurrentAttemptID, resumedAttempt.AttemptID)
	}

	completeOperatorAttempt(t, h, first.JobID, resumedAttempt, ProcessResult{
		SpawnError: &contract.SpawnFailure{Code: contract.SpawnFailureUnsupportedKind, Message: "terminal"},
	}, "latch-first")
	failed := getOperatorService(t, h, client, first.JobID)
	if failed.State != contract.JobFailed || failed.SlotHeld || failed.LastFailure == nil || failed.RestartSuppressed == "" {
		t.Fatalf("latched failure projection = %#v", failed)
	}
	status, _, body = h.do(client, http.MethodPut, serviceMutationPath(first.JobID, "desired-state"), ServiceDesiredStateRequest{DesiredState: contract.ServiceDesiredRunning})
	if status != http.StatusConflict || !bytes.Contains(body, []byte("use restart")) {
		t.Fatalf("latched start refusal = %d body=%s", status, body)
	}

	restartRequest := ServiceRestartRequest{IdempotencyKey: "restart-after-failure"}
	status, _, body = h.do(client, http.MethodPost, serviceMutationPath(first.JobID, "restart"), restartRequest)
	if status != http.StatusAccepted {
		t.Fatalf("restart latched service = %d body=%s", status, body)
	}
	var restarted Job
	if err := json.Unmarshal(body, &restarted); err != nil {
		t.Fatal(err)
	}
	if restarted.State != contract.JobQueued || restarted.LastFailure != nil || restarted.RestartStreak != 0 || restarted.NextRestartAt != nil {
		t.Fatalf("restart did not clear policy metadata: %#v", restarted)
	}
	freshAttempt := claimOperatorService(t, h, node, first.JobID)
	status, headers, body := h.do(client, http.MethodPost, serviceMutationPath(first.JobID, "restart"), restartRequest)
	if status != http.StatusOK || headers.Get("Idempotency-Replayed") != "true" {
		t.Fatalf("restart replay = %d headers=%v body=%s", status, headers, body)
	}
	lease, err := h.store.RenewLease(context.Background(), "fabric-service-node", first.JobID, freshAttempt.AttemptID, freshAttempt.FencingToken)
	if err != nil {
		t.Fatal(err)
	}
	if lease.Directive != "" {
		t.Fatalf("lost-response replay restarted the fresh attempt: directive=%q", lease.Directive)
	}

	status, _, body = h.do(client, http.MethodPost, serviceMutationPath(first.JobID, "restart"), ServiceRestartRequest{IdempotencyKey: "restart-active"})
	if status != http.StatusAccepted {
		t.Fatalf("active restart = %d body=%s", status, body)
	}
	lease, err = h.store.RenewLease(context.Background(), "fabric-service-node", first.JobID, freshAttempt.AttemptID, freshAttempt.FencingToken)
	if err != nil {
		t.Fatal(err)
	}
	if lease.Directive != AttemptDirectiveRestart {
		t.Fatalf("active restart directive = %q, want %q", lease.Directive, AttemptDirectiveRestart)
	}

	status, _, body = h.do(client, http.MethodGet, "/v1/jobs/"+first.JobID+"/logs?class=service&limit=100", nil)
	if status != http.StatusOK {
		t.Fatalf("service logs = %d body=%s", status, body)
	}
	var logs LogPage
	if err := json.Unmarshal(body, &logs); err != nil {
		t.Fatal(err)
	}
	attempts := map[string]bool{}
	for _, event := range logs.Events {
		attempts[event.AttemptID] = true
	}
	if !attempts[firstAttempt.AttemptID] || !attempts[resumedAttempt.AttemptID] {
		t.Fatalf("service logs did not span attempts: %#v", logs.Events)
	}
}

func operatorServiceSpec(dispatchKey string, tags []string) contract.JobSpec {
	spec := validJobSpec(dispatchKey, tags)
	spec.Class = contract.JobClassService
	spec.Execution.HandoffDirectory = ""
	spec.Restart = contract.RestartAlways
	return spec
}

func submitOperatorService(t *testing.T, h *integrationHarness, client *http.Client, dispatchKey string) Job {
	t.Helper()
	status, _, body := h.do(client, http.MethodPost, "/v1/jobs", operatorServiceSpec(dispatchKey, []string{"service"}))
	if status != http.StatusCreated {
		t.Fatalf("create operator service = %d body=%s", status, body)
	}
	var job Job
	if err := json.Unmarshal(body, &job); err != nil {
		t.Fatal(err)
	}
	return job
}

func claimOperatorService(t *testing.T, h *integrationHarness, node Node, wantJobID string) AttemptLease {
	t.Helper()
	claim, err := h.store.ClaimJob(context.Background(), "fabric-service-node", node.NodeID, node.BootSessionID, contract.JobClassService)
	if err != nil {
		t.Fatal(err)
	}
	if claim == nil || claim.Job.JobID != wantJobID {
		t.Fatalf("claimed service = %#v, want %q", claim, wantJobID)
	}
	return claim.Lease
}

func appendOperatorLog(t *testing.T, h *integrationHarness, jobID string, attempt AttemptLease, sequence uint64, contents string) {
	t.Helper()
	event := logEvent(attempt.AttemptID, contract.LogStdout, sequence, []byte(contents))
	if _, err := h.store.AppendLogs(context.Background(), "fabric-service-node", jobID, attempt.AttemptID, AppendLogsRequest{
		FencingToken: attempt.FencingToken, Events: []contract.LogEvent{event},
	}); err != nil {
		t.Fatal(err)
	}
}

func completeOperatorAttempt(t *testing.T, h *integrationHarness, jobID string, attempt AttemptLease, result ProcessResult, key string) {
	t.Helper()
	if _, err := h.store.CompleteAttempt(context.Background(), "fabric-service-node", jobID, attempt.AttemptID, CompletionRequest{
		FencingToken: attempt.FencingToken, IdempotencyKey: key, Result: result,
		RuntimeQuiescenceEvidence: RuntimeQuiescenceAttempt,
	}); err != nil {
		t.Fatal(err)
	}
}

func getOperatorService(t *testing.T, h *integrationHarness, client *http.Client, jobID string) Job {
	t.Helper()
	status, _, body := h.do(client, http.MethodGet, "/v1/jobs/"+jobID+"?class=service", nil)
	if status != http.StatusOK {
		t.Fatalf("get operator service = %d body=%s", status, body)
	}
	var job Job
	if err := json.Unmarshal(body, &job); err != nil {
		t.Fatal(err)
	}
	return job
}

func serviceMutationPath(jobID, operation string) string {
	return "/v1/jobs/" + jobID + "/" + operation + "?class=service"
}

func assertCapacityError(t *testing.T, status int, body []byte, nodeID string, occupancy, capacity int) {
	t.Helper()
	if status != http.StatusConflict {
		t.Fatalf("capacity status = %d body=%s", status, body)
	}
	var response contract.ErrorResponse
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatal(err)
	}
	if response.Error.Code != contract.ErrorCapacityExhausted || !response.Error.Retryable ||
		response.Error.Details["node_id"] != nodeID || response.Error.Details["occupancy"] != float64(occupancy) ||
		response.Error.Details["capacity"] != float64(capacity) ||
		!strings.Contains(response.Error.Message, nodeID) || !strings.Contains(response.Error.Message, "1/1") {
		t.Fatalf("capacity error = %#v", response.Error)
	}
}
