//go:build service_acceptance

package l3

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

type diagnosticClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *diagnosticClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *diagnosticClock) Advance(duration time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(duration)
	c.mu.Unlock()
}

type diagnosticDispatchErrorClient struct {
	JobClient
	err error
}

func (c *diagnosticDispatchErrorClient) SubmitJob(context.Context, contract.JobSpec) (l1.Job, error) {
	return l1.Job{}, c.err
}

func TestRunExecutionDiagnosticsServiceAcceptance(t *testing.T) {
	clock := &diagnosticClock{now: time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)}
	h := newIntegrationHarnessWithL1Options(t, l1.StoreOptions{Clock: clock})
	run := h.submit(inlineRunRequest("#!/bin/sh\nexit 0\n"), "execution-diagnostics")
	reconciler, err := NewReconciler(h.l3Store, h.l1Client, ReconcilerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := reconciler.ReconcileOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	record, err := h.l3Store.GetRun(context.Background(), run.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if record.L1JobID == "" || record.Status != contract.RunQueued {
		t.Fatalf("dispatched run = %#v, want queued with l1_job_id", record)
	}

	agent := h.agent()
	status, _, body := h.do(agent, http.MethodPost, "/v1/agent/jobs/claim", l1.ClaimRequest{
		NodeID: "node-1", BootSessionID: "boot-1", Class: contract.JobClassOneShot,
	}, nil)
	if status != http.StatusOK {
		t.Fatalf("claim status = %d body=%s", status, body)
	}
	var claim l1.Claim
	if err := json.Unmarshal(body, &claim); err != nil {
		t.Fatal(err)
	}
	clock.Advance(l1.DefaultLeaseDuration)
	if _, err := h.l1Store.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	exitCode := 0
	completionPath := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/complete", claim.Job.JobID, claim.Lease.AttemptID)
	status, _, body = h.do(agent, http.MethodPost, completionPath, l1.CompletionRequest{
		FencingToken: claim.Lease.FencingToken, IdempotencyKey: "late-exit-zero",
		Result: l1.ProcessResult{ExitCode: &exitCode},
	}, nil)
	assertAPIError(t, status, body, http.StatusConflict, contract.ErrorLeaseExpired)

	status, _, body = h.do(h.caller, http.MethodGet, "/v1/runs/"+run.RunID+"/execution", nil, nil)
	if status != http.StatusOK {
		t.Fatalf("execution status = %d body=%s", status, body)
	}
	var execution RunExecution
	if err := json.Unmarshal(body, &execution); err != nil {
		t.Fatal(err)
	}
	if execution.L1JobID != record.L1JobID || execution.DispatchAttempts != 1 || execution.Job == nil {
		t.Fatalf("execution projection = %#v", execution)
	}
	if execution.Job.State != contract.JobFailed || execution.Job.Spec.Execution.SensitiveEnv != nil || len(execution.Job.Attempts) != 1 {
		t.Fatalf("redacted L1 job = %#v", execution.Job)
	}
	attempt := execution.Job.Attempts[0]
	if attempt.State != contract.AttemptLost || attempt.LateResult == nil || attempt.LateResult.Result == nil ||
		attempt.LateResult.Result.ExitCode == nil || *attempt.LateResult.Result.ExitCode != 0 || !attempt.LateResult.Late {
		t.Fatalf("late attempt evidence = %#v", attempt)
	}
	// L3 is intentionally still queued: this assertion proves the diagnostic
	// crosses the asynchronous projection seam instead of waiting for it.
	record, err = h.l3Store.GetRun(context.Background(), run.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if record.Status != contract.RunQueued {
		t.Fatalf("L3 run status = %q, want stale queued projection", record.Status)
	}
}

func TestRunExecutionDispatchErrorServiceAcceptance(t *testing.T) {
	h := newIntegrationHarness(t)
	run := h.submit(inlineRunRequest("#!/bin/sh\nexit 0\n"), "execution-dispatch-error")
	want := &Error{
		Code: contract.ErrorCapacityExhausted, Message: "eligible node is full", Retryable: true,
		Details: map[string]any{"node_id": "node-1", "capacity": float64(4)}, RequestID: "request-diagnostic-1",
	}
	client := &diagnosticDispatchErrorClient{JobClient: h.l1Client, err: want}
	reconciler, err := NewReconciler(h.l3Store, client, ReconcilerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := reconciler.ReconcileOnce(context.Background()); err == nil {
		t.Fatal("dispatch error was not reported")
	}
	status, _, body := h.do(h.caller, http.MethodGet, "/v1/runs/"+run.RunID+"/execution", nil, nil)
	if status != http.StatusOK {
		t.Fatalf("execution status = %d body=%s", status, body)
	}
	var execution RunExecution
	if err := json.Unmarshal(body, &execution); err != nil {
		t.Fatal(err)
	}
	if execution.DispatchAttempts != 1 || execution.L1JobID != "" || execution.Job != nil || execution.DispatchError == nil ||
		execution.DispatchError.Code != want.Code || execution.DispatchError.Message != want.Message ||
		!execution.DispatchError.Retryable || execution.DispatchError.RequestID != want.RequestID ||
		execution.DispatchError.Details["node_id"] != "node-1" || execution.DispatchError.Details["capacity"] != float64(4) {
		t.Fatalf("dispatch diagnostic = %#v", execution)
	}
}
