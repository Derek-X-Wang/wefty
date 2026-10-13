package l3

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestRunRejectsOverflowingRuntimeSeconds(t *testing.T) {
	h := newIntegrationHarness(t)
	request := inlineRunRequest("#!/bin/sh\nexit 0\n")
	request.Limits = &contract.RunLimits{MaxRuntimeSeconds: 9223372037}
	status, _, body := h.do(h.caller, http.MethodPost, "/v1/runs", request, http.Header{"Idempotency-Key": []string{"overflow"}})
	if status != http.StatusBadRequest || !strings.Contains(string(body), "max_runtime_seconds") {
		t.Fatalf("overflow=%d %s", status, body)
	}
}

func TestReachedLimitProjectsFailureReasonOntoRun(t *testing.T) {
	for _, limit := range []string{"max_runtime", "idle_timeout"} {
		t.Run(limit, func(t *testing.T) {
			h := newIntegrationHarness(t)
			accepted := h.submit(inlineRunRequest("#!/bin/sh\nexit 0\n"), "limit-"+limit)
			reconciler, err := NewReconciler(h.l3Store, h.l1Client, ReconcilerConfig{})
			if err != nil {
				t.Fatal(err)
			}
			if err := reconciler.ReconcileOnce(t.Context()); err != nil {
				t.Fatal(err)
			}
			agent := h.agent()
			status, _, body := h.do(agent, http.MethodPost, "/v1/agent/jobs/claim", l1.ClaimRequest{NodeID: "node-1", BootSessionID: "boot-1", Class: "one-shot"}, nil)
			if status != http.StatusOK {
				t.Fatalf("claim=%d %s", status, body)
			}
			var claim l1.Claim
			if err := json.Unmarshal(body, &claim); err != nil {
				t.Fatal(err)
			}
			completion := map[string]any{"fencing_token": claim.Lease.FencingToken, "idempotency_key": "limit", "termination_initiator": "agent", "result": map[string]any{"exit_code": 0, "limit_reached": limit}}
			path := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/complete", claim.Job.JobID, claim.Lease.AttemptID)
			status, _, body = h.do(agent, http.MethodPost, path, completion, nil)
			if status != http.StatusOK {
				t.Fatalf("complete=%d %s", status, body)
			}
			if err := reconciler.ReconcileOnce(t.Context()); err != nil {
				t.Fatal(err)
			}
			status, _, body = h.do(h.caller, http.MethodGet, "/v1/runs/"+accepted.RunID, nil, nil)
			var record contract.RunRecord
			if err := json.Unmarshal(body, &record); err != nil {
				t.Fatal(err)
			}
			want := "maximum runtime reached (exit 0)"
			if limit == "idle_timeout" {
				want = "idle limit reached (exit 0)"
			}
			if status != http.StatusOK || record.Status != contract.RunFailed || record.FailureReason != want {
				t.Fatalf("run=%d %s", status, body)
			}
		})
	}
}

func TestLimitFailureReasonKeepsPayloadEvidence(t *testing.T) {
	zero := 0
	for _, tc := range []struct {
		name    string
		outcome resultOutcome
		want    string
	}{
		{"exit", resultOutcome{limitReached: contract.LimitReachedMaxRuntime, exitCode: &zero}, "maximum runtime reached (exit 0)"},
		{"signal", resultOutcome{limitReached: contract.LimitReachedIdleTimeout, signal: "killed", cause: "agent"}, "idle limit reached (signal killed (agent))"},
		{"resource flags", resultOutcome{limitReached: contract.LimitReachedMaxRuntime, signal: "killed", cause: "agent", outOfMemory: true}, "maximum runtime reached (signal killed (agent)) [out of memory]"},
		{"output error", resultOutcome{limitReached: contract.LimitReachedMaxRuntime, outputError: "upload logs"}, "maximum runtime reached (output error: upload logs)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.outcome.reason(); got != tc.want {
				t.Fatalf("reason=%q, want %q", got, tc.want)
			}
		})
	}
}
