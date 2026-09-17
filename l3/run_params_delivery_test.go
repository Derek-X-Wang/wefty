package l3

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

// A dispatched run's parameters travel to the node agent on a label so the job
// can read them from its run mailbox without holding a credential. That makes
// the label internal transport: the claiming agent must see it, and no public
// projection may.
func TestRunParamsReachTheAgentAndNoPublicProjection(t *testing.T) {
	h := newIntegrationHarness(t)
	request := inlineRunRequest("#!/bin/sh\nexit 0\n")
	request.Params = json.RawMessage(`{"ref":"main","secretish":"only-for-the-job"}`)
	run := h.submit(request, "run-params-delivery")

	reconciler, err := NewReconciler(h.l3Store, h.l1Client, ReconcilerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := reconciler.ReconcileOnce(context.Background()); err != nil {
		t.Fatal(err)
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
	if got := claim.Job.Spec.Labels[contract.LabelRunParams]; got != `{"ref":"main","secretish":"only-for-the-job"}` {
		t.Fatalf("claimed params label = %q, want the run's canonical params", got)
	}

	// The public execution projection carries the same job. It must not.
	status, _, body = h.do(h.caller, http.MethodGet, "/v1/runs/"+run.RunID+"/execution", nil, nil)
	if status != http.StatusOK {
		t.Fatalf("execution status = %d body=%s", status, body)
	}
	if bytes.Contains(body, []byte(contract.LabelRunParams)) || bytes.Contains(body, []byte("only-for-the-job")) {
		t.Fatalf("the params label leaked into the execution projection: %s", body)
	}
	var execution RunExecution
	if err := json.Unmarshal(body, &execution); err != nil {
		t.Fatal(err)
	}
	if execution.Job == nil {
		t.Fatal("execution projection has no job")
	}
	if _, present := execution.Job.Spec.Labels[contract.LabelRunParams]; present {
		t.Fatalf("execution projection labels = %v", execution.Job.Spec.Labels)
	}
	if execution.Job.Spec.Labels["run_id"] != run.RunID {
		t.Fatalf("redaction removed the ordinary labels too: %v", execution.Job.Spec.Labels)
	}

	// Redacting a projection must not disturb what the agent later claims.
	status, _, body = h.do(agent, http.MethodPost, "/v1/agent/jobs/claim", l1.ClaimRequest{
		NodeID: "node-1", BootSessionID: "boot-1", Class: contract.JobClassOneShot,
	}, nil)
	if status == http.StatusOK {
		var second l1.Claim
		if err := json.Unmarshal(body, &second); err == nil && second.Job.JobID == claim.Job.JobID {
			if second.Job.Spec.Labels[contract.LabelRunParams] == "" {
				t.Fatal("a public projection removed the params label from the stored job")
			}
		}
	}
}
