package l3

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

// TestRunResultProxiesTheLedgerRowForTheCurrentAttempt is the reader's whole
// path: the node uploads once and a person reads it per run, with no node
// involved and no knowledge that a Run is backed by a Job.
func TestRunResultProxiesTheLedgerRowForTheCurrentAttempt(t *testing.T) {
	h := newIntegrationHarness(t)
	accepted := h.submit(inlineRunRequest("#!/bin/sh\nexit 0\n"), "run-result")

	// Before dispatch there is nothing to proxy, and that reads as a typed
	// not-found rather than an empty document.
	status, _, body := h.do(h.caller, http.MethodGet, "/v1/runs/"+accepted.RunID+"/result", nil, nil)
	if status != http.StatusNotFound {
		t.Fatalf("undispatched result status = %d body=%s", status, body)
	}

	reconciler, err := NewReconciler(h.l3Store, h.l1Client, ReconcilerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := reconciler.ReconcileOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	agent := h.agent()
	status, _, body = h.do(agent, http.MethodPost, "/v1/agent/jobs/claim",
		l1.ClaimRequest{NodeID: "node-1", BootSessionID: "boot-1", Class: contract.JobClassOneShot}, nil)
	if status != http.StatusOK {
		t.Fatalf("claim status = %d body=%s", status, body)
	}
	var claim l1.Claim
	if err := json.Unmarshal(body, &claim); err != nil {
		t.Fatal(err)
	}

	// A dispatched run that has not uploaded yet is still a typed not-found.
	status, _, body = h.do(h.caller, http.MethodGet, "/v1/runs/"+accepted.RunID+"/result", nil, nil)
	if status != http.StatusNotFound {
		t.Fatalf("pending result status = %d body=%s", status, body)
	}

	document := []byte(`{"passed":true,"gates":[]}`)
	resultPath := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/result", claim.Job.JobID, claim.Lease.AttemptID)
	status, _, body = h.do(agent, http.MethodPost, resultPath,
		l1.AttemptResultRequest{FencingToken: claim.Lease.FencingToken, Document: document}, nil)
	if status != http.StatusOK {
		t.Fatalf("upload status = %d body=%s", status, body)
	}

	status, _, body = h.do(h.caller, http.MethodGet, "/v1/runs/"+accepted.RunID+"/result", nil, nil)
	if status != http.StatusOK {
		t.Fatalf("read status = %d body=%s", status, body)
	}
	var result RunResult
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatal(err)
	}
	if result.RunID != accepted.RunID {
		t.Fatalf("result run ID = %q, want the run's own %q", result.RunID, accepted.RunID)
	}
	if string(result.Document) != string(document) || result.AttemptID != claim.Lease.AttemptID {
		t.Fatalf("proxied result = %#v", result)
	}
	if result.SHA256 == "" || result.UploadedAt.IsZero() {
		t.Fatalf("proxied result lost its provenance: %#v", result)
	}
}
