package l3

import (
	"context"
	"encoding/json"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestRunTokenScopeDeniesSiblingAndAllowsOwnDescendant(t *testing.T) {
	h := newIntegrationHarness(t)
	parent := h.submit(inlineRunRequest("#!/bin/sh\necho parent\n"), "scope-parent")
	sibling := h.submit(inlineRunRequest("#!/bin/sh\necho sibling\n"), "scope-sibling")
	reconciler, err := NewReconciler(h.l3Store, h.l1Client, ReconcilerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := reconciler.ReconcileOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	tokens := claimRunTokens(t, h, 2)
	parentToken := tokens[parent.RunID]
	if parentToken == "" {
		t.Fatalf("missing parent token in claims: %#v", tokens)
	}

	workflow := h.client(fabric.Identity{NodeID: "workflow-node"}, DefaultL3Address)
	auth := http.Header{"Authorization": []string{"Bearer " + parentToken}}
	status, _, body := h.do(workflow, http.MethodGet, "/v1/runs/"+parent.RunID, nil, auth)
	if status != http.StatusOK {
		t.Fatalf("read own status = %d body=%s", status, body)
	}
	status, _, body = h.do(workflow, http.MethodGet, "/v1/runs/"+sibling.RunID, nil, auth)
	assertAPIError(t, status, body, http.StatusForbidden, contract.ErrorForbidden)

	childRequest := inlineRunRequest("#!/bin/sh\necho child\n")
	childRequest.ParentRunID = parent.RunID
	status, _, body = h.do(workflow, http.MethodPost, "/v1/runs", childRequest, http.Header{
		"Authorization":   []string{"Bearer " + parentToken},
		"Idempotency-Key": []string{"scope-child"},
	})
	if status != http.StatusCreated {
		t.Fatalf("dispatch child = %d body=%s", status, body)
	}
	var child RunAccepted
	if err := json.Unmarshal(body, &child); err != nil {
		t.Fatal(err)
	}
	status, _, body = h.do(workflow, http.MethodGet, "/v1/runs/"+child.RunID, nil, auth)
	if status != http.StatusOK {
		t.Fatalf("read descendant = %d body=%s", status, body)
	}

	parentScope, err := h.l3Store.AuthenticateRunToken(context.Background(), parentToken)
	if err != nil {
		t.Fatal(err)
	}
	status, _, body = h.do(workflow, http.MethodPost, "/v1/runs/"+parent.RunID+"/envelopes", validEnvelope(parent.RunID, parentScope.AttemptID, "scope-parent-envelope"), auth)
	if status != http.StatusCreated {
		t.Fatalf("write own envelope = %d body=%s", status, body)
	}
	status, _, body = h.do(workflow, http.MethodPost, "/v1/runs/"+sibling.RunID+"/gates", nil, auth)
	assertAPIError(t, status, body, http.StatusForbidden, contract.ErrorForbidden)

	badChild := inlineRunRequest("#!/bin/sh\necho bad child\n")
	badChild.ParentRunID = sibling.RunID
	status, _, body = h.do(workflow, http.MethodPost, "/v1/runs", badChild, http.Header{
		"Authorization":   []string{"Bearer " + parentToken},
		"Idempotency-Key": []string{"scope-bad-child"},
	})
	assertAPIError(t, status, body, http.StatusForbidden, contract.ErrorForbidden)

	if err := reconciler.ReconcileOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	childTokens := claimRunTokens(t, h, 1)
	childToken := childTokens[child.RunID]
	if childToken == "" {
		t.Fatalf("missing child token in claims: %#v", childTokens)
	}
	childAuth := http.Header{"Authorization": []string{"Bearer " + childToken}}
	status, _, body = h.do(workflow, http.MethodGet, "/v1/runs/"+parent.RunID, nil, childAuth)
	assertAPIError(t, status, body, http.StatusForbidden, contract.ErrorForbidden)
	status, _, body = h.do(workflow, http.MethodGet, "/v1/runs/"+sibling.RunID, nil, childAuth)
	assertAPIError(t, status, body, http.StatusForbidden, contract.ErrorForbidden)
	status, _, body = h.do(workflow, http.MethodGet, "/v1/runs/"+parent.RunID+"/lineage", nil, childAuth)
	assertAPIError(t, status, body, http.StatusForbidden, contract.ErrorForbidden)
	status, _, body = h.do(workflow, http.MethodPost, "/v1/runs/"+parent.RunID+"/envelopes", nil, childAuth)
	assertAPIError(t, status, body, http.StatusForbidden, contract.ErrorForbidden)
}

func TestRunTokenRequiresFabricIdentityAndDoesNotReplaceFabricPrincipal(t *testing.T) {
	h := newIntegrationHarness(t)
	accepted := h.submit(inlineRunRequest("#!/bin/sh\nexit 0\n"), "identity-layers")
	token, err := h.l3Store.ensureRunToken(context.Background(), accepted.RunID)
	if err != nil {
		t.Fatal(err)
	}

	noIdentity := h.client(fabric.Identity{}, DefaultL3Address)
	status, _, body := h.do(noIdentity, http.MethodGet, "/v1/runs/"+accepted.RunID, nil, http.Header{"Authorization": []string{"Bearer " + token}})
	assertAPIError(t, status, body, http.StatusUnauthorized, contract.ErrorUnauthorized)

	workflowIdentity := fabric.Identity{NodeID: "workflow-node"}
	workflow := h.client(workflowIdentity, DefaultL3Address)
	status, _, body = h.do(workflow, http.MethodGet, "/v1/runs/"+accepted.RunID, nil, http.Header{"Authorization": []string{"Bearer " + token}})
	if status != http.StatusOK {
		t.Fatalf("fabric plus run token = %d body=%s", status, body)
	}
	status, _, body = h.do(workflow, http.MethodGet, "/v1/runs/"+accepted.RunID, nil, nil)
	assertAPIError(t, status, body, http.StatusForbidden, contract.ErrorForbidden)

	l1Workflow := h.client(workflowIdentity, DefaultL1Address)
	status, _, body = h.do(l1Workflow, http.MethodGet, "/v1/jobs/does-not-matter", nil, http.Header{"Authorization": []string{"Bearer " + token}})
	assertAPIError(t, status, body, http.StatusForbidden, contract.ErrorPrincipalForbidden)
}

type mutableClock struct{ now time.Time }

func (c *mutableClock) Now() time.Time { return c.now }

func claimRunTokens(t *testing.T, h *integrationHarness, count int) map[string]string {
	t.Helper()
	agent := h.agent()
	tokens := make(map[string]string, count)
	for range count {
		status, _, body := h.do(agent, http.MethodPost, "/v1/agent/jobs/claim", l1.ClaimRequest{NodeID: "node-1", BootSessionID: "boot-1", Class: contract.JobClassOneShot}, nil)
		if status != http.StatusOK {
			t.Fatalf("claim status = %d body=%s", status, body)
		}
		var claim l1.Claim
		if err := json.Unmarshal(body, &claim); err != nil {
			t.Fatal(err)
		}
		tokens[claim.Job.Spec.Labels["run_id"]] = claim.Job.Spec.Execution.SensitiveEnv[contract.EnvRunToken]
	}
	return tokens
}

func TestRunEnvironmentConstantNames(t *testing.T) {
	want := []string{"WEFTY_HANDOFF_DIR", "WEFTY_L3_ENDPOINT", "WEFTY_RUN_ID", "WEFTY_RUN_TOKEN"}
	got := []string{contract.EnvHandoffDir, contract.EnvL3Endpoint, contract.EnvRunID, contract.EnvRunToken}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("run environment names = %q, want %q", got, want)
	}
}
