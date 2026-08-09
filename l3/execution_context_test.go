package l3

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"net/http"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestDispatchDeliversExactRunEnvironmentAndStoresTokenHash(t *testing.T) {
	h := newIntegrationHarness(t)
	accepted := h.submit(inlineRunRequest("#!/bin/sh\nexit 0\n"), "execution-env")
	reconciler, err := NewReconciler(h.l3Store, h.l1Client, ReconcilerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := reconciler.ReconcileOnce(context.Background()); err != nil {
		t.Fatal(err)
	}

	agent := h.agent()
	status, _, body := h.do(agent, http.MethodPost, "/v1/agent/jobs/claim", l1.ClaimRequest{NodeID: "node-1", BootSessionID: "boot-1"}, nil)
	if status != http.StatusOK {
		t.Fatalf("claim status = %d body=%s", status, body)
	}
	var claim l1.Claim
	if err := json.Unmarshal(body, &claim); err != nil {
		t.Fatal(err)
	}
	handoff := filepath.Join(DefaultHandoffRoot, accepted.RunID)
	wantPublic := map[string]string{
		contract.EnvRunID: accepted.RunID, contract.EnvL1Endpoint: DefaultL1Address,
		contract.EnvL3Endpoint: DefaultL3Address, contract.EnvHandoffDir: handoff,
	}
	if !reflect.DeepEqual(claim.Job.Spec.Execution.Env, wantPublic) {
		t.Fatalf("public env = %#v, want %#v", claim.Job.Spec.Execution.Env, wantPublic)
	}
	if len(claim.Job.Spec.Execution.SensitiveEnv) != 1 || claim.Job.Spec.Execution.SensitiveEnv[contract.EnvRunToken] == "" {
		t.Fatalf("sensitive env = %#v, want only %s", claim.Job.Spec.Execution.SensitiveEnv, contract.EnvRunToken)
	}
	token := claim.Job.Spec.Execution.SensitiveEnv[contract.EnvRunToken]
	if claim.Job.Spec.Execution.HandoffDirectory != handoff {
		t.Fatalf("handoff directory = %q, want %q", claim.Job.Spec.Execution.HandoffDirectory, handoff)
	}

	var storedHash []byte
	var attemptID string
	if err := h.l3Store.db.QueryRow(`SELECT attempt_id, token_hash FROM run_tokens WHERE run_id=?`, accepted.RunID).Scan(&attemptID, &storedHash); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte(token))
	if attemptID == "" || !bytes.Equal(storedHash, digest[:]) || bytes.Contains(storedHash, []byte(token)) {
		t.Fatalf("stored token binding/hash = %q/%x", attemptID, storedHash)
	}
	var staged *string
	if err := h.l3Store.db.QueryRow(`SELECT token_delivery FROM dispatch_outbox WHERE run_id=?`, accepted.RunID).Scan(&staged); err != nil {
		t.Fatal(err)
	}
	if staged != nil {
		t.Fatalf("token delivery remained staged after dispatch: %q", *staged)
	}
}

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
	assertAPIError(t, status, body, http.StatusForbidden, contract.ErrorForbidden)
}

func TestTerminalRunTokenGraceAndExpiry(t *testing.T) {
	now := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	clock := &mutableClock{now: now}
	store, err := OpenStore(filepath.Join(t.TempDir(), "tokens.sqlite"), StoreOptions{Clock: clock, RunTokenGrace: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	request := inlineRunRequest("#!/bin/sh\nexit 0\n")
	record, _, err := store.CreateRun(context.Background(), CreateRunInput{IdempotencyKey: "grace", Actor: "test", Request: request})
	if err != nil {
		t.Fatal(err)
	}
	token, err := store.ensureRunToken(context.Background(), record.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.beginDispatch(context.Background(), record.RunID); err != nil {
		t.Fatal(err)
	}
	if err := store.projectJobState(context.Background(), projectedRun{RunID: record.RunID, State: contract.RunDispatching}, contract.JobSucceeded); err != nil {
		t.Fatal(err)
	}
	scope, err := store.AuthenticateRunToken(context.Background(), token)
	if err != nil || scope.RunID != record.RunID || scope.AttemptID == "" {
		t.Fatalf("authenticate during grace = (%#v, %v)", scope, err)
	}
	clock.now = now.Add(time.Minute - time.Nanosecond)
	if _, err := store.AuthenticateRunToken(context.Background(), token); err != nil {
		t.Fatalf("authenticate before grace expiry: %v", err)
	}
	clock.now = now.Add(time.Minute)
	if _, err := store.AuthenticateRunToken(context.Background(), token); err == nil {
		t.Fatal("authenticate at expiry succeeded, want unauthorized")
	} else {
		var protocolErr *Error
		if !errors.As(err, &protocolErr) || protocolErr.Code != contract.ErrorUnauthorized {
			t.Fatalf("authenticate at expiry = %v, want unauthorized", err)
		}
	}
}

type mutableClock struct{ now time.Time }

func (c *mutableClock) Now() time.Time { return c.now }

func claimRunTokens(t *testing.T, h *integrationHarness, count int) map[string]string {
	t.Helper()
	agent := h.agent()
	tokens := make(map[string]string, count)
	for range count {
		status, _, body := h.do(agent, http.MethodPost, "/v1/agent/jobs/claim", l1.ClaimRequest{NodeID: "node-1", BootSessionID: "boot-1"}, nil)
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
	want := []string{"WEFTY_HANDOFF_DIR", "WEFTY_L1_ENDPOINT", "WEFTY_L3_ENDPOINT", "WEFTY_RUN_ID", "WEFTY_RUN_TOKEN"}
	got := []string{contract.EnvHandoffDir, contract.EnvL1Endpoint, contract.EnvL3Endpoint, contract.EnvRunID, contract.EnvRunToken}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("run environment names = %q, want %q", got, want)
	}
}
