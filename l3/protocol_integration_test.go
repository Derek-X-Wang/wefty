package l3

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/l1"
)

var protocolTestTime = time.Date(2026, 8, 9, 12, 30, 0, 0, time.UTC)

func TestInvalidEnvelopeStoresRejectionAndFailsRunProtocol(t *testing.T) {
	h := newIntegrationHarness(t)
	request := inlineRunRequest("#!/bin/sh\nexit 0\n")
	request.RequiredEnvelope = true
	request.EnvelopeSchema = json.RawMessage(`{
  "type":"object",
  "required":["extensions"],
  "properties":{"extensions":{"type":"object","required":["com.example.result"],"properties":{"com.example.result":{"const":"ok"}}}}
}`)
	accepted := h.submit(request, "invalid-envelope")
	claim := dispatchAndClaimRun(t, h, accepted.RunID)
	scope, err := h.l3Store.AuthenticateRunToken(context.Background(), claim.Job.Spec.Execution.SensitiveEnv[contract.EnvRunToken])
	if err != nil {
		t.Fatal(err)
	}
	envelope := validEnvelope(accepted.RunID, scope.AttemptID, "invalid-custom-envelope")
	envelope.Extensions = json.RawMessage(`{}`)

	workflow := h.client(fabric.Identity{NodeID: "workflow-node"}, DefaultL3Address)
	status, _, body := h.do(workflow, http.MethodPost, "/v1/runs/"+accepted.RunID+"/envelopes", envelope, runAuthorization(claim))
	assertAPIError(t, status, body, http.StatusBadRequest, contract.ErrorInvalidRequest)

	rejections, err := h.l3Store.ListProtocolRejections(context.Background(), accepted.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if len(rejections) != 1 || rejections[0].Kind != "envelope" || rejections[0].IdempotencyKey != envelope.IdempotencyKey {
		t.Fatalf("stored rejections = %#v", rejections)
	}
	record, err := h.l3Store.GetRun(context.Background(), accepted.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if record.Status != contract.RunFailed || len(record.Envelopes) != 0 || record.FinishedAt == nil {
		t.Fatalf("protocol-failed run = status %q envelopes %d finished %v", record.Status, len(record.Envelopes), record.FinishedAt)
	}
	if _, err := h.l3Store.db.Exec(`UPDATE protocol_rejections SET reason='changed' WHERE rejection_id=?`, rejections[0].RejectionID); err == nil {
		t.Fatal("protocol rejection update succeeded; want append-only storage")
	}
}

func TestExitZeroMissingRequiredEnvelopeFailsBlackBox(t *testing.T) {
	h := newIntegrationHarness(t)
	request := inlineRunRequest("#!/bin/sh\nexit 0\n")
	request.RequiredEnvelope = true
	accepted := h.submit(request, "missing-envelope")
	claim := dispatchAndClaimRun(t, h, accepted.RunID)
	completeClaim(t, h, claim, 0, "complete-missing-envelope")

	reconciler, err := NewReconciler(h.l3Store, h.l1Client, ReconcilerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := reconciler.ReconcileOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.ReconcileOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	record, err := h.l3Store.GetRun(context.Background(), accepted.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if record.Status != contract.RunFailed || record.FinishedAt == nil {
		t.Fatalf("exit-zero missing-envelope run = status %q finished %v", record.Status, record.FinishedAt)
	}
}

func TestEnvelopeAndGateWritesAreAppendOnlyAndIdempotent(t *testing.T) {
	h := newIntegrationHarness(t)
	request := inlineRunRequest("#!/bin/sh\nexit 0\n")
	request.RequiredEnvelope = true
	accepted := h.submit(request, "protocol-idempotency")
	claim := dispatchAndClaimRun(t, h, accepted.RunID)
	token := claim.Job.Spec.Execution.SensitiveEnv[contract.EnvRunToken]
	scope, err := h.l3Store.AuthenticateRunToken(context.Background(), token)
	if err != nil {
		t.Fatal(err)
	}
	workflow := h.client(fabric.Identity{NodeID: "workflow-node"}, DefaultL3Address)
	auth := runAuthorization(claim)
	envelope := validEnvelope(accepted.RunID, scope.AttemptID, "envelope-replay")
	gate := validGate(accepted.RunID, scope.AttemptID, "gate-replay")

	for _, write := range []struct {
		path string
		body any
	}{
		{"/v1/runs/" + accepted.RunID + "/envelopes", envelope},
		{"/v1/runs/" + accepted.RunID + "/gates", gate},
	} {
		status, headers, body := h.do(workflow, http.MethodPost, write.path, write.body, auth)
		if status != http.StatusCreated || headers.Get("Idempotency-Replayed") != "" {
			t.Fatalf("first append %s = %d/%q body=%s", write.path, status, headers.Get("Idempotency-Replayed"), body)
		}
		status, headers, body = h.do(workflow, http.MethodPost, write.path, write.body, auth)
		if status != http.StatusOK || headers.Get("Idempotency-Replayed") != "true" {
			t.Fatalf("replayed append %s = %d/%q body=%s", write.path, status, headers.Get("Idempotency-Replayed"), body)
		}
	}

	changedEnvelope := envelope
	changedEnvelope.Summary = "different body"
	status, _, body := h.do(workflow, http.MethodPost, "/v1/runs/"+accepted.RunID+"/envelopes", changedEnvelope, auth)
	assertAPIError(t, status, body, http.StatusConflict, contract.ErrorIdempotencyConflict)

	record, err := h.l3Store.GetRun(context.Background(), accepted.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if len(record.Envelopes) != 1 || len(record.Gates) != 1 {
		t.Fatalf("stored protocol writes = envelopes %d gates %d", len(record.Envelopes), len(record.Gates))
	}
	if _, err := h.l3Store.db.Exec(`UPDATE envelopes SET body_hash='changed' WHERE envelope_id=?`, envelope.EnvelopeID); err == nil {
		t.Fatal("envelope update succeeded; want append-only storage")
	}
	if _, err := h.l3Store.db.Exec(`DELETE FROM gate_results WHERE gate_id=?`, gate.GateID); err == nil {
		t.Fatal("gate delete succeeded; want append-only storage")
	}

	completeClaim(t, h, claim, 0, "complete-valid-envelope")
	reconciler, err := NewReconciler(h.l3Store, h.l1Client, ReconcilerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := reconciler.ReconcileOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := reconciler.ReconcileOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	record, err = h.l3Store.GetRun(context.Background(), accepted.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if record.Status != contract.RunSucceeded {
		t.Fatalf("valid required-envelope run status = %q, want succeeded", record.Status)
	}
}

func TestProtocolWritesBindOmittedAttemptAndRejectExplicitMismatch(t *testing.T) {
	h := newIntegrationHarness(t)
	accepted := h.submit(inlineRunRequest("#!/bin/sh\nexit 0\n"), "protocol-attempt-binding")
	claim := dispatchAndClaimRun(t, h, accepted.RunID)
	scope, err := h.l3Store.AuthenticateRunToken(context.Background(), claim.Job.Spec.Execution.SensitiveEnv[contract.EnvRunToken])
	if err != nil {
		t.Fatal(err)
	}
	workflow := h.client(fabric.Identity{NodeID: "workflow-node"}, DefaultL3Address)
	auth := runAuthorization(claim)

	envelope := protocolBodyWithoutAttempt(t, validEnvelope(accepted.RunID, scope.AttemptID, "bound-envelope"))
	status, _, body := h.do(workflow, http.MethodPost, "/v1/runs/"+accepted.RunID+"/envelopes", envelope, auth)
	if status != http.StatusCreated {
		t.Fatalf("append envelope without attempt_id = %d body=%s", status, body)
	}
	var storedEnvelope contract.Envelope
	if err := json.Unmarshal(body, &storedEnvelope); err != nil {
		t.Fatal(err)
	}
	if storedEnvelope.AttemptID != scope.AttemptID {
		t.Fatalf("bound envelope attempt_id = %q, want %q", storedEnvelope.AttemptID, scope.AttemptID)
	}

	gate := protocolBodyWithoutAttempt(t, validGate(accepted.RunID, scope.AttemptID, "bound-gate"))
	status, _, body = h.do(workflow, http.MethodPost, "/v1/runs/"+accepted.RunID+"/gates", gate, auth)
	if status != http.StatusCreated {
		t.Fatalf("append gate without attempt_id = %d body=%s", status, body)
	}
	var storedGate contract.GateResult
	if err := json.Unmarshal(body, &storedGate); err != nil {
		t.Fatal(err)
	}
	if storedGate.AttemptID != scope.AttemptID {
		t.Fatalf("bound gate attempt_id = %q, want %q", storedGate.AttemptID, scope.AttemptID)
	}

	mismatch := validEnvelope(accepted.RunID, "attempt-other", "mismatched-envelope")
	status, _, body = h.do(workflow, http.MethodPost, "/v1/runs/"+accepted.RunID+"/envelopes", mismatch, auth)
	assertAPIError(t, status, body, http.StatusConflict, contract.ErrorConflict)
	rejections, err := h.l3Store.ListProtocolRejections(context.Background(), accepted.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if len(rejections) != 0 {
		t.Fatalf("attempt mismatch stored protocol rejections = %#v", rejections)
	}

	for name, supplied := range map[string]any{
		"null": nil, "number": 7, "array": []any{"attempt-other"}, "object": map[string]any{"id": scope.AttemptID},
	} {
		t.Run("non-string "+name, func(t *testing.T) {
			body := protocolBodyWithoutAttempt(t, validEnvelope(accepted.RunID, scope.AttemptID, "non-string-"+name))
			body["attempt_id"] = supplied
			status, _, response := h.do(workflow, http.MethodPost, "/v1/runs/"+accepted.RunID+"/envelopes", body, auth)
			assertAPIError(t, status, response, http.StatusConflict, contract.ErrorConflict)
		})
	}
}

func TestCallerEnvelopeSchemaRejectsRemoteReferencesAtRunCreation(t *testing.T) {
	h := newIntegrationHarness(t)
	request := inlineRunRequest("#!/bin/sh\nexit 0\n")
	request.EnvelopeSchema = json.RawMessage(`{"$ref":"https://example.test/schema.json"}`)
	status, _, body := h.do(h.caller, http.MethodPost, "/v1/runs", request, http.Header{"Idempotency-Key": []string{"remote-envelope-schema"}})
	assertAPIError(t, status, body, http.StatusBadRequest, contract.ErrorInvalidRequest)
}

func TestFailingGateFailsRun(t *testing.T) {
	h := newIntegrationHarness(t)
	accepted := h.submit(inlineRunRequest("#!/bin/sh\nexit 0\n"), "failing-gate")
	claim := dispatchAndClaimRun(t, h, accepted.RunID)
	scope, err := h.l3Store.AuthenticateRunToken(context.Background(), claim.Job.Spec.Execution.SensitiveEnv[contract.EnvRunToken])
	if err != nil {
		t.Fatal(err)
	}
	gate := validGate(accepted.RunID, scope.AttemptID, "failing-gate")
	gate.Outcome = contract.GateFail
	workflow := h.client(fabric.Identity{NodeID: "workflow-node"}, DefaultL3Address)
	status, _, body := h.do(workflow, http.MethodPost, "/v1/runs/"+accepted.RunID+"/gates", gate, runAuthorization(claim))
	if status != http.StatusCreated {
		t.Fatalf("append failing gate = %d body=%s", status, body)
	}
	record, err := h.l3Store.GetRun(context.Background(), accepted.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if record.Status != contract.RunFailed || len(record.Gates) != 1 {
		t.Fatalf("gate-failed run = status %q gates %d", record.Status, len(record.Gates))
	}
}

func TestLineageQueryChainProvenanceAndTerminalReconciliation(t *testing.T) {
	for _, tc := range []struct {
		name          string
		childExitCode int
		wantParent    contract.RunState
	}{
		{"child success", 0, contract.RunSucceeded},
		{"child failure", 23, contract.RunFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newIntegrationHarness(t)
			parent := h.submit(inlineRunRequest("#!/bin/sh\nexit 0\n"), "lineage-parent-"+tc.name)
			parentClaim := dispatchAndClaimRun(t, h, parent.RunID)
			parentToken := parentClaim.Job.Spec.Execution.SensitiveEnv[contract.EnvRunToken]
			childRequest := inlineRunRequest("#!/bin/sh\nexit 0\n")
			childRequest.ParentRunID = parent.RunID
			workflow := h.client(fabric.Identity{NodeID: "workflow-node"}, DefaultL3Address)
			status, _, body := h.do(workflow, http.MethodPost, "/v1/runs", childRequest, http.Header{
				"Authorization":   []string{"Bearer " + parentToken},
				"Idempotency-Key": []string{"lineage-child-" + tc.name},
			})
			if status != http.StatusCreated {
				t.Fatalf("create child = %d body=%s", status, body)
			}
			var child RunAccepted
			if err := json.Unmarshal(body, &child); err != nil {
				t.Fatal(err)
			}
			childClaim := dispatchAndClaimRun(t, h, child.RunID)
			if childClaim.Job.Spec.Labels["parent_run_id"] != parent.RunID {
				t.Fatalf("child L1 labels = %#v, want parent_run_id %q", childClaim.Job.Spec.Labels, parent.RunID)
			}

			status, _, body = h.do(h.caller, http.MethodGet, "/v1/runs/"+parent.RunID+"/lineage", nil, nil)
			if status != http.StatusOK {
				t.Fatalf("lineage query = %d body=%s", status, body)
			}
			var lineage RunLineage
			if err := json.Unmarshal(body, &lineage); err != nil {
				t.Fatal(err)
			}
			if len(lineage.Ancestors) != 0 || len(lineage.Descendants) != 1 || lineage.Descendants[0].RunID != child.RunID || lineage.Descendants[0].Depth != 1 {
				t.Fatalf("parent lineage = %#v", lineage)
			}
			childRecord, err := h.l3Store.GetRun(context.Background(), child.RunID)
			if err != nil {
				t.Fatal(err)
			}
			if childRecord.ParentRunID != parent.RunID || childRecord.Trigger.Type != "chain" || childRecord.Trigger.SourceRunID != parent.RunID || childRecord.Trigger.Principal != "run:"+parent.RunID {
				t.Fatalf("child chain provenance = parent %q trigger %#v", childRecord.ParentRunID, childRecord.Trigger)
			}

			completeClaim(t, h, parentClaim, 0, "complete-parent-"+tc.name)
			reconciler, err := NewReconciler(h.l3Store, h.l1Client, ReconcilerConfig{})
			if err != nil {
				t.Fatal(err)
			}
			if err := reconciler.ReconcileOnce(context.Background()); err != nil {
				t.Fatal(err)
			}
			if tc.childExitCode == 0 {
				if err := reconciler.ReconcileOnce(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			parentRecord, err := h.l3Store.GetRun(context.Background(), parent.RunID)
			if err != nil {
				t.Fatal(err)
			}
			if parentRecord.Status != contract.RunRunning || parentRecord.FinishedAt != nil {
				t.Fatalf("parent before child terminal = status %q finished %v", parentRecord.Status, parentRecord.FinishedAt)
			}

			completeClaim(t, h, childClaim, tc.childExitCode, "complete-child-"+tc.name)
			if err := reconciler.ReconcileOnce(context.Background()); err != nil {
				t.Fatal(err)
			}
			if tc.childExitCode == 0 {
				if err := reconciler.ReconcileOnce(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			parentRecord, err = h.l3Store.GetRun(context.Background(), parent.RunID)
			if err != nil {
				t.Fatal(err)
			}
			if parentRecord.Status != tc.wantParent || parentRecord.FinishedAt == nil {
				t.Fatalf("reconciled parent = status %q finished %v, want %q", parentRecord.Status, parentRecord.FinishedAt, tc.wantParent)
			}
		})
	}
}

func validEnvelope(runID, attemptID, idempotencyKey string) contract.Envelope {
	return contract.Envelope{
		SchemaVersion:  contract.SchemaVersionV1,
		EnvelopeID:     "envelope-" + idempotencyKey,
		IdempotencyKey: idempotencyKey,
		RunID:          runID,
		StepID:         "step-1",
		AttemptID:      attemptID,
		Status:         contract.EnvelopeSucceeded,
		Summary:        "protocol output accepted",
		CreatedAt:      protocolTestTime,
	}
}

func validGate(runID, attemptID, idempotencyKey string) contract.GateResult {
	return contract.GateResult{
		SchemaVersion:  contract.SchemaVersionV1,
		GateID:         "gate-" + idempotencyKey,
		IdempotencyKey: idempotencyKey,
		RunID:          runID,
		StepID:         "step-1",
		AttemptID:      attemptID,
		Name:           "tests",
		Outcome:        contract.GatePass,
		EvaluatedAt:    protocolTestTime,
	}
}

func protocolBodyWithoutAttempt(t *testing.T, value any) map[string]any {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatal(err)
	}
	delete(body, "attempt_id")
	return body
}

func dispatchAndClaimRun(t *testing.T, h *integrationHarness, runID string) l1.Claim {
	t.Helper()
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
		t.Fatalf("claim run %s = %d body=%s", runID, status, body)
	}
	var claim l1.Claim
	if err := json.Unmarshal(body, &claim); err != nil {
		t.Fatal(err)
	}
	if claim.Job.Spec.Labels["run_id"] != runID {
		t.Fatalf("claimed run %q, want %q", claim.Job.Spec.Labels["run_id"], runID)
	}
	return claim
}

func completeClaim(t *testing.T, h *integrationHarness, claim l1.Claim, exitCode int, idempotencyKey string) {
	t.Helper()
	agent := h.agent()
	path := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/complete", claim.Job.JobID, claim.Lease.AttemptID)
	status, _, body := h.do(agent, http.MethodPost, path, l1.CompletionRequest{
		FencingToken:   claim.Lease.FencingToken,
		IdempotencyKey: idempotencyKey,
		Result:         l1.ProcessResult{ExitCode: &exitCode},
	}, nil)
	if status != http.StatusOK {
		t.Fatalf("complete run %s = %d body=%s", claim.Job.Spec.Labels["run_id"], status, body)
	}
}

func runAuthorization(claim l1.Claim) http.Header {
	return http.Header{"Authorization": []string{"Bearer " + claim.Job.Spec.Execution.SensitiveEnv[contract.EnvRunToken]}}
}
