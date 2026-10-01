package l1

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

func TestDispatchKeyLookupReturnsOnlyTheRunLedgersRootOneShot(t *testing.T) {
	h := runLedgerHarness(t, "run-ledger")
	ledger := h.client(fabric.Identity{NodeID: "run-ledger", Tags: []string{DefaultClientPrincipalTag}})
	operator := h.client(fabric.Identity{NodeID: "operator", Tags: []string{DefaultClientPrincipalTag}})
	agent := h.client(fabric.Identity{NodeID: "node-1", Tags: []string{DefaultAgentPrincipalTag}})
	node := h.register(agent, "node-1")

	spec := validJobSpec("ledger-dispatch", []string{"linux"})
	spec.Labels = map[string]string{contract.LabelRunID: "run-1", contract.LabelRunParams: `{"secret":"mailbox-only"}`}
	spec.Execution.SensitiveEnv = map[string]string{contract.EnvRunToken: "wrun_lookup_secret"}
	status, _, body := h.do(ledger, http.MethodPost, "/v1/jobs", spec)
	if status != http.StatusCreated {
		t.Fatalf("submit status = %d body=%s", status, body)
	}
	created := decodeJob(t, body)
	claim := claimOneShot(t, h, agent, node)
	if claim.Job.JobID != created.JobID {
		t.Fatalf("claimed job = %q, want %q", claim.Job.JobID, created.JobID)
	}

	status, _, body = h.do(ledger, http.MethodGet, "/v1/dispatch-keys/ledger-dispatch/job", nil)
	if status != http.StatusOK {
		t.Fatalf("lookup status = %d body=%s", status, body)
	}
	found := decodeJob(t, body)
	if found.JobID != created.JobID || found.Spec.DispatchKey != spec.DispatchKey {
		t.Fatalf("lookup job = %#v, want %q for %q", found, created.JobID, spec.DispatchKey)
	}
	if len(found.Attempts) != 1 || found.Attempts[0].AttemptID != claim.Lease.AttemptID {
		t.Fatalf("lookup attempts = %#v, want claimed attempt %q", found.Attempts, claim.Lease.AttemptID)
	}
	if found.Spec.Execution.SensitiveEnv != nil || found.Spec.Labels[contract.LabelRunParams] != "" || bytes.Contains(body, []byte("wrun_lookup_secret")) || bytes.Contains(body, []byte("mailbox-only")) {
		t.Fatalf("lookup leaked agent-only values: %s", body)
	}

	operatorJob := h.submit(operator, "operator-dispatch", nil)
	var before int
	if err := h.store.db.QueryRow(`SELECT COUNT(*) FROM jobs`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	unknownStatus, _, unknownBody := h.do(ledger, http.MethodGet, "/v1/dispatch-keys/unknown-dispatch/job", nil)
	operatorStatus, _, operatorBody := h.do(ledger, http.MethodGet, "/v1/dispatch-keys/operator-dispatch/job", nil)
	if unknownStatus != http.StatusNotFound || operatorStatus != http.StatusNotFound || !bytes.Equal(unknownBody, operatorBody) {
		t.Fatalf("out-of-scope lookups differ: unknown=%d/%s operator=%d/%s", unknownStatus, unknownBody, operatorStatus, operatorBody)
	}
	var response contract.ErrorResponse
	if err := json.Unmarshal(unknownBody, &response); err != nil {
		t.Fatal(err)
	}
	if response.Error.Code != contract.ErrorNotFound || response.Error.Retryable {
		t.Fatalf("lookup absence = %#v", response.Error)
	}
	var after int
	if err := h.store.db.QueryRow(`SELECT COUNT(*) FROM jobs`).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Fatalf("lookup changed job count from %d to %d", before, after)
	}
	if operatorJob.JobID == "" {
		t.Fatal("operator fixture has no job ID")
	}

	status, _, body = h.do(operator, http.MethodGet, "/v1/dispatch-keys/ledger-dispatch/job", nil)
	if status != http.StatusForbidden {
		t.Fatalf("non-ledger lookup status = %d body=%s", status, body)
	}
	status, _, body = h.do(agent, http.MethodGet, "/v1/dispatch-keys/ledger-dispatch/job", nil)
	if status != http.StatusForbidden {
		t.Fatalf("agent lookup status = %d body=%s", status, body)
	}

	status, _, body = h.do(ledger, http.MethodGet, "/v1/dispatch-keys/"+strings.Repeat("x", 256)+"/job", nil)
	if status != http.StatusBadRequest {
		t.Fatalf("oversized dispatch key status = %d body=%s", status, body)
	}
}
