package l1

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

func TestAcceptedCompletionPersistsProcessResult(t *testing.T) {
	assertAcceptedCompletionPersistsProcessResult(t)
}

func assertAcceptedCompletionPersistsProcessResult(t *testing.T) {
	t.Helper()
	h := newIntegrationHarness(t, map[string][]string{"node-1": {"linux"}})
	client := h.client(fabric.Identity{NodeID: "caller", Tags: []string{DefaultClientPrincipalTag}})
	agent := h.client(fabric.Identity{NodeID: "fabric-node-1", Tags: []string{DefaultAgentPrincipalTag}})
	h.register(agent, "node-1")
	job := h.submit(client, "persist-authoritative-result", []string{"linux"})
	claim := claimJob(t, h, agent, "node-1")
	exitCode := 7
	request := CompletionRequest{
		FencingToken:   claim.Lease.FencingToken,
		IdempotencyKey: "authoritative-result",
		Result:         ProcessResult{ExitCode: &exitCode},
	}
	path := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/complete", job.JobID, claim.Lease.AttemptID)
	status, _, body := h.do(agent, http.MethodPost, path, request)
	if status != http.StatusOK {
		t.Fatalf("completion status = %d body=%s", status, body)
	}

	var resultJSON []byte
	var lateResultJSON []byte
	if err := h.store.db.QueryRow(`SELECT result_json, late_result_json FROM attempts WHERE attempt_id=?`,
		claim.Lease.AttemptID).Scan(&resultJSON, &lateResultJSON); err != nil {
		t.Fatal(err)
	}
	var stored ProcessResult
	if err := json.Unmarshal(resultJSON, &stored); err != nil {
		t.Fatalf("decode persisted result %q: %v", resultJSON, err)
	}
	if stored.ExitCode == nil || *stored.ExitCode != exitCode {
		t.Fatalf("persisted result = %#v, want exit code %d", stored, exitCode)
	}
	if lateResultJSON != nil {
		t.Fatalf("accepted completion occupied late_result_json: %s", lateResultJSON)
	}
	assertJobAndAttemptState(t, h.store, job.JobID, claim.Lease.AttemptID, contract.JobFailed, contract.AttemptFailed)
}
