package l1

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

func TestBootTakeoverFencesAuthorityWritesButRetainsEvidence(t *testing.T) {
	assertBootTakeoverFencesAuthorityWritesButRetainsEvidence(t)
}

func assertBootTakeoverFencesAuthorityWritesButRetainsEvidence(t *testing.T) {
	h := newIntegrationHarness(t, map[string][]string{"node-1": nil})
	agent := h.client(fabric.Identity{NodeID: "fabric-node", Tags: []string{DefaultAgentPrincipalTag}})
	client := h.client(fabric.Identity{NodeID: "client", Tags: []string{DefaultClientPrincipalTag}})
	h.register(agent, "node-1")
	job := h.submit(client, "boot-takeover-fencing", nil)
	status, _, body := h.do(agent, http.MethodPost, "/v1/agent/jobs/claim", ClaimRequest{
		NodeID: "node-1", BootSessionID: "boot-node-1",
	})
	if status != http.StatusOK {
		t.Fatalf("claim status = %d body=%s", status, body)
	}
	var claim Claim
	if err := json.Unmarshal(body, &claim); err != nil {
		t.Fatal(err)
	}

	replacement := contract.NodeRegistration{
		NodeID: "node-1", BootSessionID: "boot-new", OS: "linux", Architecture: "arm64", AgentVersion: "test",
	}
	status, _, body = h.do(agent, http.MethodPost, "/v1/agent/nodes/register", replacement)
	if status != http.StatusOK {
		t.Fatalf("replacement registration status = %d body=%s", status, body)
	}

	attemptPath := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s", job.JobID, claim.Lease.AttemptID)
	status, _, body = h.do(agent, http.MethodPost, attemptPath+"/lease", RenewalRequest{FencingToken: claim.Lease.FencingToken})
	assertAPIError(t, status, body, http.StatusConflict, contract.ErrorNodeSessionReplaced)

	event := contract.LogEvent{
		AttemptID: claim.Lease.AttemptID, Stream: contract.LogStdout,
		Sequence: 0, Timestamp: time.Now().UTC(), Bytes: []byte("retained after takeover\n"),
	}
	status, _, body = h.do(agent, http.MethodPost, attemptPath+"/logs", AppendLogsRequest{
		FencingToken: claim.Lease.FencingToken, Events: []contract.LogEvent{event},
	})
	if status != http.StatusOK {
		t.Fatalf("evidence append after takeover status = %d body=%s", status, body)
	}
	afterLog, err := h.store.GetJob(t.Context(), job.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if afterLog.State != contract.JobClaimed {
		t.Fatalf("superseded log append promoted job to %q, want claimed", afterLog.State)
	}

	exitCode := 0
	status, _, body = h.do(agent, http.MethodPost, attemptPath+"/complete", CompletionRequest{
		FencingToken: claim.Lease.FencingToken, IdempotencyKey: "boot-takeover-completion",
		Result: ProcessResult{ExitCode: &exitCode},
	})
	assertAPIError(t, status, body, http.StatusConflict, contract.ErrorNodeSessionReplaced)
	afterCompletion, err := h.store.GetJob(t.Context(), job.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if afterCompletion.State != contract.JobClaimed {
		t.Fatalf("superseded completion changed job to %q, want claimed", afterCompletion.State)
	}
	logs, err := h.store.GetJobLogs(t.Context(), job.JobID, "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(logs.Events) != 1 || string(logs.Events[0].Bytes) != string(event.Bytes) {
		t.Fatalf("retained evidence = %#v, want takeover event", logs.Events)
	}
}
