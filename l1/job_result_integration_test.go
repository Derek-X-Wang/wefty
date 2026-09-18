package l1

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

// The result routes are the log routes' sibling, so these tests ask the same
// questions of them: who may write, who may read, and what a retry means.

func resultPath(jobID, attemptID string) string {
	return fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/result", jobID, attemptID)
}

func TestAnAttemptUploadsItsResultAndAnyoneWhoMayReadTheJobMayReadIt(t *testing.T) {
	h := newIntegrationHarness(t, map[string][]string{"node-1": {"linux"}})
	client := h.client(fabric.Identity{NodeID: "caller", Tags: []string{DefaultClientPrincipalTag}})
	agentClient := h.client(fabric.Identity{NodeID: "node-1", Tags: []string{DefaultAgentPrincipalTag}})
	h.register(agentClient, "node-1")
	job := h.submit(client, "dispatch-result", []string{"linux"})
	claim := claimForLogs(t, h, agentClient, job.JobID)

	document := []byte(`{"passed":true}`)
	status, _, body := h.do(agentClient, http.MethodPost, resultPath(job.JobID, claim.Lease.AttemptID),
		AttemptResultRequest{FencingToken: claim.Lease.FencingToken, Document: document})
	if status != http.StatusOK {
		t.Fatalf("upload status = %d body=%s", status, body)
	}
	var receipt AttemptResultResponse
	if err := json.Unmarshal(body, &receipt); err != nil {
		t.Fatal(err)
	}
	if receipt.Bytes != len(document) || receipt.SHA256 == "" {
		t.Fatalf("receipt = %#v", receipt)
	}

	status, _, body = h.do(client, http.MethodGet, "/v1/jobs/"+job.JobID+"/result", nil)
	if status != http.StatusOK {
		t.Fatalf("read status = %d body=%s", status, body)
	}
	var stored JobResult
	if err := json.Unmarshal(body, &stored); err != nil {
		t.Fatal(err)
	}
	if string(stored.Document) != string(document) {
		t.Fatalf("stored document = %q, want %q", stored.Document, document)
	}
	if stored.AttemptID != claim.Lease.AttemptID || stored.SHA256 != receipt.SHA256 {
		t.Fatalf("stored provenance = %#v", stored)
	}
	if stored.UploadedAt.IsZero() {
		t.Fatal("stored result has no upload time")
	}
}

// TestAJobWithNoUploadedResultIsATypedNotFound keeps "this run produced
// nothing" distinguishable from "this run produced an empty document".
func TestAJobWithNoUploadedResultIsATypedNotFound(t *testing.T) {
	h := newIntegrationHarness(t, map[string][]string{"node-1": nil})
	client := h.client(fabric.Identity{NodeID: "caller", Tags: []string{DefaultClientPrincipalTag}})
	agentClient := h.client(fabric.Identity{NodeID: "node-1", Tags: []string{DefaultAgentPrincipalTag}})
	h.register(agentClient, "node-1")
	job := h.submit(client, "dispatch-no-result", nil)

	status, _, body := h.do(client, http.MethodGet, "/v1/jobs/"+job.JobID+"/result", nil)
	if status != http.StatusNotFound {
		t.Fatalf("read status = %d body=%s", status, body)
	}
	var response contract.ErrorResponse
	if err := json.Unmarshal(body, &response); err != nil {
		t.Fatal(err)
	}
	if response.Error.Code != contract.ErrorNotFound {
		t.Fatalf("error = %#v", response.Error)
	}
}

// TestARestartReplacesTheResultRatherThanAppendingOne is the difference between
// a result and a log. The result of a job is what its latest attempt concluded,
// so a second attempt overwrites the first's row rather than adding one.
func TestARestartReplacesTheResultRatherThanAppendingOne(t *testing.T) {
	h := newIntegrationHarnessWithOptions(t, StoreOptions{
		Jitter: func(delay time.Duration) time.Duration { return delay },
	}, map[string]NodePolicy{"node-1": {Tags: []string{"linux"}, MaxServiceSlots: 1, MaxOneshotSlots: 1}})
	client := h.client(fabric.Identity{NodeID: "caller", Tags: []string{DefaultClientPrincipalTag}})
	agentClient := h.client(fabric.Identity{NodeID: "node-1", Tags: []string{DefaultAgentPrincipalTag}})
	node := h.register(agentClient, "node-1")
	service := submitRestartService(t, h, client, "restart-result", []string{"linux"}, nil)
	first := claimClass(t, h, agentClient, node, contract.JobClassService)

	status, _, body := h.do(agentClient, http.MethodPost, resultPath(service.JobID, first.Lease.AttemptID),
		AttemptResultRequest{FencingToken: first.Lease.FencingToken, Document: []byte(`{"attempt":1}`)})
	if status != http.StatusOK {
		t.Fatalf("first upload status = %d body=%s", status, body)
	}

	exitCode := 1
	completionPath := "/v1/agent/jobs/" + service.JobID + "/attempts/" + first.Lease.AttemptID + "/complete"
	status, _, body = h.do(agentClient, http.MethodPost, completionPath, CompletionRequest{
		FencingToken: first.Lease.FencingToken, IdempotencyKey: "restart-result-exit",
		Result: ProcessResult{ExitCode: &exitCode},
	})
	if status != http.StatusOK {
		t.Fatalf("complete status = %d body=%s", status, body)
	}
	h.clock.Advance(time.Second)
	second := claimClass(t, h, agentClient, node, contract.JobClassService)
	if second.Lease.AttemptID == first.Lease.AttemptID {
		t.Fatal("the restart reused the first attempt ID")
	}

	status, _, body = h.do(agentClient, http.MethodPost, resultPath(service.JobID, second.Lease.AttemptID),
		AttemptResultRequest{FencingToken: second.Lease.FencingToken, Document: []byte(`{"attempt":2}`)})
	if status != http.StatusOK {
		t.Fatalf("second upload status = %d body=%s", status, body)
	}

	stored, err := h.store.GetJobResult(context.Background(), service.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if string(stored.Document) != `{"attempt":2}` || stored.AttemptID != second.Lease.AttemptID {
		t.Fatalf("stored result = %#v", stored)
	}
	var rows int
	if err := h.store.db.QueryRow(`SELECT COUNT(*) FROM job_results WHERE job_id=?`, service.JobID).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("job has %d result rows, want exactly one", rows)
	}
}

// TestTheResultRouteRefusesEveryAuthorityTheLogRouteRefuses is the whole point
// of mirroring the log route: the write authority is not a new one.
func TestTheResultRouteRefusesEveryAuthorityTheLogRouteRefuses(t *testing.T) {
	h := newIntegrationHarness(t, map[string][]string{"node-1": nil, "node-2": nil})
	client := h.client(fabric.Identity{NodeID: "caller", Tags: []string{DefaultClientPrincipalTag}})
	agentClient := h.client(fabric.Identity{NodeID: "node-1", Tags: []string{DefaultAgentPrincipalTag}})
	other := h.client(fabric.Identity{NodeID: "node-2", Tags: []string{DefaultAgentPrincipalTag}})
	h.register(agentClient, "node-1")
	h.register(other, "node-2")
	job := h.submit(client, "dispatch-result-authority", nil)
	claim := claimForLogs(t, h, agentClient, job.JobID)
	path := resultPath(job.JobID, claim.Lease.AttemptID)

	for name, request := range map[string]AttemptResultRequest{
		"no fence":       {Document: []byte(`{}`)},
		"wrong fence":    {FencingToken: "not-the-fence", Document: []byte(`{}`)},
		"nothing at all": {FencingToken: claim.Lease.FencingToken},
		"both arms": {FencingToken: claim.Lease.FencingToken, Document: []byte(`{}`),
			SkipReason: contract.ResultUploadSkipOversize},
		"unknown reason": {FencingToken: claim.Lease.FencingToken,
			SkipReason: contract.ResultUploadSkipReason("because")},
		"wrong digest": {FencingToken: claim.Lease.FencingToken, Document: []byte(`{}`),
			SHA256: "0000000000000000000000000000000000000000000000000000000000000000"},
		"oversize document": {FencingToken: claim.Lease.FencingToken,
			Document: append([]byte(`{"x":"`), make([]byte, contract.MaxUploadedResultBytes)...)},
	} {
		status, _, body := h.do(agentClient, http.MethodPost, path, request)
		if status == http.StatusOK {
			t.Fatalf("%s was accepted: %s", name, body)
		}
	}

	// Another node's agent holds no authority over this attempt at all.
	status, _, body := h.do(other, http.MethodPost, path,
		AttemptResultRequest{FencingToken: claim.Lease.FencingToken, Document: []byte(`{}`)})
	if status == http.StatusOK {
		t.Fatalf("a foreign node uploaded this attempt's result: %s", body)
	}

	// And the fence that is right still works, so the refusals above are about
	// authority rather than a route that refuses everything.
	status, _, body = h.do(agentClient, http.MethodPost, path,
		AttemptResultRequest{FencingToken: claim.Lease.FencingToken, Document: []byte(`{"ok":true}`)})
	if status != http.StatusOK {
		t.Fatalf("the attempt's own upload was refused: %d %s", status, body)
	}
}

// TestAResultThatCouldNotTravelSaysSoInsteadOfBeingAbsent keeps "the run wrote
// nothing" and "the result is on the node" different answers.
func TestAResultThatCouldNotTravelSaysSoInsteadOfBeingAbsent(t *testing.T) {
	h := newIntegrationHarness(t, map[string][]string{"node-1": nil})
	client := h.client(fabric.Identity{NodeID: "caller", Tags: []string{DefaultClientPrincipalTag}})
	agentClient := h.client(fabric.Identity{NodeID: "node-1", Tags: []string{DefaultAgentPrincipalTag}})
	h.register(agentClient, "node-1")
	job := h.submit(client, "dispatch-result-skip", nil)
	claim := claimForLogs(t, h, agentClient, job.JobID)

	status, _, body := h.do(agentClient, http.MethodPost, resultPath(job.JobID, claim.Lease.AttemptID),
		AttemptResultRequest{FencingToken: claim.Lease.FencingToken, SkipReason: contract.ResultUploadSkipOversize})
	if status != http.StatusOK {
		t.Fatalf("skip receipt status = %d body=%s", status, body)
	}
	status, _, body = h.do(client, http.MethodGet, "/v1/jobs/"+job.JobID+"/result", nil)
	if status != http.StatusOK {
		t.Fatalf("read status = %d body=%s", status, body)
	}
	var stored JobResult
	if err := json.Unmarshal(body, &stored); err != nil {
		t.Fatal(err)
	}
	if stored.SkipReason != contract.ResultUploadSkipOversize || len(stored.Document) != 0 {
		t.Fatalf("stored skip receipt = %#v", stored)
	}
}

// TestAResultIsRetainedExactlyAsLongAsTheJobsLogsAre pins the retention answer
// to the one already decided for logs rather than inventing a second one.
func TestAResultIsRetainedExactlyAsLongAsTheJobsLogsAre(t *testing.T) {
	h := newIntegrationHarness(t, map[string][]string{"node-1": nil})
	client := h.client(fabric.Identity{NodeID: "caller", Tags: []string{DefaultClientPrincipalTag}})
	agentClient := h.client(fabric.Identity{NodeID: "node-1", Tags: []string{DefaultAgentPrincipalTag}})
	h.register(agentClient, "node-1")
	job := h.submit(client, "dispatch-result-cascade", nil)
	claim := claimForLogs(t, h, agentClient, job.JobID)
	status, _, body := h.do(agentClient, http.MethodPost, resultPath(job.JobID, claim.Lease.AttemptID),
		AttemptResultRequest{FencingToken: claim.Lease.FencingToken, Document: []byte(`{"ok":true}`)})
	if status != http.StatusOK {
		t.Fatalf("upload status = %d body=%s", status, body)
	}

	// Enforcement is the store's own DSN setting, not something this test
	// turns on: if it were off in production the cascade would be a comment
	// rather than a rule.
	if _, err := h.store.db.Exec(`DELETE FROM jobs WHERE job_id=?`, job.JobID); err != nil {
		t.Fatal(err)
	}
	var rows int
	if err := h.store.db.QueryRow(`SELECT COUNT(*) FROM job_results WHERE job_id=?`, job.JobID).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 0 {
		t.Fatalf("the result outlived its job (%d rows)", rows)
	}
}
