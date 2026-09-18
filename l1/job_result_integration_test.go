package l1

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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
// of mirroring the log route: the write authority is not a new one. Every
// refusal asserts its code, so a route that refused everything for the wrong
// reason would not pass.
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

	refusals := []struct {
		name    string
		request AttemptResultRequest
		status  int
		code    contract.ErrorCode
	}{
		{"no fence", AttemptResultRequest{Document: []byte(`{}`)},
			http.StatusBadRequest, contract.ErrorInvalidRequest},
		{"nothing at all", AttemptResultRequest{FencingToken: claim.Lease.FencingToken},
			http.StatusBadRequest, contract.ErrorInvalidRequest},
		{"both arms", AttemptResultRequest{FencingToken: claim.Lease.FencingToken, Document: []byte(`{}`),
			SkipReason: contract.ResultUploadSkipOversize}, http.StatusBadRequest, contract.ErrorInvalidRequest},
		{"unknown reason", AttemptResultRequest{FencingToken: claim.Lease.FencingToken,
			SkipReason: contract.ResultUploadSkipReason("because")}, http.StatusBadRequest, contract.ErrorInvalidRequest},
		{"wrong digest", AttemptResultRequest{FencingToken: claim.Lease.FencingToken, Document: []byte(`{}`),
			SHA256: "0000000000000000000000000000000000000000000000000000000000000000"},
			http.StatusBadRequest, contract.ErrorInvalidRequest},
		// Valid JSON, only the bound is over, so this case tests the bound and
		// nothing else.
		{"oversize document", AttemptResultRequest{FencingToken: claim.Lease.FencingToken,
			Document: validJSONDocumentOfSize(contract.MaxUploadedResultBytes + 1)},
			http.StatusBadRequest, contract.ErrorInvalidRequest},
		// Correct digest, correct size, correct fence: only the shape is
		// wrong, so nothing else can be what refuses it.
		{"malformed document", AttemptResultRequest{FencingToken: claim.Lease.FencingToken,
			Document: []byte("not a json document at all"),
			SHA256:   digestOf([]byte("not a json document at all"))},
			http.StatusBadRequest, contract.ErrorInvalidRequest},
		{"stale fence", AttemptResultRequest{FencingToken: "not-the-fence", Document: []byte(`{}`)},
			http.StatusConflict, contract.ErrorStaleFence},
	}
	for _, refusal := range refusals {
		status, _, body := h.do(agentClient, http.MethodPost, path, refusal.request)
		assertAPIError(t, status, body, refusal.status, refusal.code)
	}

	// Another node's agent holds no authority over this attempt at all.
	status, _, body := h.do(other, http.MethodPost, path,
		AttemptResultRequest{FencingToken: claim.Lease.FencingToken, Document: []byte(`{}`)})
	if status == http.StatusOK {
		t.Fatalf("a foreign node uploaded this attempt's result: %s", body)
	}

	// Nothing above was stored, and the fence that is right still works, so the
	// refusals are about authority and shape rather than a route that refuses
	// everything.
	if _, err := h.store.GetJobResult(context.Background(), job.JobID); err == nil {
		t.Fatal("a refused upload was stored anyway")
	}
	status, _, body = h.do(agentClient, http.MethodPost, path,
		AttemptResultRequest{FencingToken: claim.Lease.FencingToken, Document: []byte(`{"ok":true}`)})
	if status != http.StatusOK {
		t.Fatalf("the attempt's own upload was refused: %d %s", status, body)
	}
}

// TestASupersededAttemptCannotReplaceTheLatestResult is the ordering rule. A
// result is one row, so an upload that arrives late from an attempt a retry has
// already replaced must be refused rather than overwrite the answer the job
// actually has.
func TestASupersededAttemptCannotReplaceTheLatestResult(t *testing.T) {
	h := newIntegrationHarnessWithOptions(t, StoreOptions{
		Jitter: func(delay time.Duration) time.Duration { return delay },
	}, map[string]NodePolicy{"node-1": {Tags: []string{"linux"}, MaxServiceSlots: 1, MaxOneshotSlots: 1}})
	client := h.client(fabric.Identity{NodeID: "caller", Tags: []string{DefaultClientPrincipalTag}})
	agentClient := h.client(fabric.Identity{NodeID: "node-1", Tags: []string{DefaultAgentPrincipalTag}})
	node := h.register(agentClient, "node-1")
	service := submitRestartService(t, h, client, "superseded-result", []string{"linux"}, nil)
	first := restartedServiceAttempt(t, h, agentClient, node, service.JobID, nil)
	second := restartedServiceAttempt(t, h, agentClient, node, service.JobID, &first)

	// B uploads, then A's upload arrives late. A is still a real attempt with
	// a real fence, so only recency can refuse it.
	status, _, body := h.do(agentClient, http.MethodPost, resultPath(service.JobID, second.Lease.AttemptID),
		AttemptResultRequest{FencingToken: second.Lease.FencingToken, Document: []byte(`{"attempt":2}`)})
	if status != http.StatusOK {
		t.Fatalf("latest attempt upload status = %d body=%s", status, body)
	}
	status, _, body = h.do(agentClient, http.MethodPost, resultPath(service.JobID, first.Lease.AttemptID),
		AttemptResultRequest{FencingToken: first.Lease.FencingToken, Document: []byte(`{"attempt":1}`)})
	assertAPIError(t, status, body, http.StatusConflict, contract.ErrorSupersededAttempt)

	stored, err := h.store.GetJobResult(context.Background(), service.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if string(stored.Document) != `{"attempt":2}` || stored.AttemptID != second.Lease.AttemptID {
		t.Fatalf("a superseded attempt replaced the latest result: %#v", stored)
	}
}

// TestASuccessorThatWroteNoResultDisplacesItsPredecessorsDocument is the other
// half of the same rule. The row belongs to the latest attempt even when that
// attempt has nothing to put in it, so a reader is never shown an earlier
// attempt's document as this run's answer.
func TestASuccessorThatWroteNoResultDisplacesItsPredecessorsDocument(t *testing.T) {
	h := newIntegrationHarnessWithOptions(t, StoreOptions{
		Jitter: func(delay time.Duration) time.Duration { return delay },
	}, map[string]NodePolicy{"node-1": {Tags: []string{"linux"}, MaxServiceSlots: 1, MaxOneshotSlots: 1}})
	client := h.client(fabric.Identity{NodeID: "caller", Tags: []string{DefaultClientPrincipalTag}})
	agentClient := h.client(fabric.Identity{NodeID: "node-1", Tags: []string{DefaultAgentPrincipalTag}})
	node := h.register(agentClient, "node-1")
	service := submitRestartService(t, h, client, "absent-successor", []string{"linux"}, nil)
	first := restartedServiceAttempt(t, h, agentClient, node, service.JobID, nil)

	status, _, body := h.do(agentClient, http.MethodPost, resultPath(service.JobID, first.Lease.AttemptID),
		AttemptResultRequest{FencingToken: first.Lease.FencingToken, Document: []byte(`{"attempt":1}`)})
	if status != http.StatusOK {
		t.Fatalf("first upload status = %d body=%s", status, body)
	}

	second := restartedServiceAttempt(t, h, agentClient, node, service.JobID, &first)
	status, _, body = h.do(agentClient, http.MethodPost, resultPath(service.JobID, second.Lease.AttemptID),
		AttemptResultRequest{FencingToken: second.Lease.FencingToken, SkipReason: contract.ResultUploadSkipAbsent})
	if status != http.StatusOK {
		t.Fatalf("absence receipt status = %d body=%s", status, body)
	}

	stored, err := h.store.GetJobResult(context.Background(), service.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if len(stored.Document) != 0 || stored.SkipReason != contract.ResultUploadSkipAbsent ||
		stored.AttemptID != second.Lease.AttemptID {
		t.Fatalf("the predecessor's document survived a successor that wrote none: %#v", stored)
	}
}

// restartedServiceAttempt claims the service job's next attempt, failing the
// previous one first when there is one.
func restartedServiceAttempt(t *testing.T, h *integrationHarness, agentClient *http.Client,
	node Node, jobID string, previous *Claim) Claim {
	t.Helper()
	if previous != nil {
		exitCode := 1
		path := "/v1/agent/jobs/" + jobID + "/attempts/" + previous.Lease.AttemptID + "/complete"
		status, _, body := h.do(agentClient, http.MethodPost, path, CompletionRequest{
			FencingToken: previous.Lease.FencingToken, IdempotencyKey: "restart-" + previous.Lease.AttemptID,
			Result: ProcessResult{ExitCode: &exitCode},
		})
		if status != http.StatusOK {
			t.Fatalf("complete status = %d body=%s", status, body)
		}
		h.clock.Advance(time.Second)
	}
	claim := claimClass(t, h, agentClient, node, contract.JobClassService)
	if claim.Job.JobID != jobID {
		t.Fatalf("claimed job %q, want %q", claim.Job.JobID, jobID)
	}
	if previous != nil && claim.Lease.AttemptID == previous.Lease.AttemptID {
		t.Fatal("the restart reused the previous attempt ID")
	}
	return claim
}

// validJSONDocumentOfSize builds a JSON document of exactly size bytes.
func validJSONDocumentOfSize(size int64) []byte {
	document := make([]byte, 0, size)
	document = append(document, `{"x":"`...)
	for int64(len(document)) < size-2 {
		document = append(document, 'y')
	}
	return append(document, `"}`...)
}

func digestOf(document []byte) string {
	sum := sha256.Sum256(document)
	return hex.EncodeToString(sum[:])
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
