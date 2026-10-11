package l3

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/l1"
)

// These are reader characterizations: the stored shapes must keep the same
// public answer during module adoption, including legacy outbox-only links.
func TestRunJobLinkPublicViewsAndComputerPages(t *testing.T) {
	attempt := time.Date(2026, 10, 10, 0, 0, 0, 0, time.UTC)
	regression := &contract.APIError{Code: contract.ErrorNotFound, Message: "original Job is missing", Details: map[string]any{"reason": l1RegressedReason, "l1_job_id": "job-view"}}
	refusal := &contract.APIError{Code: contract.ErrorForbidden, Message: "submission refused", Retryable: false}
	for _, test := range []struct {
		name    string
		fixture linkKnowledgeFixture
		state   linkKnowledgeState
		status  contract.RunState
		jobID   string
	}{
		{name: "unknown", state: linkUnknown, status: contract.RunPending},
		{name: "unknown-terminal", state: linkUnknown, status: contract.RunFailed},
		{name: "attempted", fixture: linkKnowledgeFixture{attemptedAt: &attempt}, state: linkAttemptedUnconfirmed, status: contract.RunDispatching},
		{name: "refused", fixture: linkKnowledgeFixture{attemptedAt: &attempt, diagnostic: refusal}, state: linkAttemptedUnconfirmed, status: contract.RunFailed},
		{name: "linked", fixture: linkKnowledgeFixture{runJob: linkID("job-view"), outboxJob: linkID("job-view"), acknowledged: true, attemptedAt: &attempt}, state: linkLinked, status: contract.RunQueued, jobID: "job-view"},
		{name: "run-reference-only", fixture: linkKnowledgeFixture{runJob: linkID("job-view"), attemptedAt: &attempt}, state: linkLinked, status: contract.RunQueued, jobID: "job-view"},
		{name: "legacy-outbox-only", fixture: linkKnowledgeFixture{outboxJob: linkID("job-view"), acknowledged: true, attemptedAt: &attempt}, state: linkLinked, status: contract.RunSucceeded},
		{name: "missing-settled", fixture: linkKnowledgeFixture{settled: true, attemptedAt: &attempt}, state: linkMissingSettled, status: contract.RunFailed},
		{name: "missing-with-stored-identity", fixture: linkKnowledgeFixture{outboxJob: linkID("job-view"), settled: true, attemptedAt: &attempt}, state: linkMissingSettled, status: contract.RunFailed},
		{name: "regressed-failed", fixture: linkKnowledgeFixture{runJob: linkID("job-view"), outboxJob: linkID("job-view"), diagnostic: regression}, state: linkRegressed, status: contract.RunFailed, jobID: "job-view"},
		{name: "regressed-succeeded", fixture: linkKnowledgeFixture{runJob: linkID("job-view"), outboxJob: linkID("job-view"), diagnostic: regression}, state: linkRegressed, status: contract.RunSucceeded, jobID: "job-view"},
		{name: "legacy-regressed-outbox-only", fixture: linkKnowledgeFixture{outboxJob: linkID("job-view"), diagnostic: regression}, state: linkRegressed, status: contract.RunFailed},
		{name: "legacy-regressed-diagnostic-only", fixture: linkKnowledgeFixture{diagnostic: regression}, state: linkRegressed, status: contract.RunFailed},
	} {
		t.Run(test.name, func(t *testing.T) {
			h := newIntegrationHarness(t)
			proof := ComputerTokenScopeProof{ComputerID: "computer-view", ComputerAttemptID: "attempt-view", ComputerStorageGeneration: 1,
				SubmitIntentRevision: 1, HostNodeID: "host-view", HostStableNodeID: "stable-view", HostBootSessionID: "boot-view", SubmitMaxInflight: 20}
			h.l3Server.computerGrants = &controlledComputerGrantVerifier{proof: proof}
			grant, err := h.l3Store.MintComputerToken(t.Context(), proof)
			if err != nil {
				t.Fatal(err)
			}
			scope, err := h.l3Store.AuthenticateComputerToken(t.Context(), grant.Token)
			if err != nil {
				t.Fatal(err)
			}
			run, _, err := h.l3Store.CreateRun(t.Context(), CreateRunInput{Actor: "computer:" + proof.ComputerID, IdempotencyKey: test.name,
				ComputerScope: &scope, VerifyComputerScope: func(context.Context, ComputerTokenScope) error { return nil }, Request: inlineRunRequest("exit 0\n")})
			if err != nil {
				t.Fatal(err)
			}
			// Outcome is independent of link knowledge. This fixture changes only
			// outcome; all link storage goes through the shared test builder.
			if _, err := h.l3Store.db.Exec(`UPDATE runs SET status=? WHERE run_id=?`, test.status, run.RunID); err != nil {
				t.Fatal(err)
			}
			buildLinkKnowledge(t, h.l3Store, run.RunID, test.fixture)
			knowledge, err := h.l3Store.runJobLink(t.Context(), run.RunID)
			if err != nil || knowledge.state != test.state {
				t.Fatalf("fixture knowledge=%+v err=%v, want %s", knowledge, err, test.state)
			}
			reader := &runViewL1Reader{t: t, jobID: "job-view", missing: test.state == linkRegressed}
			h.l3Server.jobs, h.l3Server.logs, h.l3Server.results = reader, reader, reader
			path := "/v1/runs/" + run.RunID
			code, _, body := h.do(h.caller, http.MethodGet, path, nil, nil)
			var shown contract.RunRecord
			if code != http.StatusOK || json.Unmarshal(body, &shown) != nil || shown.L1JobID != test.jobID || shown.Status != test.status {
				t.Fatalf("show status=%d body=%s", code, body)
			}
			code, _, body = h.do(h.caller, http.MethodGet, path+"/execution", nil, nil)
			var execution RunExecution
			if code != http.StatusOK || json.Unmarshal(body, &execution) != nil || execution.L1JobID != test.jobID ||
				!reflect.DeepEqual(execution.DispatchError, test.fixture.diagnostic) {
				t.Fatalf("execution status=%d body=%s", code, body)
			}
			if (execution.Job != nil) != (test.jobID != "" && !reader.missing) {
				t.Fatalf("execution Job=%+v", execution.Job)
			}
			code, _, body = h.do(h.caller, http.MethodGet, path+"/logs?cursor=reader-cursor", nil, nil)
			if test.jobID == "" {
				var page l1.LogPage
				if code != http.StatusOK || json.Unmarshal(body, &page) != nil || len(page.Events) != 0 || page.NextCursor != "reader-cursor" {
					t.Fatalf("unconfirmed logs status=%d body=%s", code, body)
				}
			} else if reader.missing {
				assertAPIError(t, code, body, http.StatusNotFound, contract.ErrorNotFound)
			} else if code != http.StatusOK {
				t.Fatalf("linked logs status=%d body=%s", code, body)
			}
			code, _, body = h.do(h.caller, http.MethodGet, path+"/result", nil, nil)
			if test.jobID == "" || reader.missing {
				assertAPIError(t, code, body, http.StatusNotFound, contract.ErrorNotFound)
			} else {
				var result RunResult
				if code != http.StatusOK || json.Unmarshal(body, &result) != nil || result.RunID != run.RunID || string(result.Document) != `{"answer":42}` {
					t.Fatalf("linked result status=%d body=%s", code, body)
				}
			}
			wantCalls := 0
			if test.jobID != "" {
				wantCalls = 1
			}
			if reader.jobReads != wantCalls || reader.logReads != wantCalls || reader.resultReads != wantCalls {
				t.Fatalf("L1 reads jobs/logs/results=%d/%d/%d want %d each", reader.jobReads, reader.logReads, reader.resultReads, wantCalls)
			}
			computerClient := h.client(fabric.Identity{NodeID: proof.HostNodeID}, DefaultL3Address)
			for _, descendants := range []bool{false, true} {
				for _, computer := range []bool{false, true} {
					client, origin, token := h.caller, "computer:"+proof.ComputerID, ""
					if computer {
						client, origin, token = computerClient, "computer:self", grant.Token
					}
					code, _, body = doComputerHTTP(t, client, http.MethodGet, fmt.Sprintf("/v1/runs?origin=%s&include_descendants=%t", origin, descendants), token, "", nil)
					var page ComputerRunPage
					if code != http.StatusOK || json.Unmarshal(body, &page) != nil || len(page.Runs) != 1 || !reflect.DeepEqual(page.Runs[0], shown) {
						t.Fatalf("Computer page computer=%t descendants=%t status=%d body=%s, show=%+v", computer, descendants, code, body, shown)
					}
				}
			}
		})
	}
}

// Every successful fake read is bound to the requested Job identity, as L1's
// client is. Unexpected identities fail the test instead of returning a Job.
type runViewL1Reader struct {
	t                               *testing.T
	jobID                           string
	missing                         bool
	jobReads, logReads, resultReads int
}

func (reader *runViewL1Reader) SubmitJob(context.Context, contract.JobSpec) (l1.Job, error) {
	reader.t.Fatal("reader submitted a Job")
	return l1.Job{}, nil
}

func (reader *runViewL1Reader) checkJobID(jobID string) {
	reader.t.Helper()
	if jobID != reader.jobID {
		reader.t.Fatalf("read Job %q, want %q", jobID, reader.jobID)
	}
}

func (reader *runViewL1Reader) GetJob(_ context.Context, jobID string) (l1.Job, error) {
	reader.checkJobID(jobID)
	reader.jobReads++
	if reader.missing {
		return l1.Job{}, &JobNotFoundError{JobID: jobID}
	}
	return l1.Job{JobID: jobID, State: contract.JobQueued}, nil
}

func (reader *runViewL1Reader) GetJobLogs(_ context.Context, jobID, cursor string, _ int) (l1.LogPage, error) {
	reader.checkJobID(jobID)
	reader.logReads++
	if reader.missing {
		return l1.LogPage{}, protocolError(contract.ErrorNotFound, "Job %s was not found", jobID)
	}
	return l1.LogPage{Events: []contract.LogEvent{}, NextCursor: cursor}, nil
}

func (reader *runViewL1Reader) GetJobResult(_ context.Context, jobID string) (l1.JobResult, error) {
	reader.checkJobID(jobID)
	reader.resultReads++
	if reader.missing {
		return l1.JobResult{}, protocolError(contract.ErrorNotFound, "Job %s was not found", jobID)
	}
	return l1.JobResult{JobID: jobID, AttemptID: "attempt-view", Document: []byte(`{"answer":42}`)}, nil
}
