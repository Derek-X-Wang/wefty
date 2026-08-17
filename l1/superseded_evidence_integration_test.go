package l1

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

func TestSupersededAttemptStillAcceptsEvidenceByOriginalProvenance(t *testing.T) {
	assertSupersededAttemptEvidenceProvenance(t)
}

func assertSupersededAttemptEvidenceProvenance(t *testing.T) {
	t.Helper()
	h := newIntegrationHarness(t, map[string][]string{"node-1": {"linux"}})
	client := h.client(fabric.Identity{NodeID: "caller", Tags: []string{DefaultClientPrincipalTag}})
	agent := h.client(fabric.Identity{NodeID: "fabric-node-1", Tags: []string{DefaultAgentPrincipalTag}})
	registration := h.register(agent, "node-1").NodeRegistration
	job := h.submit(client, "superseded-evidence", []string{"linux"})
	claim := claimJob(t, h, agent, "node-1")
	h.clock.Advance(DefaultLeaseDuration)
	if _, err := h.store.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}

	registration.BootSessionID = "boot-successor"
	status, _, body := h.do(agent, http.MethodPost, "/v1/agent/nodes/register", registration)
	if status != http.StatusOK {
		t.Fatalf("successor registration status = %d body=%s", status, body)
	}
	const successorAttempt = "attempt-successor"
	now := h.clock.Now()
	if _, err := h.store.db.Exec(`INSERT INTO attempts(
		attempt_id, job_id, node_id, boot_session_id, state, fencing_token, lease_expires_ns,
		authority_generation, created_ns, updated_ns) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		successorAttempt, job.JobID, "node-1", registration.BootSessionID, contract.AttemptClaimed,
		"successor-fence", now.Add(DefaultLeaseDuration).UnixNano(), 2, now.UnixNano(), now.UnixNano()); err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.db.Exec(`UPDATE jobs SET current_attempt_id=? WHERE job_id=?`, successorAttempt, job.JobID); err != nil {
		t.Fatal(err)
	}

	logPath := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/logs", job.JobID, claim.Lease.AttemptID)
	status, _, body = h.do(agent, http.MethodPost, logPath, AppendLogsRequest{
		FencingToken: claim.Lease.FencingToken,
		Events: []contract.LogEvent{logEvent(
			claim.Lease.AttemptID, contract.LogStdout, 0, []byte("evidence-from-superseded-attempt\n"),
		)},
	})
	if status != http.StatusOK {
		t.Fatalf("superseded-attempt log status = %d body=%s", status, body)
	}

	exitCode := 0
	completionPath := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/complete", job.JobID, claim.Lease.AttemptID)
	status, _, body = h.do(agent, http.MethodPost, completionPath, CompletionRequest{
		FencingToken:   claim.Lease.FencingToken,
		IdempotencyKey: "superseded-late-completion",
		Result:         ProcessResult{ExitCode: &exitCode},
	})
	assertAPIError(t, status, body, http.StatusConflict, contract.ErrorLeaseExpired)

	var jobState contract.JobState
	var currentAttempt string
	var oldAttemptState contract.AttemptState
	var nodeGeneration int64
	var lateResultJSON []byte
	if err := h.store.db.QueryRow(`SELECT j.state, j.current_attempt_id, old.state, n.authority_generation,
		old.late_result_json FROM jobs j JOIN attempts old ON old.attempt_id=?
		JOIN nodes n ON n.node_id=old.node_id WHERE j.job_id=?`, claim.Lease.AttemptID, job.JobID).
		Scan(&jobState, &currentAttempt, &oldAttemptState, &nodeGeneration, &lateResultJSON); err != nil {
		t.Fatal(err)
	}
	if jobState != contract.JobFailed || currentAttempt != successorAttempt || oldAttemptState != contract.AttemptLost || nodeGeneration != 2 {
		t.Fatalf("evidence changed superseded authority: job=%q current=%q old=%q generation=%d",
			jobState, currentAttempt, oldAttemptState, nodeGeneration)
	}
	var lateResult LateResultEvidence
	if err := json.Unmarshal(lateResultJSON, &lateResult); err != nil {
		t.Fatal(err)
	}
	if lateResult.Kind != LateResultObservation || lateResult.Result == nil ||
		lateResult.Result.ExitCode == nil || *lateResult.Result.ExitCode != 0 {
		t.Fatalf("superseded late result = %#v", lateResult)
	}
}
