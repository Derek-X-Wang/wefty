package l1

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

// snapshotDatabase renders every row of every table, so a comparison proves a
// request wrote nothing anywhere: state, audit, or bookkeeping.
func snapshotDatabase(t *testing.T, db *sql.DB) string {
	t.Helper()
	rows, err := db.Query(`SELECT name FROM sqlite_master WHERE type='table' ORDER BY name`)
	if err != nil {
		t.Fatal(err)
	}
	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		tables = append(tables, name)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	var snapshot strings.Builder
	for _, table := range tables {
		tableRows, err := db.Query(`SELECT * FROM "` + table + `" ORDER BY rowid`)
		if err != nil {
			// WITHOUT ROWID tables order by their full row instead.
			tableRows, err = db.Query(`SELECT * FROM "` + table + `" ORDER BY 1`)
			if err != nil {
				t.Fatal(err)
			}
		}
		columns, err := tableRows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(&snapshot, "== %s %v\n", table, columns)
		for tableRows.Next() {
			values := make([]any, len(columns))
			pointers := make([]any, len(columns))
			for index := range values {
				pointers[index] = &values[index]
			}
			if err := tableRows.Scan(pointers...); err != nil {
				t.Fatal(err)
			}
			for index, value := range values {
				if raw, ok := value.([]byte); ok {
					values[index] = string(raw)
				}
			}
			fmt.Fprintf(&snapshot, "%v\n", values)
		}
		if err := tableRows.Close(); err != nil {
			t.Fatal(err)
		}
	}
	return snapshot.String()
}

func reregisterNode(t *testing.T, h *integrationHarness, agent *http.Client, nodeID, bootSessionID string) {
	t.Helper()
	status, _, body := h.do(agent, http.MethodPost, "/v1/agent/nodes/register", contract.NodeRegistration{
		NodeID: nodeID, BootSessionID: bootSessionID, RootInstanceID: "root-" + nodeID,
		OS: "linux", Architecture: "arm64", AgentVersion: "test",
		Capabilities: map[string]bool{"kind:process": true}, CapabilityRevision: 1,
		CapabilityObservedAt: h.clock.Now(), MissingCapabilities: []string{},
	})
	if status != http.StatusOK {
		t.Fatalf("re-registration status = %d body=%s", status, body)
	}
}

// #553: L1 accepted a completion, the node's agent saw an ambiguous answer
// (the #548 500) and kept its spooled copy, then the node re-registered, which
// advances authority_generation. Replaying the very same completion was
// answered node_session_replaced forever, so the node could never retire the
// copy. Now the node identity that owned the attempt, holding its fence, gets
// an idempotent already-recorded answer for a byte-identical replay, and that
// answer writes nothing. Anything else is refused exactly as before.
func TestIdenticalCompletionReplayAfterReregistrationIsAlreadyRecorded(t *testing.T) {
	h := newIntegrationHarness(t, map[string][]string{"node-1": {"linux"}, "node-2": {"linux"}})
	client := h.client(fabric.Identity{NodeID: "caller", Tags: []string{DefaultClientPrincipalTag}})
	agent := h.client(fabric.Identity{NodeID: "fabric-node", Tags: []string{DefaultAgentPrincipalTag}})
	otherAgent := h.client(fabric.Identity{NodeID: "other-fabric-node", Tags: []string{DefaultAgentPrincipalTag}})
	h.register(agent, "node-1")
	h.register(otherAgent, "node-2")
	job := h.submit(client, "completion-replay-553", []string{"linux"})
	claim := claimJob(t, h, agent, "node-1")
	exitCode := 1
	accepted := CompletionRequest{
		FencingToken: claim.Lease.FencingToken, IdempotencyKey: "completion:" + claim.Lease.AttemptID,
		Result: ProcessResult{ExitCode: &exitCode},
	}
	path := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/complete", job.JobID, claim.Lease.AttemptID)
	status, headers, body := h.do(agent, http.MethodPost, path, accepted)
	if status != http.StatusOK {
		t.Fatalf("first completion status = %d body=%s", status, body)
	}
	if headers.Get("Idempotent-Replay") != "" {
		t.Fatalf("first completion carried Idempotent-Replay %q", headers.Get("Idempotent-Replay"))
	}
	var original Job
	if err := json.Unmarshal(body, &original); err != nil {
		t.Fatal(err)
	}

	// The node re-registers (a new boot session advances the generation), and
	// time passes well beyond the attempt's lease.
	reregisterNode(t, h, agent, "node-1", "boot-node-1-restarted")
	h.clock.Advance(time.Hour)
	before := snapshotDatabase(t, h.store.db)
	if !strings.Contains(before, claim.Lease.AttemptID) || !strings.Contains(before, "boot-node-1-restarted") {
		t.Fatalf("database snapshot misses the attempt or the new registration:\n%s", before)
	}

	otherExit := 2
	otherQuiescence := RuntimeQuiescenceAttempt
	for _, refused := range []struct {
		name    string
		client  *http.Client
		request CompletionRequest
		status  int
		code    contract.ErrorCode
	}{
		// Any differing field is the idempotency conflict it was before.
		{name: "different_result", client: agent, request: CompletionRequest{
			FencingToken: accepted.FencingToken, IdempotencyKey: accepted.IdempotencyKey,
			Result: ProcessResult{ExitCode: &otherExit},
		}, status: http.StatusConflict, code: contract.ErrorIdempotencyConflict},
		{name: "different_key", client: agent, request: CompletionRequest{
			FencingToken: accepted.FencingToken, IdempotencyKey: "another-key", Result: accepted.Result,
		}, status: http.StatusConflict, code: contract.ErrorIdempotencyConflict},
		{name: "added_quiescence_evidence", client: agent, request: CompletionRequest{
			FencingToken: accepted.FencingToken, IdempotencyKey: accepted.IdempotencyKey, Result: accepted.Result,
			RuntimeQuiescenceEvidence: otherQuiescence,
		}, status: http.StatusConflict, code: contract.ErrorIdempotencyConflict},
		{name: "added_protocol_output_digest", client: agent, request: CompletionRequest{
			FencingToken: accepted.FencingToken, IdempotencyKey: accepted.IdempotencyKey, Result: accepted.Result,
			ProtocolOutputHash: strings.Repeat("a", 64),
		}, status: http.StatusConflict, code: contract.ErrorIdempotencyConflict},
		{name: "added_result_flag", client: agent, request: CompletionRequest{
			FencingToken: accepted.FencingToken, IdempotencyKey: accepted.IdempotencyKey,
			Result: ProcessResult{ExitCode: &exitCode, LogEvidenceIncomplete: true},
		}, status: http.StatusConflict, code: contract.ErrorIdempotencyConflict},
		// A different fence is not this attempt's holder.
		{name: "different_fence", client: agent, request: CompletionRequest{
			FencingToken: "not-the-fence", IdempotencyKey: accepted.IdempotencyKey, Result: accepted.Result,
		}, status: http.StatusConflict, code: contract.ErrorStaleFence},
		// Another node's Fabric identity cannot replay it, even byte-identical.
		{name: "different_node_identity", client: otherAgent, request: accepted,
			status: http.StatusForbidden, code: contract.ErrorAttemptNotOwned},
	} {
		t.Run(refused.name, func(t *testing.T) {
			status, headers, body := h.do(refused.client, http.MethodPost, path, refused.request)
			assertAPIError(t, status, body, refused.status, refused.code)
			if headers.Get("Idempotent-Replay") != "" {
				t.Fatalf("refusal carried Idempotent-Replay %q", headers.Get("Idempotent-Replay"))
			}
		})
	}

	status, headers, body = h.do(agent, http.MethodPost, path, accepted)
	if status != http.StatusOK {
		t.Fatalf("identical replay after re-registration status = %d body=%s", status, body)
	}
	if headers.Get("Idempotent-Replay") != "true" {
		t.Fatalf("identical replay Idempotent-Replay = %q, want true", headers.Get("Idempotent-Replay"))
	}
	var replayed Job
	if err := json.Unmarshal(body, &replayed); err != nil {
		t.Fatal(err)
	}
	if replayed.JobID != original.JobID || replayed.State != original.State || replayed.State != contract.JobFailed ||
		replayed.CurrentAttemptID != claim.Lease.AttemptID || !replayed.UpdatedAt.Equal(original.UpdatedAt) {
		t.Fatalf("replay answered %+v, want the recorded completion %+v", replayed, original)
	}
	if after := snapshotDatabase(t, h.store.db); after != before {
		t.Fatalf("already-recorded replay wrote to L1:\nbefore:\n%s\nafter:\n%s", before, after)
	}
	assertJobAndAttemptState(t, h.store, job.JobID, claim.Lease.AttemptID, contract.JobFailed, contract.AttemptFailed)
}

// A completion L1 has not recorded is still refused from a replaced
// registration, however often it is sent, and the refusal writes nothing.
func TestFirstCompletionFromReplacedRegistrationIsStillRefused(t *testing.T) {
	h := newIntegrationHarness(t, map[string][]string{"node-1": {"linux"}})
	client := h.client(fabric.Identity{NodeID: "caller", Tags: []string{DefaultClientPrincipalTag}})
	agent := h.client(fabric.Identity{NodeID: "fabric-node", Tags: []string{DefaultAgentPrincipalTag}})
	h.register(agent, "node-1")
	job := h.submit(client, "completion-replaced-553", []string{"linux"})
	claim := claimJob(t, h, agent, "node-1")
	reregisterNode(t, h, agent, "node-1", "boot-node-1-restarted")
	before := snapshotDatabase(t, h.store.db)
	exitCode := 0
	request := CompletionRequest{
		FencingToken: claim.Lease.FencingToken, IdempotencyKey: "completion:" + claim.Lease.AttemptID,
		Result: ProcessResult{ExitCode: &exitCode},
	}
	path := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/complete", job.JobID, claim.Lease.AttemptID)
	for range 2 {
		status, headers, body := h.do(agent, http.MethodPost, path, request)
		assertAPIError(t, status, body, http.StatusConflict, contract.ErrorNodeSessionReplaced)
		if headers.Get("Idempotent-Replay") != "" {
			t.Fatalf("refusal carried Idempotent-Replay %q", headers.Get("Idempotent-Replay"))
		}
	}
	if after := snapshotDatabase(t, h.store.db); after != before {
		t.Fatalf("refused completion wrote to L1:\nbefore:\n%s\nafter:\n%s", before, after)
	}
	assertJobAndAttemptState(t, h.store, job.JobID, claim.Lease.AttemptID, contract.JobClaimed, contract.AttemptClaimed)
}
