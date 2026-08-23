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

func TestClaimTimeAuthorityAndDurableOperatorIntent(t *testing.T) {
	assertClaimTimeAuthorityAndIntent(t)
}

func assertClaimTimeAuthorityAndIntent(t *testing.T) {
	t.Helper()

	t.Run("registration preserves every intent field", func(t *testing.T) {
		h := newIntegrationHarness(t, map[string][]string{"stable-node": {"linux"}})
		agent := h.client(fabric.Identity{NodeID: "fabric-node", Tags: []string{DefaultAgentPrincipalTag}})
		first := h.register(agent, "stable-node")
		if first.AuthorityGeneration != 1 || !first.ClaimsEnabled {
			t.Fatalf("first registration = %#v, want generation 1 with claims enabled", first)
		}

		intentTime := h.clock.Now().Add(-time.Hour).UnixNano()
		if _, err := h.store.db.Exec(`UPDATE nodes SET claims_enabled=0, intent_revision=7,
			intent_reason='operator freeze', intent_updated_at=?, intent_actor='operator-1'
			WHERE node_id='stable-node'`, intentTime); err != nil {
			t.Fatal(err)
		}
		registration := first.NodeRegistration
		registration.BootSessionID = "boot-2"
		status, _, body := h.do(agent, http.MethodPost, "/v1/agent/nodes/register", registration)
		if status != http.StatusOK {
			t.Fatalf("replacement registration status = %d body=%s", status, body)
		}
		var replaced Node
		if err := json.Unmarshal(body, &replaced); err != nil {
			t.Fatal(err)
		}
		if replaced.AuthorityGeneration != 2 || replaced.ClaimsEnabled || replaced.IntentRevision != 7 ||
			replaced.IntentReason != "operator freeze" || replaced.IntentActor != "operator-1" ||
			replaced.IntentUpdatedAt == nil || replaced.IntentUpdatedAt.UnixNano() != intentTime {
			t.Fatalf("replacement registration clobbered intent: %#v", replaced)
		}
	})

	t.Run("replacement boot is embargoed and old authority is fenced", func(t *testing.T) {
		h := newIntegrationHarness(t, map[string][]string{"stable-node": {"linux"}})
		client := h.client(fabric.Identity{NodeID: "operator", Tags: []string{DefaultClientPrincipalTag}})
		agent := h.client(fabric.Identity{NodeID: "fabric-node", Tags: []string{DefaultAgentPrincipalTag}})
		intruder := h.client(fabric.Identity{NodeID: "fabric-intruder", Tags: []string{DefaultAgentPrincipalTag}})

		registration := contract.NodeRegistration{
			NodeID: "stable-node", BootSessionID: "boot-1", OS: "linux", Architecture: "arm64", AgentVersion: "test",
			Capabilities:       map[string]bool{"kind:process": true},
			CapabilityRevision: 1, CapabilityObservedAt: h.clock.Now(), MissingCapabilities: []string{},
		}
		status, _, body := h.do(agent, http.MethodPost, "/v1/agent/nodes/register", registration)
		if status != http.StatusOK {
			t.Fatalf("first registration status = %d body=%s", status, body)
		}
		firstJob := h.submit(client, "authority-first", []string{"linux"})
		status, _, body = h.do(agent, http.MethodPost, "/v1/agent/jobs/claim", ClaimRequest{NodeID: "stable-node", BootSessionID: "boot-1", Class: contract.JobClassOneShot})
		if status != http.StatusOK {
			t.Fatalf("first claim status = %d body=%s", status, body)
		}
		var firstClaim Claim
		if err := json.Unmarshal(body, &firstClaim); err != nil {
			t.Fatal(err)
		}

		registration.BootSessionID = "boot-2"
		status, _, body = h.do(agent, http.MethodPost, "/v1/agent/nodes/register", registration)
		if status != http.StatusOK {
			t.Fatalf("replacement registration status = %d body=%s", status, body)
		}
		h.submit(client, "authority-second", []string{"linux"})
		status, _, body = h.do(agent, http.MethodPost, "/v1/agent/jobs/claim", ClaimRequest{NodeID: "stable-node", BootSessionID: "boot-2", Class: contract.JobClassOneShot})
		if status != http.StatusNoContent {
			t.Fatalf("embargoed claim status = %d, want 204 body=%s", status, body)
		}

		renewPath := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/lease", firstJob.JobID, firstClaim.Lease.AttemptID)
		status, _, body = h.do(agent, http.MethodPost, renewPath, RenewalRequest{FencingToken: firstClaim.Lease.FencingToken})
		assertAPIError(t, status, body, http.StatusConflict, contract.ErrorNodeSessionReplaced)
		zero := 0
		completePath := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/complete", firstJob.JobID, firstClaim.Lease.AttemptID)
		status, _, body = h.do(agent, http.MethodPost, completePath, CompletionRequest{
			FencingToken: firstClaim.Lease.FencingToken, IdempotencyKey: "old-completion", Result: ProcessResult{ExitCode: &zero},
		})
		assertAPIError(t, status, body, http.StatusConflict, contract.ErrorNodeSessionReplaced)

		logPath := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/logs", firstJob.JobID, firstClaim.Lease.AttemptID)
		status, _, body = h.do(agent, http.MethodPost, logPath, AppendLogsRequest{
			FencingToken: firstClaim.Lease.FencingToken,
			Events: []contract.LogEvent{{
				AttemptID: firstClaim.Lease.AttemptID, Stream: contract.LogStdout, Sequence: 0,
				Timestamp: h.clock.Now(), Bytes: []byte("late from replaced boot\n"),
			}},
		})
		if status != http.StatusOK {
			t.Fatalf("evidence append status = %d body=%s", status, body)
		}
		var attemptState contract.AttemptState
		var jobState contract.JobState
		if err := h.store.db.QueryRow("SELECT state FROM attempts WHERE attempt_id=?", firstClaim.Lease.AttemptID).Scan(&attemptState); err != nil {
			t.Fatal(err)
		}
		if err := h.store.db.QueryRow("SELECT state FROM jobs WHERE job_id=?", firstJob.JobID).Scan(&jobState); err != nil {
			t.Fatal(err)
		}
		if attemptState != contract.AttemptClaimed || jobState != contract.JobClaimed {
			t.Fatalf("evidence changed authority state to attempt/job %q/%q", attemptState, jobState)
		}

		registration.BootSessionID = "boot-intruder"
		status, _, body = h.do(intruder, http.MethodPost, "/v1/agent/nodes/register", registration)
		assertAPIError(t, status, body, http.StatusForbidden, contract.ErrorIdentityBound)

		h.clock.Advance(DefaultLeaseDuration)
		if _, err := h.store.Reconcile(t.Context()); err != nil {
			t.Fatal(err)
		}
		status, _, body = h.do(agent, http.MethodPost, "/v1/agent/jobs/claim", ClaimRequest{NodeID: "stable-node", BootSessionID: "boot-2", Class: contract.JobClassOneShot})
		if status != http.StatusOK {
			t.Fatalf("post-expiry claim status = %d body=%s", status, body)
		}
	})

	t.Run("intent defaults, CAS, and liveness independence", func(t *testing.T) {
		h := newIntegrationHarness(t, map[string][]string{"expected-node": {"linux"}, "live-node": {"linux"}})
		operator := h.client(fabric.Identity{NodeID: "operator", Tags: []string{DefaultClientPrincipalTag}})
		expectedAgent := h.client(fabric.Identity{NodeID: "fabric-expected", Tags: []string{DefaultAgentPrincipalTag}})
		unexpectedAgent := h.client(fabric.Identity{NodeID: "fabric-unexpected", Tags: []string{DefaultAgentPrincipalTag}})

		expected := h.register(expectedAgent, "expected-node")
		unexpected := h.register(unexpectedAgent, "unexpected-node")
		if !expected.ClaimsEnabled || unexpected.ClaimsEnabled {
			t.Fatalf("expected/unexpected claims defaults = %t/%t, want true/false", expected.ClaimsEnabled, unexpected.ClaimsEnabled)
		}
		h.submit(operator, "unexpected-disabled", []string{"unexpected-only"})
		status, _, body := h.do(unexpectedAgent, http.MethodPost, "/v1/agent/jobs/claim", ClaimRequest{
			NodeID: "unexpected-node", BootSessionID: "boot-unexpected-node", Class: contract.JobClassOneShot,
		})
		assertAPIError(t, status, body, http.StatusConflict, contract.ErrorNodeDraining)

		h.clock.Advance(DefaultNodeDeadAfter)
		if _, err := h.store.Reconcile(t.Context()); err != nil {
			t.Fatal(err)
		}
		status, _, body = h.do(operator, http.MethodPost, "/v1/nodes/expected-node/claims", NodeIntentRequest{
			ClaimsEnabled: false, IntentRevision: 0, Reason: "forbid work while offline",
		})
		if status != http.StatusOK {
			t.Fatalf("dead-node intent status = %d body=%s", status, body)
		}
		var disabled Node
		if err := json.Unmarshal(body, &disabled); err != nil {
			t.Fatal(err)
		}
		if disabled.State != contract.NodeDead || disabled.ClaimsEnabled || disabled.IntentRevision != 1 || disabled.IntentActor != "operator" {
			t.Fatalf("dead-node intent = %#v", disabled)
		}
		registration := expected.NodeRegistration
		registration.BootSessionID = "boot-rejoined"
		status, _, body = h.do(expectedAgent, http.MethodPost, "/v1/agent/nodes/register", registration)
		if status != http.StatusOK {
			t.Fatalf("disabled-node re-registration status = %d body=%s", status, body)
		}
		status, _, body = h.do(expectedAgent, http.MethodPost, "/v1/agent/jobs/claim", ClaimRequest{NodeID: "expected-node", BootSessionID: "boot-rejoined", Class: contract.JobClassOneShot})
		assertAPIError(t, status, body, http.StatusConflict, contract.ErrorNodeDraining)

		liveAgent := h.client(fabric.Identity{NodeID: "fabric-live", Tags: []string{DefaultAgentPrincipalTag}})
		live := h.register(liveAgent, "live-node")
		liveJob := h.submit(operator, "intent-does-not-fence", []string{"linux"})
		status, _, body = h.do(liveAgent, http.MethodPost, "/v1/agent/jobs/claim", ClaimRequest{NodeID: "live-node", BootSessionID: live.BootSessionID, Class: contract.JobClassOneShot})
		if status != http.StatusOK {
			t.Fatalf("live claim status = %d body=%s", status, body)
		}
		var liveClaim Claim
		if err := json.Unmarshal(body, &liveClaim); err != nil {
			t.Fatal(err)
		}
		status, _, body = h.do(operator, http.MethodPost, "/v1/nodes/live-node/claims", NodeIntentRequest{
			ClaimsEnabled: false, IntentRevision: 0, Reason: "finish current work only",
		})
		if status != http.StatusOK {
			t.Fatalf("live-node intent status = %d body=%s", status, body)
		}
		status, _, body = h.do(operator, http.MethodPost, "/v1/nodes/live-node/claims", NodeIntentRequest{
			ClaimsEnabled: true, IntentRevision: 0, Reason: "stale operator view",
		})
		assertAPIError(t, status, body, http.StatusConflict, contract.ErrorConflict)
		renewPath := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/lease", liveJob.JobID, liveClaim.Lease.AttemptID)
		status, _, body = h.do(liveAgent, http.MethodPost, renewPath, RenewalRequest{FencingToken: liveClaim.Lease.FencingToken})
		if status != http.StatusOK {
			t.Fatalf("renew after intent change status = %d body=%s", status, body)
		}
		zero := 0
		completePath := fmt.Sprintf("/v1/agent/jobs/%s/attempts/%s/complete", liveJob.JobID, liveClaim.Lease.AttemptID)
		status, _, body = h.do(liveAgent, http.MethodPost, completePath, CompletionRequest{
			FencingToken: liveClaim.Lease.FencingToken, IdempotencyKey: "complete-after-intent", Result: ProcessResult{ExitCode: &zero},
		})
		if status != http.StatusOK {
			t.Fatalf("complete after intent change status = %d body=%s", status, body)
		}
	})
}
