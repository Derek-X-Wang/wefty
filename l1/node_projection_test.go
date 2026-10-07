package l1

import (
	"encoding/json"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

// Decode the public wire shape so the red check also runs against the original
// Go types, where the additive projection fields and detail route do not exist.
type nodeOperatorFacts struct {
	NodeID         string `json:"node_id"`
	IntentRevision int64  `json:"intent_revision"`
	ActiveAttempts []struct {
		JobID          string                `json:"job_id"`
		AttemptID      string                `json:"attempt_id"`
		BootSessionID  string                `json:"boot_session_id"`
		Kind           string                `json:"kind"`
		Class          string                `json:"class"`
		State          contract.AttemptState `json:"state"`
		LeaseExpiresAt time.Time             `json:"lease_expires_at"`
	} `json:"active_attempts"`
	LastCondition *struct {
		Code    string         `json:"code"`
		Scope   string         `json:"scope"`
		Since   time.Time      `json:"since"`
		Details map[string]any `json:"details"`
	} `json:"last_condition"`
	AllowedActions []struct {
		Verb           string             `json:"verb"`
		Requires       map[string]any     `json:"requires"`
		RefusedBecause *contract.APIError `json:"refused_because"`
	} `json:"allowed_actions"`
}

func TestNodeAllowedActionsMatchOperatorHTTP(t *testing.T) {
	for _, state := range []contract.NodeState{contract.NodeAlive, contract.NodeStale, contract.NodeDraining, contract.NodeDead} {
		for _, enabled := range []bool{false, true} {
			for _, verb := range []string{"drain", "set-claims"} {
				t.Run(string(state)+"/"+verb+"/claims="+map[bool]string{false: "false", true: "true"}[enabled], func(t *testing.T) {
					h := newIntegrationHarness(t, map[string][]string{"node": {"linux"}})
					agent := h.client(fabric.Identity{NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag}})
					operator := h.client(fabric.Identity{NodeID: "operator", Tags: []string{DefaultClientPrincipalTag}})
					h.register(agent, "node")
					if _, err := h.store.db.Exec("UPDATE nodes SET state=?, claims_enabled=? WHERE node_id='node'", state, enabled); err != nil {
						t.Fatal(err)
					}
					status, _, body := h.do(operator, http.MethodGet, "/v1/nodes", nil)
					if status != http.StatusOK {
						t.Fatalf("list status=%d body=%s", status, body)
					}
					var list struct {
						Nodes []nodeOperatorFacts `json:"nodes"`
					}
					if err := json.Unmarshal(body, &list); err != nil {
						t.Fatal(err)
					}
					if len(list.Nodes) != 1 || len(list.Nodes[0].AllowedActions) != 2 {
						t.Fatalf("missing documented actions: %s", body)
					}
					var listedAllowed bool
					var revision int64
					found := false
					for _, action := range list.Nodes[0].AllowedActions {
						if action.Verb != verb {
							continue
						}
						found = true
						listedAllowed = action.RefusedBecause == nil
						value, ok := action.Requires["revision"].(float64)
						if !ok || action.Requires["reason"] != true {
							t.Fatalf("missing preconditions: %#v", action)
						}
						revision = int64(value)
					}
					if !found {
						t.Fatalf("missing action %s", verb)
					}
					path := "/v1/nodes/node/drain"
					if verb == "set-claims" {
						path = "/v1/nodes/node/claims"
					}
					status, _, body = h.do(operator, http.MethodPost, path, NodeIntentRequest{IntentRevision: revision, ClaimsEnabled: verb == "set-claims", Reason: "operator decision"})
					if (status == http.StatusOK) != listedAllowed {
						t.Fatalf("listed allowed=%t but handler status=%d body=%s", listedAllowed, status, body)
					}
					// Once the precondition moves, this exact read can no longer authorize a write.
					status, _, body = h.do(operator, http.MethodPost, path, NodeIntentRequest{IntentRevision: revision, Reason: "stale decision"})
					assertAPIError(t, status, body, http.StatusConflict, contract.ErrorStaleIntentRevision)
					var refusal contract.ErrorResponse
					if err := json.Unmarshal(body, &refusal); err != nil {
						t.Fatal(err)
					}
					if refusal.Error.Details["current_revision"] != float64(revision+1) {
						t.Fatalf("missing current revision: %s", body)
					}
				})
			}
		}
	}
}

func TestNodeProjectionReportsAttemptsAndDurableCondition(t *testing.T) {
	h := newIntegrationHarness(t, map[string][]string{"node": {"linux"}})
	agent := h.client(fabric.Identity{NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag}})
	operator := h.client(fabric.Identity{NodeID: "operator", Tags: []string{DefaultClientPrincipalTag}})
	node := h.register(agent, "node")
	job := h.submit(operator, "node-facts", []string{"linux"})
	status, _, body := h.do(agent, http.MethodPost, "/v1/agent/jobs/claim", ClaimRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID, Class: contract.JobClassOneShot})
	if status != http.StatusOK {
		t.Fatalf("claim status=%d body=%s", status, body)
	}
	var claim Claim
	if err := json.Unmarshal(body, &claim); err != nil {
		t.Fatal(err)
	}
	status, _, body = h.do(agent, http.MethodPost, "/v1/agent/jobs/"+job.JobID+"/attempts/"+claim.Lease.AttemptID+"/started", StartedRequest{FencingToken: claim.Lease.FencingToken})
	if status != http.StatusOK {
		t.Fatalf("started status=%d body=%s", status, body)
	}
	service, _, err := h.store.CreateJob(t.Context(), operatorServiceSpec("node-service", []string{"linux"}))
	if err != nil {
		t.Fatal(err)
	}
	serviceClaim, err := h.store.ClaimJob(t.Context(), "agent", node.NodeID, node.BootSessionID, contract.JobClassService)
	if err != nil || serviceClaim == nil {
		t.Fatalf("service claim=%#v err=%v", serviceClaim, err)
	}
	status, _, body = h.do(operator, http.MethodPost, "/v1/nodes/node/drain", NodeIntentRequest{IntentRevision: 0, Reason: "finish resident work"})
	if status != http.StatusOK {
		t.Fatalf("drain status=%d body=%s", status, body)
	}
	var facts nodeOperatorFacts
	if err := json.Unmarshal(body, &facts); err != nil {
		t.Fatal(err)
	}
	if len(facts.ActiveAttempts) != 2 {
		t.Fatalf("missing resident attempts: %s", body)
	}
	foundProcess, foundService := false, false
	for _, attempt := range facts.ActiveAttempts {
		if attempt.BootSessionID != node.BootSessionID || attempt.LeaseExpiresAt.IsZero() || attempt.Kind != "process" {
			t.Fatalf("missing execution facts: %s", body)
		}
		if attempt.JobID == job.JobID {
			foundProcess = attempt.AttemptID == claim.Lease.AttemptID && attempt.State == contract.AttemptRunning && attempt.Class == contract.JobClassOneShot
		}
		if attempt.JobID == service.JobID {
			foundService = attempt.AttemptID == serviceClaim.Lease.AttemptID && attempt.State == contract.AttemptClaimed && attempt.Class == contract.JobClassService
		}
	}
	if !foundProcess || !foundService {
		t.Fatalf("missing class-scoped execution facts: %s", body)
	}
	if strings.Contains(string(body), "fencing_token") || strings.Contains(string(body), "attempt_credential") {
		t.Fatalf("operator projection contains authority: %s", body)
	}

	if facts.LastCondition == nil || facts.LastCondition.Code != "claims_disabled" || facts.LastCondition.Scope != "node_intent" || !facts.LastCondition.Since.Equal(h.clock.Now()) || facts.LastCondition.Details["reason"] != "finish resident work" {
		t.Fatalf("missing durable intent condition: %s", body)
	}
	since := facts.LastCondition.Since
	h.clock.Advance(time.Second)
	if _, err := h.store.HeartbeatNode(t.Context(), "agent", node.NodeID, node.BootSessionID); err != nil {
		t.Fatal(err)
	}
	status, _, body = h.do(operator, http.MethodGet, "/v1/nodes/node", nil)
	if status != http.StatusOK {
		t.Fatalf("detail status=%d body=%s", status, body)
	}
	if err := json.Unmarshal(body, &facts); err != nil {
		t.Fatal(err)
	}
	if facts.LastCondition == nil || facts.LastCondition.Code != "claims_disabled" || !facts.LastCondition.Since.Equal(since) {
		t.Fatalf("ordinary heartbeat erased condition: %s", body)
	}
	registration := node.NodeRegistration
	registration.BootSessionID = "replacement-boot"
	status, _, body = h.do(agent, http.MethodPost, "/v1/agent/nodes/register", registration)
	if status != http.StatusOK {
		t.Fatalf("replacement registration status=%d body=%s", status, body)
	}
	if err := json.Unmarshal(body, &facts); err != nil {
		t.Fatal(err)
	}
	if len(facts.ActiveAttempts) != 2 || facts.IntentRevision != 1 {
		t.Fatalf("replacement hid old attempts or changed intent: %s", body)
	}
	for _, attempt := range facts.ActiveAttempts {
		if attempt.BootSessionID != node.BootSessionID {
			t.Fatalf("old attempt boot rewritten: %s", body)
		}
	}
	h.clock.Advance(DefaultNodeDeadAfter)
	status, _, body = h.do(operator, http.MethodGet, "/v1/nodes/node", nil)
	if status != http.StatusOK {
		t.Fatalf("detail status=%d body=%s", status, body)
	}
	if err := json.Unmarshal(body, &facts); err != nil {
		t.Fatal(err)
	}
	if facts.ActiveAttempts == nil || len(facts.ActiveAttempts) != 0 || facts.LastCondition == nil || facts.LastCondition.Code != "node_dead" || facts.LastCondition.Scope != "node_liveness" {
		t.Fatalf("expired attempt/dead condition: %s", body)
	}
	status, _, body = h.do(operator, http.MethodGet, "/v1/nodes/missing", nil)
	assertAPIError(t, status, body, http.StatusNotFound, contract.ErrorNotFound)
}

func TestNodeStaleRevisionHTTP(t *testing.T) {
	for _, path := range []string{"drain", "claims"} {
		t.Run(path, func(t *testing.T) {
			h := newIntegrationHarness(t, map[string][]string{"node": {"linux"}})
			agent := h.client(fabric.Identity{NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag}})
			operator := h.client(fabric.Identity{NodeID: "operator", Tags: []string{DefaultClientPrincipalTag}})
			h.register(agent, "node")
			if _, err := h.store.db.Exec("UPDATE nodes SET intent_revision=4 WHERE node_id='node'"); err != nil {
				t.Fatal(err)
			}
			status, _, body := h.do(operator, http.MethodPost, "/v1/nodes/node/"+path, NodeIntentRequest{IntentRevision: 3, Reason: "stale write"})
			assertAPIError(t, status, body, http.StatusConflict, contract.ErrorStaleIntentRevision)
			var response contract.ErrorResponse
			if err := json.Unmarshal(body, &response); err != nil {
				t.Fatal(err)
			}
			if response.Error.Details["current_revision"] != float64(4) || response.Error.Details["provided_revision"] != float64(3) {
				t.Fatalf("revision details=%s", body)
			}
			var revision int64
			if err := h.store.db.QueryRow("SELECT intent_revision FROM nodes WHERE node_id='node'").Scan(&revision); err != nil {
				t.Fatal(err)
			}
			if revision != 4 {
				t.Fatalf("stale request changed revision to %d", revision)
			}
		})
	}
}

func TestNodeConditionPersistsAcrossRecoveryAndReopen(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 10, 7, 10, 0, 0, 0, time.UTC)}
	path := filepath.Join(t.TempDir(), "nodes.sqlite")
	store, err := OpenStore(path, StoreOptions{Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	node, err := store.RegisterNode(t.Context(), fabric.Identity{NodeID: "agent"}, contract.NodeRegistration{
		NodeID: "node", BootSessionID: "boot", OS: "linux", Architecture: "arm64", AgentVersion: "test",
		Capabilities: map[string]bool{"kind:process": true}, CapabilityRevision: 1, CapabilityObservedAt: clock.Now(), MissingCapabilities: []string{},
	}, NodePolicy{Tags: []string{"linux"}, MaxOneshotSlots: 1}, true)
	if err != nil {
		t.Fatal(err)
	}
	decode := func(node Node) nodeOperatorFacts {
		t.Helper()
		data, err := json.Marshal(node)
		if err != nil {
			t.Fatal(err)
		}
		var facts nodeOperatorFacts
		if err := json.Unmarshal(data, &facts); err != nil {
			t.Fatal(err)
		}
		return facts
	}
	observation := contract.CapabilityObservation{Capabilities: map[string]bool{"kind:process": true, "kind:oci": false}, Revision: 2, ObservedAt: clock.Now(), MissingCapabilities: []string{"kind:oci"}, ReasonCode: contract.CapabilityReasonHelperHandshakeFailed}
	node, err = store.HeartbeatNodeWithCapabilityObservation(t.Context(), "agent", "node", "boot", observation, NodePolicy{Tags: []string{"linux"}, MaxOneshotSlots: 1})
	if err != nil {
		t.Fatal(err)
	}
	facts := decode(node)
	if facts.LastCondition == nil || facts.LastCondition.Code != string(observation.ReasonCode) || facts.LastCondition.Scope != "node_capability" {
		t.Fatalf("missing withdrawal condition: %#v", facts)
	}
	since := facts.LastCondition.Since
	clock.Advance(time.Second)
	observation.ObservedAt = clock.Now()
	node, err = store.HeartbeatNodeWithCapabilityObservation(t.Context(), "agent", "node", "boot", observation, NodePolicy{Tags: []string{"linux"}, MaxOneshotSlots: 1})
	if err != nil {
		t.Fatal(err)
	}
	if facts = decode(node); facts.LastCondition == nil || !facts.LastCondition.Since.Equal(since) {
		t.Fatalf("replayed observation advanced since: %#v", facts)
	}
	// Capability recovery does not erase the last notable refusal.
	observation.Revision = 3
	observation.ReasonCode = ""
	observation.MissingCapabilities = []string{}
	observation.Capabilities["kind:oci"] = true
	node, err = store.HeartbeatNodeWithCapabilityObservation(t.Context(), "agent", "node", "boot", observation, NodePolicy{Tags: []string{"linux"}, MaxOneshotSlots: 1})
	if err != nil {
		t.Fatal(err)
	}
	if facts = decode(node); facts.LastCondition == nil || facts.LastCondition.Code != string(contract.CapabilityReasonHelperHandshakeFailed) || !facts.LastCondition.Since.Equal(since) {
		t.Fatalf("recovery erased last condition: %#v", facts)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = OpenStore(path, StoreOptions{Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	nodes, err := store.ListNodes(t.Context())
	if err != nil || len(nodes) != 1 {
		t.Fatalf("reopened nodes=%#v err=%v", nodes, err)
	}
	if facts = decode(nodes[0]); facts.LastCondition == nil || !facts.LastCondition.Since.Equal(since) {
		t.Fatalf("reopen lost condition: %#v", facts)
	}
}

func TestNodeConditionUpgradeDoesNotInventHistory(t *testing.T) {
	h := newIntegrationHarness(t, map[string][]string{"node": {"linux"}})
	agent := h.client(fabric.Identity{NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag}})
	operator := h.client(fabric.Identity{NodeID: "operator", Tags: []string{DefaultClientPrincipalTag}})
	h.register(agent, "node")
	// Simulate the pre-upgrade schema. ensureColumn is the migration used by OpenStore.
	if _, err := h.store.db.Exec("ALTER TABLE nodes DROP COLUMN last_condition_json"); err != nil {
		t.Fatal(err)
	}
	if err := h.store.ensureColumn(t.Context(), "nodes", "last_condition_json", "BLOB"); err != nil {
		t.Fatal(err)
	}
	status, _, body := h.do(operator, http.MethodGet, "/v1/nodes", nil)
	if status != http.StatusOK {
		t.Fatalf("list status=%d body=%s", status, body)
	}
	var list struct {
		Nodes []map[string]any `json:"nodes"`
	}
	if err := json.Unmarshal(body, &list); err != nil {
		t.Fatal(err)
	}
	if value, present := list.Nodes[0]["last_condition"]; !present || value != nil {
		t.Fatalf("upgraded node fabricated history or omitted field: %s", body)
	}
}
