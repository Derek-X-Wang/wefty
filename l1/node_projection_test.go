package l1

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
		Verb     string         `json:"verb"`
		Requires map[string]any `json:"requires"`
		Inputs   []struct {
			Name     string `json:"name"`
			Type     string `json:"type"`
			Required bool   `json:"required"`
		} `json:"inputs"`
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
					var exactRequirements map[string]any
					found := false
					for _, action := range list.Nodes[0].AllowedActions {
						if action.Verb != verb {
							continue
						}
						found = true
						listedAllowed = action.RefusedBecause == nil
						value, ok := action.Requires["intent_revision"].(float64)
						if !ok || len(action.Requires) != map[string]int{"drain": 2, "set-claims": 1}[verb] || len(action.Inputs) != map[string]int{"drain": 1, "set-claims": 2}[verb] || action.Inputs[0].Name != "reason" || action.Inputs[0].Type != "string" || !action.Inputs[0].Required {
							t.Fatalf("missing preconditions: %#v", action)
						}
						revision = int64(value)
						exactRequirements = action.Requires
						if verb == "drain" && action.Requires["claims_enabled"] != false {
							t.Fatalf("missing exact drain value: %#v", action)
						}
						if verb == "set-claims" && (action.Inputs[1].Name != "claims_enabled" || action.Inputs[1].Type != "boolean" || !action.Inputs[1].Required) {
							t.Fatalf("missing boolean input: %#v", action)
						}
					}
					if !found {
						t.Fatalf("missing action %s", verb)
					}
					path := "/v1/nodes/node/drain"
					if verb == "set-claims" {
						path = "/v1/nodes/node/claims"
					}
					status, _, body = h.do(operator, http.MethodPost, path, func() map[string]any {
						body := map[string]any{"reason": "operator decision"}
						for name, value := range exactRequirements {
							body[name] = value
						}
						if verb == "set-claims" {
							body["claims_enabled"] = !enabled
						}
						return body
					}())
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
					if refusal.Error.Details["expected_revision"] != float64(revision+1) {
						t.Fatalf("missing current revision: %s", body)
					}
				})
			}
		}
	}
}

func TestNodeProjectionReportsAttemptsAndDurableCondition(t *testing.T) {
	h := newIntegrationHarnessWithReconcileInterval(t, StoreOptions{}, map[string]NodePolicy{
		"node": DefaultNodePolicy("linux"),
	}, true, time.Hour)
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
	lastRecorded := facts.LastCondition
	h.clock.Advance(DefaultNodeDeadAfter)
	status, _, body = h.do(operator, http.MethodGet, "/v1/nodes/node", nil)
	if status != http.StatusOK {
		t.Fatalf("detail status=%d body=%s", status, body)
	}
	if err := json.Unmarshal(body, &facts); err != nil {
		t.Fatal(err)
	}
	// Looking reports effective liveness, but leaves attempts and the last
	// recorded condition alone until a cleanup pass records the transitions.
	var effective Node
	if err := json.Unmarshal(body, &effective); err != nil {
		t.Fatal(err)
	}
	if effective.State != contract.NodeDead || len(facts.ActiveAttempts) != 2 || facts.LastCondition == nil || facts.LastCondition.Code != lastRecorded.Code || !facts.LastCondition.Since.Equal(lastRecorded.Since) {
		t.Fatalf("view fabricated recorded consequences: %s", body)
	}
	if result, err := h.store.Reconcile(t.Context()); err != nil || result.DeadNodes != 1 || result.ExpiredAttempts != 2 {
		t.Fatalf("cleanup result=%#v err=%v", result, err)
	}
	status, _, body = h.do(operator, http.MethodGet, "/v1/nodes/node", nil)
	if status != http.StatusOK {
		t.Fatalf("reconciled detail status=%d body=%s", status, body)
	}
	if err := json.Unmarshal(body, &facts); err != nil {
		t.Fatal(err)
	}
	if facts.ActiveAttempts == nil || len(facts.ActiveAttempts) != 0 || facts.LastCondition == nil || facts.LastCondition.Code != "node_dead" || facts.LastCondition.Scope != "node_liveness" || !facts.LastCondition.Since.Equal(h.clock.Now()) {
		t.Fatalf("cleanup did not record attempt expiry/dead condition: %s", body)
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
			if response.Error.Details["expected_revision"] != float64(4) || response.Error.Details["observed_revision"] != float64(3) {
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
	// Capability recovery is a new notable fact, recorded once.
	observation.Revision = 3
	observation.ReasonCode = ""
	observation.MissingCapabilities = []string{}
	observation.Capabilities["kind:oci"] = true
	node, err = store.HeartbeatNodeWithCapabilityObservation(t.Context(), "agent", "node", "boot", observation, NodePolicy{Tags: []string{"linux"}, MaxOneshotSlots: 1})
	if err != nil {
		t.Fatal(err)
	}
	if facts = decode(node); facts.LastCondition == nil || facts.LastCondition.Code != "node_capability_recovered" || facts.LastCondition.Scope != "node_capability" || !facts.LastCondition.Since.Equal(clock.Now()) || facts.LastCondition.Details["capability_revision"] != float64(3) {
		t.Fatalf("missing recovery condition: %#v", facts)
	}
	since = facts.LastCondition.Since
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

// Denied principals cannot read the client route, so exercise its projection
// with an authenticated request context, then compare to the real write route.
// Agent register/drain/heartbeat below also test publicly reachable projections.
func TestNodeAllowedActionsMatchPrincipalHTTP(t *testing.T) {
	for _, clientTag := range []string{DefaultClientPrincipalTag, "custom-client"} {
		for _, identity := range []fabric.Identity{
			{NodeID: "client", Kind: fabric.IdentityKindMachine, Tags: []string{clientTag}},
			{NodeID: "agent", Kind: fabric.IdentityKindMachine, Tags: []string{DefaultAgentPrincipalTag}},
			{NodeID: "person", FabricID: "fabric", UserID: "person", DeviceID: "device"},
			{NodeID: "untagged", Kind: fabric.IdentityKindMachine},
			{NodeID: "dual", Kind: fabric.IdentityKindMachine, Tags: []string{clientTag, DefaultAgentPrincipalTag}},
		} {
			t.Run(clientTag+"/"+identity.NodeID, func(t *testing.T) {
				h := newIntegrationHarness(t, map[string][]string{"node": {"linux"}})
				h.server.clientPrincipalTag = clientTag
				agent := h.client(fabric.Identity{NodeID: "registration-agent", Tags: []string{DefaultAgentPrincipalTag}})
				h.register(agent, "node")
				caller := h.client(identity)
				for _, verb := range []string{"drain", "set-claims"} {
					r := httptest.NewRequest(http.MethodGet, "/v1/nodes/node", nil)
					r.SetPathValue("node_id", "node")
					r = r.WithContext(context.WithValue(r.Context(), identityContextKey{}, identity))
					w := httptest.NewRecorder()
					h.server.getNode(w, r)
					if w.Code != http.StatusOK {
						t.Fatalf("projection: %d %s", w.Code, w.Body.String())
					}
					var facts nodeOperatorFacts
					if err := json.Unmarshal(w.Body.Bytes(), &facts); err != nil {
						t.Fatal(err)
					}
					var actionFound bool
					for _, action := range facts.AllowedActions {
						if action.Verb != verb {
							continue
						}
						actionFound = true
						path := "drain"
						if verb == "set-claims" {
							path = "claims"
						}
						status, _, body := h.do(caller, http.MethodPost, "/v1/nodes/node/"+path, NodeIntentRequest{IntentRevision: facts.IntentRevision, ClaimsEnabled: false, Reason: "principal parity"})
						if (status == http.StatusOK) != (action.RefusedBecause == nil) {
							t.Fatalf("action=%#v write=%d %s", action, status, body)
						}
						if action.RefusedBecause != nil {
							var response contract.ErrorResponse
							if err := json.Unmarshal(body, &response); err != nil {
								t.Fatal(err)
							}
							if response.Error.Code != action.RefusedBecause.Code || response.Error.Retryable != action.RefusedBecause.Retryable {
								t.Fatalf("action refusal=%#v write=%s", action.RefusedBecause, body)
							}
						}
					}
					if !actionFound {
						t.Fatalf("missing %s: %s", verb, w.Body.String())
					}
				}
			})
		}
	}
}

func TestAgentNodeActionsRefuseOperatorProtocol(t *testing.T) {
	h := newIntegrationHarness(t, map[string][]string{"node": {"linux"}})
	agent := h.client(fabric.Identity{NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag}})
	registration := contract.NodeRegistration{NodeID: "node", BootSessionID: "boot", OS: "linux", Architecture: "arm64", AgentVersion: "test", Capabilities: map[string]bool{"kind:process": true}, CapabilityRevision: 1, CapabilityObservedAt: h.clock.Now(), MissingCapabilities: []string{}}
	for _, check := range []struct {
		path string
		body any
	}{
		{"register", registration},
		{"node/heartbeat", HeartbeatRequest{BootSessionID: "boot", Capabilities: registration.Capabilities, CapabilityRevision: 1, CapabilityObservedAt: h.clock.Now(), MissingCapabilities: []string{}}},
		{"node/drain", DrainRequest{BootSessionID: "boot"}},
	} {
		t.Run(check.path, func(t *testing.T) {
			status, _, body := h.do(agent, http.MethodPost, "/v1/agent/nodes/"+check.path, check.body)
			if status != http.StatusOK {
				t.Fatalf("status=%d body=%s", status, body)
			}
			var facts nodeOperatorFacts
			if err := json.Unmarshal(body, &facts); err != nil {
				t.Fatal(err)
			}
			if len(facts.AllowedActions) != 2 {
				t.Fatalf("missing actions: %s", body)
			}
			for _, action := range facts.AllowedActions {
				if action.RefusedBecause == nil || action.RefusedBecause.Code != contract.ErrorPrincipalForbidden || action.RefusedBecause.Retryable {
					t.Fatalf("agent offered operator action: %s", body)
				}
			}
		})
	}
}

func TestNodeReadsDoNotAcquireWriteLock(t *testing.T) {
	h := newIntegrationHarnessWithReconcileInterval(t, StoreOptions{}, map[string]NodePolicy{
		"node": DefaultNodePolicy("linux"),
	}, true, time.Hour)
	agent := h.client(fabric.Identity{NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag}})
	operator := h.client(fabric.Identity{NodeID: "operator", Tags: []string{DefaultClientPrincipalTag}})
	node := h.register(agent, "node")
	h.clock.Advance(DefaultNodeDeadAfter)
	tx, err := h.store.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	// An uncommitted change also proves the HTTP response reads a separate
	// committed snapshot. The real handler includes Fabric authentication and
	// principal middleware; calling Store methods alone misses route writes.
	if _, err := tx.ExecContext(t.Context(), "UPDATE nodes SET intent_revision=99 WHERE node_id='node'"); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/v1/nodes/node", "/v1/nodes?state=dead"} {
		t.Run(path, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 700*time.Millisecond)
			defer cancel()
			request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://control-plane"+path, nil)
			if err != nil {
				t.Fatal(err)
			}
			response, err := operator.Do(request)
			if err != nil {
				t.Fatalf("HTTP read waited for writer: %v", err)
			}
			defer response.Body.Close()
			var got Node
			if path == "/v1/nodes/node" {
				err = json.NewDecoder(response.Body).Decode(&got)
			} else {
				var page NodeList
				err = json.NewDecoder(response.Body).Decode(&page)
				if len(page.Nodes) != 1 {
					t.Fatalf("locked list returned %#v, status=%d", page, response.StatusCode)
				}
				got = page.Nodes[0]
			}
			if err != nil || response.StatusCode != http.StatusOK || ctx.Err() != nil || got.State != contract.NodeDead || got.IntentRevision != node.IntentRevision || len(got.AllowedActions) != 2 {
				t.Fatalf("locked HTTP read: node=%#v err=%v status=%d context=%v", got, err, response.StatusCode, ctx.Err())
			}
		})
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	var recorded contract.NodeState
	var revision int64
	if err := h.store.db.QueryRow("SELECT state, intent_revision FROM nodes WHERE node_id='node'").Scan(&recorded, &revision); err != nil {
		t.Fatal(err)
	}
	if recorded != contract.NodeAlive || revision != node.IntentRevision {
		t.Fatalf("HTTP reads wrote facts: state=%s revision=%d", recorded, revision)
	}
}

func TestNodeRegistrationCapabilityConditions(t *testing.T) {
	for _, newBoot := range []bool{false, true} {
		t.Run(map[bool]string{false: "same boot", true: "new boot"}[newBoot], func(t *testing.T) {
			h := newIntegrationHarness(t, map[string][]string{"node": {"linux"}})
			agent := h.client(fabric.Identity{NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag}})
			node := h.register(agent, "node")
			registration := node.NodeRegistration
			registration.Capabilities["kind:oci"] = false
			registration.MissingCapabilities = []string{"kind:oci"}
			registration.CapabilityReasonCode = contract.CapabilityReasonHelperHandshakeFailed
			registration.SupersedeCapabilityRevision = true
			registration.CapabilityRevision = 99 // L1 stores previous revision + 1.
			withdrawn, err := h.store.RegisterNode(t.Context(), fabric.Identity{NodeID: "agent"}, registration, NodePolicy{}, true)
			if err != nil {
				t.Fatal(err)
			}
			if withdrawn.LastCondition == nil || withdrawn.LastCondition.Details["capability_revision"] != float64(withdrawn.CapabilityRevision) {
				t.Fatalf("withdrawal does not describe stored revision: %#v", withdrawn)
			}
			h.clock.Advance(time.Second)
			registration.CapabilityReasonCode = ""
			registration.SupersedeCapabilityRevision = false
			registration.CapabilityRevision = withdrawn.CapabilityRevision + 1
			registration.Capabilities["kind:oci"] = true
			registration.MissingCapabilities = []string{}
			if newBoot {
				registration.BootSessionID = "new-boot"
			}
			recovered, err := h.store.RegisterNode(t.Context(), fabric.Identity{NodeID: "agent"}, registration, NodePolicy{}, true)
			if err != nil {
				t.Fatal(err)
			}
			if recovered.LastCondition == nil || recovered.LastCondition.Code != "node_capability_recovered" || recovered.LastCondition.Scope != "node_capability" || !recovered.LastCondition.Since.Equal(h.clock.Now()) || recovered.LastCondition.Details["capability_revision"] != float64(recovered.CapabilityRevision) {
				t.Fatalf("missing registration recovery: %#v", recovered)
			}
			since := recovered.LastCondition.Since
			h.clock.Advance(time.Second)
			recovered, err = h.store.RegisterNode(t.Context(), fabric.Identity{NodeID: "agent"}, registration, NodePolicy{}, true)
			if err != nil {
				t.Fatal(err)
			}
			if !recovered.LastCondition.Since.Equal(since) {
				t.Fatalf("repeated healthy registration advanced condition: %#v", recovered.LastCondition)
			}
		})
	}
}
