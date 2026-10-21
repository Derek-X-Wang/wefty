package l1

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
)

// identityAnswerFabric is the explicit authenticated-identity test seam. No
// production setting can turn plain, peer-asserted tags into admission authority.
func newNodeAdmissionServer(t *testing.T, config ServerConfig) (*Server, *identityAnswerFabric) {
	t.Helper()
	store, err := OpenStore(filepath.Join(t.TempDir(), "l1.sqlite"), StoreOptions{
		Clock: &fakeClock{now: time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	f := &identityAnswerFabric{}
	server, err := NewServer(f, store, config)
	if err != nil {
		t.Fatal(err)
	}
	return server, f
}

func nodeAdmissionRequest(t *testing.T, s *Server, f *identityAnswerFabric, identity fabric.Identity, method, path string, body any, status int) []byte {
	t.Helper()
	f.identity = identity
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(method, path, bytes.NewReader(data))
	w := httptest.NewRecorder()
	if method == http.MethodGet {
		w = recordTestRead(r, s.Handler().ServeHTTP)
	} else {
		s.Handler().ServeHTTP(w, r)
	}
	if w.Code != status {
		t.Fatalf("%s %s: status=%d want=%d body=%s", method, path, w.Code, status, w.Body.String())
	}
	return w.Body.Bytes()
}

func nodeAdmissionRegistration() contract.NodeRegistration {
	return contract.NodeRegistration{
		NodeID: "new-node", BootSessionID: "boot-1", OS: "linux", Architecture: "arm64", AgentVersion: "test",
		Capabilities: map[string]bool{"kind:process": true}, CapabilityRevision: 1,
		CapabilityObservedAt: time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC), MissingCapabilities: []string{},
	}
}

func TestAutomaticNodeAdmissionEnablesPinnedAndMovableClaims(t *testing.T) {
	s, f := newNodeAdmissionServer(t, ServerConfig{AutomaticNodeAdmission: true})
	agent := fabric.Identity{NodeID: "enrolled-agent", Tags: []string{DefaultAgentPrincipalTag}}
	operator := fabric.Identity{NodeID: "operator", Tags: []string{DefaultClientPrincipalTag}}
	body := nodeAdmissionRequest(t, s, f, agent, "POST", "/v1/agent/nodes/register", nodeAdmissionRegistration(), http.StatusOK)
	var node Node
	if err := json.Unmarshal(body, &node); err != nil {
		t.Fatal(err)
	}
	var mark struct {
		AutomaticallyAdmitted bool `json:"automatically_admitted"`
	}
	if err := json.Unmarshal(body, &mark); err != nil {
		t.Fatal(err)
	}
	if !node.ClaimsEnabled || !mark.AutomaticallyAdmitted || !slices.Equal(node.AuthoritativeTags, []string{"wefty:node:new-node"}) ||
		node.MaxOneshotSlots != 4 || node.MaxServiceSlots != 2 {
		t.Fatalf("automatic admission = %s", body)
	}
	for _, test := range []struct {
		key  string
		tags []string
	}{
		{"pinned", []string{"wefty:node:new-node"}}, {"movable", nil},
	} {
		spec := validJobSpec(test.key, test.tags)
		submitted := nodeAdmissionRequest(t, s, f, operator, "POST", "/v1/jobs", spec, http.StatusCreated)
		var job Job
		if err := json.Unmarshal(submitted, &job); err != nil {
			t.Fatal(err)
		}
		claimed := nodeAdmissionRequest(t, s, f, agent, "POST", "/v1/agent/jobs/claim", ClaimRequest{
			NodeID: node.NodeID, BootSessionID: node.BootSessionID, Class: contract.JobClassOneShot,
		}, http.StatusOK)
		var claim Claim
		if err := json.Unmarshal(claimed, &claim); err != nil {
			t.Fatal(err)
		}
		if claim.Job.JobID != job.JobID {
			t.Fatalf("claimed %s, want %s", claim.Job.JobID, job.JobID)
		}
	}
	// The persistent mark is also available on the operator's Node listing.
	listed := nodeAdmissionRequest(t, s, f, operator, "GET", "/v1/nodes", nil, http.StatusOK)
	if !bytes.Contains(listed, []byte(`"automatically_admitted":true`)) {
		t.Fatalf("listing lost admission mark: %s", listed)
	}
}

func TestAutomaticNodeAdmissionDoesNotReadmitExistingNode(t *testing.T) {
	s, f := newNodeAdmissionServer(t, ServerConfig{AutomaticNodeAdmission: true})
	agent := fabric.Identity{NodeID: "enrolled-agent", Tags: []string{DefaultAgentPrincipalTag}}
	registration := nodeAdmissionRegistration()
	// A known, unmarked Node refreshes default policy without being admitted.
	_, err := s.store.RegisterNode(t.Context(), agent, registration, NodePolicy{
		Tags: []string{"owner-tag"}, MaxOneshotSlots: 7, MaxServiceSlots: 3,
	}, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, boot := range []string{registration.BootSessionID, "boot-2"} {
		registration.BootSessionID = boot
		body := nodeAdmissionRequest(t, s, f, agent, "POST", "/v1/agent/nodes/register", registration, http.StatusOK)
		var node Node
		if err := json.Unmarshal(body, &node); err != nil {
			t.Fatal(err)
		}
		if node.ClaimsEnabled || len(node.AuthoritativeTags) != 0 ||
			node.MaxOneshotSlots != 4 || node.MaxServiceSlots != 2 || bytes.Contains(body, []byte(`"automatically_admitted"`)) {
			t.Fatalf("re-registration did not refresh unmarked default policy: %s", body)
		}
		body = nodeAdmissionRequest(t, s, f, agent, "POST", "/v1/agent/nodes/new-node/heartbeat", heartbeatRequestForNode(node), http.StatusOK)
		if err := json.Unmarshal(body, &node); err != nil {
			t.Fatal(err)
		}
		if node.ClaimsEnabled || len(node.AuthoritativeTags) != 0 || node.MaxOneshotSlots != 4 || node.MaxServiceSlots != 2 {
			t.Fatalf("heartbeat did not refresh unmarked default policy: %s", body)
		}
	}
}

func TestAutomaticNodeAdmissionRemovedConfiguredPolicyRefreshesDefaults(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		setting := "off"
		if enabled {
			setting = "on"
		}
		for _, refresh := range []string{"registration", "heartbeat replay", "heartbeat replacement"} {
			t.Run(setting+"/"+refresh, func(t *testing.T) {
				s, f := newNodeAdmissionServer(t, ServerConfig{AutomaticNodeAdmission: enabled, NodePolicies: map[string]NodePolicy{
					"new-node": {Tags: []string{"gpu"}, MaxOneshotSlots: 7, MaxServiceSlots: 3},
				}})
				agent := fabric.Identity{NodeID: "enrolled-agent", Tags: []string{DefaultAgentPrincipalTag}}
				operator := fabric.Identity{NodeID: "operator", Tags: []string{DefaultClientPrincipalTag}}
				registration := nodeAdmissionRegistration()
				nodeAdmissionRequest(t, s, f, agent, "POST", "/v1/agent/nodes/register", registration, http.StatusOK)
				// Restart L1 with the Node removed from its configured policy.
				restarted, err := NewServer(f, s.store, ServerConfig{AutomaticNodeAdmission: enabled})
				if err != nil {
					t.Fatal(err)
				}
				var body []byte
				if refresh == "registration" {
					registration.BootSessionID = "boot-2"
					body = nodeAdmissionRequest(t, restarted, f, agent, "POST", "/v1/agent/nodes/register", registration, http.StatusOK)
				} else {
					if refresh == "heartbeat replacement" {
						registration.CapabilityRevision++
					}
					body = nodeAdmissionRequest(t, restarted, f, agent, "POST", "/v1/agent/nodes/new-node/heartbeat",
						heartbeatRequestForNode(Node{NodeRegistration: registration}), http.StatusOK)
				}
				var node Node
				if err := json.Unmarshal(body, &node); err != nil {
					t.Fatal(err)
				}
				if !node.ClaimsEnabled || node.AutomaticallyAdmitted || node.MaxOneshotSlots != 4 || node.MaxServiceSlots != 2 {
					t.Fatalf("removed configuration still grants policy: %s", body)
				}
				// As on main, an unmarked Node's tags refresh at registration;
				// a heartbeat leaves them until the next registration.
				if refresh != "registration" {
					if !slices.Equal(node.AuthoritativeTags, []string{"gpu"}) {
						t.Fatalf("heartbeat changed an unmarked Node's tags: %s", body)
					}
					return
				}
				if len(node.AuthoritativeTags) != 0 {
					t.Fatalf("removed configuration still grants tags: %s", body)
				}
				nodeAdmissionRequest(t, restarted, f, operator, "POST", "/v1/jobs", validJobSpec("gpu-only", []string{"gpu"}), http.StatusCreated)
				nodeAdmissionRequest(t, restarted, f, agent, "POST", "/v1/agent/jobs/claim", ClaimRequest{
					NodeID: node.NodeID, BootSessionID: node.BootSessionID, Class: contract.JobClassOneShot,
				}, http.StatusNoContent)
			})
		}
	}
}

func TestAutomaticNodeAdmissionOptOutRetainsPinnedPolicy(t *testing.T) {
	for _, refresh := range []string{"registration", "heartbeat"} {
		t.Run(refresh, func(t *testing.T) {
			s, f := newNodeAdmissionServer(t, ServerConfig{AutomaticNodeAdmission: true})
			agent := fabric.Identity{NodeID: "enrolled-agent", Tags: []string{DefaultAgentPrincipalTag}}
			operator := fabric.Identity{NodeID: "operator", Tags: []string{DefaultClientPrincipalTag}}
			registration := nodeAdmissionRegistration()
			nodeAdmissionRequest(t, s, f, agent, "POST", "/v1/agent/nodes/register", registration, http.StatusOK)
			restarted, err := NewServer(f, s.store, ServerConfig{})
			if err != nil {
				t.Fatal(err)
			}
			var body []byte
			if refresh == "registration" {
				registration.BootSessionID = "boot-2"
				body = nodeAdmissionRequest(t, restarted, f, agent, "POST", "/v1/agent/nodes/register", registration, http.StatusOK)
			} else {
				body = nodeAdmissionRequest(t, restarted, f, agent, "POST", "/v1/agent/nodes/new-node/heartbeat",
					heartbeatRequestForNode(Node{NodeRegistration: registration}), http.StatusOK)
			}
			var node Node
			if err := json.Unmarshal(body, &node); err != nil {
				t.Fatal(err)
			}
			if !node.ClaimsEnabled || !node.AutomaticallyAdmitted || !slices.Equal(node.AuthoritativeTags, []string{"wefty:node:new-node"}) || node.MaxOneshotSlots != 4 || node.MaxServiceSlots != 2 {
				t.Fatalf("opt-out changed admitted Node policy: %s", body)
			}
			nodeAdmissionRequest(t, restarted, f, operator, "POST", "/v1/jobs", validJobSpec("pinned", []string{"wefty:node:new-node"}), http.StatusCreated)
			nodeAdmissionRequest(t, restarted, f, agent, "POST", "/v1/agent/jobs/claim", ClaimRequest{
				NodeID: node.NodeID, BootSessionID: node.BootSessionID, Class: contract.JobClassOneShot,
			}, http.StatusOK)
		})
	}
}

func TestAutomaticNodeAdmissionIdentityCannotAdmitAnotherNode(t *testing.T) {
	for _, first := range []string{"enabled", "disabled", "unmarked"} {
		t.Run(first, func(t *testing.T) {
			s, f := newNodeAdmissionServer(t, ServerConfig{AutomaticNodeAdmission: first != "unmarked"})
			agent := fabric.Identity{NodeID: "enrolled-agent", Tags: []string{DefaultAgentPrincipalTag}}
			operator := fabric.Identity{NodeID: "operator", Tags: []string{DefaultClientPrincipalTag}}
			registration := nodeAdmissionRegistration()
			registration.NodeID = "node-a"
			nodeAdmissionRequest(t, s, f, agent, "POST", "/v1/agent/nodes/register", registration, http.StatusOK)
			if first == "disabled" {
				nodeAdmissionRequest(t, s, f, operator, "POST", "/v1/nodes/node-a/claims",
					NodeIntentRequest{ClaimsEnabled: false, Reason: "owner disabled Node"}, http.StatusOK)
			}
			restarted, err := NewServer(f, s.store, ServerConfig{AutomaticNodeAdmission: true})
			if err != nil {
				t.Fatal(err)
			}
			s = restarted
			nodeAdmissionRequest(t, s, f, operator, "POST", "/v1/jobs", validJobSpec("movable", nil), http.StatusCreated)
			for _, nodeID := range []string{"node-b", "node-c"} {
				registration.NodeID = nodeID
				body := nodeAdmissionRequest(t, s, f, agent, "POST", "/v1/agent/nodes/register", registration, http.StatusOK)
				var node Node
				if err := json.Unmarshal(body, &node); err != nil {
					t.Fatal(err)
				}
				if node.ClaimsEnabled || node.AutomaticallyAdmitted || len(node.AuthoritativeTags) != 0 || node.MaxOneshotSlots != 4 || node.MaxServiceSlots != 2 {
					t.Fatalf("one identity admitted another Node: %s", body)
				}
				nodeAdmissionRequest(t, s, f, agent, "POST", "/v1/agent/jobs/claim", ClaimRequest{
					NodeID: nodeID, BootSessionID: registration.BootSessionID, Class: contract.JobClassOneShot,
				}, http.StatusNoContent)
			}
		})
	}
}

func TestAutomaticNodeAdmissionOwnerCanDisableAndDrain(t *testing.T) {
	for _, verb := range []string{"claims", "drain"} {
		t.Run(verb, func(t *testing.T) {
			s, f := newNodeAdmissionServer(t, ServerConfig{AutomaticNodeAdmission: true})
			agent := fabric.Identity{NodeID: "enrolled-agent", Tags: []string{DefaultAgentPrincipalTag}}
			operator := fabric.Identity{NodeID: "operator", Tags: []string{DefaultClientPrincipalTag}}
			registration := nodeAdmissionRegistration()
			nodeAdmissionRequest(t, s, f, agent, "POST", "/v1/agent/nodes/register", registration, http.StatusOK)
			nodeAdmissionRequest(t, s, f, operator, "POST", "/v1/jobs", validJobSpec("queued", []string{"wefty:node:new-node"}), http.StatusCreated)
			body := nodeAdmissionRequest(t, s, f, operator, "POST", "/v1/nodes/new-node/"+verb,
				NodeIntentRequest{IntentRevision: 0, ClaimsEnabled: false, Reason: "retire unexpected Node"}, http.StatusOK)
			var disabled Node
			if err := json.Unmarshal(body, &disabled); err != nil {
				t.Fatal(err)
			}
			if disabled.ClaimsEnabled || disabled.IntentRevision != 1 || disabled.IntentActor != "operator" {
				t.Fatalf("owner intent did not disable new claims: %s", body)
			}
			// Both actions set durable claim intent. An owner drain does not
			// pretend the boot session has shut down or revoke resident work.
			for _, boot := range []string{registration.BootSessionID, "boot-2"} {
				registration.BootSessionID = boot
				body = nodeAdmissionRequest(t, s, f, agent, "POST", "/v1/agent/nodes/register", registration, http.StatusOK)
				var node Node
				if err := json.Unmarshal(body, &node); err != nil {
					t.Fatal(err)
				}
				if node.ClaimsEnabled || node.IntentRevision != disabled.IntentRevision || node.IntentReason != disabled.IntentReason ||
					node.IntentActor != disabled.IntentActor || node.IntentUpdatedAt == nil || !node.IntentUpdatedAt.Equal(*disabled.IntentUpdatedAt) ||
					!node.AutomaticallyAdmitted || !slices.Equal(node.AuthoritativeTags, []string{"wefty:node:new-node"}) || node.MaxOneshotSlots != 4 || node.MaxServiceSlots != 2 {
					t.Fatalf("registration overwrote retired admission: %s", body)
				}
				nodeAdmissionRequest(t, s, f, agent, "POST", "/v1/agent/nodes/new-node/heartbeat", heartbeatRequestForNode(node), http.StatusOK)
				nodeAdmissionRequest(t, s, f, agent, "POST", "/v1/agent/jobs/claim", ClaimRequest{
					NodeID: registration.NodeID, BootSessionID: boot, Class: contract.JobClassOneShot,
				}, http.StatusNoContent)
			}
		})
	}
}

// A Fabric without the trust-provider seam must fail closed too.
type admissionFabricWithoutTrust struct{ fabric.Fabric }

func TestAutomaticNodeAdmissionRequiresAuthenticatedFabric(t *testing.T) {
	s, _ := newNodeAdmissionServer(t, ServerConfig{})
	plainFabric := plain.NewNetwork().NewFabric(fabric.Identity{NodeID: "control-plane"})
	for name, f := range map[string]fabric.Fabric{
		"plain": plainFabric, "trust provider absent": admissionFabricWithoutTrust{Fabric: plainFabric},
	} {
		t.Run(name, func(t *testing.T) {
			for _, allowPerson := range []bool{false, true} {
				_, err := NewServer(f, s.store, ServerConfig{AutomaticNodeAdmission: true, AllowSelfAssertedPersonIdentities: allowPerson})
				if err == nil || !strings.Contains(err.Error(), "requires authenticated Fabric identities") {
					t.Fatalf("untrusted Fabric admitted: %v", err)
				}
			}
			if _, err := NewServer(f, s.store, ServerConfig{}); err != nil {
				t.Fatalf("legacy Fabric refused: %v", err)
			}
		})
	}
}

func TestAutomaticNodeAdmissionRequiresAgentAndKeepsIdentityBinding(t *testing.T) {
	s, f := newNodeAdmissionServer(t, ServerConfig{AutomaticNodeAdmission: true})
	registration := nodeAdmissionRegistration()
	for _, identity := range []fabric.Identity{
		{NodeID: "client", Tags: []string{DefaultClientPrincipalTag}},
		{NodeID: "untagged"},
		{Tags: []string{DefaultAgentPrincipalTag}},
	} {
		nodeAdmissionRequest(t, s, f, identity, "POST", "/v1/agent/nodes/register", registration, http.StatusForbidden)
		if _, err := s.store.GetNode(t.Context(), registration.NodeID); errorCode(err) != contract.ErrorNotFound {
			t.Fatalf("unauthorized registration left a Node: %v", err)
		}
	}
	agent := fabric.Identity{NodeID: "enrolled-agent", Tags: []string{DefaultAgentPrincipalTag}}
	nodeAdmissionRequest(t, s, f, agent, "POST", "/v1/agent/nodes/register", registration, http.StatusOK)
	intruder := fabric.Identity{NodeID: "other-agent", Tags: []string{DefaultAgentPrincipalTag}}
	body := nodeAdmissionRequest(t, s, f, intruder, "POST", "/v1/agent/nodes/register", registration, http.StatusForbidden)
	var refusal contract.ErrorResponse
	if err := json.Unmarshal(body, &refusal); err != nil {
		t.Fatal(err)
	}
	if refusal.Error.Code != contract.ErrorIdentityBound {
		t.Fatalf("identity binding refusal = %s", body)
	}
}

func TestAutomaticNodeAdmissionConfiguredPolicyWinsAndCapacityIsNotSelfReported(t *testing.T) {
	s, f := newNodeAdmissionServer(t, ServerConfig{AutomaticNodeAdmission: true, NodePolicies: map[string]NodePolicy{
		"new-node": {Tags: []string{"owner-tag"}, MaxOneshotSlots: 1, MaxServiceSlots: 0},
	}})
	agent := fabric.Identity{NodeID: "enrolled-agent", Tags: []string{DefaultAgentPrincipalTag}}
	registration := nodeAdmissionRegistration()
	body := nodeAdmissionRequest(t, s, f, agent, "POST", "/v1/agent/nodes/register", registration, http.StatusOK)
	var node Node
	if err := json.Unmarshal(body, &node); err != nil {
		t.Fatal(err)
	}
	if !node.ClaimsEnabled || node.AutomaticallyAdmitted || !slices.Equal(node.AuthoritativeTags, []string{"owner-tag"}) || node.MaxOneshotSlots != 1 || node.MaxServiceSlots != 0 {
		t.Fatalf("configured admission did not win: %s", body)
	}
	// An unconfigured agent cannot smuggle either class capacity through its
	// capabilities, even when automatic admission is enabled.
	for _, capability := range []string{"max_oneshot_slots", "max_service_slots"} {
		registration.NodeID = capability
		registration.Capabilities = map[string]bool{"kind:process": true, capability: true}
		nodeAdmissionRequest(t, s, f, agent, "POST", "/v1/agent/nodes/register", registration, http.StatusBadRequest)
		if _, err := s.store.GetNode(t.Context(), registration.NodeID); errorCode(err) != contract.ErrorNotFound {
			t.Fatalf("capacity refusal inserted a Node: %v", err)
		}
	}
}

func TestAutomaticNodeAdmissionOffKeepsLegacyBytes(t *testing.T) {
	// Captured from the pre-admission implementation at the dispatch base.
	want, err := os.ReadFile("testdata/node-registration-legacy.json")
	if err != nil {
		t.Fatal(err)
	}
	for _, config := range []ServerConfig{{}, {AutomaticNodeAdmission: false}} {
		s, f := newNodeAdmissionServer(t, config)
		agent := fabric.Identity{NodeID: "enrolled-agent", Tags: []string{DefaultAgentPrincipalTag}}
		body := nodeAdmissionRequest(t, s, f, agent, "POST", "/v1/agent/nodes/register", nodeAdmissionRegistration(), http.StatusOK)
		if !bytes.Equal(body, want) {
			t.Fatalf("legacy registration bytes changed:\n%s", body)
		}
		nodeAdmissionRequest(t, s, f, agent, "POST", "/v1/agent/jobs/claim", ClaimRequest{
			NodeID: "new-node", BootSessionID: "boot-1", Class: contract.JobClassOneShot,
		}, http.StatusNoContent)
	}
}

func TestAutomaticNodeAdmissionIdentityUncertaintyFailsClosed(t *testing.T) {
	for _, test := range []struct {
		name   string
		err    error
		status int
	}{
		{"absent", fabric.ErrIdentityNotFound, http.StatusUnauthorized},
		{"incomplete", fabric.ErrIdentityIncomplete, http.StatusServiceUnavailable},
		{"unavailable", errors.New("identity lookup unavailable"), http.StatusServiceUnavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			s, f := newNodeAdmissionServer(t, ServerConfig{AutomaticNodeAdmission: true})
			f.err = test.err
			nodeAdmissionRequest(t, s, f, fabric.Identity{NodeID: "unconfirmed", Tags: []string{DefaultAgentPrincipalTag}},
				"POST", "/v1/agent/nodes/register", nodeAdmissionRegistration(), test.status)
			if _, err := s.store.GetNode(t.Context(), "new-node"); errorCode(err) != contract.ErrorNotFound {
				t.Fatalf("uncertain identity inserted a Node: %v", err)
			}
		})
	}
}

func TestAutomaticNodeAdmissionUsesCanonicalPinnedTags(t *testing.T) {
	for _, test := range []struct {
		nodeID, tag string
		admitted    bool
	}{
		{"lower_node", "wefty:node:lower_node", true},
		// Canonicalization must not let a second stable ID borrow another
		// Node's reserved Pinned tag by changing only letter case.
		{"Upper_Node", "", false},
		{"invalid/node", "", false},
	} {
		t.Run(test.nodeID, func(t *testing.T) {
			s, f := newNodeAdmissionServer(t, ServerConfig{AutomaticNodeAdmission: true})
			registration := nodeAdmissionRegistration()
			registration.NodeID = test.nodeID
			body := nodeAdmissionRequest(t, s, f, fabric.Identity{NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag}},
				"POST", "/v1/agent/nodes/register", registration, http.StatusOK)
			var node Node
			if err := json.Unmarshal(body, &node); err != nil {
				t.Fatal(err)
			}
			var tags []string
			if test.tag != "" {
				tags = []string{test.tag}
			}
			if node.ClaimsEnabled != test.admitted || node.AutomaticallyAdmitted != test.admitted || !slices.Equal(node.AuthoritativeTags, tags) {
				t.Fatalf("canonical Pinned admission = %s", body)
			}
		})
	}
}

func TestAutomaticNodeAdmissionMigrationAndPersistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "l1.sqlite")
	options := StoreOptions{Clock: &fakeClock{now: time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)}}
	store, err := OpenStore(path, options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if store != nil {
			store.Close()
		}
	}()
	agent := fabric.Identity{NodeID: "enrolled-agent", Tags: []string{DefaultAgentPrincipalTag}}
	legacy, err := store.RegisterNode(t.Context(), agent, nodeAdmissionRegistration(), DefaultNodePolicy("owner-tag"), false)
	if err != nil {
		t.Fatal(err)
	}
	want, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	// Exercise OpenStore's additive migration against an existing Node row.
	if _, err := store.db.Exec("ALTER TABLE nodes DROP COLUMN automatically_admitted"); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = OpenStore(path, options)
	if err != nil {
		t.Fatal(err)
	}
	old, err := store.GetNode(t.Context(), "new-node")
	if err != nil {
		t.Fatal(err)
	}
	got, err := json.Marshal(old)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("migration changed legacy Node: %s\nwant %s", got, want)
	}
	f := &identityAnswerFabric{}
	s, err := NewServer(f, store, ServerConfig{AutomaticNodeAdmission: true})
	if err != nil {
		t.Fatal(err)
	}
	registration := nodeAdmissionRegistration()
	registration.NodeID = "another-node"
	secondAgent := fabric.Identity{NodeID: "second-enrolled-agent", Tags: []string{DefaultAgentPrincipalTag}}
	nodeAdmissionRequest(t, s, f, secondAgent, "POST", "/v1/agent/nodes/register", registration, http.StatusOK)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = OpenStore(path, options)
	if err != nil {
		t.Fatal(err)
	}
	admitted, err := store.GetNode(t.Context(), "another-node")
	if err != nil {
		t.Fatal(err)
	}
	if !admitted.AutomaticallyAdmitted || !admitted.ClaimsEnabled || !slices.Equal(admitted.AuthoritativeTags, []string{"wefty:node:another-node"}) {
		t.Fatalf("reopen lost automatic admission: %+v", admitted)
	}
}
