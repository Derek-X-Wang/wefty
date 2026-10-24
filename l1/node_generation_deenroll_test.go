package l1

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"reflect"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

func TestGenerationDeEnrollmentReturnsLegacyAuthority(t *testing.T) {
	h := newIntegrationHarness(t, map[string][]string{"node": nil})
	client := h.client(fabric.Identity{NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag}})
	registration := generationRegistration(h, "g1")
	node, err := h.store.RegisterNode(t.Context(), fabric.Identity{NodeID: "agent"}, registration, DefaultNodePolicy(), true)
	if err != nil {
		t.Fatal(err)
	}
	node, err = h.store.SetNodeClaimsByOperator(t.Context(), "node", "operator", NodeIntentRequest{IntentRevision: node.IntentRevision, ClaimsEnabled: false, Reason: "wefty maintenance operation"})
	if err != nil {
		t.Fatal(err)
	}
	inventory, err := h.store.NodeGenerations(t.Context(), "agent", "node")
	if err != nil {
		t.Fatal(err)
	}
	request := map[string]any{"operation_id": "operation", "primary_generation_id": "g1", "primary_boot_session_id": node.BootSessionID, "authority_generation": node.AuthorityGeneration, "catalog_revision": inventory.CatalogRevision, "intent_revision": node.IntentRevision, "intent_reason": node.IntentReason, "runtimes_reaped": true, "evidence_settled": true}
	for i := 0; i < 2; i++ {
		status, _, body := h.do(client, http.MethodPost, "/v1/agent/nodes/node/generations/de-enroll", request)
		if status != 200 {
			t.Fatalf("de-enrollment/replay=%d %s", status, body)
		}
		var result NodeGenerationInventory
		if json.Unmarshal(body, &result) != nil || len(result.Generations) != 1 || result.Generations[0].Role != NodeGenerationClosed {
			t.Fatalf("unexpected de-enrollment: %s", body)
		}
	}
	request["intent_revision"] = node.IntentRevision + 1
	status, _, body := h.do(client, http.MethodPost, "/v1/agent/nodes/node/generations/de-enroll", request)
	if status != 409 {
		t.Fatalf("changed replay=%d %s", status, body)
	}
	if _, err = h.store.RegisterNode(t.Context(), fabric.Identity{NodeID: "agent"}, registration, DefaultNodePolicy(), true); err == nil {
		t.Fatal("de-enrolled primary registered again")
	}
	if _, err = h.store.HeartbeatNode(t.Context(), "agent", "node", registration.BootSessionID); err == nil {
		t.Fatal("de-enrolled boot retained heartbeat authority")
	}
	registration.AgentGenerationID = ""
	registration.BootSessionID = "fresh-legacy"
	registration.Capabilities = map[string]bool{"kind:process": true}
	if _, err = h.store.RegisterNode(t.Context(), fabric.Identity{NodeID: "agent"}, registration, DefaultNodePolicy(), true); err != nil {
		t.Fatalf("same-ID legacy reinstall: %v", err)
	}
}

func TestGenerationDeEnrollmentFences(t *testing.T) {
	for _, fault := range []string{"primary", "boot", "authority", "catalog", "intent", "reason", "operation", "reap", "evidence", "staged", "retiring", "live-attempt", "unsettled-attempt", "foreign"} {
		t.Run(fault, func(t *testing.T) {
			f := newGenerationFixture(t)
			if fault == "live-attempt" || fault == "unsettled-attempt" {
				claim := f.claim(t, "work")
				if fault == "unsettled-attempt" {
					if _, err := f.h.store.db.ExecContext(t.Context(), `UPDATE attempts SET state='failed' WHERE attempt_id=?`, claim.Lease.AttemptID); err != nil {
						t.Fatal(err)
					}
				}
			}
			f.intent(t, false)
			node, err := f.h.store.SetNodeClaimsByOperator(t.Context(), "node", "operator", NodeIntentRequest{IntentRevision: f.node.IntentRevision, ClaimsEnabled: false, Reason: "wefty maintenance operation"})
			if err != nil {
				t.Fatal(err)
			}
			f.node = node
			f.refresh(t)
			r := DeEnrollNodeGenerationRequest{OperationID: "operation", PrimaryGenerationID: "g1", PrimaryBootSessionID: node.BootSessionID, AuthorityGeneration: node.AuthorityGeneration, CatalogRevision: f.inventory.CatalogRevision, IntentRevision: node.IntentRevision, IntentReason: node.IntentReason, RuntimesReaped: true, EvidenceSettled: true}
			identity := "agent"
			switch fault {
			case "primary":
				r.PrimaryGenerationID = "other"
			case "boot":
				r.PrimaryBootSessionID = "other"
			case "authority":
				r.AuthorityGeneration++
			case "catalog":
				r.CatalogRevision++
			case "intent":
				r.IntentRevision++
			case "reason":
				r.IntentReason = "other"
			case "operation":
				r.OperationID = "other"
			case "reap":
				r.RuntimesReaped = false
			case "evidence":
				r.EvidenceSettled = false
			case "staged", "retiring":
				if _, err = f.h.store.StageNodeGeneration(t.Context(), "agent", generationRegistration(f.h, "g2")); err != nil {
					t.Fatal(err)
				}
				if fault == "retiring" {
					if _, err = f.h.store.db.ExecContext(t.Context(), `UPDATE node_generations SET role='retiring' WHERE node_id='node' AND generation_id='g2'`); err != nil {
						t.Fatal(err)
					}
				}
			case "foreign":
				identity = "other"
			}
			before, err := f.h.store.NodeGenerations(t.Context(), "agent", "node")
			if err != nil {
				t.Fatal(err)
			}
			if _, err = f.h.store.DeEnrollNodeGeneration(t.Context(), identity, "node", r); err == nil {
				t.Fatal("unsafe de-enrollment accepted")
			}
			after, err := f.h.store.NodeGenerations(t.Context(), "agent", "node")
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, after) {
				t.Fatal("refusal changed generation authority")
			}
		})
	}
}

func TestGenerationDeEnrollmentPostLedgerRefusalIsTerminal(t *testing.T) {
	h := newIntegrationHarness(t, map[string][]string{"node": nil})
	identity := fabric.Identity{NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag}}
	node, err := h.store.RegisterNode(t.Context(), identity, generationRegistration(h, "g1"), DefaultNodePolicy(), true)
	if err != nil {
		t.Fatal(err)
	}
	node, err = h.store.SetNodeClaimsByOperator(t.Context(), "node", "operator", NodeIntentRequest{IntentRevision: node.IntentRevision, ClaimsEnabled: false, Reason: "wefty maintenance operation"})
	if err != nil {
		t.Fatal(err)
	}
	inventory, err := h.store.NodeGenerations(t.Context(), "agent", "node")
	if err != nil {
		t.Fatal(err)
	}
	request := DeEnrollNodeGenerationRequest{OperationID: "operation", PrimaryGenerationID: "g1", PrimaryBootSessionID: node.BootSessionID, AuthorityGeneration: node.AuthorityGeneration, CatalogRevision: inventory.CatalogRevision, IntentRevision: node.IntentRevision + 1, IntentReason: node.IntentReason, RuntimesReaped: true, EvidenceSettled: true}
	payload, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(payload)
	requestHash := "sha256:" + hex.EncodeToString(sum[:])
	client := h.client(identity)
	for pass := 0; pass < 2; pass++ {
		status, _, body := h.do(client, http.MethodPost, "/v1/agent/nodes/node/generations/de-enroll", request)
		var response contract.ErrorResponse
		if status != 409 || json.Unmarshal(body, &response) != nil || response.Error.Code != "de_enrollment_not_committed" {
			t.Fatalf("post-ledger rejection pass %d: HTTP %d %s", pass, status, body)
		}
		if response.Error.Retryable || response.Error.Details["node_id"] != "node" || response.Error.Details["operation_id"] != "operation" || response.Error.Details["request_sha256"] != requestHash {
			t.Fatal("rejection omitted exact-request proof", response)
		}
		if pass == 0 {
			// Make every fence valid after the first refusal. The saved negative
			// outcome must prevent even an already-delayed identical RPC from committing.
			node, err = h.store.SetNodeClaimsByOperator(t.Context(), "node", "operator", NodeIntentRequest{IntentRevision: node.IntentRevision, ClaimsEnabled: false, Reason: node.IntentReason})
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	after, err := h.store.NodeGenerations(t.Context(), "agent", "node")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(inventory, after) {
		t.Fatal("rejected replay changed generation authority")
	}
	changed := request
	changed.CatalogRevision++
	status, _, body := h.do(client, http.MethodPost, "/v1/agent/nodes/node/generations/de-enroll", changed)
	var response contract.ErrorResponse
	if status != 409 || json.Unmarshal(body, &response) != nil || response.Error.Code == "de_enrollment_not_committed" {
		t.Fatal("changed-payload replay was certified as rejected", status, string(body))
	}
}
