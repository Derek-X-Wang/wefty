//go:build service_acceptance

package l1

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

func TestServiceAcceptanceCapacityIsControlPlanePolicy(t *testing.T) {
	h := newIntegrationHarnessWithPolicies(t, map[string]NodePolicy{
		"capacity-node": {Tags: []string{"linux"}, MaxOneshotSlots: 2, MaxServiceSlots: 1},
	})
	agent := h.client(fabric.Identity{NodeID: "fabric-capacity-node", Tags: []string{DefaultAgentPrincipalTag}})

	selfReported := map[string]any{
		"node_id": "capacity-node", "boot_session_id": "boot-1", "os": "linux", "architecture": "arm64", "agent_version": "test",
		"capabilities": map[string]bool{"process": true, "max_service_slots": true},
	}
	status, _, body := h.do(agent, http.MethodPost, "/v1/agent/nodes/register", selfReported)
	assertAPIError(t, status, body, http.StatusBadRequest, contract.ErrorInvalidRequest)

	registration := contract.NodeRegistration{
		NodeID: "capacity-node", BootSessionID: "boot-1", OS: "linux", Architecture: "arm64", AgentVersion: "test",
		Capabilities: map[string]bool{"process": true},
	}
	status, _, body = h.do(agent, http.MethodPost, "/v1/agent/nodes/register", registration)
	if status != http.StatusOK {
		t.Fatalf("registration status = %d body=%s", status, body)
	}
	var registered Node
	if err := json.Unmarshal(body, &registered); err != nil {
		t.Fatal(err)
	}
	if registered.MaxOneshotSlots != 2 || registered.MaxServiceSlots != 1 {
		t.Fatalf("registration capacities = %d/%d, want 2/1", registered.MaxOneshotSlots, registered.MaxServiceSlots)
	}

	status, _, body = h.do(agent, http.MethodPost, "/v1/agent/nodes/capacity-node/heartbeat", HeartbeatRequest{BootSessionID: "boot-1"})
	if status != http.StatusOK {
		t.Fatalf("heartbeat status = %d body=%s", status, body)
	}
	var heartbeat Node
	if err := json.Unmarshal(body, &heartbeat); err != nil {
		t.Fatal(err)
	}
	if heartbeat.MaxOneshotSlots != 2 || heartbeat.MaxServiceSlots != 1 {
		t.Fatalf("heartbeat capacities = %d/%d, want 2/1", heartbeat.MaxOneshotSlots, heartbeat.MaxServiceSlots)
	}
}

func TestServiceAcceptanceClassScopedClaimAdmission(t *testing.T) {
	assertClaimRequiresClassSelector(t)
	assertClaimSelectorsSeparateWorkloadClasses(t)
	assertConcurrentClassClaimsStopAtConfiguredCapacity(t)
	assertServiceClaimEligibilityStaysInsideFIFOSelection(t)
	assertBoundServiceRestartsWhileOneShotCapacityIsFull(t)
}
