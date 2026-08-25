package l1

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

func TestAgentServiceBindingProofAndImageFailureLatch(t *testing.T) {
	h := newIntegrationHarness(t, map[string][]string{"node-1": {"linux"}})
	client := h.client(fabric.Identity{NodeID: "client", Tags: []string{DefaultClientPrincipalTag}})
	agent := h.client(fabric.Identity{NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag}})
	node := h.register(agent, "node-1")
	service := submitRestartService(t, h, client, "image-pin-proof", []string{"linux"}, nil)

	proofPath := "/v1/agent/jobs/" + service.JobID + "/service-binding-proof"
	status, _, body := h.do(agent, http.MethodPost, proofPath, ServiceBindingProofRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID})
	if status != http.StatusOK {
		t.Fatalf("unbound proof status=%d body=%s", status, body)
	}
	var proof ServiceBindingProofResponse
	if err := json.Unmarshal(body, &proof); err != nil || proof.Bound {
		t.Fatalf("unbound proof=%+v err=%v", proof, err)
	}

	claim := claimRestartService(t, h, agent, node)
	status, _, body = h.do(agent, http.MethodPost, proofPath, ServiceBindingProofRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID})
	if status != http.StatusOK {
		t.Fatalf("bound proof status=%d body=%s", status, body)
	}
	if err := json.Unmarshal(body, &proof); err != nil || !proof.Bound {
		t.Fatalf("bound proof=%+v err=%v", proof, err)
	}

	failurePath := "/v1/agent/jobs/" + service.JobID + "/image-reconciliation-failure"
	status, _, body = h.do(agent, http.MethodPost, failurePath, ServiceImageReconciliationFailureRequest{
		NodeID: node.NodeID, BootSessionID: node.BootSessionID,
		Failure: contract.SpawnFailure{Code: contract.SpawnFailureImageUnavailable, Message: "OCI image delivery budget exhausted"},
	})
	if status != http.StatusOK {
		t.Fatalf("image failure latch status=%d body=%s", status, body)
	}
	latched, err := h.store.GetJob(t.Context(), service.JobID)
	if err != nil || latched.State != contract.JobFailed || latched.CurrentAttemptID != "" || latched.LastFailure == nil {
		t.Fatalf("latched service=%+v err=%v", latched, err)
	}
	assertAttemptState(t, h, claim.Lease.AttemptID, contract.AttemptLost)
}

func TestServiceBindingProofRejectsReplacedBoot(t *testing.T) {
	h := newIntegrationHarness(t, map[string][]string{"node-1": {"linux"}})
	agent := h.client(fabric.Identity{NodeID: "agent", Tags: []string{DefaultAgentPrincipalTag}})
	node := h.register(agent, "node-1")
	status, _, body := h.do(agent, http.MethodPost, "/v1/agent/jobs/missing/service-binding-proof", ServiceBindingProofRequest{
		NodeID: node.NodeID, BootSessionID: node.BootSessionID + "-stale",
	})
	assertAPIError(t, status, body, http.StatusConflict, contract.ErrorNodeSessionReplaced)
}
