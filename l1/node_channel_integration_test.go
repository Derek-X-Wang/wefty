package l1

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

func TestHeartbeatIsTheNodeDirectiveAndCapacityChannel(t *testing.T) {
	assertHeartbeatIsTheNodeDirectiveAndCapacityChannel(t)
}

func assertHeartbeatIsTheNodeDirectiveAndCapacityChannel(t *testing.T) {
	t.Helper()
	h := newIntegrationHarnessWithPolicies(t, map[string]NodePolicy{
		"node-1": {Tags: []string{"node-channel"}, MaxOneshotSlots: 2, MaxServiceSlots: 2},
	})
	operator := h.client(fabric.Identity{NodeID: "operator", Tags: []string{DefaultClientPrincipalTag}})
	agent := h.client(fabric.Identity{NodeID: "fabric-agent", Tags: []string{DefaultAgentPrincipalTag}})
	registration := contract.NodeRegistration{
		NodeID: "node-1", BootSessionID: "boot-1", ConnectHost: "node-channel.example.test",
		RootInstanceID: "root-node-1", OS: "linux", Architecture: "arm64", AgentVersion: "test",
	}
	status, _, body := h.do(agent, http.MethodPost, "/v1/agent/nodes/register", registration)
	if status != http.StatusOK {
		t.Fatalf("register status = %d body=%s", status, body)
	}
	var node Node
	if err := json.Unmarshal(body, &node); err != nil {
		t.Fatal(err)
	}

	for index := 0; index < 2; index++ {
		h.submit(operator, "node-channel-one-shot-"+string(rune('a'+index)), []string{"node-channel"})
		status, _, body = h.do(agent, http.MethodPost, "/v1/agent/jobs/claim", ClaimRequest{
			NodeID: node.NodeID, BootSessionID: node.BootSessionID, Class: contract.JobClassOneShot,
		})
		if status != http.StatusOK {
			t.Fatalf("claim one-shot %d status = %d body=%s", index, status, body)
		}
	}

	services := make([]Job, 0, 2)
	for index := 0; index < 2; index++ {
		service := submitRemovalService(t, h, operator, removalServiceSpec(
			"node-channel-service-"+string(rune('a'+index)), []string{"node-channel"},
		))
		claim := claimRestartService(t, h, agent, node)
		if claim.Job.JobID != service.JobID {
			t.Fatalf("claimed service = %q, want %q", claim.Job.JobID, service.JobID)
		}
		services = append(services, service)
	}

	status, _, body = h.do(operator, http.MethodPost, "/v1/jobs/"+services[0].JobID+"/remove?class=service", nil)
	if status != http.StatusAccepted {
		t.Fatalf("remove service status = %d body=%s", status, body)
	}
	var liveRemovalAttempts int
	if err := h.store.db.QueryRow(`SELECT COUNT(*) FROM attempts
		WHERE job_id=? AND state IN (?, ?, ?)`, services[0].JobID,
		contract.AttemptClaimed, contract.AttemptRunning, contract.AttemptAwaitingInput).Scan(&liveRemovalAttempts); err != nil {
		t.Fatal(err)
	}
	if liveRemovalAttempts != 0 {
		t.Fatalf("removed service still has %d live attempts", liveRemovalAttempts)
	}

	// Simulate an L1 restart with lower authoritative configuration. The next
	// heartbeat, rather than a node re-registration, refreshes the effective
	// limits and exposes the resulting overcommit without killing residents.
	h.server.nodePolicies["node-1"] = NodePolicy{
		Tags: []string{"node-channel"}, MaxOneshotSlots: 1, MaxServiceSlots: 1,
	}
	status, _, body = h.do(agent, http.MethodPost, "/v1/agent/nodes/node-1/heartbeat", HeartbeatRequest{BootSessionID: "boot-1"})
	if status != http.StatusOK {
		t.Fatalf("heartbeat status = %d body=%s", status, body)
	}
	var heartbeat HeartbeatResponse
	if err := json.Unmarshal(body, &heartbeat); err != nil {
		t.Fatal(err)
	}
	if !heartbeat.ClaimsEnabled || heartbeat.IntentRevision != 0 || heartbeat.MaxOneshotSlots != 1 || heartbeat.MaxServiceSlots != 1 {
		t.Fatalf("heartbeat intent/capacity = %#v", heartbeat)
	}
	if heartbeat.OneshotOccupancy != 2 || heartbeat.ServiceOccupancy != 2 || !heartbeat.Overcommitted {
		t.Fatalf("heartbeat occupancy = one-shot %d/1 service %d/1 overcommitted=%t",
			heartbeat.OneshotOccupancy, heartbeat.ServiceOccupancy, heartbeat.Overcommitted)
	}
	if len(heartbeat.RemovalDirectives) != 1 || heartbeat.RemovalDirectives[0].JobID != services[0].JobID ||
		heartbeat.RemovalDirectives[0].CleanupFence == "" {
		t.Fatalf("heartbeat removal directives = %#v", heartbeat.RemovalDirectives)
	}

	h.submit(operator, "node-channel-refused-one-shot", []string{"node-channel"})
	status, _, body = h.do(agent, http.MethodPost, "/v1/agent/jobs/claim", ClaimRequest{
		NodeID: node.NodeID, BootSessionID: node.BootSessionID, Class: contract.JobClassOneShot,
	})
	if status != http.StatusNoContent {
		t.Fatalf("overcommitted one-shot claim status = %d body=%s, want 204", status, body)
	}
	submitRemovalService(t, h, operator, removalServiceSpec("node-channel-refused-service", []string{"node-channel"}))
	status, _, body = h.do(agent, http.MethodPost, "/v1/agent/jobs/claim", ClaimRequest{
		NodeID: node.NodeID, BootSessionID: node.BootSessionID, Class: contract.JobClassService,
	})
	if status != http.StatusNoContent {
		t.Fatalf("overcommitted service claim status = %d body=%s, want 204", status, body)
	}

	status, _, body = h.do(operator, http.MethodPost, "/v1/nodes/node-1/claims", NodeIntentRequest{
		ClaimsEnabled: false, IntentRevision: 0, Reason: "operator maintenance",
	})
	if status != http.StatusOK {
		t.Fatalf("disable claims status = %d body=%s", status, body)
	}
	var disabled Node
	if err := json.Unmarshal(body, &disabled); err != nil {
		t.Fatal(err)
	}
	status, _, body = h.do(agent, http.MethodPost, "/v1/agent/nodes/node-1/drain", DrainRequest{BootSessionID: "boot-1"})
	if status != http.StatusOK {
		t.Fatalf("agent shutdown drain status = %d body=%s", status, body)
	}
	var draining Node
	if err := json.Unmarshal(body, &draining); err != nil {
		t.Fatal(err)
	}
	if draining.State != contract.NodeDraining || draining.ClaimsEnabled || draining.IntentRevision != disabled.IntentRevision ||
		draining.IntentReason != disabled.IntentReason || draining.IntentActor != disabled.IntentActor ||
		(draining.IntentUpdatedAt == nil || disabled.IntentUpdatedAt == nil || !draining.IntentUpdatedAt.Equal(*disabled.IntentUpdatedAt)) {
		t.Fatalf("agent drain changed durable operator intent: before=%#v after=%#v", disabled, draining)
	}

	registration.BootSessionID = "boot-2"
	status, _, body = h.do(agent, http.MethodPost, "/v1/agent/nodes/register", registration)
	if status != http.StatusOK {
		t.Fatalf("replacement registration status = %d body=%s", status, body)
	}
	var rejoined Node
	if err := json.Unmarshal(body, &rejoined); err != nil {
		t.Fatal(err)
	}
	if rejoined.State != contract.NodeAlive || rejoined.ClaimsEnabled || rejoined.IntentRevision != disabled.IntentRevision ||
		rejoined.IntentReason != disabled.IntentReason || rejoined.ConnectHost != registration.ConnectHost {
		t.Fatalf("re-registration lost liveness/intent/connect projection: %#v", rejoined)
	}
	status, _, body = h.do(agent, http.MethodPost, "/v1/agent/nodes/node-1/heartbeat", HeartbeatRequest{BootSessionID: "boot-2"})
	if status != http.StatusOK {
		t.Fatalf("rejoined heartbeat status = %d body=%s", status, body)
	}
	heartbeat = HeartbeatResponse{}
	if err := json.Unmarshal(body, &heartbeat); err != nil {
		t.Fatal(err)
	}
	if heartbeat.ClaimsEnabled || heartbeat.IntentRevision != disabled.IntentRevision ||
		heartbeat.MaxOneshotSlots != 1 || heartbeat.MaxServiceSlots != 1 ||
		len(heartbeat.RemovalDirectives) != 1 || heartbeat.RemovalDirectives[0].JobID != services[0].JobID {
		t.Fatalf("rejoined heartbeat channel = %#v", heartbeat)
	}

	status, _, body = h.do(operator, http.MethodGet, "/v1/nodes", nil)
	if status != http.StatusOK {
		t.Fatalf("list nodes status = %d body=%s", status, body)
	}
	if bytes.Contains(body, []byte("removal_directives")) ||
		bytes.Contains(body, []byte(heartbeat.RemovalDirectives[0].CleanupFence)) {
		t.Fatalf("operator node projection leaked heartbeat cleanup authority: %s", body)
	}
	var listed NodeList
	if err := json.Unmarshal(body, &listed); err != nil {
		t.Fatal(err)
	}
	if len(listed.Nodes) != 1 || listed.Nodes[0].ConnectHost != registration.ConnectHost ||
		listed.Nodes[0].OneshotOccupancy != 2 || listed.Nodes[0].ServiceOccupancy != 2 || !listed.Nodes[0].Overcommitted {
		t.Fatalf("operator node projection = %#v", listed.Nodes)
	}
}
