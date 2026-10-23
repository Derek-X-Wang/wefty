package l1

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

func TestSeatLifecycleRoutesAreNodeFenced(t *testing.T) {
	h := newIntegrationHarnessWithPolicies(t, map[string]NodePolicy{"computer-node": {Tags: []string{contract.StableNodeTagPrefix + "computer-node"}, MaxServiceSlots: 2}})
	node := registerCapabilityNodeWithTags(t, h, "computer-node", map[string]bool{"kind:oci": true, "cgroup_v2": true, "computer": true}, []string{contract.StableNodeTagPrefix + "computer-node"})
	client := h.client(fabric.Identity{NodeID: "fabric-computer-node", Tags: []string{DefaultAgentPrincipalTag}})
	foreign := h.client(fabric.Identity{NodeID: "foreign", Tags: []string{DefaultAgentPrincipalTag}})
	cli := h.client(fabric.Identity{NodeID: "cli", Tags: []string{DefaultClientPrincipalTag}})
	settings := ComputerSettings{SeatLimit: 2, IdleAction: ComputerIdleDelete, Network: ComputerNetworkNone}
	c, _, err := h.store.CreateComputer(t.Context(), CreateComputerRequest{Name: "lifecycle-routes", Spec: computerCapabilityJobSpec("lifecycle-routes"), Actor: "test", SeatRequestSettings: &settings})
	if err != nil {
		t.Fatal(err)
	}
	id := "seat_00000000000000000000000000000001"
	_, err = h.store.BeginSeatApproval(t.Context(), "fabric-computer-node", SeatApprovalRequest{SeatID: id, NodeID: node.NodeID, BootSessionID: node.BootSessionID, Harness: "test", Folder: "/workspace", Form: "any", Network: "none", AnswerWait: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	_, err = h.store.AnswerSeatApproval(t.Context(), "fabric-computer-node", id, SeatApprovalAnswer{NodeID: node.NodeID, BootSessionID: node.BootSessionID, Approved: true})
	if err != nil {
		t.Fatal(err)
	}
	base := "/v1/agent/seat-requests/" + id
	auth := SeatNodeRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID}
	send := func(path string, input any, want SeatRequestState) {
		t.Helper()
		status, _, body := h.do(client, http.MethodPost, path, input)
		var got SeatRequest
		if status != 200 {
			t.Fatalf("%s: %d %s", path, status, body)
		}
		if err := json.Unmarshal(body, &got); err != nil || got.State != want {
			t.Fatalf("response=%+v %v", got, err)
		}
	}
	send(base+"/read", auth, SeatApproved)
	for _, other := range []*http.Client{foreign, cli} {
		status, _, body := h.do(other, http.MethodPost, base+"/read", auth)
		if status != 403 {
			t.Fatalf("foreign read=%d %s", status, body)
		}
	}
	send(base+"/reserve-room", SeatRoomReservationRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID, ComputerID: c.ComputerID, Place: 1, Room: DefaultComputerRoomConfig(1 << 30)}, SeatApproved)
	send(base+"/start", SeatStartRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID, Deadline: h.clock.Now().Add(time.Minute)}, SeatStarting)
	send(base+"/end", SeatEndRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID, Reason: "released"}, SeatEnding)
	auth.BootSessionID = "old-boot"
	status, _, body := h.do(client, http.MethodPost, base+"/read", auth)
	if status != 403 {
		t.Fatalf("stale boot=%d %s", status, body)
	}
}
