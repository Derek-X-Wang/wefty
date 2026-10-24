package l1

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

func TestSeatLogFollowAuthorityReadDoesNotWaitForWriter(t *testing.T) {
	h := newIntegrationHarnessWithPolicies(t, map[string]NodePolicy{"computer-node": {Tags: []string{contract.StableNodeTagPrefix + "computer-node"}, MaxServiceSlots: 4}})
	node := registerCapabilityNodeWithTags(t, h, "computer-node", map[string]bool{CapabilitySeatRequests: true, "kind:oci": true, "computer": true, "cgroup_v2": true}, []string{contract.StableNodeTagPrefix + "computer-node"})
	f := &seatLifecycleFixture{store: h.store, clock: h.clock, node: node}
	f.computer = f.createComputer(t, "follow", 2)
	id := "seat_00000000000000000000000000000001"
	if _, err := h.store.CreateSeatCall(t.Context(), "cli", SeatCallInput{SeatID: id, NodeID: node.NodeID, Harness: "test", Folder: "/workspace", Form: "any", Network: "none"}); err != nil {
		t.Fatal(err)
	}
	prepareSeatLogLive(t, f, id)
	call, err := h.store.GetSeatCall(t.Context(), "cli", id)
	if err != nil {
		t.Fatal(err)
	}
	client := h.client(fabric.Identity{NodeID: "cli", Tags: []string{DefaultClientPrincipalTag}})
	locked, err := h.store.beginWriteTransaction(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer locked.rollback()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://control-plane.invalid/v1/seats/"+id+"/logs", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set(SeatCredentialHeader, call.Credential)
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("follow poll required a write transaction: %v", err)
	}
	defer response.Body.Close()
	var page SeatLogPage
	if err := json.NewDecoder(response.Body).Decode(&page); err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK || page.SeatID != id || page.Ended {
		t.Fatalf("follow page=%+v status=%d", page, response.StatusCode)
	}
}

func TestSeatExpiredCredentialRefusesImmediatelyAndRetainsLog(t *testing.T) {
	policies := map[string]NodePolicy{"computer-node": {Tags: []string{contract.StableNodeTagPrefix + "computer-node"}, MaxServiceSlots: 4}}
	h := newIntegrationHarnessWithReconcileInterval(t, StoreOptions{LeaseDuration: time.Hour}, policies, true, 24*time.Hour)
	node := registerCapabilityNodeWithTags(t, h, "computer-node", map[string]bool{CapabilitySeatRequests: true, "kind:oci": true, "computer": true, "cgroup_v2": true}, []string{contract.StableNodeTagPrefix + "computer-node"})
	f := &seatLifecycleFixture{store: h.store, clock: h.clock, node: node}
	f.computer = f.createComputer(t, "expiry", 2)
	id, secret := liveSeatWithCredential(t, f)
	client := h.client(fabric.Identity{NodeID: "cli", Tags: []string{DefaultClientPrincipalTag}})
	h.clock.Advance(30 * time.Minute)
	request, err := http.NewRequest(http.MethodPost, "http://control-plane.invalid/v1/seats/"+id+"/input", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set(SeatCredentialHeader, secret)
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	var envelope contract.ErrorResponse
	if err := json.NewDecoder(response.Body).Decode(&envelope); err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusConflict || envelope.Error.Code != "seat_ended" || envelope.Error.Message != "Seat ended: expired" {
		t.Fatalf("expired credential=%+v status=%d", envelope, response.StatusCode)
	}
	// A one-time retained log read cannot revive the expired Seat.
	request, err = http.NewRequest(http.MethodGet, "http://control-plane.invalid/v1/seats/"+id+"/logs", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set(SeatCredentialHeader, secret)
	request.Header.Set("X-Wefty-Seat-Call", "start")
	response, err = client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var page SeatLogPage
	if err := json.NewDecoder(response.Body).Decode(&page); err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK || !page.Ended || page.Reason != "expired" {
		t.Fatalf("retained log=%+v status=%d", page, response.StatusCode)
	}
	warning, end := -1, -1
	for i, event := range page.Events {
		if event.Kind == SeatEventEndsSoon {
			warning = i
		}
		if event.Kind == SeatEventEnded {
			end = i
		}
	}
	if warning < 0 || end <= warning {
		t.Fatalf("sleep-skipped warning must precede expiry: %+v", page.Events)
	}
}
