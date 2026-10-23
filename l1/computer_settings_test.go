package l1

import (
	"encoding/json"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
)

func TestOwnerComputerSettingsSurviveRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "l1.sqlite")
	store, err := OpenStore(path, StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	computer, _, err := store.CreateComputer(t.Context(), CreateComputerRequest{
		Name: "owner-desktop", Spec: computerCapabilityJobSpec("owner-settings"), Actor: "owner",
	})
	if err != nil {
		store.Close()
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = OpenStore(path, StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	got, err := testReads(store).GetComputer(t.Context(), computer.ComputerID)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(payload, &fields); err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]any{
		"seat_limit": float64(0), "idle_action": "nothing", "network": "internet", "seat_request_made": false,
	} {
		if fields[key] != want {
			t.Errorf("Computer %s = %v, want %v", key, fields[key], want)
		}
	}
	if !reflect.DeepEqual(got.CurrentJob.Spec, computer.CurrentJob.Spec) {
		t.Fatal("adding settings changed the owner's memory limit or image")
	}
}

func TestSeatRequestComputerSettingsReachTheNodeClaim(t *testing.T) {
	h := newIntegrationHarnessWithPolicies(t, map[string]NodePolicy{
		"computer-node": {Tags: []string{contract.StableNodeTagPrefix + "computer-node"}, MaxServiceSlots: 2},
	})
	node := registerCapabilityNodeWithTags(t, h, "computer-node", map[string]bool{
		"kind:oci": true, "cgroup_v2": true, "computer": true,
	}, []string{contract.StableNodeTagPrefix + "computer-node"})
	settings := ComputerSettings{SeatLimit: 2, IdleAction: ComputerIdleDelete, Network: ComputerNetworkNone}
	request := CreateComputerRequest{Name: "shared-desktop", Spec: computerCapabilityJobSpec("seat-request-settings"),
		Actor: "seat-request", SeatRequestSettings: &settings}
	computer, replayed, err := h.store.CreateComputer(t.Context(), request)
	if err != nil || replayed {
		t.Fatalf("create = %v, %v", replayed, err)
	}
	again, replayed, err := h.store.CreateComputer(t.Context(), request)
	if err != nil || !replayed || again.ComputerID != computer.ComputerID {
		t.Fatalf("replay = %+v, %v, %v", again, replayed, err)
	}
	settings.Network = ComputerNetworkInternet
	if _, _, err := h.store.CreateComputer(t.Context(), request); errorCode(err) != contract.ErrorDispatchKeyConflict {
		t.Fatalf("replay with another network = %v", err)
	}
	claim, err := h.store.ClaimJob(t.Context(), "fabric-computer-node", node.NodeID, node.BootSessionID, contract.JobClassService)
	if err != nil || claim == nil || claim.ComputerStorage == nil {
		t.Fatalf("claim = %+v, %v", claim, err)
	}
	payload, err := json.Marshal(claim.ComputerStorage)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(payload, &fields); err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]any{
		"seat_limit": float64(2), "idle_action": "delete", "network": "none", "seat_request_made": true,
	} {
		if fields[key] != want {
			t.Errorf("node claim %s = %v, want %v", key, fields[key], want)
		}
	}
}

func TestPreSeatComputerUpgradeKeepsOwnerSettings(t *testing.T) {
	path := filepath.Join(t.TempDir(), "l1.sqlite")
	store, err := OpenStore(path, StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	computer, _, err := store.CreateComputer(t.Context(), CreateComputerRequest{
		Name: "existing-desktop", Spec: computerCapabilityJobSpec("pre-seat-settings"), Actor: "owner",
	})
	if err != nil {
		store.Close()
		t.Fatal(err)
	}
	// Recreate the durable schema immediately before Seat settings existed.
	for _, column := range []string{"seat_limit", "idle_action", "network", "seat_request_made"} {
		if _, err := store.db.Exec("ALTER TABLE computers DROP COLUMN " + column); err != nil {
			store.Close()
			t.Fatal(err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = OpenStore(path, StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	got, err := testReads(store).GetComputer(t.Context(), computer.ComputerID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ComputerSettings != ownerComputerSettings() || got.SeatRequestMade ||
		!reflect.DeepEqual(got.CurrentJob.Spec, computer.CurrentJob.Spec) ||
		got.StorageID != computer.StorageID || got.DesiredState != computer.DesiredState {
		t.Fatalf("upgrade changed an existing Computer: %+v", got)
	}
	if _, replayed, err := store.CreateComputer(t.Context(), CreateComputerRequest{
		Name: computer.Name, Spec: computer.CurrentJob.Spec, Actor: "owner",
	}); err != nil || !replayed {
		t.Fatalf("upgraded creation replay = %v, %v", replayed, err)
	}
}
