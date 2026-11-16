package l1

import (
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

// TestOwnerComputerSeatSettingsFollowCreationAndImage covers spec P17 and the
// Seat support rule (#1155): owner settings default to Seat limit 2 and idle
// action nothing, are validated once at the create boundary, and the image's
// Seat places belong to the projection that carries that image.
func TestOwnerComputerSeatSettingsFollowCreationAndImage(t *testing.T) {
	h := newIntegrationHarnessWithOptions(t, StoreOptions{LeaseDuration: 3 * time.Second}, map[string]NodePolicy{
		"computer-node": {Tags: []string{contract.StableNodeTagPrefix + "computer-node"}, MaxOneshotSlots: 1, MaxServiceSlots: 1},
	})
	node := registerCapabilityNodeWithTags(t, h, "computer-node", map[string]bool{
		"kind:oci": true, "cgroup_v2": true, "computer": true, "runtime_platform:linux/amd64": true,
	}, []string{contract.StableNodeTagPrefix + "computer-node"})

	legacy, _, err := h.store.CreateComputer(t.Context(), CreateComputerRequest{Name: "api-default", Spec: computerCapabilityJobSpec("computer:seat-default:v1"), Actor: "operator"})
	if err != nil || legacy.ComputerSettings != (ComputerSettings{SeatLimit: 2, IdleAction: ComputerIdleNothing, Network: ComputerNetworkInternet}) {
		t.Fatalf("API default settings = %+v err=%v", legacy.ComputerSettings, err)
	}
	// Only the Computer under test may be claimed below.
	if _, err := h.store.SetComputerDesiredState(t.Context(), legacy.ComputerID, computerDesiredRequest(legacy, contract.ServiceDesiredStopped, "stop")); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []CreateComputerRequest{
		{SeatLimit: intPointer(5)}, {SeatLimit: intPointer(-1)}, {IdleAction: "sleep"}, {ImageSeatPlaces: 5},
		{SeatRequestSettings: &ComputerSettings{SeatLimit: 2, IdleAction: ComputerIdleDelete, Network: ComputerNetworkNone}, SeatLimit: intPointer(1)},
	} {
		bad.Name, bad.Actor, bad.Spec = "bad", "operator", computerCapabilityJobSpec("computer:seat-bad:v1")
		if _, _, err := h.store.CreateComputer(t.Context(), bad); errorCode(err) != contract.ErrorInvalidRequest {
			t.Fatalf("invalid owner settings %+v err=%v", bad, err)
		}
	}

	request := CreateComputerRequest{Name: "desk", Spec: computerCapabilityJobSpec("computer:seat-desk:v1"), Actor: "operator",
		SeatLimit: intPointer(3), IdleAction: ComputerIdleStop, ImageSeatPlaces: 4}
	computer, _, err := h.store.CreateComputer(t.Context(), request)
	want := ComputerSettings{SeatLimit: 3, IdleAction: ComputerIdleStop, Network: ComputerNetworkInternet, ImageSeatPlaces: 4}
	if err != nil || computer.ComputerSettings != want || computer.SeatRequestMade {
		t.Fatalf("owner settings = %+v made=%v err=%v", computer.ComputerSettings, computer.SeatRequestMade, err)
	}
	// Places are a reading of the image, not creation intent: a replay with a
	// different reading still returns the same Computer.
	request.ImageSeatPlaces = 0
	if replay, replayed, err := h.store.CreateComputer(t.Context(), request); err != nil || !replayed || replay.ComputerID != computer.ComputerID {
		t.Fatalf("replay = %+v replayed=%v err=%v", replay.ComputerID, replayed, err)
	}
	request.IdleAction = ComputerIdleDelete
	if _, _, err := h.store.CreateComputer(t.Context(), request); errorCode(err) != contract.ErrorDispatchKeyConflict {
		t.Fatalf("replay with another idle action err=%v", err)
	}

	computer, err = h.store.SetComputerDesiredState(t.Context(), computer.ComputerID, computerDesiredRequest(computer, contract.ServiceDesiredStopped, "stop"))
	if err != nil {
		t.Fatal(err)
	}
	reimage := ComputerReimageRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"),
		Image: reimageTarget('d'), IdempotencyKey: "seat-reimage", ImageSeatPlaces: 0}
	staged, err := h.store.ReimageComputer(t.Context(), computer.ComputerID, reimage)
	if err != nil || staged.ImageSeatPlaces != 4 {
		t.Fatalf("a staged reimage changed the current image's places: %+v err=%v", staged.ComputerSettings, err)
	}
	acknowledgeReimagePreflight(t, h, node, computer.ComputerID, "", "")
	// A retry that reads the label differently still replays, and the first
	// accepted reading stays: the reading is not reimage authority.
	retry := reimage
	retry.ImageSeatPlaces = 4
	reimaged, err := h.store.ReimageComputer(t.Context(), computer.ComputerID, retry)
	if err != nil || reimaged.CurrentJobID == computer.CurrentJobID || reimaged.ImageSeatPlaces != 0 || reimaged.SeatLimit != 3 {
		t.Fatalf("reimaged settings = %+v job %s err=%v", reimaged.ComputerSettings, reimaged.CurrentJobID, err)
	}
	bad := reimage
	bad.IdempotencyKey, bad.ImageSeatPlaces = "seat-reimage-bad", 9
	if _, err := h.store.ReimageComputer(t.Context(), computer.ComputerID, bad); errorCode(err) != contract.ErrorInvalidRequest {
		t.Fatalf("invalid reimage places err=%v", err)
	}
	reimaged, err = h.store.SetComputerDesiredState(t.Context(), reimaged.ComputerID, computerDesiredRequest(reimaged, contract.ServiceDesiredRunning, "start"))
	if err != nil {
		t.Fatal(err)
	}
	claim, err := h.store.ClaimJob(t.Context(), "fabric-computer-node", node.NodeID, node.BootSessionID, contract.JobClassService)
	want.ImageSeatPlaces = 0
	if err != nil || claim == nil || claim.Job.JobID != reimaged.CurrentJobID || claim.ComputerStorage == nil || claim.ComputerStorage.ComputerSettings == nil || *claim.ComputerStorage.ComputerSettings != want {
		t.Fatalf("claim settings = %#v err=%v", claim, err)
	}
}
