package l1

import (
	"context"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

func TestAuthenticatedStorePersonDeviceBindingWithoutRequestContext(t *testing.T) {
	h := newIntegrationHarnessWithOptions(t, StoreOptions{}, map[string]NodePolicy{"computer-node": DefaultNodePolicy(contract.StableNodeTagPrefix + "computer-node")})
	node := registerCapabilityNodeWithTags(t, h, "computer-node", map[string]bool{"kind:oci": true, "cgroup_v2": true, "computer": true}, []string{contract.StableNodeTagPrefix + "computer-node"})
	ctx := context.Background()
	computer, _, err := h.store.CreateComputer(ctx, CreateComputerRequest{Name: "store-binding", Spec: computerCapabilityJobSpec("store-binding"), Actor: "test"})
	if err != nil {
		t.Fatal(err)
	}
	owner := fabric.Identity{FabricID: "issuer", UserID: "owner", DeviceID: "enrolled-device"}
	f := &identityAnswerFabric{identity: owner}
	if _, err := NewServer(f, h.store, ServerConfig{}); err != nil {
		t.Fatal(err)
	}
	challenge, err := h.store.InitiateAdminBootstrap(ctx)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := h.store.BootstrapAdmin(ctx, owner, challenge.Nonce)
	if err != nil {
		t.Fatal(err)
	}
	t.Run("bootstrap binds device", func(t *testing.T) {
		if len(policy.Admins) != 1 || policy.Admins[0].DeviceID != owner.DeviceID {
			t.Fatalf("bare-context bootstrap omitted device binding: %+v", policy.Admins)
		}
	})
	wrong := owner
	wrong.DeviceID = "other-device"
	check := func(t *testing.T) {
		t.Helper()
		t.Run("snapshot", func(t *testing.T) {
			snapshot, err := h.store.IssueComputerPolicySnapshot(ctx, "fabric-computer-node", owner.FabricID, node.NodeID, node.BootSessionID, DefaultComputerPolicyFreshness)
			if err != nil {
				t.Fatal(err)
			}
			if snapshot == nil || !snapshot.AdminDeviceRequired || len(snapshot.Admins) != 1 || snapshot.Admins[0].DeviceID != owner.DeviceID {
				t.Fatalf("bare-context snapshot omitted device binding: %+v", snapshot)
			}
			if err := ValidateComputerPolicySnapshot(*snapshot); err != nil {
				t.Fatal(err)
			}
		})
		t.Run("admin read", func(t *testing.T) {
			if _, err := h.store.GetVisibleAdminPolicy(ctx, wrong); errorCode(err) != contract.ErrorPersonDeviceMismatch {
				t.Fatalf("bare-context admin read bypassed device binding: %v", err)
			}
			if _, err := h.store.GetVisibleAdminPolicy(ctx, owner); err != nil {
				t.Fatalf("enrolled device refused: %v", err)
			}
		})
		t.Run("person discovery", func(t *testing.T) {
			if _, err := h.store.GetComputerTakeoverAvailability(ctx, wrong, computer.ComputerID); errorCode(err) != contract.ErrorPersonDeviceMismatch {
				t.Fatalf("bare-context discovery bypassed device binding: %v", err)
			}
		})
		t.Run("admin write", func(t *testing.T) {
			if _, err := h.store.RemoveAdmin(ctx, wrong, owner.UserID, policy.Revision); errorCode(err) != contract.ErrorPersonDeviceMismatch {
				t.Fatalf("bare-context admin write bypassed device binding: %v", err)
			}
		})
		t.Run("add requires subject device", func(t *testing.T) {
			if _, err := h.store.AddAdmin(ctx, owner, "new-admin", policy.Revision, ""); errorCode(err) != contract.ErrorInvalidRequest {
				t.Fatalf("bare-context add accepted absent subject device: %v", err)
			}
		})
	}
	t.Run("authenticated", check)
	// Reusing a Store with weaker trust must never turn off its requirement.
	for _, mode := range []struct {
		name string
		f    fabric.Fabric
	}{
		{"authenticated again", f},
		{"self asserted", &selfAssertedPersonAnswerFabric{f}},
		{"plain", h.server.fabric},
	} {
		t.Run(mode.name, func(t *testing.T) {
			if _, err := NewServer(mode.f, h.store, ServerConfig{AllowSelfAssertedPersonIdentities: true}); err != nil {
				t.Fatal(err)
			}
			check(t)
		})
	}
}

// A read model built without the Store's latch must refuse an administrator
// decision instead of silently skipping the device binding.
func TestUnwiredPersonDeviceBindingFailsClosed(t *testing.T) {
	var unwired *personDeviceBinding
	current, err := unwired.currentAdminPerson(context.Background(), nil, fabric.Identity{FabricID: "issuer", UserID: "owner", DeviceID: "other-device"})
	if err == nil || current {
		t.Fatalf("unwired binding: current=%t err=%v", current, err)
	}
	if err := unwired.requireCurrentAdmin(context.Background(), nil, fabric.Identity{FabricID: "issuer", UserID: "owner", DeviceID: "other-device"}); err == nil {
		t.Fatal("unwired binding allowed an administrator decision")
	}
}
