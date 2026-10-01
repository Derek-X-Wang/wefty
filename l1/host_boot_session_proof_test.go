package l1

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

func TestHostBootSessionProofAcceptsOnlyCurrentRegistration(t *testing.T) {
	h := newIntegrationHarnessWithPolicies(t, map[string]NodePolicy{
		"stable-node": DefaultNodePolicy(),
		"node-old":    DefaultNodePolicy(),
		"node-new":    DefaultNodePolicy(),
	})
	ctx := context.Background()
	register := func(stableNodeID, bootSessionID string) {
		t.Helper()
		_, err := h.store.RegisterNode(ctx, fabric.Identity{NodeID: "fabric-node"}, contract.NodeRegistration{
			NodeID: stableNodeID, BootSessionID: bootSessionID, RootInstanceID: "root-" + bootSessionID,
			OS: "linux", Architecture: "amd64", AgentVersion: "test", Capabilities: map[string]bool{"kind:process": true},
			CapabilityRevision: 1, CapabilityObservedAt: h.clock.Now(), MissingCapabilities: []string{},
		}, DefaultNodePolicy(), true)
		if err != nil {
			t.Fatal(err)
		}
	}

	register("stable-node", "boot-a")
	for name, claim := range map[string][3]string{
		"blank identity":       {"", "stable-node", "boot-a"},
		"padded identity":      {" fabric-node", "stable-node", "boot-a"},
		"blank stable node":    {"fabric-node", "", "boot-a"},
		"padded stable node":   {"fabric-node", "stable-node ", "boot-a"},
		"blank boot":           {"fabric-node", "stable-node", ""},
		"padded boot":          {"fabric-node", "stable-node", "boot-a "},
		"oversized boot":       {"fabric-node", "stable-node", strings.Repeat("b", 256)},
		"oversized identity":   {strings.Repeat("n", 256), "stable-node", "boot-a"},
		"oversized stable one": {"fabric-node", strings.Repeat("s", 256), "boot-a"},
	} {
		if err := h.store.ProveHostBootSession(ctx, claim[0], claim[1], claim[2]); errorCode(err) != contract.ErrorInvalidRequest {
			t.Fatalf("%s proof = %v, want invalid_request", name, err)
		}
	}
	if err := h.store.ProveHostBootSession(ctx, "fabric-node", "stable-node", "boot-a"); err != nil {
		t.Fatalf("prove current boot A: %v", err)
	}
	for name, claim := range map[string][3]string{
		"wrong boot":        {"fabric-node", "stable-node", "boot-b"},
		"wrong identity":    {"foreign-node", "stable-node", "boot-a"},
		"wrong stable node": {"fabric-node", "other-node", "boot-a"},
	} {
		if err := h.store.ProveHostBootSession(ctx, claim[0], claim[1], claim[2]); errorCode(err) != contract.ErrorForbidden {
			t.Fatalf("%s proof = %v, want forbidden", name, err)
		}
	}

	register("stable-node", "boot-b")
	if err := h.store.ProveHostBootSession(ctx, "fabric-node", "stable-node", "boot-a"); errorCode(err) != contract.ErrorForbidden {
		t.Fatalf("replaced boot proof = %v, want forbidden", err)
	}
	if err := h.store.ProveHostBootSession(ctx, "fabric-node", "stable-node", "boot-b"); err != nil {
		t.Fatalf("prove current boot B: %v", err)
	}

	ledger := h.client(fabric.Identity{NodeID: "run-ledger", Tags: []string{DefaultClientPrincipalTag}})
	status, _, body := h.do(ledger, http.MethodPost, "/v1/host-boot-session-proof", HostBootSessionProof{
		HostIdentityNodeID: "fabric-node", HostStableNodeID: "stable-node", BootSessionID: "boot-b",
	})
	if status != http.StatusOK {
		t.Fatalf("ledger proof status=%d body=%s", status, body)
	}
	var proof HostBootSessionProof
	if err := json.Unmarshal(body, &proof); err != nil {
		t.Fatal(err)
	}
	if proof.HostIdentityNodeID != "fabric-node" || proof.HostStableNodeID != "stable-node" || proof.BootSessionID != "boot-b" {
		t.Fatalf("proof = %#v", proof)
	}

	operator := h.client(fabric.Identity{NodeID: "operator", Tags: []string{DefaultClientPrincipalTag}})
	status, _, body = h.do(operator, http.MethodPost, "/v1/host-boot-session-proof", proof)
	assertAPIError(t, status, body, http.StatusForbidden, contract.ErrorForbidden)
}

// One Fabric identity may register several stable nodes. A boot is proved
// only against its own stable node's current registration: node-old's boot
// is never proof for node-new, and node-new's boot never proves node-old.
func TestHostBootSessionProofIsBoundToOneStableNode(t *testing.T) {
	h := newIntegrationHarnessWithPolicies(t, map[string]NodePolicy{
		"node-old": DefaultNodePolicy(),
		"node-new": DefaultNodePolicy(),
	})
	ctx := context.Background()
	for stableNodeID, bootSessionID := range map[string]string{"node-old": "boot-a", "node-new": "boot-b"} {
		_, err := h.store.RegisterNode(ctx, fabric.Identity{NodeID: "fabric-node"}, contract.NodeRegistration{
			NodeID: stableNodeID, BootSessionID: bootSessionID, RootInstanceID: "root-" + bootSessionID,
			OS: "linux", Architecture: "amd64", AgentVersion: "test", Capabilities: map[string]bool{"kind:process": true},
			CapabilityRevision: 1, CapabilityObservedAt: h.clock.Now(), MissingCapabilities: []string{},
		}, DefaultNodePolicy(), true)
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, claim := range [][2]string{{"node-old", "boot-a"}, {"node-new", "boot-b"}} {
		if err := h.store.ProveHostBootSession(ctx, "fabric-node", claim[0], claim[1]); err != nil {
			t.Fatalf("prove %s/%s: %v", claim[0], claim[1], err)
		}
	}
	for _, claim := range [][2]string{{"node-new", "boot-a"}, {"node-old", "boot-b"}} {
		if err := h.store.ProveHostBootSession(ctx, "fabric-node", claim[0], claim[1]); errorCode(err) != contract.ErrorForbidden {
			t.Fatalf("cross-node proof %s/%s = %v, want forbidden", claim[0], claim[1], err)
		}
	}
}
