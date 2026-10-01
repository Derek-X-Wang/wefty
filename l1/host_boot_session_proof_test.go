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
	})
	ctx := context.Background()
	register := func(bootSessionID string) {
		t.Helper()
		_, err := h.store.RegisterNode(ctx, fabric.Identity{NodeID: "fabric-node"}, contract.NodeRegistration{
			NodeID: "stable-node", BootSessionID: bootSessionID, RootInstanceID: "root-" + bootSessionID,
			OS: "linux", Architecture: "amd64", AgentVersion: "test", Capabilities: map[string]bool{"kind:process": true},
			CapabilityRevision: 1, CapabilityObservedAt: h.clock.Now(), MissingCapabilities: []string{},
		}, DefaultNodePolicy(), true)
		if err != nil {
			t.Fatal(err)
		}
	}

	register("boot-a")
	for name, claim := range map[string][2]string{
		"blank identity":   {"", "boot-a"},
		"padded identity":  {" fabric-node", "boot-a"},
		"blank boot":       {"fabric-node", ""},
		"padded boot":      {"fabric-node", "boot-a "},
		"oversized boot":   {"fabric-node", strings.Repeat("b", 256)},
		"oversized identity": {strings.Repeat("n", 256), "boot-a"},
	} {
		if err := h.store.ProveHostBootSession(ctx, claim[0], claim[1]); errorCode(err) != contract.ErrorInvalidRequest {
			t.Fatalf("%s proof = %v, want invalid_request", name, err)
		}
	}
	if err := h.store.ProveHostBootSession(ctx, "fabric-node", "boot-a"); err != nil {
		t.Fatalf("prove current boot A: %v", err)
	}
	for name, claim := range map[string][2]string{
		"wrong boot":     {"fabric-node", "boot-b"},
		"wrong identity": {"foreign-node", "boot-a"},
	} {
		if err := h.store.ProveHostBootSession(ctx, claim[0], claim[1]); errorCode(err) != contract.ErrorForbidden {
			t.Fatalf("%s proof = %v, want forbidden", name, err)
		}
	}

	register("boot-b")
	if err := h.store.ProveHostBootSession(ctx, "fabric-node", "boot-a"); errorCode(err) != contract.ErrorForbidden {
		t.Fatalf("replaced boot proof = %v, want forbidden", err)
	}
	if err := h.store.ProveHostBootSession(ctx, "fabric-node", "boot-b"); err != nil {
		t.Fatalf("prove current boot B: %v", err)
	}

	ledger := h.client(fabric.Identity{NodeID: "run-ledger", Tags: []string{DefaultClientPrincipalTag}})
	status, _, body := h.do(ledger, http.MethodPost, "/v1/host-boot-session-proof", HostBootSessionProof{
		HostIdentityNodeID: "fabric-node", BootSessionID: "boot-b",
	})
	if status != http.StatusOK {
		t.Fatalf("ledger proof status=%d body=%s", status, body)
	}
	var proof HostBootSessionProof
	if err := json.Unmarshal(body, &proof); err != nil {
		t.Fatal(err)
	}
	if proof.HostIdentityNodeID != "fabric-node" || proof.BootSessionID != "boot-b" {
		t.Fatalf("proof = %#v", proof)
	}

	operator := h.client(fabric.Identity{NodeID: "operator", Tags: []string{DefaultClientPrincipalTag}})
	status, _, body = h.do(operator, http.MethodPost, "/v1/host-boot-session-proof", proof)
	assertAPIError(t, status, body, http.StatusForbidden, contract.ErrorForbidden)
}
