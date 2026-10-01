package l3

import (
	"context"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
)

// hostRevocationHarness serves a real L1 and a real L3 over one plain Fabric
// network, so revoke-host is proved by L1's own registration rows.
type hostRevocationHarness struct {
	network *plain.Network
	l1Store *l1.Store
	l3Store *Store
}

func newHostRevocationHarness(t *testing.T) *hostRevocationHarness {
	t.Helper()
	ctx := context.Background()
	network := plain.NewNetwork()
	control := network.NewFabric(fabric.Identity{NodeID: "control-plane"})
	ledger := network.NewFabric(fabric.Identity{NodeID: "run-ledger", Tags: []string{l1.DefaultClientPrincipalTag}})

	l1Store, err := l1.OpenStore(filepath.Join(t.TempDir(), "l1.sqlite"), l1.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	l1Server, err := l1.NewServer(control, l1Store, l1.ServerConfig{RunLedgerNodeID: "run-ledger"})
	if err != nil {
		t.Fatal(err)
	}
	l1Listener, err := control.Listen("tcp", DefaultL1Address)
	if err != nil {
		t.Fatal(err)
	}
	l1Context, stopL1 := context.WithCancel(ctx)
	l1Done := make(chan error, 1)
	go func() { l1Done <- l1Server.Serve(l1Context, l1Listener) }()
	t.Cleanup(func() {
		stopL1()
		if err := <-l1Done; err != nil {
			t.Errorf("serve L1: %v", err)
		}
		if err := l1Store.Close(); err != nil {
			t.Errorf("close L1 store: %v", err)
		}
	})

	l1Client, err := NewL1Client(ledger, DefaultL1Address)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(l1Client.CloseIdleConnections)
	l3Store, err := OpenStore(filepath.Join(t.TempDir(), "l3.sqlite"), StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	l3Server, err := NewServer(ledger, l3Store, ServerConfig{
		Jobs: l1Client, Logs: emptyComputerJobLogs{}, HostBootSessions: l1Client,
	})
	if err != nil {
		t.Fatal(err)
	}
	l3Listener, err := ledger.Listen("tcp", DefaultL3Address)
	if err != nil {
		t.Fatal(err)
	}
	l3Context, stopL3 := context.WithCancel(ctx)
	l3Done := make(chan error, 1)
	go func() { l3Done <- l3Server.Serve(l3Context, l3Listener) }()
	t.Cleanup(func() {
		stopL3()
		if err := <-l3Done; err != nil {
			t.Errorf("serve L3: %v", err)
		}
		if err := l3Store.Close(); err != nil {
			t.Errorf("close L3 store: %v", err)
		}
	})
	return &hostRevocationHarness{network: network, l1Store: l1Store, l3Store: l3Store}
}

func (h *hostRevocationHarness) register(t *testing.T, fabricNodeID, stableNodeID, bootSessionID string) {
	t.Helper()
	_, err := h.l1Store.RegisterNode(context.Background(), fabric.Identity{NodeID: fabricNodeID}, contract.NodeRegistration{
		NodeID: stableNodeID, BootSessionID: bootSessionID, RootInstanceID: "root-" + bootSessionID,
		OS: "linux", Architecture: "amd64", AgentVersion: "test", Capabilities: map[string]bool{"kind:process": true},
		CapabilityRevision: 1, CapabilityObservedAt: time.Now(), MissingCapabilities: []string{},
	}, l1.DefaultNodePolicy(), true)
	if err != nil {
		t.Fatal(err)
	}
}

func (h *hostRevocationHarness) mint(t *testing.T, computerID, fabricNodeID, stableNodeID, bootSessionID string) ComputerTokenGrant {
	t.Helper()
	proof := testComputerScope()
	proof.ComputerID = computerID
	proof.ComputerAttemptID = "attempt-" + computerID
	proof.HostNodeID = fabricNodeID
	proof.HostStableNodeID = stableNodeID
	proof.HostBootSessionID = bootSessionID
	grant, err := h.l3Store.MintComputerToken(context.Background(), proof)
	if err != nil {
		t.Fatal(err)
	}
	return grant
}

func (h *hostRevocationHarness) revokeHost(t *testing.T, fabricNodeID, stableNodeID, bootSessionID string) (int, []byte) {
	t.Helper()
	host := h.network.NewFabric(fabric.Identity{NodeID: fabricNodeID})
	client := &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
		return host.Dial(ctx, network, DefaultL3Address)
	}}}
	defer client.CloseIdleConnections()
	status, _, body := doComputerHTTP(t, client, http.MethodPost, "/v1/computer-token/revoke-host", "", "",
		HostComputerTokenRevocationRequest{Reason: "agent_restart", StableNodeID: stableNodeID, BootSessionID: bootSessionID})
	return status, body
}

func (h *hostRevocationHarness) active(grant ComputerTokenGrant) bool {
	_, err := h.l3Store.AuthenticateComputerToken(context.Background(), grant.Token)
	return err == nil
}

func TestRevokeHostProvesCurrentBootWithL1(t *testing.T) {
	h := newHostRevocationHarness(t)
	h.register(t, "host-fabric", "stable-node", "boot-a")
	grant := h.mint(t, "computer-1", "host-fabric", "stable-node", "boot-a")
	h.register(t, "host-fabric", "stable-node", "boot-b")

	status, body := h.revokeHost(t, "host-fabric", "stable-node", "boot-a")
	assertAPIError(t, status, body, http.StatusForbidden, contract.ErrorForbidden)
	if !h.active(grant) {
		t.Fatal("replaced boot claim changed the grant")
	}

	if status, body := h.revokeHost(t, "host-fabric", "stable-node", "boot-b"); status != http.StatusNoContent {
		t.Fatalf("current boot revoke-host status=%d body=%s", status, body)
	}
	if h.active(grant) {
		t.Fatal("current boot's revoke-host left the earlier boot grant active")
	}
}

// One Fabric identity may hold several stable node registrations. A delayed
// revoke-host from node-old's boot must never end grants that node-new's
// current boot minted, even though node-old's boot is still current for
// node-old and the two boots differ. It still ends node-old's own earlier-boot
// grants, and once node-old boots again the stale claim is refused outright.
func TestDelayedRevokeHostFromAnotherStableNodeKeepsItsGrants(t *testing.T) {
	h := newHostRevocationHarness(t)
	h.register(t, "host-fabric", "node-old", "boot-a0")
	oldPriorBootGrant := h.mint(t, "computer-old", "host-fabric", "node-old", "boot-a0")
	h.register(t, "host-fabric", "node-old", "boot-a")
	h.register(t, "host-fabric", "node-new", "boot-b")
	newBootGrant := h.mint(t, "computer-new", "host-fabric", "node-new", "boot-b")

	if status, body := h.revokeHost(t, "host-fabric", "node-old", "boot-a"); status != http.StatusNoContent {
		t.Fatalf("node-old revoke-host status=%d body=%s", status, body)
	}
	if !h.active(newBootGrant) {
		t.Fatal("a delayed node-old revoke-host ended node-new's current-boot grant")
	}
	if h.active(oldPriorBootGrant) {
		t.Fatal("node-old revoke-host left node-old's earlier-boot grant active")
	}

	// node-old's boot is not proof for node-new.
	status, body := h.revokeHost(t, "host-fabric", "node-new", "boot-a")
	assertAPIError(t, status, body, http.StatusForbidden, contract.ErrorForbidden)

	// node-old boots again: boot A's duplicated request is refused.
	h.register(t, "host-fabric", "node-old", "boot-a2")
	status, body = h.revokeHost(t, "host-fabric", "node-old", "boot-a")
	assertAPIError(t, status, body, http.StatusForbidden, contract.ErrorForbidden)
	if !h.active(newBootGrant) {
		t.Fatal("a refused stale node-old revoke-host changed node-new's grant")
	}
}
