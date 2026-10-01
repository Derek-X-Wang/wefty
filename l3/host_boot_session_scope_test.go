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

func TestRevokeHostProvesCurrentBootWithL1(t *testing.T) {
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

	register := func(bootSessionID string) {
		t.Helper()
		_, err := l1Store.RegisterNode(ctx, fabric.Identity{NodeID: "host-fabric"}, contract.NodeRegistration{
			NodeID: "stable-node", BootSessionID: bootSessionID, RootInstanceID: "root-" + bootSessionID,
			OS: "linux", Architecture: "amd64", AgentVersion: "test", Capabilities: map[string]bool{"kind:process": true},
			CapabilityRevision: 1, CapabilityObservedAt: time.Now(), MissingCapabilities: []string{},
		}, l1.DefaultNodePolicy(), true)
		if err != nil {
			t.Fatal(err)
		}
	}
	register("boot-a")
	proof := testComputerScope()
	proof.HostNodeID = "host-fabric"
	proof.HostBootSessionID = "boot-a"
	grant, err := l3Store.MintComputerToken(ctx, proof)
	if err != nil {
		t.Fatal(err)
	}
	register("boot-b")

	host := network.NewFabric(fabric.Identity{NodeID: "host-fabric"})
	httpClient := &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
		return host.Dial(ctx, network, DefaultL3Address)
	}}}
	t.Cleanup(httpClient.CloseIdleConnections)
	status, _, body := doComputerHTTP(t, httpClient, http.MethodPost, "/v1/computer-token/revoke-host", "", "",
		HostComputerTokenRevocationRequest{Reason: "agent_restart", BootSessionID: "boot-a"})
	assertAPIError(t, status, body, http.StatusForbidden, contract.ErrorForbidden)
	if _, err := l3Store.AuthenticateComputerToken(ctx, grant.Token); err != nil {
		t.Fatalf("replaced boot claim changed the grant: %v", err)
	}

	status, _, body = doComputerHTTP(t, httpClient, http.MethodPost, "/v1/computer-token/revoke-host", "", "",
		HostComputerTokenRevocationRequest{Reason: "agent_restart", BootSessionID: "boot-b"})
	if status != http.StatusNoContent {
		t.Fatalf("current boot revoke-host status=%d body=%s", status, body)
	}
	if _, err := l3Store.AuthenticateComputerToken(ctx, grant.Token); err == nil {
		t.Fatal("current boot's revoke-host left the earlier boot grant active")
	}
}
