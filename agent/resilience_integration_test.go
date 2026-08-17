//go:build darwin || linux

package agent

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestAgentReregistersAfterControlPlaneReturns(t *testing.T) {
	assertAgentReregistersAfterControlPlaneReturns(t)
}

func assertAgentReregistersAfterControlPlaneReturns(t *testing.T) {
	network := plain.NewNetwork()
	serverFabric := network.NewFabric(fabric.Identity{NodeID: "control-plane"})
	store, err := l1.OpenStore(filepath.Join(t.TempDir(), "reregister.sqlite"), l1.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	l1Server, err := l1.NewServer(serverFabric, store, l1.ServerConfig{NodePolicies: map[string]l1.NodePolicy{
		"node-1": l1.DefaultNodePolicy("linux"),
	}})
	if err != nil {
		t.Fatal(err)
	}
	agentFabric := network.NewFabric(fabric.Identity{NodeID: "fabric-node", Tags: []string{l1.DefaultAgentPrincipalTag}})
	nodeAgent, err := New(Config{
		Fabric: agentFabric, ControlPlaneAddress: "wefty://control-plane",
		NodeID: "node-1", BootSessionID: "boot-1", Version: "test",
		HeartbeatInterval: time.Second, ClaimInterval: 10 * time.Millisecond,
		LogSpoolDirectory: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer nodeAgent.Close()
	if status := nodeAgent.Status(); status.State != LifecycleRegistering {
		t.Fatalf("new agent lifecycle = %q, want registering", status.State)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- nodeAgent.Run(ctx) }()
	waitForAgentLifecycle(t, nodeAgent, LifecycleRejoining)
	rejoining := nodeAgent.Status()
	if rejoining.SessionBackoff <= 0 {
		cancel()
		t.Fatalf("rejoining session backoff = %s, want positive", rejoining.SessionBackoff)
	}
	listener, err := serverFabric.Listen("tcp", "wefty://control-plane")
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	serverContext, stopServer := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- l1Server.Serve(serverContext, listener) }()
	defer func() {
		stopServer()
		if err := <-served; err != nil {
			t.Errorf("serve recovered control plane: %v", err)
		}
	}()
	waitForAgentLifecycle(t, nodeAgent, LifecycleReady)
	nodes, err := store.ListNodes(context.Background())
	if err != nil || len(nodes) != 1 || nodes[0].BootSessionID != "boot-1" {
		cancel()
		t.Fatalf("registered nodes = %#v error %v", nodes, err)
	}
	select {
	case err := <-done:
		cancel()
		t.Fatalf("agent exited after successful re-registration: %v", err)
	default:
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("agent Run() after cancellation = %v", err)
	}
}

func TestAgentQuarantineIsObservableAndNonTerminal(t *testing.T) {
	assertAgentQuarantineIsObservableAndNonTerminal(t)
}

func assertAgentQuarantineIsObservableAndNonTerminal(t *testing.T) {
	network := plain.NewNetwork()
	_, stopServer := startFailureServer(t, network, nil, map[string][]string{"node-1": {"linux"}})
	defer stopServer()
	agentFabric := network.NewFabric(fabric.Identity{NodeID: "fabric-node"})
	nodeAgent, err := New(Config{
		Fabric: agentFabric, ControlPlaneAddress: "wefty://control-plane",
		NodeID: "node-1", BootSessionID: "boot-1", Version: "test",
		LogSpoolDirectory: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer nodeAgent.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- nodeAgent.Run(ctx) }()
	waitForAgentLifecycle(t, nodeAgent, LifecycleQuarantined)
	status := nodeAgent.Status()
	if status.SessionBackoff != DefaultSessionBackoffMax {
		cancel()
		t.Fatalf("quarantine backoff = %s, want %s", status.SessionBackoff, DefaultSessionBackoffMax)
	}
	if status.LastSemanticError == nil || status.LastSemanticError.Code != contract.ErrorPrincipalForbidden {
		cancel()
		t.Fatalf("last semantic error = %#v, want principal_forbidden", status.LastSemanticError)
	}
	if len(status.Attempts) != 0 || status.OneShot.Occupied != 0 || status.OneShot.Limit != prefactorClassLimit {
		cancel()
		t.Fatalf("quarantined occupancy = attempts %#v one-shot %#v", status.Attempts, status.OneShot)
	}
	select {
	case err := <-done:
		cancel()
		t.Fatalf("quarantined agent exited: %v", err)
	default:
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("quarantined agent Run() after cancellation = %v", err)
	}
}

func waitForAgentLifecycle(t *testing.T, nodeAgent *Agent, want LifecycleState) Status {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		status := nodeAgent.Status()
		if status.State == want {
			return status
		}
		select {
		case <-time.After(time.Millisecond):
		case <-t.Context().Done():
			t.Fatal(t.Context().Err())
		}
	}
	status := nodeAgent.Status()
	t.Fatalf("agent lifecycle = %q, want %q; last semantic error %#v", status.State, want, status.LastSemanticError)
	return status
}

func assertAttemptStatus(t *testing.T, nodeAgent *Agent, state AttemptLifecycleState) AttemptStatus {
	t.Helper()
	status := nodeAgent.Status()
	for _, attempt := range status.Attempts {
		if attempt.State == state {
			return attempt
		}
	}
	t.Fatalf("attempt states = %#v, want %q", status.Attempts, state)
	return AttemptStatus{}
}
