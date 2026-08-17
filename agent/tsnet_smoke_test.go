//go:build tsnet_smoke

package agent

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/tsnet"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestTSNetSmoke(t *testing.T) {
	authKey := os.Getenv("TS_AUTHKEY")
	if authKey == "" {
		t.Skip("TS_AUTHKEY is not available")
	}
	controlURL := os.Getenv("TS_CONTROL_URL")
	agentPrincipalTag := os.Getenv("TS_AGENT_PRINCIPAL_TAG")
	if agentPrincipalTag == "" {
		agentPrincipalTag = l1.DefaultAgentPrincipalTag
	}
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	serverName := "wefty://node/agent-smoke-server-" + suffix
	agentName := "wefty://node/agent-smoke-client-" + suffix
	stableNodeID := "agent-smoke-" + suffix

	serverFabric, err := tsnet.New(tsnet.Config{
		Name: serverName, StateDir: t.TempDir(), Credential: fabric.Credential{Value: authKey},
		Ephemeral: true, CoordinatorURL: controlURL, Logf: t.Logf,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer serverFabric.Close()
	agentFabric, err := tsnet.New(tsnet.Config{
		Name: agentName, StateDir: t.TempDir(), Credential: fabric.Credential{Value: authKey},
		Ephemeral: true, CoordinatorURL: controlURL, Logf: t.Logf,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer agentFabric.Close()
	store, err := l1.OpenStore(filepath.Join(t.TempDir(), "l1.sqlite"), l1.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	server, err := l1.NewServer(serverFabric, store, l1.ServerConfig{
		AgentPrincipalTag: agentPrincipalTag,
		NodePolicies: map[string]l1.NodePolicy{
			stableNodeID: l1.DefaultNodePolicy("tsnet-smoke"),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := serverFabric.Listen("tcp", serverName)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	served := make(chan error, 1)
	go func() { served <- server.Serve(ctx, listener) }()

	nodeAgent, err := New(Config{
		Fabric: agentFabric, ControlPlaneAddress: serverName,
		NodeID: stableNodeID, BootSessionID: "boot-" + suffix, Version: "tsnet-smoke",
		LogSpoolDirectory: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer nodeAgent.Close()
	node, err := nodeAgent.Register(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if node.NodeID != stableNodeID || node.BootSessionID != "boot-"+suffix {
		t.Fatalf("registered node = %#v", node)
	}
	cancel()
	if err := <-served; err != nil {
		t.Fatal(err)
	}
}
