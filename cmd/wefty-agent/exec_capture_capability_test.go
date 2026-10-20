package main

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/agent"
	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestDefaultAgentAdvertisesExecCaptureBundle(t *testing.T) {
	network := plain.NewNetwork()
	control := network.NewFabric(fabric.Identity{NodeID: "control-plane"})
	participant := network.NewFabric(fabric.Identity{NodeID: "bundle-agent", Tags: []string{l1.DefaultAgentPrincipalTag}})
	store, e := l1.OpenStore(filepath.Join(t.TempDir(), "l1.sqlite"), l1.StoreOptions{})
	if e != nil {
		t.Fatal(e)
	}
	defer store.Close()
	server, e := l1.NewServer(control, store, l1.ServerConfig{NodePolicies: map[string]l1.NodePolicy{"bundle-node": l1.DefaultNodePolicy("linux")}})
	if e != nil {
		t.Fatal(e)
	}
	address := "wefty://control-plane"
	listener, e := control.Listen("tcp", address)
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	served := make(chan error, 1)
	go func() { served <- server.Serve(ctx, listener) }()
	defer func() { cancel(); <-served }()
	root, e := filepath.EvalSymlinks(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	node, e := agent.New(agent.Config{Fabric: participant, ControlPlaneAddress: address, NodeID: "bundle-node", BootSessionID: "bundle-boot", Version: "test", OS: "linux", Architecture: "amd64", Capabilities: defaultAgentCapabilities(), ManagedRootDirectory: root, LogSpoolDirectory: t.TempDir(), HandoffRoot: t.TempDir(), HeartbeatInterval: 20 * time.Millisecond, ClaimInterval: 20 * time.Millisecond})
	if e != nil {
		t.Fatal(e)
	}
	defer node.Close()
	done := make(chan error, 1)
	go func() { done <- node.Run(ctx) }()
	defer func() { cancel(); <-done }()
	deadline := time.Now().Add(3 * time.Second)
	for {
		record, e := store.GetNode(ctx, "bundle-node")
		if e == nil {
			if !record.Capabilities[contract.CapabilityExecCaptureBundle] {
				t.Fatal("node-agent command omitted bundle capability from its L1 advertisement")
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("node did not advertise: %v", e)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
