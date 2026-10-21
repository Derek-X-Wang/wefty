package main

import (
	"context"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric/local"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
)

func TestSetupSmokeRunsPinnedOneShotThroughExistingClients(t *testing.T) {
	home := newFleetTestHome(t)
	stack := newExecStack(t, 0)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	server, err := local.NewServer(local.DefaultSocketPath(home), map[string]local.Participant{local.Client: {NodeID: "operator", Fabric: stack.clients.fabric, Check: func(probe context.Context) error { return probe.Err() }}})
	if err != nil {
		t.Fatal(err)
	}
	done := serveTestServer(ctx, func() error { return server.Serve(ctx) })
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("client service: %v", err)
		}
	})
	p := connprofile.Profile{L1Address: stack.l1Address, L3Address: stack.l3Address, Participants: map[string]connprofile.Participant{connprofile.ParticipantClient: {NodeID: "operator"}}}
	var clients *apiClients
	for {
		clients, err = setupFleetClients(ctx, setupPaths{home: home}, p)
		if err == nil {
			break
		}
		if waitForContext(ctx, 20*time.Millisecond) != nil {
			t.Fatalf("local client service startup: %v", err)
		}
	}
	defer clients.close()
	clients.l3.client.Transport = execTransport{base: clients.l3.client.Transport, stack: stack}
	id, err := runSetupSmoke(ctx, clients, "node-cli")
	if err != nil {
		t.Fatalf("setup smoke: %s %v", id, err)
	}
	run, err := clients.getRun(ctx, id)
	if err != nil || run.Status != contract.RunSucceeded || run.NodeID != "node-cli" {
		t.Fatalf("smoke result: %+v %v", run, err)
	}
	if len(stack.requests) != 1 || len(stack.requests[0].Tags) != 1 || stack.requests[0].Tags[0] != contract.StableNodeTagPrefix+"node-cli" || stack.requests[0].Limits.MaxRuntimeSeconds != 30 {
		t.Fatalf("smoke request: %+v", stack.requests)
	}
}
