package agent

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	"github.com/coder/websocket"
)

func TestComputerServicePublishesOnlyFabricFrontDoorAndAdmissionDialsView(t *testing.T) {
	identity := fabric.Identity{FabricID: "fabric-one", UserID: "person-a", DeviceID: "device-a"}
	privateFabric := &recordingComputerServiceFabric{identity: identity}
	now := time.Now().UTC()
	cache := NewComputerPolicyCache(systemClock{}, "node-1", "boot-1")
	defer cache.Close()
	if _, err := cache.Install(policySnapshot(t, now, 1, 1, nil, l1.ComputerGrant{
		FabricID: identity.FabricID, UserID: identity.UserID, Permission: l1.ComputerGrantControl, PolicyRevision: 1,
	})); err != nil {
		t.Fatal(err)
	}
	backend := newComputerBackend(t, computerBackendOptions{})
	defer backend.Close()
	var dialMu sync.Mutex
	dials := map[string]int{}
	dial := func(ctx context.Context, name string) (net.Conn, error) {
		dialMu.Lock()
		dials[name]++
		dialMu.Unlock()
		return backend.dial(ctx)
	}
	type publication struct {
		ready    bool
		endpoint string
	}
	publications := make(chan publication, 4)
	ctx, cancel := context.WithCancel(t.Context())
	runtime := &opaqueEndpointRuntime{release: make(chan struct{})}
	result := make(chan error, 1)
	go func() {
		_, err := runComputerService(ctx, runtime, workloadrunner.Request{}, nil, computerServiceConfig{
			clock: systemClock{}, fabric: privateFabric, authorizer: cache, auditor: &recordingComputerAuditor{},
			computerID: "computer-1", jobID: "job-1", attemptID: "attempt-1", fencingToken: "fence-1", dial: dial,
			publish: func(_ context.Context, ready bool, endpoint string) error {
				publications <- publication{ready: ready, endpoint: endpoint}
				return nil
			},
		})
		result <- err
	}()
	var published publication
	select {
	case published = <-publications:
	case <-time.After(5 * time.Second):
		t.Fatal("Computer front door was not published")
	}
	if !published.ready || published.endpoint == "" || privateFabric.listenNetwork != "tcp" || privateFabric.listenAddress != ":0" {
		t.Fatalf("private publication=%#v listen=%q %q", published, privateFabric.listenNetwork, privateFabric.listenAddress)
	}
	connection, _, err := websocket.Dial(t.Context(), published.endpoint, &websocket.DialOptions{Subprotocols: []string{computerWebSocketSubprotocol}})
	if err != nil {
		t.Fatal(err)
	}
	if kind, banner, err := connection.Read(t.Context()); err != nil || kind != websocket.MessageBinary || string(banner) != "RFB 003.008\n" {
		t.Fatalf("front-door banner=%q kind=%v err=%v", banner, kind, err)
	}
	_ = connection.CloseNow()
	cancel()
	select {
	case <-result:
	case <-time.After(5 * time.Second):
		t.Fatal("Computer service did not stop")
	}
	select {
	case withdrawn := <-publications:
		if withdrawn.ready {
			t.Fatalf("stop republished ready: %#v", withdrawn)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Computer stop did not withdraw publication")
	}
	dialMu.Lock()
	defer dialMu.Unlock()
	if dials[workloadrunner.AttemptEndpointView] != dials[workloadrunner.AttemptEndpointControl]+1 {
		t.Fatalf("endpoint dials=%v, want exactly one admission-only view dial", dials)
	}
}

type recordingComputerServiceFabric struct {
	identity                     fabric.Identity
	listenNetwork, listenAddress string
}

func (value *recordingComputerServiceFabric) Listen(network, address string) (net.Listener, error) {
	value.listenNetwork, value.listenAddress = network, address
	if network != "tcp" || address != ":0" {
		return nil, errors.New("Computer listener escaped the private Fabric wildcard contract")
	}
	return net.Listen("tcp4", "127.0.0.1:0")
}

func (*recordingComputerServiceFabric) Dial(context.Context, string, string) (net.Conn, error) {
	return nil, errors.New("unused")
}
func (value *recordingComputerServiceFabric) WhoIs(context.Context, string) (fabric.Identity, error) {
	return value.identity, nil
}
func (*recordingComputerServiceFabric) ConnectHost() string { return "127.0.0.1" }

var _ WorkloadRuntime = (*opaqueEndpointRuntime)(nil)
