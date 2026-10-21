package agent

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestImmediateHeartbeatUsesExistingLoopBeforeFifteenSecondTick(t *testing.T) {
	network := plain.NewNetwork()
	serverFabric := network.NewFabric(fabric.Identity{NodeID: "control-plane"})
	listener, err := serverFabric.Listen("tcp", "wefty://control-plane")
	if err != nil {
		t.Fatal(err)
	}
	observed := make(chan time.Time, 1)
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/agent/nodes/node/heartbeat" {
			t.Errorf("unexpected API: %s", r.URL.Path)
			w.WriteHeader(404)
			return
		}
		observed <- time.Now()
		_ = json.NewEncoder(w).Encode(l1.HeartbeatResponse{Node: l1.Node{NodeRegistration: contract.NodeRegistration{NodeID: "node", BootSessionID: "boot"}, State: contract.NodeAlive, ClaimsEnabled: true, MaxOneshotSlots: 4, MaxServiceSlots: 2}})
	})}
	go server.Serve(listener)
	t.Cleanup(func() { server.Close() })
	client, err := newClient(network.NewFabric(fabric.Identity{NodeID: "agent"}), "wefty://control-plane", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	clock := systemClock{}
	registration := contract.NodeRegistration{NodeID: "node", BootSessionID: "boot", AgentVersion: "test"}
	session := newAgentSession(client, registration, newCapabilityState(nil, nil, clock, 0, nil), 15*time.Second, time.Second, clock, newLifecycleObserver(clock), nil, 4, 2)
	a := &Agent{session: session}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); session.heartbeatLoop(ctx, make(chan destinationError, 1)) }()
	started := time.Now()
	a.RequestHeartbeat()
	select {
	case at := <-observed:
		if at.Sub(started) >= time.Second {
			t.Fatal("heartbeat waited for phase")
		}
	case <-time.After(time.Second):
		t.Fatal("immediate heartbeat waited for 15 s tick")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("heartbeat loop did not stop")
	}
}
