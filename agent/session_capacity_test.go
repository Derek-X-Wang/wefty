package agent

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestAgentResizesPoolFromHeartbeatGrantedCapacity(t *testing.T) {
	assertAgentResizesPoolFromHeartbeatGrantedCapacity(t)
}

func assertAgentResizesPoolFromHeartbeatGrantedCapacity(t *testing.T) {
	t.Helper()
	network := plain.NewNetwork()
	serverFabric := network.NewFabric(fabric.Identity{NodeID: "control-plane"})
	listener, err := serverFabric.Listen("tcp", "wefty://control-plane")
	if err != nil {
		t.Fatal(err)
	}
	heartbeats := make(chan l1.Node)
	handler := http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		node := l1.Node{
			NodeRegistration: contract.NodeRegistration{NodeID: "node-1", BootSessionID: "boot-1"},
			State:            contract.NodeAlive, MaxOneshotSlots: 1, MaxServiceSlots: 1,
		}
		if request.URL.Path != "/v1/agent/nodes/register" {
			select {
			case node = <-heartbeats:
			case <-request.Context().Done():
				return
			}
		}
		if err := json.NewEncoder(response).Encode(node); err != nil {
			t.Errorf("encode node capacity response: %v", err)
		}
	})
	server := &http.Server{Handler: handler}
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	defer func() {
		_ = server.Close()
		if err := <-served; err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("serve capacity responses: %v", err)
		}
	}()

	agentFabric := network.NewFabric(fabric.Identity{NodeID: "agent"})
	client, err := newClient(agentFabric, "wefty://control-plane", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	clock := systemClock{}
	registration := contract.NodeRegistration{NodeID: "node-1", BootSessionID: "boot-1", AgentVersion: "test"}
	session := newAgentSession(
		client,
		registration,
		newCapabilityState(registration.Capabilities, nil, clock, 0),
		5*time.Millisecond,
		time.Second,
		clock,
		newLifecycleObserver(clock),
		nil,
		3,
		2,
	)
	if _, err := session.register(context.Background()); err != nil {
		t.Fatal(err)
	}
	if occupancy := session.gates[workloadClassOneShot].occupancy(); occupancy.Limit != 1 {
		t.Fatalf("registration-sized one-shot pool = %#v, want limit 1", occupancy)
	}

	ctx, cancel := context.WithCancel(context.Background())
	failures := make(chan destinationError, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		session.heartbeatLoop(ctx, failures)
	}()
	heartbeats <- l1.Node{MaxOneshotSlots: 3, MaxServiceSlots: 2}
	waitForClassLimit(t, session, workloadClassOneShot, 3)
	waitForClassLimit(t, session, workloadClassService, 2)

	gate := session.gates[workloadClassOneShot]
	release := make(chan struct{})
	started := make(chan struct{}, 3)
	workDone := make(chan struct{}, 3)
	for range 3 {
		go func() {
			_, _ = gate.execute(context.Background(), func(context.Context) (errorDestination, error) {
				started <- struct{}{}
				<-release
				return errorDestinationUnclassified, nil
			}, func(_ errorDestination, err error) error { return err })
			workDone <- struct{}{}
		}()
	}
	for range 3 {
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatal("failed to occupy grown one-shot pool")
		}
	}
	heartbeats <- l1.Node{MaxOneshotSlots: 1, MaxServiceSlots: 1}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		occupancy := gate.occupancy()
		if occupancy.Limit == 1 && occupancy.Overcommitted {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if occupancy := gate.occupancy(); occupancy.Limit != 1 || occupancy.Occupied != 3 || !occupancy.Overcommitted {
		t.Fatalf("heartbeat-reduced pool = %#v, want visible 3/1 overcommit", occupancy)
	}
	close(release)
	for range 3 {
		<-workDone
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("heartbeat loop did not stop")
	}
	select {
	case failure := <-failures:
		t.Fatalf("heartbeat capacity routing failed: %v", failure.err)
	default:
	}
}

func waitForClassLimit(t *testing.T, session *agentSession, class workloadClass, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if session.gates[class].occupancy().Limit == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("class %d limit = %d, want %d", class, session.gates[class].occupancy().Limit, want)
}
