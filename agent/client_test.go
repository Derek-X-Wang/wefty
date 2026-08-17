package agent

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
)

func TestClientBoundsAnOperationWhoseServerNeverResponds(t *testing.T) {
	network := plain.NewNetwork()
	serverFabric := network.NewFabric(fabric.Identity{NodeID: "control-plane"})
	listener, err := serverFabric.Listen("tcp", "wefty://control-plane")
	if err != nil {
		t.Fatal(err)
	}
	accepted := make(chan struct{}, 1)
	server := &http.Server{Handler: http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
		accepted <- struct{}{}
		<-request.Context().Done()
	})}
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	defer func() {
		_ = server.Close()
		if err := <-served; err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("serve hanging operation: %v", err)
		}
	}()

	agentFabric := network.NewFabric(fabric.Identity{NodeID: "agent"})
	client, err := newClient(agentFabric, "wefty://control-plane", 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	started := time.Now()
	_, err = client.Register(context.Background(), contract.NodeRegistration{
		NodeID: "node-1", BootSessionID: "boot-1", AgentVersion: "test",
	})
	if err == nil {
		t.Fatal("hanging registration returned nil error")
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("bounded registration took %s, want less than one second", elapsed)
	}
	select {
	case <-accepted:
	default:
		t.Fatal("server did not accept the bounded operation")
	}
}

func TestRenewalRequestTimeoutIsStrictlyInsideAuthority(t *testing.T) {
	for _, test := range []struct {
		remaining, operation time.Duration
	}{
		{remaining: 30 * time.Second, operation: 10 * time.Second},
		{remaining: 3 * time.Second, operation: 10 * time.Second},
		{remaining: time.Nanosecond, operation: 10 * time.Second},
	} {
		timeout := renewalRequestTimeout(test.remaining, test.operation)
		if test.remaining <= time.Nanosecond {
			if timeout != 0 {
				t.Fatalf("renewal timeout(%s, %s) = %s, want no unsafe request", test.remaining, test.operation, timeout)
			}
			continue
		}
		if timeout <= 0 || timeout >= test.remaining {
			t.Fatalf("renewal timeout(%s, %s) = %s, want positive and strictly shorter than remaining authority", test.remaining, test.operation, timeout)
		}
		if timeout > test.operation {
			t.Fatalf("renewal timeout(%s, %s) = %s, exceeds operation bound", test.remaining, test.operation, timeout)
		}
	}
}
