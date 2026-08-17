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

func TestClaimRequestsOneShotWorkWithLocalJobExclusions(t *testing.T) {
	network := plain.NewNetwork()
	serverFabric := network.NewFabric(fabric.Identity{NodeID: "control-plane"})
	listener, err := serverFabric.Listen("tcp", "wefty://control-plane")
	if err != nil {
		t.Fatal(err)
	}
	received := make(chan l1.ClaimRequest, 1)
	server := &http.Server{Handler: http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		var claim l1.ClaimRequest
		if err := json.NewDecoder(request.Body).Decode(&claim); err != nil {
			t.Errorf("decode claim request: %v", err)
		}
		received <- claim
		response.WriteHeader(http.StatusNoContent)
	})}
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	defer func() {
		_ = server.Close()
		if err := <-served; err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("serve claim request: %v", err)
		}
	}()

	agentFabric := network.NewFabric(fabric.Identity{NodeID: "agent"})
	client, err := newClient(agentFabric, "wefty://control-plane", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	claim, err := client.Claim(
		context.Background(), "node-1", "boot-1", contract.JobClassOneShot, "job-finalizing-a", "job-finalizing-b",
	)
	if err != nil || claim != nil {
		t.Fatalf("claim = %#v, err = %v, want empty successful claim", claim, err)
	}
	request := <-received
	if request.Class != contract.JobClassOneShot {
		t.Fatalf("claim class = %q, want %q", request.Class, contract.JobClassOneShot)
	}
	if len(request.ExcludedJobIDs) != 2 || request.ExcludedJobIDs[0] != "job-finalizing-a" || request.ExcludedJobIDs[1] != "job-finalizing-b" {
		t.Fatalf("claim exclusions = %#v, want both locally finalizing jobs", request.ExcludedJobIDs)
	}
}

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

func TestClientTransportSupportsConcurrentAttemptTraffic(t *testing.T) {
	assertClientTransportSupportsConcurrentAttemptTraffic(t)
}

func assertClientTransportSupportsConcurrentAttemptTraffic(t *testing.T) {
	t.Helper()
	participant := plain.NewNetwork().NewFabric(fabric.Identity{NodeID: "agent"})
	client, err := NewClient(participant, "wefty://control-plane")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if got := client.transport.MaxIdleConnsPerHost; got != DefaultMaxIdleConnsPerHost || got <= http.DefaultMaxIdleConnsPerHost {
		t.Fatalf("MaxIdleConnsPerHost = %d, want raised concurrent-attempt pool %d", got, DefaultMaxIdleConnsPerHost)
	}
}
