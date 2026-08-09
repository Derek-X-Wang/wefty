package agent

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
)

func TestWorkflowBridgeForwardsL3AndRestrictsL1ToClientReads(t *testing.T) {
	network := plain.NewNetwork()
	l1Fabric := network.NewFabric(fabric.Identity{NodeID: "control-plane"})
	l3Fabric := network.NewFabric(fabric.Identity{NodeID: "run-ledger"})
	agentFabric := network.NewFabric(fabric.Identity{NodeID: "agent"})

	var l1Requests atomic.Int32
	l1Listener, err := l1Fabric.Listen("tcp", "wefty://control-plane")
	if err != nil {
		t.Fatal(err)
	}
	l1Server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		l1Requests.Add(1)
		_, _ = io.WriteString(w, request.Method+" "+request.URL.RequestURI())
	})}
	go func() { _ = l1Server.Serve(l1Listener) }()
	defer l1Server.Close()

	l3Listener, err := l3Fabric.Listen("tcp", "wefty://run-ledger")
	if err != nil {
		t.Fatal(err)
	}
	l3Server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		_, _ = io.WriteString(w, request.Method+" "+request.URL.RequestURI()+" "+request.Header.Get("Authorization"))
	})}
	go func() { _ = l3Server.Serve(l3Listener) }()
	defer l3Server.Close()

	bridge, err := newWorkflowBridge(context.Background(), agentFabric, "wefty://control-plane", "wefty://run-ledger")
	if err != nil {
		t.Fatal(err)
	}
	defer bridge.close()

	request, err := http.NewRequest(http.MethodPost, bridge.l3Endpoint+"/v1/runs", strings.NewReader(`{"params":{}}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer run-token")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if got, want := string(body), "POST /v1/runs Bearer run-token"; got != want {
		t.Fatalf("L3 bridge response = %q, want %q", got, want)
	}

	response, err = http.Get(bridge.l1Endpoint + "/v1/jobs/job-1/logs?limit=5")
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(response.Body)
	response.Body.Close()
	if got, want := string(body), "GET /v1/jobs/job-1/logs?limit=5"; got != want {
		t.Fatalf("L1 bridge response = %q, want %q", got, want)
	}

	request, err = http.NewRequest(http.MethodPost, bridge.l1Endpoint+"/v1/agent/jobs/claim", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusForbidden || l1Requests.Load() != 1 {
		t.Fatalf("agent route = status %d, upstream requests %d", response.StatusCode, l1Requests.Load())
	}
}

func TestWorkflowBridgeIsClosedWithAttempt(t *testing.T) {
	network := plain.NewNetwork()
	participant := network.NewFabric(fabric.Identity{NodeID: "agent"})
	bridge, err := newWorkflowBridge(context.Background(), participant, "wefty://control-plane", "wefty://run-ledger")
	if err != nil {
		t.Fatal(err)
	}
	endpoint := bridge.l3Endpoint
	if err := bridge.close(); err != nil {
		t.Fatal(err)
	}
	if _, err := http.Get(endpoint + "/v1/runs/run-1"); err == nil {
		t.Fatal("workflow bridge remained reachable after close")
	}
}
