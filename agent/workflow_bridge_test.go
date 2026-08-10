package agent

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
)

func TestWorkflowBridgeForwardsOnlyL3(t *testing.T) {
	network := plain.NewNetwork()
	l3Fabric := network.NewFabric(fabric.Identity{NodeID: "run-ledger"})
	agentFabric := network.NewFabric(fabric.Identity{NodeID: "agent"})

	l3Listener, err := l3Fabric.Listen("tcp", "wefty://run-ledger")
	if err != nil {
		t.Fatal(err)
	}
	l3Server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		_, _ = io.WriteString(w, request.Method+" "+request.URL.RequestURI()+" "+request.Header.Get("Authorization"))
	})}
	go func() { _ = l3Server.Serve(l3Listener) }()
	defer l3Server.Close()

	bridge, err := newWorkflowBridge(context.Background(), agentFabric, "wefty://run-ledger")
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

	response, err = http.Get(strings.Replace(bridge.l3Endpoint, "/l3", "/l1", 1) + "/v1/jobs/job-1/logs?limit=5")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("L1 bridge route status = %d, want 404", response.StatusCode)
	}
}

func TestWorkflowBridgeIsClosedWithAttempt(t *testing.T) {
	network := plain.NewNetwork()
	participant := network.NewFabric(fabric.Identity{NodeID: "agent"})
	bridge, err := newWorkflowBridge(context.Background(), participant, "wefty://run-ledger")
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
