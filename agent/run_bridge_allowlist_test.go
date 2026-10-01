package agent

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l3"
)

func TestRunBridgeAllowlistExactlyMirrorsL3RunTokenRoutes(t *testing.T) {
	authoritative := l3.RunTokenRoutes()
	if len(runBridgeRoutes) != len(authoritative) {
		t.Fatalf("run bridge routes = %#v, L3 run-token routes = %#v", runBridgeRoutes, authoritative)
	}
	for index, route := range authoritative {
		if runBridgeRoutes[index] != (bridgeRoute{Method: route.Method, Path: route.Path}) {
			t.Fatalf("run bridge routes = %#v, L3 run-token routes = %#v", runBridgeRoutes, authoritative)
		}
	}
}

// recordingL3 is a fake run ledger that echoes and records every request that
// reaches it, so a test can prove a refused request never left the bridge.
type recordingL3 struct {
	mu   sync.Mutex
	hits []string
}

func (l *recordingL3) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	line := request.Method + " " + request.URL.RequestURI() + " " + request.Header.Get("Authorization")
	l.mu.Lock()
	l.hits = append(l.hits, line)
	l.mu.Unlock()
	_, _ = io.WriteString(w, line)
}

func (l *recordingL3) recorded() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.hits...)
}

// startRunBridgeWithRecordingL3 builds the ordinary run surface exactly as a
// ledger-dispatched one-shot gets it in production.
func startRunBridgeWithRecordingL3(t *testing.T) (*workflowBridge, *recordingL3) {
	t.Helper()
	network := plain.NewNetwork()
	l3Fabric := network.NewFabric(fabric.Identity{NodeID: "run-ledger"})
	agentFabric := network.NewFabric(fabric.Identity{NodeID: "node-1"})
	listener, err := l3Fabric.Listen("tcp", "wefty://run-ledger")
	if err != nil {
		t.Fatal(err)
	}
	recorder := &recordingL3{}
	server := &http.Server{Handler: recorder}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close() })
	agent := &Agent{fabric: agentFabric, runLedgerAddr: "wefty://run-ledger", controlPlaneAddr: "wefty://control-plane"}
	bridge, err := agent.startWorkflowBridge(t.Context(), contract.JobKindProcess, contract.ExecutionSpec{}, true)
	if err != nil {
		t.Fatal(err)
	}
	if bridge == nil || bridge.surface != workflowBridgeSurfaceRun || bridge.l3Endpoint == "" {
		t.Fatalf("ordinary bridge = %+v; want the run surface with an /l3 endpoint", bridge)
	}
	t.Cleanup(func() { _ = bridge.close() })
	return bridge, recorder
}

func TestRunBridgeRefusesNodeAdministrationAndUnlistedL3Routes(t *testing.T) {
	bridge, recorder := startRunBridgeWithRecordingL3(t)
	refused := []struct{ method, path string }{
		// Node-authenticated Computer-pass administration (#595).
		{http.MethodPost, "/v1/computer-token/revoke-host"},
		{http.MethodPost, "/v1/computer-token/mint"},
		{http.MethodPost, "/v1/computer-token/revoke"},
		{http.MethodPost, "/v1/computer-token/revoke-attempt"},
		{http.MethodGet, "/v1/computers/computer-1/inflight"},
		// Routes L3 refuses a run token on.
		{http.MethodGet, "/v1/computer/self"},
		{http.MethodGet, "/v1/runs"},
		{http.MethodGet, "/v1/runs?origin=computer:computer-1"},
		{http.MethodPost, "/v1/runs/run-1/rerun"},
		{http.MethodPost, "/v1/runs/run-1/cancel"},
		{http.MethodPost, "/v1/workflows/workflow-1/versions"},
		{http.MethodGet, "/v1/workflows/workflow-1/versions/1"},
		// Wrong method on an allowlisted path, and paths L3 does not have.
		{http.MethodDelete, "/v1/runs/run-1"},
		{http.MethodPut, "/v1/runs/run-1/envelopes"},
		{http.MethodGet, "/v1/runs/run-1/envelopes"},
		{http.MethodGet, "/v1/not-a-route"},
		{http.MethodPost, "/v1/agent/jobs/claim"},
		{http.MethodGet, "/"},
	}
	for _, test := range refused {
		request, err := http.NewRequestWithContext(t.Context(), test.method, bridge.l3Endpoint+test.path, strings.NewReader(`{"reason":"workload"}`))
		if err != nil {
			t.Fatal(err)
		}
		// A bearer is present so the refusal is the allowlist's, not the
		// missing-credential check's.
		request.Header.Set("Authorization", "Bearer run-token")
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatalf("%s %s: %v", test.method, test.path, err)
		}
		var typed contract.ErrorResponse
		decodeErr := json.NewDecoder(response.Body).Decode(&typed)
		response.Body.Close()
		if response.StatusCode != http.StatusForbidden || decodeErr != nil || typed.Error.Code != contract.ErrorForbidden {
			t.Errorf("%s %s status=%d code=%q; want 403 %q", test.method, test.path, response.StatusCode, typed.Error.Code, contract.ErrorForbidden)
		}
	}
	if hits := recorder.recorded(); len(hits) != 0 {
		t.Fatalf("refused routes reached L3: %q", hits)
	}
}

func TestRunBridgeRequiresABearerOnAllowlistedRoutes(t *testing.T) {
	bridge, recorder := startRunBridgeWithRecordingL3(t)
	for _, authorization := range []string{"", "Bearer ", "Basic dXNlcjpwYXNz", "run-token"} {
		request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, bridge.l3Endpoint+"/v1/runs/run-1", nil)
		if err != nil {
			t.Fatal(err)
		}
		if authorization != "" {
			request.Header.Set("Authorization", authorization)
		}
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		var typed contract.ErrorResponse
		decodeErr := json.NewDecoder(response.Body).Decode(&typed)
		response.Body.Close()
		if response.StatusCode != http.StatusUnauthorized || decodeErr != nil || typed.Error.Code != contract.ErrorUnauthorized {
			t.Errorf("Authorization %q: status=%d code=%q; want 401 %q", authorization, response.StatusCode, typed.Error.Code, contract.ErrorUnauthorized)
		}
	}
	if hits := recorder.recorded(); len(hits) != 0 {
		t.Fatalf("credential-free requests reached L3 as the agent: %q", hits)
	}
}

func TestRunBridgeForwardsEveryRunTokenRoute(t *testing.T) {
	bridge, recorder := startRunBridgeWithRecordingL3(t)
	allowed := []struct{ method, path string }{
		{http.MethodPost, "/v1/runs"},
		{http.MethodGet, "/v1/runs/run-1"},
		{http.MethodGet, "/v1/runs/run-1/lineage"},
		{http.MethodGet, "/v1/runs/run-1/logs?limit=7&cursor=next"},
		{http.MethodGet, "/v1/runs/run-1/execution"},
		{http.MethodGet, "/v1/runs/run-1/result"},
		{http.MethodPost, "/v1/runs/run-1/envelopes"},
		{http.MethodPost, "/v1/runs/run-1/gates"},
	}
	if len(allowed) != len(l3.RunTokenRoutes()) {
		t.Fatalf("table covers %d routes, L3 declares %d", len(allowed), len(l3.RunTokenRoutes()))
	}
	for _, test := range allowed {
		request, err := http.NewRequestWithContext(t.Context(), test.method, bridge.l3Endpoint+test.path, strings.NewReader(`{}`))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Authorization", "Bearer run-token")
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatalf("%s %s: %v", test.method, test.path, err)
		}
		body, _ := io.ReadAll(response.Body)
		response.Body.Close()
		if want := test.method + " " + test.path + " Bearer run-token"; response.StatusCode != http.StatusOK || string(body) != want {
			t.Errorf("%s %s: status=%d body=%q; want 200 %q", test.method, test.path, response.StatusCode, body, want)
		}
	}
	if hits := recorder.recorded(); len(hits) != len(allowed) {
		t.Fatalf("L3 saw %d request(s), want %d: %q", len(hits), len(allowed), hits)
	}
}

// TestOrdinaryRunBridgeCannotRevokeOrRemintNeighbourComputerPasses is the
// #595 audit probe as a regression: a live Computer pass on node-1, a real
// L3, and an ordinary run's bridge on the same node. Before the allowlist the
// workload's credential-free revoke-host returned 204 and the neighbour's pass
// stopped authenticating.
func TestOrdinaryRunBridgeCannotRevokeOrRemintNeighbourComputerPasses(t *testing.T) {
	network := plain.NewNetwork()
	l3Fabric := network.NewFabric(fabric.Identity{NodeID: "run-ledger"})
	agentFabric := network.NewFabric(fabric.Identity{NodeID: "node-1"})
	proof := l3.ComputerTokenScopeProof{ComputerID: "computer-neighbour", ComputerAttemptID: "attempt-neighbour",
		ComputerStorageGeneration: 3, SubmitIntentRevision: 1, HostNodeID: "node-1", HostBootSessionID: "boot-1", SubmitMaxInflight: 4}
	store, err := l3.OpenStore(filepath.Join(t.TempDir(), "l3.sqlite"), l3.StoreOptions{ComputerAuthorityInstanceID: "run-bridge-595"})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	server, err := l3.NewServer(l3Fabric, store, l3.ServerConfig{ComputerGrants: staticComputerGrantVerifier{proof: proof}})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := l3Fabric.Listen("tcp", "wefty://run-ledger")
	if err != nil {
		t.Fatal(err)
	}
	serveContext, cancelServe := context.WithCancel(context.Background())
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(serveContext, listener) }()
	defer func() {
		cancelServe()
		if err := <-serveDone; err != nil {
			t.Errorf("serve real L3: %v", err)
		}
	}()
	grant, err := store.MintComputerToken(t.Context(), proof)
	if err != nil {
		t.Fatal(err)
	}

	agent := &Agent{fabric: agentFabric, runLedgerAddr: "wefty://run-ledger", controlPlaneAddr: "wefty://control-plane"}
	bridge, err := agent.startWorkflowBridge(t.Context(), contract.JobKindProcess, contract.ExecutionSpec{}, true)
	if err != nil {
		t.Fatal(err)
	}
	defer bridge.close()

	attacks := []struct {
		path, body, authorization string
	}{
		{"/v1/computer-token/revoke-host", `{"reason":"workload"}`, ""},
		{"/v1/computer-token/revoke-host", `{"reason":"workload"}`, "Bearer anything"},
		{"/v1/computer-token/revoke-attempt", `{"computer_id":"computer-neighbour","computer_attempt_id":"attempt-neighbour","reason":"workload"}`, "Bearer anything"},
		{"/v1/computer-token/mint", `{"computer_id":"computer-neighbour","computer_attempt_id":"attempt-neighbour"}`, "Bearer anything"},
	}
	for _, attack := range attacks {
		request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, bridge.l3Endpoint+attack.path, strings.NewReader(attack.body))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Content-Type", "application/json")
		if attack.authorization != "" {
			request.Header.Set("Authorization", attack.authorization)
		}
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(response.Body)
		response.Body.Close()
		if response.StatusCode != http.StatusForbidden {
			t.Errorf("POST %s (Authorization %q) through the run bridge: status=%d body=%s; want 403", attack.path, attack.authorization, response.StatusCode, body)
		}
	}

	computerBridge, err := newComputerAttemptBridge(t.Context(), agentFabric, "wefty://run-ledger", true)
	if err != nil {
		t.Fatal(err)
	}
	defer computerBridge.close()
	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, computerBridge.l3Endpoint+"/v1/computer/self", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+grant.Token)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("neighbour Computer pass after the attempts: status=%d body=%s; want it still authenticating", response.StatusCode, body)
	}
}

// A Connection header that names Authorization makes the proxy strip the
// credential as hop-by-hop after the inbound check has seen it. L3 would then
// fall back to the agent's Fabric identity (#595 review).
func TestRunBridgeRefusesAConnectionHeaderThatWouldDropAuthorization(t *testing.T) {
	bridge, recorder := startRunBridgeWithRecordingL3(t)
	for _, connection := range [][]string{
		{"Authorization"},
		{"authorization"},
		{"AUTHORIZATION"},
		{"keep-alive, authorization"},
		{"keep-alive,Authorization "},
		{"keep-alive", "Authorization"},
	} {
		for _, route := range []struct{ method, path string }{
			{http.MethodGet, "/v1/runs/run-other"},
			{http.MethodPost, "/v1/runs"},
		} {
			request, err := http.NewRequestWithContext(t.Context(), route.method, bridge.l3Endpoint+route.path, strings.NewReader(`{}`))
			if err != nil {
				t.Fatal(err)
			}
			request.Header.Set("Authorization", "Bearer anything")
			for _, value := range connection {
				request.Header.Add("Connection", value)
			}
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			var typed contract.ErrorResponse
			decodeErr := json.NewDecoder(response.Body).Decode(&typed)
			response.Body.Close()
			if response.StatusCode != http.StatusUnauthorized || decodeErr != nil || typed.Error.Code != contract.ErrorUnauthorized {
				t.Errorf("Connection %q on %s %s: status=%d code=%q; want 401 %q", connection, route.method, route.path,
					response.StatusCode, typed.Error.Code, contract.ErrorUnauthorized)
			}
		}
	}
	if hits := recorder.recorded(); len(hits) != 0 {
		t.Fatalf("requests whose credential the proxy dropped reached L3: %q", hits)
	}
}
