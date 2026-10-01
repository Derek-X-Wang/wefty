package agent

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l3"
)

func startComputerBridgeWithRecordingL3(t *testing.T) (*workflowBridge, *recordingL3) {
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
	bridge, err := newComputerAttemptBridge(t.Context(), agentFabric, "wefty://run-ledger", true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bridge.close() })
	return bridge, recorder
}

// assertComputerBridgeRefusal checks the Computer surface's vocabulary: a
// bridge refusal is 403 forbidden and never unauthorized, which only L3 may
// return to a Computer tenant.
func assertComputerBridgeRefusal(t *testing.T, response *http.Response, label string) {
	t.Helper()
	var typed contract.ErrorResponse
	decodeErr := json.NewDecoder(response.Body).Decode(&typed)
	response.Body.Close()
	if response.StatusCode != http.StatusForbidden || decodeErr != nil || typed.Error.Code != contract.ErrorForbidden {
		t.Errorf("%s: status=%d code=%q; want 403 %q", label, response.StatusCode, typed.Error.Code, contract.ErrorForbidden)
	}
}

func TestComputerBridgeRequiresAPassBearerOnAllowlistedRoutes(t *testing.T) {
	bridge, recorder := startComputerBridgeWithRecordingL3(t)
	for _, route := range l3.ComputerTokenRoutes() {
		path := strings.ReplaceAll(route.Path, "{run_id}", "run-1")
		for _, authorization := range []string{"", "Bearer ", "Basic dXNlcjpwYXNz", "computer-pass"} {
			request, err := http.NewRequestWithContext(t.Context(), route.Method, bridge.l3Endpoint+path, strings.NewReader(`{}`))
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
			assertComputerBridgeRefusal(t, response, route.Method+" "+path+" Authorization "+authorization)
		}
	}
	if hits := recorder.recorded(); len(hits) != 0 {
		t.Fatalf("credential-free requests reached L3 as the agent: %q", hits)
	}
}

func TestComputerBridgeRefusesAConnectionHeaderThatWouldDropAuthorization(t *testing.T) {
	bridge, recorder := startComputerBridgeWithRecordingL3(t)
	for _, connection := range [][]string{
		{"Authorization"},
		{"authorization"},
		{"AUTHORIZATION"},
		{"keep-alive, authorization"},
		{"keep-alive,Authorization "},
		{"keep-alive", "Authorization"},
	} {
		for _, route := range []struct{ method, path string }{
			{http.MethodGet, "/v1/runs"},
			{http.MethodGet, "/v1/runs/run-other"},
			{http.MethodPost, "/v1/runs"},
		} {
			request, err := http.NewRequestWithContext(t.Context(), route.method, bridge.l3Endpoint+route.path, strings.NewReader(`{}`))
			if err != nil {
				t.Fatal(err)
			}
			request.Header.Set("Authorization", "Bearer computer-pass")
			for _, value := range connection {
				request.Header.Add("Connection", value)
			}
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			assertComputerBridgeRefusal(t, response, strings.Join(connection, "|")+" on "+route.method+" "+route.path)
		}
	}
	if hits := recorder.recorded(); len(hits) != 0 {
		t.Fatalf("requests whose credential the proxy dropped reached L3: %q", hits)
	}
}

// The guard must hold without the inbound check: it judges the request after
// the proxy's hop-by-hop cleanup and /l3 strip, the one L3 receives.
func TestComputerBridgeProxyJudgesTheOutboundRequestNotTheInboundOne(t *testing.T) {
	upstream := &countingRoundTripper{}
	proxy := newComputerBridgeProxy(upstream)
	for _, connection := range []string{"Authorization", "keep-alive, authorization"} {
		request := httptest.NewRequest(http.MethodGet, "http://bridge.invalid/l3/v1/runs/run-other", nil)
		request.Header.Set("Authorization", "Bearer computer-pass")
		request.Header.Set("Connection", connection)
		response := httptest.NewRecorder()
		proxy.ServeHTTP(response, request)
		assertComputerBridgeRefusal(t, response.Result(), "Connection "+connection)
	}
	for _, path := range []string{"/l3/v1/computer-token/revoke-host", "/l3/v1/runs/run-1/execution", "/l3/v1/runs//logs"} {
		request := httptest.NewRequest(http.MethodGet, "http://bridge.invalid"+path, nil)
		request.Header.Set("Authorization", "Bearer computer-pass")
		response := httptest.NewRecorder()
		proxy.ServeHTTP(response, request)
		assertComputerBridgeRefusal(t, response.Result(), "GET "+path)
	}
	if calls := upstream.calls.Load(); calls != 0 {
		t.Fatalf("guard let %d refused request(s) through", calls)
	}
	request := httptest.NewRequest(http.MethodGet, "http://bridge.invalid/l3/v1/computer/self", nil)
	request.Header.Set("Authorization", "Bearer computer-pass")
	request.Header.Set("Connection", "keep-alive")
	response := httptest.NewRecorder()
	proxy.ServeHTTP(response, request)
	if response.Code != http.StatusOK || upstream.calls.Load() != 1 ||
		upstream.authorization.Load() != "Bearer computer-pass" || upstream.path.Load() != "/v1/computer/self" {
		t.Fatalf("allowed request: status=%d calls=%d authorization=%v path=%v", response.Code, upstream.calls.Load(),
			upstream.authorization.Load(), upstream.path.Load())
	}
}

// With a real L3 and an agent identity that also carries the client tag, a
// credential-free request through the Computer bridge would be served as the
// agent: the whole Run listing, any Run, and root submissions. The bridge
// refuses it; the real pass still works.
func TestComputerBridgeNeverLendsTheAgentsFabricIdentityToATenant(t *testing.T) {
	network := plain.NewNetwork()
	l3Fabric := network.NewFabric(fabric.Identity{NodeID: "run-ledger"})
	agentFabric := network.NewFabric(fabric.Identity{NodeID: "node-1", Tags: []string{l3.DefaultCallerPrincipalTag}})
	proof := l3.ComputerTokenScopeProof{ComputerID: "computer-1", ComputerAttemptID: "attempt-1",
		ComputerStorageGeneration: 1, SubmitIntentRevision: 1, HostNodeID: "node-1", HostStableNodeID: "stable-node", HostBootSessionID: "boot-1", SubmitMaxInflight: 4}
	store, err := l3.OpenStore(filepath.Join(t.TempDir(), "l3.sqlite"), l3.StoreOptions{ComputerAuthorityInstanceID: "computer-bridge-595"})
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
	bridge, err := newComputerAttemptBridge(t.Context(), agentFabric, "wefty://run-ledger", true)
	if err != nil {
		t.Fatal(err)
	}
	defer bridge.close()

	for _, attempt := range []struct {
		name          string
		authorization string
		connection    string
	}{
		{name: "no bearer"},
		{name: "pass stripped as hop-by-hop", authorization: "Bearer " + grant.Token, connection: "keep-alive, Authorization"},
	} {
		request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, bridge.l3Endpoint+"/v1/runs", nil)
		if err != nil {
			t.Fatal(err)
		}
		if attempt.authorization != "" {
			request.Header.Set("Authorization", attempt.authorization)
		}
		if attempt.connection != "" {
			request.Header.Set("Connection", attempt.connection)
		}
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		assertComputerBridgeRefusal(t, response, attempt.name)
	}

	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, bridge.l3Endpoint+"/v1/computer/self", nil)
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
		t.Fatalf("real pass through the Computer bridge: status=%d body=%s", response.StatusCode, body)
	}
}
