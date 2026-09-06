package agent

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l3"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	ocirunner "github.com/Derek-X-Wang/wefty/runner/oci"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

type staticComputerGrantVerifier struct{ proof l3.ComputerTokenScopeProof }

func (verifier staticComputerGrantVerifier) ProveComputerTokenScope(context.Context, string, string, string, string) (l3.ComputerTokenScopeProof, error) {
	return verifier.proof, nil
}

type workflowBridgeBinderFunc func(context.Context) (workloadrunner.WorkflowBridgeBinding, error)

func (bind workflowBridgeBinderFunc) Bind(ctx context.Context) (workloadrunner.WorkflowBridgeBinding, error) {
	return bind(ctx)
}

type readObservedListener struct {
	net.Listener
	readStarted chan struct{}
	readBytes   chan struct{}
	once        sync.Once
	bytesOnce   sync.Once
}

func (listener *readObservedListener) Accept() (net.Conn, error) {
	connection, err := listener.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &readObservedConnection{Conn: connection, observe: func() {
		listener.once.Do(func() { close(listener.readStarted) })
	}, observeBytes: func() {
		listener.bytesOnce.Do(func() { close(listener.readBytes) })
	}}, nil
}

type readObservedConnection struct {
	net.Conn
	observe      func()
	observeBytes func()
}

type closeErrorListener struct {
	net.Listener
	err           error
	acceptStarted chan struct{}
	once          sync.Once
}

func (listener *closeErrorListener) Accept() (net.Conn, error) {
	listener.once.Do(func() { close(listener.acceptStarted) })
	return listener.Listener.Accept()
}

func (listener *closeErrorListener) Close() error {
	return errors.Join(listener.Listener.Close(), listener.err)
}

type acceptedCloseErrorListener struct {
	net.Listener
	err         error
	readStarted chan struct{}
	once        sync.Once
}

func (listener *acceptedCloseErrorListener) Accept() (net.Conn, error) {
	connection, err := listener.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &acceptedCloseErrorConnection{Conn: connection, err: listener.err, observeRead: func() {
		listener.once.Do(func() { close(listener.readStarted) })
	}}, nil
}

type acceptedCloseErrorConnection struct {
	net.Conn
	err         error
	observeRead func()
}

func (connection *acceptedCloseErrorConnection) Read(buffer []byte) (int, error) {
	connection.observeRead()
	return connection.Conn.Read(buffer)
}

func (connection *acceptedCloseErrorConnection) Close() error {
	return errors.Join(connection.Conn.Close(), connection.err)
}

type workflowBridgeDrainFailure interface {
	error
	ActiveConnectionCount() int
}

func (connection *readObservedConnection) Read(buffer []byte) (int, error) {
	connection.observe()
	count, err := connection.Conn.Read(buffer)
	if count > 0 && connection.observeBytes != nil {
		connection.observeBytes()
	}
	return count, err
}

func TestComputerAttemptBridgeAllowlistExactlyMirrorsL3ComputerRoutes(t *testing.T) {
	if authoritative := l3.ComputerTokenRoutes(); !reflect.DeepEqual(computerBridgeRoutes, authoritative) {
		t.Fatalf("Computer bridge routes = %#v, L3 Computer routes = %#v", computerBridgeRoutes, authoritative)
	}
}

func TestComputerBridgeCancellationCausesRemainDistinct(t *testing.T) {
	bridge := &workflowBridge{surface: workflowBridgeSurfaceComputer}
	bridge.setReachable(true)
	reminted, cancelReminted, ok := bridge.reachableRequestContext(t.Context())
	if !ok {
		t.Fatal("reachable bridge omitted policy re-mint context")
	}
	defer cancelReminted()
	bridge.setReachable(true)
	<-reminted.Done()
	if !errors.Is(context.Cause(reminted), errComputerSubmissionPolicyReminted) {
		t.Fatalf("policy re-mint cause = %v", context.Cause(reminted))
	}

	revoked, cancelRevoked, ok := bridge.reachableRequestContext(t.Context())
	if !ok {
		t.Fatal("re-minted bridge omitted revocation context")
	}
	defer cancelRevoked()
	bridge.setReachable(false)
	<-revoked.Done()
	if !errors.Is(context.Cause(revoked), errComputerSubmissionRevoked) {
		t.Fatalf("revocation cause = %v", context.Cause(revoked))
	}

	bridge.setReachable(true)
	closed, cancelClosed, ok := bridge.reachableRequestContext(t.Context())
	if !ok {
		t.Fatal("re-enabled bridge omitted attempt-close context")
	}
	defer cancelClosed()
	bridge.mu.Lock()
	bridge.setReachabilityLocked(false, errComputerAttemptClosed)
	bridge.mu.Unlock()
	<-closed.Done()
	if !errors.Is(context.Cause(closed), errComputerAttemptClosed) {
		t.Fatalf("attempt-close cause = %v", context.Cause(closed))
	}
}

func TestStartWorkflowBridgeSelectsComputerSurfaceOnForcedMacFallback(t *testing.T) {
	network := plain.NewNetwork()
	l3Fabric := network.NewFabric(fabric.Identity{NodeID: "run-ledger"})
	agentFabric := network.NewFabric(fabric.Identity{NodeID: "agent"})
	l3Listener, err := l3Fabric.Listen("tcp", "wefty://run-ledger")
	if err != nil {
		t.Fatal(err)
	}
	var forwarded atomic.Int64
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		forwarded.Add(1)
		w.WriteHeader(http.StatusOK)
	})}
	go func() { _ = server.Serve(l3Listener) }()
	defer server.Close()

	agent := &Agent{fabric: agentFabric, runLedgerAddr: "wefty://run-ledger", ociBridgeBinder: workflowBridgeBinderFunc(func(context.Context) (workloadrunner.WorkflowBridgeBinding, error) {
		listener, err := net.Listen("tcp4", "127.0.0.1:0")
		return workloadrunner.WorkflowBridgeBinding{Listener: listener, AdvertiseHost: "127.0.0.1", HostBridgeFallback: true}, err
	})}
	disabledExecution := contract.ExecutionSpec{OCI: &contract.OCIExecutionSpec{Computer: &contract.OCIComputerSpec{DiskBytes: 8 << 30}}}
	if disabledBridge, err := agent.startWorkflowBridge(t.Context(), contract.JobKindOCI, disabledExecution); err != nil || disabledBridge != nil {
		t.Fatalf("default-off production bridge = %v, err=%v; want no endpoint", disabledBridge, err)
	}
	execution := contract.ExecutionSpec{OCI: &contract.OCIExecutionSpec{Computer: &contract.OCIComputerSpec{DiskBytes: 8 << 30}}, SensitiveEnv: map[string]string{contract.EnvComputerToken: "real-computer-pass"}}
	bridge, err := agent.startWorkflowBridge(t.Context(), contract.JobKindOCI, execution)
	if err != nil {
		t.Fatal(err)
	}
	defer bridge.close()
	if bridge.surface != workflowBridgeSurfaceComputer || !bridge.hostBridgeFallback {
		t.Fatalf("production bridge surface=%v fallback=%t", bridge.surface, bridge.hostBridgeFallback)
	}
	response, err := http.Get(bridge.l3Endpoint + "/v1/computer/self")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK || forwarded.Load() != 1 {
		t.Fatalf("allowed production route status=%d forwarded=%d", response.StatusCode, forwarded.Load())
	}
	response, err = http.Post(bridge.l3Endpoint+"/v1/computer-token/mint", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusForbidden || forwarded.Load() != 1 {
		t.Fatalf("forbidden production route status=%d forwarded=%d", response.StatusCode, forwarded.Load())
	}
}

func TestForgedProvenanceHeadersCannotChangeRealL3ComputerScopeThroughBridge(t *testing.T) {
	network := plain.NewNetwork()
	l3Fabric := network.NewFabric(fabric.Identity{NodeID: "run-ledger"})
	agentFabric := network.NewFabric(fabric.Identity{NodeID: "agent-node"})
	proof := l3.ComputerTokenScopeProof{ComputerID: "computer-real", ComputerAttemptID: "attempt-real",
		ComputerStorageGeneration: 9, SubmitIntentRevision: 4, HostNodeID: "agent-node", SubmitMaxInflight: 20}
	store, err := l3.OpenStore(filepath.Join(t.TempDir(), "l3.sqlite"), l3.StoreOptions{ComputerAuthorityInstanceID: "bridge-test"})
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
	content := "exit 0\n"
	digest := sha256.Sum256([]byte(content))
	body, err := json.Marshal(l3.CreateRunRequest{InlineScript: &l3.InlineScriptInput{Content: content, SHA256: hex.EncodeToString(digest[:]), Interpreter: []string{"/bin/sh"}}, Params: json.RawMessage(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, bridge.l3Endpoint+"/v1/runs", strings.NewReader(string(body)))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+grant.Token)
	request.Header.Set("Idempotency-Key", "forged-header-receipt")
	request.Header.Set("X-Wefty-Computer-ID", "forged")
	request.Header.Set("X-Wefty-Computer-Attempt-ID", "forged")
	request.Header.Set("X-Wefty-Computer-Storage-Generation", "999")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var accepted l3.RunAccepted
	if err := json.NewDecoder(response.Body).Decode(&accepted); err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("real L3 submission status=%d", response.StatusCode)
	}
	trigger, err := store.GetTrigger(t.Context(), accepted.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if trigger.ComputerID != proof.ComputerID || trigger.ComputerAttemptID != proof.ComputerAttemptID || trigger.ComputerStorageGeneration != proof.ComputerStorageGeneration {
		t.Fatalf("forged headers changed L3 provenance: %+v", trigger)
	}
}

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

func TestComputerAttemptBridgeProjectsOnlyTheL3OwnedSurface(t *testing.T) {
	network := plain.NewNetwork()
	l3Fabric := network.NewFabric(fabric.Identity{NodeID: "run-ledger"})
	agentFabric := network.NewFabric(fabric.Identity{NodeID: "agent"})
	l3Listener, err := l3Fabric.Listen("tcp", "wefty://run-ledger")
	if err != nil {
		t.Fatal(err)
	}
	var forwarded atomic.Int64
	l3Server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		forwarded.Add(1)
		if request.Method == http.MethodPost && request.URL.Path == "/v1/runs" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{
				Code: contract.ErrorSubmitInflightLimit, Message: "Computer root Lineage limit reached",
				Details: map[string]any{"count": 20, "limit": 20},
			}})
			return
		}
		_, _ = io.WriteString(w, request.Method+" "+request.URL.RequestURI())
	})}
	go func() { _ = l3Server.Serve(l3Listener) }()
	defer l3Server.Close()

	bridge, err := newComputerAttemptBridge(t.Context(), agentFabric, "wefty://run-ledger", true)
	if err != nil {
		t.Fatal(err)
	}
	defer bridge.close()
	allowed := []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/v1/computer/self"},
		{http.MethodGet, "/v1/runs"},
		{http.MethodPost, "/v1/runs"},
		{http.MethodGet, "/v1/runs/run-1"},
		{http.MethodGet, "/v1/runs/run-1/lineage"},
		{http.MethodGet, "/v1/runs/run-1/logs?limit=7&cursor=next"},
	}
	for _, test := range allowed {
		request, err := http.NewRequestWithContext(t.Context(), test.method, bridge.l3Endpoint+test.path, strings.NewReader(`{}`))
		if err != nil {
			t.Fatal(err)
		}
		if test.method == http.MethodPost {
			request.Header.Set("X-Wefty-Computer-ID", "forged")
			request.Header.Set("X-Wefty-Computer-Attempt-ID", "forged")
			request.Header.Set("X-Wefty-Computer-Storage-Generation", "999")
			request.Header.Set("X-Wefty-Submit-Intent-Revision", "999")
		}
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatalf("allowed %s %s: %v", test.method, test.path, err)
		}
		body, _ := io.ReadAll(response.Body)
		response.Body.Close()
		if test.method == http.MethodPost {
			var typed contract.ErrorResponse
			if response.StatusCode != http.StatusConflict || json.Unmarshal(body, &typed) != nil || typed.Error.Code != contract.ErrorSubmitInflightLimit {
				t.Fatalf("typed inflight limit was not surfaced unchanged: status=%d body=%s", response.StatusCode, body)
			}
		} else if response.StatusCode != http.StatusOK {
			t.Fatalf("allowed %s %s status=%d body=%s", test.method, test.path, response.StatusCode, body)
		}
	}
	if got := forwarded.Load(); got != int64(len(allowed)) {
		t.Fatalf("forwarded allowed routes = %d, want %d", got, len(allowed))
	}
}

func TestComputerAttemptBridgePublishesPrivateNamespaceEndpoint(t *testing.T) {
	participant := plain.NewNetwork().NewFabric(fabric.Identity{NodeID: "agent"})
	controller := newComputerAttemptBridgeController(t.Context(), func(ctx context.Context, _ string, _ contract.ExecutionSpec) (*workflowBridge, error) {
		return newComputerAttemptBridge(ctx, participant, "wefty://run-ledger", true)
	}, contract.JobKindOCI, contract.ExecutionSpec{OCI: &contract.OCIExecutionSpec{Computer: &contract.OCIComputerSpec{DiskBytes: 8 << 30}}})
	const guestEndpoint = "http://127.0.0.1:42424/l3"
	if err := controller.setGuestEndpoint(guestEndpoint); err != nil {
		t.Fatal(err)
	}
	endpoint, err := controller.enable("computer-pass")
	if err != nil {
		t.Fatal(err)
	}
	defer controller.disable(errComputerAttemptClosed)
	if endpoint != guestEndpoint {
		t.Fatalf("Computer bridge endpoint = %q, want private namespace endpoint %q", endpoint, guestEndpoint)
	}
}

func TestComputerAttemptBridgeNegativeRouteReceiptIsAssertionDerived(t *testing.T) {
	network := plain.NewNetwork()
	l3Fabric := network.NewFabric(fabric.Identity{NodeID: "run-ledger"})
	agentFabric := network.NewFabric(fabric.Identity{NodeID: "agent"})
	l3Listener, err := l3Fabric.Listen("tcp", "wefty://run-ledger")
	if err != nil {
		t.Fatal(err)
	}
	var forwarded atomic.Int64
	l3Server := &http.Server{Handler: http.HandlerFunc(func(http.ResponseWriter, *http.Request) { forwarded.Add(1) })}
	go func() { _ = l3Server.Serve(l3Listener) }()
	defer l3Server.Close()
	bridge, err := newComputerAttemptBridge(t.Context(), agentFabric, "wefty://run-ledger", true)
	if err != nil {
		t.Fatal(err)
	}
	defer bridge.close()

	negative := []struct{ method, path string }{
		{http.MethodPost, "/v1/computer/self"},
		{http.MethodPut, "/v1/computer/self"},
		{http.MethodGet, "/v1/computer-token/mint"},
		{http.MethodPost, "/v1/computer-token/mint"},
		{http.MethodPost, "/v1/computer-token/revoke"},
		{http.MethodPost, "/v1/computer-token/revoke-attempt"},
		{http.MethodPost, "/v1/computer-token/revoke-host"},
		{http.MethodGet, "/v1/workflows/workflow-1/versions/1"},
		{http.MethodPost, "/v1/workflows/workflow-1/versions"},
		{http.MethodGet, "/v1/runs/run-1/execution"},
		{http.MethodGet, "/v1/runs/run-1/envelopes"},
		{http.MethodPost, "/v1/runs/run-1/envelopes"},
		{http.MethodPost, "/v1/runs/run-1/gates"},
		{http.MethodPost, "/v1/runs/run-1/rerun"},
		{http.MethodPost, "/v1/runs/run-1/cancel"},
		{http.MethodDelete, "/v1/runs/run-1"},
		{http.MethodPatch, "/v1/runs/run-1"},
		{http.MethodPut, "/v1/runs/run-1"},
		{http.MethodGet, "/v1/runs/run-1/gates"},
		{http.MethodGet, "/v1/runs/run-1/rerun"},
	}
	type receipt struct{ rejected, total int }
	got := receipt{total: len(negative)}
	for _, test := range negative {
		request, err := http.NewRequestWithContext(t.Context(), test.method, bridge.l3Endpoint+test.path, strings.NewReader(`{}`))
		if err != nil {
			t.Fatal(err)
		}
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatalf("negative %s %s: %v", test.method, test.path, err)
		}
		response.Body.Close()
		if response.StatusCode == http.StatusForbidden {
			got.rejected++
		}
	}
	if got.total != 20 || got.rejected != got.total || forwarded.Load() != 0 {
		t.Fatalf("mutation-check receipt = %d/%d rejected, forwarded=%d; want 20/20 and zero forwarded", got.rejected, got.total, forwarded.Load())
	}
}

func TestComputerAttemptBridgeRejectsUncleanRunSegments(t *testing.T) {
	for _, path := range []string{"/v1/runs//logs", "/v1/runs/./logs", "/v1/runs/../logs", "/v1/runs/ run-1/logs"} {
		if computerBridgeRouteAllowed(http.MethodGet, path) {
			t.Fatalf("unclean Computer run path %q was allowlisted", path)
		}
	}
	network := plain.NewNetwork()
	l3Fabric := network.NewFabric(fabric.Identity{NodeID: "run-ledger"})
	agentFabric := network.NewFabric(fabric.Identity{NodeID: "agent"})
	listener, err := l3Fabric.Listen("tcp", "wefty://run-ledger")
	if err != nil {
		t.Fatal(err)
	}
	var forwarded atomic.Int64
	server := &http.Server{Handler: http.HandlerFunc(func(http.ResponseWriter, *http.Request) { forwarded.Add(1) })}
	go func() { _ = server.Serve(listener) }()
	defer server.Close()
	bridge, err := newComputerAttemptBridge(t.Context(), agentFabric, "wefty://run-ledger", true)
	if err != nil {
		t.Fatal(err)
	}
	defer bridge.close()
	for _, encodedPath := range []string{"/v1/runs//logs", "/v1/runs/%2e/logs", "/v1/runs/%2e%2e/logs", "/v1/runs/%20run-1/logs"} {
		response, err := http.Get(bridge.l3Endpoint + encodedPath)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusForbidden {
			t.Fatalf("unclean HTTP path %q status=%d", encodedPath, response.StatusCode)
		}
	}
	if forwarded.Load() != 0 {
		t.Fatalf("unclean HTTP paths forwarded %d request(s)", forwarded.Load())
	}
}

func TestComputerAttemptBridgeUnavailableIsTyped(t *testing.T) {
	participant := plain.NewNetwork().NewFabric(fabric.Identity{NodeID: "agent"})
	bridge, err := newComputerAttemptBridge(t.Context(), participant, "wefty://run-ledger", false)
	if err != nil {
		t.Fatal(err)
	}
	defer bridge.close()
	response, err := http.Get(bridge.l3Endpoint + "/v1/computer/self")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var typed contract.ErrorResponse
	if err := json.NewDecoder(response.Body).Decode(&typed); err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusServiceUnavailable || typed.Error.Code != contract.ErrorPassUnavailable {
		t.Fatalf("unavailable bridge status=%d code=%q", response.StatusCode, typed.Error.Code)
	}
}

func TestComputerSubmissionPolicyLossCancelsInflightAndReenableRestoresTransport(t *testing.T) {
	network := plain.NewNetwork()
	l3Fabric := network.NewFabric(fabric.Identity{NodeID: "run-ledger"})
	agentFabric := network.NewFabric(fabric.Identity{NodeID: "agent"})
	l3Listener, err := l3Fabric.Listen("tcp", "wefty://run-ledger")
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	canceled := make(chan struct{})
	var calls atomic.Int64
	l3Server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if calls.Add(1) == 1 {
			close(started)
			<-request.Context().Done()
			close(canceled)
			return
		}
		w.WriteHeader(http.StatusOK)
	})}
	go func() { _ = l3Server.Serve(l3Listener) }()
	defer l3Server.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	computerExecution := contract.ExecutionSpec{OCI: &contract.OCIExecutionSpec{Computer: &contract.OCIComputerSpec{DiskBytes: 8 << 30}}}
	controller := newComputerAttemptBridgeController(ctx, func(ctx context.Context, _ string, _ contract.ExecutionSpec) (*workflowBridge, error) {
		return newComputerAttemptBridge(ctx, agentFabric, "wefty://run-ledger", true)
	}, contract.JobKindOCI, computerExecution)
	endpoint, err := controller.enable("initial-pass")
	if err != nil {
		t.Fatal(err)
	}
	defer controller.disable(errComputerAttemptClosed)
	runtime := &recordingComputerTokenFileRuntime{writes: make(chan computerSubmissionWrite, 4)}
	minter := &recordingComputerTokenMinter{grant: l3.ComputerTokenGrant{Token: "replacement-pass", ComputerID: "computer-1",
		ComputerAttemptID: "attempt-1", SubmitIntentRevision: 3, SubmitMaxInflight: 20}}
	updates := make(chan ComputerSubmissionAuthority, 2)
	syncDone := make(chan error, 1)
	enabled := ComputerSubmissionAuthority{ComputerID: "computer-1", Enabled: true, SubmitIntentRevision: 2, SubmitMaxInflight: 20}
	go func() {
		syncDone <- syncComputerTokenFile(ctx, runtime, workloadrunner.AttemptAuthority{}, systemClock{}, minter,
			controller, "computer-1", "attempt-1", enabled, enabled, updates)
	}()
	type bridgeResponse struct {
		status int
		body   contract.ErrorResponse
		err    error
	}
	requestDone := make(chan bridgeResponse, 1)
	go func() {
		response, err := http.Get(endpoint + "/v1/runs/run-1")
		result := bridgeResponse{err: err}
		if response != nil {
			result.status = response.StatusCode
			result.err = errors.Join(result.err, json.NewDecoder(response.Body).Decode(&result.body), response.Body.Close())
		}
		requestDone <- result
	}()
	select {
	case <-started:
	case <-t.Context().Done():
		t.Fatal("in-flight request did not reach L3")
	}
	updates <- ComputerSubmissionAuthority{ComputerID: "computer-1", SubmitIntentRevision: 3, SubmitMaxInflight: 20}
	assertTokenFileWrite(t, runtime.writes, "", "")
	select {
	case <-canceled:
	case <-t.Context().Done():
		t.Fatal("disable did not cancel in-flight L3 traffic")
	}
	select {
	case result := <-requestDone:
		if result.err != nil || result.status != http.StatusBadGateway || result.body.Error.Code != contract.ErrorPassUnavailable ||
			result.body.Error.Message != errComputerSubmissionRevoked.Error() {
			t.Fatalf("canceled bridge response = status %d error %#v err %v, want typed indeterminate revocation cancellation", result.status, result.body.Error, result.err)
		}
	case <-t.Context().Done():
		t.Fatal("canceled bridge request did not return")
	}
	if response, err := http.Get(endpoint + "/v1/computer/self"); err == nil {
		response.Body.Close()
		t.Fatal("disabled policy left the old Computer endpoint reachable")
	}
	updates <- ComputerSubmissionAuthority{ComputerID: "computer-1", Enabled: true, SubmitIntentRevision: 3, SubmitMaxInflight: 20}
	assertTokenFileWrite(t, runtime.writes, "", "")
	reenabled := assertTokenFileWrite(t, runtime.writes, "replacement-pass", "")
	response, err := http.Get(reenabled.endpoint + "/v1/computer/self")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK || calls.Load() != 2 {
		t.Fatalf("re-enabled bridge status=%d L3 calls=%d, want 200 and two calls", response.StatusCode, calls.Load())
	}
	cancel()
	if err := <-syncDone; err != nil {
		t.Fatal(err)
	}
}

func TestComputerSubmissionPolicyRemintClosesUnusedHelperPreconnection(t *testing.T) {
	participant := plain.NewNetwork().NewFabric(fabric.Identity{NodeID: "agent"})
	tcpListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	listener := &readObservedListener{Listener: tcpListener, readStarted: make(chan struct{}), readBytes: make(chan struct{})}
	bridge, err := newWorkflowBridgeWithBindingAndSurface(t.Context(), participant, "wefty://run-ledger", workloadrunner.WorkflowBridgeBinding{
		Listener: listener, AdvertiseHost: "127.0.0.1",
	}, workflowBridgeSurfaceComputer, true)
	if err != nil {
		t.Fatal(err)
	}
	guest, err := bridge.dial(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer guest.Close()
	select {
	case <-listener.readStarted:
	case <-t.Context().Done():
		t.Fatal("bridge did not begin reading the incomplete guest request")
	}
	// The helper pump connects this host side before a guest has connected to
	// its namespace listener, so no HTTP bytes exist to create a request context.
	if err := bridge.closeWithCause(errComputerSubmissionPolicyReminted); err != nil {
		t.Fatalf("policy remint ended the Computer attempt while closing an unused helper preconnection: %v", err)
	}
	if err := guest.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := guest.Read(make([]byte, 1)); err == nil {
		t.Fatal("unused helper preconnection remained open after policy remint")
	} else if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
		t.Fatal("unused helper preconnection was not closed before the read deadline")
	}
}

func TestComputerSubmissionPolicyRemintDrainsFourWaitingHostBridgePumpsWithoutForcedClose(t *testing.T) {
	engine := newWorkflowBridgeMarkerEngine()
	adapter, stopAdapter := startWorkflowBridgeMarkerAdapter(t, engine)
	defer stopAdapter()
	participant := plain.NewNetwork().NewFabric(fabric.Identity{NodeID: "agent"})
	bridge, err := newComputerAttemptBridge(t.Context(), participant, "wefty://run-ledger", true)
	if err != nil {
		t.Fatal(err)
	}

	runContext, cancelRun := context.WithCancel(t.Context())
	defer cancelRun()
	request := workflowBridgeMarkerRequest(bridge)
	runDone := make(chan error, 1)
	go func() {
		_, err := adapter.Run(runContext, request, nil)
		runDone <- err
	}()
	const bridgeConcurrency = 4
	for pump := 0; pump < bridgeConcurrency; pump++ {
		select {
		case <-engine.bridgeEntered:
		case <-time.After(time.Second):
			t.Fatalf("host-bridge pump %d/%d did not reach helper readiness", pump+1, bridgeConcurrency)
		}
	}
	if count := trackedWorkflowBridgeConnectionCount(bridge); count != 0 {
		t.Fatalf("waiting helper pumps eagerly created %d host bridge connection(s)", count)
	}

	started := time.Now()
	if err := bridge.closeWithCause(errComputerSubmissionPolicyReminted); err != nil {
		t.Fatalf("policy remint forced closed waiting helper pumps: %v", err)
	}
	if elapsed := time.Since(started); elapsed >= workflowBridgeCloseTimeout {
		t.Fatalf("policy remint drain elapsed=%s, reached unchanged force-close bound %s", elapsed, workflowBridgeCloseTimeout)
	}
	if count := trackedWorkflowBridgeConnectionCount(bridge); count != 0 {
		t.Fatalf("policy remint reached a force-close set of %d connection(s)", count)
	}
	cancelRun()
	select {
	case err := <-runDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled host-bridge run = %v, want context cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("adapter did not finish after canceling the waiting host-bridge pumps")
	}
}

func TestHostBridgeMarkerPreservesPausedRequestPolicyRemintResponse(t *testing.T) {
	network := plain.NewNetwork()
	l3Fabric := network.NewFabric(fabric.Identity{NodeID: "run-ledger"})
	agentFabric := network.NewFabric(fabric.Identity{NodeID: "agent"})
	l3Listener, err := l3Fabric.Listen("tcp", "wefty://run-ledger")
	if err != nil {
		t.Fatal(err)
	}
	admitted := make(chan struct{})
	canceled := make(chan struct{})
	bodyRead := make(chan error, 1)
	l3Server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		w.WriteHeader(http.StatusContinue)
		close(admitted)
		go func() {
			<-request.Context().Done()
			close(canceled)
		}()
		_, readErr := io.ReadAll(request.Body)
		bodyRead <- readErr
		panic(http.ErrAbortHandler)
	})}
	go func() { _ = l3Server.Serve(l3Listener) }()
	defer l3Server.Close()

	engine := newWorkflowBridgeMarkerEngine()
	adapter, stopAdapter := startWorkflowBridgeMarkerAdapter(t, engine)
	defer stopAdapter()
	bridge, err := newComputerAttemptBridge(t.Context(), agentFabric, "wefty://run-ledger", true)
	if err != nil {
		t.Fatal(err)
	}
	runContext, cancelRun := context.WithCancel(t.Context())
	defer cancelRun()
	request := workflowBridgeMarkerRequest(bridge)
	runDone := make(chan error, 1)
	go func() {
		_, err := adapter.Run(runContext, request, nil)
		runDone <- err
	}()
	const bridgeConcurrency = 4
	for pump := 0; pump < bridgeConcurrency; pump++ {
		select {
		case <-engine.bridgeEntered:
		case <-time.After(time.Second):
			t.Fatalf("host-bridge pump %d/%d did not reach helper readiness", pump+1, bridgeConcurrency)
		}
	}
	guest, helperGuest := net.Pipe()
	defer guest.Close()
	engine.guestConnections <- helperGuest
	deadline := time.Now().Add(time.Second)
	for trackedWorkflowBridgeConnectionCount(bridge) != 1 && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if count := trackedWorkflowBridgeConnectionCount(bridge); count != 1 {
		t.Fatalf("backend-ready marker established %d host connections, want 1", count)
	}

	httpRequest, err := http.NewRequest(http.MethodPost, bridge.l3Endpoint+"/v1/runs", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(guest, "POST /l3/v1/runs HTTP/1.1\r\nHost: wefty.invalid\r\nContent-Length: 1\r\nExpect: 100-continue\r\nConnection: close\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-admitted:
	case <-time.After(time.Second):
		t.Fatal("marker-backed paused request did not reach upstream admission")
	}
	reader := bufio.NewReader(guest)
	interim, err := http.ReadResponse(reader, httpRequest)
	if err != nil {
		t.Fatal(err)
	}
	interim.Body.Close()
	if interim.StatusCode != http.StatusContinue {
		t.Fatalf("paused request acknowledgement status=%d, want 100", interim.StatusCode)
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- bridge.closeWithCause(errComputerSubmissionPolicyReminted) }()
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("policy remint did not cancel the marker-backed request")
	}
	if _, err := io.WriteString(guest, "x"); err != nil {
		t.Fatalf("release marker-backed paused body after policy remint: %v", err)
	}
	var response *http.Response
	for {
		response, err = http.ReadResponse(reader, httpRequest)
		if err != nil {
			t.Fatalf("marker-backed request received EOF instead of typed policy-remint response: %v", err)
		}
		if response.StatusCode != http.StatusContinue {
			break
		}
		response.Body.Close()
	}
	defer response.Body.Close()
	var typed contract.ErrorResponse
	if err := json.NewDecoder(response.Body).Decode(&typed); err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusBadGateway || typed.Error.Code != contract.ErrorPassUnavailable || typed.Error.Message != errComputerSubmissionPolicyReminted.Error() {
		t.Fatalf("marker-backed policy-remint response = status %d error %#v, want typed 502", response.StatusCode, typed.Error)
	}
	if err := <-closeDone; err != nil {
		t.Fatalf("marker-backed request did not drain: %v", err)
	}
	select {
	case <-bodyRead:
	case <-time.After(time.Second):
		t.Fatal("marker-backed upstream body reader did not drain")
	}
	cancelRun()
	select {
	case err := <-runDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled marker-backed host-bridge run = %v, want context cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("adapter did not finish after marker-backed request cancellation")
	}
}

type workflowBridgeMarkerEngine struct {
	ocihelper.UnavailableEngine
	bridgeEntered    chan struct{}
	guestConnections chan net.Conn
}

func newWorkflowBridgeMarkerEngine() *workflowBridgeMarkerEngine {
	return &workflowBridgeMarkerEngine{bridgeEntered: make(chan struct{}, 4), guestConnections: make(chan net.Conn, 1)}
}

func (*workflowBridgeMarkerEngine) EnsureImage(_ context.Context, _ ocihelper.EnsureImageRequest, _ io.Reader, emit func(ocihelper.EnsureImageEvent) error) error {
	response := workflowBridgeMarkerImage()
	return emit(ocihelper.EnsureImageEvent{Kind: ocihelper.ImageComplete, Result: &response})
}

func (*workflowBridgeMarkerEngine) Run(_ context.Context, request ocihelper.RunRequest) (ocihelper.RunResponse, error) {
	image := workflowBridgeMarkerImage().Evidence
	response := ocihelper.RunResponse{Started: true, StartedAt: time.Now().UTC(), Image: &image}
	if !strings.HasPrefix(request.Authority.JobID, "probe-") {
		response.HostBridgeReady = true
		response.HostBridgeEndpoint = "http://127.0.0.1:42002/l3"
	}
	return response, nil
}

func (*workflowBridgeMarkerEngine) Watch(ctx context.Context, request ocihelper.WatchRequest, emit func(ocihelper.WatchEvent) error) error {
	if !strings.HasPrefix(request.Authority.JobID, "probe-") {
		<-ctx.Done()
		return ctx.Err()
	}
	exitCode := 0
	return emit(ocihelper.WatchEvent{Kind: ocihelper.WatchComplete, Result: &ocihelper.WatchResponse{ExitCode: &exitCode}})
}

func (*workflowBridgeMarkerEngine) Delete(context.Context, ocihelper.DeleteRequest) (ocihelper.DeleteResponse, error) {
	return ocihelper.DeleteResponse{Deleted: true}, nil
}

func (*workflowBridgeMarkerEngine) Sweep(context.Context, ocihelper.SweepRequest) (ocihelper.SweepResponse, error) {
	return ocihelper.SweepResponse{SweepEpoch: "workflow-bridge-marker-sweep"}, nil
}

func (*workflowBridgeMarkerEngine) Verify(context.Context, ocihelper.VerifyRequest) (ocihelper.VerifyResponse, error) {
	return ocihelper.VerifyResponse{Absent: true}, nil
}

func (*workflowBridgeMarkerEngine) ReapSession(context.Context, ocihelper.SessionIdentity) (ocihelper.SweepResponse, error) {
	return ocihelper.SweepResponse{SweepEpoch: "workflow-bridge-marker-reap"}, nil
}

func (engine *workflowBridgeMarkerEngine) DialHostBridge(ctx context.Context, _ ocihelper.DialHostBridgeRequest, stream io.ReadWriteCloser) error {
	engine.bridgeEntered <- struct{}{}
	select {
	case guest := <-engine.guestConnections:
		defer guest.Close()
		if _, err := stream.Write([]byte{1}); err != nil {
			return err
		}
		return ocihelper.Relay(ctx, stream, guest)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func workflowBridgeMarkerImage() ocihelper.EnsureImageResponse {
	const digest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	return ocihelper.EnsureImageResponse{
		TopLevelDigest: digest, PlatformDigest: digest,
		Evidence: ocihelper.ImageEvidence{
			SubmittedReference: "example.invalid/image", TopLevelDigest: digest,
			TopLevelMediaType: "application/vnd.oci.image.manifest.v1+json", PlatformManifestDigest: digest,
			Platform:       ocihelper.OCIPlatform{OS: "linux", Architecture: "amd64"},
			RuntimeHandler: ocihelper.DefaultRuntimeHandler, Snapshotter: ocihelper.DefaultSnapshotter,
		},
	}
}

func startWorkflowBridgeMarkerAdapter(t *testing.T, engine ocihelper.Engine) (*ocirunner.Adapter, func()) {
	t.Helper()
	barrier, stop := startPreAdmissionHelper(t, engine, time.Now)
	if err := barrier.Ensure(t.Context()); err != nil {
		stop()
		t.Fatal(err)
	}
	adapter := ocirunner.NewAdapter(barrier)
	const digest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	if err := adapter.Probe(t.Context(), "pre-admission-node", "pre-admission-boot", "example.invalid/image", digest, time.Second); err != nil {
		stop()
		t.Fatal(err)
	}
	return adapter, stop
}

func workflowBridgeMarkerRequest(bridge *workflowBridge) workloadrunner.Request {
	digest := "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	return workloadrunner.Request{
		Authority: workloadrunner.AttemptAuthority{
			NodeID: "pre-admission-node", BootSessionID: "pre-admission-boot", JobID: "bridge-job", AttemptID: "bridge-attempt",
			FencingToken: "bridge-fence", WorkloadClass: contract.JobClassOneShot, RemovalGeneration: "bridge-removal",
		},
		RuntimeHandler: ocihelper.DefaultRuntimeHandler,
		Execution: contract.ExecutionSpec{OCI: &contract.OCIExecutionSpec{
			Image: contract.OCIImageSpec{Reference: "example.invalid/image", Digest: &digest}, Argv: []string{"/bin/true"},
		}},
		InitialDeadman:           time.Second,
		OCIImageResolved:         func(context.Context, workloadrunner.OCIImageObservation) error { return nil },
		OCIStarted:               func(context.Context, workloadrunner.OCIImageObservation) error { return nil },
		HostBridgeDial:           bridge.dial,
		HostBridgeFallbackActive: true,
	}
}

func TestWorkflowBridgeUntracksClosedConnections(t *testing.T) {
	participant := plain.NewNetwork().NewFabric(fabric.Identity{NodeID: "agent"})
	tcpListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	listener := &readObservedListener{Listener: tcpListener, readStarted: make(chan struct{}), readBytes: make(chan struct{})}
	bridge, err := newWorkflowBridgeWithBindingAndSurface(t.Context(), participant, "wefty://run-ledger", workloadrunner.WorkflowBridgeBinding{
		Listener: listener, AdvertiseHost: "127.0.0.1",
	}, workflowBridgeSurfaceComputer, true)
	if err != nil {
		t.Fatal(err)
	}
	guest, err := bridge.dial(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer guest.Close()
	select {
	case <-listener.readStarted:
	case <-t.Context().Done():
		t.Fatal("bridge did not begin reading the inactive connection")
	}
	if err := bridge.closeWithCause(errComputerSubmissionPolicyReminted); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for trackedWorkflowBridgeConnectionCount(bridge) != 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if count := trackedWorkflowBridgeConnectionCount(bridge); count != 0 {
		t.Fatalf("closed workflow bridge retained %d tracked connection(s)", count)
	}
}

func trackedWorkflowBridgeConnectionCount(bridge *workflowBridge) int {
	count := 0
	bridge.connections.Range(func(_, _ any) bool {
		count++
		return true
	})
	return count
}

func TestComputerBridgeClosePreservesNonDeadlineShutdownError(t *testing.T) {
	participant := plain.NewNetwork().NewFabric(fabric.Identity{NodeID: "agent"})
	tcpListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	closeFailure := errors.New("close listener failed")
	listener := &closeErrorListener{Listener: tcpListener, err: closeFailure, acceptStarted: make(chan struct{})}
	bridge, err := newWorkflowBridgeWithBindingAndSurface(t.Context(), participant, "wefty://run-ledger", workloadrunner.WorkflowBridgeBinding{
		Listener: listener, AdvertiseHost: "127.0.0.1",
	}, workflowBridgeSurfaceComputer, true)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-listener.acceptStarted:
	case <-t.Context().Done():
		t.Fatal("bridge did not begin accepting connections")
	}
	if err := bridge.closeWithCause(errComputerSubmissionPolicyReminted); !errors.Is(err, closeFailure) {
		t.Fatalf("Computer bridge close error = %v, want listener failure", err)
	}
}

func TestComputerSubmissionPolicyRemintSurfacesInactiveConnectionCloseError(t *testing.T) {
	participant := plain.NewNetwork().NewFabric(fabric.Identity{NodeID: "agent"})
	tcpListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	closeFailure := errors.New("close inactive connection failed")
	listener := &acceptedCloseErrorListener{Listener: tcpListener, err: closeFailure, readStarted: make(chan struct{})}
	bridge, err := newWorkflowBridgeWithBindingAndSurface(t.Context(), participant, "wefty://run-ledger", workloadrunner.WorkflowBridgeBinding{
		Listener: listener, AdvertiseHost: "127.0.0.1",
	}, workflowBridgeSurfaceComputer, true)
	if err != nil {
		t.Fatal(err)
	}
	guest, err := bridge.dial(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer guest.Close()
	select {
	case <-listener.readStarted:
	case <-t.Context().Done():
		t.Fatal("bridge did not begin reading the inactive connection")
	}
	if err := bridge.closeWithCause(errComputerSubmissionPolicyReminted); !errors.Is(err, closeFailure) {
		t.Fatalf("inactive connection close error = %v, want surfaced close failure", err)
	}
}

func TestWorkflowBridgeDrainTimeoutRemainsObservable(t *testing.T) {
	participant := plain.NewNetwork().NewFabric(fabric.Identity{NodeID: "agent"})
	tcpListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	listener := &readObservedListener{Listener: tcpListener, readStarted: make(chan struct{}), readBytes: make(chan struct{})}
	bridge, err := newWorkflowBridgeWithBinding(t.Context(), participant, "wefty://run-ledger", workloadrunner.WorkflowBridgeBinding{
		Listener: listener, AdvertiseHost: "127.0.0.1",
	})
	if err != nil {
		t.Fatal(err)
	}
	guest, err := bridge.dial(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer guest.Close()
	select {
	case <-listener.readStarted:
	case <-t.Context().Done():
		t.Fatal("workflow bridge did not begin reading the incomplete request")
	}
	if err := bridge.close(); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ordinary workflow bridge close error = %v, want bounded drain timeout", err)
	}
}

func TestComputerSubmissionPolicyRemintClosesPartialHeaderConnection(t *testing.T) {
	participant := plain.NewNetwork().NewFabric(fabric.Identity{NodeID: "agent"})
	tcpListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	listener := &readObservedListener{Listener: tcpListener, readStarted: make(chan struct{}), readBytes: make(chan struct{})}
	bridge, err := newWorkflowBridgeWithBindingAndSurface(t.Context(), participant, "wefty://run-ledger", workloadrunner.WorkflowBridgeBinding{
		Listener: listener, AdvertiseHost: "127.0.0.1",
	}, workflowBridgeSurfaceComputer, true)
	if err != nil {
		t.Fatal(err)
	}
	guest, err := bridge.dial(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer guest.Close()
	if _, err := guest.Write([]byte("G")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-listener.readBytes:
	case <-t.Context().Done():
		t.Fatal("bridge did not observe the started guest request")
	}
	if err := bridge.closeWithCause(errComputerSubmissionPolicyReminted); err != nil {
		t.Fatalf("policy remint ended the Computer attempt while closing a partial-header connection: %v", err)
	}
	if err := guest.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := guest.Read(make([]byte, 1)); err == nil {
		t.Fatal("partial-header connection remained open after policy remint")
	} else if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
		t.Fatal("partial-header connection was not closed before the read deadline")
	}
}

func TestComputerSubmissionPolicyRemintReturnsTypedFailureToActiveRequest(t *testing.T) {
	network := plain.NewNetwork()
	l3Fabric := network.NewFabric(fabric.Identity{NodeID: "run-ledger"})
	agentFabric := network.NewFabric(fabric.Identity{NodeID: "agent"})
	l3Listener, err := l3Fabric.Listen("tcp", "wefty://run-ledger")
	if err != nil {
		t.Fatal(err)
	}
	admitted := make(chan struct{})
	canceled := make(chan struct{})
	bodyRead := make(chan error, 1)
	l3Server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		w.WriteHeader(http.StatusContinue)
		close(admitted)
		go func() {
			<-request.Context().Done()
			close(canceled)
		}()
		_, readErr := io.ReadAll(request.Body)
		bodyRead <- readErr
		panic(http.ErrAbortHandler)
	})}
	go func() { _ = l3Server.Serve(l3Listener) }()
	defer l3Server.Close()
	bridge, err := newComputerAttemptBridge(t.Context(), agentFabric, "wefty://run-ledger", true)
	if err != nil {
		t.Fatal(err)
	}
	guest, err := bridge.dial(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer guest.Close()
	request, err := http.NewRequest(http.MethodPost, bridge.l3Endpoint+"/v1/runs", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(guest, "POST /l3/v1/runs HTTP/1.1\r\nHost: wefty.invalid\r\nContent-Length: 1\r\nExpect: 100-continue\r\nConnection: close\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-admitted:
	case <-t.Context().Done():
		t.Fatal("paused request did not reach upstream admission")
	}
	reader := bufio.NewReader(guest)
	interim, err := http.ReadResponse(reader, request)
	if err != nil {
		t.Fatal(err)
	}
	interim.Body.Close()
	if interim.StatusCode != http.StatusContinue {
		t.Fatalf("paused request acknowledgement status=%d, want 100", interim.StatusCode)
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- bridge.closeWithCause(errComputerSubmissionPolicyReminted) }()
	select {
	case <-canceled:
	case <-t.Context().Done():
		t.Fatal("policy remint did not cancel the admitted upstream request")
	}
	if _, err := io.WriteString(guest, "x"); err != nil {
		t.Fatalf("release paused request body after policy remint: %v", err)
	}
	var response *http.Response
	for {
		response, err = http.ReadResponse(reader, request)
		if err != nil {
			t.Fatalf("active request received EOF instead of typed policy-remint response: %v", err)
		}
		if response.StatusCode != http.StatusContinue {
			break
		}
		response.Body.Close()
	}
	defer response.Body.Close()
	var typed contract.ErrorResponse
	if err := json.NewDecoder(response.Body).Decode(&typed); err != nil {
		t.Fatalf("decode policy-remint response status=%d headers=%v: %v", response.StatusCode, response.Header, err)
	}
	if response.StatusCode != http.StatusBadGateway || typed.Error.Code != contract.ErrorPassUnavailable ||
		typed.Error.Message != errComputerSubmissionPolicyReminted.Error() {
		t.Fatalf("policy-remint response = status %d error %#v, want typed 502", response.StatusCode, typed.Error)
	}
	if err := <-closeDone; err != nil {
		t.Fatalf("drained active request ended Computer rotation: %v", err)
	}
	select {
	case <-bodyRead:
	case <-t.Context().Done():
		t.Fatal("upstream body reader did not drain after request cancellation")
	}
}

func TestComputerSubmissionPolicyRemintReportsUndrainedActiveRequest(t *testing.T) {
	network := plain.NewNetwork()
	l3Fabric := network.NewFabric(fabric.Identity{NodeID: "run-ledger"})
	agentFabric := network.NewFabric(fabric.Identity{NodeID: "agent"})
	l3Listener, err := l3Fabric.Listen("tcp", "wefty://run-ledger")
	if err != nil {
		t.Fatal(err)
	}
	admitted := make(chan struct{})
	bodyRead := make(chan struct{})
	l3Server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		w.WriteHeader(http.StatusContinue)
		close(admitted)
		_, _ = io.ReadAll(request.Body)
		close(bodyRead)
		panic(http.ErrAbortHandler)
	})}
	go func() { _ = l3Server.Serve(l3Listener) }()
	defer l3Server.Close()
	bridge, err := newComputerAttemptBridge(t.Context(), agentFabric, "wefty://run-ledger", true)
	if err != nil {
		t.Fatal(err)
	}
	guest, err := bridge.dial(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodPost, bridge.l3Endpoint+"/v1/runs", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(guest, "POST /l3/v1/runs HTTP/1.1\r\nHost: wefty.invalid\r\nContent-Length: 1\r\nExpect: 100-continue\r\nConnection: close\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-admitted:
	case <-t.Context().Done():
		t.Fatal("paused request did not reach upstream admission")
	}
	interim, err := http.ReadResponse(bufio.NewReader(guest), request)
	if err != nil {
		t.Fatal(err)
	}
	interim.Body.Close()
	if interim.StatusCode != http.StatusContinue {
		t.Fatalf("paused request acknowledgement status=%d, want 100", interim.StatusCode)
	}
	drainErr := bridge.closeWithCause(errComputerSubmissionPolicyReminted)
	if err := guest.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		t.Fatal(err)
	}
	select {
	case <-bodyRead:
	case <-t.Context().Done():
		t.Fatal("closing the paused guest did not release the upstream body reader")
	}
	var typed workflowBridgeDrainFailure
	if !errors.As(drainErr, &typed) || typed.ActiveConnectionCount() != 1 ||
		drainErr.Error() != "bridge drain incomplete: 1 active connection" {
		t.Fatalf("undrained active request error = %v, want typed one-connection failure", drainErr)
	}
}

func TestComputerBridgeNeverSynthesizesAuthorizationAfterUpstreamCommit(t *testing.T) {
	network := plain.NewNetwork()
	l3Fabric := network.NewFabric(fabric.Identity{NodeID: "run-ledger"})
	agentFabric := network.NewFabric(fabric.Identity{NodeID: "agent"})
	l3Listener, err := l3Fabric.Listen("tcp", "wefty://run-ledger")
	if err != nil {
		t.Fatal(err)
	}
	committed := make(chan struct{})
	canceled := make(chan struct{})
	l3Server := &http.Server{Handler: http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
		close(committed)
		<-request.Context().Done()
		close(canceled)
	})}
	go func() { _ = l3Server.Serve(l3Listener) }()
	defer l3Server.Close()
	bridge, err := newComputerAttemptBridge(t.Context(), agentFabric, "wefty://run-ledger", true)
	if err != nil {
		t.Fatal(err)
	}
	type bridgeResponse struct {
		status int
		body   contract.ErrorResponse
		err    error
	}
	requestDone := make(chan bridgeResponse, 1)
	go func() {
		response, requestErr := http.Get(bridge.l3Endpoint + "/v1/runs/run-committed")
		result := bridgeResponse{err: requestErr}
		if response != nil {
			result.status = response.StatusCode
			result.err = errors.Join(result.err, json.NewDecoder(response.Body).Decode(&result.body), response.Body.Close())
		}
		requestDone <- result
	}()
	select {
	case <-committed:
	case <-t.Context().Done():
		t.Fatal("fake L3 did not commit before bridge closure")
	}
	if err := bridge.closeWithCause(errComputerSubmissionRevoked); err != nil {
		t.Fatal(err)
	}
	select {
	case <-canceled:
	case <-t.Context().Done():
		t.Fatal("fake L3 did not observe cancellation after commit")
	}
	select {
	case result := <-requestDone:
		if result.err != nil || result.status != http.StatusBadGateway || result.body.Error.Code != contract.ErrorPassUnavailable {
			t.Fatalf("post-commit cancellation response = status %d error %#v err %v, want typed indeterminate outcome", result.status, result.body.Error, result.err)
		}
		if result.status == http.StatusUnauthorized || result.body.Error.Code == contract.ErrorUnauthorized {
			t.Fatalf("bridge fabricated authorization verdict after upstream commit: status %d error %#v", result.status, result.body.Error)
		}
	case <-t.Context().Done():
		t.Fatal("post-commit cancellation did not return to the guest")
	}
}

func TestWorkflowBridgeRejectsWildcardListener(t *testing.T) {
	listener, err := net.Listen("tcp4", "0.0.0.0:0")
	if err != nil {
		t.Fatal(err)
	}
	participant := plain.NewNetwork().NewFabric(fabric.Identity{NodeID: "agent"})
	_, err = newWorkflowBridgeWithBinding(t.Context(), participant, "wefty://run-ledger", workloadrunner.WorkflowBridgeBinding{
		Listener: listener, AdvertiseHost: "0.0.0.0",
	})
	if err == nil || !strings.Contains(err.Error(), "unspecified") {
		t.Fatalf("wildcard binding error = %v", err)
	}
}
