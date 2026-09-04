package agent

import (
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
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l3"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
)

type staticComputerGrantVerifier struct{ proof l3.ComputerTokenScopeProof }

func (verifier staticComputerGrantVerifier) ProveComputerTokenScope(context.Context, string, string, string, string) (l3.ComputerTokenScopeProof, error) {
	return verifier.proof, nil
}

type workflowBridgeBinderFunc func(context.Context) (workloadrunner.WorkflowBridgeBinding, error)

func (bind workflowBridgeBinderFunc) Bind(ctx context.Context) (workloadrunner.WorkflowBridgeBinding, error) {
	return bind(ctx)
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
