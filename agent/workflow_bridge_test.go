package agent

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
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
		{http.MethodGet, "/v1/runs/run-1/envelopes?limit=7&cursor=next"},
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
		{http.MethodPost, "/v1/runs/run-1/envelopes"},
		{http.MethodPost, "/v1/runs/run-1/gates"},
		{http.MethodPost, "/v1/runs/run-1/rerun"},
		{http.MethodPost, "/v1/runs/run-1/cancel"},
		{http.MethodDelete, "/v1/runs/run-1"},
		{http.MethodPatch, "/v1/runs/run-1"},
		{http.MethodPut, "/v1/runs/run-1"},
		{http.MethodGet, "/v1/runs/run-1/gates"},
		{http.MethodGet, "/v1/runs/run-1/rerun"},
		{http.MethodGet, "/v1/runs/run-1/cancel"},
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

func TestComputerAttemptBridgeDisableCancelsInflightAndReenableRestoresTransport(t *testing.T) {
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
	bridge, err := newComputerAttemptBridge(t.Context(), agentFabric, "wefty://run-ledger", true)
	if err != nil {
		t.Fatal(err)
	}
	defer bridge.close()
	requestDone := make(chan error, 1)
	go func() {
		response, err := http.Get(bridge.l3Endpoint + "/v1/runs/run-1")
		if response != nil {
			response.Body.Close()
		}
		requestDone <- err
	}()
	select {
	case <-started:
	case <-t.Context().Done():
		t.Fatal("in-flight request did not reach L3")
	}
	bridge.setReachable(false)
	select {
	case <-canceled:
	case <-t.Context().Done():
		t.Fatal("disable did not cancel in-flight L3 traffic")
	}
	select {
	case <-requestDone:
	case <-t.Context().Done():
		t.Fatal("canceled bridge request did not return")
	}
	response, err := http.Get(bridge.l3Endpoint + "/v1/computer/self")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable || calls.Load() != 1 {
		t.Fatalf("disabled bridge status=%d L3 calls=%d, want 503 and one call", response.StatusCode, calls.Load())
	}
	bridge.setReachable(true)
	response, err = http.Get(bridge.l3Endpoint + "/v1/computer/self")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK || calls.Load() != 2 {
		t.Fatalf("re-enabled bridge status=%d L3 calls=%d, want 200 and two calls", response.StatusCode, calls.Load())
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

func TestAttemptBridgeEligibilityAddsOnlyComputerServices(t *testing.T) {
	computer := contract.ExecutionSpec{OCI: &contract.OCIExecutionSpec{Computer: &contract.OCIComputerSpec{DiskBytes: 8 << 30}}}
	if !needsAttemptBridge(contract.JobClassOneShot, contract.ExecutionSpec{}) {
		t.Fatal("one-shot workflow bridge eligibility was removed")
	}
	if !needsAttemptBridge(contract.JobClassService, computer) {
		t.Fatal("Computer service did not receive an attempt bridge")
	}
	if needsAttemptBridge(contract.JobClassService, contract.ExecutionSpec{}) {
		t.Fatal("plain service received a workflow bridge")
	}
}
