package l1

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
)

func TestL1ErrorRequestIDBodyHeaderAndLog(t *testing.T) {
	store, err := OpenStore(filepath.Join(t.TempDir(), "control.sqlite"), StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	participant := plain.NewNetwork().NewFabric(fabric.Identity{NodeID: "control"})
	server, err := NewServer(participant, store, ServerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	logs := &recordedLog{}
	server.logf = logs.record
	endpoint := httptest.NewServer(server.Handler())
	defer endpoint.Close()
	seen := map[string]bool{}
	for _, path := range []string{"/v1/nodes", "/v1/nodes", "/v1/unknown"} {
		request, _ := http.NewRequest(http.MethodGet, endpoint.URL+path, nil)
		request.Header.Set("X-Request-Id", "untrusted-caller-id")
		response, err := endpoint.Client().Do(request)
		if err != nil {
			t.Fatal(err)
		}
		var envelope contract.ErrorResponse
		err = json.NewDecoder(response.Body).Decode(&envelope)
		response.Body.Close()
		id := response.Header.Get("X-Request-Id")
		if err != nil || response.StatusCode < 400 || id == "" || id == "untrusted-caller-id" || seen[id] || envelope.Error.RequestID != id || !strings.Contains(logs.text(), "request_id="+id) {
			t.Fatalf("status=%d id=%q error=%+v logs=%s decode=%v", response.StatusCode, id, envelope.Error, logs.text(), err)
		}
		seen[id] = true
	}
}

// Compare every response field except the independently generated request ID;
// unknown additions must still fail the anti-oracle equality checks.
func errorBodyWithoutRequestID(t *testing.T, body []byte) []byte {
	t.Helper()
	var document map[string]any
	if err := json.Unmarshal(body, &document); err != nil {
		t.Fatal(err)
	}
	errorObject, ok := document["error"].(map[string]any)
	if !ok {
		t.Fatalf("missing error object: %s", body)
	}
	if id, ok := errorObject["request_id"].(string); !ok || id == "" {
		t.Fatalf("missing request ID: %s", body)
	}
	delete(errorObject, "request_id")
	normalized, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	return normalized
}

func TestL1AgentClaimLogsOnlyErrors(t *testing.T) {
	h := newIntegrationHarness(t, map[string][]string{"node-1": {"kind:process"}})
	logs := &recordedLog{}
	h.server.logf = logs.record
	agent := h.client(fabric.Identity{NodeID: "node-1", Tags: []string{DefaultAgentPrincipalTag}})
	h.register(agent, "node-1")
	status, headers, body := h.do(agent, http.MethodPost, "/v1/agent/jobs/claim", ClaimRequest{NodeID: "node-1", BootSessionID: "boot-node-1", Class: contract.JobClassOneShot})
	if status != 204 || headers.Get(contract.RequestIDHeader) == "" {
		t.Fatalf("status=%d body=%s", status, body)
	}
	if strings.Contains(logs.text(), "event=l1_request") {
		t.Fatalf("routine agent traffic logged: %s", logs.text())
	}
	status, headers, body = h.do(agent, http.MethodPost, "/v1/agent/jobs/claim", ClaimRequest{NodeID: "node-1", BootSessionID: "boot-node-1", Class: "invalid"})
	if status != 400 || !strings.Contains(logs.text(), "request_id="+headers.Get(contract.RequestIDHeader)) {
		t.Fatalf("status=%d body=%s logs=%s", status, body, logs.text())
	}
}
