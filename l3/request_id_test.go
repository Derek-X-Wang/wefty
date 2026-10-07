package l3

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
	"github.com/Derek-X-Wang/wefty/l1"
)

type requestLog struct {
	mu   sync.Mutex
	text string
}

func (l *requestLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.text += string(p)
	return len(p), nil
}
func (l *requestLog) contains(s string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Contains(l.text, s)
}

func TestL3ErrorRequestIDBodyHeaderAndLog(t *testing.T) {
	logs := &requestLog{}
	logger := log.New(logs, "", 0)
	store, err := OpenStore(filepath.Join(t.TempDir(), "ledger.sqlite"), StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	participant := plain.NewNetwork().NewFabric(fabric.Identity{NodeID: "ledger"})
	server, err := NewServer(participant, store, ServerConfig{Logf: logger.Printf})
	if err != nil {
		t.Fatal(err)
	}
	endpoint := httptest.NewServer(server.Handler())
	defer endpoint.Close()
	seen := map[string]bool{}
	for _, path := range []string{"/v1/runs/missing", "/v1/runs/missing", "/v1/unknown"} {
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
		if err != nil || response.StatusCode < 400 || id == "" || id == "untrusted-caller-id" || seen[id] || envelope.Error.RequestID != id || !logs.contains("request_id="+id) {
			t.Fatalf("status=%d id=%q error=%+v logged=%t decode=%v", response.StatusCode, id, envelope.Error, logs.contains("request_id="+id), err)
		}
		seen[id] = true
	}
}

// An alternate L1 log client supplies the same typed error as the HTTP client.
type requestIDLogFailure struct{ failure *Error }

func (c requestIDLogFailure) GetJobLogs(context.Context, string, string, int) (l1.LogPage, error) {
	return l1.LogPage{}, c.failure
}

func TestL3RelayedErrorKeepsBothRequestIDs(t *testing.T) {
	logs := &requestLog{}
	logger := log.New(logs, "", 0)
	h := newIntegrationHarness(t)
	h.l3Server.logf = logger.Printf
	accepted := h.submit(inlineRunRequest("#!/bin/sh\necho test\n"), "correlation-test")
	reconciler, err := NewReconciler(h.l3Store, h.l1Client, ReconcilerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := reconciler.ReconcileOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, code := range []contract.ErrorCode{contract.ErrorConflict, contract.ErrorInternal} {
		source := &Error{Code: code, Message: "upstream failure", Retryable: code == contract.ErrorInternal, RequestID: "l1-correlation-id", Details: map[string]any{"reason": "upstream"}}
		h.l3Server.logs = requestIDLogFailure{source}
		status, headers, body := h.do(h.caller, http.MethodGet, "/v1/runs/"+accepted.RunID+"/logs", nil, nil)
		var envelope contract.ErrorResponse
		if err := json.Unmarshal(body, &envelope); err != nil {
			t.Fatal(err)
		}
		local := headers.Get("X-L3-Request-Id")
		if status < 400 || local == "" || local == source.RequestID || headers.Get("X-Request-Id") != source.RequestID || envelope.Error.RequestID != source.RequestID || envelope.Error.Details["l3_request_id"] != local || !logs.contains("request_id="+local+" upstream_request_id="+strconv.Quote(source.RequestID)) {
			t.Fatalf("status=%d headers=%v error=%+v", status, headers, envelope.Error)
		}
		if source.Details["l3_request_id"] != nil {
			t.Fatal("upstream error details mutated")
		}
		if code == contract.ErrorConflict && envelope.Error.Details["reason"] != "upstream" {
			t.Fatal("upstream details lost")
		}
		if code == contract.ErrorInternal && (envelope.Error.Details["reason"] != nil || envelope.Error.Message != "internal server error" || !envelope.Error.Retryable) {
			t.Fatalf("internal scrubbing changed: %+v", envelope.Error)
		}
	}
}
