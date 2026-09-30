package agent

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
)

type countingRoundTripper struct {
	calls         atomic.Int64
	authorization atomic.Value
	path          atomic.Value
}

func (c *countingRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	c.calls.Add(1)
	c.authorization.Store(request.Header.Get("Authorization"))
	c.path.Store(request.URL.Path)
	return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("")), Header: http.Header{}, Request: request}, nil
}

// The guard is the enforcement, so it must hold without the inbound check: it
// judges the request after the proxy's hop-by-hop cleanup, the one L3 gets.
func TestRunLedgerProxyJudgesTheOutboundRequestNotTheInboundOne(t *testing.T) {
	upstream := &countingRoundTripper{}
	proxy := newRunLedgerProxy(upstream)

	for _, connection := range []string{"Authorization", "keep-alive, authorization"} {
		request := httptest.NewRequest(http.MethodGet, "http://bridge.invalid/l3/v1/runs/run-other", nil)
		request.Header.Set("Authorization", "Bearer anything")
		request.Header.Set("Connection", connection)
		response := httptest.NewRecorder()
		proxy.ServeHTTP(response, request)
		var typed contract.ErrorResponse
		if response.Code != http.StatusUnauthorized || json.Unmarshal(response.Body.Bytes(), &typed) != nil || typed.Error.Code != contract.ErrorUnauthorized {
			t.Errorf("Connection %q: status=%d body=%s; want typed 401", connection, response.Code, response.Body.Bytes())
		}
	}
	for _, path := range []string{"/l3/v1/computer-token/revoke-host", "/l3/v1/runs", "/l3/v1/runs/run-1/rerun"} {
		request := httptest.NewRequest(http.MethodGet, "http://bridge.invalid"+path, nil)
		request.Header.Set("Authorization", "Bearer anything")
		response := httptest.NewRecorder()
		proxy.ServeHTTP(response, request)
		if response.Code != http.StatusForbidden {
			t.Errorf("GET %s: status=%d; want 403", path, response.Code)
		}
	}
	if calls := upstream.calls.Load(); calls != 0 {
		t.Fatalf("guard let %d refused request(s) through", calls)
	}

	request := httptest.NewRequest(http.MethodGet, "http://bridge.invalid/l3/v1/runs/run-1", nil)
	request.Header.Set("Authorization", "Bearer run-token")
	request.Header.Set("Connection", "keep-alive")
	response := httptest.NewRecorder()
	proxy.ServeHTTP(response, request)
	if response.Code != http.StatusOK || upstream.calls.Load() != 1 ||
		upstream.authorization.Load() != "Bearer run-token" || upstream.path.Load() != "/v1/runs/run-1" {
		t.Fatalf("allowed request: status=%d calls=%d authorization=%v path=%v", response.Code, upstream.calls.Load(),
			upstream.authorization.Load(), upstream.path.Load())
	}
}
