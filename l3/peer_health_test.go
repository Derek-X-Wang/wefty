package l3

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/fabric"
)

func TestPeerHealthRejectsBearerBeforeL1(t *testing.T) {
	h := newHoldHTTPHarness(t)
	calls := 0
	h.l1Client.client.Transport = recoveryRoundTripper(func(*http.Request) (*http.Response, error) {
		calls++
		t.Error("peer health bearer touched L1 client")
		return nil, context.Canceled
	})
	caller := h.client(fabric.Identity{NodeID: "client", Tags: []string{DefaultCallerPrincipalTag}}, DefaultL3Address)
	code, _, body := h.do(caller, "GET", "/v1/peer-health", nil, http.Header{"Authorization": {"Bearer must-not-be-authenticated"}})
	if code != 403 || calls != 0 || !strings.Contains(string(body), "not authorized") {
		t.Fatalf("status=%d calls=%d body=%s", code, calls, body)
	}
	// The control plane's existing health permission remains refused.
	control := h.client(fabric.Identity{NodeID: "control-plane"}, DefaultL3Address)
	code, _, body = h.do(control, "GET", "/v1/health", nil, nil)
	if code != 403 {
		t.Fatalf("existing health permission widened: %d %s", code, body)
	}
	code, _, body = h.do(control, "GET", "/v1/peer/ping", nil, nil)
	if code != 200 || string(body) != "{\"ok\":true}\n" {
		t.Fatalf("peer ping: %d %s", code, body)
	}
}
