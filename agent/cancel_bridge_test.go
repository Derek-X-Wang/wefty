package agent

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCancelPassesThroughAttemptCredentialBridge(t *testing.T) {
	bridge := &workflowBridge{}
	for _, probe := range []struct {
		method, path string
		allowed      bool
	}{
		{http.MethodPost, "/l1/v1/jobs/child/cancel", true},
		{http.MethodGet, "/l1/v1/jobs/child/cancel", false},
		{http.MethodPost, "/l1/v1/jobs/child/remove", false},
	} {
		request := httptest.NewRequest(probe.method, probe.path, nil)
		request.Header.Set("Authorization", "Bearer parent-attempt")
		forwarded := false
		handler := bridge.controlPlaneHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			forwarded = true
			if r.Header.Get("Authorization") != "Bearer parent-attempt" {
				t.Error("credential not forwarded")
			}
			w.WriteHeader(http.StatusOK)
		}))
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if forwarded != probe.allowed {
			t.Fatalf("%s %s forwarded=%v response=%d %s", probe.method, probe.path, forwarded, response.Code, response.Body.String())
		}
	}
}
