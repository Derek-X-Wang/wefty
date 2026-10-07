package contract

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRequestTraceErrorsOnly(t *testing.T) {
	for _, path := range []string{"/v1/agent/jobs/claim", "/v1/agent/nodes/node/heartbeat", "/v1/agent/jobs/job/attempts/attempt/lease", "/v1/agent/jobs/job/attempts/attempt/logs", "/v1/agent/nodes/node/computer-policy"} {
		for _, status := range []int{200, 204, 400, 503} {
			t.Run(fmt.Sprintf("%s/%d", path, status), func(t *testing.T) {
				var logs strings.Builder
				handler := ObserveHTTPRequests("l1", func(format string, args ...any) { fmt.Fprintf(&logs, format, args...) }, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(status) }))
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, httptest.NewRequest("POST", path, nil))
				if response.Header().Get(RequestIDHeader) == "" {
					t.Fatal("missing request ID")
				}
				if status < 400 && logs.Len() != 0 {
					t.Fatalf("success logged: %s", logs.String())
				}
				if status >= 400 && !strings.Contains(logs.String(), "request_id="+response.Header().Get(RequestIDHeader)) {
					t.Fatalf("error not logged: %s", logs.String())
				}
			})
		}
	}
}

func TestRequestTracePanicStatus(t *testing.T) {
	for _, written := range []bool{false, true} {
		t.Run(fmt.Sprint(written), func(t *testing.T) {
			var logs strings.Builder
			handler := ObserveHTTPRequests("l3", func(format string, args ...any) { fmt.Fprintf(&logs, format, args...) }, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if written {
					w.WriteHeader(200)
				}
				panic("private panic")
			}))
			func() {
				defer func() {
					if recover() != "private panic" {
						t.Error("panic not propagated")
					}
				}()
				handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/test", nil))
			}()
			if !strings.Contains(logs.String(), "status=500") || !strings.Contains(logs.String(), "panic=true") || strings.Contains(logs.String(), "private panic") {
				t.Fatalf("panic log: %s", logs.String())
			}
		})
	}
}

func TestRequestTraceNoRouteReason(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /exists", func(w http.ResponseWriter, _ *http.Request) {})
	for _, path := range []string{"/missing", "/exists"} {
		t.Run(path, func(t *testing.T) {
			response := httptest.NewRecorder()
			ObserveHTTPRequests("l1", nil, mux).ServeHTTP(response, httptest.NewRequest("POST", path, nil))
			var envelope ErrorResponse
			if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
				t.Fatal(err)
			}
			if envelope.Error.Details["reason"] != "no_route" {
				t.Fatalf("error: %+v", envelope.Error)
			}
		})
	}
}

func TestRequestTraceQuotesUpstreamID(t *testing.T) {
	var logs strings.Builder
	upstream := "upstream status=200\nforged"
	handler := ObserveHTTPRequests("l3", func(format string, args ...any) { fmt.Fprintf(&logs, format, args...) }, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(RequestIDHeader, upstream)
		w.WriteHeader(400)
	}))
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/test", nil))
	if !strings.Contains(logs.String(), fmt.Sprintf("upstream_request_id=%q", upstream)) {
		t.Fatalf("unquoted ID: %s", logs.String())
	}
}
