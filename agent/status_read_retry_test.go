package agent

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestStatusReadHTTPRetry(t *testing.T) {
	const busy = `{"error":{"code":"unavailable","retryable":true,"details":{"reason":"read_snapshot_expired"}}}`
	for _, test := range []struct {
		name, method, path, refusal string
		status, failures, wantCalls int
	}{
		{"job recovers", "GET", "/v1/jobs/job?class=service", busy, 503, 1, 2},
		{"Computer recovers", "GET", "/v1/computers/computer", busy, 503, 1, 2},
		{"exhaustion preserves last refusal", "GET", "/v1/jobs/job", busy, 503, 10, 4},
		{"nonretryable", "GET", "/v1/jobs/job", `{"error":{"code":"unavailable","retryable":false}}`, 503, 1, 1},
		{"different code", "GET", "/v1/jobs/job", `{"error":{"code":"internal","retryable":true}}`, 503, 1, 1},
		{"different status", "GET", "/v1/jobs/job", busy, 500, 1, 1},
		{"malformed", "GET", "/v1/jobs/job", `invalid`, 503, 1, 1},
		{"write", "POST", "/v1/jobs/job/restart", busy, 503, 1, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				call := calls.Add(1)
				if int(call) <= test.failures {
					w.WriteHeader(test.status)
					_, _ = w.Write([]byte(test.refusal))
					return
				}
				_, _ = w.Write([]byte(`{"job_id":"job"}`))
			}))
			defer server.Close()
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			request, err := http.NewRequestWithContext(ctx, test.method, server.URL+test.path, nil)
			if err != nil {
				t.Fatal(err)
			}
			status, body, err := testStatusHTTP(ctx, server.Client(), request)
			wantStatus := test.status
			if test.wantCalls > test.failures {
				wantStatus = http.StatusOK
			}
			if err != nil || int(calls.Load()) != test.wantCalls || status != wantStatus {
				t.Fatalf("calls=%d status=%d body=%s err=%v", calls.Load(), status, body, err)
			}
			if status != http.StatusOK && string(body) != test.refusal {
				t.Fatalf("refusal changed: %s", body)
			}
		})
	}
}

func TestStatusReadHTTPDeadlineAndTransportFailure(t *testing.T) {
	t.Run("deadline", func(t *testing.T) {
		var calls atomic.Int32
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":{"code":"unavailable","retryable":true}}`))
		}))
		defer server.Close()
		request, _ := http.NewRequest(http.MethodGet, server.URL+"/v1/jobs/job", nil)
		// Cancel after receiving the refusal, before the backoff can finish.
		client := &http.Client{Transport: statusReadTransport(func(r *http.Request) (*http.Response, error) {
			response, err := server.Client().Do(r)
			cancel()
			return response, err
		})}
		_, _, err := testStatusHTTP(ctx, client, request)
		if !errors.Is(err, context.Canceled) || calls.Load() != 1 {
			t.Fatalf("calls=%d err=%v", calls.Load(), err)
		}
	})
	t.Run("transport", func(t *testing.T) {
		var calls atomic.Int32
		want := errors.New("connection lost")
		client := &http.Client{Transport: statusReadTransport(func(*http.Request) (*http.Response, error) {
			calls.Add(1)
			return nil, want
		})}
		request, _ := http.NewRequest(http.MethodGet, "http://l1/v1/jobs/job", nil)
		_, _, err := testStatusHTTP(t.Context(), client, request)
		if !errors.Is(err, want) || calls.Load() != 1 {
			t.Fatalf("calls=%d err=%v", calls.Load(), err)
		}
	})
}

type statusReadTransport func(*http.Request) (*http.Response, error)

func (transport statusReadTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return transport(request)
}
