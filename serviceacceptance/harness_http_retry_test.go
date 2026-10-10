package serviceacceptance

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
)

type harnessReadTransport func(*http.Request) (*http.Response, error)

func (call harnessReadTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return call(request)
}

func TestHarnessHTTPRetry(t *testing.T) {
	const unavailable = `{"error":{"code":"unavailable","retryable":true,"details":{"reason":"read_snapshot_expired"}}}`
	for _, test := range []struct {
		name, method, path, key, refusal string
		failures, wantCalls, wantStatus  int
	}{
		{"read recovers", "GET", "/v1/jobs/job", "", unavailable, 2, 3, 200},
		{"keyed root submission recovers", "POST", "/v1/runs", "same-root", unavailable, 2, 3, 200},
		{"retry exhaustion preserves refusal", "GET", "/v1/computers/id", "", unavailable, 10, 4, 503},
		{"definitive unavailable", "GET", "/v1/jobs/job", "", `{"error":{"code":"unavailable","retryable":false}}`, 1, 1, 503},
		{"other retryable code", "GET", "/v1/jobs/job", "", `{"error":{"code":"internal","retryable":true}}`, 1, 1, 503},
		{"malformed refusal", "GET", "/v1/jobs/job", "", `invalid`, 1, 1, 503},
		{"unkeyed root is not replayed", "POST", "/v1/runs", "", unavailable, 1, 1, 503},
		{"other mutation is not replayed", "POST", "/v1/jobs", "mutation-key", unavailable, 1, 1, 503},
	} {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			client := &http.Client{Transport: harnessReadTransport(func(request *http.Request) (*http.Response, error) {
				calls++
				if request.Header.Get("Idempotency-Key") != test.key {
					t.Fatal("idempotency key changed during retry")
				}
				body, err := io.ReadAll(request.Body)
				request.Body.Close()
				if err != nil || string(body) != `{"root":"same"}` {
					t.Fatalf("replayed body = %s, err = %v", body, err)
				}
				status, payload := http.StatusOK, `{"ok":true}`
				if calls <= test.failures {
					status, payload = http.StatusServiceUnavailable, test.refusal
				}
				return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(payload)), Header: make(http.Header)}, nil
			})}
			request, err := http.NewRequest(test.method, "http://control-plane.invalid"+test.path, strings.NewReader(`{"root":"same"}`))
			if err != nil {
				t.Fatal(err)
			}
			request.Header.Set("Idempotency-Key", test.key)
			status, body, err := harnessHTTP(t.Context(), t.Logf, client, request)
			if err != nil || calls != test.wantCalls || status != test.wantStatus {
				t.Fatalf("calls=%d status=%d body=%s err=%v", calls, status, body, err)
			}
			if status == 503 && string(body) != test.refusal {
				t.Fatalf("last refusal changed: %s", body)
			}
		})
	}
}

func TestHarnessRetryStopsOnTransportFailureAndCancellation(t *testing.T) {
	t.Run("transport failure", func(t *testing.T) {
		want := errors.New("connection lost")
		calls := 0
		_, _, err := retryHarnessRequest(t.Context(), t.Logf, http.MethodGet, "/v1/jobs", "", func() (int, []byte, error) {
			calls++
			return 0, nil, want
		})
		if !errors.Is(err, want) || calls != 1 {
			t.Fatalf("calls=%d err=%v", calls, err)
		}
	})
	t.Run("canceled backoff", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		calls := 0
		_, _, err := retryHarnessRequest(ctx, t.Logf, http.MethodGet, "/v1/jobs", "", func() (int, []byte, error) {
			calls++
			cancel()
			return 503, []byte(`{"error":{"code":"` + string(contract.ErrorUnavailable) + `","retryable":true}}`), nil
		})
		if !errors.Is(err, context.Canceled) || calls != 1 {
			t.Fatalf("calls=%d err=%v", calls, err)
		}
	})
}
