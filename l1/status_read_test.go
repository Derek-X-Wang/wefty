package l1

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

// Test status reads retry only typed, retryable unavailable refusals. Keep the
// caller's deadline and last result intact; bound standalone reads too.
func retryTestStatusRead[T any](ctx context.Context, read func() (T, error)) (T, error) {
	for attempt := 0; ; attempt++ {
		value, err := read()
		if observe, ok := ctx.Value(testReadAttemptObserverKey{}).(func(error)); ok {
			observe(err)
		}
		if err == nil || attempt == 3 || !testReadRetryable(err) {
			return value, err
		}
		timer := time.NewTimer(20 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return value, ctx.Err()
		case <-timer.C:
		}
	}
}

// The observer is used only by hard-expiry proofs. It does not alter the read.
type testReadAttemptObserverKey struct{}

func testReadRetryable(err error) bool {
	var typed *Error
	if !errors.As(err, &typed) {
		return false
	}
	api := apiErrorFromDecision(err)
	return api.Code == contract.ErrorUnavailable && api.Retryable &&
		(api.Details["reason"] == "read_snapshot_expired" || api.Details["reason"] == "read_snapshot_admission_expired")
}

func (h *integrationHarness) readStatus(ctx context.Context, client *http.Client, path string) (int, http.Header, []byte) {
	h.t.Helper()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://control-plane.invalid"+path, nil)
	if err != nil {
		h.t.Fatal(err)
	}
	parts := strings.Split(strings.Trim(request.URL.Path, "/"), "/")
	if len(parts) < 2 || len(parts) > 3 || parts[0] != "v1" || (parts[1] != "computers" && parts[1] != "jobs") {
		h.t.Fatalf("not a status read: %s", path)
	}
	response, err := doTestReadHTTP(client, request)
	if err != nil {
		h.t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		h.t.Fatal(err)
	}
	return response.StatusCode, response.Header.Clone(), body
}

// Refusals retain their complete HTTP answer when retries are exhausted.
func testReadRefusal(status int, body []byte) error {
	var refusal contract.ErrorResponse
	if status == http.StatusServiceUnavailable && json.Unmarshal(body, &refusal) == nil && refusal.Error.Code == contract.ErrorUnavailable {
		return &Error{Code: refusal.Error.Code, Message: refusal.Error.Message, Details: refusal.Error.Details, notRetryable: !refusal.Error.Retryable}
	}
	return nil
}

// GETs and the three POST proof routes are reads. All mutation routes
// still use one round trip, even when their post-change projection expires.
func doTestReadHTTP(client *http.Client, request *http.Request) (*http.Response, error) {
	if !testRequestIsRead(request) {
		return client.Do(request)
	}
	if client.Timeout > 0 {
		ctx, cancel := context.WithTimeout(request.Context(), client.Timeout)
		defer cancel()
		request = request.Clone(ctx)
	}
	if err := prepareTestReadBody(request); err != nil {
		return nil, err
	}
	response, err := retryTestStatusRead(request.Context(), func() (*http.Response, error) {
		clone, err := cloneTestReadRequest(request)
		if err != nil {
			return nil, err
		}
		response, err := client.Do(clone)
		if err != nil {
			return response, err
		}
		body, err := io.ReadAll(response.Body)
		response.Body.Close()
		response.Body = io.NopCloser(strings.NewReader(string(body)))
		if err != nil {
			return response, err
		}
		return response, testReadRefusal(response.StatusCode, body)
	})
	var typed *Error
	if response != nil && errors.As(err, &typed) {
		return response, nil
	}
	return response, err
}

func recordTestRead(request *http.Request, handler http.HandlerFunc) *httptest.ResponseRecorder {
	if !testRequestIsRead(request) {
		panic("l1: test read helper received a mutation")
	}
	if err := prepareTestReadBody(request); err != nil {
		panic(err)
	}
	response, _ := retryTestStatusRead(request.Context(), func() (*httptest.ResponseRecorder, error) {
		response := httptest.NewRecorder()
		clone, err := cloneTestReadRequest(request)
		if err != nil {
			panic(err)
		}
		handler(response, clone)
		return response, testReadRefusal(response.Code, response.Body.Bytes())
	})
	return response
}

// An observation test must retry with a fresh fixture. Once its writer has
// committed, replaying only the read would compare two different snapshots.
func retryTestSnapshotScenario(t *testing.T, scenario func() error) {
	t.Helper()
	_, err := retryTestStatusRead(t.Context(), func() (struct{}, error) { return struct{}{}, scenario() })
	if err != nil {
		t.Fatal(err)
	}
}

func testRequestIsRead(request *http.Request) bool {
	if request.Method == http.MethodGet {
		return true
	}
	if request.Method != http.MethodPost {
		return false
	}
	parts := strings.Split(strings.Trim(request.URL.Path, "/"), "/")
	return request.URL.Path == "/v1/host-boot-session-proof" ||
		len(parts) == 4 && parts[0] == "v1" && parts[1] == "computers" && parts[2] != "" && parts[3] == "token-scope-proof" ||
		len(parts) == 5 && parts[0] == "v1" && parts[1] == "agent" && parts[2] == "jobs" && parts[3] != "" && parts[4] == "service-binding-proof"
}

func prepareTestReadBody(request *http.Request) error {
	if request.Body == nil || request.GetBody != nil {
		return nil
	}
	body, err := io.ReadAll(request.Body)
	request.Body.Close()
	if err != nil {
		return err
	}
	request.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(strings.NewReader(string(body))), nil }
	request.Body, err = request.GetBody()
	return err
}

func cloneTestReadRequest(request *http.Request) (*http.Request, error) {
	clone := request.Clone(request.Context())
	if request.GetBody != nil {
		var err error
		clone.Body, err = request.GetBody()
		if err != nil {
			return nil, err
		}
	}
	return clone, nil
}
