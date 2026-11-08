//go:build darwin || linux

package agent

import (
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestCanceledE2EResultPollRetriesOnlyRetryableAnswers(t *testing.T) {
	answer := func(status int, body string) *http.Response {
		return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body))}
	}
	unavailable := func(retryable string) *http.Response {
		return answer(http.StatusServiceUnavailable,
			`{"error":{"code":"unavailable","retryable":`+retryable+`,"details":{"reason":"read_snapshot_expired"}}}`)
	}
	for _, test := range []struct {
		name    string
		answers []*http.Response
		wantErr bool
	}{
		// Document is []byte, so the wire carries it as base64.
		{"not uploaded, then retryable unavailable, then the result", []*http.Response{
			answer(http.StatusNotFound, `{"error":{"code":"not_found"}}`),
			unavailable("true"),
			answer(http.StatusOK, `{"attempt_id":"attempt-1","document":"eyJjYW5jZWxsYXRpb25fZXZpZGVuY2UiOnRydWV9"}`),
		}, false},
		{"non-retryable unavailable is fatal", []*http.Response{unavailable("false")}, true},
		{"server error is fatal", []*http.Response{answer(http.StatusInternalServerError, `{"error":{"code":"internal","retryable":true}}`)}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			client := &http.Client{Transport: onFailureE2ERoundTrip(func(*http.Request) (*http.Response, error) {
				calls++
				if calls > len(test.answers) {
					t.Fatalf("poll made call %d after the last stubbed answer", calls)
				}
				return test.answers[calls-1], nil
			})}
			uploaded, err := pollCanceledE2EResult(t, client, "job-1", 5*time.Second)
			if test.wantErr {
				if err == nil || calls != 1 {
					t.Fatalf("err=%v after %d calls, want an error after 1", err, calls)
				}
				return
			}
			if err != nil || uploaded.AttemptID != "attempt-1" || string(uploaded.Document) != `{"cancellation_evidence":true}` || calls != len(test.answers) {
				t.Fatalf("uploaded=%+v err=%v after %d calls", uploaded, err, calls)
			}
		})
	}
}
