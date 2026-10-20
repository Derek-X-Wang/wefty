//go:build darwin || linux

package agent

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

type onFailureE2ERoundTrip func(*http.Request) (*http.Response, error)

func (f onFailureE2ERoundTrip) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestOnFailureE2EServiceReadRetriesRetryableUnavailable(t *testing.T) {
	calls := 0
	client := &http.Client{Transport: onFailureE2ERoundTrip(func(*http.Request) (*http.Response, error) {
		calls++
		if calls < 3 {
			return &http.Response{StatusCode: http.StatusServiceUnavailable, Body: io.NopCloser(strings.NewReader(
				`{"error":{"code":"unavailable","retryable":true,"details":{"reason":"read_snapshot_expired"}}}`))}, nil
		}
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"job_id":"job-1"}`))}, nil
	})}
	if job := getOnFailureE2EService(t, client, "job-1"); job.JobID != "job-1" || calls != 3 {
		t.Fatalf("read job %q after %d calls, want job-1 after 3", job.JobID, calls)
	}
}
