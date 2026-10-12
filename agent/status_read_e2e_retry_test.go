//go:build darwin || linux

package agent

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

// Exercise the actual E2E status observation, without starting real processes.
func TestE2EJobStatusRetriesReadSnapshotExpired(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		call := calls.Add(1)
		if call == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":{"code":"unavailable","retryable":true,"details":{"reason":"read_snapshot_expired"}}}`))
			return
		}
		_, _ = w.Write([]byte(`{"job_id":"job","state":"succeeded"}`))
	}))
	defer server.Close()
	client := &http.Client{Transport: statusReadTransport(func(r *http.Request) (*http.Response, error) {
		copy := r.Clone(r.Context())
		copy.URL.Scheme = "http"
		copy.URL.Host = server.Listener.Addr().String()
		return server.Client().Do(copy)
	})}
	job := getE2EJobContext(t, t.Context(), client, "job")
	if calls.Load() != 2 || job.JobID != "job" || job.State != "succeeded" {
		t.Fatalf("calls=%d job=%+v", calls.Load(), job)
	}
}
