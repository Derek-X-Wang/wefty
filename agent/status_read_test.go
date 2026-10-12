package agent

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

// Status observations may retry L1's typed busy answer, never a mutation or
// another refusal. The caller's poll context bounds requests and backoff; the
// small retry cap also bounds observations outside a polling loop.
func testStatusHTTP(ctx context.Context, client *http.Client, request *http.Request) (int, []byte, error) {
	parts := strings.Split(strings.Trim(request.URL.Path, "/"), "/")
	statusRead := request.Method == http.MethodGet && len(parts) == 3 && parts[0] == "v1" && (parts[1] == "jobs" || parts[1] == "computers")
	for attempt := 0; ; attempt++ {
		response, err := client.Do(request.Clone(ctx))
		if err != nil {
			return 0, nil, err
		}
		body, err := io.ReadAll(response.Body)
		response.Body.Close()
		var refusal contract.ErrorResponse
		if err != nil || !statusRead || attempt == 3 || response.StatusCode != http.StatusServiceUnavailable ||
			json.Unmarshal(body, &refusal) != nil || refusal.Error.Code != contract.ErrorUnavailable || !refusal.Error.Retryable {
			return response.StatusCode, body, err
		}
		timer := time.NewTimer(20 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return response.StatusCode, body, ctx.Err()
		case <-timer.C:
		}
	}
}

// Store errors keep their retryability policy private to L1. Observe service
// status over HTTP so these polls honor the authoritative typed answer too.
func getTestServiceJob(t *testing.T, ctx context.Context, client *http.Client, jobID string) l1.Job {
	t.Helper()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://control-plane.invalid/v1/jobs/"+jobID+"?class=service", nil)
	if err != nil {
		t.Fatal(err)
	}
	status, body, err := testStatusHTTP(ctx, client, request)
	if err != nil {
		t.Fatal(err)
	}
	if status != http.StatusOK {
		t.Fatalf("get service job status = %d body=%s", status, body)
	}
	var job l1.Job
	if err := json.Unmarshal(body, &job); err != nil {
		t.Fatal(err)
	}
	return job
}
