package serviceacceptance

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

// Keep the last refusal intact. Only a typed, retryable unavailable answer
// permits another request; reachability failures and definitive refusals do not.
// The live Computer probe also submits roots through L3. Those POSTs carry a
// stable idempotency key, so replay cannot create another Run (#163).
// Each retried refusal is logged so lane evidence still shows slow reads.
// Callers pass t.Context(), which is already cancelled inside t.Cleanup:
// these helpers are for test bodies, not cleanup.
func retryHarnessRequest(ctx context.Context, logf func(string, ...any), method, path, key string, call func() (int, []byte, error)) (int, []byte, error) {
	mayRetry := method == http.MethodGet || method == http.MethodPost && path == "/v1/runs" && key != ""
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return 0, nil, err
		}
		status, body, err := call()
		var refusal contract.ErrorResponse
		if err != nil || !mayRetry || attempt == 3 || status != http.StatusServiceUnavailable ||
			json.Unmarshal(body, &refusal) != nil || refusal.Error.Code != contract.ErrorUnavailable || !refusal.Error.Retryable {
			return status, body, err
		}
		backoff := 100 * time.Millisecond << attempt
		logf("harness %s %s: retryable unavailable (%v); retry %d in %s", method, path, refusal.Error.Details["reason"], attempt+1, backoff)
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return status, body, ctx.Err()
		case <-timer.C:
		}
	}
}

func harnessHTTP(ctx context.Context, logf func(string, ...any), client *http.Client, request *http.Request) (int, []byte, error) {
	return retryHarnessRequest(ctx, logf, request.Method, request.URL.Path, request.Header.Get("Idempotency-Key"), func() (int, []byte, error) {
		copy := request.Clone(ctx)
		if request.GetBody != nil {
			body, err := request.GetBody()
			if err != nil {
				return 0, nil, err
			}
			copy.Body = body
		}
		response, err := client.Do(copy)
		if err != nil {
			return 0, nil, err
		}
		defer response.Body.Close()
		body, err := io.ReadAll(response.Body)
		return response.StatusCode, body, err
	})
}
