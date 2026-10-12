package l1

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

// Test status reads retry only typed, retryable unavailable refusals. Keep the
// caller's deadline and last result intact; bound standalone reads too.
func retryTestStatusRead[T any](ctx context.Context, read func() (T, error)) (T, error) {
	for attempt := 0; ; attempt++ {
		value, err := read()
		var typed *Error
		if err == nil || attempt == 3 || !errors.As(err, &typed) {
			return value, err
		}
		api := apiErrorFromDecision(err)
		if api.Code != contract.ErrorUnavailable || !api.Retryable {
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

func (h *integrationHarness) readStatus(ctx context.Context, client *http.Client, path string) (int, http.Header, []byte) {
	h.t.Helper()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://control-plane.invalid"+path, nil)
	if err != nil {
		h.t.Fatal(err)
	}
	type answer struct {
		status int
		header http.Header
		body   []byte
	}
	parts := strings.Split(strings.Trim(request.URL.Path, "/"), "/")
	if len(parts) < 2 || len(parts) > 3 || parts[0] != "v1" || (parts[1] != "computers" && parts[1] != "jobs") {
		h.t.Fatalf("not a status read: %s", path)
	}
	result, err := retryTestStatusRead(ctx, func() (answer, error) {
		response, err := client.Do(request.Clone(ctx))
		if err != nil {
			return answer{}, err
		}
		defer response.Body.Close()
		body, err := io.ReadAll(response.Body)
		result := answer{response.StatusCode, response.Header.Clone(), body}
		var refusal contract.ErrorResponse
		if err == nil && response.StatusCode == http.StatusServiceUnavailable && json.Unmarshal(body, &refusal) == nil && refusal.Error.Code == contract.ErrorUnavailable {
			return result, &Error{Code: refusal.Error.Code, Message: refusal.Error.Message, Details: refusal.Error.Details, notRetryable: !refusal.Error.Retryable}
		}
		return result, err
	})
	var typed *Error
	if err != nil && !errors.As(err, &typed) {
		h.t.Fatal(err)
	}
	return result.status, result.header, result.body
}
