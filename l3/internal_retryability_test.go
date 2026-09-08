package l3

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
)

func TestLocalInternalErrorPreservesCauseAndRetryAdvice(t *testing.T) {
	err := internalError(context.DeadlineExceeded, "read private lineage")
	var typed *Error
	if !errors.As(err, &typed) || typed.Code != contract.ErrorInternal || typed.Cause != context.DeadlineExceeded || !errors.Is(fmt.Errorf("outer: %w", err), context.DeadlineExceeded) {
		t.Fatalf("internal cause/code changed: %#v", err)
	}
	if typed.Message != "read private lineage" || !typed.Retryable {
		t.Fatalf("local internal error=%+v", typed)
	}
	typed.Details = map[string]any{"private": "lineage"}
	typed.RequestID = "request-id"
	advisory := apiErrorFrom(fmt.Errorf("outer: %w", err))
	if !advisory.Retryable || advisory.Details["private"] != "lineage" || advisory.RequestID != "request-id" {
		t.Fatalf("typed advisory=%+v", advisory)
	}
}

func TestInternalRetryabilityWireBoundary(t *testing.T) {
	tests := []struct {
		name    string
		err     error
		status  int
		code    contract.ErrorCode
		retry   bool
		message string
	}{
		{"local internal", internalError(context.DeadlineExceeded, "private cause"), http.StatusServiceUnavailable, contract.ErrorInternal, true, "internal server error"},
		{"raw internal", errors.New("private raw cause"), http.StatusServiceUnavailable, contract.ErrorInternal, true, "internal server error"},
		{"explicit internal false", &Error{Code: contract.ErrorInternal, Message: "private remote", Retryable: false, Details: map[string]any{"secret": "private"}}, http.StatusInternalServerError, contract.ErrorInternal, false, "internal server error"},
		{"explicit internal true", &Error{Code: contract.ErrorInternal, Message: "private remote", Retryable: true}, http.StatusServiceUnavailable, contract.ErrorInternal, true, "internal server error"},
		{"authority false", protocolError(contract.ErrorForbidden, "denied"), http.StatusForbidden, contract.ErrorForbidden, false, "denied"},
		{"reserved false", protocolError(contract.ErrorNotImplemented, "reserved"), http.StatusNotImplemented, contract.ErrorNotImplemented, false, "reserved"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			writeError(recorder, fmt.Errorf("wrapped: %w", test.err))
			var response contract.ErrorResponse
			if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
				t.Fatal(err)
			}
			if recorder.Code != test.status || response.Error.Code != test.code || response.Error.Retryable != test.retry || response.Error.Message != test.message || response.Error.Details != nil {
				t.Fatalf("status=%d response=%+v", recorder.Code, response)
			}
		})
	}
}
