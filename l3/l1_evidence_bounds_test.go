package l3

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestDispatchL1EvidenceBounds(t *testing.T) {
	for _, field := range []string{"code", "body request_id", "header request_id"} {
		for _, character := range []string{"x", "界"} {
			t.Run(field+"/"+character, func(t *testing.T) {
				h := newHoldHTTPHarness(t)
				ctx := context.Background()
				// Exercise both the boundary and the first oversized string,
				// then a much larger response still within the body limit.
				for _, length := range []int{128, 129, 10000} {
					run := h.submit(inlineRunRequest("exit 0\n"), fmt.Sprintf("evidence-%d", length))
					value := strings.Repeat(character, length)
					want := value
					if length > 128 {
						want = strings.Repeat(character, 127) + "…"
					}
					code, requestID, headerID := "future_code", "short-request", "header-request"
					switch field {
					case "code":
						code = value
					case "body request_id":
						requestID = value
					case "header request_id":
						requestID, headerID = "", value
					}
					wireError := map[string]any{"code": code, "message": "private-message", "retryable": false}
					if requestID != "" {
						wireError["request_id"] = requestID
					}
					body, err := json.Marshal(map[string]any{"error": wireError})
					if err != nil {
						t.Fatal(err)
					}
					h.l1Client.client.Transport = recoveryRoundTripper(func(*http.Request) (*http.Response, error) {
						return &http.Response{StatusCode: 409, Header: http.Header{"X-Request-Id": {headerID}}, Body: io.NopCloser(bytes.NewReader(body))}, nil
					})
					var logs bytes.Buffer
					logger := log.New(&logs, "", 0)
					var reported error
					reconciler, err := NewReconciler(h.l3Store, h.l1Client, ReconcilerConfig{OnError: func(err error) {
						reported = err
						logger.Printf("reconcile: %v", err)
					}})
					if err != nil {
						t.Fatal(err)
					}
					reconciler.reconcileAndReport(ctx)
					execution, err := h.l3Store.GetRunExecution(ctx, run.RunID)
					if err != nil {
						t.Fatal(err)
					}
					if execution.DispatchError == nil || reported == nil {
						t.Fatalf("missing stored or logged dispatch error: %+v %v", execution.DispatchError, reported)
					}
					got := execution.DispatchError.RequestID
					if field == "code" {
						got, _ = execution.DispatchError.Details["l1_code"].(string)
					}
					if got != want || !utf8.ValidString(got) {
						t.Fatalf("length %d: stored %d runes, want %d with truncation marker", length, utf8.RuneCountInString(got), utf8.RuneCountInString(want))
					}
					if !strings.Contains(logs.String(), fmt.Sprintf("%q", want)) || strings.Contains(logs.String(), "private-message") {
						t.Fatalf("length %d: log lost bounded evidence or leaked message", length)
					}
					w := httptest.NewRecorder()
					w.Header().Set("X-L3-Request-Id", "local-request")
					writeError(w, reported)
					if field != "code" && w.Header().Get("X-Request-Id") != want {
						t.Fatalf("length %d: response request ID was not bounded", length)
					}
					if length > 128 && strings.Contains(logs.String(), value) {
						t.Fatalf("length %d: log retained oversized evidence", length)
					}
				}
			})
		}
	}
}
