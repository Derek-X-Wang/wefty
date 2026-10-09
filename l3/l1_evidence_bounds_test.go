package l3

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/Derek-X-Wang/wefty/contract"
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
						want = strings.Repeat(character, 125) + "..."
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

// Exercise dispatch persistence, the actual reconciler logger callback, and
// the caller-facing error writer for each kind of submission answer.
func TestDispatchClassifiedL1EvidenceBounds(t *testing.T) {
	for _, tt := range []struct {
		name, code string
		status     int
		retry      bool
		kind       l1AnswerKind
	}{
		{"unavailable", "unavailable", 503, true, l1Transient},
		{"internal", "internal", 500, true, l1Transient},
		{"work refused", "conflict", 409, false, l1WorkRefused},
		{"ledger not admitted", "unauthorized", 401, false, l1LedgerNotAdmitted},
		{"protocol violation", strings.Repeat("c", 10000), 409, false, l1ProtocolViolation},
	} {
		t.Run(tt.name, func(t *testing.T) {
			h := newHoldHTTPHarness(t)
			ctx := context.Background()
			run := h.submit(inlineRunRequest("exit 0\n"), "classified-evidence")
			requestID := strings.Repeat("r", 10000)
			message := strings.Repeat("m", 10000)
			reason := strings.Repeat("s", 10000)
			body, err := json.Marshal(contract.ErrorResponse{Error: contract.APIError{
				Code: contract.ErrorCode(tt.code), RequestID: requestID, Message: message,
				Retryable: tt.retry, Details: map[string]any{"reason": reason},
			}})
			if err != nil {
				t.Fatal(err)
			}
			h.l1Client.client.Transport = recoveryRoundTripper(func(r *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: tt.status, Header: http.Header{"X-Request-Id": {requestID}}, Body: io.NopCloser(bytes.NewReader(body))}, nil
			})
			// Check classification at the same seam used by the reconciler.
			_, seamErr := h.l1Client.SubmitJob(ctx, contract.JobSpec{})
			answer := classifyL1Answer(seamErr)
			if answer.Kind != tt.kind {
				t.Fatalf("kind=%d want %d", answer.Kind, tt.kind)
			}
			assertL1StringBound(t, "seam code", string(answer.Response.protocol.Code), boundedTestValue(tt.code))
			assertL1StringBound(t, "seam message", answer.Response.protocol.Message, boundedTestValue(message))
			assertL1StringBound(t, "seam reason", answer.Response.protocol.Details["reason"].(string), boundedTestValue(reason))
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
				t.Fatal("missing stored/logged error")
			}
			assertL1StringBound(t, "stored request_id", execution.DispatchError.RequestID, strings.Repeat("r", 125)+"...")
			if tt.kind == l1Transient || tt.kind == l1WorkRefused {
				assertL1StringBound(t, "stored message", execution.DispatchError.Message, strings.Repeat("m", 125)+"...")
				assertL1StringBound(t, "stored reason", execution.DispatchError.Details["reason"].(string), strings.Repeat("s", 125)+"...")
			}
			if tt.kind == l1LedgerNotAdmitted {
				if execution.DispatchHold == nil {
					t.Fatal("missing hold")
				}
				assertL1StringBound(t, "hold reason", execution.DispatchHold.Reason, strings.Repeat("s", 125)+"...")
				assertL1StringBound(t, "stored reason", execution.DispatchError.Details["reason"].(string), strings.Repeat("s", 125)+"...")
			}
			if tt.kind == l1ProtocolViolation {
				assertL1StringBound(t, "stored code", execution.DispatchError.Details["l1_code"].(string), strings.Repeat("c", 125)+"...")
			}
			for _, raw := range []string{requestID, message, reason, strings.Repeat("c", 10000)} {
				if strings.Contains(logs.String(), raw) {
					t.Error("logger retained oversized L1 string")
				}
			}
			if tt.kind == l1Transient || tt.kind == l1WorkRefused {
				if !strings.Contains(logs.String(), strings.Repeat("m", 125)+"...") {
					t.Error("logger lost bounded message")
				}
			}
			w := httptest.NewRecorder()
			w.Header().Set("X-L3-Request-Id", "local-request")
			writeError(w, reported)
			assertL1StringBound(t, "relayed header", w.Header().Get("X-Request-Id"), strings.Repeat("r", 125)+"...")
			for _, b := range []byte(w.Header().Get("X-Request-Id")) {
				if b < 32 || b > 126 {
					t.Error("truncated ASCII request ID contains non-visible-ASCII byte")
				}
			}
			var relayed contract.ErrorResponse
			if err := json.Unmarshal(w.Body.Bytes(), &relayed); err != nil {
				t.Fatal(err)
			}
			assertL1StringBound(t, "relayed request_id", relayed.Error.RequestID, strings.Repeat("r", 125)+"...")
			if tt.kind == l1WorkRefused || tt.name == "unavailable" {
				assertL1StringBound(t, "relayed message", relayed.Error.Message, strings.Repeat("m", 125)+"...")
			}
			if tt.kind == l1WorkRefused || tt.name == "unavailable" || tt.kind == l1LedgerNotAdmitted {
				assertL1StringBound(t, "relayed reason", relayed.Error.Details["reason"].(string), strings.Repeat("s", 125)+"...")
			}
		})
	}
}

func boundedTestValue(value string) string {
	runes := []rune(value)
	if len(runes) <= 128 {
		return value
	}
	return string(runes[:125]) + "..."
}

func assertL1StringBound(t *testing.T, field, got, want string) {
	t.Helper()
	if got != want || !utf8.ValidString(got) {
		t.Errorf("%s: got %d runes, want %d with expected prefix and ASCII marker", field, utf8.RuneCountInString(got), utf8.RuneCountInString(want))
	}
}

// Authoritative absence must keep its typed identity and classification while
// bounding the envelope before any consumer can inspect or relay it.
func TestL1AbsenceEvidenceBounds(t *testing.T) {
	for _, lookup := range []bool{false, true} {
		for _, character := range []string{"x", "界"} {
			t.Run(fmt.Sprintf("lookup=%t/%s", lookup, character), func(t *testing.T) {
				value := strings.Repeat(character, 10000)
				want := strings.Repeat(character, 125) + "..."
				body, err := json.Marshal(contract.ErrorResponse{Error: contract.APIError{
					Code: contract.ErrorNotFound, Message: value, RequestID: value,
					Details: map[string]any{"reason": value, "nested": []any{map[string]any{value: value}}, "flag": true, "count": 7},
				}})
				if err != nil {
					t.Fatal(err)
				}
				c := &L1Client{operationTimeout: time.Second, client: &http.Client{Transport: recoveryRoundTripper(func(*http.Request) (*http.Response, error) {
					return &http.Response{StatusCode: 404, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(body))}, nil
				})}}
				var failure error
				if lookup {
					_, failure = c.LookupJobByDispatchKey(context.Background(), "key")
				} else {
					_, failure = c.GetJob(context.Background(), "job")
				}
				if classifyL1Answer(failure).Kind != l1AuthoritativeAbsence ||
					(lookup && !isMissingDispatch(failure, "key")) || (!lookup && !isMissingL1Job(failure, "job")) {
					t.Fatal("absence lost authority or requested identity")
				}
				var protocol *Error
				if !errors.As(failure, &protocol) {
					t.Fatal("missing envelope")
				}
				assertL1StringBound(t, "absence message", protocol.Message, want)
				assertL1StringBound(t, "absence request_id", protocol.RequestID, want)
				assertL1StringBound(t, "absence reason", protocol.Details["reason"].(string), want)
				nested := protocol.Details["nested"].([]any)[0].(map[string]any)
				nestedValue, ok := nested[want].(string)
				if !ok {
					t.Error("nested detail key was not bounded")
				}
				assertL1StringBound(t, "nested detail value", nestedValue, want)
				if protocol.Details["flag"] != true || protocol.Details["count"] != float64(7) {
					t.Error("non-string details changed")
				}
				w := httptest.NewRecorder()
				w.Header().Set("X-L3-Request-Id", "local-request")
				writeError(w, failure)
				assertL1StringBound(t, "absence relayed header", w.Header().Get("X-Request-Id"), want)
				var relayed contract.ErrorResponse
				if err := json.Unmarshal(w.Body.Bytes(), &relayed); err != nil {
					t.Fatal(err)
				}
				assertL1StringBound(t, "absence relayed message", relayed.Error.Message, want)
				assertL1StringBound(t, "absence relayed reason", relayed.Error.Details["reason"].(string), want)
			})
		}
	}
}

// Use real L3 HTTP middleware and its logger, so oversized IDs cannot sneak
// into the upstream_request_id field even when a read reports absence.
func TestL3RelayedClassifiedL1EvidenceBounds(t *testing.T) {
	for _, tt := range []struct {
		name, code            string
		status, relayedStatus int
		retry                 bool
	}{
		{"transient", "unavailable", 503, 503, true},
		{"ledger not admitted", "unauthorized", 401, 503, false},
		{"work refused", "conflict", 409, 409, false},
		{"absence", "not_found", 404, 404, false},
		{"protocol violation", "future_code", 409, 503, false},
	} {
		for _, requestID := range []string{"short-request", strings.Repeat("r", 10000)} {
			t.Run(fmt.Sprintf("%s/id-runes=%d", tt.name, len(requestID)), func(t *testing.T) {
				h := newHoldHTTPHarness(t)
				ctx := context.Background()
				run := h.submit(inlineRunRequest("exit 0\n"), "relay-evidence")
				reconciler, err := NewReconciler(h.l3Store, h.l1Client, ReconcilerConfig{})
				if err != nil {
					t.Fatal(err)
				}
				if err := reconciler.ReconcileOnce(ctx); err != nil {
					t.Fatal(err)
				}
				message, reason := strings.Repeat("m", 10000), strings.Repeat("s", 10000)
				body, err := json.Marshal(contract.ErrorResponse{Error: contract.APIError{
					Code: contract.ErrorCode(tt.code), Message: message, RequestID: requestID,
					Retryable: tt.retry, Details: map[string]any{"reason": reason},
				}})
				if err != nil {
					t.Fatal(err)
				}
				h.l1Client.client.Transport = recoveryRoundTripper(func(*http.Request) (*http.Response, error) {
					return &http.Response{StatusCode: tt.status, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(body))}, nil
				})
				logs := &requestLog{}
				h.l3Server.logf = log.New(logs, "", 0).Printf
				status, headers, responseBody := h.do(h.caller, http.MethodGet, "/v1/runs/"+run.RunID+"/logs", nil, nil)
				if status != tt.relayedStatus {
					t.Fatalf("status=%d want=%d", status, tt.relayedStatus)
				}
				wantID := boundedTestValue(requestID)
				assertL1StringBound(t, "HTTP request_id", headers.Get("X-Request-Id"), wantID)
				if !logs.contains("upstream_request_id=" + fmt.Sprintf("%q", wantID)) {
					t.Error("HTTP log lost bounded upstream ID")
				}
				if len(requestID) > 128 && logs.contains(requestID) {
					t.Error("HTTP log retained oversized upstream ID")
				}
				if logs.contains(message) || logs.contains(reason) {
					t.Error("HTTP log retained oversized message/reason")
				}
				localID := headers.Get("X-L3-Request-Id")
				if localID == "" || localID == wantID {
					t.Error("local request correlation changed")
				}
				var relayed contract.ErrorResponse
				if err := json.Unmarshal(responseBody, &relayed); err != nil {
					t.Fatal(err)
				}
				assertL1StringBound(t, "HTTP envelope request_id", relayed.Error.RequestID, wantID)
				if relayed.Error.Details["l3_request_id"] != localID {
					t.Error("local envelope correlation changed")
				}
				if tt.name == "transient" || tt.name == "work refused" || tt.name == "absence" {
					assertL1StringBound(t, "HTTP message", relayed.Error.Message, boundedTestValue(message))
					assertL1StringBound(t, "HTTP reason", relayed.Error.Details["reason"].(string), boundedTestValue(reason))
				}
			})
		}
	}
}

func hugeL1Details() map[string]any {
	details := make(map[string]any, 60001)
	for i := 0; i < 60000; i++ {
		details[fmt.Sprintf("detail-%05d", i)] = "short-evidence"
	}
	details["reason"] = "capacity_exhausted"
	return details
}

func l1DetailsBody(t *testing.T, code contract.ErrorCode, details map[string]any) []byte {
	t.Helper()
	body, err := json.Marshal(contract.ErrorResponse{Error: contract.APIError{
		Code: code, Message: "L1 error", Retryable: code == contract.ErrorUnavailable,
		RequestID: "upstream-request", Details: details,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(body) > 2<<20 {
		t.Fatalf("fixture exceeds the L1 read limit: %d bytes", len(body))
	}
	return body
}

func l1DetailsClient(body []byte, status int) *L1Client {
	return &L1Client{operationTimeout: time.Second, client: &http.Client{Transport: recoveryRoundTripper(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(body))}, nil
	})}}
}

func assertL1DetailsSize(t *testing.T, details map[string]any) []byte {
	t.Helper()
	encoded, err := json.Marshal(details)
	if err != nil {
		t.Fatal(err)
	}
	// Literal contract limit so the test cannot grow with a changed constant.
	if len(encoded) > 4096 {
		t.Fatalf("kept details = %d encoded bytes, want <= 4096", len(encoded))
	}
	return encoded
}

// Reproduce the wide-answer failure through the production client, reconciler,
// dispatch_outbox persistence and HTTP writer, for retry and refusal answers.
func TestDispatchL1DetailsSizeBound(t *testing.T) {
	for _, tt := range []struct {
		code   contract.ErrorCode
		status int
		kind   l1AnswerKind
	}{
		{contract.ErrorUnavailable, 503, l1Transient},
		{contract.ErrorConflict, 409, l1WorkRefused},
	} {
		t.Run(string(tt.code), func(t *testing.T) {
			h := newHoldHTTPHarness(t)
			ctx := context.Background()
			run := h.submit(inlineRunRequest("exit 0\n"), "wide-evidence")
			body := l1DetailsBody(t, tt.code, hugeL1Details())
			h.l1Client.client.Transport = l1DetailsClient(body, tt.status).client.Transport
			_, seamErr := h.l1Client.SubmitJob(ctx, contract.JobSpec{})
			answer := classifyL1Answer(seamErr)
			if answer.Kind != tt.kind || answer.Reason != "capacity_exhausted" {
				t.Fatalf("classification changed: kind=%d reason=%q", answer.Kind, answer.Reason)
			}
			var reported error
			reconciler, err := NewReconciler(h.l3Store, h.l1Client, ReconcilerConfig{OnError: func(err error) { reported = err }})
			if err != nil {
				t.Fatal(err)
			}
			reconciler.reconcileAndReport(ctx)
			if reported == nil {
				t.Fatal("missing reported dispatch error")
			}
			var stored []byte
			if err := h.l3Store.db.QueryRowContext(ctx, `SELECT last_error FROM dispatch_outbox WHERE run_id=?`, run.RunID).Scan(&stored); err != nil {
				t.Fatal(err)
			}
			var protocol contract.APIError
			if err := json.Unmarshal(stored, &protocol); err != nil {
				t.Fatal(err)
			}
			kept := assertL1DetailsSize(t, protocol.Details)
			if protocol.Details["reason"] != "capacity_exhausted" || protocol.Retryable != (tt.kind == l1Transient) {
				t.Fatal("stored reason or retryability changed")
			}
			if !bytes.Equal(kept, assertL1DetailsSize(t, answer.Response.protocol.Details)) {
				t.Fatal("stored details differ from bounded client evidence")
			}
			// reason sorts after all 60,000 optional keys, but survives the cap.
			for i := 0; i < len(protocol.Details)-1; i++ {
				if protocol.Details[fmt.Sprintf("detail-%05d", i)] != "short-evidence" {
					t.Fatal("optional details did not retain a sorted prefix")
				}
			}
			w := httptest.NewRecorder()
			w.Header().Set("X-L3-Request-Id", "local-request")
			writeError(w, reported)
			var relayed contract.ErrorResponse
			if err := json.Unmarshal(w.Body.Bytes(), &relayed); err != nil {
				t.Fatal(err)
			}
			if w.Code != tt.status || relayed.Error.Details["reason"] != "capacity_exhausted" || relayed.Error.Details["l3_request_id"] != "local-request" {
				t.Fatal("relayed status, reason or local correlation changed")
			}
			delete(relayed.Error.Details, "l3_request_id")
			if !bytes.Equal(kept, assertL1DetailsSize(t, relayed.Error.Details)) {
				t.Fatal("relay lost bounded upstream details")
			}
			if len(stored) > 4096+512 || w.Body.Len() > 4096+512 {
				t.Fatalf("envelopes exceeded cap plus metadata: stored=%d relayed=%d", len(stored), w.Body.Len())
			}
			t.Logf("input=%d kept=%d stored=%d relayed=%d bytes", len(body), len(kept), len(stored), w.Body.Len())
		})
	}
}

func TestL1DetailsSizeBound(t *testing.T) {
	wide := hugeL1Details()
	items := make([]any, 60000)
	for i := range items {
		items[i] = "short-evidence"
	}
	escaped := make(map[string]any, 1000)
	for i := 0; i < 1000; i++ {
		escaped[fmt.Sprintf("<%04d\x00界", i)] = strings.Repeat("\x00<界", 40)
	}
	for _, tt := range []struct {
		name   string
		code   contract.ErrorCode
		status int
		reason any
		extra  any
		kind   l1AnswerKind
	}{
		{"ledger hold", contract.ErrorUnavailable, 503, "identity_unverifiable", wide, l1LedgerNotAdmitted},
		{"absence", contract.ErrorNotFound, 404, "missing", wide, l1AuthoritativeAbsence},
		{"no route", contract.ErrorUnavailable, 503, "no_route", wide, l1ProtocolViolation},
		{"malformed reason", contract.ErrorUnavailable, 503, false, wide, l1ProtocolViolation},
		{"reason object", contract.ErrorUnavailable, 503, wide, nil, l1ProtocolViolation},
		{"reason array", contract.ErrorUnavailable, 503, items, nil, l1ProtocolViolation},
		{"nested array", contract.ErrorUnavailable, 503, "capacity_exhausted", items, l1Transient},
		{"escaped Unicode", contract.ErrorUnavailable, 503, "capacity_exhausted", escaped, l1Transient},
		{"escaped keys tiny values", contract.ErrorUnavailable, 503, "capacity_exhausted", escapeHeavyL1DetailKeys(), l1Transient},
	} {
		t.Run(tt.name, func(t *testing.T) {
			details := map[string]any{"reason": tt.reason, "aaa": tt.extra}
			client := l1DetailsClient(l1DetailsBody(t, tt.code, details), tt.status)
			_, err := client.GetJob(context.Background(), "job")
			answer := classifyL1Answer(err)
			if answer.Kind != tt.kind {
				t.Fatalf("kind=%d want=%d", answer.Kind, tt.kind)
			}
			kept := answer.Response.protocol.Details
			reason, exists := kept["reason"]
			if !exists || fmt.Sprintf("%T", reason) != fmt.Sprintf("%T", tt.reason) {
				t.Fatalf("classification reason lost or changed type: %T", reason)
			}
			if want, ok := tt.reason.(string); ok && reason != want {
				t.Fatal("classification reason changed")
			}
			assertL1DetailsSize(t, kept)
		})
	}
}

func escapeHeavyL1DetailKeys() map[string]any {
	details := make(map[string]any, 1000)
	for i := 0; i < 1000; i++ {
		details[fmt.Sprintf("\x00<>&%06d", i)] = ""
	}
	return details
}

func TestBoundedL1DetailValueEncodedSize(t *testing.T) {
	for _, tt := range []struct {
		name  string
		value any
	}{
		{"escaped keys tiny values", escapeHeavyL1DetailKeys()},
		{"nested containers", map[string]any{
			"reason": "capacity_exhausted",
			"\x00<>&": []any{true, float64(7), nil, map[string]any{
				"\x00<>&": strings.Repeat("\x00<>&界", 40),
			}},
		}},
	} {
		for _, budget := range []int{256, 4096} {
			t.Run(fmt.Sprintf("%s/budget=%d", tt.name, budget), func(t *testing.T) {
				out, size, fits := boundedL1DetailValue(tt.value, budget)
				if !fits {
					t.Fatal("container did not fit")
				}
				encoded, err := json.Marshal(out)
				if err != nil {
					t.Fatal(err)
				}
				if size != len(encoded) {
					t.Errorf("reported size = %d, encoded size = %d", size, len(encoded))
				}
				if len(encoded) > budget {
					t.Errorf("encoded size = %d, budget = %d", len(encoded), budget)
				}
			})
		}
	}
}

func TestL1DetailsStopAtFirstMisfit(t *testing.T) {
	// Thirty bounded ASCII strings fill most of the 4 KiB budget. The next
	// string expands during JSON escaping and cannot fit, but the tiny item
	// after it could fit if the client incorrectly skipped the misfit.
	prefix := make([]any, 30)
	for i := range prefix {
		prefix[i] = strings.Repeat("x", 128)
	}
	misfit := strings.Repeat("\x00<>&", 32)
	for _, container := range []string{"object", "array"} {
		t.Run(container, func(t *testing.T) {
			details := map[string]any{"a": prefix, "b": misfit, "z": ""}
			if container == "array" {
				details = map[string]any{"items": []any{prefix, misfit, ""}}
			}
			client := l1DetailsClient(l1DetailsBody(t, contract.ErrorUnavailable, details), 503)
			_, err := client.GetJob(context.Background(), "job")
			var protocol *Error
			if !errors.As(err, &protocol) {
				t.Fatalf("missing L1 error envelope: %v", err)
			}
			kept := protocol.Details
			assertL1DetailsSize(t, kept)
			var keptPrefix []any
			if container == "object" {
				if _, exists := kept["b"]; exists {
					t.Error("misfit string was retained")
				}
				if _, exists := kept["z"]; exists {
					t.Error("tiny later key was retained after the first misfit")
				}
				keptPrefix = kept["a"].([]any)
			} else {
				items := kept["items"].([]any)
				if len(items) != 1 {
					t.Errorf("kept %d array items, want only the prefix before the misfit", len(items))
				}
				keptPrefix = items[0].([]any)
			}
			if len(keptPrefix) != len(prefix) {
				t.Errorf("kept %d prefix strings, want %d", len(keptPrefix), len(prefix))
			}
		})
	}
}

func TestL1DetailKeyCollisionDeterministic(t *testing.T) {
	prefix := strings.Repeat("界", 125)
	first, second := prefix+"aaaa", prefix+"zzzz"
	details := map[string]any{second: "second", first: "first"}
	body := l1DetailsBody(t, contract.ErrorUnavailable, map[string]any{
		"reason": "capacity_exhausted", "nested": []any{details},
		first: "first", second: "second",
	})
	client := l1DetailsClient(body, 503)
	for i := 0; i < 200; i++ {
		_, err := client.GetJob(context.Background(), "job")
		answer := classifyL1Answer(err)
		kept := answer.Response.protocol.Details
		nested := kept["nested"].([]any)[0].(map[string]any)
		if kept[prefix+"..."] != "first" || nested[prefix+"..."] != "first" {
			t.Fatalf("iteration %d: collision winner top=%v nested=%v, want first sorted key", i, kept[prefix+"..."], nested[prefix+"..."])
		}
	}
}
