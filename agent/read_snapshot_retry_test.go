package agent

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	"github.com/Derek-X-Wang/wefty/l3"
)

// Advance each backoff synchronously; no wall-clock sleeps or polling are needed.
type snapshotRetryClock struct {
	*manualClock
	delays  []time.Duration
	onTimer func()
	jump    time.Duration
}

func (clock *snapshotRetryClock) NewTimer(delay time.Duration) Timer {
	clock.delays = append(clock.delays, delay)
	timer := clock.manualClock.NewTimer(delay)
	if clock.onTimer != nil {
		clock.onTimer()
	} else {
		clock.Advance(max(delay, clock.jump))
	}
	return timer
}

func TestSnapshotRetryClientBoundaries(t *testing.T) {
	for _, boundary := range []string{"copy authority", "Computer token", "service binding"} {
		t.Run(boundary, func(t *testing.T) {
			for _, scenario := range []struct {
				name                                                 string
				reason                                               string
				code                                                 contract.ErrorCode
				status                                               int
				failures, calls                                      int
				delays                                               []time.Duration
				slowRequest, backoffBudget, cancelBackoff, transport bool
				preCanceled, malformed                               bool
			}{
				{name: "one expiry then success", reason: "read_snapshot_expired", failures: 1, calls: 2, delays: []time.Duration{100 * time.Millisecond}},
				{name: "one admission expiry then success", reason: "read_snapshot_admission_expired", failures: 1, calls: 2, delays: []time.Duration{100 * time.Millisecond}},
				{name: "try limit", reason: "read_snapshot_expired", failures: 10, calls: 3, delays: []time.Duration{100 * time.Millisecond, 200 * time.Millisecond}},
				{name: "slow first request succeeds", calls: 1, slowRequest: true},
				{name: "failure after retry budget", reason: "read_snapshot_expired", failures: 10, calls: 1, slowRequest: true},
				{name: "backoff consumes budget", reason: "read_snapshot_expired", failures: 10, calls: 1, backoffBudget: true, delays: []time.Duration{100 * time.Millisecond}},
				{name: "cancel backoff", reason: "read_snapshot_expired", failures: 1, calls: 1, cancelBackoff: true, delays: []time.Duration{100 * time.Millisecond}},
				{name: "pre-canceled context", preCanceled: true},
				{name: "non-JSON 503", failures: 1, calls: 1, malformed: true},
				{name: "nonretryable", reason: "read_snapshot_expired", failures: 1, calls: 1},
				{name: "other reason", reason: "other", failures: 1, calls: 1},
				{name: "missing reason", failures: 1, calls: 1},
				{name: "other code", reason: "read_snapshot_expired", code: contract.ErrorInternal, failures: 1, calls: 1},
				{name: "other status", reason: "read_snapshot_expired", status: 500, failures: 1, calls: 1},
				{name: "transport", failures: 1, calls: 1, transport: true},
			} {
				t.Run(scenario.name, func(t *testing.T) {
					ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
					defer cancel()
					if scenario.preCanceled {
						cancel()
					}
					clock := &snapshotRetryClock{manualClock: newManualClock(time.Now())}
					if scenario.cancelBackoff {
						clock.onTimer = cancel
					}
					if scenario.backoffBudget {
						clock.jump = 5 * time.Second
					}
					code := scenario.code
					if code == "" {
						code = contract.ErrorUnavailable
					}
					status := scenario.status
					if status == 0 {
						status = http.StatusServiceUnavailable
					}
					refusal := contract.APIError{Code: code, Message: "injected refusal", Retryable: scenario.name != "nonretryable", Details: map[string]any{"reason": scenario.reason}}
					body, err := json.Marshal(contract.ErrorResponse{Error: refusal})
					if err != nil {
						t.Fatal(err)
					}
					if scenario.malformed {
						body = []byte("<html>busy</html>")
					}
					calls := 0
					var firstPayload string
					lost := errors.New("injected connection loss")
					client := &http.Client{Timeout: DefaultOperationTimeout, Transport: statusReadTransport(func(r *http.Request) (*http.Response, error) {
						calls++
						deadline, ok := r.Context().Deadline()
						if !ok || time.Until(deadline) <= 5*time.Second || time.Until(deadline) > DefaultOperationTimeout {
							t.Fatal("retry window changed the client's per-request timeout")
						}
						payload, err := io.ReadAll(r.Body)
						if err != nil {
							t.Fatal(err)
						}
						if calls == 1 {
							firstPayload = string(payload)
						} else if string(payload) != firstPayload {
							t.Fatal("retry changed request payload")
						}
						if scenario.slowRequest {
							clock.Advance(6 * time.Second)
						}
						if scenario.transport {
							return nil, lost
						}
						response := `{"bound":true,"generation":7,"token":"fresh-token"}`
						responseStatus := http.StatusOK
						if calls <= scenario.failures {
							response, responseStatus = string(body), status
						}
						return &http.Response{StatusCode: responseStatus, Body: io.NopCloser(strings.NewReader(response)), Header: make(http.Header)}, nil
					})}
					control := &Client{baseURL: "http://l1.invalid", httpClient: client, operationTimeout: DefaultOperationTimeout}
					switch boundary {
					case "copy authority":
						var value l1.SeatCopyAuthority
						value, err = control.authorizeSeatCopyWithClock(ctx, "seat", l1.SeatCopyAuthorityInput{Credential: "credential"}, clock)
						if err == nil && value.Generation != 7 {
							t.Fatalf("authority = %+v", value)
						}
					case "Computer token":
						var value l3.ComputerTokenGrant
						value, err = (&computerTokenClient{httpClient: client}).mintComputerTokenWithClock(ctx, l3.ComputerTokenMintRequest{ComputerID: "computer", ComputerAttemptID: "attempt"}, clock)
						if err == nil && value.Token != "fresh-token" {
							t.Fatalf("grant = %+v", value)
						}
					case "service binding":
						var bound bool
						bound, err = control.proveServiceBindingWithClock(ctx, "node", "boot", "job", clock)
						if err == nil && !bound {
							t.Fatal("lost positive proof")
						}
					}
					if calls != scenario.calls || !reflect.DeepEqual(clock.delays, scenario.delays) {
						t.Fatalf("calls=%d delays=%v; want %d %v", calls, clock.delays, scenario.calls, scenario.delays)
					}
					switch {
					case scenario.preCanceled || scenario.cancelBackoff:
						if !errors.Is(err, context.Canceled) {
							t.Fatalf("error = %v", err)
						}
					case scenario.transport:
						if !errors.Is(err, lost) {
							t.Fatalf("error = %v", err)
						}
					case scenario.malformed:
						want := "l1 agent protocol: HTTP 503 with invalid error response: invalid character '<' looking for beginning of value"
						if boundary == "Computer token" {
							want = "agent: Computer token mint returned HTTP 503"
						}
						if err == nil || err.Error() != want {
							t.Fatalf("error=%v want=%s", err, want)
						}
					case scenario.calls > scenario.failures:
						if err != nil {
							t.Fatal(err)
						}
					case boundary == "Computer token":
						want := "agent: Computer token mint rejected: " + string(code) + ": injected refusal"
						if err == nil || err.Error() != want {
							t.Fatalf("error=%v want=%s", err, want)
						}
					default:
						var protocol *ProtocolError
						if !errors.As(err, &protocol) || protocol.StatusCode != status || !reflect.DeepEqual(protocol.APIError, refusal) {
							t.Fatalf("changed refusal: %v", err)
						}
					}
				})
			}
		})
	}
}
