package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/l1"
)

func startupClientFailure(t *testing.T, cause error, status int, body string) error {
	t.Helper()
	c := apiClient{name: "L1", flag: "l1", address: "fixture", client: &http.Client{Transport: forwardingRoundTripper(func(*http.Request) (*http.Response, error) {
		if cause != nil {
			return nil, cause
		}
		return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
	})}}
	var target any
	err := c.do(withoutClientRetry(t.Context()), http.MethodGet, "/v1/nodes/proof", nil, nil, &target, http.StatusOK)
	if err == nil {
		t.Fatal("fixture did not produce a failed exchange")
	}
	return err
}

func TestRollingControlRenewalStartupClassification(t *testing.T) {
	cases := []struct {
		name   string
		cause  error
		status int
		body   string
		retry  bool
	}{
		{name: "refused", cause: syscall.ECONNREFUSED, retry: true},
		{name: "reset", cause: syscall.ECONNRESET, retry: true},
		{name: "dial_timeout", cause: &net.OpError{Op: "dial", Net: "tcp", Err: context.DeadlineExceeded}, retry: true},
		{name: "fresh_EOF", cause: io.EOF, retry: true},
		{name: "bare_502", status: 502, body: "starting", retry: true},
		{name: "bare_503", status: 503, body: "starting", retry: true},
		{name: "bare_500", status: 500, body: "unknown server failure"},
		{name: "definitive_503", status: 503, body: `{"error":{"code":"unavailable","message":"L1 refused","retryable":false}}`},
		{name: "gate_refusal", status: 409, body: `{"error":{"code":"conflict","message":"identity mismatch","retryable":true}}`},
		{name: "decode_failure", status: 200, body: `{"broken"`},
		{name: "unknown_transport", cause: errors.New("unknown failure")},
		{name: "permission_denied", cause: syscall.EPERM},
		{name: "caller_canceled", cause: context.Canceled},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			failure := startupClientFailure(t, tc.cause, tc.status, tc.body)
			now := time.Date(2026, 10, 24, 0, 0, 0, 0, time.UTC)
			start := now
			baseline := l1.Node{NodeRegistration: contract.NodeRegistration{BootSessionID: "same-boot"}, AuthorityGeneration: 2}
			calls, waits := 0, 0
			r := rollingUpgrade{op: upgradeHost{
				now:  func() time.Time { return now },
				wait: func(context.Context, time.Duration) error { waits++; now = now.Add(100 * time.Millisecond); return nil },
				observe: func(context.Context, connprofile.Profile) (l1.Node, error) {
					calls++
					if calls == 1 {
						return l1.Node{}, fmt.Errorf("wrapped: %w", failure)
					}
					node := baseline
					node.LastHeartbeatAt = now
					return node, nil
				},
			}}
			_, err := r.waitControlRenewal(t.Context(), baseline, start, start)
			if tc.retry {
				// The first reachable L1 read anchors heartbeat evidence; the
				// next one must advance it, independently of the CLI clock.
				if err != nil || calls != 3 || waits != 2 {
					t.Fatalf("startup gap not retried: calls=%d waits=%d err=%v", calls, waits, err)
				}
			} else if !errors.Is(err, failure) || calls != 1 || waits != 0 {
				t.Fatalf("definitive failure was retried/lost: calls=%d waits=%d err=%v", calls, waits, err)
			}
		})
	}
}

func TestRollingControlRenewalStartupDeadlineAndAttemptProof(t *testing.T) {
	for _, mode := range []string{"unreachable", "job_starting", "replacement_attempt", "stale_lease", "wrong_boot", "wrong_authority", "late_ready", "job_refusal", "canceled"} {
		t.Run(mode, func(t *testing.T) {
			failure := startupClientFailure(t, syscall.ECONNREFUSED, 0, "")
			now := time.Date(2026, 10, 24, 0, 0, 0, 0, time.UTC)
			start := now
			baseline := l1.Node{NodeRegistration: contract.NodeRegistration{BootSessionID: "same-boot"}, AuthorityGeneration: 2,
				ActiveAttempts: []l1.NodeActiveAttempt{{JobID: "job", AttemptID: "original", LeaseExpiresAt: now.Add(l1.DefaultLeaseDuration)}}}
			calls, reads, waits := 0, 0, 0
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if mode == "canceled" {
				cancel()
			}
			r := rollingUpgrade{op: upgradeHost{
				now:  func() time.Time { return now },
				wait: func(context.Context, time.Duration) error { waits++; now = now.Add(time.Second); return nil },
				observe: func(context.Context, connprofile.Profile) (l1.Node, error) {
					calls++
					if mode == "unreachable" {
						return l1.Node{}, failure
					}
					node := baseline
					if mode == "wrong_boot" {
						node.BootSessionID = "other-boot"
					}
					if mode == "wrong_authority" {
						node.AuthorityGeneration++
					}
					if mode == "late_ready" {
						now = start.Add(upgradeRenewalWindow)
					}
					node.LastHeartbeatAt = now.Add(time.Nanosecond)
					return node, nil
				},
				readJob: func(context.Context, connprofile.Profile, string) (l1.Job, error) {
					reads++
					if mode == "job_refusal" {
						return l1.Job{}, &apiResponseError{Service: "L1", StatusCode: 409, APIError: contract.APIError{Code: contract.ErrorConflict}}
					}
					if mode == "job_starting" && reads == 1 {
						return l1.Job{}, failure
					}
					id := "original"
					if mode == "replacement_attempt" {
						id = "replacement"
					}
					expiry := now.Add(l1.DefaultLeaseDuration)
					if mode == "stale_lease" {
						expiry = baseline.ActiveAttempts[0].LeaseExpiresAt
					}
					return l1.Job{Attempts: []l1.Attempt{{AttemptID: id, State: contract.AttemptRunning, UpdatedAt: now, LeaseExpiresAt: expiry, LeaseLive: true}}}, nil
				},
			}}
			elapsed, err := r.waitControlRenewal(ctx, baseline, start, start)
			switch mode {
			case "job_starting":
				if err != nil || reads != 3 || waits != 2 {
					t.Fatalf("job transport not retried: reads=%d waits=%d err=%v", reads, waits, err)
				}
			case "job_refusal":
				var refusal *apiResponseError
				if !errors.As(err, &refusal) || waits != 0 || reads != 1 {
					t.Fatalf("job refusal was retried/lost: %v", err)
				}
			case "canceled":
				if !errors.Is(err, context.Canceled) || calls != 0 || waits != 0 {
					t.Fatalf("cancellation lost: %v", err)
				}
			default:
				if err == nil || elapsed != upgradeRenewalWindow {
					t.Fatalf("deadline/proof changed: elapsed=%v err=%v", elapsed, err)
				}
				if mode == "unreachable" && !errors.Is(err, failure) {
					t.Fatalf("last transport error lost: %v", err)
				}
			}
		})
	}
}

func TestRollingControlRenewalDefinitiveWinsJoinedTransient(t *testing.T) {
	transient := startupClientFailure(t, syscall.ECONNREFUSED, 0, "")
	refusal := startupClientFailure(t, nil, 409, `{"error":{"code":"conflict","message":"identity mismatch","retryable":false}}`)
	now := time.Date(2026, 10, 24, 0, 0, 0, 0, time.UTC)
	waits := 0
	r := rollingUpgrade{op: upgradeHost{now: func() time.Time { return now }, wait: func(context.Context, time.Duration) error { waits++; now = now.Add(upgradeRenewalWindow); return nil }, observe: func(context.Context, connprofile.Profile) (l1.Node, error) {
		return l1.Node{}, errors.Join(transient, refusal)
	}}}
	_, err := r.waitControlRenewal(t.Context(), l1.Node{}, now, now)
	if !errors.Is(err, refusal) || waits != 0 {
		t.Fatalf("definitive joined refusal was retried: waits=%d err=%v", waits, err)
	}
}
