package l1

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

func TestComputerStatusReadRetriesReadSnapshotExpired(t *testing.T) {
	const busy = `{"error":{"code":"unavailable","retryable":true,"details":{"reason":"read_snapshot_expired"}}}`
	for _, test := range []struct {
		name, refusal string
		wantCalls     int
	}{
		{"recovers", busy, 2},
		{"nonretryable", `{"error":{"code":"unavailable","retryable":false}}`, 1},
		{"different code", `{"error":{"code":"internal","retryable":true}}`, 1},
		{"malformed", `invalid`, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			h, _, computer := backupHarness(t, 0, nil)
			client := h.client(fabric.Identity{NodeID: "operator", Tags: []string{DefaultClientPrincipalTag}})
			base := client.Transport
			calls := 0
			client.Transport = statusReadTransport(func(request *http.Request) (*http.Response, error) {
				calls++
				if calls == 1 {
					return &http.Response{StatusCode: http.StatusServiceUnavailable, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(test.refusal))}, nil
				}
				return base.RoundTrip(request)
			})
			status, _, body := h.readStatus(t.Context(), client, "/v1/computers/"+computer.ComputerID)
			wantStatus := http.StatusServiceUnavailable
			if test.wantCalls == 2 {
				wantStatus = http.StatusOK
			}
			if status != wantStatus || calls != test.wantCalls {
				t.Fatalf("calls=%d status=%d body=%s", calls, status, body)
			}
			if status != http.StatusOK && string(body) != test.refusal {
				t.Fatalf("refusal changed: %s", body)
			}
		})
	}
}

func TestStoreStatusReadRetry(t *testing.T) {
	busy := snapshotUnavailable("read_snapshot_expired", context.DeadlineExceeded)
	for _, test := range []struct {
		name      string
		refusal   error
		failures  int
		wantCalls int
	}{
		{"recovers", fmt.Errorf("wrapped: %w", busy), 1, 2},
		{"exhaustion", busy, 10, 4},
		{"nonretryable", &Error{Code: contract.ErrorUnavailable, notRetryable: true}, 1, 1},
		{"different code", &Error{Code: contract.ErrorInternal}, 1, 1},
		{"untyped", errors.New("read snapshot unavailable"), 1, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			h, _, computer := backupHarness(t, 0, nil)
			calls := 0
			got, err := retryTestStatusRead(t.Context(), func() (Computer, error) {
				calls++
				if calls <= test.failures {
					return Computer{}, test.refusal
				}
				return h.store.GetComputer(t.Context(), computer.ComputerID)
			})
			if calls != test.wantCalls {
				t.Fatalf("calls=%d err=%v", calls, err)
			}
			if test.wantCalls > test.failures {
				if err != nil || got.ComputerID != computer.ComputerID {
					t.Fatalf("Computer=%+v err=%v", got, err)
				}
			} else if err != test.refusal {
				t.Fatalf("refusal changed: %v", err)
			}
		})
	}
}

func TestStoreStatusReadStopsAtCallerDeadline(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	calls := 0
	_, err := retryTestStatusRead(ctx, func() (Computer, error) {
		calls++
		cancel()
		return Computer{}, snapshotUnavailable("read_snapshot_expired", context.DeadlineExceeded)
	})
	if !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
}

type statusReadTransport func(*http.Request) (*http.Response, error)

func (transport statusReadTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return transport(request)
}
