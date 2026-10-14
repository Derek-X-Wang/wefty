package l1

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

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
		{"different reason", snapshotUnavailable("read_snapshot_post_change_failed", nil), 1, 1},
		{"missing reason", &Error{Code: contract.ErrorUnavailable}, 1, 1},
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

func TestTestHTTPReadRetryPolicy(t *testing.T) {
	const expired = `{"error":{"code":"unavailable","retryable":true,"details":{"reason":"read_snapshot_expired"}}}`
	for _, test := range []struct {
		name, method, refusal string
		failures, wantCalls   int
	}{
		{"listing recovers", http.MethodGet, expired, 1, 2},
		{"listing exhausts", http.MethodGet, expired, 10, 4},
		{"nonretryable", http.MethodGet, `{"error":{"code":"unavailable","retryable":false,"details":{"reason":"read_snapshot_expired"}}}`, 1, 1},
		{"other reason", http.MethodGet, `{"error":{"code":"unavailable","retryable":true,"details":{"reason":"read_snapshot_post_change_failed"}}}`, 1, 1},
		{"malformed", http.MethodGet, `not-json`, 1, 1},
		{"POST never retries", http.MethodPost, expired, 1, 1},
		{"PUT never retries", http.MethodPut, expired, 1, 1},
		{"DELETE never retries", http.MethodDelete, expired, 1, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			client := &http.Client{Transport: statusReadTransport(func(request *http.Request) (*http.Response, error) {
				calls++
				status, body := http.StatusOK, "complete"
				if calls <= test.failures {
					status, body = http.StatusServiceUnavailable, test.refusal
				}
				return &http.Response{StatusCode: status, Header: http.Header{"X-Test": {"preserved"}}, Body: io.NopCloser(strings.NewReader(body))}, nil
			})}
			request, _ := http.NewRequestWithContext(t.Context(), test.method, "http://control-plane/v1/nodes", nil)
			response, err := doTestReadHTTP(client, request)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			body, err := io.ReadAll(response.Body)
			expected := test.refusal
			if test.wantCalls > test.failures {
				expected = "complete"
			}
			if err != nil || calls != test.wantCalls || string(body) != expected || response.Header.Get("X-Test") != "preserved" {
				t.Fatalf("calls=%d response=%+v body=%s err=%v", calls, response, body, err)
			}
		})
	}
}

func TestTestReadHandlerRecoversFromRealSnapshotExpiry(t *testing.T) {
	h, _, computer := backupHarness(t, 2, nil)
	h.stopServer()
	original := h.store.clock
	calls := 0
	h.store.clock = ClockFunc(func() time.Time {
		calls++
		if calls == 1 {
			time.Sleep(readSnapshotHardLimit + 50*time.Millisecond)
		}
		return original.Now()
	})
	defer func() { h.store.clock = original }()
	var attempts []error
	ctx := context.WithValue(t.Context(), testReadAttemptObserverKey{}, func(err error) { attempts = append(attempts, err) })
	response := recordTestRead(computerSnapshotRequest(ctx, computer.ComputerID), h.server.getComputer)
	if len(attempts) != 2 || response.Code != http.StatusOK {
		t.Fatalf("attempts=%v status=%d body=%s", attempts, response.Code, response.Body.String())
	}
	assertSnapshotUnavailable(t, attempts[0], "read_snapshot_expired")
}

func TestTestHTTPReadRecoversFromRealSnapshotExpiry(t *testing.T) {
	h, _, computer := backupHarness(t, 2, nil)
	h.stopServer()
	original := h.store.clock
	calls := 0
	h.store.clock = ClockFunc(func() time.Time {
		calls++
		if calls == 1 {
			time.Sleep(readSnapshotHardLimit + 50*time.Millisecond)
		}
		return original.Now()
	})
	defer func() { h.store.clock = original }()
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.server.getComputer(w, computerSnapshotRequest(r.Context(), computer.ComputerID))
	}))
	defer endpoint.Close()
	var attempts []error
	ctx := context.WithValue(t.Context(), testReadAttemptObserverKey{}, func(err error) { attempts = append(attempts, err) })
	request, _ := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.URL, nil)
	response, err := doTestReadHTTP(endpoint.Client(), request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if len(attempts) != 2 || response.StatusCode != http.StatusOK {
		t.Fatalf("attempts=%v status=%d", attempts, response.StatusCode)
	}
	assertSnapshotUnavailable(t, attempts[0], "read_snapshot_expired")
}

func TestTestPOSTProofReadReplaysOnlyReadBody(t *testing.T) {
	const body = `{"node_id":"stable","boot_session_id":"boot"}`
	const expired = `{"error":{"code":"unavailable","retryable":true,"details":{"reason":"read_snapshot_expired"}}}`
	for _, path := range []string{"/v1/host-boot-session-proof", "/v1/computers/computer/token-scope-proof", "/v1/agent/jobs/job/service-binding-proof"} {
		for _, direct := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/direct=%t", path, direct), func(t *testing.T) {
				calls := 0
				handler := func(w http.ResponseWriter, r *http.Request) {
					calls++
					got, err := io.ReadAll(r.Body)
					if err != nil || string(got) != body {
						t.Fatalf("attempt %d body=%s err=%v", calls, got, err)
					}
					if calls == 1 {
						w.WriteHeader(http.StatusServiceUnavailable)
						io.WriteString(w, expired)
						return
					}
					io.WriteString(w, "complete")
				}
				request := httptest.NewRequest(http.MethodPost, "http://control-plane"+path, strings.NewReader(body))
				if direct {
					response := recordTestRead(request, handler)
					if calls != 2 || response.Code != http.StatusOK || response.Body.String() != "complete" {
						t.Fatalf("calls=%d status=%d body=%s", calls, response.Code, response.Body.String())
					}
				} else {
					request.RequestURI = ""
					client := &http.Client{Transport: statusReadTransport(func(r *http.Request) (*http.Response, error) {
						w := httptest.NewRecorder()
						handler(w, r)
						return w.Result(), nil
					})}
					response, err := doTestReadHTTP(client, request)
					if err != nil {
						t.Fatal(err)
					}
					defer response.Body.Close()
					if calls != 2 || response.StatusCode != http.StatusOK {
						t.Fatalf("calls=%d status=%d", calls, response.StatusCode)
					}
				}
			})
		}
	}
}

func TestTestHTTPReadStopsAtCallerCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	calls := 0
	client := &http.Client{Transport: statusReadTransport(func(*http.Request) (*http.Response, error) {
		calls++
		cancel()
		return &http.Response{StatusCode: http.StatusServiceUnavailable, Body: io.NopCloser(strings.NewReader(`{"error":{"code":"unavailable","retryable":true,"details":{"reason":"read_snapshot_expired"}}}`))}, nil
	})}
	request, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://control-plane/v1/nodes", nil)
	response, err := doTestReadHTTP(client, request)
	if response != nil {
		response.Body.Close()
	}
	if calls != 1 || !errors.Is(err, context.Canceled) {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
}
