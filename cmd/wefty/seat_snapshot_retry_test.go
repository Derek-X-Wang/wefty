package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

func seatSnapshotRetryStub(t *testing.T, answer func(int32, *http.Request, http.ResponseWriter)) (*apiClients, *atomic.Int32) {
	t.Helper()
	var requests atomic.Int32
	client := &apiClient{name: "L1", client: &http.Client{Transport: storageRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Body == nil {
			r.Body = http.NoBody
		}
		w := httptest.NewRecorder()
		w.Header().Set("Content-Type", "application/json")
		answer(requests.Add(1), r, w)
		return w.Result(), nil
	})}}
	return &apiClients{l1: client}, &requests
}

func TestSeatReleaseBareSnapshotExpiryResendsOnce(t *testing.T) {
	for _, test := range []struct {
		name, reason string
		failures     int32
		wantRequests int32
		wantExit     int
	}{
		{"hold expiry", "read_snapshot_expired", 1, 2, 0},
		{"admission expiry", "read_snapshot_admission_expired", 1, 2, 0},
		{"two expiries", "read_snapshot_expired", 2, 2, exitUnavailable},
		{"two admission expiries", "read_snapshot_admission_expired", 2, 2, exitUnavailable},
		{"post change belongs to G1", "read_snapshot_post_change_failed", 1, 1, exitUnavailable},
		{"other unavailable", "identity_unverifiable", 1, 1, exitUnavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("WEFTY_SEAT_STATE_ROOT", filepath.Join(t.TempDir(), "seats"))
			id := "seat_" + strings.Repeat("b", 32)
			const secret = "release-retry-secret"
			if _, err := storeSeatCredential(id, secret); err != nil {
				t.Fatal(err)
			}
			var firstHeaders http.Header
			clients, requests := seatSnapshotRetryStub(t, func(seen int32, r *http.Request, w http.ResponseWriter) {
				body, err := io.ReadAll(r.Body)
				if err != nil || len(body) != 0 || r.Method != http.MethodPost || r.URL.Path != "/v1/seats/"+id+"/release" || r.Header.Get(l1.SeatCredentialHeader) != secret {
					t.Errorf("release request: %s %s body=%q err=%v", r.Method, r.URL.Path, body, err)
				}
				if seen == 1 {
					firstHeaders = r.Header.Clone()
				} else if !reflect.DeepEqual(firstHeaders, r.Header) {
					t.Error("resend changed release headers")
				}
				if seen <= test.failures {
					if test.reason == "read_snapshot_post_change_failed" {
						writeCommittedReadUnavailable(w, true)
					} else {
						writeRetryableUnavailable(w, test.reason)
					}
					return
				}
				_ = json.NewEncoder(w).Encode(l1.SeatCallResult{SeatRequest: l1.SeatRequest{SeatID: id, State: l1.SeatEnded, Reason: "released"}})
			})
			waits := 0
			withRetryWait(t, func(context.Context, time.Duration) error { waits++; return nil })
			var out bytes.Buffer
			err := executeSeatRelease(t.Context(), clients, true, id, &out)
			exit := 0
			if err != nil {
				exit = commandExitCode(err)
			}
			if exit != test.wantExit || requests.Load() != test.wantRequests || waits != int(test.wantRequests-1) {
				t.Fatalf("release: err=%v exit=%d requests=%d waits=%d", err, exit, requests.Load(), waits)
			}
			if err == nil {
				var reply l1.SeatCallResult
				if err := json.Unmarshal(out.Bytes(), &reply); err != nil || reply.SeatID != id || reply.State != l1.SeatEnded || reply.Reason != "released" {
					t.Fatalf("release reply=%s err=%v", &out, err)
				}
			} else {
				var response *apiResponseError
				if !errors.As(err, &response) || response.APIError.Details["reason"] != test.reason {
					t.Fatalf("last refusal changed: %v", err)
				}
			}
		})
	}
}

func TestComputerGrantBareSnapshotExpiryReplaysSameRequest(t *testing.T) {
	for _, reason := range []string{"read_snapshot_expired", "read_snapshot_admission_expired"} {
		t.Run(reason, func(t *testing.T) {
			request := l1.ComputerGrantMutationRequest{PolicyRevision: 7, FabricID: "fabric-test", Permission: l1.ComputerGrantView, IdempotencyKey: "same-grant-key"}
			var firstBody string
			var firstHeaders http.Header
			clients, requests := seatSnapshotRetryStub(t, func(seen int32, r *http.Request, w http.ResponseWriter) {
				body, err := io.ReadAll(r.Body)
				var decoded l1.ComputerGrantMutationRequest
				if err != nil || json.Unmarshal(body, &decoded) != nil || decoded != request || r.Method != http.MethodPut || r.URL.Path != "/v1/computers/computer-test/grants/person-test" {
					t.Errorf("grant request: %s %s body=%s err=%v", r.Method, r.URL.Path, body, err)
				}
				if seen == 1 {
					firstBody, firstHeaders = string(body), r.Header.Clone()
					writeRetryableUnavailable(w, reason)
					return
				}
				if string(body) != firstBody || !reflect.DeepEqual(r.Header, firstHeaders) {
					t.Error("resend changed grant body or headers")
				}
				w.Header().Set("Idempotent-Replay", "true")
				_ = json.NewEncoder(w).Encode(l1.ComputerGrantMutationResult{Grant: l1.ComputerGrant{UserID: "person-test", Permission: l1.ComputerGrantView, PolicyRevision: 8}})
			})
			waits := 0
			withRetryWait(t, func(context.Context, time.Duration) error { waits++; return nil })
			result, err := clients.mutateComputerGrant(t.Context(), "computer-test", "person-test", request)
			if err != nil || requests.Load() != 2 || waits != 1 || !result.Replayed || result.MutationApplied || result.Grant.PolicyRevision != 8 || result.Grant.Permission != request.Permission {
				t.Fatalf("grant replay: result=%+v err=%v requests=%d waits=%d", result, err, requests.Load(), waits)
			}
		})
	}
}

func TestSeatReleaseSnapshotRetryCancellation(t *testing.T) {
	t.Setenv("WEFTY_SEAT_STATE_ROOT", filepath.Join(t.TempDir(), "seats"))
	id := "seat_" + strings.Repeat("b", 32)
	if _, err := storeSeatCredential(id, "cancel-secret"); err != nil {
		t.Fatal(err)
	}
	clients, requests := seatSnapshotRetryStub(t, func(_ int32, _ *http.Request, w http.ResponseWriter) {
		writeRetryableUnavailable(w, "read_snapshot_expired")
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	withRetryWait(t, func(ctx context.Context, _ time.Duration) error { cancel(); return ctx.Err() })
	err := executeSeatRelease(ctx, clients, true, id, io.Discard)
	if !errors.Is(err, context.Canceled) || requests.Load() != 1 {
		t.Fatalf("release cancellation: err=%v requests=%d", err, requests.Load())
	}
}

func TestSeatReplayFailureIsNotResent(t *testing.T) {
	for _, mode := range []string{"transport", "not retryable", "wrong code"} {
		t.Run(mode, func(t *testing.T) {
			requests := 0
			client := &apiClient{name: "L1", client: &http.Client{Transport: storageRoundTripFunc(func(r *http.Request) (*http.Response, error) {
				requests++
				if mode == "transport" {
					return nil, io.ErrUnexpectedEOF
				}
				code := contract.ErrorUnavailable
				if mode == "wrong code" {
					code = contract.ErrorForbidden
				}
				body, _ := json.Marshal(contract.ErrorResponse{Error: contract.APIError{Code: code, Retryable: mode != "not retryable", Details: map[string]any{"reason": "read_snapshot_expired"}}})
				return &http.Response{StatusCode: http.StatusServiceUnavailable, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(body)), Request: r}, nil
			})}}
			withRetryWait(t, func(context.Context, time.Duration) error { t.Error("unexpected resend backoff"); return nil })
			_, err := client.resendOnBareSnapshotExpiry(t.Context(), http.MethodPost, "/v1/seats/id/release", nil, nil, nil, http.StatusOK)
			if err == nil || requests != 1 {
				t.Fatalf("%s failure: err=%v requests=%d", mode, err, requests)
			}
		})
	}
}

func TestSeatScreenDiscoverySnapshotExpiryKeepsOneAction(t *testing.T) {
	clients, id, stats := seatScreenReworkFixtureWithStats(t, "discovery-expiry")
	t.Cleanup(clients.close)
	withRetryWait(t, func(context.Context, time.Duration) error { return nil })
	err := executeSeatScreen(t.Context(), clients, true, []string{"click", id, "1", "2"}, io.Discard, io.Discard)
	if err != nil || stats.discovery.Load() != 2 || stats.input.Load() != 1 || stats.events.Load() != 2 {
		t.Fatalf("discovery retry: err=%v discoveries=%d input=%d events=%d", err, stats.discovery.Load(), stats.input.Load(), stats.events.Load())
	}
}

func TestSeatWaitSnapshotExpiryUsesPollCadence(t *testing.T) {
	t.Setenv("WEFTY_SEAT_STATE_ROOT", filepath.Join(t.TempDir(), "seats"))
	id := "seat_" + strings.Repeat("c", 32)
	clients, requests := seatSnapshotRetryStub(t, func(seen int32, r *http.Request, w http.ResponseWriter) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/seat-requests/"+id {
			t.Errorf("unexpected poll: %s %s", r.Method, r.URL.Path)
		}
		if seen == 2 {
			writeRetryableUnavailable(w, "read_snapshot_expired")
			return
		}
		state, secret := l1.SeatStarting, ""
		if seen == 3 {
			state, secret = l1.SeatLive, "wait-secret"
		}
		_ = json.NewEncoder(w).Encode(l1.SeatCallResult{SeatRequest: l1.SeatRequest{SeatID: id, State: state}, Credential: secret})
	})
	withRetryWait(t, func(context.Context, time.Duration) error {
		t.Error("poll used quiet client retry")
		return context.Canceled
	})
	waits := 0
	clients.wait = func(ctx context.Context, d time.Duration) error {
		waits++
		if d != 250*time.Millisecond {
			t.Errorf("poll cadence=%v", d)
		}
		if _, bounded := ctx.Deadline(); bounded {
			t.Error("Seat wait acquired a deadline")
		}
		return nil
	}
	var out bytes.Buffer
	err := waitSeatCall(context.Background(), clients, true, id, &out)
	secret, credentialErr := loadSeatCredential(id)
	var result l1.SeatCallResult
	decodeErr := json.Unmarshal(out.Bytes(), &result)
	if err != nil || credentialErr != nil || decodeErr != nil || result.State != l1.SeatLive || secret != "wait-secret" || requests.Load() != 3 || waits != 2 {
		t.Fatalf("wait after expiry: err=%v credentialErr=%v requests=%d waits=%d out=%s", err, credentialErr, requests.Load(), waits, &out)
	}
}

func TestSeatWaitSnapshotExpiryCallerCancelStillRecorded(t *testing.T) {
	id := "seat_" + strings.Repeat("c", 32)
	clients, requests := seatSnapshotRetryStub(t, func(_ int32, _ *http.Request, w http.ResponseWriter) {
		writeRetryableUnavailable(w, "read_snapshot_admission_expired")
	})
	withRetryWait(t, func(context.Context, time.Duration) error {
		t.Error("poll used quiet client retry")
		return context.Canceled
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	waits := 0
	clients.wait = func(ctx context.Context, d time.Duration) error {
		waits++
		if d != 250*time.Millisecond {
			t.Errorf("poll cadence=%v", d)
		}
		if waits == 2 {
			cancel()
		}
		return ctx.Err()
	}
	err := waitSeatCall(ctx, clients, true, id, io.Discard)
	if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "Seat request "+id+" still recorded; wait again with wefty seat wait "+id) || requests.Load() != 2 || waits != 2 {
		t.Fatalf("cancelled wait: err=%v requests=%d waits=%d", err, requests.Load(), waits)
	}
}

func TestSeatWaitSnapshotExpiryKeepsCleanupLimit(t *testing.T) {
	for _, reason := range []string{"read_snapshot_expired", "read_snapshot_admission_expired"} {
		t.Run(reason, func(t *testing.T) {
			clock := &nodeCLIClock{now: time.Date(2026, 11, 6, 0, 0, 0, 0, time.UTC)}
			id := "seat_" + strings.Repeat("d", 32)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			clients, requests := seatSnapshotRetryStub(t, func(seen int32, _ *http.Request, w http.ResponseWriter) {
				if seen > 3 {
					t.Error("wait polled past the cleanup limit")
					cancel()
				}
				if seen == 1 {
					_ = json.NewEncoder(w).Encode(l1.SeatCallResult{SeatRequest: l1.SeatRequest{SeatID: id, State: l1.SeatEnded, Reason: "released"}, CleanupPending: true})
					return
				}
				switch seen {
				case 2:
					clock.Advance(time.Minute - time.Nanosecond)
				case 3:
					clock.Advance(time.Nanosecond)
				}
				writeRetryableUnavailable(w, reason)
			})
			clients.seatWaitNow = clock.Now
			clients.wait = func(context.Context, time.Duration) error { return nil }
			err := waitSeatCall(ctx, clients, true, id, io.Discard)
			var apiErr *apiResponseError
			if !errors.As(err, &apiErr) || apiErr.APIError.Code != seatErrorEnded || apiErr.APIError.Details["cleanup_pending"] != true || commandExitCode(err) != 5 {
				t.Fatalf("busy L1 after the cleanup limit must end with the Seat's outcome, got %v", err)
			}
			if requests.Load() != 3 {
				t.Fatalf("wait ended before or after the exact cleanup boundary: requests=%d", requests.Load())
			}
		})
	}
}
