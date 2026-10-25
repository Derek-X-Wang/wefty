package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestSeatWaitCleanupHasBoundedTypedOutcome(t *testing.T) {
	for _, state := range []l1.SeatRequestState{l1.SeatRefused, l1.SeatEnded} {
		for _, complete := range []bool{false, true} {
			t.Run(string(state)+"/complete="+map[bool]string{false: "false", true: "true"}[complete], func(t *testing.T) {
				clock := &nodeCLIClock{now: time.Date(2026, 10, 24, 0, 0, 0, 0, time.UTC)}
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				var requests atomic.Int32
				id := "seat_00000000000000000000000000000921"
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method != http.MethodGet || r.URL.Path != "/v1/seat-requests/"+id {
						t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
					}
					n := requests.Add(1)
					switch n {
					case 2:
						clock.Advance(time.Minute - time.Nanosecond)
					case 3:
						clock.Advance(time.Nanosecond)
					case 4:
						cancel()
					}
					json.NewEncoder(w).Encode(l1.SeatCallResult{SeatRequest: l1.SeatRequest{SeatID: id, State: state, Reason: "Computer failed to start"}, CleanupPending: !complete || n < 3})
				}))
				defer server.Close()
				clients := &apiClients{l1: statusClient(t, "L1", "test", server), seatWaitNow: clock.Now, wait: func(ctx context.Context, _ time.Duration) error { return ctx.Err() }}
				var out bytes.Buffer
				err := executeSeatRequest(ctx, clients, true, []string{"wait", id}, &out, io.Discard)
				var apiErr *apiResponseError
				want := seatErrorStartFailed
				if state == l1.SeatEnded {
					want = seatErrorEnded
				}
				if !errors.As(err, &apiErr) || apiErr.APIError.Code != want || commandExitCode(err) != 5 {
					t.Fatalf("wait must end with its typed outcome, got %v", err)
				}
				if requests.Load() != 3 {
					t.Fatalf("wait ended before or after the exact cleanup boundary: requests=%d", requests.Load())
				}
				writeCommandError(&out, err, true)
				var report contract.ErrorResponse
				if err := json.Unmarshal(out.Bytes(), &report); err != nil {
					t.Fatal(err)
				}
				pending := !complete
				if bytes.Contains(out.Bytes(), []byte("cleanup still pending")) != pending || (report.Error.Details["cleanup_pending"] == true) != pending {
					t.Fatalf("pending disclosure: %s", out.String())
				}
				if report.Error.Details["seat_id"] != id {
					t.Fatalf("missing replay ID: %s", out.String())
				}
			})
		}
	}
}
