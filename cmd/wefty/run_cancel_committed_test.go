package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
)

// committedUnreadCancelCases are the first answers to a run cancel. Only the
// committed-unread answer earns one resend (#1560); the near matches keep
// today's single request and error.
var committedUnreadCancelCases = []struct {
	name               string
	retryable, applied bool
	wantCancels        int32
}{
	{"committed unread resends once", true, true, 2},
	{"not applied keeps the error", true, false, 1},
	{"not retryable keeps the error", false, true, 1},
}

// stubRunCancel answers the first run cancel with the committed-unread relay
// and every later one with the cancelled run.
func stubRunCancel(retryable, applied bool, cancels *atomic.Int32, w http.ResponseWriter, runID string) {
	if cancels.Add(1) == 1 {
		response := committedUnreadRunCancel(retryable, applied)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(response.StatusCode)
		_, _ = io.Copy(w, response.Body)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"run_id":"` + runID + `","status":"failed","failure_reason":"the L1 job was canceled"}`))
}

func TestCancelRunResendsAfterCommittedReadUnavailable(t *testing.T) {
	for _, test := range committedUnreadCancelCases {
		t.Run(test.name, func(t *testing.T) {
			var cancels atomic.Int32
			clients := &apiClients{l3: &apiClient{name: "L3", flag: "l3", address: "stub", client: &http.Client{Transport: handlerTransport{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost || r.URL.Path != "/v1/runs/run_cancel/cancel" {
					t.Errorf("request=%s %s", r.Method, r.URL.Path)
				}
				stubRunCancel(test.retryable, test.applied, &cancels, w, "run_cancel")
			})}}}}
			var stdout bytes.Buffer
			err := execute(t.Context(), clients, true, []string{"cancel", "run_cancel"}, &stdout, io.Discard)
			if got := cancels.Load(); got != test.wantCancels {
				t.Fatalf("run cancel requests=%d, want %d; err=%v", got, test.wantCancels, err)
			}
			if test.wantCancels == 2 {
				if err != nil || !strings.Contains(stdout.String(), `"failure_reason": "the L1 job was canceled"`) {
					t.Fatalf("committed cancel reported as failure: %v %q", err, stdout.String())
				}
				return
			}
			if err == nil {
				t.Fatalf("unconfirmed cancel lost its error: %q", stdout.String())
			}
		})
	}
}

// TestSetupSmokeCleanupResendsAfterCommittedReadUnavailable: a smoke run that
// does not settle is cancelled in cleanup. A committed cancel whose read
// expired is resent once, so the cleanup is not reported as unconfirmed.
func TestSetupSmokeCleanupResendsAfterCommittedReadUnavailable(t *testing.T) {
	for _, test := range committedUnreadCancelCases {
		t.Run(test.name, func(t *testing.T) {
			var cancels atomic.Int32
			clients := &apiClients{l3: &apiClient{name: "L3", flag: "l3", address: "stub", client: &http.Client{Transport: handlerTransport{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch {
				case r.Method == http.MethodPost && r.URL.Path == "/v1/runs":
					w.WriteHeader(http.StatusCreated)
					_, _ = w.Write([]byte(`{"run_id":"run_smoke"}`))
				case r.Method == http.MethodGet && r.URL.Path == "/v1/runs/run_smoke":
					// The run never settles, so cleanup must cancel it.
					w.WriteHeader(http.StatusNotFound)
					_ = json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{Code: contract.ErrorNotFound, Message: "run not found"}})
				case r.Method == http.MethodPost && r.URL.Path == "/v1/runs/run_smoke/cancel":
					stubRunCancel(test.retryable, test.applied, &cancels, w, "run_smoke")
				default:
					t.Errorf("request=%s %s", r.Method, r.URL.Path)
					w.WriteHeader(http.StatusTeapot)
				}
			})}}}}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			_, err := runSetupSmoke(ctx, clients, "node-cli")
			if got := cancels.Load(); got != test.wantCancels {
				t.Fatalf("run cancel requests=%d, want %d; err=%v", got, test.wantCancels, err)
			}
			if err == nil || !strings.Contains(err.Error(), "run not found") {
				t.Fatalf("smoke lost its first error: %v", err)
			}
			unconfirmed := strings.Contains(err.Error(), "smoke cancellation was not confirmed")
			if unconfirmed != (test.wantCancels == 1) {
				t.Fatalf("cleanup confirmation=%v for %q: %v", !unconfirmed, test.name, err)
			}
		})
	}
}
