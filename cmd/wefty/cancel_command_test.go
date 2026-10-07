package main

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

func TestCancelExitCodesFromRealBinary(t *testing.T) {
	binary := buildWefty(t)
	for _, test := range []struct {
		name   string
		status int
		code   contract.ErrorCode
		exit   int
	}{
		{"canceled", http.StatusOK, "", 0},
		{"unauthorized", http.StatusUnauthorized, contract.ErrorUnauthorized, exitUnauthorized},
		{"not-found", http.StatusNotFound, contract.ErrorNotFound, exitNotFound},
		{"service", http.StatusConflict, contract.ErrorCode("cancel_service"), exitConflict},
		{"claimed", http.StatusConflict, contract.ErrorCode("cancel_not_queued"), exitConflict},
		{"internal", http.StatusInternalServerError, contract.ErrorInternal, exitFailure},
	} {
		t.Run(test.name, func(t *testing.T) {
			address := startStubLedger(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost || r.URL.Path != "/v1/jobs/job-cancel/cancel" {
					t.Errorf("request=%s %s", r.Method, r.URL.Path)
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(test.status)
				if test.code == "" {
					_, _ = w.Write([]byte(`{"job_id":"job-cancel","state":"failed","outcome":"canceled"}`))
					return
				}
				_ = json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{Code: test.code, Message: "cancel refusal"}})
			})
			code, output := runWefty(t, binary, 30*time.Second, "--json", "--l1="+address, "--l3=", "cancel", "job-cancel")
			if code != test.exit {
				t.Fatalf("exit=%d want=%d output=%s", code, test.exit, output)
			}
			if test.code == "" && !strings.Contains(output, `"outcome": "canceled"`) {
				t.Fatalf("missing outcome=%s", output)
			}
		})
	}
	code, output := runWefty(t, binary, 30*time.Second, "cancel")
	if code != exitUsage {
		t.Fatalf("usage exit=%d output=%s", code, output)
	}
}

func TestCancelRunExitCodesFromRealBinary(t *testing.T) {
	binary := buildWefty(t)
	for _, test := range []struct {
		name   string
		status int
		code   contract.ErrorCode
		exit   int
	}{
		{"canceled", http.StatusOK, "", 0},
		{"forbidden", http.StatusForbidden, contract.ErrorForbidden, exitUnauthorized},
		{"not-found", http.StatusNotFound, contract.ErrorNotFound, exitNotFound},
		{"internal", http.StatusInternalServerError, contract.ErrorInternal, exitFailure},
	} {
		t.Run(test.name, func(t *testing.T) {
			address := startStubLedger(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost || r.URL.Path != "/v1/runs/run_cancel/cancel" {
					t.Errorf("request=%s %s", r.Method, r.URL.Path)
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(test.status)
				if test.code == "" {
					_, _ = w.Write([]byte(`{"run_id":"run_cancel","status":"failed","failure_reason":"the L1 job was canceled"}`))
					return
				}
				_ = json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{Code: test.code, Message: "cancel refusal"}})
			})
			code, output := runWefty(t, binary, 30*time.Second, "--json", "--l1="+unusedAddress(t), "--l3="+address, "cancel", "run_cancel")
			if code != test.exit {
				t.Fatalf("exit=%d want=%d output=%s", code, test.exit, output)
			}
			if test.code == "" && !strings.Contains(output, `"failure_reason": "the L1 job was canceled"`) {
				t.Fatalf("missing cancellation=%s", output)
			}
		})
	}
	code, output := runWefty(t, binary, 30*time.Second, "--l1="+unusedAddress(t), "--l3=", "cancel", "run_cancel")
	if code != exitUsage || !strings.Contains(output, "--l3") {
		t.Fatalf("L1-only run cancel: exit=%d output=%s", code, output)
	}
}
