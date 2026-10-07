package main

import (
	"encoding/json"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

func TestEveryCommandErrorContractFromRealBinary(t *testing.T) {
	binary := buildWefty(t)
	cases := []struct {
		code         contract.ErrorCode
		exit, status int
	}{
		{contract.ErrorInvalidRequest, exitUsage, 400},
		{contract.ErrorForbidden, exitUnauthorized, 403},
		{contract.ErrorNotFound, exitNotFound, 404},
		{contract.ErrorConflict, exitConflict, 409},
		{contract.ErrorInternal, exitFailure, 503},
	}
	commands := [][]string{
		{"submit", "--workflow-ref=test/v1"}, {"rerun", "missing"},
		{"drain", "missing"}, {"nodes", "list"}, {"runs", "list"},
		{"logs", "missing"}, {"inspect", "missing"}, {"results", "missing"},
		{"services", "status", "missing"},
	}
	for _, c := range cases {
		address := startStubLedger(t, func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(c.status)
			_ = json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{
				Code: c.code, Message: "contract refusal", Retryable: c.code == contract.ErrorInternal,
				Details: map[string]any{"reason": "test"}, RequestID: "upstream-test-id",
			}})
		})
		for _, command := range commands {
			t.Run(command[0]+"/"+string(c.code), func(t *testing.T) {
				args := append([]string{"--l1", address, "--l3", address}, command...)
				args = append(args, "--json")
				code, output := runWefty(t, binary, 30*time.Second, args...)
				if code != c.exit {
					t.Errorf("exit=%d want=%d output=%s", code, c.exit, output)
				}
				var envelope contract.ErrorResponse
				if err := json.Unmarshal([]byte(output), &envelope); err != nil || envelope.Error.Code != c.code || envelope.Error.RequestID != "upstream-test-id" || envelope.Error.Details["reason"] != "test" {
					t.Fatalf("error envelope=%s decode=%v", output, err)
				}
			})
		}
	}
}

func TestLocalAndUsageJSONErrorsFromRealBinary(t *testing.T) {
	binary := buildWefty(t)
	for _, jsonFlag := range []string{"--json", "--json=true"} {
		for _, index := range []int{0, 1, 3} {
			args := []string{"submit", "--script", filepath.Join(t.TempDir(), "absent.sh")}
			args = append(args[:index], append([]string{jsonFlag}, args[index:]...)...)
			code, output := runWefty(t, binary, 30*time.Second, args...)
			var envelope contract.ErrorResponse
			if err := json.Unmarshal([]byte(output), &envelope); err != nil || code != exitFailure || envelope.Error.Code != contract.ErrorInternal || envelope.Error.Message == "" || envelope.Error.Retryable || envelope.Error.RequestID != "" {
				t.Errorf("local args=%v exit=%d output=%s decode=%v", args, code, output, err)
			}
		}
	}
	for _, args := range [][]string{
		{"--bad-global", "--json"}, {"--json=garbage"}, {"--json", "unknown-command"},
		{"submit", "--bad", "--json"}, {"rerun", "missing", "--bad", "--json"},
		{"nodes", "list", "--bad", "--json"}, {"runs", "list", "--bad", "--json"},
		{"logs", "missing", "--bad", "--json"}, {"results", "missing", "--bad", "--json"},
		{"drain", "missing", "--revision=bad", "--json"}, {"inspect", "--json"},
		{"services", "status", "--json"}, {"node", "--json"},
	} {
		code, output := runWefty(t, binary, 30*time.Second, args...)
		var envelope contract.ErrorResponse
		if err := json.Unmarshal([]byte(output), &envelope); err != nil || code != exitUsage || envelope.Error.Code != contract.ErrorInvalidRequest || envelope.Error.Retryable {
			t.Errorf("usage args=%v exit=%d output=%s decode=%v", args, code, output, err)
		}
	}
	code, output := runWefty(t, binary, 30*time.Second, "--json", "unknown-command", "--json=false")
	if code != exitUsage || json.Valid([]byte(output)) {
		t.Errorf("false flag exit=%d output=%s", code, output)
	}
}
