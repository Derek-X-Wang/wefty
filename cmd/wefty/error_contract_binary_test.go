package main

import (
	"encoding/json"
	"io"
	"net/http"
	"path/filepath"
	"reflect"
	"strings"
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
		{contract.ErrorUnavailable, 13, 503},
	}
	commands := [][]string{
		{"submit", "--workflow-ref=test/v1"}, {"rerun", "missing"},
		{"drain", "missing"}, {"nodes", "list"}, {"runs", "list"},
		{"logs", "missing"}, {"inspect", "missing"}, {"results", "missing"},
		{"services", "status", "missing"}, {"computers", "list"}, {"jobs", "list"},
	}
	for _, c := range cases {
		address := startStubLedger(t, func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(c.status)
			_ = json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{
				Code: c.code, Message: "contract refusal", Retryable: c.code == contract.ErrorUnavailable,
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
		{"nodes", "list", "--bad", "--json"}, {"computers", "list", "--bad", "--json"}, {"runs", "list", "--bad", "--json"},
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

func TestReviewTypedOutcomesFromRealBinary(t *testing.T) {
	binary := buildWefty(t)
	address := startStubLedger(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v1/nodes":
			_, _ = w.Write([]byte(`{"nodes":[]}`))
		case "/v1/runs/failed":
			_ = json.NewEncoder(w).Encode(contract.RunRecord{RunID: "failed", Status: contract.RunFailed, FailureReason: "test failure"})
		default:
			_ = json.NewEncoder(w).Encode(contract.RunRecord{RunID: "queued", Status: contract.RunQueued})
		}
	})
	for _, c := range []struct {
		args  []string
		exit  int
		field string
		value any
	}{
		{[]string{"wait", "queued", "--timeout=100ms"}, 11, "timed_out", true},
		{[]string{"wait", "failed"}, 10, "status", "failed"},
		{[]string{"status"}, 12, "ready", false},
	} {
		t.Run(c.args[0]+"/"+c.field, func(t *testing.T) {
			args := append([]string{"--json", "--l1", address, "--l3", address}, c.args...)
			code, output := runWefty(t, binary, 30*time.Second, args...)
			var document map[string]any
			if err := json.Unmarshal([]byte(output), &document); err != nil || code != c.exit || document[c.field] != c.value || document["error"] != nil {
				t.Fatalf("exit=%d output=%s decode=%v", code, output, err)
			}
		})
	}
}

func TestReviewUnavailableFromRealBinary(t *testing.T) {
	binary := buildWefty(t)
	gateway := startStubLedger(t, func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "gateway offline", 502) })
	for _, address := range []string{unusedAddress(t), gateway} {
		code, output := runWefty(t, binary, 30*time.Second, "--json", "--l3", address, "inspect", "missing")
		var envelope contract.ErrorResponse
		if err := json.Unmarshal([]byte(output), &envelope); err != nil || code != 13 || envelope.Error.Code != "unavailable" || !envelope.Error.Retryable {
			t.Errorf("exit=%d output=%s decode=%v", code, output, err)
		}
	}
}

func TestReviewMissingAddressUsageFromRealBinary(t *testing.T) {
	binary := buildWefty(t)
	for _, args := range [][]string{
		{"--json", "--l1=", "nodes", "list"},
		{"--json", "--l3=", "inspect", "missing"},
	} {
		code, output := runWefty(t, binary, 30*time.Second, args...)
		var envelope contract.ErrorResponse
		if err := json.Unmarshal([]byte(output), &envelope); err != nil || code != 2 || envelope.Error.Code != contract.ErrorInvalidRequest {
			t.Errorf("exit=%d output=%s decode=%v", code, output, err)
		}
	}
}

func TestReviewJSONFlagValueCollision(t *testing.T) {
	for _, value := range []string{"--json", "--json=nope", "--json=false"} {
		for _, flag := range []string{"--params", "--script", "--argv", "--summary", "--cursor", "--plain-identity"} {
			args := []string{"submit", flag, value, "--json"}
			options, got, err := parseGlobalOptions(args, io.Discard)
			if err != nil || !options.jsonOutput || !reflect.DeepEqual(got, args[:3]) {
				t.Errorf("args=%v got=%v json=%v err=%v", args, got, options.jsonOutput, err)
			}
			if hasJSONFlag(args[:3]) {
				t.Errorf("flag value detected as global flag: %v", args[:3])
			}
		}
	}
	for _, args := range [][]string{{"submit", "--again", "--json"}, {"services", "grant", "computer", "person", "--wait", "--json"}} {
		if !hasJSONFlag(args) {
			t.Errorf("boolean flag swallowed JSON flag: %v", args)
		}
	}
	for _, args := range [][]string{{"submit", "--", "--json"}, {"submit", "--params=--json=nope"}} {
		if hasJSONFlag(args) {
			t.Errorf("literal JSON argument extracted: %v", args)
		}
	}

}

func TestReviewJSONFlagValueFromRealBinary(t *testing.T) {
	binary := buildWefty(t)
	code, output := runWefty(t, binary, 30*time.Second, "--json", "submit", "--workflow-ref=test/v1", "--params", "--json=nope")
	var envelope contract.ErrorResponse
	if err := json.Unmarshal([]byte(output), &envelope); err != nil || code != exitFailure || envelope.Error.Code != contract.ErrorInternal || !strings.Contains(envelope.Error.Message, "params: must be a JSON object") {
		t.Fatalf("exit=%d output=%s decode=%v", code, output, err)
	}
}
