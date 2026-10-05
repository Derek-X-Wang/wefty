package main

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

func TestServiceOnFailureCreateFromRealBinary(t *testing.T) {
	binary := buildWefty(t)
	script := filepath.Join(t.TempDir(), "service.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nexit 0\n"), 0600); err != nil {
		t.Fatal(err)
	}
	server := startStubLedger(t, func(w http.ResponseWriter, r *http.Request) {
		var spec contract.JobSpec
		if err := json.NewDecoder(r.Body).Decode(&spec); err != nil {
			t.Error(err)
		}
		if spec.Restart != "on-failure" {
			t.Errorf("restart=%q", spec.Restart)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(l1.Job{JobID: "policy-service", Spec: spec})
	})
	code, output := runWefty(t, binary, 30*time.Second, "--json", "--l1="+server, "services", "create", "--script", script, "--restart=on-failure")
	if code != 0 || !strings.Contains(output, "policy-service") {
		t.Fatalf("create exit=%d output=%s", code, output)
	}
}

func TestServiceCreateTypedExitsFromRealBinary(t *testing.T) {
	binary := buildWefty(t)
	script := filepath.Join(t.TempDir(), "service.sh")
	if err := os.WriteFile(script, []byte("exit 0\n"), 0600); err != nil {
		t.Fatal(err)
	}
	refusal := startStubLedger(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": contract.APIError{Code: contract.ErrorIdempotencyConflict, Message: "creation conflicts"}})
	})
	for _, tc := range []struct {
		name string
		args []string
		want int
	}{
		{"invalid policy", []string{"--l1=" + refusal, "services", "create", "--script", script, "--restart=never"}, exitUsage},
		{"server refusal", []string{"--l1", refusal, "services", "create", "--script", script}, exitConflict},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, output := runWefty(t, binary, 30*time.Second, tc.args...)
			if code != tc.want {
				t.Fatalf("exit=%d want=%d output=%s", code, tc.want, output)
			}
		})
	}
}

func TestServicePolicyStopStatusFromRealBinary(t *testing.T) {
	binary := buildWefty(t)
	server := startStubLedger(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"job_id":"policy-service","state":"stopped","status":"stopped","desired_state":"running","holds_slot":false,"spec":{"schema_version":1,"dispatch_key":"policy","kind":"process","class":"service","restart":"on-failure","execution":{"executable":{"path":"/bin/true"},"argv":["true"],"working_directory":"/tmp"}},"policy_stop":{"exit_code":0},"restart_suppressed_reason":"policy stop: on-failure payload exited cleanly; use start or restart"}`))
	})
	code, output := runWefty(t, binary, 30*time.Second, "--l1="+server, "services", "status", "policy-service")
	if code != 0 || !strings.Contains(output, "POLICY STOP") || !strings.Contains(output, `"exit_code":0`) {
		t.Fatalf("status exit=%d output=%s", code, output)
	}
}
