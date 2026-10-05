package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

func TestInstanceKeyCLI(t *testing.T) {
	h := newServiceCLIHarness(t)
	script := filepath.Join(t.TempDir(), "service.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nexit 0\n"), 0600); err != nil {
		t.Fatal(err)
	}
	out := runServiceCLI(t, context.Background(), h.clients, true, "services", "create", "--script", script, "--instance-key", "cli-agent", "--idempotency-key", "cli-first")
	var job map[string]any
	if err := json.Unmarshal(out, &job); err != nil {
		t.Fatal(err)
	}
	spec, ok := job["spec"].(map[string]any)
	if !ok || spec["instance_key"] != "cli-agent" {
		t.Fatalf("CLI key not exposed: %s", out)
	}
	var stdout, stderr bytes.Buffer
	err := execute(context.Background(), h.clients, true, []string{"services", "create", "--script", script, "--instance-key", "cli-agent", "--idempotency-key", "cli-second"}, &stdout, &stderr)
	if err == nil || commandExitCode(err) != exitConflict || !strings.Contains(err.Error(), "instance_key_conflict") {
		t.Fatalf("CLI conflict=%v", err)
	}
	for _, key := range []string{"", " spaced"} {
		err := executeServiceCreate(context.Background(), nil, true, []string{"--script", script, "--instance-key", key}, &stdout, &stderr)
		if err == nil || commandExitCode(err) != exitUsage {
			t.Fatalf("CLI invalid key %q=%v", key, err)
		}
	}
	err = executeServiceCreate(context.Background(), nil, true, []string{"--image", "alpine:latest", "--computer", "--instance-key", "excluded"}, &stdout, &stderr)
	if err == nil || commandExitCode(err) != exitUsage || !strings.Contains(err.Error(), "Computers") {
		t.Fatalf("CLI Computer key=%v", err)
	}
}

func TestInstanceKeyConflictExitFromRealBinary(t *testing.T) {
	binary := buildWefty(t)
	script := filepath.Join(t.TempDir(), "service.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nexit 0\n"), 0600); err != nil {
		t.Fatal(err)
	}
	address := startStubLedger(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/jobs" {
			t.Errorf("request=%s %s", r.Method, r.URL.Path)
		}
		var request map[string]any
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil || request["instance_key"] != "binary-instance" {
			t.Errorf("keyed request=%+v %v", request, err)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(contract.ErrorResponse{Error: contract.APIError{Code: contract.ErrorCode("instance_key_conflict"), Message: "instance is already live", Details: map[string]any{"job_id": "holder-job", "instance_key": "binary-instance"}}})
	})
	code, out := runWefty(t, binary, 30*time.Second, "--json", "--l1="+address, "--l3=", "services", "create", "--script", script, "--instance-key", "binary-instance", "--idempotency-key", "binary-dispatch")
	if code != exitConflict || !strings.Contains(out, "holder-job") || !strings.Contains(out, "instance_key_conflict") {
		t.Fatalf("conflict exit=%d output=%s", code, out)
	}
}
