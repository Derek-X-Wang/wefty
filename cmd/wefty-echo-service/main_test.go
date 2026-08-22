package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
)

func TestRunOnceWritesHandoffCallsBridgeAndSplitsMarkers(t *testing.T) {
	const token = "one-shot-secret-token"
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		requests++
		if request.Method != http.MethodGet || request.URL.Path != "/v1/runs/run-once" {
			t.Fatalf("bridge request = %s %s", request.Method, request.URL.Path)
		}
		if request.Header.Get("Authorization") != "Bearer "+token {
			t.Fatalf("bridge authorization = %q", request.Header.Get("Authorization"))
		}
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{"run_id":"run-once"}`))
	}))
	defer server.Close()

	handoffDirectory := t.TempDir()
	t.Setenv(contract.EnvHandoffDir, handoffDirectory)
	t.Setenv(contract.EnvL3Endpoint, server.URL)
	t.Setenv(contract.EnvRunToken, token)
	t.Setenv(contract.EnvRunID, "run-once")
	var stdout, stderr bytes.Buffer
	if err := runOnce(server.Client(), &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if requests != 1 {
		t.Fatalf("bridge requests = %d, want 1", requests)
	}
	marker, err := os.ReadFile(filepath.Join(handoffDirectory, "wefty-echo-once.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(marker) != "wefty echo one-shot handoff\n" {
		t.Fatalf("handoff marker = %q", marker)
	}
	if stdout.String() != "wefty-echo-once-stdout\n" || stderr.String() != "wefty-echo-once-stderr\n" {
		t.Fatalf("once markers stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
	if bytes.Contains(stdout.Bytes(), []byte(token)) || bytes.Contains(stderr.Bytes(), []byte(token)) {
		t.Fatal("one-shot mode logged its run token")
	}
}
