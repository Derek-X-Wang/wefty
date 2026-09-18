package main

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/fabric/plain"
)

// An exit code is a promise to whatever called the binary, and the only way to
// check a promise like that is to call the binary. These tests build `wefty`
// and read the process's own exit status -- because the bug they exist to
// prevent lived entirely between the command's return value and that status,
// and every in-process test passed while the real binary exited 1.

func buildWefty(t *testing.T) string {
	t.Helper()
	binary := filepath.Join(t.TempDir(), "wefty")
	build := exec.Command("go", "build", "-o", binary, ".")
	build.Env = os.Environ()
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build wefty: %v\n%s", err, output)
	}
	return binary
}

// runWefty runs the built binary and returns its exit status and output.
func runWefty(t *testing.T, binary string, budget time.Duration, args ...string) (int, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), budget)
	defer cancel()
	command := exec.CommandContext(ctx, binary, args...)
	command.Env = os.Environ()
	output, err := command.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("`wefty %s` did not finish within %s:\n%s", strings.Join(args, " "), budget, output)
	}
	var exitErr *exec.ExitError
	switch {
	case err == nil:
		return 0, string(output)
	case errors.As(err, &exitErr):
		return exitErr.ExitCode(), string(output)
	default:
		t.Fatalf("run wefty: %v\n%s", err, output)
		return 0, ""
	}
}

// unusedAddress binds a port and releases it, so the address is real and
// refuses connections rather than hanging.
func unusedAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

// TestStatusExitsTwelveFromTheRealBinary is the promise the skill makes to an
// agent. A refused connection, not a black hole: the command has to conclude on
// its own rather than be timed out by the caller.
func TestStatusExitsTwelveFromTheRealBinary(t *testing.T) {
	t.Parallel()

	binary := buildWefty(t)
	nothing := unusedAddress(t)
	code, output := runWefty(t, binary, 30*time.Second,
		"--l1="+nothing, "--l3="+unusedAddress(t), "status")
	if code != exitNotReady {
		t.Fatalf("`wefty status` against nothing exited %d, want %d:\n%s", code, exitNotReady, output)
	}
	if !strings.Contains(output, "not ready:") {
		t.Fatalf("status printed no verdict:\n%s", output)
	}
	// And --json still emits a document a script can read on the way out.
	code, output = runWefty(t, binary, 30*time.Second,
		"--json", "--l1="+nothing, "--l3="+unusedAddress(t), "status")
	if code != exitNotReady {
		t.Fatalf("`wefty --json status` exited %d, want %d:\n%s", code, exitNotReady, output)
	}
	var status clusterStatus
	if err := json.Unmarshal([]byte(firstJSONDocument(output)), &status); err != nil {
		t.Fatalf("status --json is not JSON: %v\n%s", err, output)
	}
	if status.Ready {
		t.Fatalf("status = %#v", status)
	}
}

// TestStatusUsageMistakeExitsTwoFromTheRealBinary keeps the cluster's answer and
// the caller's mistake apart at the process boundary too.
func TestStatusUsageMistakeExitsTwoFromTheRealBinary(t *testing.T) {
	t.Parallel()

	binary := buildWefty(t)
	code, output := runWefty(t, binary, 30*time.Second, "status", "--not-a-flag")
	if code != exitUsage {
		t.Fatalf("`wefty status --not-a-flag` exited %d, want %d:\n%s", code, exitUsage, output)
	}
}

// TestWaitExitCodesFromTheRealBinary is #477's promise, checked the only way it
// can be. A stub ledger answers as L3 would; the binary's own exit status is
// the assertion.
func TestWaitExitCodesFromTheRealBinary(t *testing.T) {
	t.Parallel()

	binary := buildWefty(t)

	var mu sync.Mutex
	status := contract.RunFailed
	ledger := startStubLedger(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		current := status
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(contract.RunRecord{RunID: "run-under-test", Status: current})
	})

	code, output := runWefty(t, binary, 60*time.Second,
		"--l1="+unusedAddress(t), "--l3="+ledger, "wait", "run-under-test", "--timeout", "30s")
	if code != exitRunFailed {
		t.Fatalf("`wefty wait` on a failed run exited %d, want %d:\n%s", code, exitRunFailed, output)
	}
	if !strings.Contains(output, string(contract.RunFailed)) {
		t.Fatalf("wait printed no status:\n%s", output)
	}

	mu.Lock()
	status = contract.RunSucceeded
	mu.Unlock()
	code, output = runWefty(t, binary, 60*time.Second,
		"--l1="+unusedAddress(t), "--l3="+ledger, "wait", "run-under-test", "--timeout", "30s")
	if code != 0 {
		t.Fatalf("`wefty wait` on a succeeded run exited %d, want 0:\n%s", code, output)
	}

	mu.Lock()
	status = contract.RunRunning
	mu.Unlock()
	started := time.Now()
	code, output = runWefty(t, binary, 60*time.Second,
		"--l1="+unusedAddress(t), "--l3="+ledger, "wait", "run-under-test", "--timeout", "900ms")
	if code != exitWaitTimeout {
		t.Fatalf("`wefty wait` on a running run exited %d, want %d:\n%s", code, exitWaitTimeout, output)
	}
	if elapsed := time.Since(started); elapsed > 20*time.Second {
		t.Fatalf("a 900ms wait took %s", elapsed)
	}
}

// startStubLedger serves one handler behind a plain Fabric listener, so the
// binary's own Fabric client can reach it: the client writes an identity
// preamble that an ordinary HTTP server would read as garbage.
func startStubLedger(t *testing.T, handler http.HandlerFunc) string {
	t.Helper()
	network := plain.NewNetwork()
	participant := network.NewFabric(fabric.Identity{NodeID: "stub-ledger"})
	listener, err := participant.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: handler}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close() })
	return listener.Addr().String()
}

// firstJSONDocument trims anything a command wrote around its JSON body.
func firstJSONDocument(output string) string {
	start := strings.Index(output, "{")
	end := strings.LastIndex(output, "}")
	if start < 0 || end < start {
		return output
	}
	return output[start : end+1]
}
