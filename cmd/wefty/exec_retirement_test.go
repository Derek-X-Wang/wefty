package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l3"
)

// Block the real removal at the shell boundary. Absolute command functions
// work in both macOS Bash 3.2 and Linux Bash; production still uses /bin/rm.
func blockExecRemoval(t *testing.T) (observed, release string) {
	t.Helper()
	dir := t.TempDir()
	observed, release = filepath.Join(dir, "observed"), filepath.Join(dir, "release")
	env := filepath.Join(dir, "bash-env")
	writeExecFile(t, env, fmt.Sprintf(`function /bin/rm() {
  printf '%%s\n' "$*" > %q
  while [ ! -f %q ]; do sleep 0.1; done
  command /bin/rm "$@"
}
`, observed, release))
	t.Setenv("BASH_ENV", env)
	t.Cleanup(func() { writeExecFile(t, release, "release") })
	return observed, release
}

func waitExecPath(t *testing.T, path string, present bool) {
	t.Helper()
	ctx, stop := context.WithTimeout(t.Context(), 10*time.Second)
	defer stop()
	for {
		_, err := os.Lstat(path)
		if (present && err == nil) || (!present && os.IsNotExist(err)) {
			return
		}
		if err := waitForContext(ctx, 20*time.Millisecond); err != nil {
			t.Fatalf("path %s present=%v: %v", path, present, err)
		}
	}
}

func TestExecSlowRemovalSurvivesAttemptEnd(t *testing.T) {
	observed, release := blockExecRemoval(t)
	stack := newExecStack(t, execTestIdleLimit)
	t.Chdir(execTestRepo(t))
	ctx, stop := context.WithTimeout(t.Context(), 8*time.Second)
	defer stop()
	var stderr bytes.Buffer
	started := time.Now()
	err := execute(ctx, stack.clients, false, []string{"exec", "--", "/bin/sh", "-c", `test "$1" = __WEFTY_EXEC_CLEANUP__`, "sh", "__WEFTY_EXEC_CLEANUP__"}, io.Discard, &stderr)
	if err != nil {
		t.Fatalf("removal kept run open: %v (%s), %s", err, time.Since(started), stderr.String())
	}
	waitExecPath(t, observed, true)
	raw, err := os.ReadFile(observed)
	if err != nil {
		t.Fatal(err)
	}
	var params execParams
	if err := json.Unmarshal(stack.requests[0].Params, &params); err != nil {
		t.Fatal(err)
	}
	trash := strings.TrimPrefix(strings.TrimSpace(string(raw)), "-rf -- ")
	if !strings.HasPrefix(filepath.Base(trash), ".wefty-trash-") || filepath.Dir(trash) != filepath.Dir(params.Storage) {
		t.Fatalf("removal was not retired in the same root: %q", raw)
	}
	waitExecPath(t, params.Storage, false)
	waitExecPath(t, trash, true)
	// Outlast the old total removal budget, after real L1/agent completion
	// has reaped the attempt's process group. Then let the same helper finish.
	if err := waitForContext(t.Context(), 11*time.Second); err != nil {
		t.Fatal(err)
	}
	writeExecFile(t, release, "release")
	waitExecPath(t, trash, false)
}

func TestExecSweepTrashDoesNotWaitForDeletion(t *testing.T) {
	observed, release := blockExecRemoval(t)
	stack := newExecStack(t, execTestIdleLimit)
	root := os.Getenv("WEFTY_EXEC_STORAGE_ROOT")
	trash := filepath.Join(root, ".wefty-trash-large")
	for i := range 1024 {
		writeExecFile(t, filepath.Join(trash, "work", fmt.Sprint(i)), "generated data")
	}
	// Young trash and even an unverifiable marker need no acceptance lookup.
	writeExecFile(t, filepath.Join(trash, "run-id"), "run-unverifiable")
	outside := t.TempDir()
	writeExecFile(t, filepath.Join(outside, "kept"), "owner data")
	if err := os.Symlink(outside, filepath.Join(root, ".wefty-trash-link")); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	var stderr bytes.Buffer
	if err := sweepExecStorage(t.Context(), stack.clients, &stderr); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed > execSweepBudget || stderr.Len() != 0 {
		t.Fatalf("trash consumed foreground sweep budget: %s %s", elapsed, stderr.String())
	}
	waitExecPath(t, observed, true)
	waitExecPath(t, trash, true)
	writeExecFile(t, release, "release")
	waitExecPath(t, trash, false)
	waitExecPath(t, filepath.Join(outside, "kept"), true)
}

func TestExecSweepTrashHasOneDeleterAndRecoversKilledOwner(t *testing.T) {
	stack := newExecStack(t, execTestIdleLimit)
	dir := t.TempDir()
	observed, release := filepath.Join(dir, "observed"), filepath.Join(dir, "release")
	env := filepath.Join(dir, "bash-env")
	writeExecFile(t, env, fmt.Sprintf(`function /bin/rm() {
  printf '%%s\n' "$$" >> %q
  while [ ! -f %q ]; do sleep 0.1; done
  command /bin/rm "$@"
}
`, observed, release))
	t.Setenv("BASH_ENV", env)
	t.Cleanup(func() { writeExecFile(t, release, "release") })
	trash := filepath.Join(os.Getenv("WEFTY_EXEC_STORAGE_ROOT"), ".wefty-trash-in-flight")
	writeExecFile(t, filepath.Join(trash, "work", "leftover"), "generated data")
	sweep := func() {
		t.Helper()
		if err := sweepExecStorage(t.Context(), stack.clients, io.Discard); err != nil {
			t.Fatal(err)
		}
	}
	deleters := func() []string {
		t.Helper()
		raw, err := os.ReadFile(observed)
		if err != nil {
			t.Fatal(err)
		}
		return strings.Fields(string(raw))
	}
	sweep()
	waitExecPath(t, observed, true)
	sweep()
	// Allow the second asynchronous sweep to reach deletion. This is an
	// observation interval, not a sub-second performance requirement.
	if err := waitForContext(t.Context(), 2*time.Second); err != nil {
		t.Fatal(err)
	}
	owners := deleters()
	if len(owners) != 1 {
		t.Fatalf("second sweep started another deleter: %v", owners)
	}
	pid, err := strconv.Atoi(owners[0])
	if err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(filepath.Dir(trash), ".wefty-delete-"+filepath.Base(trash))
	waitExecPath(t, filepath.Join(marker, owners[0]), true)
	if err := syscall.Kill(-pid, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithTimeout(t.Context(), 10*time.Second)
	defer stop()
	for syscall.Kill(-pid, 0) == nil {
		if err := waitForContext(ctx, 20*time.Millisecond); err != nil {
			t.Fatalf("killed deleter is still alive: %v", err)
		}
	}
	waitExecPath(t, trash, true)
	writeExecFile(t, release, "release")
	sweep()
	waitExecPath(t, trash, false)
	waitExecPath(t, marker, false)
	if owners := deleters(); len(owners) != 2 {
		t.Fatalf("stale owner was not replaced once: %v", owners)
	}
}

func TestExecSweepOldCaptureRetiresBeforeDeletion(t *testing.T) {
	observed, release := blockExecRemoval(t)
	stack := newExecStack(t, execTestIdleLimit)
	storage := filepath.Join(os.Getenv("WEFTY_EXEC_STORAGE_ROOT"), "wefty-exec-old-leftover")
	writeExecFile(t, filepath.Join(storage, "work", "leftover"), "old data")
	ageExecCapture(t, storage)
	if err := sweepExecStorage(t.Context(), stack.clients, io.Discard); err != nil {
		t.Fatal(err)
	}
	waitExecPath(t, observed, true)
	raw, err := os.ReadFile(observed)
	if err != nil {
		t.Fatal(err)
	}
	trash := strings.TrimPrefix(strings.TrimSpace(string(raw)), "-rf -- ")
	if !strings.HasPrefix(filepath.Base(trash), ".wefty-trash-") {
		t.Fatalf("old capture was deleted without retirement: %q", raw)
	}
	waitExecPath(t, storage, false)
	writeExecFile(t, release, "release")
	waitExecPath(t, trash, false)
}

func TestExecMissingCaptureWrapperReason(t *testing.T) {
	t.Setenv("WEFTY_EXEC_STORAGE_ROOT", t.TempDir())
	capture, err := captureExec(t.Context(), execTestRepo(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(capture.Storage); err != nil {
		t.Fatal(err)
	}
	handoff := t.TempDir()
	ctx, stop := context.WithTimeout(t.Context(), 10*time.Second)
	defer stop()
	cmd := exec.CommandContext(ctx, "/bin/bash", "-c", buildExecWrapper(capture, []string{"true"}))
	cmd.Env = append(os.Environ(), "WEFTY_RUN_ID=run-gone", "WEFTY_HANDOFF_DIR="+handoff)
	output, err := cmd.CombinedOutput()
	want := "the exec snapshot was deleted when the run ended; run `wefty exec` again"
	if err == nil || string(output) != "wefty: "+want+"\n" {
		t.Fatalf("missing capture: %v %q", err, output)
	}
	raw, err := os.ReadFile(filepath.Join(handoff, "result.json"))
	var result map[string]any
	if err != nil || json.Unmarshal(raw, &result) != nil || result["infrastructure_error"] != want || result["exit_code"] != nil {
		t.Fatalf("missing capture infrastructure result: %s %v", raw, err)
	}
}

func TestExecRerunGoneCaptureFromRealBinary(t *testing.T) {
	binary := buildWefty(t)
	stack := newExecStack(t, execTestIdleLimit)
	t.Chdir(execTestRepo(t))
	if err := execute(t.Context(), stack.clients, false, []string{"exec", "--", "true"}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	page, err := stack.clients.listRuns(t.Context(), "", 10)
	if err != nil || len(page.Runs) != 1 {
		t.Fatalf("source runs: %+v %v", page, err)
	}
	// Existing ledger entries predate the explicit exec_version discriminator.
	// Submit the old immutable schema too; it must receive the same refusal.
	legacyRequest := stack.requests[0]
	var legacyParams map[string]any
	if err := json.Unmarshal(legacyRequest.Params, &legacyParams); err != nil {
		t.Fatal(err)
	}
	delete(legacyParams, "exec_version")
	legacyRequest.Params, err = json.Marshal(legacyParams)
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := stack.clients.submitRun(t.Context(), legacyRequest, "legacy-exec")
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{page.Runs[0].RunID, legacy.RunID} {
		for _, extra := range [][]string{nil, {"--again"}, {"--idempotency-key=explicit"}, {"--json"}} {
			args := []string{"--l1=" + stack.l1Address, "--l3=" + stack.l3Address, "rerun", id}
			args = append(args, extra...)
			code, output := runWefty(t, binary, 30*time.Second, args...)
			want := "wefty: the exec snapshot was deleted when the run ended; run `wefty exec` again\n"
			if len(extra) != 0 && extra[0] == "--json" {
				var response contract.ErrorResponse
				if json.Unmarshal([]byte(output), &response) != nil || response.Error.Code != contract.ErrorInvalidRequest || response.Error.Message != strings.TrimSuffix(strings.TrimPrefix(want, "wefty: "), "\n") {
					t.Errorf("JSON refusal: %s", output)
				}
			} else if output != want {
				t.Errorf("rerun %s %v: output=%q", id, extra, output)
			}
			if code != 2 {
				t.Errorf("rerun %s %v: exit=%d output=%q", id, extra, code, output)
			}
		}
	}
	page, err = stack.clients.listRuns(t.Context(), "", 10)
	if err != nil || len(page.Runs) != 2 {
		t.Fatalf("refusal created a run: %+v %v", page, err)
	}
	// A generic inline run with a similarly named parameter remains rerunnable.
	ordinaryScript := "#!/bin/sh\nexit 0\n"
	digest := sha256.Sum256([]byte(ordinaryScript))
	ordinary, err := stack.clients.submitRun(t.Context(), l3.CreateRunRequest{
		Params:       json.RawMessage(`{"snapshot_storage":"/missing/wefty-exec-generic"}`),
		Tags:         []string{contract.StableNodeTagPrefix + "node-cli"},
		InlineScript: &l3.InlineScriptInput{Content: ordinaryScript, SHA256: hex.EncodeToString(digest[:]), Interpreter: []string{"/bin/sh"}},
	}, "ordinary-inline")
	if err != nil {
		t.Fatal(err)
	}
	code, output := runWefty(t, binary, 30*time.Second, "--l1="+stack.l1Address, "--l3="+stack.l3Address, "rerun", ordinary.RunID)
	if code != 0 {
		t.Fatalf("ordinary run was mistaken for exec: exit=%d %s", code, output)
	}
}
