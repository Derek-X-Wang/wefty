package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
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

// The delete helper leaves the test's process group, so nothing reaps a
// blocked hook. A test binary that dies before its cleanup (a go test
// timeout runs none) must not leave the hook waiting forever (#1042).
func execReleaseWait(release string) string {
	return fmt.Sprintf("while [ ! -f %q ] && kill -0 %d 2>/dev/null; do sleep 0.1; done", release, os.Getpid())
}

// Block the real removal at the shell boundary. Absolute command functions
// work in both macOS Bash 3.2 and Linux Bash; production still uses /bin/rm.
func blockExecRemoval(t *testing.T) (observed, release string) {
	t.Helper()
	dir := t.TempDir()
	observed, release = filepath.Join(dir, "observed"), filepath.Join(dir, "release")
	env := filepath.Join(dir, "bash-env")
	writeExecFile(t, env, fmt.Sprintf(`function /bin/rm() {
  if [ "$1" != -rf ]; then command /bin/rm "$@"; return; fi
  printf '%%s\n' "$*" > %[1]q.$$ && command mv %[1]q.$$ %[1]q
  %[2]s
  command /bin/rm "$@"
}
`, observed, execReleaseWait(release)))
	t.Setenv("BASH_ENV", env)
	t.Cleanup(func() { writeExecFile(t, release, "release") })
	return observed, release
}

func waitExecPath(t *testing.T, path string, present bool) {
	t.Helper()
	// Waits for a run to start too; leave slow shared CI runners room.
	ctx, stop := context.WithTimeout(t.Context(), 20*time.Second)
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

// execTestStorageRoot gives a test its own exec storage root. Retirement
// returns before its detached delete step ends, so wait for every delete step
// in the root before TempDir removes it (#1220). Cleanups run last in, first
// out: this wait follows the test's own cleanups.
func execTestStorageRoot(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	t.Setenv("WEFTY_EXEC_STORAGE_ROOT", root)
	t.Cleanup(func() { waitExecDeleteSteps(t, root) })
	return root
}

// waitExecDeleteSteps waits until root holds no renamed capture and no delete
// marker. A symlink with a trash name is never deleted, so it is no sign of a
// running step. The test context has ended by cleanup time, so poll the clock.
func waitExecDeleteSteps(t *testing.T, root string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		var busy []string
		entries, _ := os.ReadDir(root)
		for _, entry := range entries {
			name := entry.Name()
			if entry.Type()&os.ModeSymlink == 0 && (strings.HasPrefix(name, ".wefty-trash-") || strings.HasPrefix(name, ".wefty-delete-")) {
				busy = append(busy, name)
			}
		}
		if len(busy) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Errorf("exec delete steps still own %s: %v", root, busy)
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// A missing capture path proves retirement only. Check the root too, so a
// leaked temp file under renamed trash cannot masquerade as deletion.
func waitExecStorageEmpty(t *testing.T, root string) {
	t.Helper()
	ctx, stop := context.WithTimeout(t.Context(), 10*time.Second)
	defer stop()
	for {
		entries, err := os.ReadDir(root)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) == 0 {
			return
		}
		if err := waitForContext(ctx, 20*time.Millisecond); err != nil {
			t.Fatalf("storage root still contains retired data: %v: %v", entries, err)
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
  if [ "$1" != -rf ]; then command /bin/rm "$@"; return; fi
  printf '%%s\n' "$$" >> %q
  %s
  command /bin/rm "$@"
}
`, observed, execReleaseWait(release)))
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
	// The hook creates the file before it appends its PID (#1417).
	for deadline := time.Now().Add(20 * time.Second); len(deleters()) == 0; {
		if time.Now().After(deadline) {
			t.Fatal("first deleter did not record its PID")
		}
		if err := waitForContext(t.Context(), 20*time.Millisecond); err != nil {
			t.Fatal(err)
		}
	}
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
	writeExecFile(t, filepath.Join(storage, "tmp", "run-file"), "temp data")
	linkDir, err := os.MkdirTemp("/tmp", "wefty-run.")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(linkDir); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(storage, "tmp"), linkDir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Remove(linkDir) })
	writeExecFile(t, filepath.Join(storage, "tmp-link"), linkDir+"\n")
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
	waitExecPath(t, linkDir, false)
	waitExecPath(t, filepath.Join(trash, "tmp", "run-file"), true)
	writeExecFile(t, release, "release")
	waitExecPath(t, trash, false)
}

func TestExecMissingCaptureWrapperReason(t *testing.T) {
	execTestStorageRoot(t)
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

func TestExecRetirementTempLinkSafety(t *testing.T) {
	for _, kind := range []string{"owned", "outside-prefix", "directory", "wrong-target", "unsafe-suffix"} {
		t.Run(kind, func(t *testing.T) {
			storage := filepath.Join(t.TempDir(), "wefty-exec-owned")
			target := filepath.Join(storage, "tmp")
			writeExecFile(t, filepath.Join(target, "run-file"), "run data")
			link, err := os.MkdirTemp("/tmp", "wefty-run.")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.RemoveAll(link) })
			if kind == "directory" {
				writeExecFile(t, filepath.Join(link, "kept"), "owner data")
			} else {
				if err := os.Remove(link); err != nil {
					t.Fatal(err)
				}
				if kind == "outside-prefix" {
					link = filepath.Join(t.TempDir(), "owner-link")
				}
				if kind == "unsafe-suffix" {
					link += "!"
				}
				if kind == "wrong-target" {
					target = t.TempDir()
					writeExecFile(t, filepath.Join(target, "kept"), "owner data")
				}
				if err := os.Symlink(target, link); err != nil {
					t.Fatal(err)
				}
			}
			writeExecFile(t, filepath.Join(storage, "tmp-link"), link+"\n")
			if err := cleanupExecStorage(storage); err != nil {
				t.Fatal(err)
			}
			waitExecPath(t, storage, false)
			waitExecPath(t, link, kind != "owned")
			if kind == "directory" {
				waitExecPath(t, filepath.Join(link, "kept"), true)
			}
			if kind == "wrong-target" {
				waitExecPath(t, filepath.Join(target, "kept"), true)
			}
		})
	}
}

func TestExecTempSetupFailureIsInfrastructure(t *testing.T) {
	for _, step := range []string{"folder", "link"} {
		t.Run(step, func(t *testing.T) {
			execTestStorageRoot(t)
			capture, err := captureExec(t.Context(), execTestRepo(t))
			if err != nil {
				t.Fatal(err)
			}
			handoff := t.TempDir()
			probe := filepath.Join(t.TempDir(), "bash-env")
			shim := `mkdir() { case "${*: -1}" in */tmp) return 1 ;; esac; command mkdir "$@"; }`
			if step == "link" {
				shim = `ln() { return 1; }`
			}
			writeExecFile(t, probe, shim+"\n")
			cmd := exec.CommandContext(t.Context(), "/bin/bash", "-c", buildExecWrapper(capture, []string{"true"}))
			cmd.Env = append(os.Environ(), "BASH_ENV="+probe, "WEFTY_HANDOFF_DIR="+handoff)
			output, err := cmd.CombinedOutput()
			want := "could not create the run's temp folder"
			if err == nil || !strings.Contains(string(output), "wefty: "+want) {
				t.Fatalf("temp setup failure: %v %q", err, output)
			}
			raw, err := os.ReadFile(filepath.Join(handoff, "result.json"))
			var result map[string]any
			if err != nil || json.Unmarshal(raw, &result) != nil || result["infrastructure_error"] != want || result["exit_code"] != nil {
				t.Fatalf("temp setup infrastructure result: %s %v", raw, err)
			}
			waitExecPath(t, capture.Storage, false)
		})
	}
}

func TestExecTempLinkRemovalFailurePreservesVerdict(t *testing.T) {
	for _, exitCode := range []int{0, 1} {
		t.Run(fmt.Sprint(exitCode), func(t *testing.T) {
			root := t.TempDir()
			t.Setenv("WEFTY_EXEC_STORAGE_ROOT", root)
			capture, err := captureExec(t.Context(), execTestRepo(t))
			if err != nil {
				t.Fatal(err)
			}
			probe := filepath.Join(t.TempDir(), "bash-env")
			// Fail only the short link itself. Inside a wefty run TMPDIR is
			// such a link too, so the storage root lies beneath one and the
			// delete step's rm must still work (#1220).
			writeExecFile(t, probe, `function /bin/rm() {
  case "${*: -1}" in /tmp/wefty-run.*/*) ;; /tmp/wefty-run.*) return 1 ;; esac
  command /bin/rm "$@"
}
`)
			cmd := exec.CommandContext(t.Context(), "/bin/bash", "-c", buildExecWrapper(capture, []string{"/bin/sh", "-c", `printf '%s' "$TMPDIR"; exit "$1"`, "sh", fmt.Sprint(exitCode)}))
			cmd.Env = append(os.Environ(), "BASH_ENV="+probe)
			var stdout, stderr bytes.Buffer
			cmd.Stdout, cmd.Stderr = &stdout, &stderr
			err = cmd.Run()
			got := 0
			if err != nil {
				var status *exec.ExitError
				if !errors.As(err, &status) {
					t.Fatal(err)
				}
				got = status.ExitCode()
			}
			link := stdout.String()
			t.Cleanup(func() { _ = os.Remove(link) })
			if got != exitCode || !strings.Contains(stderr.String(), "warning: could not remove the temp link of "+capture.Storage) || strings.Contains(stderr.String(), "snapshot cleanup failed") {
				t.Fatalf("unlink failure changed result: exit=%d want=%d stderr=%s", got, exitCode, stderr.String())
			}
			waitExecPath(t, capture.Storage, false)
			waitExecPath(t, link, true)
			// The delete helper outlives the rename and still owns its trash
			// and lock folders in the root; TempDir cleanup must not race it (#1197).
			waitExecStorageEmpty(t, root)
		})
	}
}

func TestExecTempLinkRetriesCollision(t *testing.T) {
	execTestStorageRoot(t)
	capture, err := captureExec(t.Context(), execTestRepo(t))
	if err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(t.TempDir(), "collision")
	probe := filepath.Join(t.TempDir(), "bash-env")
	writeExecFile(t, probe, fmt.Sprintf(`ln() {
  if [ ! -f %q ]; then
    candidate="${*: -1}"
    printf 'owner data' > "$candidate"
    printf '%%s' "$candidate" > %q
    return 1
  fi
  command ln "$@"
}
`, marker, marker))
	cmd := exec.CommandContext(t.Context(), "/bin/bash", "-c", buildExecWrapper(capture, []string{"/bin/sh", "-c", `printf '%s' "$TMPDIR"`}))
	cmd.Env = append(os.Environ(), "BASH_ENV="+probe)
	output, err := cmd.Output()
	if err != nil {
		t.Fatalf("collision retry: %v", err)
	}
	raw, err := os.ReadFile(marker)
	if err != nil {
		t.Fatal(err)
	}
	collision := string(raw)
	t.Cleanup(func() { _ = os.Remove(collision) })
	kept, err := os.ReadFile(collision)
	if err != nil || string(kept) != "owner data" || string(output) == collision {
		t.Fatalf("collision was overwritten or reused: kept=%q err=%v used=%q collision=%q", kept, err, output, collision)
	}
	waitExecPath(t, string(output), false)
}
