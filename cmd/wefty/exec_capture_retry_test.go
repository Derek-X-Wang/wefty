package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

const captureChangingMessage = "files kept changing while wefty copied them (another agent may be editing this folder); retry when edits pause"

// Intercept real Git at a deterministic capture boundary, including when the
// caller is the shipped binary. All reads, copies and checks still use Git.
func editExecCapture(t *testing.T, repo, kind string, forever bool) func() int {
	t.Helper()
	git, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	shim := t.TempDir()
	counter := filepath.Join(shim, "attempts")
	boundary := 1
	action := fmt.Sprintf("printf 'final-%%s' \"$attempt\" > %s", captureTestShellQuote(filepath.Join(repo, "sub", "tracked")))
	switch kind {
	case "head":
		boundary = 2
		action = fmt.Sprintf("env -u GIT_DIR -u GIT_INDEX_FILE %s -C %s -c user.name=test -c user.email=test@example.com -c maintenance.auto=false -c gc.auto=0 commit --allow-empty -qm changed", captureTestShellQuote(git), captureTestShellQuote(repo))
	case "status":
		boundary = 2
		action = fmt.Sprintf("env -u GIT_DIR -u GIT_INDEX_FILE %s -C %s add sub/tracked", captureTestShellQuote(git), captureTestShellQuote(repo))
	}
	condition := "[ \"$attempt\" -eq 1 ]"
	if forever {
		condition = "true"
	}
	script := fmt.Sprintf(`#!/bin/sh
counter=%s
if [ "$5" = clone ]; then
 attempt=0
 [ ! -f "$counter" ] || read -r attempt < "$counter"
 attempt=$((attempt + 1))
 printf '%%s\n' "$attempt" > "$counter"
fi
if [ "$5" = write-tree ]; then
 %s "$@" || exit
 attempt=0
 read -r attempt < "$counter"
 count=0
 [ ! -f "$GIT_DIR/tree-writes" ] || read -r count < "$GIT_DIR/tree-writes"
 count=$((count + 1))
 printf '%%s\n' "$count" > "$GIT_DIR/tree-writes"
 if [ "$count" -eq %d ] && %s; then
  %s || exit
 fi
 exit 0
fi
exec %s "$@"
`, captureTestShellQuote(counter), captureTestShellQuote(git), boundary, condition, action, captureTestShellQuote(git))
	path := filepath.Join(shim, "git")
	writeExecFile(t, path, script)
	if err := os.Chmod(path, 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", shim+string(os.PathListSeparator)+os.Getenv("PATH"))
	return func() int {
		t.Helper()
		raw, err := os.ReadFile(counter)
		if err != nil {
			t.Fatal(err)
		}
		n, err := strconv.Atoi(strings.TrimSpace(string(raw)))
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
}

func TestExecCaptureRetriesFreshCheckoutAndSubmitsFinalState(t *testing.T) {
	for _, kind := range []string{"image", "head", "status"} {
		t.Run(kind, func(t *testing.T) {
			stack := newExecStack(t, execTestIdleLimit)
			repo := execTestRepo(t)
			writeExecFile(t, filepath.Join(repo, "sub", "tracked"), "dirty")
			t.Chdir(repo)
			attempts := editExecCapture(t, repo, kind, false)
			var waits []time.Duration
			stack.clients.wait = func(ctx context.Context, duration time.Duration) error {
				waits = append(waits, duration)
				return ctx.Err()
			}
			var out, stderr bytes.Buffer
			holdExecNode(t, stack)
			err := execute(t.Context(), stack.clients, false, []string{"exec", "--detach", "--", "true"}, &out, &stderr)
			if err != nil || len(stack.requests) != 1 || attempts() != 2 || !reflect.DeepEqual(waits, []time.Duration{time.Second}) {
				t.Fatalf("retry did not submit final capture: err=%v attempts=%d submissions=%d stderr=%s", err, attempts(), len(stack.requests), &stderr)
			}
			var params execParams
			if err := json.Unmarshal(stack.requests[0].Params, &params); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = cleanupExecStorage(params.Storage) })
			got, err := execGit(t.Context(), repo, filepath.Join(params.Storage, "snapshot.git"), nil, "show", params.Tree+":sub/tracked")
			want, readErr := os.ReadFile(filepath.Join(repo, "sub", "tracked"))
			head := strings.TrimSpace(execTestGit(t, repo, "rev-parse", "HEAD"))
			if err != nil || readErr != nil || got != string(want) || params.Base != head {
				t.Fatalf("mixed or stale capture: bytes=%q want=%q base=%s want=%s err=%v read=%v", got, want, params.Base, head, err, readErr)
			}
			captures, err := filepath.Glob(filepath.Join(os.Getenv("WEFTY_EXEC_STORAGE_ROOT"), "wefty-exec-*"))
			if err != nil || len(captures) != 1 || captures[0] != params.Storage {
				t.Fatalf("failed capture retained: %v %v", captures, err)
			}
		})
	}
}

func TestExecCaptureKeepsChangingFromRealBinary(t *testing.T) {
	binary := buildWefty(t)
	for _, jsonOutput := range []bool{false, true} {
		t.Run(fmt.Sprint(jsonOutput), func(t *testing.T) {
			stack := newExecStack(t, execRealBinaryIdleLimit)
			repo := execTestRepo(t)
			t.Chdir(repo)
			attempts := editExecCapture(t, repo, "image", true)
			args := []string{"--l1=" + stack.l1Address, "--l3=" + stack.l3Address, "--node-config=" + stack.clients.nodeConfigPath, "exec"}
			if jsonOutput {
				args = append(args, "--json")
			}
			args = append(args, "--", "true")
			code, stdout, stderr := runPolishBinary(t, binary, args...)
			if code != exitConflict || len(stdout) != 0 || attempts() != 3 {
				t.Errorf("exit=%d attempts=%d stdout=%s stderr=%s", code, attempts(), stdout, stderr)
			}
			if jsonOutput {
				var envelope contract.ErrorResponse
				if err := json.Unmarshal(stderr, &envelope); err != nil || envelope.Error.Code != contract.ErrorConflict || envelope.Error.Message != captureChangingMessage || !envelope.Error.Retryable {
					t.Errorf("JSON=%s decode=%v", stderr, err)
				}
			} else if string(stderr) != "wefty: "+captureChangingMessage+"\n" {
				t.Errorf("plain=%q", stderr)
			}
			page, err := stack.clients.listRuns(t.Context(), "", 10)
			if err != nil || len(page.Runs) != 0 {
				t.Fatalf("changing capture submitted: %+v %v", page, err)
			}
			captures, err := filepath.Glob(filepath.Join(os.Getenv("WEFTY_EXEC_STORAGE_ROOT"), "wefty-exec-*"))
			if err != nil || len(captures) != 0 {
				t.Fatalf("failed captures retained: %v %v", captures, err)
			}
		})
	}
}

func captureTestShellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}

func TestExecCaptureRetryCancellation(t *testing.T) {
	stack := newExecStack(t, execTestIdleLimit)
	repo := execTestRepo(t)
	t.Chdir(repo)
	attempts := editExecCapture(t, repo, "image", true)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	waits := 0
	stack.clients.wait = func(ctx context.Context, duration time.Duration) error {
		waits++
		if duration != time.Second {
			t.Fatalf("first wait=%s", duration)
		}
		cancel()
		return waitForContext(ctx, duration)
	}
	err := execute(ctx, stack.clients, false, []string{"exec", "--", "true"}, io.Discard, io.Discard)
	if !errors.Is(err, context.Canceled) || commandExitCode(err) != exitFailure || attempts() != 1 || waits != 1 || len(stack.requests) != 0 {
		t.Fatalf("canceled wait retried or submitted: err=%v attempts=%d waits=%d requests=%d", err, attempts(), waits, len(stack.requests))
	}
}

func TestExecCaptureRetryWaitsAndOtherFailures(t *testing.T) {
	for _, kind := range []string{"image", "head", "failure"} {
		t.Run(kind, func(t *testing.T) {
			stack := newExecStack(t, execTestIdleLimit)
			repo := execTestRepo(t)
			t.Chdir(repo)
			attempts := editExecCapture(t, repo, kind, true)
			if kind == "failure" {
				writeExecFile(t, filepath.Join(repo, ".gitattributes"), "sub/tracked filter=lfs\n")
			}
			var waits []time.Duration
			stack.clients.wait = func(ctx context.Context, duration time.Duration) error {
				waits = append(waits, duration)
				return ctx.Err()
			}
			err := execute(t.Context(), stack.clients, false, []string{"exec", "--", "true"}, io.Discard, io.Discard)
			if kind == "failure" {
				if err == nil || !strings.Contains(err.Error(), "Git LFS") || commandExitCode(err) != exitUsage || len(waits) != 0 {
					t.Fatalf("unrelated failure retried: err=%v waits=%v", err, waits)
				}
			} else if commandExitCode(err) != exitConflict || attempts() != 3 || !reflect.DeepEqual(waits, []time.Duration{time.Second, 2 * time.Second}) {
				t.Fatalf("retry schedule: err=%v attempts=%d waits=%v", err, attempts(), waits)
			}
			if len(stack.requests) != 0 {
				t.Fatal("failed capture submitted")
			}
		})
	}
}
