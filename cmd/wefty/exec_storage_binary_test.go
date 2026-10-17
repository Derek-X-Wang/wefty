package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func denyExecStorageParent(t *testing.T, parent string) {
	t.Helper()
	if err := os.MkdirAll(parent, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(parent, 0500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(parent, 0700) })
	if err := os.Mkdir(filepath.Join(parent, "permission-probe"), 0700); !errors.Is(err, os.ErrPermission) {
		t.Fatalf("fixture must refuse writes: %v", err)
	}
}

func wantExecStorageRefusal(root string) string {
	return fmt.Sprintf("wefty: cannot write exec snapshot folder %q; allow writes to this folder, or set WEFTY_EXEC_STORAGE_ROOT to a writable folder outside the checkout that the node can read (see the wefty skill: skills/wefty/SKILL.md)\n", root)
}

func TestExecUnwritableStorageFromRealBinary(t *testing.T) {
	binary := buildWefty(t)
	for _, kind := range []string{"default", "both-unwritable", "explicit"} {
		t.Run(kind, func(t *testing.T) {
			stack := newExecStack(t, execRealBinaryIdleLimit)
			t.Chdir(execTestRepo(t))
			t.Setenv("WEFTY_EXEC_STORAGE_ROOT", "")
			t.Setenv("XDG_STATE_HOME", "")
			temp := t.TempDir()
			t.Setenv("TMPDIR", temp)
			parent := filepath.Join(os.Getenv("HOME"), ".local", "state", "wefty")
			denyExecStorageParent(t, parent)
			root := filepath.Join(parent, "exec")
			if kind == "default" {
				if err := os.Mkdir(filepath.Join(temp, "wefty-exec"), 0755); err != nil {
					t.Fatal(err)
				}
			}
			if kind == "both-unwritable" {
				denyExecStorageParent(t, temp)
			}
			if kind == "explicit" {
				t.Setenv("WEFTY_EXEC_STORAGE_ROOT", root)
			}
			code, output := runWefty(t, binary, 30*time.Second,
				"--l1="+stack.l1Address, "--l3="+stack.l3Address,
				"--node-config="+stack.clients.nodeConfigPath, "exec", "--", "/bin/sh", "-c", "cat sub/tracked; pwd")
			page, err := stack.clients.listRuns(t.Context(), "", 10)
			if err != nil {
				t.Fatal(err)
			}
			if kind == "default" && runtime.GOOS == "darwin" {
				if code != 0 || !strings.Contains(output, "base") || !strings.Contains(output, "wefty-exec-") {
					t.Fatalf("fallback capture did not run: exit=%d output=%q", code, output)
				}
				if len(page.Runs) != 1 {
					t.Fatalf("fallback submitted %d runs", len(page.Runs))
				}
				record, err := stack.clients.getRun(t.Context(), page.Runs[0].RunID)
				if err != nil {
					t.Fatal(err)
				}
				var params execParams
				if err := json.Unmarshal(record.Params, &params); err != nil {
					t.Fatal(err)
				}
				if filepath.Dir(params.Storage) != filepath.Join(temp, "wefty-exec") {
					t.Fatalf("run used another capture: %s", params.Storage)
				}
				info, err := os.Stat(filepath.Join(temp, "wefty-exec"))
				if err != nil || info.Mode().Perm() != 0700 {
					t.Fatalf("fallback permissions: %v %v", info, err)
				}
			} else {
				want := wantExecStorageRefusal(root)
				if code != 1 || output != want {
					t.Fatalf("exec storage refusal: exit=%d, want 1\noutput=%q\nwant=%q", code, output, want)
				}
				if len(page.Runs) != 0 {
					t.Fatalf("unwritable storage submitted %d runs", len(page.Runs))
				}
				if kind == "explicit" {
					if _, err := os.Stat(filepath.Join(temp, "wefty-exec")); !os.IsNotExist(err) {
						t.Fatalf("explicit override attempted fallback: %v", err)
					}
				}
			}
		})
	}
}

func TestExecPrivateTempFromRealBinary(t *testing.T) {
	binary := buildWefty(t)
	var previous string
	for _, exit := range []int{0, 1} {
		t.Run(fmt.Sprint(exit), func(t *testing.T) {
			stack := newExecStack(t, execRealBinaryIdleLimit)
			callerTemp := os.Getenv("TMPDIR")
			t.Chdir(execTestRepo(t))
			script := `set -e
printf '%s\000' "$TMPDIR" "$(readlink "$TMPDIR")" "$(ls -A "$TMPDIR")"
printf 'run data' > "$TMPDIR/run-file"
exit "$1"`
			code, stdout, stderr := runPolishBinary(t, binary,
				"--l1="+stack.l1Address, "--l3="+stack.l3Address,
				"--node-config="+stack.clients.nodeConfigPath, "exec", "--", "/bin/sh", "-c", script, "sh", fmt.Sprint(exit))
			wantCode := 0
			if exit != 0 {
				wantCode = 10
			}
			if code != wantCode {
				t.Fatalf("exit=%d want=%d stdout=%q stderr=%s", code, wantCode, stdout, stderr)
			}
			parts := strings.Split(string(stdout), "\x00")
			if len(parts) != 4 {
				t.Fatalf("temp probe output: %q", stdout)
			}
			link := parts[0]
			if link == callerTemp || link == previous || !strings.HasPrefix(link, "/tmp/wefty-run.") || len(link) > 32 || strings.HasSuffix(link, "/") {
				t.Fatalf("run TMPDIR=%q caller=%q previous=%q", link, callerTemp, previous)
			}
			previous = link
			page, err := stack.clients.listRuns(t.Context(), "", 10)
			if err != nil || len(page.Runs) != 1 {
				t.Fatalf("run list: %+v %v", page, err)
			}
			record, err := stack.clients.getRun(t.Context(), page.Runs[0].RunID)
			if err != nil {
				t.Fatal(err)
			}
			var params execParams
			if err := json.Unmarshal(record.Params, &params); err != nil {
				t.Fatal(err)
			}
			target := filepath.Join(params.Storage, "tmp")
			if parts[1] != target || parts[2] != "" {
				t.Fatalf("temp target=%q want=%q initial contents=%q", parts[1], target, parts[2])
			}
			for _, path := range []string{link, target, filepath.Join(target, "run-file")} {
				waitExecPath(t, path, false)
			}
			waitExecStorageEmpty(t, filepath.Dir(params.Storage))
		})
	}
}
