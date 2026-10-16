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
