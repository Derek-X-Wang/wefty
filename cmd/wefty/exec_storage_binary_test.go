package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestExecUnwritableStorageFromRealBinary(t *testing.T) {
	binary := buildWefty(t)
	stack := newExecStack(t, 10*time.Second)
	t.Chdir(execTestRepo(t))
	// Exercise the normal HOME-derived location, independent of the caller's
	// storage overrides. The node and ledger are reachable, but no run may start.
	t.Setenv("WEFTY_EXEC_STORAGE_ROOT", "")
	t.Setenv("XDG_STATE_HOME", "")
	parent := filepath.Join(os.Getenv("HOME"), ".local", "state", "wefty")
	if err := os.MkdirAll(parent, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(parent, 0500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(parent, 0700) })
	probe := filepath.Join(parent, "permission-probe")
	if err := os.Mkdir(probe, 0700); !errors.Is(err, os.ErrPermission) {
		t.Fatalf("fixture must refuse writes: %v", err)
	}
	root := filepath.Join(parent, "exec")
	want := fmt.Sprintf("wefty: cannot write exec snapshot folder %q; a sandboxed AI agent must allow writes to this folder (see the wefty skill: skills/wefty/SKILL.md)\n", root)
	code, output := runWefty(t, binary, 30*time.Second,
		"--l1="+stack.l1Address, "--l3="+stack.l3Address,
		"--node-config="+stack.clients.nodeConfigPath, "exec", "--", "go", "version")
	if code != 1 || output != want {
		t.Fatalf("exec storage refusal: exit=%d, want 1\noutput=%q\nwant=%q", code, output, want)
	}
	runs, err := stack.clients.listRuns(t.Context(), "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(runs.Runs) != 0 {
		t.Fatalf("unwritable storage submitted %d runs", len(runs.Runs))
	}
}
