package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestExecWrapperPhysicalWorkingDirectory(t *testing.T) {
	root := execTestStorageRoot(t)
	link := filepath.Join(t.TempDir(), "storage")
	if err := os.Symlink(root, link); err != nil {
		t.Fatal(err)
	}
	t.Setenv("WEFTY_EXEC_STORAGE_ROOT", link)
	capture, err := captureExec(t.Context(), filepath.Join(execTestRepo(t), "sub"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cleanupExecStorage(capture.Storage) })
	physicalStorage, err := filepath.EvalSymlinks(capture.Storage)
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Dir(capture.Storage) != link || capture.Storage == physicalStorage {
		t.Fatalf("storage must use the symlink: logical=%q physical=%q", capture.Storage, physicalStorage)
	}
	command := exec.CommandContext(t.Context(), "/bin/bash", "-c", buildExecWrapper(capture, []string{"/bin/sh", "-c", `printf '%s\n' "$PWD"; pwd -P`}))
	var stderr bytes.Buffer
	command.Stderr = &stderr
	out, err := command.Output()
	if err != nil {
		t.Fatalf("wrapper: %v: %s", err, &stderr)
	}
	want := filepath.Join(physicalStorage, "work", "sub")
	if string(out) != want+"\n"+want+"\n" {
		t.Fatalf("PWD and physical cwd = %q, want both %q", out, want)
	}
}
