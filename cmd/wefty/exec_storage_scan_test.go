package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestExecTrashRetirementSkipsProcessInventory(t *testing.T) {
	root := execTestStorageRoot(t)
	trash := filepath.Join(root, ".wefty-trash-ready")
	writeExecFile(t, filepath.Join(trash, "leftover"), "data")
	marker := filepath.Join(t.TempDir(), "scanner-invoked")
	shim := t.TempDir()
	for _, name := range []string{"lsof", "find"} {
		path := filepath.Join(shim, name)
		writeExecFile(t, path, fmt.Sprintf("#!/bin/sh\nprintf invoked > %q\n", marker))
		if err := os.Chmod(path, 0700); err != nil {
			t.Fatal(err)
		}
	}
	command := exec.CommandContext(t.Context(), "/bin/bash", "-c", execCleanupScript+"\nretire_exec_storage \"$1\" trash", "trash-retirement", trash)
	command.Env = append(os.Environ(), "PATH="+shim+":"+os.Getenv("PATH"))
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("retirement: %v %s", err, output)
	}
	waitExecStorageEmpty(t, root)
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Errorf("trash retirement invoked process discovery: %v", err)
	}
}

func TestExecCaptureSweepRemovesOnlyRetiredStorage(t *testing.T) {
	root := execTestStorageRoot(t)
	storage := filepath.Join(root, "wefty-exec-capture")
	writeExecFile(t, filepath.Join(storage, "leftover"), "data")
	record := filepath.Join(t.TempDir(), "removals")
	env := filepath.Join(t.TempDir(), "bash-env")
	writeExecFile(t, env, fmt.Sprintf(`function /bin/rm() {
 printf '%%s\n' "$*" >> %q
 command /bin/rm "$@"
}
`, record))
	command := exec.CommandContext(t.Context(), "/bin/bash", "-c", execCleanupScript+"\nretire_exec_storage \"$1\" capture", "capture-retirement", storage)
	command.Env = append(os.Environ(), "BASH_ENV="+env)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("retirement: %v %s", err, output)
	}
	waitExecStorageEmpty(t, root)
	raw, err := os.ReadFile(record)
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		target := strings.TrimPrefix(line, "-rf -- ")
		if !strings.HasPrefix(filepath.Base(target), ".wefty-trash-") {
			t.Errorf("sweep removed a scratch path before retirement: %s", line)
		}
	}
}

func TestExecCaptureScanFailureWarnsOnce(t *testing.T) {
	root := execTestStorageRoot(t)
	storage := filepath.Join(root, "wefty-exec-discovery-failure")
	writeExecFile(t, filepath.Join(storage, "leftover"), "data")
	shim := t.TempDir()
	for _, name := range []string{"lsof", "find"} {
		path := filepath.Join(shim, name)
		writeExecFile(t, path, "#!/bin/sh\nprintf 'scanner detail\\n' >&2\nexit 1\n")
		if err := os.Chmod(path, 0700); err != nil {
			t.Fatal(err)
		}
	}
	command := exec.CommandContext(t.Context(), "/bin/bash", "-c", execCleanupScript+"\nretire_exec_storage \"$1\" capture", "failed-inventory", storage)
	command.Env = append(os.Environ(), "PATH="+shim+":"+os.Getenv("PATH"))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("scan failure blocked retirement: %v %s", err, output)
	}
	if string(output) != "wefty: warning: could not scan processes in run storage\n" {
		t.Errorf("scan failure did not emit exactly one warning: %q", output)
	}
	waitExecStorageEmpty(t, root)
}
