package main

import (
	"encoding/json"
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

func TestExecStorageSweepConfirmsProcessBeforeKill(t *testing.T) {
	for _, test := range []struct {
		name    string
		second  string
		gone    bool
		kill    bool
		stopped bool
		warning bool
	}{
		{name: "absent", stopped: true},
		{name: "outside", second: `printf 'p43210\ncsleep\nn%s-outside\n' "$test_storage"`},
		{name: "different-name", second: `printf 'p43210\ncother\nn%s\n' "$test_storage"`},
		{name: "agent", second: `printf 'p43210\ncwefty-agent\nn%s\n' "$test_storage"`},
		{name: "still-matches", second: `printf 'p43210\ncsleep\nn%s\n' "$test_storage"`, kill: true, stopped: true},
		{name: "failed", second: "return 1", warning: true},
		{name: "timeout", second: "return 137", warning: true},
		{name: "failed-and-gone", second: "return 1", gone: true, stopped: true, warning: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			storage, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			signals := filepath.Join(t.TempDir(), "signals")
			// Override shell functions only in this test invocation. Darwin-format
			// records exercise both passes on either host; kill records decisions
			// without ever signaling a fabricated pid or an unrelated process.
			script := execCleanupScript + fmt.Sprintf(`
 test_storage=$1 test_signals=$2
 uname() { printf 'Darwin\n'; }
 exec_storage_bounded_inventory() {
   if [ "${3:-1}" = 1 ]; then
     printf 'p43210\ncsleep\nn%%s\n' "$test_storage"
   else
     [ "$3" = 0.6 ] || return 1
     %s
   fi
 }
 kill() {
   printf '%%s\n' "$*" >> "$test_signals"
   if [ "$1" = -0 ] && [ %t = true ]; then return 1; fi
   return 0
 }
 sweep_exec_storage_processes "$test_storage"
 printf '[%%s]\n' "$exec_stopped_processes"
`, test.second+"\n   :", test.gone)
			command := exec.CommandContext(t.Context(), "/bin/bash", "-c", script, "confirm-kill", storage, signals)
			var stderr strings.Builder
			command.Stderr = &stderr
			output, err := command.Output()
			if err != nil {
				t.Fatalf("sweep: %v: %s", err, &stderr)
			}
			raw, err := os.ReadFile(signals)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(raw), "-TERM 43210\n") {
				t.Fatalf("missing TERM: %s", raw)
			}
			if got := strings.Contains(string(raw), "-KILL 43210\n"); got != test.kill {
				t.Errorf("KILL = %t, want %t: %s", got, test.kill, raw)
			}
			var stopped []struct {
				PID  int    `json:"pid"`
				Name string `json:"name"`
				CWD  string `json:"cwd"`
			}
			if err := json.Unmarshal(output, &stopped); err != nil {
				t.Fatal(err)
			}
			if test.stopped {
				if len(stopped) != 1 || stopped[0].PID != 43210 || stopped[0].Name != "sleep" || stopped[0].CWD != storage {
					t.Errorf("stopped process lost original evidence: %s", output)
				}
			} else if len(stopped) != 0 {
				t.Errorf("unconfirmed process reported stopped: %s", output)
			}
			wantStopped, wantWarning := 0, 0
			if test.stopped {
				wantStopped = 1
			}
			if test.warning {
				wantWarning = 1
			}
			if got := strings.Count(stderr.String(), "wefty: stopped process 43210 (sleep) left in run storage\n"); got != wantStopped {
				t.Errorf("stopped log count = %d: %s", got, &stderr)
			}
			if got := strings.Count(stderr.String(), "wefty: warning: could not stop a process in run storage\n"); got != wantWarning {
				t.Errorf("warning count = %d: %s", got, &stderr)
			}
		})
	}
}
