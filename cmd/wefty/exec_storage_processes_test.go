//go:build darwin || linux

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Wrapper modes create a daemon that leaves the wrapper's process group.
// The budget mode exercises CLI retirement in the test's isolated group.
func TestExecStorageProcessHelper(t *testing.T) {
	mode := os.Getenv("EXEC_STORAGE_PROCESS_HELPER")
	if mode == "" {
		return
	}
	if mode == "stale-retirement-budget" {
		if execSweepBudget != 2*time.Second {
			t.Fatalf("CLI sweep budget = %s, want 2s", execSweepBudget)
		}
		storage := os.Getenv("EXEC_STORAGE_BUDGET_STORAGE")
		ctx, cancel := context.WithTimeout(t.Context(), execSweepBudget)
		defer cancel()
		started := time.Now()
		err := retireExecStorageContext(ctx, storage, "capture")
		elapsed, contextErr := time.Since(started), ctx.Err()
		// Include filesystem and rename scheduling in the real two-second budget.
		if err != nil || contextErr != nil || elapsed >= execSweepBudget {
			t.Fatalf("retirement: error=%v context=%v elapsed=%s", err, contextErr, elapsed)
		}
		if _, err := os.Lstat(storage); !os.IsNotExist(err) {
			t.Errorf("storage was not retired: %v", err)
		}
		t.Logf("retirement completed in %s before its 2s deadline", elapsed)
		return
	}
	if mode == "sleep" {
		signal.Ignore(syscall.SIGTERM)
		// The reader parses the PID once the marker exists, so publish it whole.
		ready := os.Getenv("EXEC_STORAGE_READY")
		if err := os.WriteFile(ready+".tmp", []byte(strconv.Itoa(os.Getpid())), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(ready+".tmp", ready); err != nil {
			t.Fatal(err)
		}
		for {
			time.Sleep(time.Hour)
		}
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	child := exec.Command(binary, "-test.run=^TestExecStorageProcessHelper$")
	child.Env = append(os.Environ(), "EXEC_STORAGE_PROCESS_HELPER=sleep")
	child.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if os.Getenv("EXEC_STORAGE_USE_TMP") == "1" {
		child.Dir = os.Getenv("TMPDIR")
	}
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	ready := os.Getenv("EXEC_STORAGE_READY")
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(ready); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("child did not start")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if mode == "cancel" {
		for {
			time.Sleep(time.Hour)
		}
	}
}

func TestExecStaleRetirementScanFitsForegroundBudget(t *testing.T) {
	root := execTestStorageRoot(t)
	storage := filepath.Join(root, "wefty-exec-stale-timeout")
	writeExecFile(t, filepath.Join(storage, "leftover"), "data")
	shim := t.TempDir()
	for _, name := range []string{"lsof", "find"} {
		path := filepath.Join(shim, name)
		writeExecFile(t, path, "#!/bin/sh\nexec /bin/sleep 30\n")
		if err := os.Chmod(path, 0700); err != nil {
			t.Fatal(err)
		}
	}
	outcome := filepath.Join(t.TempDir(), "outcome")
	observer := filepath.Join(t.TempDir(), "bash-env")
	// Observe the existing shell variable without changing the real CLI path.
	// Fresh deletion shells have no sweep outcome and leave this file alone.
	writeExecFile(t, observer, `trap 'if [ -n "${exec_sweep_outcome:-}" ]; then
  printf "%s\n" "$exec_sweep_outcome" > "$EXEC_STORAGE_SWEEP_OUTCOME"
fi' EXIT
`)
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, binary, "-test.run=^TestExecStorageProcessHelper$", "-test.v")
	command.Env = append(os.Environ(),
		"EXEC_STORAGE_PROCESS_HELPER=stale-retirement-budget",
		"EXEC_STORAGE_BUDGET_STORAGE="+storage,
		"EXEC_STORAGE_SWEEP_OUTCOME="+outcome,
		"BASH_ENV="+observer,
		"PATH="+shim+":"+os.Getenv("PATH"))
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	group := command.Process.Pid
	t.Cleanup(func() { _ = syscall.Kill(-group, syscall.SIGKILL) })
	if err := command.Wait(); err != nil {
		t.Errorf("retirement helper: %v\n%s\n%s", err, &stdout, &stderr)
	}
	t.Logf("retirement helper:\n%s", &stdout)
	if got := stderr.String(); got != "wefty: warning: could not scan processes in run storage\n" {
		t.Errorf("scan diagnostics = %q", got)
	}
	if got, err := os.ReadFile(outcome); err != nil || string(got) != "skipped\n" {
		t.Errorf("outcome = %q, want skipped: %v", got, err)
	}
	// Inspect only the isolated fixture's process group.
	processes, err := exec.Command("ps", "-axo", "pid=,pgid=,comm=").Output()
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(processes), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 2 && fields[1] == strconv.Itoa(group) {
			t.Errorf("retirement left a process in group %d: %s", group, line)
		}
	}
	waitExecStorageEmpty(t, root)
}

func TestExecStorageSweepStopsEscapedProcesses(t *testing.T) {
	for _, mode := range []string{"normal", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			root := filepath.Join(execTestStorageRoot(t), `名字-"quoted"`)
			if err := os.Mkdir(root, 0700); err != nil {
				t.Fatal(err)
			}
			t.Setenv("WEFTY_EXEC_STORAGE_ROOT", root)
			capture, err := captureExec(t.Context(), execTestRepo(t))
			if err != nil {
				t.Fatal(err)
			}
			physicalStorage, err := filepath.EvalSymlinks(capture.Storage)
			if err != nil {
				t.Fatal(err)
			}
			wantCWD := filepath.Join(physicalStorage, "work", capture.Folder)
			handoff := t.TempDir()
			marker := filepath.Join(t.TempDir(), "child")
			binary, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			// This same-user process is ours, but its cwd shares only the prefix.
			outsideDir := capture.Storage + "-outside"
			if err := os.Mkdir(outsideDir, 0700); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.RemoveAll(outsideDir) })
			outside := exec.Command("/bin/sleep", "60")
			outside.Dir = outsideDir
			if err := outside.Start(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = outside.Process.Kill(); _ = outside.Wait() })
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
			defer cancel()
			wrapper := exec.CommandContext(ctx, "/bin/bash", "-c", buildExecWrapper(capture, []string{binary, "-test.run=^TestExecStorageProcessHelper$"}))
			wrapper.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
			wrapper.Env = append(os.Environ(), "EXEC_STORAGE_PROCESS_HELPER="+mode, "EXEC_STORAGE_READY="+marker, "WEFTY_HANDOFF_DIR="+handoff)
			if mode == "cancel" {
				wrapper.Env = append(wrapper.Env, "EXEC_STORAGE_USE_TMP=1")
			}
			var stderr bytes.Buffer
			wrapper.Stderr = &stderr
			if err := wrapper.Start(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = wrapper.Process.Kill() })
			waitExecPath(t, marker, true)
			if mode == "cancel" {
				record, err := os.ReadFile(filepath.Join(capture.Storage, "tmp-dir"))
				if err != nil {
					t.Fatal(err)
				}
				wantCWD = filepath.Join(strings.TrimSuffix(string(record), "\n"), "tmp")
			}
			raw, err := os.ReadFile(marker)
			if err != nil {
				t.Fatal(err)
			}
			pid, err := strconv.Atoi(string(raw))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = syscall.Kill(pid, syscall.SIGKILL) })
			if mode == "cancel" {
				if err := syscall.Kill(-wrapper.Process.Pid, syscall.SIGTERM); err != nil {
					t.Fatal(err)
				}
			}
			err = wrapper.Wait()
			if mode == "normal" && err != nil {
				t.Fatalf("wrapper: %v: %s", err, &stderr)
			}
			if mode == "cancel" && (err == nil || wrapper.ProcessState.ExitCode() != 143) {
				t.Fatalf("cancel: %v: %s", err, &stderr)
			}
			// A dead process can briefly remain a zombie on Linux; a live one still
			// has a working directory. The platform helper reaps only this test child.
			if !execStorageChildGone(pid) {
				t.Errorf("escaped child %d survived retirement", pid)
			}
			if !strings.Contains(stderr.String(), fmt.Sprintf("wefty: stopped process %d (", pid)) {
				t.Errorf("missing stopped-process log: %q", &stderr)
			}
			if err := outside.Process.Signal(syscall.Signal(0)); err != nil {
				t.Errorf("outside process was stopped: %v", err)
			}
			var result struct {
				ExitCode int `json:"exit_code"`
				Stopped  []struct {
					PID  int    `json:"pid"`
					Name string `json:"name"`
					CWD  string `json:"cwd"`
				} `json:"stopped_processes"`
			}
			document, err := os.ReadFile(filepath.Join(handoff, "result.json"))
			if err != nil {
				t.Errorf("result: %v", err)
			} else if err := json.Unmarshal(document, &result); err != nil {
				t.Fatal(err)
			} else if len(result.Stopped) != 1 || result.Stopped[0].PID != pid || result.Stopped[0].Name == "" || result.Stopped[0].CWD != wantCWD {
				t.Errorf("result omitted stopped child: %s", document)
			}
			_ = outside.Process.Kill()
			_ = outside.Wait()
			_ = os.RemoveAll(outsideDir)
			waitExecStorageEmpty(t, root)
		})
	}
}

func TestExecStorageSweepEvidenceReachesAttempt(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "child")
	t.Setenv("EXEC_STORAGE_PROCESS_HELPER", "normal")
	t.Setenv("EXEC_STORAGE_READY", marker)
	stack := newExecStack(t, execTestIdleLimit)
	t.Chdir(execTestRepo(t))
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	if err := execute(t.Context(), stack.clients, false, []string{"exec", "--", binary, "-test.run=^TestExecStorageProcessHelper$"}, io.Discard, &stderr); err != nil {
		t.Fatalf("exec: %v: %s", err, &stderr)
	}
	raw, err := os.ReadFile(marker)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(string(raw))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if !execStorageChildGone(pid) {
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
	})
	runs, err := stack.clients.listRuns(t.Context(), "", 10)
	if err != nil || len(runs.Runs) != 1 {
		t.Fatalf("runs: %+v %v", runs, err)
	}
	// exec returns at the command's exit, and the Run can become terminal before its result upload arrives.
	record := waitExecRunResult(t, stack.clients, runs.Runs[0].RunID)
	var result struct {
		Stopped []struct {
			PID int `json:"pid"`
		} `json:"stopped_processes"`
	}
	if err := json.Unmarshal(record.Document, &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Stopped) != 1 || result.Stopped[0].PID != pid {
		t.Errorf("Run result: %s", record.Document)
	}
	execution, err := stack.clients.getRunExecution(t.Context(), runs.Runs[0].RunID)
	if err != nil {
		t.Fatal(err)
	}
	document, err := json.Marshal(execution)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(document, []byte(fmt.Sprintf(`"pid":%d`, pid))) {
		t.Errorf("Attempt result omitted stopped process: %s", document)
	}
	if !execStorageChildGone(pid) {
		t.Errorf("escaped process survived: %d", pid)
	}
	if !strings.Contains(stderr.String(), fmt.Sprintf("wefty: stopped process %d (", pid)) {
		t.Errorf("run log omitted stopped process: %s", &stderr)
	}
}

func TestExecStorageSweepProtectsRetiringShellAndParent(t *testing.T) {
	root := execTestStorageRoot(t)
	storage := filepath.Join(root, "wefty-exec-ancestor")
	if err := os.Mkdir(storage, 0700); err != nil {
		t.Fatal(err)
	}
	// Both shells have cwd inside storage. They are test-owned, so a broken
	// exclusion cannot signal the go test process or any unrelated host process.
	inner := execCleanupScript + "\nretire_exec_storage \"$1\" capture\nprintf 'inner survived\\n'"
	cmd := exec.CommandContext(t.Context(), "/bin/bash", "-c", `cd "$1" && /bin/bash -c "$2" inner "$1" && printf 'ancestor survived\n'`, "outer", storage, inner)
	output, err := cmd.CombinedOutput()
	if err != nil || !bytes.Contains(output, []byte("inner survived\nancestor survived\n")) {
		t.Fatalf("retiring shells were stopped: %v %s", err, output)
	}
	if bytes.Contains(output, []byte("stopped process")) {
		t.Errorf("protected process reported stopped: %s", output)
	}
	waitExecStorageEmpty(t, root)
}

func TestExecStorageSweepDiscoveryFailureDoesNotBlockRetirement(t *testing.T) {
	root := execTestStorageRoot(t)
	storage := filepath.Join(root, "wefty-exec-discovery-failed")
	if err := os.Mkdir(storage, 0700); err != nil {
		t.Fatal(err)
	}
	shim := t.TempDir()
	writeExecFile(t, filepath.Join(shim, "uname"), "#!/bin/sh\nexit 1\n")
	if err := os.Chmod(filepath.Join(shim, "uname"), 0700); err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(t.Context(), "/bin/bash", "-c", execCleanupScript+"\nretire_exec_storage \"$1\" capture", "cleanup", storage)
	cmd.Env = append(os.Environ(), "PATH="+shim+":"+os.Getenv("PATH"))
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("discovery failure prevented retirement: %v %s", err, output)
	}
	if strings.Count(string(output), "wefty: warning: could not scan processes in run storage\n") != 1 {
		t.Errorf("expected one scan warning: %q", output)
	}
	waitExecStorageEmpty(t, root)
}

func TestExecStorageSweepPreservesResultWhenEmpty(t *testing.T) {
	for _, existing := range []bool{false, true} {
		t.Run(fmt.Sprintf("existing=%t", existing), func(t *testing.T) {
			root := execTestStorageRoot(t)
			storage := filepath.Join(root, "wefty-exec-empty")
			if err := os.Mkdir(storage, 0700); err != nil {
				t.Fatal(err)
			}
			handoff := t.TempDir()
			result := filepath.Join(handoff, "result.json")
			original := []byte("{\n  \"exit_code\": 7, \"extra\": \"keep exactly\"\n}\n")
			if existing {
				if err := os.WriteFile(result, original, 0600); err != nil {
					t.Fatal(err)
				}
			}
			command := exec.CommandContext(t.Context(), "/bin/bash", "-c", execCleanupScript+"\ncd /tmp && retire_exec_storage \"$1\" capture \"$2\" 143", "empty-result", storage, handoff)
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("retirement: %v %s", err, output)
			}
			document, err := os.ReadFile(result)
			if existing {
				if err != nil || !bytes.Equal(document, original) {
					t.Errorf("empty sweep changed result: %q %v", document, err)
				}
			} else if !os.IsNotExist(err) {
				t.Errorf("empty sweep created result: %q %v", document, err)
			}
			waitExecStorageEmpty(t, root)
		})
	}
}
