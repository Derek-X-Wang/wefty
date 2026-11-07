//go:build darwin || linux

package execsweep

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestBoundedInventoryLeavesNoProcesses(t *testing.T) {
	for _, tc := range []struct {
		name, scanner, deadline, output string
		status                          int
		beforePIDWrite                  string
		failPIDWrite                    bool
		maxElapsed                      time.Duration
	}{
		{name: "complete", scanner: "/bin/sleep 0.1; printf inventory-bytes", deadline: "3", output: "inventory-bytes"},
		{name: "status_one", scanner: "/bin/sleep 0.1; printf partial-bytes; exit 1", deadline: "3", output: "partial-bytes", status: 1},
		{name: "timeout", scanner: "printf partial-bytes; exec /bin/sleep 30", deadline: "0.6", output: "partial-bytes", status: 137},
		// The scanner sleeps 8 s, so a 6 s cap still catches an unbounded wait
		// while leaving room for a loaded CI runner.
		{name: "late_pid", scanner: "printf partial-bytes; exec /bin/sleep 8", deadline: "1", output: "partial-bytes", status: 137,
			beforePIDWrite: "    /bin/sleep 1.5\n", maxElapsed: 6 * time.Second},
		{name: "failed_pid_write", scanner: "exec /bin/sleep 8", deadline: "1", status: 2,
			failPIDWrite: true, maxElapsed: 6 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			platform, tool := "Linux", "find"
			if runtime.GOOS == "darwin" {
				platform, tool = "Darwin", "lsof"
			}
			if err := os.WriteFile(filepath.Join(dir, tool), []byte("#!/bin/sh\n"+tc.scanner+"\n"), 0700); err != nil {
				t.Fatal(err)
			}
			gate := filepath.Join(dir, "gate")
			if err := syscall.Mkfifo(gate, 0600); err != nil {
				t.Fatal(err)
			}
			// Keep the FIFO open so the caller blocks in Bash's read builtin,
			// without a harness sleep that could hide an inventory sleep leak.
			gateFile, err := os.OpenFile(gate, os.O_RDWR, 0600)
			if err != nil {
				t.Fatal(err)
			}
			defer gateFile.Close()
			ready := filepath.Join(dir, "ready")
			output := filepath.Join(dir, "output")
			markers := filepath.Join(dir, "markers")
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			inventoryScript := Script
			if tc.beforePIDWrite != "" || tc.failPIDWrite {
				// Inject only into this fixture's script copy, at PID publication.
				const writePID = `    if ! printf '%s\n' "$scan" > "$marker"; then`
				if strings.Count(inventoryScript, writePID) != 1 {
					t.Fatal("PID publication seam changed")
				}
				replacement := tc.beforePIDWrite + writePID
				if tc.failPIDWrite {
					replacement = "    if ! false; then"
				}
				inventoryScript = strings.Replace(inventoryScript, writePID, replacement, 1)
			}
			command := exec.CommandContext(ctx, "/bin/bash", "-c", inventoryScript+`
marker_log=$6
mktemp() {
  local path
  path=$(command mktemp "$@") || return
  printf '%s\n' "$path" >> "$marker_log"
  printf '%s\n' "$path"
}
inventory=$(exec_storage_bounded_inventory "$1" 0 "$2")
status=$?
printf '%s' "$inventory" > "$3"
printf '%s\n' "$status" > "$4"
IFS= read -r release < "$5"
`, "inventory-processes", platform, tc.deadline, output, ready, gate, markers)
			command.Env = append(os.Environ(), "PATH="+dir+":"+os.Getenv("PATH"))
			command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
			var stderr strings.Builder
			command.Stderr = &stderr
			started := time.Now()
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- command.Wait() }()
			t.Cleanup(func() {
				// This group belongs only to the fixture, including old-code leaks.
				_ = syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
				<-done
			})
			var status []byte
			for {
				status, err = os.ReadFile(ready)
				if err == nil && strings.Contains(string(status), "\n") {
					break
				}
				select {
				case <-ctx.Done():
					t.Fatalf("inventory did not finish: %v; %s", ctx.Err(), &stderr)
				case <-time.After(5 * time.Millisecond):
				}
			}
			if elapsed := time.Since(started); tc.maxElapsed > 0 && elapsed > tc.maxElapsed {
				t.Errorf("inventory took %s, want at most %s", elapsed, tc.maxElapsed)
			}
			if got := strings.TrimSpace(string(status)); got != strconv.Itoa(tc.status) {
				t.Errorf("status=%s, want %d; %s", got, tc.status, &stderr)
			}
			if got, err := os.ReadFile(output); err != nil || string(got) != tc.output {
				t.Errorf("stdout=%q, want %q: %v", got, tc.output, err)
			}
			processes, err := exec.Command("ps", "-axo", "pid=,pgid=,comm=").Output()
			if err != nil {
				t.Fatalf("inspect fixture process group: %v", err)
			}
			group := strconv.Itoa(command.Process.Pid)
			for _, line := range strings.Split(string(processes), "\n") {
				fields := strings.Fields(line)
				if len(fields) >= 2 && fields[1] == group && fields[0] != group {
					t.Errorf("inventory left a process in group %s: %s", group, line)
				}
			}
			// Private markers must be gone before inventory returns, including
			// timeout. Track them through mktemp rather than scanning shared /tmp.
			paths, err := os.ReadFile(markers)
			if err != nil && !os.IsNotExist(err) {
				t.Fatal(err)
			}
			for _, path := range strings.Fields(string(paths)) {
				if !strings.HasPrefix(path, "/tmp/wefty-exec-scan.") {
					t.Errorf("marker is not in the private /tmp namespace: %s", path)
				}
				if _, err := os.Lstat(path); !os.IsNotExist(err) {
					t.Errorf("inventory left its marker %s: %v", path, err)
				}
			}
		})
	}
}
