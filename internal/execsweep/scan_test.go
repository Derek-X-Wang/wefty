package execsweep

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestFirstInventoryDeadline(t *testing.T) {
	tool := "find"
	if runtime.GOOS == "darwin" {
		tool = "lsof"
	} else if runtime.GOOS != "linux" {
		t.Skip("process inventory requires Darwin or Linux")
	}
	actual, err := exec.LookPath(tool)
	if err != nil {
		t.Fatal(err)
	}
	shim := t.TempDir()
	if err := os.WriteFile(filepath.Join(shim, tool), []byte(fmt.Sprintf("#!/bin/sh\nsleep 1.5\nexec %q \"$@\"\n", actual)), 0700); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ name, setup, want string }{
		{"node", "exec_scan_deadline=3", "complete"},
		{"cli_default", "", "skipped"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			command := exec.CommandContext(t.Context(), "/bin/bash", "-c", Script+"\n"+tc.setup+`
sweep_exec_storage_processes "$1"
printf '%s\n' "$exec_sweep_outcome"
`, "scan-deadline", t.TempDir())
			command.Env = append(os.Environ(), "PATH="+shim+":"+os.Getenv("PATH"))
			var stderr strings.Builder
			command.Stderr = &stderr
			out, err := command.Output()
			if err != nil || strings.TrimSpace(string(out)) != tc.want {
				t.Fatalf("outcome=%q want=%s: %v; %s", out, tc.want, err, &stderr)
			}
			wantWarning := ""
			if tc.want == "skipped" {
				wantWarning = "wefty: warning: could not scan processes in run storage\n"
			}
			if stderr.String() != wantWarning {
				t.Fatalf("diagnostics=%q, want %q", &stderr, wantWarning)
			}
		})
	}
}
