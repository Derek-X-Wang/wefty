package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/execsweep"
)

// sweepExecCaptureRoot uses only the retained mailbox directory, never its
// workload-visible pathname. Derive its physical path from the open directory
// (F_GETPATH on Darwin, /proc/self/fd on Linux), then check its identity against
// that handle. Failed or stale resolution skips the sweep with a warning.
func (m *runMailbox) sweepExecCaptureRoot(ctx context.Context, root *os.Root, stale bool) []contract.StoppedProcess {
	diagnose := func(output []byte) {
		if stale {
			output = bytes.ReplaceAll(output, []byte("wefty: "), []byte("wefty: stale exec cleanup: "))
		}
		if m.captureStderr != nil && m.captureStderr(ctx, output) == nil {
			return
		}
		m.log("agent: %s", strings.TrimSpace(string(output)))
	}
	info, err := root.Lstat("wefty-exec-work")
	if os.IsNotExist(err) || (err == nil && !info.IsDir()) {
		return nil
	}
	if err != nil {
		diagnose([]byte(fmt.Sprintf("wefty: warning: could not inspect exec storage: %v\n", err)))
		return nil
	}
	directory, err := root.Open(".")
	if err != nil {
		diagnose([]byte(fmt.Sprintf("wefty: warning: could not open retained exec mailbox: %v\n", err)))
		return nil
	}
	defer directory.Close()
	physical, err := execMailboxPhysicalPath(directory)
	if err == nil {
		var held, resolved os.FileInfo
		held, err = directory.Stat()
		if err == nil {
			resolved, err = os.Stat(physical)
			if err == nil && (!filepath.IsAbs(physical) || !os.SameFile(held, resolved)) {
				err = fmt.Errorf("physical path does not identify the retained mailbox")
			}
		}
	}
	if err != nil {
		diagnose([]byte(fmt.Sprintf("wefty: warning: could not resolve retained exec mailbox for process sweep: %v\n", err)))
		return nil
	}
	// Do not inherit a cancelled workload/finalization context: cleanup still
	// needs its own bounded window after cancellation. The script's inventory
	// deadlines remain unchanged; this outer bound also covers a stuck helper.
	sweepCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	command := exec.CommandContext(sweepCtx, "/bin/bash", "-c", execsweep.Script+`
sweep_exec_storage_processes "$1/wefty-exec-work"
printf '[%s]\n' "$exec_stopped_processes"
`, "wefty-agent-exec-sweep", physical)
	command.Dir = "/"
	command.WaitDelay = 100 * time.Millisecond
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	// Bash is the agent's direct child. Its $$ / $PPID exclusions protect
	// itself and this agent, including in tests with a different process name.
	err = command.Run()
	if stderr.Len() > 0 {
		diagnose(stderr.Bytes())
	}
	if err != nil {
		diagnose([]byte(fmt.Sprintf("wefty: warning: exec storage process sweep failed: %v\n", err)))
	}
	var stopped []contract.StoppedProcess
	if stdout.Len() > 0 {
		if err := json.Unmarshal(stdout.Bytes(), &stopped); err != nil {
			diagnose([]byte(fmt.Sprintf("wefty: warning: could not read exec storage process sweep evidence: %v\n", err)))
			return nil
		}
	}
	return stopped
}
