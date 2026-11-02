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
	"syscall"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/execsweep"
)

// sweepExecCaptureRoot uses only the retained mailbox directory, never its
// workload-visible pathname. Derive its physical path from the open directory
// (F_GETPATH on Darwin, /proc/self/fd on Linux), then check its identity against
// that handle. Failed or stale resolution skips the sweep with a warning.
func (m *runMailbox) sweepExecCaptureRoot(ctx context.Context, root *os.Root, stale bool) []contract.StoppedProcess {
	return m.sweepExecCaptureRootWithProbe(ctx, root, stale, 5*time.Second, probeExecSweepDirectory)
}

type execSweepDirectory struct {
	directory          *os.File
	physical, identity string
}

func probeExecSweepDirectory(root *os.Root) (target execSweepDirectory, err error) {
	info, err := root.Lstat("wefty-exec-work")
	if os.IsNotExist(err) || (err == nil && !info.IsDir()) {
		return target, nil
	}
	if err != nil {
		return target, fmt.Errorf("inspect exec storage: %w", err)
	}
	target.directory, err = root.Open(".")
	if err != nil {
		return target, fmt.Errorf("open retained exec mailbox: %w", err)
	}
	// The caller owns directory even on error, including a late probe result.
	target.physical, err = execMailboxPhysicalPath(target.directory)
	if err != nil {
		return target, fmt.Errorf("resolve retained exec mailbox: %w", err)
	}
	held, err := target.directory.Stat()
	if err != nil {
		return target, err
	}
	resolved, err := os.Stat(target.physical)
	if err != nil {
		return target, err
	}
	if !filepath.IsAbs(target.physical) || !os.SameFile(held, resolved) {
		return target, fmt.Errorf("physical path does not identify the retained mailbox")
	}
	stat := held.Sys().(*syscall.Stat_t)
	target.identity = fmt.Sprintf("%d:%d", stat.Dev, stat.Ino)
	return target, nil
}

func (m *runMailbox) sweepExecCaptureRootWithProbe(ctx context.Context, root *os.Root, stale bool, budget time.Duration, probe func(*os.Root) (execSweepDirectory, error)) []contract.StoppedProcess {
	// Cancellation must not skip cleanup. This single budget starts before
	// filesystem probes; Bash receives only the time remaining after them.
	sweepCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), budget)
	defer cancel()
	diagnose := func(output []byte) {
		if stale {
			output = bytes.ReplaceAll(output, []byte("wefty: "), []byte("wefty: stale exec cleanup: "))
		}
		if m.captureStderr != nil && m.captureStderr(ctx, output) == nil {
			return
		}
		m.log("agent: %s", strings.TrimSpace(string(output)))
	}
	type probeResult struct {
		target execSweepDirectory
		err    error
	}
	ready := make(chan probeResult)
	go func() {
		// A stalled filesystem can keep this goroutine blocked after timeout.
		// It retains ownership until the unbuffered send transfers the handle;
		// if it finishes late, it closes the handle itself instead.
		target, err := probe(root)
		select {
		case ready <- probeResult{target, err}:
		case <-sweepCtx.Done():
			if target.directory != nil {
				_ = target.directory.Close()
			}
		}
	}()
	var result probeResult
	select {
	case result = <-ready:
		if result.target.directory != nil {
			defer result.target.directory.Close()
		}
	case <-sweepCtx.Done():
		diagnose([]byte("wefty: warning: exec storage process sweep probes exceeded deadline; skipping sweep\n"))
		return nil
	}
	if sweepCtx.Err() != nil {
		diagnose([]byte("wefty: warning: exec storage process sweep probes exceeded deadline; skipping sweep\n"))
		return nil
	}
	if result.err != nil {
		diagnose([]byte(fmt.Sprintf("wefty: warning: could not probe retained exec mailbox for process sweep: %v\n", result.err)))
		return nil
	}
	if result.target.directory == nil {
		return nil
	}
	command := exec.CommandContext(sweepCtx, "/bin/bash", "-c", execsweep.Script+`
skip_sweep() {
  printf 'wefty: warning: could not verify retained exec mailbox identity; skipping process sweep\n' >&2
  printf '[]\n'
  exit 0
}
cd -P -- "$1" || skip_sweep
platform=$(uname -s) || skip_sweep
case "$platform" in
  Darwin) identity=$(stat -f '%d:%i' .) || skip_sweep ;;
  Linux) identity=$(stat -c '%d:%i' .) || skip_sweep ;;
  *) skip_sweep ;;
esac
[ "$identity" = "$2" ] || skip_sweep
# Preserve trailing newlines so the shared script rejects unsupported paths.
physical=$(pwd -P && printf '.') || skip_sweep
physical=${physical%$'\n.'}
sweep_exec_storage_processes "$physical/wefty-exec-work" "$3/wefty-exec-work"
printf '[%s]\n' "$exec_stopped_processes"
`, "wefty-agent-exec-sweep", result.target.physical, result.target.identity, m.directory)
	command.Dir = "/"
	command.WaitDelay = 100 * time.Millisecond
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	// Bash is the agent's direct child. Its $$ / $PPID exclusions protect
	// itself and this agent, including in tests with a different process name.
	err := command.Run()
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
