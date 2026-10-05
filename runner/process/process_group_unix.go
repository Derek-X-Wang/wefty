//go:build darwin || linux

package process

import (
	"errors"
	"os"
	"os/exec"
	"syscall"

	"github.com/Derek-X-Wang/wefty/contract"
)

func configureProcessGroup(command *exec.Cmd) error {
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	return nil
}

func terminateProcessGroup(processGroupID int) error {
	return ignoreMissingProcessGroup(syscall.Kill(-processGroupID, syscall.SIGTERM))
}

// deliverTermination sends TERM to the process group and reports whether the
// kernel accepted it for a member; ESRCH means the group was already gone.
func deliverTermination(processGroupID int) bool {
	return syscall.Kill(-processGroupID, syscall.SIGTERM) == nil
}

func killProcessGroup(processGroupID int) error {
	return ignoreMissingProcessGroup(syscall.Kill(-processGroupID, syscall.SIGKILL))
}

func processGroupAlive(processGroupID int) bool {
	err := syscall.Kill(-processGroupID, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}

func ignoreMissingProcessGroup(err error) error {
	if errors.Is(err, syscall.ESRCH) {
		return nil
	}
	return err
}

func resultFromWait(waitErr error, state *os.ProcessState, cause contract.TerminationCause) contract.ProcessResult {
	if state != nil {
		if waitStatus, ok := state.Sys().(syscall.WaitStatus); ok && waitStatus.Signaled() {
			return contract.ProcessResult{Signal: waitStatus.Signal().String(), TerminationCause: cause}
		}
		exitCode := state.ExitCode()
		result := contract.ProcessResult{ExitCode: &exitCode}
		// A payload that handles TERM can answer a termination it was asked for
		// with any exit code, zero included. Who asked is kept, so policy never
		// reads that answer as the payload deciding to stop. Callers pass a
		// non-spontaneous cause only when the stop was confirmed delivered.
		if cause != contract.TerminationCauseSpontaneous {
			result.TerminationInitiator = cause
		}
		return result
	}

	return spawnFailure(contract.SpawnFailureProcessWait, waitErr)
}
