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
		return contract.ProcessResult{ExitCode: &exitCode}
	}

	return spawnFailure(contract.SpawnFailureProcessWait, waitErr)
}
