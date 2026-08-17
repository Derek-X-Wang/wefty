//go:build !darwin && !linux

package process

import (
	"errors"
	"os"
	"os/exec"

	"github.com/Derek-X-Wang/wefty/contract"
)

var errUnsupportedPlatform = errors.New("process runner requires darwin or linux process groups")

func configureProcessGroup(*exec.Cmd) error { return errUnsupportedPlatform }
func terminateProcessGroup(int) error       { return errUnsupportedPlatform }
func killProcessGroup(int) error            { return errUnsupportedPlatform }
func processGroupAlive(int) bool            { return false }

func resultFromWait(waitErr error, state *os.ProcessState, _ contract.TerminationCause) contract.ProcessResult {
	if state != nil {
		exitCode := state.ExitCode()
		return contract.ProcessResult{ExitCode: &exitCode}
	}
	return spawnFailure(contract.SpawnFailureProcessWait, waitErr)
}
