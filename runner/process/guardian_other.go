//go:build !darwin && !linux

package process

import (
	"context"
	"io"
	"os"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
)

func (runner *Runner) runGuarded(
	context.Context,
	Request,
	OutputSink,
	time.Duration,
	time.Duration,
	time.Duration,
) (contract.ProcessResult, error) {
	return spawnFailure(contract.SpawnFailureProcessGroupSetup, errUnsupportedPlatform), errUnsupportedPlatform
}

func serveGuardian(*os.File, io.Writer, io.Writer) error { return errUnsupportedPlatform }
