//go:build !unix

package agent

import (
	"errors"
	"os"
)

const noFollowOpenFlag = 0

// Do not silently weaken the directory identity guarantee on platforms without
// the no-follow directory-open implementation.
func openHandoffDirectory(*os.Root, string) (*os.Root, error) {
	return nil, errors.New("retained handoff directories require Unix no-follow directory handles")
}

func openHandoffFile(*os.Root, string, int, os.FileMode) (*os.File, error) {
	return nil, errors.New("retained handoff files require Unix no-follow opens")
}
