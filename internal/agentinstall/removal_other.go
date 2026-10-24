//go:build !darwin && !linux

package agentinstall

import (
	"errors"
	"os"
)

type heldGenerationDirectory struct{ root *os.Root }

func holdGenerationDirectory(*os.Root, string) (*heldGenerationDirectory, error) {
	return nil, errors.New("generation removal requires Darwin or Linux")
}
func (*heldGenerationDirectory) Close() {}
func (*heldGenerationDirectory) remove(*os.Root, string, string) error {
	return errors.New("unsupported generation removal")
}
func (*heldGenerationDirectory) retainEmpty(*os.Root, string, func(string) error) error {
	return errors.New("unsupported generation removal")
}

func (*heldGenerationDirectory) verify(*os.Root, string) error {
	return errors.New("unsupported generation directory verification")
}
