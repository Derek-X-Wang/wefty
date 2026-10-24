//go:build !darwin && !linux

package agentinstall

import (
	"errors"
	"os"
)

func renameInitializationEntry(*os.Root, string, *os.Root, string) error {
	return errors.New("initialization custody requires Darwin or Linux")
}
func linkInitializationEntry(*os.Root, string, *os.Root, string) error {
	return errors.New("initialization custody requires Darwin or Linux")
}
