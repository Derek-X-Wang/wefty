//go:build !unix

package agentinstall

import (
	"errors"
	"os"
)

func readRetainedPlist(*os.Root, string) ([]byte, error) {
	return nil, errors.New("retained generation plist descriptor reads require Unix")
}
