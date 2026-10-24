//go:build !unix

package agentinstall

import (
	"errors"
	"os"
)

func readRetainedPlist(*os.Root, string) ([]byte, error) {
	return nil, errors.New("retained generation plist descriptor reads require Unix")
}

func ReadPrivateFile(*os.Root, string) ([]byte, error) {
	return nil, errors.New("private descriptor reads require Unix")
}

func readOwnedFile(*os.Root, string, os.FileMode) ([]byte, error) {
	return nil, errors.New("descriptor reads require Unix")
}

func ReadOwnedRegularFile(*os.Root, string, os.FileMode) ([]byte, error) {
	return nil, errors.New("descriptor reads require Unix")
}
