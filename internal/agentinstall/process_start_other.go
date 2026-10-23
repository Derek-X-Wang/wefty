//go:build !darwin

package agentinstall

import "errors"

// Rolling generation activation is Mac-only. Other platforms fail closed.
func processStartIdentity(int) (string, error) {
	return "", errors.New("generation process start identity requires Darwin")
}
