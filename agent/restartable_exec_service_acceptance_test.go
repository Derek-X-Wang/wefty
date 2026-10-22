//go:build service_acceptance && (darwin || linux)

package agent

import "testing"

func TestRestartableExecControlledShutdownServiceAcceptance(t *testing.T) {
	assertRestartableExecRecovery(t, false)
}
