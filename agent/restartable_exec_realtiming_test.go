//go:build service_acceptance_realtiming && (darwin || linux)

package agent

import "testing"

func TestRestartableExecGuardianLeaseLossRealtiming(t *testing.T) {
	assertRestartableExecRecovery(t, true)
}
