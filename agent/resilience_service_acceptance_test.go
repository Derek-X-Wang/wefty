//go:build service_acceptance && (darwin || linux)

package agent

import "testing"

func TestServiceAcceptanceAgentResilienceCutover(t *testing.T) {
	t.Run("attempt authority is isolated and the daemon rejoins", assertPartitionedAgentRejoinsWithoutRepeatingExpiredAttempt)
	t.Run("a silent renewal cannot outlive local authority", assertSilentRenewalHangCannotOutliveLocalAuthority)
	t.Run("suspend wall time consumes authority", assertAuthorityWatchdogCountsSuspendWallGap)
	t.Run("blocked reap remains visible without process exit", assertUnreapedPayloadStaysVisibleWithoutKillingDaemon)
	t.Run("registration recovers with observable backoff", assertAgentReregistersAfterControlPlaneReturns)
	t.Run("non-retryable session failure is observable without process exit", assertAgentQuarantineIsObservableAndNonTerminal)
}
