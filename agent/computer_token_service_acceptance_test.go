//go:build service_acceptance

package agent

import "testing"

func TestServiceAcceptanceComputerTokenInjection(t *testing.T) {
	t.Run("default off and enabled closed injection", TestComputerTokenMintedIntoSensitiveClosedInputOnlyWhenEnabled)
	t.Run("mint failure is pass unavailable before runtime", TestComputerTokenMintFailureStopsBeforeRuntime)
	t.Run("live policy change rotates attempt-local token file", TestComputerSubmissionPolicyChangeRotatesAttemptTokenFile)
	t.Run("transport allowlist and typed errors", TestComputerAttemptBridgeProjectsOnlyTheL3OwnedSurface)
	t.Run("negative mutation receipt", TestComputerAttemptBridgeNegativeRouteReceiptIsAssertionDerived)
	t.Run("policy loss cancellation and re-enable", TestComputerSubmissionPolicyLossCancelsInflightAndReenableRestoresTransport)
	t.Run("production Computer surface and forced Mac fallback", TestStartWorkflowBridgeSelectsComputerSurfaceOnForcedMacFallback)
	t.Run("allowlist equals L3 Computer routes", TestComputerAttemptBridgeAllowlistExactlyMirrorsL3ComputerRoutes)
}
