//go:build service_acceptance

package agent

import "testing"

func TestServiceAcceptanceComputerTokenInjection(t *testing.T) {
	t.Run("default off and enabled closed injection", TestComputerTokenMintedIntoSensitiveClosedInputOnlyWhenEnabled)
	t.Run("mint failure is pass unavailable before runtime", TestComputerTokenMintFailureStopsBeforeRuntime)
	t.Run("live policy change rotates attempt-local token file", TestComputerSubmissionPolicyChangeRotatesAttemptTokenFile)
	t.Run("transport allowlist and typed errors", TestComputerAttemptBridgeProjectsOnlyTheL3OwnedSurface)
	t.Run("negative mutation receipt", TestComputerAttemptBridgeNegativeRouteReceiptIsAssertionDerived)
	t.Run("disable cancellation and re-enable", TestComputerAttemptBridgeDisableCancelsInflightAndReenableRestoresTransport)
	t.Run("Computer-only service eligibility", TestAttemptBridgeEligibilityAddsOnlyComputerServices)
}
