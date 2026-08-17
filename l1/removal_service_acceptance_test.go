//go:build service_acceptance

package l1

import "testing"

func TestServiceAcceptanceRemovalProtocolAndSecretScrubbing(t *testing.T) {
	assertServiceRemovalControllerTransactionAndAttestation(t)
	assertServiceRemovalAcceptsEveryBoundServiceState(t)
	assertUnboundServiceRemovalFinalizesWithoutAgentAttestation(t)
	assertServiceRemovalForceForgetLeavesDirectiveStanding(t)
}
