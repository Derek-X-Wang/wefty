//go:build service_acceptance

package l1

import "testing"

func TestServiceAcceptanceCapabilityScheduling(t *testing.T) {
	assertClaimRequiresAdvertisedCapabilities(t)
	assertUnknownKindDiagnosticClearsWhenCapabilityAppears(t)
	assertConcurrentClaimsCannotBypassCapabilities(t)
}
