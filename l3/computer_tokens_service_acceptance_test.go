//go:build service_acceptance

package l3

import "testing"

func TestServiceAcceptanceComputerTokenScopeAndRunProvenance(t *testing.T) {
	t.Run("hash-only revocation and explicit L3 promotion generation", TestComputerTokenIsHashOnlyRevocableAndPromotionInvalidated)
	t.Run("root provenance replay and atomic inflight limit", TestComputerRunProvenanceAndAtomicInflightLimit)
	t.Run("attempt and host revocations are identity scoped", TestComputerAttemptAndHostRevocationsAreIdentityScoped)
	t.Run("HTTP surface and host binding", TestComputerHTTPAuthoritySurfaceAndNodeBinding)
	t.Run("disable race is fenced inside Run write", TestComputerCreateRunRechecksRevocationAfterAuthentication)
	t.Run("transient proof failure preserves grant", TestComputerTransientScopeProofFailureDoesNotRevokeGrant)
	t.Run("lineage authorization failure is fail closed", TestRunTokenLineageFilterErrorNeverReturnsUnfilteredDescendants)
}
