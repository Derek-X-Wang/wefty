//go:build service_acceptance

package l1

import "testing"

func TestServiceAcceptanceClaimTimeAuthorityAndDurableOperatorIntent(t *testing.T) {
	assertClaimTimeAuthorityAndIntent(t)
}

func TestServiceAcceptanceBootTakeoverFencing(t *testing.T) {
	assertBootTakeoverFencesAuthorityWritesButRetainsEvidence(t)
}
