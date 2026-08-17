//go:build service_acceptance

package l1

import "testing"

func TestServiceAcceptanceClaimTimeAuthorityAndDurableOperatorIntent(t *testing.T) {
	assertClaimTimeAuthorityAndIntent(t)
}
