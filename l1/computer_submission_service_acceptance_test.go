//go:build service_acceptance

package l1

import "testing"

func TestServiceAcceptanceComputerSubmissionIntent(t *testing.T) {
	t.Run("default and immutable audit", TestComputerSubmissionIntentDefaultsOffAndAdvancesWithAudit)
	t.Run("L3 revocation precedes admin success", TestComputerSubmissionRouteRevokesL3BeforeReportingSuccess)
	t.Run("scope proof requires live lease and installed policy", TestComputerTokenScopeProofRequiresLiveAttemptAndInstalledPolicy)
}
