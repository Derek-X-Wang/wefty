//go:build service_acceptance

package l1

import "testing"

func TestServiceAcceptanceComputerSubmissionIntent(t *testing.T) {
	t.Run("default and immutable audit", TestComputerSubmissionIntentDefaultsOffAndAdvancesWithAudit)
	t.Run("admin change commits before its L3 revocation", TestComputerSubmissionRouteCommitsBeforeRevokingL3)
	t.Run("scope proof requires live lease and installed policy", TestComputerTokenScopeProofRequiresLiveAttemptAndInstalledPolicy)
}
