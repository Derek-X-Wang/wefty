//go:build service_acceptance

package agent

import "testing"

func TestServiceAcceptanceLiveCapabilityWithdrawal(t *testing.T) {
	assertFailedCapabilityProbeSuppressesLocalOCIStartImmediately(t)
	assertAgentPublishesProbeWithdrawalByNextSuccessfulHeartbeat(t)
}
