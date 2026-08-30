//go:build service_acceptance

package l1

import "testing"

func TestServiceAcceptanceRestartableCompletionCreatesFreshAttempt(t *testing.T) {
	assertServiceRestartableCompletionRequeuesWithPersistedBackoff(t)
}

func TestServiceAcceptanceCompletionClassificationAndCappedBackoff(t *testing.T) {
	assertServiceCompletionClassification(t)
	assertServiceBackoffUsesPinnedSequenceAndEffectiveLeaseCap(t)
}

func TestServiceAcceptancePublishedListenerFailureRequeuesWithoutLatching(t *testing.T) {
	assertPublishedListenerFailureRequeuesWithoutLatching(t)
}

func TestServiceAcceptanceClassAwareExpiryAtBothSites(t *testing.T) {
	assertServiceLeaseExpiryRequeuesWithoutConsumingRestartBudget(t)
	assertStoppedServiceLeaseExpiryLatchesWhenQuiescenceIsUnconfirmed(t)
}

func TestServiceAcceptanceStabilityWindowOrdering(t *testing.T) {
	assertPortlessServiceExecutionAcknowledgementStartsStabilityWindow(t)
	assertServiceStabilityWindowUsesDurableTimestampAndAliveNode(t)
}
