//go:build service_acceptance && (darwin || linux)

package agent

import "testing"

func TestServiceAcceptanceFabricNamespaceReservation(t *testing.T) {
	assertPublishedPortFailureComesFromFabricNamespace(t)
}

func TestServiceAcceptancePortlessSkipsReachabilityMachinery(t *testing.T) {
	assertPortlessServiceSkipsFabricProxyProbeAndDeadline(t)
}

func TestServiceAcceptanceReadinessLossNeverKills(t *testing.T) {
	assertPostStartupProbeLossWithdrawsAndRecoversWithoutKilling(t)
}
