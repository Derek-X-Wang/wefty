//go:build service_acceptance

package main

import "testing"

func TestServiceAcceptanceComputerLifecycleCLIRealRoutes(t *testing.T) {
	TestComputerCLIRealRoutesDefaultsLifecycleAndReplay(t)
	TestComputerCLIMutationNegativeReceiptFailsEveryRow20Of20(t)
}
