//go:build service_acceptance

package main

import "testing"

func TestServiceAcceptanceComputerSubmissionAndOriginCLI(t *testing.T) {
	assertComputerSubmissionAndOriginCLIOverRealRoutes(t)
}
