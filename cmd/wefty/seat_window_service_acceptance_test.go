//go:build service_acceptance

package main

import "testing"

func TestServiceAcceptanceSeatApprovalAP4(t *testing.T) {
	TestSeatApprovalNoCLIOrControlSocketAnswer(t)
}
