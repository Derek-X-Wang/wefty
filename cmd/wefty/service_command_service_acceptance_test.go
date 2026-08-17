//go:build service_acceptance

package main

import "testing"

func TestServiceAcceptanceCLIContract(t *testing.T) {
	assertServiceCLIContractOverJobKeyedL1Routes(t)
}
