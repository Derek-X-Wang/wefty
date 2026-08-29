//go:build service_acceptance

package main

import "testing"

func TestServiceAcceptanceComputerStorageCLIRealRoutesAndProvenance(t *testing.T) {
	assertComputerStorageCLIOverRealL1AndHelperSeams(t)
	assertComputerStorageCLIWaitPathsOverHelperSeam(t)
	assertComputerStorageCLIAdversarialRows(t)
	assertComputerCLIUsageForeignIDAndRoutePaginationErrors(t)
}
