//go:build service_acceptance

package l1

import "testing"

func TestServiceAcceptanceComputerTakeoverAudit(t *testing.T) {
	assertComputerTakeoverAuditContract(t)
}
