//go:build service_acceptance

package l1

import "testing"

// Ticket #177 proves person grants, CAS/replay/audit, node/boot/generation
// snapshots, explicit install acknowledgement, and pending/completed revoke.
func TestServiceAcceptanceComputerGrantPolicyDistribution(t *testing.T) {
	assertComputerGrantPolicyDistribution(t)
}
