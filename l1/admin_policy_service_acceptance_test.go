//go:build service_acceptance

package l1

import "testing"

// Ticket #176 proves person-stable Fabric identity, local challenge bootstrap,
// admin-only CAS, final-admin protection, and atomic device-bearing L1 audit.
func TestServiceAcceptancePersonIdentityAndAdminBootstrap(t *testing.T) {
	assertAdminBootstrapAndPolicyContract(t)
}
