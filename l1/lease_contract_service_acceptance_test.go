//go:build service_acceptance

package l1

import "testing"

func TestServiceAcceptanceLeaseTTLAndAttemptScopedDirectives(t *testing.T) {
	assertLeaseTTLAndAttemptScopedDirectives(t)
}
