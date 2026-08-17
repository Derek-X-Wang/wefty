//go:build service_acceptance

package l1

import "testing"

func TestServiceAcceptanceDurablePostAuthorityEvidence(t *testing.T) {
	assertDurablePostAuthorityEvidence(t)
	assertEvidenceGapContract(t)
	assertAcceptedCompletionPersistsProcessResult(t)
	assertSupersededAttemptEvidenceProvenance(t)
}
