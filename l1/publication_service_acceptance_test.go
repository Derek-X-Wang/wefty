//go:build service_acceptance

package l1

import "testing"

func TestServiceAcceptanceFencedPublicationAndDatabaseNoOps(t *testing.T) {
	assertAttemptPublicationFencedMutationAndTrueNoOps(t)
}

func TestServiceAcceptancePublicationApplicabilityAndLifecycleClearing(t *testing.T) {
	assertAttemptPublicationRejectsPortlessAndOmitsReadiness(t)
	assertAttemptPublicationClearsAcrossLifecycleAndRejectsTerminalReplay(t)
}

func TestServiceAcceptanceReadyRequiresCurrentActiveAuthority(t *testing.T) {
	assertOperatorReadyRequiresCurrentActiveAuthority(t)
}
