//go:build service_acceptance

package l1

import "testing"

func TestServiceAcceptanceAttemptCredential(t *testing.T) {
	t.Run("spawns and reads own children", TestAttemptCredentialSpawnsAndReadsOwnChildren)
	t.Run("refused after lease loss", TestAttemptCredentialIsRefusedAfterLeaseLoss)
	t.Run("supersession keeps job-level children", TestAttemptCredentialSupersessionKeepsJobLevelChildren)
	t.Run("refused after authority loss with a live lease", TestAttemptCredentialIsRefusedAfterAuthorityLossWithALiveLease)
	t.Run("authority predicates", TestAttemptCredentialAuthorityPredicates)
	t.Run("cannot act on another job", TestAttemptCredentialCannotActOnAnotherJob)
	t.Run("replay stays inside its own parent", TestAttemptCredentialReplayStaysInsideItsOwnParent)
	t.Run("spawn depth exceeded is a non-retryable conflict", TestSpawnDepthExceededIsANonRetryableConflictOnTheWire)
	t.Run("reaches no operator route", TestAttemptCredentialReachesNoOperatorRoute)
	t.Run("spawn depth is capped", TestAttemptCredentialSpawnDepthIsCapped)
	t.Run("absent from public job projections", TestAttemptCredentialIsAbsentFromPublicJobProjections)
	t.Run("refuses unknown bearer", TestAttemptCredentialRefusesUnknownBearerAndRecordsRootSubmitter)
	t.Run("child listing is a client contract", TestChildJobListingIsAvailableToClientPrincipals)
}
