//go:build service_acceptance

package l1

import "testing"

// This tagged row intentionally reuses the injected-clock, assertion-derived
// integration proofs. It adds no wall-clock wait and cannot report success if
// the cap, resume/race, receipt-mutation, or copy-removal rows did not run.
func TestServiceAcceptanceComputerColdBackupContract(t *testing.T) {
	t.Run("cap and immutable publication", TestComputerBackupRunningQuiescesPublishesAndResumesSameRevision)
	t.Run("stale intent never resumes", TestComputerBackupStopAndFailureRacesNeverResumeStaleIntent)
	t.Run("receipt authority mutations", TestComputerBackupReceiptAuthorityMutationsFailClosed)
	t.Run("explicit and composite removal", TestComputerBackupExplicitPruneAndRemovalSupersession)
}
