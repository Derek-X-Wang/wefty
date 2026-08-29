//go:build service_acceptance

package l1

import "testing"

// This tagged row reuses the injected-clock and assertion-derived integration
// proofs. It cannot pass if restore identity/authority, clone custody, or any
// of the twenty negative receipt mutations were skipped.
func TestServiceAcceptanceComputerRestoreAndCloneContract(t *testing.T) {
	t.Run("restore generation and authority", TestComputerRestorePublishesExactlyOneStoppedGenerationAndKeepsSource)
	t.Run("restore removal supersession", TestComputerRemovalSupersedesAndAttestsRestorePrecommittedBackup)
	t.Run("clone identity and expansion", TestComputerCloneCreatesNewStoppedIdentityWithoutGrantsAndExpands)
	t.Run("clone custody removal", TestComputerCloneCustodyRemovalReducesThenCoordinatedRemovalVerifies)
	t.Run("twenty receipt mutations", TestComputerStorageCopyReceiptMutationRowsFailTwentyOfTwenty)
	t.Run("node session and root fences", TestComputerStorageCopyAcknowledgementRequiresBoundNodeSessionAndRoot)
}
