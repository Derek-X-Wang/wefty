//go:build service_acceptance

package l1

import "testing"

// The portable reconfiguration lane uses injected clocks and exact durable
// receipts. Linux helper mechanics and real lease timing remain in the tagged
// realtiming lane; no unavailable hardware result is promoted to PASS here.
func TestServiceAcceptanceComputerReconfigurationContract(t *testing.T) {
	t.Run("reimage identity and same-generation data", TestComputerReimagePreservesIdentityStorageAndIntent)
	t.Run("running reimage internal quiescence", TestRunningComputerReimageUsesInternalQuiescence)
	t.Run("grow identity and attempt preservation", TestComputerGrowPreservesJobAttemptAndStorageGeneration)
	t.Run("grow receipt mutation matrix 20 of 20", TestComputerGrowReceiptFailsEveryNegativeRow)
	t.Run("insufficient disk explicit recovery", TestComputerGrowInsufficientDiskLatchesUntilExplicitRestart)
	t.Run("dead-node abort escape hatch", TestReconfigurationAbortRequiresDeadBoundNodeAndLeavesExplicitRestart)
}
