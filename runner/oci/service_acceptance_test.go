//go:build service_acceptance

package oci

import "testing"

func TestServiceAcceptanceOCIAgentAdapterEvidenceTruth(t *testing.T) {
	t.Run("L1 Started refusal is terminal authority evidence", TestAdapterRequiresAuthoritativeStartedBeforeLocalPromotion)
	t.Run("post-Started runtime loss retains runtime failure arm", TestAdapterMapsLogsExitSignalOOMAndRuntimeLoss)
	t.Run("image absence is permanent pre-start evidence", TestAdapterPreservesImageUnavailableAsPermanentSpawnEvidence)
}
