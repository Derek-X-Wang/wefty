//go:build service_acceptance && (darwin || linux)

package agent

import "testing"

func TestServiceAcceptanceDurableAgentEvidence(t *testing.T) {
	t.Run("completion is durable before delivery begins", assertAttemptPersistsCompletionBeforeDelivery)
	t.Run("expired logs and completion replay without a boot crash-loop", assertAgentBootDeliversDurableLateEvidenceWithoutCrashLoop)
	t.Run("pending evidence does not block registration", assertPendingEvidenceDoesNotBlockRegistration)
	t.Run("a transient poison attempt cannot starve later evidence", assertEvidenceRecoveryIsolatesTransientPoisonAttempt)
	t.Run("permanent replay rejection becomes a truthful gap", assertEvidenceRecoveryReplacesPermanentReplayRejectionWithGap)
	t.Run("authority loss seals an incomplete tombstone and releases raw evidence", assertEvidenceRecoverySealsAuthorityLossIncomplete)
	t.Run("stable-node lock precedes spool ownership", assertAgentTakesStableNodeLockBeforeOpeningSpool)
}
