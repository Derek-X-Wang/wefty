//go:build rolling_upgrade_acceptance && (darwin || linux)

package agentinstall

import "testing"

// These reuse production-called installation seams and disposable directories.
// No launchctl command runs. Manager observations are explicitly fake-launchd.
func TestB1FakeLaunchdRebootAndDelayedCleanup(t *testing.T) {
	t.Run("three-generations-reboot-primary-only", TestThreeGenerationsKeepLabelsExecutablesAndRebootOnlyPrimary)
	t.Run("reconcile-post-promotion-reboot", TestRebootReconcilesCommittedPrimaryWithoutOldReadiness)
	t.Run("wait-for-positive-closure-and-process-exit", TestCleanupNeedsPositiveL1ClosureAndExitedProcess)
	t.Run("cleanup-publication-interruptions", TestCleanupResumesFromReceiptAndRefusesChangedOwnedFiles)
	t.Run("delayed-control-evidence", TestFinishedGenerationCleanupAfterControlExchangeAndRetirementAck)
}
