//go:build rolling_upgrade_acceptance

package l1

import "testing"

// Real SQLite with injected time/boot/registration. No agent crash or reboot
// occurs here: this proves authority fencing and honest absence of results.
func TestB1SQLiteFailureBoundaries(t *testing.T) {
	t.Run("reboot-only-restartable-recovers", TestGenerationRebootFencesRetireesAndRecoversOnlyRestartable)
	t.Run("expired-retiree-not-masked-by-heartbeat", TestNodeGenerationRetiringHeartbeatCannotMaskPrimaryOrExpiredLease)
	t.Run("restartable-recovery-across-promotion", TestNodeGenerationRestartableRecoveryAcrossPromotion)
}
