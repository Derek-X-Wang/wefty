//go:build service_acceptance && (darwin || linux)

package agent

import "testing"

func TestServiceAcceptanceClassScopedSpoolBudgets(t *testing.T) {
	t.Run("default raw-payload ceiling is 96 MiB", func(t *testing.T) {
		if DefaultLogSpoolMaxBytes != 64<<20 || DefaultServiceLogSpoolMaxBytes != 32<<20 ||
			DefaultLogSpoolMaxBytes+DefaultServiceLogSpoolMaxBytes != 96<<20 {
			t.Fatalf("class budgets = %d + %d", DefaultLogSpoolMaxBytes, DefaultServiceLogSpoolMaxBytes)
		}
	})
	t.Run("budgets are class aggregates and service pressure rings across siblings", assertLogSpoolBudgetsAreClassScopedAndServiceRingEvictsAcrossAttempts)
	t.Run("service eviction declares gaps independently per stream", assertLogSpoolServiceRingDeclaresPerStreamGaps)
	t.Run("oversized service events become truthful gaps", assertLogSpoolServiceOversizedEventBecomesGap)
	t.Run("eviction and triggering insertion are one transaction", assertLogSpoolServiceEvictionRollsBackWhenTriggeringInsertFails)
	t.Run("service console mirroring is best effort while one-shot stays fatal", assertConsoleMirrorFailurePolicyByClass)
}
