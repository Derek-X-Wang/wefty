//go:build service_acceptance

package l1

import "testing"

// TestCustodyServiceAcceptance is the named contract lane for permanent
// pre-write taint, verified no-grant import, and descendant removal truth.
func TestCustodyServiceAcceptance(t *testing.T) {
	t.Run("export race and non-upgrading attestation", TestCustodyExportCommitsTaintBeforeBytesAndAttestationNeverUpgrades)
	t.Run("verified import and inherited descendant taint", TestCustodyImportValidatesReceiptCreatesNoGrantIdentityAndTaintsDescendants)
	t.Run("assertion-derived export receipt", TestCustodyExportReceiptMutationRowsFail)
}
