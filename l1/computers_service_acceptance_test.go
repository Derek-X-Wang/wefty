//go:build service_acceptance

package l1

import "testing"

// Ticket #172's acceptance row proves that Computer removal uses the ordinary
// durable cleanup directive and releases its sole service Slot only after the
// authenticated agent acknowledgement is finalized.
func TestServiceAcceptanceComputerRemovalReleasesOccupancy(t *testing.T) {
	assertComputerRemovalDirectiveCompletionReleasesSlot(t)
}
