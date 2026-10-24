//go:build service_acceptance && (darwin || linux)

package agent

import "testing"

func TestServiceAcceptanceSeatRequestCutOffSL1(t *testing.T) {
	TestSeatRequestCutOffWaitKeepsOneDialogAndSeat(t)
}
