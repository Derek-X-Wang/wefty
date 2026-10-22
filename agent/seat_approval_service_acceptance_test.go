//go:build service_acceptance && (darwin || linux)

package agent

import "testing"

// AP-5 and AP-6 run entirely with the fake dialog, real L1 and injected clocks.
// The native macOS alert is reserved for the attended acceptance lane.
func TestServiceAcceptanceSeatApprovalAP5AP6(t *testing.T) {
	t.Run("AP-5", TestSeatApprovalUnansweredRefusedWithInjectedClock)
	t.Run("AP-6", TestSeatApprovalWindowClockAndClose)
}
