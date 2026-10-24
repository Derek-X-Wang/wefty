//go:build service_acceptance && (darwin || linux)

package agent

import "testing"

func TestServiceAcceptanceSeatRequestCutOffSL1(t *testing.T) {
	t.Run("cut off wait", TestSeatRequestCutOffWaitKeepsOneDialogAndSeat)
	t.Run("answer post outage", TestSeatApprovalAnswerBlipRetriesWithoutSecondDialog)
	t.Run("nil intent", TestSeatRequestNilIntentAfterDialog)
}
