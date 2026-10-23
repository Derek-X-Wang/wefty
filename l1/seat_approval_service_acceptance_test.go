//go:build service_acceptance

package l1

import "testing"

func TestServiceAcceptanceSeatApprovalAuthorityAP4(t *testing.T) {
	t.Run("node identity", TestSeatApprovalWindowAuthorityAndExpiry)
	t.Run("route structure", TestSeatApprovalRoutesRemainNodeOnly)
	t.Run("answer wait cap", TestSeatApprovalAnswerWaitCapped)
}
