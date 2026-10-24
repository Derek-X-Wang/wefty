//go:build service_acceptance

package main

import "testing"

func TestServiceAcceptanceSeatRequestCLI(t *testing.T) {
	t.Run("discover and name network", TestSeatRequestHelpAndMissingNetworkRealBinary)
	t.Run("lost initial reply", TestSeatRequestLostPostReplyReplaysPrivateReceipt)
	t.Run("typed refusals and private credential", TestSeatRequestRefusalsAndCredentialFromRealBinary)
}
