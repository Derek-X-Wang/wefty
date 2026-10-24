//go:build service_acceptance

package main

import "testing"

func TestServiceAcceptanceSeatRequestCLI(t *testing.T) {
	t.Run("additional typed outcomes", TestSeatRequestAdditionalRefusalsRealBinary)
	t.Run("state folders", TestSeatCredentialStateRoots)
	t.Run("folder owner", TestSeatCredentialRejectsForeignOwner)
	t.Run("macOS fallback", TestSeatCredentialMacFallbackAndReadSearch)
	t.Run("discover and name network", TestSeatRequestHelpAndMissingNetworkRealBinary)
	t.Run("lost initial reply", TestSeatRequestLostPostReplyReplaysPrivateReceipt)
	t.Run("typed refusals and private credential", TestSeatRequestRefusalsAndCredentialFromRealBinary)
}
