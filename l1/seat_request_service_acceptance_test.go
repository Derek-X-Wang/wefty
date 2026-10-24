//go:build service_acceptance

package l1

import "testing"

// #1143 owns SL-1/SL-2 and RM-1/RM-2, with secondary RM-3 evidence.
// Runtime and native-dialog proof remains in the attended #1154 lane.
func TestServiceAcceptanceSeatRequestSL1SL2RM1RM2RM3(t *testing.T) {
	t.Run("unpolled call bounded", TestSeatRequestUnpolledAlwaysEnds)
	t.Run("poll and admission race", TestSeatRequestPollCannotRefuseRoomWinner)
	t.Run("allowed windows", TestSeatRequestWindowRefusesBeforeDialog)
	t.Run("bad cleanup isolated", TestSeatRequestBadCleanupDoesNotBlockOtherCalls)
	t.Run("slow first image", TestSeatRequestSlowFirstImagePullKeepsOriginalDeadline)
	t.Run("credential scrub and retention", TestSeatCallFinalCredentialErasedAndRetentionFollowsSeat)
	t.Run("once-live receipt retention", TestSeatCallOnceLiveSurvivesReceiptPrune)
	t.Run("durable replay", TestSeatRequestCallReplayAndCallerFence)
	t.Run("approval stays node-only", TestSeatApprovalRoutesRemainNodeOnly)
	t.Run("room and shared rollover", TestSeatRequestFlowSharedRolloverRoomAndDedicatedCap)
	t.Run("last-room atomic creation", TestSeatRequestLosingRoomCreatesNoComputer)
	t.Run("original deadline", TestSeatRequestDeadlineIncludesDispatchDelay)
	t.Run("credential and denial", TestSeatRequestReadyCredentialReplayAndRefusedCleanup)
	t.Run("failed new Computer", TestSeatRequestFailedNewComputerUsesVerifiedRemoval)
	t.Run("runtime failure cleanup and latch", TestSeatRequestRuntimeFailureNewRemovalAndExistingLatch)
	t.Run("existing partial cleanup", TestSeatRequestRefusalWaitsForExistingSeatCleanup)
	t.Run("once-live Computer", TestSeatRequestOnceLiveComputerSurvivesLaterSeatFailure)
	t.Run("stale start refusal", TestSeatRequestStaleStartRefusalCannotEndLiveSeat)
}
