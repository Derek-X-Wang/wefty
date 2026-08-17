//go:build service_acceptance

package l1

import "testing"

func TestServiceAcceptanceBoundedL1LogRetention(t *testing.T) {
	assertServiceLogByteRetentionAndDerivedJSONL(t)
	assertServiceLogWatermarksPreserveStrictPerStreamContinuity(t)
	assertServiceLogAgeRetentionRunsFromReconcile(t)
	assertServiceAttemptSummariesAreBounded(t)
}
