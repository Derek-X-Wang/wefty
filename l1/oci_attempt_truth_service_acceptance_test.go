//go:build service_acceptance

package l1

import (
	"context"
	"testing"

	"github.com/Derek-X-Wang/wefty/contract"
)

func TestServiceAcceptanceOCIAgentStartAndRuntimeLossTruth(t *testing.T) {
	t.Run("renewal log and completion cannot imply Started", TestOCIAttemptRequiresStartedForRunning)
	t.Run("pre-Started runtime loss requeues within budget", TestOCIPrestartRuntimeLossRequeuesOnceAndExhaustsBudget)
	t.Run("post-Started runtime loss is terminal for one-shot", func(t *testing.T) {
		h := newIntegrationHarness(t, map[string][]string{"node-1": {}})
		registerOCIFixtureNode(t, h)
		job := createOCIFixtureJob(t, h, "oci-post-start-runtime-loss", contract.JobClassOneShot)
		claim := claimOCIFixture(t, h, contract.JobClassOneShot)
		if _, err := h.store.ObserveAttemptImage(context.Background(), "agent", job.JobID, claim.Lease.AttemptID, testImageObservation(claim.Lease.FencingToken)); err != nil {
			t.Fatal(err)
		}
		if _, err := h.store.StartAttempt(context.Background(), "agent", job.JobID, claim.Lease.AttemptID, StartedRequest{FencingToken: claim.Lease.FencingToken}); err != nil {
			t.Fatal(err)
		}
		completed, err := h.store.CompleteAttempt(context.Background(), "agent", job.JobID, claim.Lease.AttemptID, CompletionRequest{FencingToken: claim.Lease.FencingToken, IdempotencyKey: "post-start-runtime-loss", Result: ProcessResult{RuntimeFailure: &contract.RuntimeFailure{Code: contract.RuntimeFailureUnavailable, Message: "helper lost"}}})
		if err != nil || completed.State != contract.JobFailed || completed.CurrentAttemptID != claim.Lease.AttemptID {
			t.Fatalf("post-Started runtime loss job=%+v err=%v", completed, err)
		}
	})
}
