package main

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/l1"
)

// A crash after durable interruption needs no upgrade-local restart list. L1
// retains the queued Job; receipt cleanup only restores owned admission.
func TestUpgradeRestartableInterruptedCleanupRealL1(t *testing.T) {
	f := newUpgradeFixture(t)
	h := newNodeCLIHarness(t)
	ctx := t.Context()
	policy := l1.NodePolicy{Tags: []string{"recovery"}, MaxOneshotSlots: 2}
	registered := h.registerNode(t, f.node.NodeID, "recovery", policy)
	registration := registered.node.NodeRegistration
	registration.Capabilities[l1.CapabilityOneshotRestartable] = true
	registration.CapabilityRevision++
	var err error
	registered.node, err = h.store.RegisterNode(ctx, registered.identity, registration, policy, true)
	if err != nil {
		t.Fatal(err)
	}
	spec := nodeCLIJobSpec("interrupted-upgrade", contract.JobClassOneShot, "recovery")
	spec.Restartable = true
	job, _, err := h.store.CreateJob(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	first, err := h.store.ClaimJob(ctx, registered.identity.NodeID, registered.node.NodeID, registered.node.BootSessionID, contract.JobClassOneShot)
	if err != nil || first == nil {
		t.Fatalf("claim: %+v %v", first, err)
	}
	op := *f.operations
	op.readNode = func(ctx context.Context, _ connprofile.Profile) (l1.Node, error) {
		return h.clients.getNode(ctx, registered.node.NodeID)
	}
	op.setClaims = func(ctx context.Context, _ connprofile.Profile, r l1.NodeIntentRequest) (l1.Node, error) {
		return h.clients.setNodeClaims(ctx, registered.node.NodeID, r)
	}
	paths, _ := resolveSetupPaths(f.home)
	profile := f.profile(t)
	receipt, err := beginUpgradeDrain(ctx, op, paths, profile, registered.node, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	request := l1.InterruptRestartableAttemptRequest{JobID: job.JobID, BootSessionID: registered.node.BootSessionID, IntentRevision: receipt.Revision, IdempotencyKey: "durable-stop", Reason: l1.InterruptionUpgrade}
	if _, err := h.clients.interruptRestartableAttempt(ctx, registered.node.NodeID, first.Lease.AttemptID, request); err != nil {
		t.Fatal(err)
	}
	_, err = h.store.CompleteAttempt(ctx, registered.identity.NodeID, job.JobID, first.Lease.AttemptID, l1.CompletionRequest{FencingToken: first.Lease.FencingToken, IdempotencyKey: "stopped", Result: l1.ProcessResult{Signal: "SIGTERM", TerminationCause: contract.TerminationCauseAgent}, InterruptionReason: l1.InterruptionUpgrade, InterruptionRequestKey: request.IdempotencyKey, RuntimeQuiescenceEvidence: l1.RuntimeQuiescenceAttempt})
	if err != nil {
		t.Fatal(err)
	}
	if claim, err := h.store.ClaimJob(ctx, registered.identity.NodeID, registered.node.NodeID, registered.node.BootSessionID, contract.JobClassOneShot); err != nil || claim != nil {
		t.Fatalf("claimed before receipt recovery: %+v %v", claim, err)
	}
	if err := recoverUpgradeDrain(ctx, op, paths, profile, io.Discard); err != nil {
		t.Fatal(err)
	}
	next, err := h.store.ClaimJob(ctx, registered.identity.NodeID, registered.node.NodeID, registered.node.BootSessionID, contract.JobClassOneShot)
	if err != nil || next == nil || next.Job.JobID != job.JobID || next.Lease.AttemptID == first.Lease.AttemptID || next.Lease.FencingToken == first.Lease.FencingToken || next.Job.RestartRecovery.Count != 1 {
		t.Fatalf("old agent failed ordinary queued recovery: %+v %v", next, err)
	}
	assertNoUpgradeDrainReceipt(t, f)
}
