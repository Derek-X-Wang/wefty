package agent

import (
	"context"
	"errors"
	"fmt"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
)

// UpgradeServiceQuiescenceError describes one exact blocked Attempt. Pending
// means its worker is still finalizing; a failure may be retried with the same
// operation/inventory after its cause is resolved. Neither reopens admission.
type UpgradeServiceQuiescenceError struct {
	JobID     string
	AttemptID string
	Phase     string
	Pending   bool
	Cause     error
}

func (e *UpgradeServiceQuiescenceError) Error() string {
	return fmt.Sprintf("upgrade service %s attempt %s phase %s (pending=%t): %v", e.JobID, e.AttemptID, e.Phase, e.Pending, e.Cause)
}
func (e *UpgradeServiceQuiescenceError) Unwrap() error { return e.Cause }
func upgradeServiceFailure(job, attempt, phase string, pending bool, cause error) error {
	var failure *UpgradeServiceQuiescenceError
	if errors.As(cause, &failure) {
		return cause
	}
	return &UpgradeServiceQuiescenceError{JobID: job, AttemptID: attempt, Phase: phase, Pending: pending, Cause: cause}
}

// Completion replay remains authoritative after renewal ends. If it cannot
// settle the evidence, include the operator stop that ended renewal in the
// barrier's recoverable state, without replacing or sealing the original bytes.
func (s *agentSession) upgradeServiceEvidenceFailure(job, attempt string, cause error) error {
	s.claimMu.Lock()
	var stopCause error
	if record := s.retirementAttempts[attempt]; record != nil && record.completionLease != nil {
		stopCause = record.completionLease.stopCause
	}
	s.claimMu.Unlock()
	if stopCause != nil {
		return &UpgradeServiceQuiescenceError{JobID: job, AttemptID: attempt, Phase: "lease", Cause: errors.Join(stopCause, cause)}
	}
	return upgradeServiceFailure(job, attempt, "evidence", false, cause)
}

func (s *agentSession) trackUpgradeServiceEvidence(attempt string, retry func(context.Context) error) {
	if s == nil {
		return
	}
	s.claimMu.Lock()
	settled := false
	if record := s.retirementAttempts[attempt]; record != nil && (retry != nil || record.reaped) {
		record.recoverEvidence = retry
		settled = retry == nil
	}
	s.claimMu.Unlock()
	if settled {
		s.stopUpgradeServiceLease(attempt)
	}
}
func (s *agentSession) trackUpgradeServiceRuntime(attempt string, retry func(context.Context) (workloadrunner.ReapReceipt, error)) {
	if s == nil {
		return
	}
	s.claimMu.Lock()
	defer s.claimMu.Unlock()
	if record := s.retirementAttempts[attempt]; record != nil && (retry != nil || record.reaped) {
		record.recheckRuntime = retry
	}
}

// Explicit upgrade replay uses the same immutable completion body and ordered
// spool, but preserves rejected bytes instead of sealing them as abandoned.
func (lifecycle *attemptLifecycle) recoverUpgradeServiceEvidence(ctx context.Context, claim l1.Claim) error {
	fail := func(phase string, err error) error {
		return upgradeServiceFailure(claim.Job.JobID, claim.Lease.AttemptID, phase, false, err)
	}
	outbox := lifecycle.dependencies.outbox
	if outbox == nil {
		return fail("evidence", errors.New("durable service evidence is unavailable"))
	}
	receipt := outbox.spool.inspectCompletion(ctx, claim.Lease.AttemptID)
	if receipt.InspectionError != "" {
		return fail("evidence", errors.New(receipt.InspectionError))
	}
	if receipt.State == "sealed_incomplete" || receipt.State == "dead_undecodable_completion" || receipt.State == "suppressed" || receipt.State == "withheld" {
		return fail("evidence", fmt.Errorf("%s: %s", receipt.State, receipt.Reason))
	}
	if receipt.State == "delivered" || lifecycle.completionAccepted.Load() {
		if receipt.EventCount == 0 {
			return nil
		}
	}
	request := lifecycle.completion.Load()
	if request == nil {
		return fail("initialization", errors.New("service completion has not been recorded"))
	}
	if receipt.State == "never_persisted" && !lifecycle.completionAccepted.Load() {
		if err := outbox.ensureAttempt(ctx, claim); err != nil {
			return fail("persistence", err)
		}
		if err := outbox.storeDurableCompletion(ctx, claim.Lease.AttemptID, durableCompletionOf(*request), lifecycle.dependencies.clock.Now()); err != nil {
			return fail("persistence", err)
		}
	}
	if err := outbox.waitForLateAppends(ctx, claim.Lease.AttemptID); err != nil {
		return fail("evidence", err)
	}
	attempt := logSpoolAttempt{jobID: claim.Job.JobID, attemptID: claim.Lease.AttemptID, fencingToken: claim.Lease.FencingToken, kind: claim.Job.Spec.Kind, class: claim.Job.Spec.Class}
	if err := outbox.recoverLogsRetaining(ctx, lifecycle.dependencies.client, attempt, true); err != nil {
		return fail("evidence", err)
	}
	if lifecycle.completionAccepted.Load() {
		return nil
	}
	failure := lifecycle.completeWithRetryMode(ctx, claim, *request, completionDeliveryUpgrade)
	if failure.err != nil {
		return fail("completion", failure.err)
	}
	return nil
}

func (a *Agent) verifyStoppedUpgradeService(ctx context.Context, claim l1.Claim) (workloadrunner.ReapReceipt, error) {
	verifier, ok := a.managedResource.(interface {
		verifyStoppedAttempt(context.Context, workloadrunner.AttemptAuthority) (workloadrunner.ReapReceipt, error)
	})
	if !ok {
		return workloadrunner.ReapReceipt{}, errors.New("exact process-group verification is unavailable")
	}
	return verifier.verifyStoppedAttempt(ctx, workloadAuthority(a.registration.NodeID, a.registration.BootSessionID, claim))
}

// This path runs before any runtime preflight or payload preparation. Its
// no-runtime proof comes from that ordering, not a canceled SQL operation.
func (lifecycle *attemptLifecycle) finalizeUnstartedUpgradeService(ctx context.Context, claim l1.Claim) (errorDestination, error) {
	receipt := workloadrunner.ReapReceipt{RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidenceNoRuntime, BootSessionID: lifecycle.dependencies.bootSessionID}
	if lifecycle.dependencies.runtimeReaped != nil {
		lifecycle.dependencies.runtimeReaped(claim.Job.JobID, receipt, nil)
	}
	lifecycle.deliveryCompletionRequest(claim, contract.ProcessResult{Signal: "terminated", TerminationCause: contract.TerminationCauseAgent}, workloadrunner.ReapEvidenceNoRuntime)
	bounded, cancel := context.WithTimeout(context.WithoutCancel(ctx), lifecycle.dependencies.client.operationTimeout)
	defer cancel()
	return errorDestinationUnclassified, lifecycle.recoverUpgradeServiceEvidence(bounded, claim)
}
