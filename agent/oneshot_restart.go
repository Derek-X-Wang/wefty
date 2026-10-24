package agent

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
	wr "github.com/Derek-X-Wang/wefty/runner"
)

// ErrAgentShutdown identifies an intentional node agent shutdown. Supervision
// failures must retain their own cause and cannot request a shutdown restart.
var ErrAgentShutdown = errors.New("node agent shutdown")

func controlledRestartCause(cause error) bool {
	return errors.Is(cause, ErrAgentShutdown) || errors.Is(cause, errAttemptDirectiveRestart) || errors.Is(cause, errAttemptDirectiveCancel) || errors.Is(cause, errAttemptDirectiveStop)
}

type restartInterruption struct {
	reason l1.InterruptionReason
	key    string
}

// InterruptRestartableForShutdown is used only by the node agent signal path.
// Latch under the resident lock so a claim already in flight cannot miss it.
func (a *Agent) InterruptRestartableForShutdown() {
	a.session.drainOnce.Do(func() { close(a.session.drainRequested) })
	a.session.claimMu.Lock()
	defer a.session.claimMu.Unlock()
	a.session.shutdownRestartable = true
	for _, resident := range a.session.resident {
		if resident.restartable {
			resident.cancel(ErrAgentShutdown)
		}
	}
}

func agentInitiatedProcessStop(result contract.ProcessResult) bool {
	return result.TerminationCause == contract.TerminationCauseAgent || result.TerminationCause == contract.TerminationCauseGuardian || result.TerminationInitiator == contract.TerminationCauseAgent || result.TerminationInitiator == contract.TerminationCauseGuardian
}

func (lifecycle *attemptLifecycle) restartCompletionRequest(claim l1.Claim, result contract.ProcessResult, evidence wr.ReapEvidence) l1.CompletionRequest {
	request := completionRequest(claim, result, evidence)
	var interruption *restartInterruption
	// The fence winner identifies the stop that actually began termination.
	// A later directive may carry a key, but cannot relabel an earlier failure.
	if cause := lifecycle.fenceCause.Load(); cause != nil {
		if errors.Is(*cause, ErrAgentShutdown) {
			interruption = &restartInterruption{reason: l1.InterruptionAgentShutdown}
		} else if errors.Is(*cause, errAttemptDirectiveRestart) {
			interruption = lifecycle.interruption.Load()
		}
	}
	if !claim.Job.Spec.Restartable || interruption == nil || (evidence != wr.ReapEvidenceAttempt && evidence != wr.ReapEvidenceNoRuntime) {
		return request
	}
	initiated := agentInitiatedProcessStop(result)
	if !initiated || result.SpawnError != nil || result.RuntimeFailure != nil || result.OutputError != "" || result.LimitReached != "" || result.OOM || result.DiskExhausted {
		return request
	}
	request.InterruptionReason, request.InterruptionRequestKey = interruption.reason, interruption.key
	return request
}

const restartableResultsDirectory = ".wefty-attempts"

func restartableResultPath(attemptID string) string {
	return filepath.Join(restartableResultsDirectory, attemptID)
}

// Keep results outside exec storage: cleanup can outlive the command, and a
// predecessor can overlap a successor. Neither may publish into the other's file.
func prepareRestartableResults(spec contract.JobSpec, attemptID string) error {
	if !validRunMailboxSegment(attemptID) {
		return fmt.Errorf("invalid restartable Attempt ID %q", attemptID)
	}
	root, err := os.OpenRoot(spec.Execution.HandoffDirectory)
	if err != nil {
		return err
	}
	defer root.Close()
	if err := ensureMailboxSubdirectory(root, restartableResultsDirectory); err != nil {
		return err
	}
	parent, err := root.OpenRoot(restartableResultsDirectory)
	if err != nil {
		return err
	}
	defer parent.Close()
	return ensureMailboxSubdirectory(parent, attemptID)
}

// Freeze the canonical completion before persistence. A later cancel can win
// in L1, but cannot change an already durable idempotent completion body.
func (lifecycle *attemptLifecycle) deliveryCompletionRequest(claim l1.Claim, result contract.ProcessResult, evidence wr.ReapEvidence) l1.CompletionRequest {
	if !claim.Job.Spec.Restartable {
		request := completionRequest(claim, result, evidence)
		if lifecycle.dependencies.upgradeEvidence != nil && claim.Job.Spec.Class == contract.JobClassService && claim.Job.Spec.Kind == contract.JobKindProcess {
			lifecycle.completion.CompareAndSwap(nil, &request)
			return *lifecycle.completion.Load()
		}
		return request
	}
	if request := lifecycle.completion.Load(); request != nil {
		return *request
	}
	request := lifecycle.restartCompletionRequest(claim, result, evidence)
	lifecycle.completion.CompareAndSwap(nil, &request)
	return *lifecycle.completion.Load()
}
