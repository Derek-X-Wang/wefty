package agent

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/l1"
)

var errUpgradeServiceQuiescence = errors.New("agent: service stopped for rolling upgrade")

// UpgradeServiceQuiescence binds the selective barrier to the activation
// operation and exact pre-stop inventory. Persist that inventory before calling
// QuiesceUpgradeServices; persist the returned proof before promotion/recovery.
// This is local evidence, not L1 promotion or restart authority.
type UpgradeServiceQuiescence struct {
	IntentRevision    int64                  `json:"intent_revision"`
	IntentReason      string                 `json:"intent_reason"`
	OperationID       string                 `json:"operation_id"`
	NodeID            string                 `json:"node_id"`
	AgentGenerationID string                 `json:"agent_generation_id"`
	BootSessionID     string                 `json:"boot_session_id"`
	Services          []l1.NodeActiveAttempt `json:"services"`
}

// QuiesceUpgradeServices is the pre-promotion service barrier. The installation
// caller must close L1 admission and persist its service inventory first. The
// claim mutex joins any in-flight response. Only exact process services are
// canceled; one-shots retain their contexts, Guardians, Leases and resources.
// Timeout or failure leaves service admission closed for safe replay.
func (a *Agent) QuiesceUpgradeServices(ctx context.Context, request UpgradeServiceQuiescence) (UpgradeServiceQuiescence, error) {
	s := a.session
	if err := lockMutexContext(ctx, &s.upgradeServiceMu); err != nil {
		return UpgradeServiceQuiescence{}, err
	}
	defer s.upgradeServiceMu.Unlock()
	if request.OperationID == "" || request.NodeID != s.registration.NodeID || request.AgentGenerationID == "" || request.AgentGenerationID != s.registration.AgentGenerationID || request.BootSessionID != s.registration.BootSessionID {
		return UpgradeServiceQuiescence{}, upgradeControlConflict("agent: upgrade service identity mismatch")
	}
	if s.upgradeServiceResumed[request.OperationID] {
		return UpgradeServiceQuiescence{}, upgradeControlConflict("agent: resumed upgrade operation cannot quiesce services again")
	}
	request.Services = slices.Clone(request.Services)
	selected := make(map[string]l1.NodeActiveAttempt, len(request.Services))
	s.claimMu.Lock()
	replaying := s.upgradeServiceOperation != ""
	if s.upgradeServiceOperation != "" && (s.upgradeServiceOperation != request.OperationID || s.upgradeServiceIntent.IntentRevision != request.IntentRevision || s.upgradeServiceIntent.Reason != request.IntentReason || !slices.Equal(s.upgradeServiceInventory, request.Services)) {
		s.claimMu.Unlock()
		return UpgradeServiceQuiescence{}, upgradeControlConflict("agent: another service quiescence operation is pending")
	}
	for _, service := range request.Services {
		record := s.retirementAttempts[service.AttemptID]
		_, duplicate := selected[service.JobID]
		if duplicate || service.Class != contract.JobClassService || service.Kind != contract.JobKindProcess || service.BootSessionID != request.BootSessionID || record == nil || record.jobID != service.JobID || record.class != service.Class || record.kind != service.Kind {
			s.claimMu.Unlock()
			return UpgradeServiceQuiescence{}, upgradeControlConflict(fmt.Sprintf("agent: service %q lacks exact local upgrade ownership", service.JobID))
		}
		selected[service.JobID] = service
	}
	// A missing service must refuse before any cancellation, including a claim
	// admitted before its worker has installed the resident context.
	for attemptID, record := range s.retirementAttempts {
		if _, admitted := s.residentJobID[record.jobID]; admitted && record.class == contract.JobClassService {
			if resident := s.resident[record.jobID]; (resident != nil && resident.attemptID != attemptID) || (resident == nil && record.finished) {
				continue
			}
			if service, found := selected[record.jobID]; !found || service.AttemptID != attemptID {
				s.claimMu.Unlock()
				return UpgradeServiceQuiescence{}, upgradeControlConflict("agent: upgrade inventory omits a resident service")
			}
		}
	}
	// Join older terminal process-service evidence too. Otherwise a removed
	// or naturally stopped service omitted from active L1 inventory could leave
	// an obsolete delivery query holding retirement open after promotion.
	checks := make([]string, 0, len(s.retirementAttempts))
	for attemptID, record := range s.retirementAttempts {
		if record.class == contract.JobClassService && record.kind == contract.JobKindProcess {
			checks = append(checks, attemptID)
		}
	}
	if a.outbox != nil {
		for _, attemptID := range checks {
			a.outbox.holdUpgradeAttempt(attemptID)
		}
	}
	s.upgradeServiceOperation = request.OperationID
	s.upgradeServiceIntent = l1.NodeIntentRequest{IntentRevision: request.IntentRevision, Reason: request.IntentReason}
	s.upgradeServiceInventory = slices.Clone(request.Services)
	for _, service := range selected {
		s.keepUpgradeServiceLease(service.AttemptID, a.renewalInterval)
	}
	for jobID, service := range selected {
		if resident := s.resident[jobID]; resident != nil && resident.attemptID == service.AttemptID {
			resident.cancel(errUpgradeServiceQuiescence)
		}
	}
	s.claimMu.Unlock()
	for {
		settled := true
		var pending error
		for _, attemptID := range checks {
			s.claimMu.Lock()
			record := s.retirementAttempts[attemptID]
			_, admitted := s.residentJobID[record.jobID]
			reaped, delivered, check := record.reaped, record.evidenceSettled, record.checkEvidence
			finished, reapErr, finalErr := record.finished, record.reapErr, record.finalizationErr
			recheck, recoverEvidence := record.recheckRuntime, record.recoverEvidence
			jobID := record.jobID
			s.claimMu.Unlock()
			if admitted || !finished && !reaped {
				settled = false
				pending = upgradeServiceFailure(jobID, attemptID, "finalization", true, errors.New("service worker has not joined"))
				continue
			}
			if !reaped {
				if replaying && recheck != nil {
					bounded, cancel := context.WithTimeout(ctx, s.client.operationTimeout)
					receipt, err := recheck(bounded)
					cancel()
					_, err = verifiedRuntimeReap(jobID, runtimeReapOutcome{receipt: receipt, err: err})
					if err != nil {
						return UpgradeServiceQuiescence{}, upgradeServiceFailure(jobID, attemptID, "reap", false, err)
					}
					s.claimMu.Lock()
					record.reaped = true
					record.reapErr = nil
					s.claimMu.Unlock()
					s.retainRemovalReap(jobID, receipt)
				} else {
					if reapErr == nil {
						reapErr = upgradeControlConflict("positive runtime reap is unavailable")
					}
					return UpgradeServiceQuiescence{}, upgradeServiceFailure(jobID, attemptID, "reap", false, reapErr)
				}
			}
			if !delivered && check != nil {
				var err error
				delivered, err = check(ctx)
				if err != nil {
					return UpgradeServiceQuiescence{}, s.upgradeServiceEvidenceFailure(jobID, attemptID, err)
				}
			}
			if !delivered && finished {
				if replaying && recoverEvidence != nil {
					bounded, cancel := context.WithTimeout(ctx, s.client.operationTimeout)
					err := recoverEvidence(bounded)
					cancel()
					if err != nil {
						return UpgradeServiceQuiescence{}, s.upgradeServiceEvidenceFailure(jobID, attemptID, err)
					}
					if check != nil {
						delivered, err = check(ctx)
					}
					if err != nil {
						return UpgradeServiceQuiescence{}, s.upgradeServiceEvidenceFailure(jobID, attemptID, err)
					}
					s.claimMu.Lock()
					record.finalizationErr = nil
					s.claimMu.Unlock()
					finalErr = nil
				}
				if !delivered {
					if finalErr == nil {
						finalErr = errors.New("service evidence has not been accepted")
						if a.outbox != nil {
							state := a.outbox.spool.inspectCompletion(ctx, attemptID)
							if state.InspectionError != "" {
								finalErr = errors.New(state.InspectionError)
							} else {
								finalErr = fmt.Errorf("service evidence %s: %s", state.State, state.Reason)
							}
						}
					}
					return UpgradeServiceQuiescence{}, s.upgradeServiceEvidenceFailure(jobID, attemptID, finalErr)
				}
			}
			if !delivered {
				pending = upgradeServiceFailure(jobID, attemptID, "evidence", true, errors.New("service evidence has not settled"))
			}
			if delivered {
				s.stopUpgradeServiceLease(attemptID)
			}
			settled = settled && delivered
		}
		if settled {
			s.claimMu.Lock()
			s.upgradeServicesSettled = true
			for _, attemptID := range checks {
				s.retirementAttempts[attemptID].recheckRuntime = nil
				s.retirementAttempts[attemptID].recoverEvidence = nil
			}
			s.claimMu.Unlock()
			s.stopUpgradeServiceLeases(false)
			if a.outbox != nil {
				for _, attemptID := range checks {
					a.outbox.releaseUpgradeAttempt(attemptID)
				}
			}
			return request, nil
		}
		timer := s.clock.NewTimer(s.claimInterval)
		select {
		case <-ctx.Done():
			stopTimer(timer)
			var failure *UpgradeServiceQuiescenceError
			if errors.As(pending, &failure) {
				copy := *failure
				copy.Cause = ctx.Err()
				return UpgradeServiceQuiescence{}, &copy
			}
			return UpgradeServiceQuiescence{}, ctx.Err()
		case <-timer.C():
		}
	}
}

// ResumeUpgradeServiceClaims lets pre-promotion recovery release the local
// service gate. L1 must still name this exact primary; demoted generations can
// never use rollback to reopen admission. This does not restore L1 intent.
func (a *Agent) ResumeUpgradeServiceClaims(ctx context.Context, operationID string) error {
	s := a.session
	if err := lockMutexContext(ctx, &s.upgradeServiceMu); err != nil {
		return err
	}
	defer s.upgradeServiceMu.Unlock()
	inventory, err := s.client.NodeGenerations(ctx, s.registration.NodeID)
	if err != nil {
		return err
	}
	if inventory.NodeID == "" {
		return errors.New("agent: malformed generation inventory reply")
	}
	primary := slices.ContainsFunc(inventory.Generations, func(g l1.NodeGeneration) bool {
		return g.Role == l1.NodeGenerationPrimary && g.AgentGenerationID == s.registration.AgentGenerationID && g.BootSessionID == s.registration.BootSessionID
	})
	s.claimMu.Lock()
	defer s.claimMu.Unlock()
	if primary && !s.isRetiring() && operationID != "" && s.upgradeServiceOperation == "" && s.upgradeServiceResumed[operationID] {
		return nil
	}
	if !primary || s.isRetiring() || !s.upgradeServicesSettled || operationID == "" || s.upgradeServiceOperation != operationID {
		return upgradeControlConflict("agent: service admission resume lacks primary operation authority")
	}
	if s.upgradeServiceResumed == nil {
		s.upgradeServiceResumed = map[string]bool{}
	}
	s.upgradeServiceResumed[operationID] = true
	s.upgradeServiceOperation = ""
	s.upgradeServiceIntent = l1.NodeIntentRequest{}
	s.upgradeServiceInventory = nil
	s.upgradeServicesSettled = false
	return nil
}
