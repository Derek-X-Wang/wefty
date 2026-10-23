package agent

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/Derek-X-Wang/wefty/l1"
)

// retirementAttempt retains local proof after a claim worker releases residency.
// Absence from the running map never proves reap or evidence delivery.
type retirementAttempt struct {
	jobID, fencingToken     string
	reaped, evidenceSettled bool
	checkEvidence           func(context.Context) (bool, error)
}

func (session *agentSession) isRetiring() bool {
	select {
	case <-session.retiringRequested:
		return true
	default:
		return false
	}
}

func (session *agentSession) setSession(state LifecycleState, delay time.Duration, err error) {
	if session.isRetiring() {
		if session.isDraining() {
			state = LifecycleDraining
		} else {
			state = LifecycleRetiring
		}
	}
	session.observer.setSession(state, delay, err)
}

// Retire joins any in-flight claim and acknowledges the authenticated L1
// demotion. It never initiates promotion, Drain, shutdown, or a deadline.
// Identical retries are safe. A primary cannot retire by local request alone.
func (a *Agent) Retire(ctx context.Context) (l1.NodeGeneration, error) {
	retiring, err := a.session.observeGeneration(ctx)
	if err != nil {
		return l1.NodeGeneration{}, err
	}
	if !retiring {
		return l1.NodeGeneration{}, errors.New("agent: L1 has not retired this generation")
	}
	a.session.retirementMu.Lock()
	defer a.session.retirementMu.Unlock()
	generation := *a.session.retiringGeneration
	generation.ResidentAttempts = slices.Clone(generation.ResidentAttempts)
	generation.RetirementProtocolVersions = slices.Clone(generation.RetirementProtocolVersions)
	return generation, nil
}

func (session *agentSession) observeGeneration(ctx context.Context) (bool, error) {
	session.retirementMu.Lock()
	defer session.retirementMu.Unlock()
	if session.isRetiring() {
		return true, nil
	}
	if session.registration.AgentGenerationID == "" {
		return false, errors.New("agent: no Node agent generation identity")
	}
	inventory, err := session.client.NodeGenerations(ctx, session.registration.NodeID)
	if err != nil {
		return false, err
	}
	for _, generation := range inventory.Generations {
		if generation.AgentGenerationID != session.registration.AgentGenerationID {
			continue
		}
		if generation.BootSessionID != session.registration.BootSessionID {
			return false, errors.New("agent: generation boot session mismatch")
		}
		if generation.Role == l1.NodeGenerationPrimary {
			return false, nil
		}
		if generation.Role != l1.NodeGenerationRetiring || generation.PromotionOperationID == "" || generation.SelectedProtocol != 1 {
			return false, errors.New("agent: generation has no retiring authority")
		}
		// claim holds claimMu through the RPC and admission record. Joining it
		// resolves the response before we freeze local claims or acknowledge L1.
		session.claimMu.Lock()
		for _, resident := range generation.ResidentAttempts {
			local := session.retirementAttempts[resident.AttemptID]
			if resident.BootSessionID != session.registration.BootSessionID || resident.AuthorityGeneration != generation.AuthorityGeneration || (local != nil && (local.jobID != resident.JobID || local.fencingToken != resident.FencingToken)) {
				session.claimMu.Unlock()
				return false, fmt.Errorf("agent: retiring resident %s lacks exact local ownership", resident.AttemptID)
			}
			if local == nil {
				// L1 can commit a claim whose response never reached us. After
				// joining claimMu, no admission record proves we never started
				// that Attempt. Do not fabricate its credentials or execute it.
				// L1 close still waits for its authoritative Lease end.
				session.retirementAttempts[resident.AttemptID] = &retirementAttempt{jobID: resident.JobID, fencingToken: resident.FencingToken, reaped: true, evidenceSettled: true}
			}
		}
		session.retiringGeneration = &generation
		close(session.retiringRequested)
		session.claimMu.Unlock()
		session.setSession(LifecycleRetiring, 0, nil)
		return true, nil
	}
	return false, errors.New("agent: generation is absent from L1 inventory")
}

// trackRetirementEvidence retains a query, rather than the natural-exit
// callback's snapshot. Terminal directives, Lease loss and later recovery all
// reconcile the same logs/completion/mailbox/result publication state.
func (session *agentSession) trackRetirementEvidence(attemptID string, check func(context.Context) (bool, error)) {
	session.claimMu.Lock()
	defer session.claimMu.Unlock()
	if record := session.retirementAttempts[attemptID]; record != nil {
		record.checkEvidence = check
	}
}

// closeRetirement is the shared #1176 seam. The local half is positive runtime
// reap plus delivered evidence (or an exact durable custody proof). The close
// transaction independently enforces accepted completion or an ended Lease;
// no local clock or evidence acknowledgment substitutes for that L1 check.
func (session *agentSession) closeRetirement(ctx context.Context) (bool, error) {
	session.retirementMu.Lock()
	generation := *session.retiringGeneration
	generation.ResidentAttempts = slices.Clone(generation.ResidentAttempts)
	session.retirementMu.Unlock()
	session.claimMu.Lock()
	if len(session.residentJobID) != 0 {
		session.claimMu.Unlock()
		return false, nil
	}
	type proof struct {
		identity        l1.ResidentAttempt
		evidenceSettled bool
		check           func(context.Context) (bool, error)
	}
	proofs := make([]proof, 0, len(session.retirementAttempts))
	for attemptID, record := range session.retirementAttempts {
		if !record.reaped {
			session.claimMu.Unlock()
			return false, nil
		}
		proofs = append(proofs, proof{identity: l1.ResidentAttempt{JobID: record.jobID, AttemptID: attemptID, FencingToken: record.fencingToken, BootSessionID: generation.BootSessionID, AuthorityGeneration: generation.AuthorityGeneration}, evidenceSettled: record.evidenceSettled, check: record.checkEvidence})
	}
	session.claimMu.Unlock()
	evidenceSettled := true
	var pending []l1.ResidentAttempt
	for _, proof := range proofs {
		settled := proof.evidenceSettled
		if proof.check != nil {
			var err error
			settled, err = proof.check(ctx)
			if err != nil {
				return false, err
			}
		}
		evidenceSettled = evidenceSettled && settled
		if !settled {
			pending = append(pending, proof.identity)
		}
	}
	if session.retirementEvidenceSettled != nil {
		var err error
		evidenceSettled, err = session.retirementEvidenceSettled(ctx, generation, pending)
		if err != nil {
			return false, err
		}
	}
	if !evidenceSettled {
		return false, nil
	}
	err := session.client.CloseRetiringGeneration(ctx, session.registration.NodeID, generation.AgentGenerationID, l1.CloseNodeGenerationRequest{
		BootSessionID: generation.BootSessionID, PromotionOperationID: generation.PromotionOperationID,
		ResidentAttempts: generation.ResidentAttempts, RuntimesReaped: true, EvidenceSettled: true,
	})
	return err == nil, err
}
