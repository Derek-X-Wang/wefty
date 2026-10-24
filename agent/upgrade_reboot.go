package agent

import (
	"context"
	"github.com/Derek-X-Wang/wefty/internal/agentinstall"
	"github.com/Derek-X-Wang/wefty/internal/hostboot"
	"github.com/Derek-X-Wang/wefty/l1"
)

// Only a registered primary on a positively different system boot may attest
// that prior-boot runtimes no longer exist. Evidence must independently settle;
// unavailable or unaccounted storage keeps retirement pending without a deadline.
func (a *Agent) reconcileRebootRetirees(ctx context.Context, store agentinstall.Store, in l1.NodeGenerationInventory) error {
	s := a.session
	if in.NodeID != s.registration.NodeID {
		return upgradeControlConflict("agent: reboot inventory node differs")
	}
	if !hostboot.ValidIdentity(s.registration.SystemBootID) || s.retirementEvidenceSettled == nil {
		return nil
	}
	currentPrimary := false
	for _, g := range in.Generations {
		if g.Role == l1.NodeGenerationPrimary && g.AgentGenerationID == s.registration.AgentGenerationID && g.BootSessionID == s.registration.BootSessionID {
			currentPrimary = true
		}
	}
	if !currentPrimary {
		return nil
	}
	catalog, err := store.Read()
	if err != nil {
		return err
	}
	if catalog.Primary != s.registration.AgentGenerationID || catalog.PendingReceipt != "" {
		return nil
	}
	for _, g := range in.Generations {
		local, present := catalog.Generations[g.AgentGenerationID]
		if !present || local.Role != "retiring" || local.Process == nil || !hostboot.ValidIdentity(g.Registration.SystemBootID) || hostboot.SameBoot(g.Registration.SystemBootID, s.registration.SystemBootID) {
			continue
		}
		if !hostboot.SameBoot(local.Process.SystemBootID, g.Registration.SystemBootID) || local.Process.BootSessionID != g.BootSessionID {
			return upgradeControlConflict("agent: prior-boot retirement binding differs")
		}
		if g.Role != l1.NodeGenerationRetiring && g.Role != l1.NodeGenerationClosed {
			continue
		}
		state, err := store.Manager.State(ctx, local.Label)
		if err != nil {
			return err
		}
		if state.Running {
			continue
		}
		pending := g.ResidentAttempts
		if a.outbox != nil {
			var known bool
			pending, known, err = a.outbox.rebootRetainedResidents(ctx, g)
			if err != nil {
				return err
			}
			if !known {
				continue
			}
		}
		settled, err := s.retirementEvidenceSettled(ctx, g, pending)
		if err != nil {
			return err
		}
		if !settled {
			continue
		}
		if g.Role == l1.NodeGenerationRetiring {
			err = s.client.CloseRetiringGeneration(ctx, s.registration.NodeID, g.AgentGenerationID, l1.CloseNodeGenerationRequest{BootSessionID: g.BootSessionID, PromotionOperationID: g.PromotionOperationID, ResidentAttempts: g.ResidentAttempts, RuntimesReaped: true, EvidenceSettled: true})
			if err != nil {
				return err
			}
			observed, err := s.client.NodeGenerations(ctx, s.registration.NodeID)
			if err != nil {
				return err
			}
			for _, closed := range observed.Generations {
				if closed.AgentGenerationID == g.AgentGenerationID && closed.Role == l1.NodeGenerationClosed {
					g = closed
				}
			}
		}
		if g.Role != l1.NodeGenerationClosed {
			return upgradeControlConflict("agent: reboot retirement closure unverified")
		}
		if err = store.PublishClosedGeneration(g); err != nil {
			return err
		}
	}
	return nil
}

// Reconcile only stored evidence whose immutable identity matches the frozen
// L1 roster. An empty process map after reboot cannot account for shared rows.
// Publication/custody barriers still run before L1's Lease/completion barrier.
func (outbox *evidenceOutbox) rebootRetainedResidents(ctx context.Context, g l1.NodeGeneration) ([]l1.ResidentAttempt, bool, error) {
	identities := map[string]l1.ResidentAttempt{}
	for _, r := range g.ResidentAttempts {
		if r.BootSessionID != g.BootSessionID || r.AuthorityGeneration != g.AuthorityGeneration {
			return nil, false, upgradeControlConflict("agent: reboot resident authority differs")
		}
		identities[r.AttemptID] = r
	}
	rows, err := outbox.spool.db.QueryContext(ctx, `SELECT attempt_id,job_id,fencing_token FROM spool_attempts WHERE owner_generation=? AND (result_json IS NOT NULL OR incomplete_json IS NOT NULL OR EXISTS(SELECT 1 FROM spool_events e WHERE e.attempt_id=spool_attempts.attempt_id))`, g.AgentGenerationID)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	pending := []l1.ResidentAttempt{}
	known := true
	for rows.Next() {
		var attempt, job, fence string
		if err = rows.Scan(&attempt, &job, &fence); err != nil {
			return nil, false, err
		}
		r, ok := identities[attempt]
		if !ok {
			known = false
			continue
		}
		if r.JobID != job || r.FencingToken != fence {
			return nil, false, upgradeControlConflict("agent: retained reboot evidence identity differs")
		}
		pending = append(pending, r)
	}
	return pending, known, rows.Err()
}
