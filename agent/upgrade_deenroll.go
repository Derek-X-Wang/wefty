package agent

import (
	"context"
	"errors"
	"reflect"
	"time"

	"github.com/Derek-X-Wang/wefty/internal/agentinstall"
	"github.com/Derek-X-Wang/wefty/l1"
)

// DefinitiveDeEnrollmentRefusal requires an exact-request post-ledger outcome.
// Authentication, routing, generic conflicts and malformed replies prove nothing
// about whether the operation committed and must retain its attestation.
func DefinitiveDeEnrollmentRefusal(err error, nodeID string, request l1.DeEnrollNodeGenerationRequest) bool {
	var refused *ProtocolError
	return errors.As(err, &refused) && refused.StatusCode == 409 && l1.IsDeEnrollmentNotCommitted(refused.APIError, nodeID, request)
}

// deEnrollGeneration is a named inverse-enrollment operation. Local evidence
// is checked by this process, never asserted by the CLI or inferred from L1.
func (a *Agent) deEnrollGeneration(ctx context.Context, store agentinstall.Store, r agentinstall.ControlRequest) (l1.NodeGenerationInventory, error) {
	var request l1.DeEnrollNodeGenerationRequest
	if err := decodeUpgradeControlPayload(r.Payload, &request, "de-enrollment"); err != nil {
		return l1.NodeGenerationInventory{}, err
	}
	s := a.session
	if request.OperationID != r.ID || request.PrimaryGenerationID != r.GenerationID || request.PrimaryBootSessionID != r.BootSessionID {
		return l1.NodeGenerationInventory{}, upgradeControlConflict("agent: de-enrollment envelope differs")
	}
	request.RuntimesReaped, request.EvidenceSettled = true, true
	prior, err := store.ReadDeEnrollment()
	if err != nil {
		return l1.NodeGenerationInventory{}, err
	}
	if prior != nil {
		if prior.Request != request {
			return l1.NodeGenerationInventory{}, upgradeControlConflict("agent: de-enrollment payload changed")
		}
		if prior.Result != nil {
			return *prior.Result, nil
		}
	} else {
		catalog, err := store.CheckMaintenance(nil)
		if err != nil {
			return l1.NodeGenerationInventory{}, err
		}
		if catalog == nil || catalog.NodeID != r.NodeID || catalog.Primary != r.GenerationID {
			return l1.NodeGenerationInventory{}, upgradeControlConflict("agent: de-enrollment requires the quiet recorded primary")
		}
		node, err := s.heartbeat(ctx)
		if err != nil {
			return l1.NodeGenerationInventory{}, err
		}
		if node.NodeID == "" || node.AgentGenerationID == "" || node.BootSessionID == "" {
			return l1.NodeGenerationInventory{}, errors.New("agent: malformed de-enrollment heartbeat reply")
		}
		if node.NodeID != r.NodeID || node.AgentGenerationID != r.GenerationID || node.BootSessionID != r.BootSessionID || node.AuthorityGeneration != request.AuthorityGeneration || node.ClaimsEnabled || node.IntentRevision != request.IntentRevision || node.IntentReason != request.IntentReason || request.IntentReason != "wefty maintenance "+r.ID {
			return l1.NodeGenerationInventory{}, upgradeControlConflict("agent: de-enrollment primary or admission fence changed")
		}
		// Match graceful installed shutdown without closing drainRequested:
		// this process must remain alive to persist the L1 result. Admission is
		// already closed, and claimMu joins any response admitted before CAS.
		s.claimMu.Lock()
		for _, resident := range s.resident {
			if resident.restartable {
				resident.cancel(ErrAgentShutdown)
			}
		}
		s.claimMu.Unlock()
		for {
			s.claimMu.Lock()
			pending := len(s.residentJobID) != 0
			s.claimMu.Unlock()
			if !pending {
				break
			}
			timer := s.clock.NewTimer(100 * time.Millisecond)
			select {
			case <-ctx.Done():
				stopTimer(timer)
				return l1.NodeGenerationInventory{}, ctx.Err()
			case <-timer.C():
			}
		}
		// Join in-flight claims, then demand positive reap/finalization for every
		// local Attempt. Closed L1 admission prevents a subsequent claim admission.
		s.claimMu.Lock()
		if len(s.residentJobID) != 0 {
			s.claimMu.Unlock()
			return l1.NodeGenerationInventory{}, upgradeControlConflict("agent: primary still has resident work")
		}
		records := make(map[string]retirementAttempt, len(s.retirementAttempts))
		for id, record := range s.retirementAttempts {
			records[id] = *record
		}
		s.claimMu.Unlock()
		for _, record := range records {
			if !record.reaped || !record.finished {
				return l1.NodeGenerationInventory{}, upgradeControlConflict("agent: primary lacks positive runtime reap")
			}
			settled := record.evidenceSettled
			if record.checkEvidence != nil {
				settled, err = record.checkEvidence(ctx)
				if err != nil {
					return l1.NodeGenerationInventory{}, err
				}
			}
			if !settled {
				return l1.NodeGenerationInventory{}, upgradeControlConflict("agent: primary evidence is unsettled")
			}
		}
		// A fresh process has no runtime map for older local work. Even an
		// empty spool row needs positive runtime reap; no events is not proof.
		if a.outbox != nil {
			rows, err := a.outbox.spool.db.QueryContext(ctx, `SELECT attempt_id FROM spool_attempts WHERE owner_generation=?`, r.GenerationID)
			if err != nil {
				return l1.NodeGenerationInventory{}, err
			}
			unaccounted := false
			for rows.Next() {
				var id string
				if err := rows.Scan(&id); err != nil {
					rows.Close()
					return l1.NodeGenerationInventory{}, err
				}
				_, known := records[id]
				unaccounted = unaccounted || !known
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				return l1.NodeGenerationInventory{}, err
			}
			if unaccounted {
				return l1.NodeGenerationInventory{}, upgradeControlConflict("agent: primary spool contains work without positive runtime reap")
			}
		}
		if s.retirementEvidenceSettled != nil {
			safe, e := s.retirementEvidenceSettled(ctx, l1.NodeGeneration{AgentGenerationID: r.GenerationID}, nil)
			if e != nil {
				return l1.NodeGenerationInventory{}, e
			}
			if !safe {
				return l1.NodeGenerationInventory{}, upgradeControlConflict("agent: primary shared evidence remains unsettled")
			}
		}
		if err = store.PrepareDeEnrollment(r.NodeID, request); err != nil {
			return l1.NodeGenerationInventory{}, err
		}
	}
	in, err := s.client.DeEnrollNodeGeneration(ctx, r.NodeID, request)
	if err != nil {
		if DefinitiveDeEnrollmentRefusal(err, r.NodeID, request) {
			return in, errors.Join(err, store.AbortDeEnrollment(request))
		}
		return in, err
	}
	// Persist success before replying, shutdown, or local installation cleanup.
	if err = store.CompleteDeEnrollment(in); err != nil {
		return in, err
	}
	saved, err := store.ReadDeEnrollment()
	if err != nil {
		return in, err
	}
	if saved == nil || saved.Result == nil || !reflect.DeepEqual(in, *saved.Result) {
		return in, upgradeControlConflict("agent: de-enrollment result was not retained")
	}
	return in, nil
}
