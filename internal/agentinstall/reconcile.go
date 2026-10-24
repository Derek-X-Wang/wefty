package agentinstall

import (
	"context"
	"encoding/json"
	"slices"

	"github.com/Derek-X-Wang/wefty/l1"
)

// ReconcilePromoted publishes only a transition positively observed at L1. It
// does not promote, revive a process, or convert old readiness into survival.
// It is also used before installed-agent startup after a reboot.
func (s Store) ReconcilePromoted(ctx context.Context, in l1.NodeGenerationInventory) error {
	c, err := s.Read()
	if err != nil {
		return err
	}
	if c.PendingReceipt == "" {
		return nil
	}
	r, err := s.ReadActivationReceipt(c.PendingReceipt)
	if err != nil {
		return err
	}
	if in.NodeID != c.NodeID || in.CatalogRevision != r.L1CatalogRevision+1 {
		return &VerificationRefusal{Message: "pending promotion L1 inventory differs"}
	}
	primary, old := false, false
	for _, g := range in.Generations {
		if g.AgentGenerationID == r.Candidate {
			primary = g.Role == l1.NodeGenerationPrimary && g.BootSessionID == r.Processes[r.Candidate].BootSessionID && g.PromotionOperationID == r.L1OperationID
		}
		if g.AgentGenerationID == r.ExpectedPrimary {
			old = (g.Role == l1.NodeGenerationRetiring || g.Role == l1.NodeGenerationClosed) && g.BootSessionID == r.Processes[r.ExpectedPrimary].BootSessionID && g.PromotionOperationID == r.L1OperationID && slices.Equal(g.ResidentAttempts, asL1Residents(r.Residents[r.ExpectedPrimary]))
		}
	}
	if !primary || !old {
		return &VerificationRefusal{Message: "L1 has not committed this pending promotion"}
	}
	// Keep the exact successful reply before opening candidate startup. A CLI
	// retry can consume it after the predecessor exits or after reply loss.
	if r.PromotionRequest != nil {
		if err := s.BindPromotionRequest(r.ID, *r.PromotionRequest); err != nil {
			return err
		}
		payload, err := json.Marshal(in)
		if err != nil {
			return err
		}
		requestPayload, err := json.Marshal(r.PromotionRequest)
		if err != nil {
			return err
		}
		request := ControlRequest{ID: r.L1OperationID, NodeID: c.NodeID, GenerationID: r.ExpectedPrimary, BootSessionID: r.Processes[r.ExpectedPrimary].BootSessionID, Action: "promote_generation", Payload: requestPayload}
		if err = s.RetainPromotionResult(ControlResult{Request: request, Payload: payload}); err != nil {
			return err
		}
	}
	return s.commitPromotion(ctx, r.ID, r.L1OperationID, r.SystemBootID, in.CatalogRevision, false)
}
func asL1Residents(in []ResidentAttempt) []l1.ResidentAttempt {
	out := make([]l1.ResidentAttempt, len(in))
	for i, a := range in {
		out[i] = l1.ResidentAttempt(a)
	}
	return out
}

// BindPromotionRequest makes reply-loss resolution available to installed-agent
// startup before a request may be sent. A changed replay cannot rewrite it.
func (s Store) BindPromotionRequest(id string, request l1.PromoteNodeGenerationRequest) error {
	root, close, err := s.open(false)
	if err != nil {
		return err
	}
	defer close()
	var r ActivationReceipt
	if !validID(id) {
		return &VerificationRefusal{Message: "invalid activation receipt ID"}
	}
	if err = readPrivate(root, activationName(id), &r); err != nil {
		return err
	}
	if r.ID != id || r.L1OperationID != request.OperationID || r.Candidate != request.CandidateGenerationID || r.ExpectedPrimary != request.ExpectedPrimaryGenerationID || r.IntentRevision != request.IntentRevision || r.L1CatalogRevision != request.ExpectedCatalogRevision || r.Processes[r.Candidate].BootSessionID != request.CandidateBootSessionID || r.Processes[r.ExpectedPrimary].BootSessionID != request.ExpectedPrimaryBootSessionID || r.Protocol != int(request.SelectedProtocol) || request.CatalogRevision != r.L1CatalogRevision+1 || !slices.Equal(asL1Residents(r.Residents[r.ExpectedPrimary]), request.ResidentAttempts) || !slices.Equal(r.Retiring, request.RetiringGenerationIDs) {
		return &VerificationRefusal{Message: "promotion request differs from activation receipt"}
	}
	if r.PromotionRequest != nil {
		a, _ := json.Marshal(r.PromotionRequest)
		b, _ := json.Marshal(request)
		if string(a) != string(b) {
			return &VerificationRefusal{Message: "promotion request payload changed"}
		}
		return nil
	}
	r.PromotionRequest = &request
	return writePrivate(root, activationName(id), r)
}

// AbortPromotion settles only positively uncommitted staged intent. L1 closure
// is an abort acknowledgment, never a reversal of a committed primary.
func (s Store) AbortPromotion(in l1.NodeGenerationInventory) error {
	c, err := s.Read()
	if err != nil || c.PendingReceipt == "" {
		return err
	}
	return s.AbortPreparedPromotion(c.PendingReceipt, in)
}

// AbortPreparedPromotion releases a local pending activation only after L1
// positively closed its never-authorized candidate under the exact operation.
// It never restores a database or undoes a committed promotion.
func (s Store) AbortPreparedPromotion(id string, in l1.NodeGenerationInventory) error {
	root, close, err := s.open(false)
	if err != nil {
		return err
	}
	defer close()
	c, err := s.read(root)
	if err != nil {
		return err
	}
	var r ActivationReceipt
	if !validID(id) {
		return &VerificationRefusal{Message: "invalid abort activation ID"}
	}
	if err = readPrivate(root, activationName(id), &r); err != nil {
		return err
	}
	if r.ID != id || r.Committed || r.L1PromotionRevision != 0 || c.Primary != r.ExpectedPrimary || in.NodeID != c.NodeID || in.CatalogRevision != r.L1CatalogRevision || (c.PendingReceipt != id && !(c.PendingReceipt == "" && r.Aborted)) || len(r.SuppressedPlists) != 0 {
		return &VerificationRefusal{Message: "activation abort would undo committed or changed ownership"}
	}
	primary, closed := false, false
	for _, g := range in.Generations {
		if g.AgentGenerationID == r.ExpectedPrimary {
			primary = g.Role == l1.NodeGenerationPrimary && g.BootSessionID == r.Processes[r.ExpectedPrimary].BootSessionID && g.AuthorityGeneration == r.ExpectedAuthorityGeneration
		}
		if g.AgentGenerationID == r.Candidate {
			closed = g.Role == l1.NodeGenerationClosed && g.BootSessionID == r.Processes[r.Candidate].BootSessionID && g.PromotionOperationID == r.L1OperationID && len(g.ResidentAttempts) == 0
		}
	}
	if !primary || !closed {
		return &VerificationRefusal{Message: "activation abort lacks positive staged closure and original primary"}
	}
	candidate, present := c.Generations[r.Candidate]
	if !present || candidate.Role != "" || (candidate.PreparationOperation != "" && candidate.PreparationOperation != r.L1OperationID) || (candidate.AbortOperation != "" && candidate.AbortOperation != r.L1OperationID) {
		return &VerificationRefusal{Message: "activation abort candidate ownership changed"}
	}
	if !r.Aborted {
		r.Aborted = true
		if err = writePrivate(root, activationName(id), r); err != nil {
			return err
		}
		if s.afterPromotionWrite != nil {
			if err = s.afterPromotionWrite("activation_aborted"); err != nil {
				return err
			}
		}
	}
	if c.PendingReceipt != "" {
		candidate.PreparationOperation = r.L1OperationID
		candidate.AbortOperation = r.L1OperationID
		c.Generations[candidate.ID] = candidate
		c.PendingReceipt = ""
		c.Revision++
		if err = writePrivate(root, catalogName, c); err != nil {
			return err
		}
		if s.afterPromotionWrite != nil {
			return s.afterPromotionWrite("abort_pending_cleared")
		}
	}
	return nil
}
