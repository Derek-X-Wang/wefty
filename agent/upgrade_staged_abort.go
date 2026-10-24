package agent

import (
	"context"
	"errors"

	"github.com/Derek-X-Wang/wefty/internal/agentinstall"
	"github.com/Derek-X-Wang/wefty/internal/hostboot"
	"github.com/Derek-X-Wang/wefty/l1"
)

// abortStaged is a named recovery operation, never an arbitrary node call.
// A staged candidate has never held execution authority. L1 separately fences
// its empty resident set, the current primary, the operation and admission.
func (a *Agent) abortStaged(ctx context.Context, store agentinstall.Store, r agentinstall.ControlRequest) (l1.NodeGenerationInventory, error) {
	var payload agentinstall.StagedAbortControl
	if err := decodeUpgradeControlPayload(r.Payload, &payload, "staged abort"); err != nil {
		return l1.NodeGenerationInventory{}, err
	}
	s := a.session
	c, err := store.Read()
	if err != nil {
		return l1.NodeGenerationInventory{}, err
	}
	candidate, ok := c.Generations[payload.CandidateGenerationID]
	request := payload.Request
	abort := request.Abort
	if !ok || candidate.Role != "" || candidate.PreparationOperation != r.ID || c.Primary != s.registration.AgentGenerationID || c.NodeID != s.registration.NodeID || abort == nil || abort.OperationID != r.ID || request.PromotionOperationID != r.ID || request.BootSessionID != candidate.CandidateBootSessionID || abort.PrimaryGenerationID != s.registration.AgentGenerationID || abort.PrimaryBootSessionID != s.registration.BootSessionID || !hostboot.ValidIdentity(s.registration.SystemBootID) || !hostboot.ValidIdentity(candidate.StartBootID) {
		return l1.NodeGenerationInventory{}, upgradeControlConflict("agent: staged abort lacks exact staged candidate and current primary authority")
	}
	if c.PendingReceipt != "" {
		pending, err := store.ReadActivationReceipt(c.PendingReceipt)
		if err != nil {
			return l1.NodeGenerationInventory{}, err
		}
		if pending.Committed || pending.L1PromotionRevision != 0 || pending.L1OperationID != r.ID || pending.Candidate != candidate.ID || pending.ExpectedPrimary != c.Primary || pending.L1CatalogRevision != abort.CatalogRevision || pending.IntentRevision != abort.IntentRevision || pending.IntentReason != abort.IntentReason {
			return l1.NodeGenerationInventory{}, upgradeControlConflict("agent: staged abort activation fence changed")
		}
	}
	node, err := s.heartbeat(ctx)
	if err != nil {
		return l1.NodeGenerationInventory{}, err
	}
	if node.NodeID == "" || node.AgentGenerationID == "" || node.BootSessionID == "" {
		return l1.NodeGenerationInventory{}, errors.New("agent: malformed staged abort heartbeat")
	}
	if node.AgentGenerationID != s.registration.AgentGenerationID || node.BootSessionID != s.registration.BootSessionID || node.AuthorityGeneration != abort.AuthorityGeneration || node.ClaimsEnabled || node.IntentRevision != abort.IntentRevision || node.IntentReason != abort.IntentReason {
		return l1.NodeGenerationInventory{}, upgradeControlConflict("agent: staged abort admission or boot fence changed")
	}
	if err = s.client.CloseRetiringGeneration(ctx, s.registration.NodeID, candidate.ID, request); err != nil {
		return l1.NodeGenerationInventory{}, err
	}
	return s.client.NodeGenerations(ctx, s.registration.NodeID)
}
