package main

import (
	"context"
	"time"

	"github.com/Derek-X-Wang/wefty/agent"
	"github.com/Derek-X-Wang/wefty/fabric"
	"github.com/Derek-X-Wang/wefty/internal/agentinstall"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/internal/fabricconfig"
	"github.com/Derek-X-Wang/wefty/internal/hostboot"
	"github.com/Derek-X-Wang/wefty/l1"
)

// This narrow pre-start adapter only resolves a previously committed operation.
// It opens no managed root, registers no boot, and starts no payload. The CLI
// never obtains this participant. Authority unavailable means startup stays fenced.
func reconcileInstalledActivation(ctx context.Context, store agentinstall.Store, profilePath string, boot string) error {
	c, err := store.Read()
	if err != nil || c.PendingReceipt == "" {
		return err
	}
	receipt, err := store.ReadActivationReceipt(c.PendingReceipt)
	if err != nil {
		return err
	}
	profile, _, err := connprofile.Resolve(profilePath, "")
	if err != nil {
		return err
	}
	if profile.Fabric != "plain" || profile.Identities.Node != c.NodeID {
		return &agentinstall.VerificationRefusal{Message: "activation recovery requires matching plain node profile"}
	}
	bounded, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	config, err := fabricconfig.SavedService(profile, connprofile.ParticipantNodeAgent, fabricconfig.Config{Mode: "plain", PlainFabricID: profile.PlainFabricID, Identity: fabric.Identity{NodeID: c.NodeID, Tags: []string{l1.DefaultAgentPrincipalTag}}}, nil)
	if err != nil {
		return err
	}
	participant, closeFabric, err := fabricconfig.Open(bounded, config)
	if closeFabric != nil {
		defer closeFabric()
	}
	if err != nil {
		return err
	}
	client, err := agent.NewClient(participant, profile.L1Address)
	if err != nil {
		return err
	}
	defer client.Close()
	inventory, err := client.NodeGenerations(bounded, c.NodeID)
	if err != nil {
		return err
	}
	if inventory.NodeID != c.NodeID {
		return &agentinstall.VerificationRefusal{Message: "activation inventory node differs"}
	}
	if receipt.PromotionRequest == nil && receipt.ExpectedAuthorityGeneration > 0 {
		request := l1.PromoteNodeGenerationRequest{OperationID: receipt.L1OperationID, ExpectedPrimaryGenerationID: receipt.ExpectedPrimary, ExpectedPrimaryBootSessionID: receipt.Processes[receipt.ExpectedPrimary].BootSessionID, ExpectedAuthorityGeneration: receipt.ExpectedAuthorityGeneration, CandidateGenerationID: receipt.Candidate, CandidateBootSessionID: receipt.Processes[receipt.Candidate].BootSessionID, ExpectedCatalogRevision: receipt.L1CatalogRevision, CatalogRevision: receipt.L1CatalogRevision + 1, IntentRevision: receipt.IntentRevision, SelectedProtocol: uint32(receipt.Protocol), RetiringGenerationIDs: receipt.Retiring, ResidentAttempts: []l1.ResidentAttempt{}}
		for _, a := range receipt.Residents[receipt.ExpectedPrimary] {
			request.ResidentAttempts = append(request.ResidentAttempts, l1.ResidentAttempt(a))
		}
		if err = store.BindPromotionRequest(receipt.ID, request); err != nil {
			return err
		}
		receipt.PromotionRequest = &request
	}
	for _, g := range inventory.Generations {
		if g.Role == l1.NodeGenerationPrimary && g.AgentGenerationID == receipt.Candidate && g.PromotionOperationID == receipt.L1OperationID && g.BootSessionID == receipt.Processes[receipt.Candidate].BootSessionID && inventory.CatalogRevision == receipt.L1CatalogRevision+1 {
			if receipt.PromotionRequest != nil {
				inventory, err = client.PromoteNodeGeneration(bounded, c.NodeID, *receipt.PromotionRequest)
				if err != nil {
					return err
				}
			}
			return store.ReconcilePromoted(bounded, inventory)
		}
	}
	if !hostboot.SameBoot(boot, receipt.SystemBootID) && receipt.PromotionRequest != nil {
		request := *receipt.PromotionRequest
		abort := l1.CloseNodeGenerationRequest{BootSessionID: request.CandidateBootSessionID, PromotionOperationID: request.OperationID, ResidentAttempts: []l1.ResidentAttempt{}, RuntimesReaped: true, EvidenceSettled: true, Abort: &l1.StagedGenerationAbort{OperationID: request.OperationID, PrimaryGenerationID: request.ExpectedPrimaryGenerationID, PrimaryBootSessionID: request.ExpectedPrimaryBootSessionID, AuthorityGeneration: request.ExpectedAuthorityGeneration, CatalogRevision: request.ExpectedCatalogRevision, IntentRevision: request.IntentRevision, IntentReason: receipt.IntentReason}}
		if err = client.CloseRetiringGeneration(bounded, c.NodeID, receipt.Candidate, abort); err != nil {
			return err
		}
		inventory, err = client.NodeGenerations(bounded, c.NodeID)
		if err != nil {
			return err
		}
		return store.AbortPromotion(inventory)
	}
	return nil // uncommitted intent remains fenced; it grants no startup authority
}
