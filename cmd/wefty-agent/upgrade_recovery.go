package main

import (
	"context"
	"errors"
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

// Reconcile only an already-attested de-enrollment intent before registration.
// L1's exact-request ledger resolves death after commit but before local result
// publication. No new boot, managed root or payload is opened on this path.
func reconcileInstalledDeEnrollment(ctx context.Context, store agentinstall.Store, profilePath string) (bool, error) {
	record, err := store.ReadDeEnrollment()
	if err != nil || record == nil {
		return false, err
	}
	if record.Result != nil {
		return true, nil
	}
	if rejected, e := store.DeEnrollmentRejected(record.Request); e != nil {
		return true, e
	} else if rejected {
		return false, store.AbortDeEnrollment(record.Request)
	}
	profile, _, err := connprofile.Resolve(profilePath, "")
	if err != nil {
		return true, err
	}
	if profile.Fabric != "plain" || profile.Identities.Node != record.NodeID {
		return true, &agentinstall.VerificationRefusal{Message: "de-enrollment recovery requires matching plain profile"}
	}
	bounded, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	config, err := fabricconfig.SavedService(profile, connprofile.ParticipantNodeAgent, fabricconfig.Config{Mode: "plain", PlainFabricID: profile.PlainFabricID, Identity: fabric.Identity{NodeID: record.NodeID, Tags: []string{l1.DefaultAgentPrincipalTag}}}, nil)
	if err != nil {
		return true, err
	}
	participant, closeFabric, err := fabricconfig.Open(bounded, config)
	if closeFabric != nil {
		defer closeFabric()
	}
	if err != nil {
		return true, err
	}
	client, err := agent.NewClient(participant, profile.L1Address)
	if err != nil {
		return true, err
	}
	defer client.Close()
	result, err := client.DeEnrollNodeGeneration(bounded, record.NodeID, record.Request)
	if err != nil {
		if agent.DefinitiveDeEnrollmentRefusal(err, record.NodeID, record.Request) {
			if abortErr := store.AbortDeEnrollment(record.Request); abortErr != nil {
				return true, errors.Join(err, abortErr)
			}
			return false, nil
		}
		return true, err
	}
	return true, store.CompleteDeEnrollment(result)
}
