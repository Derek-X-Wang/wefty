package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/agent"
	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/agentinstall"
	"github.com/Derek-X-Wang/wefty/internal/connprofile"
	"github.com/Derek-X-Wang/wefty/internal/durable"
	"github.com/Derek-X-Wang/wefty/internal/hostboot"
	"github.com/Derek-X-Wang/wefty/internal/launchagent"
	"github.com/Derek-X-Wang/wefty/l1"
)

// These are installation files only. Live databases are never captured or restored.
type rollingCompletionFile struct {
	PreviousDigest    string `json:"previous_sha256,omitempty"`
	PreviouslyMissing bool   `json:"previously_missing,omitempty"`
	Path              string `json:"path"`
	Payload           []byte `json:"payload"`
	Mode              uint32 `json:"mode"`
}

func hasInterruptedRolling(paths setupPaths) bool {
	r, err := readUpgradeRolling(paths)
	return !errors.Is(err, os.ErrNotExist) && (err != nil || (r.Phase != "complete" && r.Phase != "rolled_back" && r.Phase != "reboot_reconciled"))
}

// Called under the installation lock, before stale-stage deletion or legacy
// drain recovery. A retry first settles the previous operation, even without
// --rolling. It never silently begins another activation in the same attempt.
func recoverRollingUpgrade(ctx context.Context, host setupHost, op upgradeHost, paths setupPaths, out io.Writer) (upgradeReport, error) {
	receipt, err := readUpgradeRolling(paths)
	if err != nil {
		return upgradeReport{}, err
	}
	if receipt.Protocol != 1 || !hostboot.ValidIdentity(receipt.SystemBootID) {
		return upgradeReport{}, generationControlConflict("rolling recovery protocol or system boot evidence is invalid")
	}
	known := slices.Contains([]string{"prepared", "admission_closing", "admission_closed", "services_quiescing", "services_quiesced", "installation_writing", "control_plane_renewed", "candidate_preparing", "candidate_loading", "candidate_ready", "candidate_staging", "promotion_requested", "promotion_committed", "catalog_reconciled", "retirement_acknowledged", "survivor_renewal_proved", "installation_completing", "installation_complete", "admission_settling", "rolling_back", "legacy_admission_closing", "legacy_draining", "legacy_stopping", "legacy_generation_loading", "legacy_generation_ready", "legacy_installation_complete", "legacy_admission_settling"}, receipt.Phase)
	if !known {
		return upgradeReport{}, generationControlConflict("unknown interrupted rolling phase: " + receipt.Phase)
	}
	if receipt.Promotion != nil && (receipt.Promotion.OperationID != receipt.OperationID || receipt.Promotion.CandidateGenerationID != receipt.Candidate.ID || receipt.Promotion.CandidateBootSessionID != receipt.Candidate.CandidateBootSessionID) {
		return upgradeReport{}, generationControlConflict("rolling recovery promotion identity differs")
	}

	report := upgradeReport{Version: receipt.Candidate.Version, RunningWork: slices.Clone(receipt.Preserved), RecoveredOperation: receipt.OperationID}
	profile, err := connprofile.Read(paths.profile)
	if err != nil {
		return report, err
	}
	if receipt.NodeID != profile.Identities.Node {
		return report, generationControlConflict("rolling recovery node identity differs")
	}
	boot, err := op.bootIdentity()
	if err != nil {
		return report, err
	}
	r := &rollingUpgrade{op: op, paths: paths, profile: profile, store: upgradeGenerationStore(host, paths, op), receipt: *receipt}
	for _, g := range receipt.Inventory.Generations {
		if g.Role == l1.NodeGenerationPrimary {
			r.primary = g
		}
	}
	if strings.HasPrefix(receipt.Phase, "legacy_") {
		return r.recoverLegacyEnrollment(ctx, boot, out)
	}
	if err := r.bindLegacyCandidate(); err != nil {
		return report, err
	}
	if !hostboot.SameBoot(boot, receipt.SystemBootID) {
		return r.recoverReboot(ctx, boot, out)
	}
	drain, err := r.recoveryDrain()
	if err != nil {
		return report, err
	}
	if drain != nil && (drain.NodeID != receipt.NodeID || drain.Home != paths.home) {
		return report, generationControlConflict("rolling recovery admission identity differs")
	}
	if slices.Contains([]string{"candidate_ready", "candidate_staging", "promotion_requested"}, receipt.Phase) {
		catalog, err := r.store.Read()
		if err != nil {
			return report, generationControlTransportError(err)
		}
		if candidate, ok := catalog.Generations[receipt.Candidate.ID]; ok && catalog.Primary == r.primary.AgentGenerationID {
			state, err := r.store.Manager.State(ctx, candidate.Label)
			if err != nil {
				return report, err
			}
			if !state.Running {
				inventory, err := r.op.generations(ctx, r.profile)
				if err != nil {
					return report, err
				}
				if slices.ContainsFunc(inventory.Generations, func(g l1.NodeGeneration) bool {
					return g.Role == l1.NodeGenerationPrimary && g.AgentGenerationID == r.primary.AgentGenerationID && g.BootSessionID == r.primary.BootSessionID
				}) {
					err = r.rollbackCandidate(ctx, drain, out)
					report.Version = catalog.Generations[catalog.Primary].Version
					report.RecoveryState = r.receipt.RecoveryState
					report.ClaimsDisabled = drain != nil && drain.claimsDisabled
					return report, err
				}
			}
		}
	}
	r.receipt.RecoveryState = "recovering"
	if err := r.persist(receipt.Phase); err != nil {
		return report, err
	}
	switch receipt.Phase {
	case "prepared", "admission_closing", "admission_closed", "services_quiescing", "services_quiesced", "installation_writing", "control_plane_renewed", "candidate_preparing", "candidate_loading", "rolling_back":
		err = r.rollbackCandidate(ctx, drain, out)
		report.ClaimsDisabled = drain != nil && drain.claimsDisabled
		report.RecoveryState = r.receipt.RecoveryState
		if err == nil {
			catalog, readErr := r.store.Read()
			if readErr != nil {
				return report, readErr
			}
			report.Version = catalog.Generations[catalog.Primary].Version
		}
		return report, err
	case "candidate_ready", "candidate_staging":
		if drain == nil {
			return report, generationControlConflict("forward recovery lacks admission receipt")
		}
		err = r.resumeReadyPromotion(ctx, drain)
	case "promotion_requested", "promotion_committed", "catalog_reconciled", "retirement_acknowledged", "survivor_renewal_proved", "installation_completing", "installation_complete", "admission_settling":
		if receipt.Promotion == nil || receipt.ActivationReceiptID == "" {
			return report, generationControlConflict("recovery lacks exact promotion request")
		}
		// Reconcile the exact request/result; no version or PID is promotion proof.
		_, err = r.finishPromotion(ctx, launchagent.Unchanged)
	default:
		return report, generationControlConflict("unknown interrupted rolling phase: " + receipt.Phase)
	}
	if err != nil {
		return report, err
	}
	if err = r.persist("installation_completing"); err != nil {
		return report, err
	}
	if err = r.completeInstallationFiles(); err != nil {
		return report, err
	}
	if err = r.persist("installation_complete"); err != nil {
		return report, err
	}
	if drain != nil {
		r.receipt.RecoveryDrain = drain
		if err = r.persist("admission_settling"); err != nil {
			return report, err
		}
		if err = finishUpgradeDrain(ctx, op, paths, profile, drain, false, out); err != nil {
			return report, err
		}
		report.RestartedServices = slices.Clone(drain.restarted)
		report.ClaimsDisabled = drain.claimsDisabled
	}
	r.receipt.RecoveryState = "recovered"
	err = r.persist("complete")
	report.RecoveryState = r.receipt.RecoveryState
	return report, err
}

func (r *rollingUpgrade) rollbackCandidate(ctx context.Context, drain *upgradeDrainReceipt, out io.Writer) error {
	in, err := r.op.generations(ctx, r.profile)
	if err != nil {
		return err
	}
	if in.NodeID == "" {
		return errors.New("malformed rollback generation inventory")
	}
	if in.NodeID != r.receipt.NodeID {
		return generationControlConflict("rollback inventory identity differs")
	}
	primary := false
	for _, g := range in.Generations {
		if g.Role == l1.NodeGenerationPrimary {
			primary = g.AgentGenerationID == r.primary.AgentGenerationID && g.BootSessionID == r.primary.BootSessionID && g.AuthorityGeneration == r.primary.AuthorityGeneration
		}
		if g.Role == l1.NodeGenerationStaged && g.AgentGenerationID != r.receipt.Candidate.ID {
			return generationControlConflict("rollback inventory contains another staged candidate")
		}
	}
	if !primary {
		return generationControlConflict("rollback would destroy a replacement primary; forward reconciliation required")
	}
	c, err := r.store.Read()
	if err != nil {
		return generationControlTransportError(err)
	}
	if c.Primary != r.primary.AgentGenerationID {
		return generationControlConflict("rollback local primary changed")
	}
	in, err = r.abortStagedCandidate(ctx, in, drain)
	if err != nil {
		return err
	}
	if c.PendingReceipt != "" {
		if err = r.store.AbortPreparedPromotion(c.PendingReceipt, in); err != nil {
			return generationControlTransportError(err)
		}
	}
	// Quiescence may have completed with its reply lost. Re-obtain and persist the
	// exact proof before releasing the gate or queuing service recovery.
	if drain != nil && r.receipt.Phase == "services_quiescing" {
		proof := agent.UpgradeServiceQuiescence{IntentRevision: drain.Revision, IntentReason: drain.Reason, OperationID: r.receipt.OperationID, NodeID: r.receipt.NodeID, AgentGenerationID: r.primary.AgentGenerationID, BootSessionID: r.primary.BootSessionID, Services: slices.Clone(drain.Services)}
		payload, _ := json.Marshal(proof)
		result, err := r.op.generationControl(ctx, r.store, agentinstall.ControlRequest{ID: r.receipt.OperationID, Action: "quiesce_services", NodeID: r.receipt.NodeID, GenerationID: r.primary.AgentGenerationID, BootSessionID: r.primary.BootSessionID, Payload: payload})
		if err != nil {
			return err
		}
		var accepted agent.UpgradeServiceQuiescence
		if err = json.Unmarshal(result, &accepted); err != nil {
			return err
		}
		if !reflect.DeepEqual(proof, accepted) {
			return generationControlConflict("recovery service proof identity differs")
		}
		drain.ServiceQuiescence = &accepted
		if err = writeUpgradeDrain(r.paths.config, drain); err != nil {
			return err
		}
	}
	r.receipt.RecoveryDrain = drain
	if err = r.persist("rolling_back"); err != nil {
		return err
	}
	for _, g := range c.Generations {
		if g.PreparationOperation != r.receipt.OperationID {
			continue
		}
		if slices.ContainsFunc(in.Generations, func(remote l1.NodeGeneration) bool {
			return remote.AgentGenerationID == g.ID && !(remote.Role == l1.NodeGenerationClosed && remote.PromotionOperationID == r.receipt.OperationID && remote.BootSessionID == g.CandidateBootSessionID && len(remote.ResidentAttempts) == 0)
		}) {
			return generationControlConflict("candidate has L1 authority history; removal refused")
		}
		deadline := r.op.now().Add(upgradeReplacementBudget)
		for {
			err = r.store.RemoveCandidate(ctx, g.ID, r.receipt.OperationID)
			var pending *agentinstall.RecordPendingError
			if !errors.As(err, &pending) {
				break
			}
			if !r.op.now().Before(deadline) {
				return fmt.Errorf("candidate exit unverified: %w", err)
			}
			if err = r.op.wait(ctx, 100*time.Millisecond); err != nil {
				return err
			}
		}
		if err != nil {
			return generationControlTransportError(err)
		}
	}
	if len(r.receipt.CompletionFiles) > 0 {
		written := map[string]string{}
		for _, name := range setupBinaries {
			digest, err := executableDigest(filepath.Join(r.paths.bin, name))
			if err != nil {
				return err
			}
			written[filepath.Join(r.paths.binRel, name)] = digest
		}
		if err = refreshRollingPartialReceipt(r.paths, written); err != nil {
			return err
		}
	}
	if drain != nil {
		if drain.ServiceQuiescence != nil {
			_, err = r.op.generationControl(ctx, r.store, agentinstall.ControlRequest{ID: r.receipt.OperationID, Action: "resume_services", NodeID: r.receipt.NodeID, GenerationID: r.primary.AgentGenerationID, BootSessionID: r.primary.BootSessionID})
			if err != nil {
				return err
			}
		}
		if err = finishUpgradeDrain(ctx, r.op, r.paths, r.profile, drain, false, out); err != nil {
			return err
		}
	}
	r.receipt.RecoveryState = "rolled_back"
	return r.persist("rolled_back")
}

// No unhealthy replacement is stopped by recovery. Even with no active Node
// projection, missing accounting or compatible overlap proof forbids rollback.
type destructiveRollingRollbackError struct{ apiResponseError }

func (e *destructiveRollingRollbackError) Unwrap() error { return &e.apiResponseError }

func (r *rollingUpgrade) refuseDestructiveRollback(ctx context.Context) error {
	node, err := r.op.readNode(ctx, r.profile)
	if err != nil {
		return err
	}
	if node.NodeID == "" {
		return errors.New("malformed rollback work observation")
	}
	if node.NodeID != r.receipt.NodeID {
		return generationControlConflict("rollback accounting belongs to another node")
	}
	var accepted []l1.NodeActiveAttempt
	for _, a := range node.ActiveAttempts {
		if a.BootSessionID == r.receipt.Candidate.CandidateBootSessionID {
			accepted = append(accepted, a)
		}
	}
	r.receipt.RecoveryState = "rollback_refused"
	if err = r.persist(r.receipt.Phase); err != nil {
		return err
	}
	return &destructiveRollingRollbackError{apiResponseError: apiResponseError{APIError: contract.APIError{Code: contract.ErrorConflict, Message: fmt.Sprintf("destructive rolling rollback refused: new primary accepted %d active Attempts; compatible replacement and complete work accounting are unverified; all generations, live databases and receipts retained. When the primary control adapter answers, run wefty upgrade --rolling to a fixed build; otherwise follow the manual repair in docs/contracts/plain-node-upgrade.md", len(accepted)), Details: map[string]any{"reason": "destructive_rollback", "operation_id": r.receipt.OperationID, "candidate_generation_id": r.receipt.Candidate.ID, "accepted_attempts": accepted, "forward_command": "wefty upgrade --rolling", "manual_repair": "docs/contracts/plain-node-upgrade.md#manual-repair-after-destructive-rollback-refusal"}}}}
}

func (r *rollingUpgrade) resumeReadyPromotion(ctx context.Context, drain *upgradeDrainReceipt) error {
	c, err := r.store.Read()
	if err != nil {
		return generationControlTransportError(err)
	}
	if c.PendingReceipt == "" {
		_, err = r.promoteReady(ctx, drain, launchagent.Unchanged)
		return err
	}
	activation, err := r.store.ReadActivationReceipt(c.PendingReceipt)
	if err != nil {
		return generationControlTransportError(err)
	}
	if activation.L1OperationID != r.receipt.OperationID || activation.ExpectedPrimary != r.primary.AgentGenerationID || activation.Candidate != r.receipt.Candidate.ID || activation.IntentRevision != drain.Revision || activation.IntentReason != drain.Reason {
		return generationControlConflict("pending recovery activation fences differ")
	}
	request := l1.PromoteNodeGenerationRequest{OperationID: activation.L1OperationID, ExpectedPrimaryGenerationID: activation.ExpectedPrimary, ExpectedPrimaryBootSessionID: r.primary.BootSessionID, ExpectedAuthorityGeneration: r.primary.AuthorityGeneration, CandidateGenerationID: activation.Candidate, CandidateBootSessionID: activation.Processes[activation.Candidate].BootSessionID, ExpectedCatalogRevision: activation.L1CatalogRevision, CatalogRevision: activation.L1CatalogRevision + 1, IntentRevision: activation.IntentRevision, SelectedProtocol: uint32(activation.Protocol), RetiringGenerationIDs: slices.Clone(activation.Retiring), ResidentAttempts: []l1.ResidentAttempt{}}
	for _, a := range activation.Residents[activation.ExpectedPrimary] {
		request.ResidentAttempts = append(request.ResidentAttempts, l1.ResidentAttempt(a))
	}
	if activation.PromotionRequest != nil {
		request = *activation.PromotionRequest
	}
	if err = r.store.BindPromotionRequest(activation.ID, request); err != nil {
		return generationControlTransportError(err)
	}
	r.receipt.ActivationReceiptID = activation.ID
	r.receipt.Promotion = &request
	if err = r.persist("promotion_requested"); err != nil {
		return err
	}
	_, err = r.finishPromotion(ctx, launchagent.Unchanged)
	return err
}

func (r *rollingUpgrade) recoverReboot(ctx context.Context, boot string, out io.Writer) (upgradeReport, error) {
	report := upgradeReport{Version: r.receipt.Candidate.Version, RunningWork: slices.Clone(r.receipt.Preserved), RecoveredOperation: r.receipt.OperationID}
	r.receipt.RecoveryState = "reboot_unverified"
	if err := r.persist(r.receipt.Phase); err != nil {
		return report, err
	}
	c, err := r.store.Read()
	if err != nil {
		return report, generationControlTransportError(err)
	}
	if c.PendingReceipt != "" {
		return report, generationControlConflict("reboot recovery awaits installed primary reconciliation of pending L1 operation")
	}
	in, err := r.op.generations(ctx, r.profile)
	if err != nil {
		return report, err
	}
	if in.NodeID == "" {
		return report, errors.New("malformed reboot generation inventory")
	}
	if in.NodeID != r.receipt.NodeID || c.NodeID != r.receipt.NodeID {
		return report, generationControlConflict("reboot recovery inventory differs")
	}
	winner, known := c.Generations[c.Primary]
	if !known || (winner.ID != r.primary.AgentGenerationID && winner.ID != r.receipt.Candidate.ID) || winner.Process == nil || !hostboot.SameBoot(winner.Process.SystemBootID, boot) {
		return report, generationControlConflict("reboot recovery lacks a freshly bound primary process")
	}
	report.Version = winner.Version
	observed := false
	for _, g := range in.Generations {
		if g.Role == l1.NodeGenerationPrimary && g.AgentGenerationID == winner.ID && g.BootSessionID == winner.Process.BootSessionID && hostboot.SameBoot(g.Registration.SystemBootID, boot) {
			observed = true
		}
	}
	if !observed {
		return report, generationControlConflict("reboot recovery lacks current L1 primary and host boot")
	}
	if err = r.store.VerifyRunning(ctx, winner.ID, boot); err != nil {
		return report, generationControlTransportError(err)
	}
	r.receipt.RebootAttempts = map[string]l1.Attempt{}
	for _, old := range r.receipt.Preserved {
		job, err := r.op.readJob(ctx, r.profile, old.JobID)
		if err != nil {
			return report, err
		}
		found := false
		for _, a := range job.Attempts {
			if a.AttemptID == old.AttemptID {
				found = true
				if a.State == contract.AttemptClaimed || a.State == contract.AttemptRunning || a.State == contract.AttemptAwaitingInput {
					return report, generationControlConflict("reboot recovery awaits prior Attempt Lease reconciliation; no process adoption or ordinary restart")
				}
				r.receipt.RebootAttempts[a.AttemptID] = a
			}
		}
		if !found {
			return report, generationControlConflict("reboot recovery cannot account for an original Attempt")
		}
	}
	if err = r.persist(r.receipt.Phase); err != nil {
		return report, err
	}
	// A pre-promotion winner can remove only this operation's never-authorized
	// candidate. A closed staged abort is positive L1 evidence, not a promotion.
	if winner.ID == r.primary.AgentGenerationID {
		for _, g := range c.Generations {
			if g.PreparationOperation != r.receipt.OperationID {
				continue
			}
			for _, remote := range in.Generations {
				if remote.AgentGenerationID != g.ID {
					continue
				}
				if remote.Role == l1.NodeGenerationStaged {
					node, err := r.op.readNode(ctx, r.profile)
					if err != nil {
						return report, err
					}
					if node.NodeID != r.receipt.NodeID || node.BootSessionID != winner.Process.BootSessionID {
						return report, generationControlConflict("staged reboot abort primary observation differs")
					}
					request := l1.CloseNodeGenerationRequest{BootSessionID: g.CandidateBootSessionID, PromotionOperationID: r.receipt.OperationID, ResidentAttempts: []l1.ResidentAttempt{}, RuntimesReaped: true, EvidenceSettled: true, Abort: &l1.StagedGenerationAbort{OperationID: r.receipt.OperationID, PrimaryGenerationID: winner.ID, PrimaryBootSessionID: winner.Process.BootSessionID, AuthorityGeneration: node.AuthorityGeneration, CatalogRevision: in.CatalogRevision, IntentRevision: node.IntentRevision, IntentReason: node.IntentReason}}
					drain, err := readRollingDrain(r.paths.config)
					if err != nil {
						return report, err
					}
					if node.ClaimsEnabled || drain.Revision != node.IntentRevision || drain.Reason != node.IntentReason {
						return report, generationControlConflict("staged reboot abort original admission fence changed")
					}
					payload, _ := json.Marshal(agentinstall.StagedAbortControl{CandidateGenerationID: g.ID, Request: request})
					reply, err := r.op.generationControl(ctx, r.store, agentinstall.ControlRequest{ID: r.receipt.OperationID, NodeID: r.receipt.NodeID, GenerationID: winner.ID, BootSessionID: winner.Process.BootSessionID, Action: "abort_staged_generation", Payload: payload})
					if err != nil {
						return report, err
					}
					var aborted l1.NodeGenerationInventory
					if err = json.Unmarshal(reply, &aborted); err != nil {
						return report, err
					}
					closed := false
					for _, ack := range aborted.Generations {
						if ack.AgentGenerationID == g.ID && ack.Role == l1.NodeGenerationClosed && ack.PromotionOperationID == r.receipt.OperationID && ack.BootSessionID == g.CandidateBootSessionID {
							closed = true
						}
					}
					if aborted.NodeID != r.receipt.NodeID || !closed {
						return report, generationControlConflict("staged reboot abort closure unverified")
					}
				} else if remote.Role != l1.NodeGenerationClosed || remote.PromotionOperationID != r.receipt.OperationID {
					return report, generationControlConflict("reboot candidate authority remains unresolved")
				}
			}
			if err = r.store.RemoveCandidate(ctx, g.ID, r.receipt.OperationID); err != nil {
				return report, generationControlTransportError(err)
			}
		}
	} else {
		if len(r.receipt.CompletionFiles) == 0 {
			return report, generationControlConflict("reboot recovery lacks prepared installation completion")
		}
		if err = r.completeInstallationFiles(); err != nil {
			return report, err
		}
	}
	drain, err := r.recoveryDrain()
	if err != nil {
		return report, err
	}
	if drain != nil {
		if drain.NodeID != r.receipt.NodeID || drain.Home != r.paths.home {
			return report, generationControlConflict("reboot admission receipt differs")
		}
		if len(drain.Services) > 0 && drain.ServiceQuiescence == nil {
			return report, generationControlConflict("reboot service recovery lacks positive pre-promotion quiescence; queue retained")
		}
		r.receipt.RecoveryDrain = drain
		if err = r.persist(r.receipt.Phase); err != nil {
			return report, err
		}
		if err = finishUpgradeDrain(ctx, r.op, r.paths, r.profile, drain, false, out); err != nil {
			return report, err
		}
		report.ClaimsDisabled = drain.claimsDisabled
		report.RestartedServices = slices.Clone(drain.restarted)
	}
	r.receipt.RecoveryState = "reboot_interrupted"
	err = r.persist("reboot_reconciled")
	report.RecoveryState = r.receipt.RecoveryState
	return report, err
}

func readRollingDrain(config string) (*upgradeDrainReceipt, error) {
	root, err := os.OpenRoot(config)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	payload, err := agentinstall.ReadPrivateFile(root, upgradeDrainFile)
	if err != nil {
		return nil, generationControlTransportError(err)
	}
	var receipt upgradeDrainReceipt
	if err = json.Unmarshal(payload, &receipt); err != nil {
		return nil, err
	}
	if receipt.Version != 1 || !filepath.IsAbs(receipt.Home) || receipt.NodeID == "" || receipt.Revision < 0 || (receipt.RestoreClaims && !strings.HasPrefix(receipt.Reason, upgradeIntentPrefix)) {
		return nil, generationControlConflict("invalid rolling admission receipt")
	}
	return &receipt, nil
}

func refreshRollingPartialReceipt(paths setupPaths, written map[string]string) error {
	root, err := os.OpenRoot(paths.config)
	if err != nil {
		return err
	}
	defer root.Close()
	payload, err := agentinstall.ReadPrivateFile(root, setupManifestName)
	if err != nil {
		return generationControlTransportError(err)
	}
	var manifest setupManifest
	if err = json.Unmarshal(payload, &manifest); err != nil {
		return err
	}
	if manifest.Version != 1 {
		return generationControlConflict("invalid partial installation manifest")
	}
	manifest.InstalledVersion = ""
	manifest.UpgradeSource = true
	if manifest.Files == nil {
		manifest.Files = map[string]string{}
	}
	for name, digest := range written {
		manifest.Files[name] = digest
	}
	payload, err = json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	return durable.WriteFile(root, setupManifestName, append(payload, '\n'), 0600)
}

func readRollingCompletionFile(home *os.Root, file rollingCompletionFile) ([]byte, error) {
	parent, err := home.OpenRoot(filepath.Dir(file.Path))
	if err != nil {
		return nil, err
	}
	defer parent.Close()
	return agentinstall.ReadOwnedRegularFile(parent, filepath.Base(file.Path), os.FileMode(file.Mode))
}
func (r *rollingUpgrade) completeInstallationFiles() error {
	if len(r.receipt.CompletionFiles) == 0 {
		return generationControlConflict("recovery lacks prepared installation completion")
	}
	root, err := os.OpenRoot(r.paths.home)
	if err != nil {
		return err
	}
	defer root.Close()
	for _, file := range r.receipt.CompletionFiles {
		if !filepath.IsLocal(file.Path) || (file.Path != filepath.Join(r.paths.configRel, setupManifestName) && !setupSkillPath(r.paths, file.Path)) || (file.Mode != 0600 && file.Mode != 0644) {
			return generationControlConflict("recovery installation file is outside the completion plan")
		}
		current, err := readRollingCompletionFile(root, file)
		if errors.Is(err, os.ErrNotExist) && file.PreviouslyMissing {
			continue
		}
		if err != nil {
			return generationControlTransportError(err)
		}
		digest := payloadDigest(current)
		if digest != payloadDigest(file.Payload) && digest != file.PreviousDigest {
			return generationControlConflict("installation completion file changed after preparation; owner bytes retained")
		}
	}
	for _, file := range r.receipt.CompletionFiles {
		_, err = convergeHomeFile(root, setupFile{folder: filepath.Dir(file.Path), name: filepath.Base(file.Path), payload: file.Payload, mode: os.FileMode(file.Mode)})
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *rollingUpgrade) bindLegacyCandidate() error {
	if !slices.Contains([]string{"candidate_preparing", "candidate_loading", "candidate_ready", "candidate_staging", "rolling_back"}, r.receipt.Phase) {
		return nil
	}
	c, err := r.store.Read()
	if err != nil {
		return generationControlTransportError(err)
	}
	if c.PendingReceipt != "" {
		return nil
	}
	var matches []agentinstall.Generation
	for _, g := range c.Generations {
		if g.Role != "" || g.PreparationOperation != "" {
			continue
		}
		if r.receipt.Candidate.ID != "" && g.ID != r.receipt.Candidate.ID {
			continue
		}
		if g.Digest != r.receipt.Candidate.Digest || g.Version != r.receipt.Candidate.Version || !hostboot.SameBoot(g.StartBootID, r.receipt.SystemBootID) {
			continue
		}
		if r.receipt.Candidate.ID == "" && (c.Revision != r.receipt.LocalCatalogRevision+1 || g.Process != nil || g.PlistDigest != "") {
			return generationControlConflict("legacy preparation lacks exact candidate identity")
		}
		matches = append(matches, g)
	}
	if len(matches) > 1 {
		return generationControlConflict("legacy candidate ownership is ambiguous")
	}
	if len(matches) == 1 {
		return generationControlTransportError(r.store.BindCandidateOperation(matches[0], r.primary.AgentGenerationID, r.receipt.OperationID, c.Revision))
	}
	return nil
}

// Catalog publication can precede registered readiness. Allow the real startup
// window without treating old candidate readiness as proof of a live primary.
func (r *rollingUpgrade) waitRecoveryPrimary(ctx context.Context) error {
	deadline := r.op.now().Add(upgradeReplacementBudget)
	for {
		node, err := r.op.observe(ctx, r.profile)
		if err != nil {
			return err
		}
		if node.NodeID == "" || node.BootSessionID == "" || node.AgentGenerationID == "" || node.AgentVersion == "" || node.State == "" {
			return errors.New("malformed registered primary observation")
		}
		if node.NodeID != r.receipt.NodeID || node.AgentGenerationID != r.receipt.Candidate.ID {
			return generationControlConflict("registered recovery primary identity differs")
		}
		c, err := r.store.Read()
		if err != nil {
			return generationControlTransportError(err)
		}
		g, ok := c.Generations[r.receipt.Candidate.ID]
		if !ok || c.Primary != g.ID || c.PendingReceipt != "" {
			return generationControlConflict("recovery primary ownership changed")
		}
		err = r.store.VerifyRunning(ctx, g.ID, r.receipt.SystemBootID)
		if err == nil && g.Process != nil && node.NodeID == r.receipt.NodeID && node.AgentGenerationID == g.ID && node.BootSessionID == g.Process.BootSessionID && node.AgentVersion == g.Version && node.State == contract.NodeAlive {
			r.verifiedPrimaryBoot = g.Process.BootSessionID
			return nil
		}
		var refusal *agentinstall.VerificationRefusal
		var pending *agentinstall.RecordPendingError
		if err != nil && !errors.As(err, &refusal) && !errors.As(err, &pending) {
			return err
		}
		state, stateErr := r.store.Manager.State(ctx, g.Label)
		if stateErr != nil {
			return stateErr
		}
		if !state.Running || !r.op.now().Before(deadline) {
			return r.refuseDestructiveRollback(ctx)
		}
		if err = r.op.wait(ctx, 100*time.Millisecond); err != nil {
			return err
		}
	}
}

func (r *rollingUpgrade) verifyRetiringProcess(ctx context.Context, attempt l1.NodeActiveAttempt) error {
	if r.receipt.PromotionResult == nil {
		return generationControlConflict("survival recovery lacks the exact promoted cohort")
	}
	for _, g := range r.receipt.PromotionResult.Generations {
		if g.Role != l1.NodeGenerationRetiring || g.BootSessionID != attempt.BootSessionID {
			continue
		}
		if slices.ContainsFunc(g.ResidentAttempts, func(a l1.ResidentAttempt) bool { return a.JobID == attempt.JobID && a.AttemptID == attempt.AttemptID }) {
			return generationControlTransportError(r.store.VerifyRunning(ctx, g.AgentGenerationID, r.receipt.SystemBootID))
		}
	}
	return generationControlConflict("surviving Attempt is not bound to a promoted retiring generation")
}

// The rolling receipt records the admission request before the drain file or
// CAS. A missing drain file can then use exact persisted ownership, never infer
// that admission was restored merely because the file disappeared.
func (r *rollingUpgrade) drainSnapshotHost() upgradeHost {
	op := r.op
	write := op.writeDrain
	op.writeDrain = func(ctx context.Context, config string, drain *upgradeDrainReceipt) error {
		r.receipt.RecoveryDrain = drain
		if err := r.persist("admission_closing"); err != nil {
			return err
		}
		if write != nil {
			return write(ctx, config, drain)
		}
		return writeUpgradeDrain(config, drain)
	}
	return op
}

func (r *rollingUpgrade) recoveryDrain() (*upgradeDrainReceipt, error) {
	drain, err := readRollingDrain(r.paths.config)
	if errors.Is(err, os.ErrNotExist) {
		drain = r.receipt.RecoveryDrain
		if drain == nil && r.receipt.ForwardAdmission != nil {
			copy := *r.receipt.ForwardAdmission
			drain = &copy
		}
		if drain == nil {
			if r.receipt.DrainIntentSequenced && slices.Contains([]string{"prepared", "rolling_back", "legacy_admission_closing"}, r.receipt.Phase) {
				return nil, nil
			}
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	r.inheritForwardAdmission(drain)
	r.inheritForwardAdmission(r.receipt.RecoveryDrain)
	if snapshot := r.receipt.RecoveryDrain; snapshot != nil && (snapshot.Home != drain.Home || snapshot.NodeID != drain.NodeID || snapshot.Revision != drain.Revision || snapshot.Reason != drain.Reason || snapshot.PreviousReason != drain.PreviousReason || snapshot.RestoreClaims != drain.RestoreClaims) {
		return nil, generationControlConflict("rolling admission receipt differs from persisted operation")
	}
	if drain.Version != 1 || drain.Home != r.paths.home || drain.NodeID != r.receipt.NodeID || drain.Revision < 0 || (drain.RestoreClaims && !strings.HasPrefix(drain.Reason, upgradeIntentPrefix)) {
		return nil, generationControlConflict("rolling admission snapshot identity differs")
	}
	drain, err = r.reconcileForwardDrain(drain)
	if err != nil {
		return nil, err
	}
	if proof := drain.ServiceQuiescence; proof != nil && proof.OperationID != r.receipt.OperationID {
		return nil, generationControlConflict("service proof belongs to another rolling operation")
	}
	if err := validateUpgradeServiceQuiescence(drain); err != nil {
		return nil, err
	}
	return drain, nil
}

func (r *rollingUpgrade) abortStagedCandidate(ctx context.Context, in l1.NodeGenerationInventory, drain *upgradeDrainReceipt) (l1.NodeGenerationInventory, error) {
	staged := slices.ContainsFunc(in.Generations, func(g l1.NodeGeneration) bool {
		return g.AgentGenerationID == r.receipt.Candidate.ID && g.Role == l1.NodeGenerationStaged
	})
	if !staged {
		return in, nil
	}
	if drain == nil {
		return in, generationControlConflict("staged rollback lacks admission evidence")
	}
	if r.receipt.StagedAbort == nil {
		request := l1.CloseNodeGenerationRequest{BootSessionID: r.receipt.Candidate.CandidateBootSessionID, PromotionOperationID: r.receipt.OperationID, ResidentAttempts: []l1.ResidentAttempt{}, RuntimesReaped: true, EvidenceSettled: true, Abort: &l1.StagedGenerationAbort{OperationID: r.receipt.OperationID, PrimaryGenerationID: r.primary.AgentGenerationID, PrimaryBootSessionID: r.primary.BootSessionID, AuthorityGeneration: r.primary.AuthorityGeneration, CatalogRevision: in.CatalogRevision, IntentRevision: drain.Revision, IntentReason: drain.Reason}}
		r.receipt.StagedAbort = &agentinstall.StagedAbortControl{CandidateGenerationID: r.receipt.Candidate.ID, Request: request}
		r.receipt.RecoveryDrain = drain
		if err := r.persist("rolling_back"); err != nil {
			return in, err
		}
	}
	payload, err := json.Marshal(r.receipt.StagedAbort)
	if err != nil {
		return in, err
	}
	reply, err := r.op.generationControl(ctx, r.store, agentinstall.ControlRequest{ID: r.receipt.OperationID, Action: "abort_staged_generation", NodeID: r.receipt.NodeID, GenerationID: r.primary.AgentGenerationID, BootSessionID: r.primary.BootSessionID, Payload: payload})
	if err != nil {
		return in, err
	}
	var closed l1.NodeGenerationInventory
	if err = json.Unmarshal(reply, &closed); err != nil {
		return in, err
	}
	if closed.NodeID != r.receipt.NodeID || !slices.ContainsFunc(closed.Generations, func(g l1.NodeGeneration) bool {
		return g.AgentGenerationID == r.receipt.Candidate.ID && g.BootSessionID == r.receipt.Candidate.CandidateBootSessionID && g.Role == l1.NodeGenerationClosed && g.PromotionOperationID == r.receipt.OperationID && len(g.ResidentAttempts) == 0
	}) {
		return in, generationControlConflict("staged rollback closure unverified")
	}
	return closed, nil
}
