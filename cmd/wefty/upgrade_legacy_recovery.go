package main

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/internal/agentinstall"
	"github.com/Derek-X-Wang/wefty/internal/durable"
	"github.com/Derek-X-Wang/wefty/internal/hostboot"
	"github.com/Derek-X-Wang/wefty/internal/launchagent"
)

// Legacy enrollment is a drain, never overlap. Recovery either leaves the
// still-running legacy agent intact or finishes the single recorded primary.
func (r *rollingUpgrade) recoverLegacyEnrollment(ctx context.Context, boot string, out io.Writer) (upgradeReport, error) {
	report := upgradeReport{RecoveredOperation: r.receipt.OperationID, RunningWork: r.receipt.Preserved}
	drain, err := r.recoveryDrain()
	if err != nil {
		return report, err
	}
	if drain != nil && (drain.NodeID != r.receipt.NodeID || drain.Home != r.paths.home) {
		return report, generationControlConflict("legacy recovery admission identity differs")
	}
	c, readErr := readUpgradeCatalog(r.store)
	if readErr != nil {
		if _, statErr := os.Lstat(r.store.Root()); !errors.Is(statErr, os.ErrNotExist) {
			if statErr != nil {
				return report, statErr
			}
			pending, err := r.store.HasInitializationIntent(r.receipt.NodeID, r.receipt.OperationID)
			if err != nil {
				return report, generationControlTransportError(err)
			}
			if !pending {
				return report, generationControlTransportError(readErr)
			}
		}
		if !errors.Is(readErr, os.ErrNotExist) {
			return report, readErr
		}
		state, err := r.store.Manager.State(ctx, setupNodeAgentLabel)
		if err != nil {
			return report, err
		}
		if r.receipt.Phase != "legacy_stopping" || state.Running {
			node, err := r.op.readNode(ctx, r.profile)
			if err != nil {
				return report, err
			}
			if node.NodeID != r.receipt.NodeID || node.AgentGenerationID != "" {
				return report, generationControlConflict("legacy rollback no longer owns the primary")
			}
			if len(r.receipt.CompletionFiles) > 0 {
				if err = r.recordPartialBinaries(); err != nil {
					return report, err
				}
			}
			// Enabling is durable and idempotent. Keep completion and admission
			// recovery fenced until the retained legacy owner can autostart again.
			if err = r.store.Manager.RestoreLegacyStart(ctx, setupNodeAgentLabel); err != nil {
				return report, err
			}
			r.receipt.RecoveryDrain = drain
			r.receipt.RecoveryState = "rolled_back"
			if err = r.persist(r.receipt.Phase); err != nil {
				return report, err
			}
			if drain != nil {
				if err = finishUpgradeDrain(ctx, r.op, r.paths, r.profile, drain, false, out); err != nil {
					return report, err
				}
				report.ClaimsDisabled = drain.claimsDisabled
			}
			report.Version = node.AgentVersion
			report.RecoveryState = r.receipt.RecoveryState
			return report, r.persist("rolled_back")
		}
		if drain == nil {
			return report, generationControlConflict("legacy initialization recovery lacks original admission evidence")
		}
		current, err := r.op.readNode(ctx, r.profile)
		if err != nil {
			return report, err
		}
		if current.NodeID != r.receipt.NodeID || current.AgentGenerationID != "" || current.BootSessionID != drain.OldBoot || current.ClaimsEnabled || current.IntentRevision != drain.Revision || current.IntentReason != drain.Reason {
			return report, generationControlConflict("legacy initialization recovery primary or admission fence changed")
		}
		// Stop was durable but initialization was not reached. No old Attempt may
		// be relabeled or repeated by creating the first generation.
		if r.receipt.LegacyBase == nil || r.receipt.Candidate.Version == "" {
			return report, generationControlConflict("legacy recovery lacks the original generation launch plan")
		}
		if err = r.waitLegacyAttempts(ctx); err != nil {
			return report, err
		}
		for _, name := range setupBinaries {
			digest, err := executableDigest(filepath.Join(r.paths.bin, name))
			if err != nil {
				return report, err
			}
			if digest != r.receipt.BinaryDigests[name] {
				return report, generationControlConflict("legacy recovery installed binaries changed")
			}
		}
		if err = r.removeLegacyAutostart(ctx); err != nil {
			return report, err
		}
		g, err := r.store.Initialize(r.receipt.NodeID, filepath.Join(r.paths.bin, "wefty-agent"), r.receipt.Candidate.Version, r.receipt.SystemBootID, []int{1}, r.receipt.OperationID)
		if err != nil {
			return report, generationControlTransportError(err)
		}
		r.receipt.Candidate = g
		if err = r.persist("legacy_generation_loading"); err != nil {
			return report, err
		}
		c, err = readUpgradeCatalog(r.store)
		if err != nil {
			return report, err
		}
	}
	if c.NodeID != r.receipt.NodeID || c.PendingReceipt != "" || len(c.Generations) != 1 {
		return report, generationControlConflict("legacy recovery cannot change an existing generation cohort")
	}
	g := c.Generations[c.Primary]
	if g.Role != "primary" || g.Digest != strings.TrimPrefix(r.receipt.BinaryDigests["wefty-agent"], "sha256:") || (r.receipt.Candidate.ID != "" && r.receipt.Candidate.ID != g.ID) || (g.PreparationOperation != "" && g.PreparationOperation != r.receipt.OperationID) {
		return report, generationControlConflict("legacy recovery primary differs from the enrollment receipt")
	}
	if err = r.waitLegacyAttempts(ctx); err != nil {
		return report, err
	}
	if err = r.removeLegacyAutostart(ctx); err != nil {
		return report, err
	}
	state, err := r.store.Manager.State(ctx, g.Label)
	if err != nil {
		return report, err
	}
	if !state.Loaded {
		if r.receipt.LegacyBase == nil {
			return report, generationControlConflict("legacy recovery lacks its recorded launch definition")
		}
		if _, err = r.store.LoadCandidate(ctx, g.ID, *r.receipt.LegacyBase); err != nil {
			return report, generationControlTransportError(err)
		}
	}
	deadline := r.op.now().Add(upgradeReplacementBudget)
	for {
		node, err := r.op.observe(ctx, r.profile)
		if err != nil {
			return report, err
		}
		c, err = readUpgradeCatalog(r.store)
		if err != nil {
			return report, generationControlTransportError(err)
		}
		g = c.Generations[c.Primary]
		if node.NodeID == r.receipt.NodeID && node.AgentGenerationID == g.ID && node.AgentVersion == g.Version && node.State == contract.NodeAlive && g.Process != nil && node.BootSessionID == g.Process.BootSessionID && hostboot.SameBoot(g.Process.SystemBootID, boot) {
			err = r.store.VerifyRunning(ctx, g.ID, boot)
			if err == nil {
				break
			}
			var pending *agentinstall.RecordPendingError
			var refused *agentinstall.VerificationRefusal
			if !errors.As(err, &pending) && !errors.As(err, &refused) {
				return report, err
			}
		}
		if !r.op.now().Before(deadline) {
			return report, generationControlConflict("legacy recovery registered primary remains unverified")
		}
		if err = r.op.wait(ctx, 100*time.Millisecond); err != nil {
			return report, err
		}
	}
	r.receipt.Candidate = g
	if err = r.persist("legacy_generation_ready"); err != nil {
		return report, err
	}
	if err = r.completeInstallationFiles(); err != nil {
		return report, err
	}
	if err = r.persist("legacy_installation_complete"); err != nil {
		return report, err
	}
	r.receipt.RecoveryDrain = drain
	if err = r.persist("legacy_admission_settling"); err != nil {
		return report, err
	}
	if drain != nil {
		if err = finishUpgradeDrain(ctx, r.op, r.paths, r.profile, drain, false, out); err != nil {
			return report, err
		}
		report.ClaimsDisabled = drain.claimsDisabled
		report.RestartedServices = drain.restarted
	}
	r.receipt.RecoveryState = "recovered"
	if !hostboot.SameBoot(boot, r.receipt.SystemBootID) {
		r.receipt.RecoveryState = "reboot_interrupted"
	}
	report.Version = g.Version
	report.RecoveryState = r.receipt.RecoveryState
	return report, r.persist("complete")
}

func (r *rollingUpgrade) waitLegacyAttempts(ctx context.Context) error {
	deadline := r.op.now().Add(upgradeReplacementBudget)
	for {
		settled := true
		for _, old := range r.receipt.Preserved {
			job, err := r.op.readJob(ctx, r.profile, old.JobID)
			if err != nil {
				return err
			}
			if !upgradeAttemptSettled(job, old.AttemptID) {
				settled = false
			}
		}
		if settled {
			return nil
		}
		if !r.op.now().Before(deadline) {
			return generationControlConflict("legacy recovery awaits exact interrupted Attempt settlement")
		}
		if err := r.op.wait(ctx, 100*time.Millisecond); err != nil {
			return err
		}
	}
}
func (r *rollingUpgrade) recordPartialBinaries() error {
	written := map[string]string{}
	for _, name := range setupBinaries {
		digest, err := executableDigest(filepath.Join(r.paths.bin, name))
		if err != nil {
			return err
		}
		written[filepath.Join(r.paths.binRel, name)] = digest
	}
	return refreshRollingPartialReceipt(r.paths, written)
}

// Removal is bound to the bytes verified before the original unload. A missing
// definition is a completed retry, never permission to remove changed bytes.
func (r *rollingUpgrade) removeLegacyAutostart(ctx context.Context) error {
	if r.receipt.LegacyPlistDigest == "" {
		return generationControlConflict("legacy removal lacks original plist ownership")
	}
	state, err := r.store.Manager.State(ctx, setupNodeAgentLabel)
	if err != nil {
		return err
	}
	if state.Running {
		return generationControlConflict("legacy process remains running before generation activation")
	}
	folder, err := os.OpenRoot(r.paths.launchAgents)
	if err != nil {
		return err
	}
	defer folder.Close()
	name := setupNodeAgentLabel + ".plist"
	payload, err := agentinstall.ReadOwnedRegularFile(folder, name, 0644)
	missing := errors.Is(err, os.ErrNotExist)
	if err != nil && !missing {
		return generationControlTransportError(err)
	}
	if !missing && payloadDigest(payload) != r.receipt.LegacyPlistDigest {
		return generationControlConflict("legacy autostart differs from recorded removal intent")
	}
	if err = r.store.Manager.UnloadStoppedLegacy(ctx, setupNodeAgentLabel); err != nil {
		var running *launchagent.RunningProcessError
		if errors.As(err, &running) {
			return generationControlConflict(running.Error())
		}
		return err
	}
	// Even an absent plist can leave a loaded definition after interrupted
	// bootout or reboot. Unload confirms launchd absence before completion.
	if !missing {
		if err = folder.Remove(name); err != nil {
			return err
		}
	}
	// A prior removal may have died before its directory sync. Absence on
	// retry must cross the same durable fence before generation activation.
	if err = durable.SyncDir(folder); err != nil {
		return err
	}
	if _, err = folder.Lstat(name); !errors.Is(err, os.ErrNotExist) {
		if err != nil {
			return err
		}
		return generationControlConflict("legacy autostart reappeared under restart fence")
	}
	return nil
}
