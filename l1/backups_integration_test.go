package l1

import (
	"context"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/contract"
	"github.com/Derek-X-Wang/wefty/fabric"
)

func backupHarness(t *testing.T, clusterCap int64, perComputerCap *int64) (*integrationHarness, Node, Computer) {
	t.Helper()
	h := newIntegrationHarnessWithOptions(t, StoreOptions{LeaseDuration: 3 * time.Second, ComputerBackupCap: clusterCap}, map[string]NodePolicy{
		"computer-node": {Tags: []string{contract.StableNodeTagPrefix + "computer-node"}, MaxOneshotSlots: 1, MaxServiceSlots: 1},
	})
	node := registerCapabilityNodeWithTags(t, h, "computer-node", map[string]bool{
		"kind:oci": true, "cgroup_v2": true, "computer": true,
	}, []string{contract.StableNodeTagPrefix + "computer-node"})
	computer, _, err := h.store.CreateComputer(context.Background(), CreateComputerRequest{
		Name: "backup-computer", Spec: computerCapabilityJobSpec("computer:backup"), BackupCap: perComputerCap, Actor: "operator",
	})
	if err != nil {
		t.Fatal(err)
	}
	return h, node, computer
}

func startBackupComputer(t *testing.T, h *integrationHarness, node Node, computer Computer) (Computer, *Claim) {
	t.Helper()
	claim, err := h.store.ClaimJob(context.Background(), "fabric-computer-node", node.NodeID, node.BootSessionID, contract.JobClassService)
	if err != nil || claim == nil {
		t.Fatalf("claim Computer = %#v err=%v", claim, err)
	}
	if _, err := h.store.ObserveAttemptImage(context.Background(), "fabric-computer-node", claim.Job.JobID,
		claim.Lease.AttemptID, testImageObservation(claim.Lease.FencingToken)); err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.StartAttempt(context.Background(), "fabric-computer-node", claim.Job.JobID,
		claim.Lease.AttemptID, StartedRequest{FencingToken: claim.Lease.FencingToken}); err != nil {
		t.Fatal(err)
	}
	computer, err = h.store.GetComputer(context.Background(), computer.ComputerID)
	if err != nil {
		t.Fatal(err)
	}
	return computer, claim
}

func finishBackupQuiescence(t *testing.T, h *integrationHarness, claim *Claim, key string) {
	t.Helper()
	if _, err := h.store.CompleteAttempt(context.Background(), "fabric-computer-node", claim.Job.JobID,
		claim.Lease.AttemptID, CompletionRequest{FencingToken: claim.Lease.FencingToken,
			IdempotencyKey: key, Result: ProcessResult{OutputError: "quiesced for cold Backup"},
			RuntimeQuiescenceEvidence: RuntimeQuiescenceAttempt}); err != nil {
		t.Fatal(err)
	}
}

func successfulBackupReceipt(directive ComputerBackupDirective) ComputerBackupCopyReceipt {
	return ComputerBackupCopyReceipt{Kind: computerBackupCopyReceiptKind, ReceiptID: "receipt-" + directive.CopyID,
		BackupID: directive.BackupID, CopyID: directive.CopyID, ComputerID: directive.ComputerID,
		StorageID: directive.StorageID, StorageGeneration: directive.StorageGeneration,
		NodeID: directive.BoundNodeID, RootInstanceID: directive.RootInstanceID, JobID: directive.JobID,
		OperationRevision: directive.OperationRevision, CleanupFence: directive.CleanupFence,
		HelperGeneration: 7, AllocatedSize: directive.AllocatedSize,
		ContentDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Encryption:    BackupEncryptionNone}
}

func acknowledgeBackup(t *testing.T, h *integrationHarness, node Node, directive ComputerBackupDirective, receipt ComputerBackupCopyReceipt) (Backup, Computer) {
	t.Helper()
	backup, computer, err := h.store.AcknowledgeComputerBackup(context.Background(), "fabric-computer-node", directive.ComputerID,
		ComputerBackupAcknowledgementRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID,
			IdempotencyKey: receipt.ReceiptID, Receipt: receipt})
	if err != nil {
		t.Fatal(err)
	}
	return backup, computer
}

func backupRemovalReceipt(directive ComputerBackupPruneDirective) ComputerBackupCopyRemovalReceipt {
	return ComputerBackupCopyRemovalReceipt{Kind: computerBackupRemovalReceiptKind,
		ReceiptID: "removed-" + directive.CopyID, BackupID: directive.BackupID, CopyID: directive.CopyID,
		ComputerID: directive.ComputerID, StorageID: directive.StorageID,
		StorageGeneration: directive.StorageGeneration, NodeID: directive.BoundNodeID,
		RootInstanceID: directive.RootInstanceID, OperationRevision: directive.OperationRevision,
		CleanupFence: directive.CleanupFence, HelperGeneration: 7, Absent: true}
}

func TestComputerBackupDefaultCapZeroAndExplicitOverride(t *testing.T) {
	h, node, computer := backupHarness(t, 0, nil)
	computer, _ = startBackupComputer(t, h, node, computer)
	if computer.BackupCap != 0 {
		t.Fatalf("shipped Backup cap = %d, want 0", computer.BackupCap)
	}
	request := ComputerBackupCreateRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"), IdempotencyKey: "backup-0", AllowPowerOff: true}
	if _, _, err := h.store.BeginComputerBackup(context.Background(), computer.ComputerID, request); errorCode(err) != contract.ErrorConflict {
		t.Fatalf("default-zero Backup error = %v, want %q", err, contract.ErrorConflict)
	}
	unchanged, err := h.store.GetComputer(context.Background(), computer.ComputerID)
	if err != nil || unchanged.IntentRevision != computer.IntentRevision || unchanged.CurrentJob.State != contract.JobRunning {
		t.Fatalf("default-zero Backup changed Computer = %#v err=%v", unchanged, err)
	}
	mutable, err := h.store.SetComputerBackupCap(context.Background(), unchanged.ComputerID,
		ComputerBackupCapRequest{ComputerMutationPrecondition: computerPrecondition(unchanged, "operator"), BackupCap: 1})
	if err != nil || mutable.BackupCap != 1 || mutable.IntentRevision != unchanged.IntentRevision+1 || mutable.AppliedRevision != mutable.IntentRevision {
		t.Fatalf("mutable Backup cap = %#v err=%v", mutable, err)
	}
	intents, err := h.store.ListComputerIntents(context.Background(), mutable.ComputerID, "", 10)
	if err != nil || intents.Intents[len(intents.Intents)-1].Operation != ComputerIntentBackupCap || intents.Intents[len(intents.Intents)-1].BackupCap != 1 {
		t.Fatalf("revisioned Backup cap intent = %#v err=%v", intents, err)
	}

	override := int64(1)
	h2, _, overridden := backupHarness(t, 4, &override)
	if overridden.BackupCap != 1 {
		t.Fatalf("per-Computer Backup cap = %d, want 1", overridden.BackupCap)
	}
	list, err := h2.store.ListComputerBackups(context.Background(), overridden.ComputerID)
	if err != nil || len(list.Backups) != 0 {
		t.Fatalf("fresh Backup list = %#v err=%v", list, err)
	}
}

func TestComputerBackupRunningQuiescesPublishesAndResumesSameRevision(t *testing.T) {
	h, node, computer := backupHarness(t, 1, nil)
	computer, claim := startBackupComputer(t, h, node, computer)
	if _, _, err := h.store.BeginComputerBackup(context.Background(), computer.ComputerID,
		ComputerBackupCreateRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"), IdempotencyKey: "backup-without-power-off"}); errorCode(err) != contract.ErrorConflict {
		t.Fatalf("running Backup without allow_power_off = %v, want %q", err, contract.ErrorConflict)
	}
	reserved, replayed, err := h.store.BeginComputerBackup(context.Background(), computer.ComputerID,
		ComputerBackupCreateRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"), IdempotencyKey: "backup-1", AllowPowerOff: true})
	if err != nil || replayed {
		t.Fatalf("begin Backup = %#v replayed=%t err=%v", reserved, replayed, err)
	}
	if reserved.DesiredState != contract.ServiceDesiredRunning || reserved.CurrentJob.DesiredState != contract.ServiceDesiredStopped ||
		reserved.CurrentJob.State != contract.JobStopping || reserved.ReconfigurationPhase != ComputerReconfigurationBackingUp ||
		reserved.IntentRevision != computer.IntentRevision+1 || reserved.AppliedRevision != computer.AppliedRevision {
		t.Fatalf("disruptive Backup intent = %#v", reserved)
	}
	if directives, err := h.store.ListNodeComputerBackupDirectives(context.Background(), "fabric-computer-node", node.NodeID, node.BootSessionID); err != nil || len(directives) != 0 {
		t.Fatalf("live Computer Backup directives = %#v err=%v", directives, err)
	}
	finishBackupQuiescence(t, h, claim, "backup-quiescence")
	directives, err := h.store.ListNodeComputerBackupDirectives(context.Background(), "fabric-computer-node", node.NodeID, node.BootSessionID)
	if err != nil || len(directives) != 1 || directives[0].OperationRevision != reserved.IntentRevision || directives[0].AllocatedSize != computer.DesiredDiskBytes {
		t.Fatalf("detached Computer Backup directives = %#v err=%v", directives, err)
	}
	backup, resumed := acknowledgeBackup(t, h, node, directives[0], successfulBackupReceipt(directives[0]))
	if backup.BackupID != directives[0].BackupID || backup.Encryption != BackupEncryptionNone ||
		backup.SourceStorageID != computer.StorageID || backup.SourceGeneration != computer.StorageGeneration ||
		len(backup.Copies) != 1 || backup.Copies[0].CopyID != directives[0].CopyID ||
		backup.Provenance.Kind != "backup" || backup.Provenance.BackupID != backup.BackupID {
		t.Fatalf("published Backup = %#v", backup)
	}
	if resumed.IntentRevision != reserved.IntentRevision || resumed.AppliedRevision != reserved.IntentRevision ||
		resumed.DesiredState != contract.ServiceDesiredRunning || resumed.CurrentJob.State != contract.JobQueued ||
		resumed.ReconfigurationPhase != ComputerReconfigurationStable {
		t.Fatalf("unchanged-intent Backup resume = %#v", resumed)
	}
	if _, _, err := h.store.BeginComputerBackup(context.Background(), resumed.ComputerID,
		ComputerBackupCreateRequest{ComputerMutationPrecondition: computerPrecondition(resumed, "operator"), IdempotencyKey: "at-cap", AllowPowerOff: true}); errorCode(err) != contract.ErrorConflict {
		t.Fatalf("Backup at cap error = %v, want %q", err, contract.ErrorConflict)
	}
	list, err := h.store.ListComputerBackups(context.Background(), resumed.ComputerID)
	if err != nil || len(list.Backups) != 1 || list.Backups[0].BackupID != backup.BackupID {
		t.Fatalf("cap pressure auto-deleted Backup: %#v err=%v", list, err)
	}
}

func TestComputerBackupStopAndFailureRacesNeverResumeStaleIntent(t *testing.T) {
	t.Run("stopped stays stopped", func(t *testing.T) {
		h, node, computer := backupHarness(t, 2, nil)
		computer, claim := startBackupComputer(t, h, node, computer)
		_, err := h.store.SetComputerDesiredState(context.Background(), computer.ComputerID,
			computerDesiredRequest(computer, contract.ServiceDesiredStopped, "operator-stop"))
		if err != nil {
			t.Fatal(err)
		}
		finishBackupQuiescence(t, h, claim, "operator-stop-quiescence")
		stopped, err := h.store.GetComputer(context.Background(), computer.ComputerID)
		if err != nil || stopped.CurrentJob.State != contract.JobStopped || stopped.BoundNodeID == "" {
			t.Fatalf("stopped bound Computer = %#v err=%v", stopped, err)
		}
		reserved, _, err := h.store.BeginComputerBackup(context.Background(), stopped.ComputerID,
			ComputerBackupCreateRequest{ComputerMutationPrecondition: computerPrecondition(stopped, "operator"), IdempotencyKey: "backup-stopped"})
		if err != nil {
			t.Fatal(err)
		}
		directives, err := h.store.ListNodeComputerBackupDirectives(context.Background(), "fabric-computer-node", node.NodeID, node.BootSessionID)
		if err != nil || len(directives) != 1 {
			t.Fatalf("stopped Backup directive = %#v err=%v", directives, err)
		}
		_, completed := acknowledgeBackup(t, h, node, directives[0], successfulBackupReceipt(directives[0]))
		if completed.DesiredState != contract.ServiceDesiredStopped || completed.CurrentJob.State != contract.JobStopped ||
			completed.IntentRevision != reserved.IntentRevision {
			t.Fatalf("stopped Backup resumed Computer: %#v", completed)
		}
	})

	t.Run("stop wins successful publication", func(t *testing.T) {
		h, node, computer := backupHarness(t, 2, nil)
		computer, claim := startBackupComputer(t, h, node, computer)
		reserved, _, err := h.store.BeginComputerBackup(context.Background(), computer.ComputerID,
			ComputerBackupCreateRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"), IdempotencyKey: "backup-stop", AllowPowerOff: true})
		if err != nil {
			t.Fatal(err)
		}
		finishBackupQuiescence(t, h, claim, "backup-stop-quiescence")
		stopped, err := h.store.SetComputerDesiredState(context.Background(), computer.ComputerID,
			computerDesiredRequest(reserved, contract.ServiceDesiredStopped, "operator-stop"))
		if err != nil || stopped.IntentRevision != reserved.IntentRevision+1 || stopped.ReconfigurationPhase != ComputerReconfigurationBackingUp ||
			stopped.AppliedRevision != reserved.AppliedRevision {
			t.Fatalf("stop during Backup = %#v err=%v", stopped, err)
		}
		directives, err := h.store.ListNodeComputerBackupDirectives(context.Background(), "fabric-computer-node", node.NodeID, node.BootSessionID)
		if err != nil || len(directives) != 1 {
			t.Fatalf("stopped Backup directive = %#v err=%v", directives, err)
		}
		_, completed := acknowledgeBackup(t, h, node, directives[0], successfulBackupReceipt(directives[0]))
		if completed.DesiredState != contract.ServiceDesiredStopped || completed.CurrentJob.State != contract.JobStopped ||
			completed.IntentRevision != stopped.IntentRevision || completed.AppliedRevision != stopped.IntentRevision {
			t.Fatalf("Backup publication resumed stale desired-running intent: %#v", completed)
		}
	})

	t.Run("ENOSPC proves absence and resumes only unchanged intent", func(t *testing.T) {
		h, node, computer := backupHarness(t, 2, nil)
		computer, claim := startBackupComputer(t, h, node, computer)
		_, _, err := h.store.BeginComputerBackup(context.Background(), computer.ComputerID,
			ComputerBackupCreateRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"), IdempotencyKey: "backup-enospc", AllowPowerOff: true})
		if err != nil {
			t.Fatal(err)
		}
		finishBackupQuiescence(t, h, claim, "backup-enospc-quiescence")
		directives, _ := h.store.ListNodeComputerBackupDirectives(context.Background(), "fabric-computer-node", node.NodeID, node.BootSessionID)
		receipt := successfulBackupReceipt(directives[0])
		receipt.Kind = computerBackupFailureReceiptKind
		receipt.ContentDigest = ""
		receipt.FailureCode = "insufficient_disk"
		receipt.CopyAbsent = true
		backup, resumed := acknowledgeBackup(t, h, node, directives[0], receipt)
		if backup.BackupID != "" || resumed.CurrentJob.State != contract.JobQueued || resumed.ReconfigurationPhase != ComputerReconfigurationStable {
			t.Fatalf("ENOSPC Backup result = backup=%#v Computer=%#v", backup, resumed)
		}
		list, err := h.store.ListComputerBackups(context.Background(), computer.ComputerID)
		if err != nil || len(list.Backups) != 0 || list.LastOperation == nil ||
			list.LastOperation.Status != "failed" || list.LastOperation.FailureCode != ComputerBackupFailureInsufficientDisk ||
			resumed.LastBackupOperation == nil || resumed.LastBackupOperation.FailureCode != ComputerBackupFailureInsufficientDisk {
			t.Fatalf("ENOSPC created a Backup: %#v err=%v", list, err)
		}
	})

	t.Run("latched failure stays failed", func(t *testing.T) {
		h, node, computer := backupHarness(t, 2, nil)
		computer, claim := startBackupComputer(t, h, node, computer)
		reserved, _, err := h.store.BeginComputerBackup(context.Background(), computer.ComputerID,
			ComputerBackupCreateRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"), IdempotencyKey: "backup-failed", AllowPowerOff: true})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := h.store.CompleteAttempt(context.Background(), "fabric-computer-node", claim.Job.JobID,
			claim.Lease.AttemptID, CompletionRequest{FencingToken: claim.Lease.FencingToken,
				IdempotencyKey: "backup-failed-quiescence", Result: ProcessResult{OutputError: "quiescence failed"}}); err != nil {
			t.Fatal(err)
		}
		directives, err := h.store.ListNodeComputerBackupDirectives(context.Background(), "fabric-computer-node", node.NodeID, node.BootSessionID)
		if err != nil || len(directives) != 1 {
			t.Fatalf("latched-failed Backup directive = %#v err=%v", directives, err)
		}
		_, completed := acknowledgeBackup(t, h, node, directives[0], successfulBackupReceipt(directives[0]))
		if completed.CurrentJob.State != contract.JobFailed || completed.ReconfigurationPhase != ComputerReconfigurationStable ||
			completed.AppliedRevision != reserved.IntentRevision {
			t.Fatalf("latched-failed Backup completion = %#v", completed)
		}
	})
}

func TestComputerBackupExplicitPruneAndRemovalSupersession(t *testing.T) {
	t.Run("explicit prune retains immutable logical record", func(t *testing.T) {
		h, node, computer := backupHarness(t, 2, nil)
		computer, claim := startBackupComputer(t, h, node, computer)
		_, _, err := h.store.BeginComputerBackup(context.Background(), computer.ComputerID,
			ComputerBackupCreateRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"), IdempotencyKey: "backup-prune-source", AllowPowerOff: true})
		if err != nil {
			t.Fatal(err)
		}
		finishBackupQuiescence(t, h, claim, "backup-prune-quiescence")
		creates, _ := h.store.ListNodeComputerBackupDirectives(context.Background(), "fabric-computer-node", node.NodeID, node.BootSessionID)
		backup, completed := acknowledgeBackup(t, h, node, creates[0], successfulBackupReceipt(creates[0]))
		planned, replayed, err := h.store.BeginComputerBackupPrune(context.Background(), completed.ComputerID,
			ComputerBackupPruneRequest{ComputerMutationPrecondition: computerPrecondition(completed, "operator"),
				BackupID: backup.BackupID, IdempotencyKey: "prune-explicit"})
		if err != nil || replayed || planned.Status != "pruning" {
			t.Fatalf("plan prune = %#v replayed=%t err=%v", planned, replayed, err)
		}
		directives, err := h.store.ListNodeComputerBackupPruneDirectives(context.Background(), "fabric-computer-node", node.NodeID, node.BootSessionID)
		if err != nil || len(directives) != 1 {
			t.Fatalf("prune directives = %#v err=%v", directives, err)
		}
		validRemoval := backupRemovalReceipt(directives[0])
		for name, mutate := range map[string]func(*ComputerBackupCopyRemovalReceipt){
			"backup":     func(r *ComputerBackupCopyRemovalReceipt) { r.BackupID = "other" },
			"copy":       func(r *ComputerBackupCopyRemovalReceipt) { r.CopyID = "other" },
			"computer":   func(r *ComputerBackupCopyRemovalReceipt) { r.ComputerID = "other" },
			"storage":    func(r *ComputerBackupCopyRemovalReceipt) { r.StorageID = "other" },
			"generation": func(r *ComputerBackupCopyRemovalReceipt) { r.StorageGeneration++ },
			"node":       func(r *ComputerBackupCopyRemovalReceipt) { r.NodeID = "other" },
			"root":       func(r *ComputerBackupCopyRemovalReceipt) { r.RootInstanceID = "other" },
			"operation":  func(r *ComputerBackupCopyRemovalReceipt) { r.OperationRevision++ },
			"fence":      func(r *ComputerBackupCopyRemovalReceipt) { r.CleanupFence = "other" },
			"helper":     func(r *ComputerBackupCopyRemovalReceipt) { r.HelperGeneration = 0 },
		} {
			t.Run("reject "+name, func(t *testing.T) {
				mutated := validRemoval
				mutate(&mutated)
				if _, err := h.store.AcknowledgeComputerBackupPrune(context.Background(), "fabric-computer-node", completed.ComputerID,
					ComputerBackupPruneAcknowledgementRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID,
						IdempotencyKey: "mutated-" + name, Receipt: mutated}); err == nil {
					t.Fatal("mutated Backup prune receipt was accepted")
				}
			})
		}
		pruned, err := h.store.AcknowledgeComputerBackupPrune(context.Background(), "fabric-computer-node", completed.ComputerID,
			ComputerBackupPruneAcknowledgementRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID,
				IdempotencyKey: "prune-receipt", Receipt: validRemoval})
		if err != nil || pruned.Status != "pruned" || len(pruned.Copies) != 1 || pruned.Copies[0].Phase != "removed" {
			t.Fatalf("pruned Backup = %#v err=%v", pruned, err)
		}
	})

	t.Run("remove wins in-flight create and gates composite acknowledgement", func(t *testing.T) {
		h, node, computer := backupHarness(t, 2, nil)
		computer, claim := startBackupComputer(t, h, node, computer)
		reserved, _, err := h.store.BeginComputerBackup(context.Background(), computer.ComputerID,
			ComputerBackupCreateRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"), IdempotencyKey: "backup-remove", AllowPowerOff: true})
		if err != nil {
			t.Fatal(err)
		}
		finishBackupQuiescence(t, h, claim, "backup-remove-quiescence")
		removed, err := h.store.RemoveComputer(context.Background(), computer.ComputerID,
			ComputerRemoveRequest{ComputerMutationPrecondition: computerPrecondition(reserved, "operator-remove")})
		if err != nil || removed.DesiredState != contract.ServiceDesiredRemoved {
			t.Fatalf("remove during Backup = %#v err=%v", removed, err)
		}
		removals, err := h.store.ListNodeRemovalDirectives(context.Background(), "fabric-computer-node", node.NodeID, node.BootSessionID)
		if err != nil || len(removals) != 1 || removals[0].ComputerBackupCopies == nil || len(removals[0].ComputerBackupCopies.Copies) != 1 {
			t.Fatalf("composite removal directive = %#v err=%v", removals, err)
		}
		serviceReceipt := RemovalAcknowledgementRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID,
			RemovalGeneration: removals[0].RemovalGeneration, CleanupFence: removals[0].CleanupFence,
			RootInstanceID: removals[0].RootInstanceID, IdempotencyKey: "service-removed"}
		if _, err := h.store.AcknowledgeServiceRemoval(context.Background(), "fabric-computer-node", removals[0].JobID, serviceReceipt); errorCode(err) != contract.ErrorConflict {
			t.Fatalf("service removal without Backup absence = %v, want conflict", err)
		}
		copyDirective := removals[0].ComputerBackupCopies.Copies[0]
		if !copyDirective.Superseded {
			t.Fatalf("in-flight create removal directive lacks durable supersession state: %#v", copyDirective)
		}
		if _, err := h.store.AcknowledgeComputerBackupPrune(context.Background(), "fabric-computer-node", computer.ComputerID,
			ComputerBackupPruneAcknowledgementRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID,
				IdempotencyKey: "superseded-copy-removed", Receipt: backupRemovalReceipt(copyDirective)}); err != nil {
			t.Fatal(err)
		}
		if _, err := h.store.AcknowledgeServiceRemoval(context.Background(), "fabric-computer-node", removals[0].JobID, serviceReceipt); err != nil {
			t.Fatalf("service removal after Backup absence: %v", err)
		}
	})
}

func TestComputerRemovalGatesPublishedBackupCopyAbsence(t *testing.T) {
	h, node, computer := backupHarness(t, 2, nil)
	computer, claim := startBackupComputer(t, h, node, computer)
	_, _, err := h.store.BeginComputerBackup(context.Background(), computer.ComputerID,
		ComputerBackupCreateRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"), IdempotencyKey: "backup-published-remove", AllowPowerOff: true})
	if err != nil {
		t.Fatal(err)
	}
	finishBackupQuiescence(t, h, claim, "backup-published-remove-quiescence")
	creates, _ := h.store.ListNodeComputerBackupDirectives(context.Background(), "fabric-computer-node", node.NodeID, node.BootSessionID)
	_, completed := acknowledgeBackup(t, h, node, creates[0], successfulBackupReceipt(creates[0]))
	removed, err := h.store.RemoveComputer(context.Background(), completed.ComputerID,
		ComputerRemoveRequest{ComputerMutationPrecondition: computerPrecondition(completed, "operator-remove")})
	if err != nil || removed.DesiredState != contract.ServiceDesiredRemoved {
		t.Fatalf("remove published Backup Computer = %#v err=%v", removed, err)
	}
	removals, err := h.store.ListNodeRemovalDirectives(context.Background(), "fabric-computer-node", node.NodeID, node.BootSessionID)
	if err != nil || len(removals) != 1 || removals[0].ComputerBackupCopies == nil || len(removals[0].ComputerBackupCopies.Copies) != 1 {
		t.Fatalf("published composite removal directive = %#v err=%v", removals, err)
	}
	copyDirective := removals[0].ComputerBackupCopies.Copies[0]
	if copyDirective.Superseded {
		t.Fatalf("published Backup copy was mislabeled as a stale create: %#v", copyDirective)
	}
	serviceReceipt := RemovalAcknowledgementRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID,
		RemovalGeneration: removals[0].RemovalGeneration, CleanupFence: removals[0].CleanupFence,
		RootInstanceID: removals[0].RootInstanceID, IdempotencyKey: "published-service-removed"}
	if _, err := h.store.AcknowledgeServiceRemoval(context.Background(), "fabric-computer-node", removals[0].JobID, serviceReceipt); errorCode(err) != contract.ErrorConflict {
		t.Fatalf("published service removal without Backup absence = %v, want conflict", err)
	}
	if _, err := h.store.AcknowledgeComputerBackupPrune(context.Background(), "fabric-computer-node", computer.ComputerID,
		ComputerBackupPruneAcknowledgementRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID,
			IdempotencyKey: "published-copy-removed", Receipt: backupRemovalReceipt(copyDirective)}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.AcknowledgeServiceRemoval(context.Background(), "fabric-computer-node", removals[0].JobID, serviceReceipt); err != nil {
		t.Fatalf("published service removal after Backup absence: %v", err)
	}
}

func TestComputerBackupReceiptAuthorityMutationsFailClosed(t *testing.T) {
	row := computerBackupOperationRow{ComputerID: "computer", OperationRevision: 3, BackupID: "backup",
		CopyID: "copy", StorageID: "storage", StorageGeneration: 2, AllocatedSize: 1024,
		BoundNodeID: "node", RootInstanceID: "root", JobID: "job", CleanupFence: "fence"}
	valid := ComputerBackupCopyReceipt{Kind: computerBackupCopyReceiptKind, ReceiptID: "receipt",
		BackupID: row.BackupID, CopyID: row.CopyID, ComputerID: row.ComputerID, StorageID: row.StorageID,
		StorageGeneration: row.StorageGeneration, NodeID: row.BoundNodeID, RootInstanceID: row.RootInstanceID,
		JobID: row.JobID, OperationRevision: row.OperationRevision, CleanupFence: row.CleanupFence,
		HelperGeneration: 1, AllocatedSize: row.AllocatedSize,
		ContentDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Encryption: BackupEncryptionNone}
	mutations := map[string]func(*ComputerBackupCopyReceipt){
		"backup":     func(r *ComputerBackupCopyReceipt) { r.BackupID = "other" },
		"copy":       func(r *ComputerBackupCopyReceipt) { r.CopyID = "other" },
		"computer":   func(r *ComputerBackupCopyReceipt) { r.ComputerID = "other" },
		"storage":    func(r *ComputerBackupCopyReceipt) { r.StorageID = "other" },
		"generation": func(r *ComputerBackupCopyReceipt) { r.StorageGeneration++ },
		"node":       func(r *ComputerBackupCopyReceipt) { r.NodeID = "other" },
		"root":       func(r *ComputerBackupCopyReceipt) { r.RootInstanceID = "other" },
		"job":        func(r *ComputerBackupCopyReceipt) { r.JobID = "other" },
		"operation":  func(r *ComputerBackupCopyReceipt) { r.OperationRevision++ },
		"fence":      func(r *ComputerBackupCopyReceipt) { r.CleanupFence = "other" },
		"helper":     func(r *ComputerBackupCopyReceipt) { r.HelperGeneration = 0 },
		"size":       func(r *ComputerBackupCopyReceipt) { r.AllocatedSize++ },
		"digest":     func(r *ComputerBackupCopyReceipt) { r.ContentDigest = "sha256:bad" },
		"encryption": func(r *ComputerBackupCopyReceipt) { r.Encryption = "aes" },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			receipt := valid
			mutate(&receipt)
			if err := validateBackupReceipt(row, receipt); err == nil {
				t.Fatal("mutated Backup receipt was accepted")
			}
		})
	}
}

func TestComputerBackupAcknowledgementsRequireBoundNodeAndCurrentRoot(t *testing.T) {
	h, node, computer := backupHarness(t, 2, nil)
	computer, claim := startBackupComputer(t, h, node, computer)
	_, _, err := h.store.BeginComputerBackup(context.Background(), computer.ComputerID,
		ComputerBackupCreateRequest{ComputerMutationPrecondition: computerPrecondition(computer, "operator"), IdempotencyKey: "backup-authority", AllowPowerOff: true})
	if err != nil {
		t.Fatal(err)
	}
	finishBackupQuiescence(t, h, claim, "backup-authority-quiescence")
	creates, err := h.store.ListNodeComputerBackupDirectives(context.Background(), "fabric-computer-node", node.NodeID, node.BootSessionID)
	if err != nil || len(creates) != 1 {
		t.Fatalf("Backup authority directive = %#v err=%v", creates, err)
	}
	createReceipt := successfulBackupReceipt(creates[0])
	otherRegistration := node.NodeRegistration
	otherRegistration.NodeID = "other-backup-node"
	otherRegistration.BootSessionID = "other-backup-boot"
	otherRegistration.RootInstanceID = "other-backup-root"
	otherNode, err := h.store.RegisterNode(context.Background(), fabric.Identity{NodeID: "fabric-computer-node"},
		otherRegistration, NodePolicy{MaxOneshotSlots: 1, MaxServiceSlots: 1}, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := h.store.AcknowledgeComputerBackup(context.Background(), "fabric-computer-node", computer.ComputerID,
		ComputerBackupAcknowledgementRequest{NodeID: otherNode.NodeID, BootSessionID: otherNode.BootSessionID,
			IdempotencyKey: "forged-create-node", Receipt: createReceipt}); errorCode(err) != contract.ErrorAttemptNotOwned {
		t.Fatalf("forged create node acknowledgement = %v, want %q", err, contract.ErrorAttemptNotOwned)
	}
	if _, err := h.store.db.Exec(`UPDATE nodes SET root_instance_id='recreated-backup-root' WHERE node_id=?`, node.NodeID); err != nil {
		t.Fatal(err)
	}
	if _, _, err := h.store.AcknowledgeComputerBackup(context.Background(), "fabric-computer-node", computer.ComputerID,
		ComputerBackupAcknowledgementRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID,
			IdempotencyKey: "recreated-create-root", Receipt: createReceipt}); errorCode(err) != contract.ErrorConflict {
		t.Fatalf("recreated create root acknowledgement = %v, want %q", err, contract.ErrorConflict)
	}
	if _, err := h.store.db.Exec(`UPDATE nodes SET root_instance_id=? WHERE node_id=?`, creates[0].RootInstanceID, node.NodeID); err != nil {
		t.Fatal(err)
	}
	backup, completed := acknowledgeBackup(t, h, node, creates[0], createReceipt)
	planned, _, err := h.store.BeginComputerBackupPrune(context.Background(), computer.ComputerID,
		ComputerBackupPruneRequest{ComputerMutationPrecondition: computerPrecondition(completed, "operator"),
			BackupID: backup.BackupID, IdempotencyKey: "prune-authority"})
	if err != nil || planned.Status != "pruning" {
		t.Fatalf("plan authority prune = %#v err=%v", planned, err)
	}
	prunes, err := h.store.ListNodeComputerBackupPruneDirectives(context.Background(), "fabric-computer-node", node.NodeID, node.BootSessionID)
	if err != nil || len(prunes) != 1 {
		t.Fatalf("Backup prune authority directive = %#v err=%v", prunes, err)
	}
	pruneReceipt := backupRemovalReceipt(prunes[0])
	if _, err := h.store.AcknowledgeComputerBackupPrune(context.Background(), "fabric-computer-node", computer.ComputerID,
		ComputerBackupPruneAcknowledgementRequest{NodeID: otherNode.NodeID, BootSessionID: otherNode.BootSessionID,
			IdempotencyKey: "forged-prune-node", Receipt: pruneReceipt}); errorCode(err) != contract.ErrorAttemptNotOwned {
		t.Fatalf("forged prune node acknowledgement = %v, want %q", err, contract.ErrorAttemptNotOwned)
	}
	if _, err := h.store.db.Exec(`UPDATE nodes SET root_instance_id='recreated-prune-root' WHERE node_id=?`, node.NodeID); err != nil {
		t.Fatal(err)
	}
	if _, err := h.store.AcknowledgeComputerBackupPrune(context.Background(), "fabric-computer-node", computer.ComputerID,
		ComputerBackupPruneAcknowledgementRequest{NodeID: node.NodeID, BootSessionID: node.BootSessionID,
			IdempotencyKey: "recreated-prune-root", Receipt: pruneReceipt}); errorCode(err) != contract.ErrorConflict {
		t.Fatalf("recreated prune root acknowledgement = %v, want %q", err, contract.ErrorConflict)
	}
}
