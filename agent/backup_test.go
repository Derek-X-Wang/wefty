package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/Derek-X-Wang/wefty/l1"
	workloadrunner "github.com/Derek-X-Wang/wefty/runner"
	"github.com/Derek-X-Wang/wefty/runner/ocihelper"
)

func TestBackupControllerRejectsForeignNodeAndManagedRootBeforeMutation(t *testing.T) {
	controller := &backupController{nodeID: "node-1", bootSessionID: "boot-1", rootInstanceID: "root-1"}
	create := l1.ComputerBackupDirective{BoundNodeID: "node-1", RootInstanceID: "root-1"}
	prune := l1.ComputerBackupPruneDirective{BoundNodeID: "node-1", RootInstanceID: "root-1"}
	for name, mutate := range map[string]func() error{
		"create node": func() error {
			copy := create
			copy.BoundNodeID = "node-2"
			return controller.processCreate(t.Context(), copy)
		},
		"create root": func() error {
			copy := create
			copy.RootInstanceID = "root-2"
			return controller.processCreate(t.Context(), copy)
		},
		"prune node": func() error {
			copy := prune
			copy.BoundNodeID = "node-2"
			return controller.processPrune(t.Context(), copy)
		},
		"prune root": func() error {
			copy := prune
			copy.RootInstanceID = "root-2"
			return controller.processPrune(t.Context(), copy)
		},
	} {
		t.Run(name, func(t *testing.T) {
			// The nil runtime would panic if authority validation did not return
			// before reaching mutation mechanics.
			if err := mutate(); err == nil {
				t.Fatal("foreign Backup authority reached mutation mechanics")
			}
		})
	}
}

// stubBackupCopyRuntime is the helper seam for Backup-copy deletion: it
// records every copy it is asked to delete, refuses the ones named, and -- as
// the real helper does -- mints a fresh receipt identity on every successful
// call. That fresh identity is precisely what makes a replayed deletion look
// like new evidence to L1.
type stubBackupCopyRuntime struct {
	deletions []string
	refuse    map[string]bool
	minted    int
}

func (runtime *stubBackupCopyRuntime) CreateComputerBackup(context.Context, workloadrunner.ComputerBackupRequest) (workloadrunner.ComputerBackupCopyReceipt, error) {
	return workloadrunner.ComputerBackupCopyReceipt{}, errors.New("stubBackupCopyRuntime creates no Backup")
}

func (runtime *stubBackupCopyRuntime) DeleteComputerBackupCopy(_ context.Context, request workloadrunner.ComputerBackupCopyRemovalRequest) (workloadrunner.ComputerBackupCopyRemovalReceipt, error) {
	runtime.deletions = append(runtime.deletions, request.CopyID)
	if runtime.refuse[request.CopyID] {
		return workloadrunner.ComputerBackupCopyRemovalReceipt{}, &ocihelper.RPCError{
			Code: ocihelper.CodeEngineFailure, Message: "Backup copy deletion refused by the engine"}
	}
	runtime.minted++
	return workloadrunner.ComputerBackupCopyRemovalReceipt{
		Kind: "computer_backup_copy_removed", ReceiptID: fmt.Sprintf("absence-%s-%d", request.CopyID, runtime.minted),
		BackupID: request.BackupID, CopyID: request.CopyID, ComputerID: request.Storage.ComputerID,
		StorageID: request.Storage.StorageID, StorageGeneration: request.Storage.StorageGeneration,
		NodeID: request.NodeID, RootInstanceID: request.RootInstanceID,
		OperationRevision: request.OperationRevision, CleanupFence: request.CleanupFence,
		HelperGeneration: 7, Absent: true}, nil
}

func (runtime *stubBackupCopyRuntime) deletionsOf(copyID string) int {
	count := 0
	for _, deleted := range runtime.deletions {
		if deleted == copyID {
			count++
		}
	}
	return count
}

// TestStaleDirectiveReplaySkipsAnAcknowledgedBackupCopy is the partially
// completed list from #501. A removal deletes the first copy, L1 accepts its
// absence, and the second copy is refused; the list the node still holds names
// both. Replaying it must not delete the acknowledged copy again: a second
// deletion mints a second receipt, L1 refuses the different receipt for an
// already-removed prune, and since #465 that refusal counts into the removal's
// stall streak -- so a copy that is gone would keep the removal from ever
// finishing.
func TestStaleDirectiveReplaySkipsAnAcknowledgedBackupCopy(t *testing.T) {
	spool := openTestLogSpool(t, t.TempDir(), "replay-node", 1024)
	defer spool.Close()
	removal := testRuntimeRemoval("replay-job")
	prepared := time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)
	if err := spool.storeRuntimeResourceManifest(t.Context(), testRuntimeResourceManifest(removal.jobID, "attempt"), prepared); err != nil {
		t.Fatal(err)
	}
	if err := spool.beginRemoval(t.Context(), removal, prepared); err != nil {
		t.Fatal(err)
	}

	// The control plane behaves exactly as L1 did before #501: a positive
	// absence receipt whose identity differs from the accepted one is refused
	// for an already-removed prune. The test never wants to see that answer.
	accepted := map[string]string{}
	acknowledgements := 0
	client := newRoundTripClient(t, http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		var acknowledgement l1.ComputerBackupPruneAcknowledgementRequest
		if err := json.NewDecoder(request.Body).Decode(&acknowledgement); err != nil {
			t.Errorf("decode Backup prune acknowledgement: %v", err)
		}
		copyID := acknowledgement.Receipt.CopyID
		if prior, replayed := accepted[copyID]; replayed && prior != acknowledgement.Receipt.ReceiptID {
			response.Header().Set("Content-Type", "application/json")
			response.WriteHeader(http.StatusConflict)
			_, _ = response.Write([]byte(`{"error":{"code":"conflict","message":"Computer Backup prune replay differs from the accepted receipt"}}`))
			return
		}
		accepted[copyID] = acknowledgement.Receipt.ReceiptID
		acknowledgements++
		response.WriteHeader(http.StatusNoContent)
	}))

	helper := &stubBackupCopyRuntime{refuse: map[string]bool{"copy-second": true}}
	backups := &backupController{client: client, backupper: helper, nodeID: "replay-node",
		bootSessionID: "boot", rootInstanceID: removal.rootInstanceID, logf: t.Logf,
		inflight: make(map[string]struct{})}
	backups.copyRemovalAcknowledged = spool.backupCopyRemovalAcknowledged
	backups.recordCopyRemovalAcknowledged = func(ctx context.Context, directive l1.ComputerBackupPruneDirective) error {
		return spool.recordBackupCopyRemovalAcknowledged(ctx, directive, prepared)
	}

	now := prepared.Add(l1.DefaultRemovalStallBound + time.Minute)
	controller := &removalController{nodeID: "replay-node", bootSessionID: "boot",
		stallBound: l1.DefaultRemovalStallBound, now: func() time.Time { return now }, logf: t.Logf}
	controller.beginRemoval = func(context.Context, localRemoval) error { return nil }
	controller.loadRuntimeRemoval = spool.runtimeRemoval
	controller.removalStartedAt = spool.removalStartedAt
	controller.recordRemovalFailure = func(ctx context.Context, target localRemoval, code, detail string) error {
		return spool.recordRuntimeRemovalFailure(ctx, target, code, detail, controller.bootSessionID, now)
	}
	controller.recordUntypedFailure = func(ctx context.Context, target localRemoval) error {
		return spool.recordRuntimeRemovalUntypedFailure(ctx, target, controller.bootSessionID, now)
	}
	controller.ackRemovalStall = func(context.Context, localRemoval, runtimeRemovalRecord) error {
		t.Fatal("a partially completed Backup-copy list declared the removal stalled")
		return nil
	}
	// The one wiring under test: the same per-copy closure the agent builds.
	controller.removeBackupCopies = func(ctx context.Context, directives []l1.ComputerBackupPruneDirective) error {
		for _, directive := range directives {
			if err := backups.processPrune(ctx, directive); err != nil {
				return err
			}
		}
		return nil
	}
	controller.reapService = func(context.Context, string, string, []workloadrunner.RuntimeResourceManifest) (workloadrunner.ReapReceipt, error) {
		return workloadrunner.ReapReceipt{RuntimeQuiesced: true, Evidence: workloadrunner.ReapEvidenceAttempt,
			BootSessionID: controller.bootSessionID}, nil
	}
	controller.recordRuntimeQuiesced = func(ctx context.Context, target localRemoval, receipt workloadrunner.ReapReceipt) error {
		return spool.recordRuntimeQuiesced(ctx, target, receipt, now)
	}
	controller.recordRuntimeAttested = func(ctx context.Context, target localRemoval, attestation workloadrunner.RuntimeRemovalAttestation) error {
		return spool.recordRuntimeAttested(ctx, target, attestation, now)
	}
	controller.purgeJob = func(context.Context, string) error { return nil }
	controller.removeResource = func(context.Context, localRemoval) error { return nil }
	controller.deleteRuntimeData = func(context.Context, workloadrunner.RuntimeRemovalProofRequest) error { return nil }
	controller.attestRuntimeRemoval = func(_ context.Context, request workloadrunner.RuntimeRemovalProofRequest) (workloadrunner.RuntimeRemovalAttestation, error) {
		return testRuntimeRemovalAttestation(runtimeRemovalManifest{Version: 1, JobID: request.JobID,
			RemovalGeneration: request.RemovalGeneration, Attempts: request.Attempts}), nil
	}
	removalAcknowledgements := 0
	controller.ackRemoval = func(context.Context, localRemoval) error { removalAcknowledgements++; return nil }
	controller.finishRemoval = func(ctx context.Context, target localRemoval) error {
		return spool.completeRemoval(ctx, target)
	}

	copies := []l1.ComputerBackupPruneDirective{
		{BackupID: "backup-first", CopyID: "copy-first", ComputerID: "computer", StorageID: "storage",
			StorageGeneration: 1, BoundNodeID: "replay-node", RootInstanceID: removal.rootInstanceID,
			OperationRevision: 1, CleanupFence: removal.cleanupFence},
		{BackupID: "backup-second", CopyID: "copy-second", ComputerID: "computer", StorageID: "storage",
			StorageGeneration: 1, BoundNodeID: "replay-node", RootInstanceID: removal.rootInstanceID,
			OperationRevision: 1, CleanupFence: removal.cleanupFence},
	}
	directive := l1.RemovalDirective{JobID: removal.jobID, BoundNodeID: controller.nodeID, Kind: removal.kind,
		RemovalGeneration: removal.generation, CleanupFence: removal.cleanupFence,
		RootInstanceID:       removal.rootInstanceID,
		ComputerBackupCopies: &l1.ComputerBackupCopyClaims{Copies: copies}}

	if err := controller.reconcile(t.Context(), directive); err == nil {
		t.Fatal("the refused second Backup copy was reported as a completed removal")
	}
	refused, found, err := spool.runtimeRemoval(t.Context(), removal.jobID)
	if err != nil || !found || refused.failedAttempts != 1 ||
		refused.lastRefusalCode != string(ocihelper.CodeEngineFailure) {
		t.Fatalf("durable record after the partial list = %+v found=%t err=%v", refused, found, err)
	}
	if helper.deletionsOf("copy-first") != 1 || acknowledgements != 1 {
		t.Fatalf("first pass deleted copy-first %d times and acknowledged %d copies",
			helper.deletionsOf("copy-first"), acknowledgements)
	}

	// The node still holds the list L1 built before the first copy was
	// acknowledged. Replay it with the second copy's refusal lifted.
	helper.refuse = nil
	helper.deletions = nil
	acknowledgementsBefore := acknowledgements
	now = now.Add(time.Minute)
	if err := controller.reconcile(t.Context(), directive); err != nil {
		t.Fatalf("the replayed stale list did not finish the removal: %v", err)
	}
	if len(helper.deletions) != 1 || helper.deletionsOf("copy-second") != 1 {
		t.Fatalf("replay asked the helper to delete %v, want copy-second only", helper.deletions)
	}
	if acknowledgements-acknowledgementsBefore != 1 {
		t.Fatalf("replay sent %d acknowledgements, want 1", acknowledgements-acknowledgementsBefore)
	}
	if removalAcknowledgements != 1 {
		t.Fatalf("service removal acknowledgements = %d, want 1", removalAcknowledgements)
	}
	// The streak never grew past the one real refusal: the acknowledged copy
	// contributed nothing to it, and the completed removal released its record.
	if _, found, err := spool.runtimeRemoval(t.Context(), removal.jobID); err != nil || found {
		t.Fatalf("completed removal kept its durable record: found=%t err=%v", found, err)
	}
}
